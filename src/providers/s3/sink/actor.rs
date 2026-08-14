use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::delivery::{validate_batch_against_discovery, DeliveryDiscovery};
use crate::durable::DurableStorage;
use crate::metrics::SinkCounters;
use crate::pipeline::delivery_tracker::DeliveryTracker;
use crate::pipeline::memory::MemoryReservation;
use crate::pipeline::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
use crate::pipeline::PipelineFailure;
use crate::serializer::JsonBatchEncoder;

use super::config::{PartitionPathChange, S3SinkConfig};
use super::journal::{EpochJournal, OpenDisposition};
use super::object_key::ObjectKey;
use super::partitioning::{Partitioner, RowRoute};
use super::upload::{upload_with_retry, ObjectUploader};

const MAX_GROUPS_BEFORE_YIELD: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BufferKey {
    table: Arc<str>,
    partition_path: Arc<str>,
}

struct ObjectBuffer {
    topic: Arc<str>,
    source_partition: i64,
    start_offset: i64,
    rows: usize,
    payload: Vec<u8>,
}

#[derive(Default)]
struct Epoch {
    buffers: BTreeMap<BufferKey, ObjectBuffer>,
    delivery_rows: Vec<(DeliveryId, usize)>,
    reservations: Vec<MemoryReservation>,
    retained_bytes: usize,
    record_time_base_ms: Option<i64>,
    last_main_partition: Option<Arc<str>>,
    first_seen: Option<Instant>,
}

pub(super) struct ClosedObject {
    pub(super) epoch_id: u64,
    pub(super) key: ObjectKey,
    pub(super) payload: Bytes,
    pub(super) rows: usize,
}

struct ClosedEpoch {
    remaining_objects: usize,
    delivery_rows: Vec<(DeliveryId, usize)>,
    reservations: Vec<MemoryReservation>,
    journal: Arc<EpochJournal>,
    journal_closed: bool,
}

struct RoutedRow {
    output_index: usize,
    row_index: usize,
    table: Arc<str>,
    is_dlq: bool,
    route: RowRoute,
}

struct PendingDelivery {
    delivery_id: DeliveryId,
    rows: Vec<RoutedRow>,
    encoders: Vec<JsonBatchEncoder>,
    next_row: usize,
    _input_reservations: Vec<MemoryReservation>,
    _route_reservations: Vec<MemoryReservation>,
}

fn routed_row_order(left: &RoutedRow, right: &RoutedRow) -> core::cmp::Ordering {
    (
        left.route.topic.as_ref(),
        left.route.partition,
        left.route.offset,
        left.route.message_index,
        left.is_dlq,
    )
        .cmp(&(
            right.route.topic.as_ref(),
            right.route.partition,
            right.route.offset,
            right.route.message_index,
            right.is_dlq,
        ))
}

const ROUTE_RETAINED_OVERHEAD_BYTES: usize = 128;

fn routed_row_retained_bytes(row: &RoutedRow, serialized_bytes: usize) -> usize {
    serialized_bytes
        .saturating_add(ROUTE_RETAINED_OVERHEAD_BYTES)
        .saturating_add(row.table.len())
        .saturating_add(row.route.topic.len())
        .saturating_add(row.route.partition_path.len())
}

struct ActiveUpload {
    object: ClosedObject,
    result: Result<(), PipelineFailure>,
}

pub struct S3Sink {
    config: S3ActorConfig,
    uploader: Arc<dyn ObjectUploader>,
    partitioner: Partitioner,
    counters: Arc<SinkCounters>,
    keep_system_columns: bool,
    expected_partition_id: i64,
    discovery: Arc<DeliveryDiscovery>,
    epoch: Epoch,
    ready: VecDeque<ClosedObject>,
    closed_epochs: BTreeMap<u64, ClosedEpoch>,
    next_epoch_id: u64,
    progress: DeliveryTracker,
    buffered_bytes: usize,
    epoch_byte_limit: usize,
    in_flight_objects: usize,
    memory_barrier_epoch: Option<u64>,
    durable_storage: Arc<dyn DurableStorage>,
}

struct S3ActorConfig {
    prefix: String,
    rotation: super::config::RotationConfig,
    buffering: super::config::BufferingConfig,
    retry: super::config::RetryConfig,
    max_in_flight_objects: usize,
}

impl S3Sink {
    pub fn new(
        config: S3SinkConfig,
        uploader: Arc<dyn ObjectUploader>,
        counters: Arc<SinkCounters>,
        keep_system_columns: bool,
        expected_partition_id: i64,
        discovery: Arc<DeliveryDiscovery>,
        durable_storage: Arc<dyn DurableStorage>,
    ) -> anyhow::Result<Self> {
        let partitioner = Partitioner::new(&config.partitioning)?;
        let epoch_byte_limit = config.epoch_byte_limit();
        let config = S3ActorConfig {
            prefix: config.prefix,
            rotation: config.rotation,
            buffering: config.buffering,
            retry: config.retry,
            max_in_flight_objects: config.upload.max_in_flight_objects,
        };
        Ok(Self {
            config,
            uploader,
            partitioner,
            counters,
            keep_system_columns,
            expected_partition_id,
            discovery,
            epoch: Epoch::default(),
            ready: VecDeque::new(),
            closed_epochs: BTreeMap::new(),
            next_epoch_id: 0,
            progress: DeliveryTracker::new(),
            buffered_bytes: 0,
            epoch_byte_limit,
            in_flight_objects: 0,
            memory_barrier_epoch: None,
            durable_storage,
        })
    }

    fn route_delivery(&mut self, delivery: &Delivery) -> anyhow::Result<Vec<RoutedRow>> {
        let mut rows = Vec::new();
        for (output_index, output) in delivery.outputs.iter().enumerate() {
            let routes = self.partitioner.route_batch(output)?;
            for (row_index, route) in routes.into_iter().enumerate() {
                anyhow::ensure!(
                    route.partition == self.expected_partition_id,
                    "S3 source partition mismatch: actor owns {}, routed row belongs to {}",
                    self.expected_partition_id,
                    route.partition,
                );
                rows.push(RoutedRow {
                    output_index,
                    row_index,
                    table: Arc::clone(&output.table),
                    is_dlq: output.is_dlq,
                    route,
                });
            }
        }
        if !rows.is_sorted_by(|left, right| {
            routed_row_order(left, right) != core::cmp::Ordering::Greater
        }) {
            rows.sort_unstable_by(routed_row_order);
        }
        Ok(rows)
    }

    fn prepare_delivery(
        &mut self,
        delivery: Delivery,
        memory: &crate::pipeline::memory::PipelineMemory,
    ) -> anyhow::Result<PendingDelivery> {
        for output in &delivery.outputs {
            validate_batch_against_discovery(&self.discovery, output).map_err(|error| {
                anyhow::anyhow!(
                    "S3 delivery validation failed for dataset '{}': {error}",
                    output.table,
                )
            })?;
        }
        let route_bound = delivery.outputs.iter().try_fold(0_usize, |bytes, output| {
            let fixed = output
                .rows()
                .saturating_mul(ROUTE_RETAINED_OVERHEAD_BYTES.saturating_add(output.table.len()));
            Ok::<_, anyhow::Error>(
                bytes
                    .saturating_add(fixed)
                    .saturating_add(self.partitioner.route_strings_memory_bound(output)?),
            )
        })?;
        let mut route_reservations = Vec::with_capacity(2);
        if route_bound > 0 {
            route_reservations.push(memory.reserve_transform(route_bound));
        }
        let rows = self.route_delivery(&delivery)?;
        let delivery_id = delivery.id;
        let pending_rows = rows.len();
        let route_bytes = rows.iter().fold(0_usize, |bytes, row| {
            bytes.saturating_add(routed_row_retained_bytes(row, 0))
        });
        debug_assert!(
            route_bytes <= route_bound,
            "S3 route memory bound must cover every materialized route"
        );
        if let Some(reservation) = route_reservations.first() {
            if route_bytes <= route_bound {
                let _shrunk = reservation.shrink_to(route_bytes);
            } else {
                // Keep accounting correct if a future route representation grows
                // beyond this version's conservative pre-admission formula.
                route_reservations.push(memory.reserve_transform(route_bytes - route_bound));
            }
        }
        let encoders = delivery
            .outputs
            .iter()
            .map(|output| {
                let mut visible = vec![true; output.batch.num_columns()];
                if !self.keep_system_columns {
                    for column in output.system_columns.iter() {
                        visible[column.index] = false;
                    }
                }
                JsonBatchEncoder::new(&output.batch, |index| visible[index])
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.progress
            .accept(delivery_id, pending_rows, delivery.meta.source_messages)?;
        let input_reservations = delivery
            .outputs
            .into_iter()
            .map(|output| output.memory)
            .collect();
        Ok(PendingDelivery {
            delivery_id,
            rows,
            encoders,
            next_row: 0,
            _input_reservations: input_reservations,
            _route_reservations: route_reservations,
        })
    }

    async fn accept_next_message_group(
        &mut self,
        pending: &mut PendingDelivery,
        memory: &crate::pipeline::memory::PipelineMemory,
    ) -> Result<bool, PipelineFailure> {
        let start = pending.next_row;
        if start < pending.rows.len() {
            let identity = (
                &pending.rows[start].route.topic,
                pending.rows[start].route.partition,
                pending.rows[start].route.offset,
            );
            let mut end = start + 1;
            while end < pending.rows.len()
                && (
                    &pending.rows[end].route.topic,
                    pending.rows[end].route.partition,
                    pending.rows[end].route.offset,
                ) == identity
            {
                end += 1;
            }
            let epoch_before = self.next_epoch_id;
            self.accept_message_group(
                &pending.rows[start..end],
                &pending.encoders,
                pending.delivery_id,
                memory,
            )
            .await?;
            pending.next_row = end;
            self.memory_barrier_epoch = (self.next_epoch_id != epoch_before
                && memory.is_transform_pressured())
            .then(|| self.next_epoch_id.saturating_sub(1));
        }
        self.update_buffer_gauges();
        Ok(pending.next_row == pending.rows.len())
    }

    async fn accept_message_group(
        &mut self,
        rows: &[RoutedRow],
        encoders: &[JsonBatchEncoder],
        delivery_id: DeliveryId,
        memory: &crate::pipeline::memory::PipelineMemory,
    ) -> Result<(), PipelineFailure> {
        let first_main = rows.iter().find(|row| !row.is_dlq);
        let last_main = rows.iter().rfind(|row| !row.is_dlq);
        let record_time_ms = rows.iter().find_map(|row| row.route.record_time_ms);

        let partition_changed = self.config.rotation.on_partition_path_change
            == PartitionPathChange::Rotate
            && first_main.is_some_and(|row| {
                self.epoch
                    .last_main_partition
                    .as_deref()
                    .is_some_and(|previous| previous != row.route.partition_path.as_ref())
            });
        let record_time_rotated =
            self.config
                .rotation
                .record_time_interval
                .is_some_and(|interval| {
                    record_time_ms
                        .zip(self.epoch.record_time_base_ms)
                        .is_some_and(|(current, base)| {
                            current.saturating_sub(base)
                                >= i64::try_from(interval.0.as_millis()).unwrap_or(i64::MAX)
                        })
                });
        if partition_changed || record_time_rotated {
            self.close_epoch().await?;
        }
        if self.epoch.first_seen.is_none() {
            self.epoch.first_seen = Some(Instant::now());
            self.epoch.record_time_base_ms = record_time_ms;
        }
        if let Some(main) = last_main {
            self.epoch.last_main_partition = Some(Arc::clone(&main.route.partition_path));
        }

        // A source message is the smallest deterministic routing unit. Keep
        // its retained-memory lease with the epoch that owns its rows rather
        // than with the timing-dependent Delivery that happened to carry it.
        // A durable closed epoch can then release capacity even if a later
        // epoch still contains rows from the same Delivery.
        let mut object_limit_reached = false;
        let mut retained_bytes = 0_usize;
        let mut serialized_bytes = 0_usize;
        for row in rows {
            let key = BufferKey {
                table: Arc::clone(&row.table),
                partition_path: Arc::clone(&row.route.partition_path),
            };
            let buffer = self
                .epoch
                .buffers
                .entry(key)
                .or_insert_with(|| ObjectBuffer {
                    topic: Arc::clone(&row.route.topic),
                    source_partition: row.route.partition,
                    start_offset: row.route.offset,
                    rows: 0,
                    payload: Vec::new(),
                });
            buffer.start_offset = buffer.start_offset.min(row.route.offset);
            buffer.rows = buffer.rows.saturating_add(1);
            let before = buffer.payload.len();
            encoders[row.output_index].write_row(row.row_index, &mut buffer.payload);
            let row_bytes = buffer.payload.len().saturating_sub(before);
            serialized_bytes = serialized_bytes.saturating_add(row_bytes);
            retained_bytes =
                retained_bytes.saturating_add(routed_row_retained_bytes(row, row_bytes));
            object_limit_reached |= buffer.rows >= self.config.rotation.max_rows
                || buffer.payload.len() >= self.config.rotation.max_bytes.0;
        }
        if retained_bytes > 0 {
            self.epoch
                .reservations
                .push(memory.reserve_transform(retained_bytes));
        }
        self.buffered_bytes = self.buffered_bytes.saturating_add(serialized_bytes);
        if self.buffered_bytes > self.config.buffering.max_buffered_bytes.0 {
            tracing::warn!(
                buffered_bytes = self.buffered_bytes,
                configured_limit_bytes = self.config.buffering.max_buffered_bytes.0,
                "one atomic source message temporarily exceeded the per-partition S3 buffering limit"
            );
        }
        self.epoch.retained_bytes = self.epoch.retained_bytes.saturating_add(retained_bytes);
        match self.epoch.delivery_rows.last_mut() {
            Some((previous_delivery_id, delivery_rows)) if *previous_delivery_id == delivery_id => {
                *delivery_rows += rows.len();
            }
            _ => self.epoch.delivery_rows.push((delivery_id, rows.len())),
        }

        let budget_reached = self.epoch.buffers.len() > self.config.buffering.max_epoch_buffers
            || self.epoch.retained_bytes >= self.epoch_byte_limit;
        if self.epoch.buffers.len() > self.config.buffering.max_epoch_buffers {
            tracing::warn!(
                open_objects = self.epoch.buffers.len(),
                configured_limit = self.config.buffering.max_epoch_buffers,
                "one atomic source message temporarily exceeded the per-partition S3 open-object limit"
            );
        }
        // Pending uploads are deliberately not a rotation input: their count
        // depends on I/O timing and would produce different object boundaries
        // when the same source deliveries are replayed. The pending limit is
        // enforced only by run-loop admission below.
        if object_limit_reached || budget_reached {
            self.close_epoch().await?;
        }
        if self.pending_upload_objects() > self.config.buffering.max_pending_upload_objects {
            tracing::warn!(
                pending_upload_objects = self.pending_upload_objects(),
                configured_limit = self.config.buffering.max_pending_upload_objects,
                "one atomic source message temporarily exceeded the per-partition S3 pending-object soft limit"
            );
        }
        Ok(())
    }

    async fn close_epoch(&mut self) -> Result<(), PipelineFailure> {
        if self.epoch.buffers.is_empty() {
            return Ok(());
        }
        let epoch = std::mem::take(&mut self.epoch);
        let epoch_id = self.next_epoch_id;
        self.next_epoch_id = self.next_epoch_id.saturating_add(1);
        let remaining_objects = epoch.buffers.len();
        let mut objects = Vec::with_capacity(remaining_objects);
        for (buffer_key, buffer) in epoch.buffers {
            let key = ObjectKey::for_json_object(
                &self.config.prefix,
                &buffer_key.table,
                &buffer_key.partition_path,
                &buffer.topic,
                buffer.source_partition,
                buffer.start_offset,
            )
            .map_err(PipelineFailure::fatal)?;
            objects.push(ClosedObject {
                epoch_id,
                key,
                payload: Bytes::from(buffer.payload),
                rows: buffer.rows,
            });
        }
        let journal = Arc::new(EpochJournal::new(
            Arc::clone(&self.durable_storage),
            self.expected_partition_id,
            &objects,
        )?);
        let disposition = journal.ensure_open().await?;
        self.closed_epochs.insert(
            epoch_id,
            ClosedEpoch {
                remaining_objects,
                delivery_rows: epoch.delivery_rows,
                reservations: epoch.reservations,
                journal: Arc::clone(&journal),
                journal_closed: disposition == OpenDisposition::AlreadyClosed,
            },
        );
        match disposition {
            OpenDisposition::Upload => self.ready.extend(objects),
            OpenDisposition::AlreadyClosed => {
                for object in objects {
                    self.counters.add_rows(object.rows as u64);
                    self.counters.add_bytes(object.payload.len() as u64);
                    self.counters.add_flush();
                    self.buffered_bytes = self
                        .buffered_bytes
                        .checked_sub(object.payload.len())
                        .ok_or_else(|| {
                            PipelineFailure::fatal(anyhow::anyhow!(
                                "S3 replayed buffered byte counter underflow"
                            ))
                        })?;
                    self.complete_durable_object(&object)?;
                }
            }
        }
        self.update_buffer_gauges();
        Ok(())
    }

    fn start_upload(
        &mut self,
        uploads: &mut JoinSet<ActiveUpload>,
        cancellation: &CancellationToken,
    ) -> bool {
        let Some(object) = self.ready.pop_front() else {
            return false;
        };
        let uploader = Arc::clone(&self.uploader);
        let retry = self.config.retry.clone();
        let cancellation = cancellation.clone();
        let counters = Arc::clone(&self.counters);
        self.in_flight_objects = self.in_flight_objects.saturating_add(1);
        uploads.spawn(async move {
            let result = upload_with_retry(
                uploader,
                retry,
                object.key.as_str(),
                &object.payload,
                &cancellation,
                counters.as_ref(),
            )
            .await;
            ActiveUpload { object, result }
        });
        true
    }

    fn complete_upload(&mut self, active: ActiveUpload) -> Result<(), PipelineFailure> {
        self.in_flight_objects = self.in_flight_objects.checked_sub(1).ok_or_else(|| {
            PipelineFailure::fatal(anyhow::anyhow!("S3 in-flight upload counter underflow"))
        })?;
        active.result?;
        self.counters.add_rows(active.object.rows as u64);
        self.counters.add_bytes(active.object.payload.len() as u64);
        self.counters.add_flush();
        self.buffered_bytes = self
            .buffered_bytes
            .checked_sub(active.object.payload.len())
            .ok_or_else(|| {
                PipelineFailure::fatal(anyhow::anyhow!("S3 buffered byte counter underflow"))
            })?;

        self.complete_durable_object(&active.object)?;
        Ok(())
    }

    fn complete_durable_object(&mut self, object: &ClosedObject) -> Result<(), PipelineFailure> {
        let epoch_complete = {
            let epoch = self
                .closed_epochs
                .get_mut(&object.epoch_id)
                .ok_or_else(|| {
                    PipelineFailure::fatal(anyhow::anyhow!(
                        "missing S3 epoch progress {}",
                        object.epoch_id
                    ))
                })?;
            epoch.remaining_objects = epoch.remaining_objects.checked_sub(1).ok_or_else(|| {
                PipelineFailure::fatal(anyhow::anyhow!("S3 epoch progress underflow"))
            })?;
            epoch.remaining_objects == 0
        };
        if epoch_complete {
            // Actual upload completion is finalized asynchronously in the actor loop. Replayed
            // CLOSED epochs may finalize synchronously because the journal already proves the
            // entire object set durable.
            if self.closed_epochs[&object.epoch_id].journal_closed {
                self.finalize_epoch(object.epoch_id)?;
            }
        }
        tracing::info!(
            object_key = object.key.as_str(),
            rows = object.rows,
            bytes = object.payload.len(),
            "S3 object is durable"
        );
        Ok(())
    }

    fn finalize_epoch(&mut self, epoch_id: u64) -> Result<(), PipelineFailure> {
        let epoch = self.closed_epochs.remove(&epoch_id).ok_or_else(|| {
            PipelineFailure::fatal(anyhow::anyhow!("completed S3 epoch {epoch_id} disappeared"))
        })?;
        for (delivery_id, rows) in epoch.delivery_rows {
            self.progress
                .complete(delivery_id, rows)
                .map_err(PipelineFailure::fatal)?;
        }
        drop(epoch.reservations);
        Ok(())
    }

    async fn close_completed_epoch_journal(&mut self) -> Result<bool, PipelineFailure> {
        let Some((&epoch_id, epoch)) = self
            .closed_epochs
            .iter()
            .find(|(_, epoch)| epoch.remaining_objects == 0 && !epoch.journal_closed)
        else {
            return Ok(false);
        };
        let journal = Arc::clone(&epoch.journal);
        journal.mark_closed().await?;
        self.closed_epochs
            .get_mut(&epoch_id)
            .ok_or_else(|| {
                PipelineFailure::fatal(anyhow::anyhow!(
                    "S3 epoch {epoch_id} disappeared while closing its journal"
                ))
            })?
            .journal_closed = true;
        self.finalize_epoch(epoch_id)?;
        Ok(true)
    }

    async fn emit_committed(
        &mut self,
        events: &tokio::sync::mpsc::Sender<SinkEvent>,
    ) -> anyhow::Result<()> {
        if let Some(committed) = self.progress.take_committed() {
            self.counters.add_source_messages(committed.source_messages);
            events
                .send(SinkEvent::CommittedThrough(committed.through))
                .await
                .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
        }
        Ok(())
    }

    fn wall_clock_deadline(&self) -> Option<Instant> {
        self.config
            .rotation
            .wall_clock_interval
            .zip(self.epoch.first_seen)
            .map(|(interval, started)| started + interval.0)
    }

    fn update_gauges(&self) {
        self.update_buffer_gauges();
        self.counters
            .set_inflight_objects(self.in_flight_objects as u64);
    }

    fn update_buffer_gauges(&self) {
        self.counters.set_buffered_bytes(self.buffered_bytes as u64);
        self.counters
            .set_open_objects(self.epoch.buffers.len() as u64);
        self.counters.set_ready_objects(self.ready.len() as u64);
    }

    fn reset_gauges(&self) {
        self.counters.set_buffered_bytes(0);
        self.counters.set_open_objects(0);
        self.counters.set_ready_objects(0);
        self.counters.set_inflight_objects(0);
    }

    fn pending_upload_objects(&self) -> usize {
        self.ready.len().saturating_add(self.in_flight_objects)
    }

    async fn run_actor(mut self, mut io: SinkIo) -> anyhow::Result<()> {
        let max_in_flight_objects = self.config.max_in_flight_objects;
        let mut uploads = JoinSet::new();
        let upload_cancellation = CancellationToken::new();
        let mut input_closed = false;
        let mut pending_delivery: Option<PendingDelivery> = None;
        let mut groups_since_yield = 0_usize;
        let mut backpressure_started: Option<std::time::Instant> = None;
        let result: anyhow::Result<()> = async {
            loop {
                self.emit_committed(&io.events).await?;
                if self.close_completed_epoch_journal().await? {
                    continue;
                }
                if input_closed && pending_delivery.is_none() && !self.epoch.buffers.is_empty() {
                    self.close_epoch().await?;
                    continue;
                }
                while self.in_flight_objects < max_in_flight_objects
                    && self.start_upload(&mut uploads, &upload_cancellation)
                {}
                self.update_gauges();
                if input_closed
                    && pending_delivery.is_none()
                    && uploads.is_empty()
                    && self.ready.is_empty()
                {
                    self.emit_committed(&io.events).await?;
                    if !self.progress.is_empty() || !self.closed_epochs.is_empty() {
                        return Err(PipelineFailure::fatal(anyhow::anyhow!(
                            "S3 sink stopped with incomplete delivery progress"
                        ))
                        .into());
                    }
                    return Ok(());
                }

                let below_live_limits = self.buffered_bytes
                    < self.config.buffering.max_buffered_bytes.0
                    && self.pending_upload_objects()
                        < self.config.buffering.max_pending_upload_objects;
                if self.memory_barrier_epoch.is_some_and(|epoch_id| {
                    !io.memory.is_transform_pressured()
                        || !self.closed_epochs.contains_key(&epoch_id)
                }) {
                    self.memory_barrier_epoch = None;
                }
                let memory_admissible = self.memory_barrier_epoch.is_none();
                let can_resume_pending =
                    below_live_limits && memory_admissible && pending_delivery.is_some();
                if can_resume_pending {
                    let Some(pending) = pending_delivery.as_mut() else {
                        return Err(PipelineFailure::fatal(anyhow::anyhow!(
                            "S3 resumable delivery state disappeared"
                        ))
                        .into());
                    };
                    let done = self.accept_next_message_group(pending, &io.memory).await?;
                    if done {
                        pending_delivery = None;
                    }
                    groups_since_yield = groups_since_yield.saturating_add(1);
                    if groups_since_yield >= MAX_GROUPS_BEFORE_YIELD {
                        groups_since_yield = 0;
                        tokio::task::yield_now().await;
                    }
                    continue;
                }
                groups_since_yield = 0;

                let can_accept = pending_delivery.is_none()
                    && !input_closed
                    && memory_admissible
                    && self.buffered_bytes < self.config.buffering.max_buffered_bytes.0
                    && self.pending_upload_objects()
                        < self.config.buffering.max_pending_upload_objects;
                if !input_closed && !can_accept {
                    backpressure_started.get_or_insert_with(std::time::Instant::now);
                } else if let Some(started) = backpressure_started.take() {
                    self.counters.add_backpressure(started.elapsed());
                }
                let deadline = pending_delivery
                    .is_none()
                    .then(|| self.wall_clock_deadline())
                    .flatten();
                let wall_sleep = async move {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::pin!(wall_sleep);
                tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    result = uploads.join_next(), if !uploads.is_empty() => {
                        let completed = result
                            .ok_or_else(|| anyhow::anyhow!("S3 upload set ended unexpectedly"))?
                            .map_err(|error| anyhow::anyhow!("S3 upload task failed: {error}"))?;
                        self.complete_upload(completed)?;
                    }
                    delivery = io.deliveries.recv(), if can_accept => {
                        match delivery {
                            Some(delivery) => {
                                let prepared = self
                                    .prepare_delivery(delivery, &io.memory)
                                    .map_err(PipelineFailure::fatal)?;
                                if prepared.rows.is_empty() {
                                    drop(prepared);
                                } else {
                                    pending_delivery = Some(prepared);
                                }
                            }
                            None => input_closed = true,
                        }
                    }
                    () = &mut wall_sleep => self.close_epoch().await?,
                }
            }
        }
        .await;

        if let Some(started) = backpressure_started.take() {
            self.counters.add_backpressure(started.elapsed());
        }
        cancel_and_drain_uploads(&mut uploads, &upload_cancellation).await;
        self.in_flight_objects = 0;
        self.reset_gauges();
        result
    }
}

async fn cancel_and_drain_uploads(
    uploads: &mut JoinSet<ActiveUpload>,
    cancellation: &CancellationToken,
) {
    cancellation.cancel();
    let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while uploads.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!("timed out aborting S3 multipart uploads; cancelling upload tasks");
        uploads.abort_all();
        while uploads.join_next().await.is_some() {}
    }
}

impl Sink for S3Sink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_actor(io).await })
    }
}
