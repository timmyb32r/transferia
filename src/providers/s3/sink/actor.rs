use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::task::JoinSet;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::CancellationToken;

use crate::metrics::SinkCounters;
use crate::pipeline::delivery_tracker::DeliveryTracker;
use crate::pipeline::memory::MemoryReservation;
use crate::pipeline::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
use crate::pipeline::PipelineFailure;
use crate::serializer::JsonBatchEncoder;

use super::config::{PartitionChange, S3SinkConfig};
use super::partitioning::{percent_encode, Partitioner, RowRoute};
use super::upload::{upload_with_retry, ObjectUploader, UploadStats};

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
    delivery_rows: BTreeMap<DeliveryId, usize>,
}

#[derive(Default)]
struct Epoch {
    buffers: BTreeMap<BufferKey, ObjectBuffer>,
    bytes: usize,
    record_time_base_ms: Option<i64>,
    last_main_partition: Option<Arc<str>>,
    first_seen: Option<Instant>,
}

struct ClosedObject {
    key: String,
    payload: Bytes,
    rows: usize,
    delivery_rows: BTreeMap<DeliveryId, usize>,
}

struct EncodedRow {
    table: Arc<str>,
    is_dlq: bool,
    route: RowRoute,
    payload: std::ops::Range<usize>,
    delivery_id: DeliveryId,
}

struct EncodedDelivery {
    delivery: Delivery,
    rows: Vec<EncodedRow>,
    payload: Vec<u8>,
}

struct ActiveUpload {
    object: ClosedObject,
    result: Result<UploadStats, PipelineFailure>,
}

pub struct S3Sink {
    config: S3SinkConfig,
    uploader: Arc<dyn ObjectUploader>,
    partitioner: Partitioner,
    counters: Arc<SinkCounters>,
    keep_system_columns: bool,
    epoch: Epoch,
    ready: VecDeque<ClosedObject>,
    progress: DeliveryTracker<MemoryReservation>,
    buffered_bytes: usize,
    epoch_byte_limit: usize,
    in_flight_objects: usize,
}

impl S3Sink {
    pub fn new(
        config: S3SinkConfig,
        uploader: Arc<dyn ObjectUploader>,
        counters: Arc<SinkCounters>,
        keep_system_columns: bool,
    ) -> anyhow::Result<Self> {
        let partitioner = Partitioner::new(&config.partitioning)?;
        let epoch_byte_limit = config.epoch_byte_limit();
        Ok(Self {
            config,
            uploader,
            partitioner,
            counters,
            keep_system_columns,
            epoch: Epoch::default(),
            ready: VecDeque::new(),
            progress: DeliveryTracker::new(),
            buffered_bytes: 0,
            epoch_byte_limit,
            in_flight_objects: 0,
        })
    }

    fn encode(&mut self, delivery: Delivery) -> anyhow::Result<EncodedDelivery> {
        let mut rows = Vec::new();
        let mut payload = Vec::new();
        for output in &delivery.outputs {
            let mut visible = vec![true; output.batch.num_columns()];
            if !self.keep_system_columns {
                for column in output.system_columns.iter() {
                    visible[column.index] = false;
                }
            }
            let encoder = JsonBatchEncoder::new(&output.batch, |index| visible[index])?;
            let routes = self.partitioner.route_batch(output)?;
            for (row, route) in routes.into_iter().enumerate() {
                let start = payload.len();
                encoder.write_row(row, &mut payload);
                rows.push(EncodedRow {
                    table: Arc::clone(&output.table),
                    is_dlq: output.is_dlq,
                    route,
                    payload: start..payload.len(),
                    delivery_id: delivery.id,
                });
            }
        }
        rows.sort_by(|left, right| {
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
        });
        Ok(EncodedDelivery {
            delivery,
            rows,
            payload,
        })
    }

    fn accept(
        &mut self,
        delivery: Delivery,
        memory: &crate::pipeline::memory::PipelineMemory,
    ) -> anyhow::Result<()> {
        let encoded = self.encode(delivery)?;
        let serialized_bytes = encoded.payload.len();
        let delivery_id = encoded.delivery.id;
        let pending_rows = encoded.rows.len();
        let reservation =
            (serialized_bytes > 0).then(|| memory.reserve_transform(serialized_bytes));
        let copy_reservation =
            (serialized_bytes > 0).then(|| memory.reserve_transform(serialized_bytes));
        self.progress.accept(
            delivery_id,
            pending_rows,
            encoded.delivery.meta.source_messages,
            reservation,
        )?;
        drop(encoded.delivery.outputs);
        self.buffered_bytes = self.buffered_bytes.saturating_add(serialized_bytes);
        if self.buffered_bytes > self.config.buffering.max_buffered_bytes.0 {
            tracing::warn!(
                buffered_bytes = self.buffered_bytes,
                configured_limit_bytes = self.config.buffering.max_buffered_bytes.0,
                "one source delivery temporarily exceeded the S3 buffering limit"
            );
        }

        let mut start = 0;
        while start < encoded.rows.len() {
            let identity = (
                &encoded.rows[start].route.topic,
                encoded.rows[start].route.partition,
                encoded.rows[start].route.offset,
            );
            let mut end = start + 1;
            while end < encoded.rows.len()
                && (
                    &encoded.rows[end].route.topic,
                    encoded.rows[end].route.partition,
                    encoded.rows[end].route.offset,
                ) == identity
            {
                end += 1;
            }
            self.accept_message_group(&encoded.rows[start..end], &encoded.payload);
            start = end;
        }
        drop(encoded.payload);
        drop(copy_reservation);
        self.update_buffer_gauges();
        Ok(())
    }

    fn accept_message_group(&mut self, rows: &[EncodedRow], payload: &[u8]) {
        let main = rows.iter().find(|row| !row.is_dlq);
        let record_time_ms = rows.iter().find_map(|row| row.route.record_time_ms);

        let partition_changed = self.config.rotation.on_partition_change == PartitionChange::Rotate
            && main.is_some_and(|row| {
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
            self.close_epoch();
        }
        if self.epoch.first_seen.is_none() {
            self.epoch.first_seen = Some(Instant::now());
            self.epoch.record_time_base_ms = record_time_ms;
        }
        if let Some(main) = main {
            self.epoch.last_main_partition = Some(Arc::clone(&main.route.partition_path));
        }

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
                    delivery_rows: BTreeMap::new(),
                });
            buffer.start_offset = buffer.start_offset.min(row.route.offset);
            buffer.rows = buffer.rows.saturating_add(1);
            buffer
                .payload
                .extend_from_slice(&payload[row.payload.clone()]);
            *buffer.delivery_rows.entry(row.delivery_id).or_default() += 1;
            self.epoch.bytes = self.epoch.bytes.saturating_add(row.payload.len());
        }

        let object_limit_reached = self.epoch.buffers.values().any(|buffer| {
            buffer.rows >= self.config.rotation.max_rows
                || buffer.payload.len() >= self.config.rotation.max_bytes.0
        });
        let budget_reached = self.epoch.buffers.len() > self.config.buffering.max_open_objects
            || self.epoch.bytes >= self.epoch_byte_limit;
        if self.epoch.buffers.len() > self.config.buffering.max_open_objects {
            tracing::warn!(
                open_objects = self.epoch.buffers.len(),
                configured_limit = self.config.buffering.max_open_objects,
                "one atomic source message temporarily exceeded the S3 open-object limit"
            );
        }
        if self.pending_objects() > self.config.buffering.max_pending_objects {
            tracing::warn!(
                pending_objects = self.pending_objects(),
                configured_limit = self.config.buffering.max_pending_objects,
                "one atomic source message temporarily exceeded the S3 pending-object limit"
            );
        }
        // Pending uploads are deliberately not a rotation input: their count
        // depends on I/O timing and would produce different object boundaries
        // when the same source deliveries are replayed. The pending limit is
        // enforced only by run-loop admission below.
        if object_limit_reached || budget_reached {
            self.close_epoch();
        }
    }

    fn close_epoch(&mut self) {
        if self.epoch.buffers.is_empty() {
            return;
        }
        let epoch = std::mem::take(&mut self.epoch);
        for (buffer_key, buffer) in epoch.buffers {
            let topic = percent_encode(buffer.topic.as_bytes());
            let filename = format!(
                "{topic}+{}+{}.json",
                buffer.source_partition, buffer.start_offset
            );
            let prefix = self.config.prefix.trim_matches('/');
            let key = if prefix.is_empty() {
                format!(
                    "{}/{}/{}",
                    buffer_key.table, buffer_key.partition_path, filename
                )
            } else {
                format!(
                    "{prefix}/{}/{}/{}",
                    buffer_key.table, buffer_key.partition_path, filename
                )
            };
            self.ready.push_back(ClosedObject {
                key,
                payload: Bytes::from(buffer.payload),
                rows: buffer.rows,
                delivery_rows: buffer.delivery_rows,
            });
        }
        self.update_buffer_gauges();
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
        self.in_flight_objects = self.in_flight_objects.saturating_add(1);
        uploads.spawn(async move {
            let result =
                upload_with_retry(uploader, retry, &object.key, &object.payload, &cancellation)
                    .await;
            ActiveUpload { object, result }
        });
        true
    }

    fn complete_upload(&mut self, active: ActiveUpload) -> anyhow::Result<()> {
        self.in_flight_objects = self.in_flight_objects.saturating_sub(1);
        let stats = active.result.map_err(anyhow::Error::new)?;
        self.counters.add_busy(stats.busy);
        self.counters.add_upload_retries(stats.retries);
        self.counters.add_rows(active.object.rows as u64);
        self.counters.add_bytes(active.object.payload.len() as u64);
        self.counters.add_flush();
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_sub(active.object.payload.len());
        for (delivery_id, rows) in active.object.delivery_rows {
            self.progress.complete(delivery_id, rows)?;
        }
        tracing::info!(
            object_key = active.object.key,
            rows = active.object.rows,
            bytes = active.object.payload.len(),
            "S3 object upload completed"
        );
        Ok(())
    }

    async fn emit_committed(
        &mut self,
        events: &tokio::sync::mpsc::Sender<SinkEvent>,
    ) -> anyhow::Result<()> {
        if let Some(committed) = self.progress.take_committed() {
            self.counters.add_unique_offsets(committed.source_messages);
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

    fn pending_objects(&self) -> usize {
        self.ready.len().saturating_add(self.in_flight_objects)
    }

    async fn run_actor(mut self, mut io: SinkIo) -> anyhow::Result<()> {
        let max_in_flight_objects = self.config.upload.max_in_flight_objects;
        let mut uploads = JoinSet::new();
        let upload_cancellation = CancellationToken::new();
        let mut input_closed = false;
        let mut backpressure_started: Option<std::time::Instant> = None;
        let result: anyhow::Result<()> = async {
            loop {
                self.emit_committed(&io.events).await?;
                if input_closed && !self.epoch.buffers.is_empty() {
                    self.close_epoch();
                    continue;
                }
                while self.in_flight_objects < max_in_flight_objects
                    && self.start_upload(&mut uploads, &upload_cancellation)
                {}
                self.update_gauges();
                if input_closed && uploads.is_empty() && self.ready.is_empty() {
                    self.emit_committed(&io.events).await?;
                    anyhow::ensure!(
                        self.progress.is_empty(),
                        "S3 sink stopped with incomplete deliveries"
                    );
                    return Ok(());
                }

                let can_accept = !input_closed
                    && self.buffered_bytes < self.config.buffering.max_buffered_bytes.0
                    && self.pending_objects() < self.config.buffering.max_pending_objects;
                if !input_closed && !can_accept {
                    backpressure_started.get_or_insert_with(std::time::Instant::now);
                } else if let Some(started) = backpressure_started.take() {
                    self.counters.add_backpressure(started.elapsed());
                }
                let deadline = self.wall_clock_deadline();
                let mut wall_sleep = wall_clock_sleep(deadline);
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
                            Some(delivery) => self.accept(delivery, &io.memory)?,
                            None => input_closed = true,
                        }
                    }
                    () = &mut wall_sleep, if deadline.is_some() => self.close_epoch(),
                }
            }
        }
        .await;

        cancel_and_drain_uploads(&mut uploads, &upload_cancellation).await;
        self.in_flight_objects = 0;
        self.update_gauges();
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

fn wall_clock_sleep(deadline: Option<Instant>) -> std::pin::Pin<Box<Sleep>> {
    Box::pin(tokio::time::sleep_until(deadline.unwrap_or_else(|| {
        Instant::now() + std::time::Duration::from_hours(24)
    })))
}

impl Sink for S3Sink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_actor(io).await })
    }
}
