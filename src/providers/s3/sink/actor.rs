use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Sleep};

use crate::metrics::SinkCounters;
use crate::pipeline::memory::MemoryReservation;
use crate::pipeline::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
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
    key: BufferKey,
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
    rows: usize,
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

struct DeliveryProgress {
    pending_rows: usize,
    source_messages: u64,
    serialized_memory: Option<MemoryReservation>,
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
    result: anyhow::Result<UploadStats>,
}

pub struct S3Sink {
    config: S3SinkConfig,
    uploader: Arc<dyn ObjectUploader>,
    partitioner: Partitioner,
    counters: Arc<SinkCounters>,
    keep_system_columns: bool,
    epoch: Epoch,
    ready: VecDeque<ClosedObject>,
    progress: BTreeMap<DeliveryId, DeliveryProgress>,
    next_received: DeliveryId,
    next_ack: DeliveryId,
    buffered_bytes: usize,
    epoch_byte_limit: usize,
    highest_time_slot_ms: Option<i64>,
}

impl S3Sink {
    pub fn new(
        config: S3SinkConfig,
        uploader: Arc<dyn ObjectUploader>,
        counters: Arc<SinkCounters>,
        keep_system_columns: bool,
    ) -> anyhow::Result<Self> {
        let partitioner = Partitioner::new(&config.partitioning)?;
        let epoch_byte_limit = config.buffering.max_buffered_bytes.0;
        Ok(Self {
            config,
            uploader,
            partitioner,
            counters,
            keep_system_columns,
            epoch: Epoch::default(),
            ready: VecDeque::new(),
            progress: BTreeMap::new(),
            next_received: DeliveryId::new(1),
            next_ack: DeliveryId::new(1),
            buffered_bytes: 0,
            epoch_byte_limit,
            highest_time_slot_ms: None,
        })
    }

    fn encode(&mut self, delivery: Delivery) -> anyhow::Result<EncodedDelivery> {
        let mut rows = Vec::new();
        let mut payload = Vec::new();
        for output in &delivery.outputs {
            let hidden: Vec<usize> = if self.keep_system_columns {
                Vec::new()
            } else {
                output
                    .system_columns
                    .iter()
                    .map(|column| column.index)
                    .collect()
            };
            let encoder = JsonBatchEncoder::new(&output.batch, |index| !hidden.contains(&index))?;
            for row in 0..output.rows() {
                let route = self.partitioner.route(output, row)?;
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
        anyhow::ensure!(
            delivery.id == self.next_received,
            "sink delivery order violation: expected {}, got {}",
            self.next_received.get(),
            delivery.id.get()
        );
        self.next_received = self.next_received.next();
        let encoded = self.encode(delivery)?;
        let serialized_bytes = encoded.payload.len();
        let delivery_id = encoded.delivery.id;
        let pending_rows = encoded.rows.len();
        let reservation =
            (serialized_bytes > 0).then(|| memory.reserve_transform(serialized_bytes));
        let copy_reservation =
            (serialized_bytes > 0).then(|| memory.reserve_transform(serialized_bytes));
        self.progress.insert(
            delivery_id,
            DeliveryProgress {
                pending_rows,
                source_messages: encoded.delivery.meta.source_messages,
                serialized_memory: reservation,
            },
        );
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
            self.accept_message_group(&encoded.rows[start..end], &encoded.payload)?;
            start = end;
        }
        drop(encoded.payload);
        drop(copy_reservation);
        self.update_buffer_gauges();
        Ok(())
    }

    fn accept_message_group(&mut self, rows: &[EncodedRow], payload: &[u8]) -> anyhow::Result<()> {
        let main = rows.iter().find(|row| !row.is_dlq);
        let record_time_ms = rows.iter().find_map(|row| row.route.record_time_ms);
        if let Some(main) = main {
            if let Some(slot) = main.route.time_slot_ms {
                if self
                    .highest_time_slot_ms
                    .is_some_and(|highest| slot < highest)
                {
                    anyhow::bail!("S3 time partition regression: event maps to already closed slot {slot}, highest observed slot is {}", self.highest_time_slot_ms.unwrap_or(slot));
                }
                self.highest_time_slot_ms = Some(
                    self.highest_time_slot_ms
                        .map_or(slot, |highest| highest.max(slot)),
                );
            }
        }

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
                .entry(key.clone())
                .or_insert_with(|| ObjectBuffer {
                    key,
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
            self.epoch.rows = self.epoch.rows.saturating_add(1);
            self.epoch.bytes = self.epoch.bytes.saturating_add(row.payload.len());
        }

        let object_limit_reached = self.epoch.buffers.values().any(|buffer| {
            buffer.rows >= self.config.rotation.max_rows
                || buffer.payload.len() >= self.config.rotation.max_bytes.0
        });
        let budget_reached = self.epoch.buffers.len() >= self.config.buffering.max_open_objects
            || self.epoch.bytes >= self.epoch_byte_limit;
        if self.epoch.buffers.len() > self.config.buffering.max_open_objects {
            tracing::warn!(
                open_objects = self.epoch.buffers.len(),
                configured_limit = self.config.buffering.max_open_objects,
                "one atomic source message temporarily exceeded the S3 open-object limit"
            );
        }
        if object_limit_reached || budget_reached {
            self.close_epoch();
        }
        Ok(())
    }

    fn close_epoch(&mut self) {
        if self.epoch.buffers.is_empty() {
            return;
        }
        let epoch = std::mem::take(&mut self.epoch);
        for (_, buffer) in epoch.buffers {
            let topic = percent_encode(buffer.topic.as_bytes());
            let filename = format!(
                "{topic}+{}+{}.json",
                buffer.source_partition, buffer.start_offset
            );
            let prefix = self.config.prefix.trim_matches('/');
            let key = if prefix.is_empty() {
                format!(
                    "{}/{}/{}",
                    buffer.key.table, buffer.key.partition_path, filename
                )
            } else {
                format!(
                    "{prefix}/{}/{}/{}",
                    buffer.key.table, buffer.key.partition_path, filename
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

    fn start_upload(&mut self) -> Option<JoinHandle<ActiveUpload>> {
        let object = self.ready.pop_front()?;
        let uploader = Arc::clone(&self.uploader);
        let retry = self.config.retry.clone();
        let key = object.key.clone();
        let payload = object.payload.clone();
        self.update_gauges(true);
        Some(tokio::spawn(async move {
            let result = upload_with_retry(uploader, retry, key, payload).await;
            ActiveUpload { object, result }
        }))
    }

    fn complete_upload(&mut self, active: ActiveUpload) -> anyhow::Result<()> {
        let stats = active.result?;
        self.counters.add_busy(stats.busy);
        self.counters.add_upload_retries(stats.retries);
        self.counters.add_rows(active.object.rows as u64);
        self.counters.add_bytes(active.object.payload.len() as u64);
        self.counters.add_flush();
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_sub(active.object.payload.len());
        for (delivery_id, rows) in active.object.delivery_rows {
            let progress = self.progress.get_mut(&delivery_id).ok_or_else(|| {
                anyhow::anyhow!("missing delivery progress {}", delivery_id.get())
            })?;
            progress.pending_rows = progress
                .pending_rows
                .checked_sub(rows)
                .ok_or_else(|| anyhow::anyhow!("delivery row accounting underflow"))?;
            if progress.pending_rows == 0 {
                progress.serialized_memory = None;
            }
        }
        tracing::info!(
            object_key = active.object.key,
            rows = active.object.rows,
            bytes = active.object.payload.len(),
            "S3 object upload completed"
        );
        self.update_gauges(false);
        Ok(())
    }

    async fn emit_committed(
        &mut self,
        events: &tokio::sync::mpsc::Sender<SinkEvent>,
    ) -> anyhow::Result<()> {
        let mut committed = None;
        let mut source_messages = 0_u64;
        while self
            .progress
            .get(&self.next_ack)
            .is_some_and(|progress| progress.pending_rows == 0)
        {
            let progress = self
                .progress
                .remove(&self.next_ack)
                .ok_or_else(|| anyhow::anyhow!("missing completed delivery"))?;
            source_messages = source_messages.saturating_add(progress.source_messages);
            committed = Some(self.next_ack);
            self.next_ack = self.next_ack.next();
        }
        if let Some(id) = committed {
            self.counters.add_unique_offsets(source_messages);
            events
                .send(SinkEvent::CommittedThrough(id))
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

    fn update_gauges(&self, upload_active: bool) {
        self.update_buffer_gauges();
        self.counters.set_inflight_objects(u64::from(upload_active));
    }

    fn update_buffer_gauges(&self) {
        self.counters.set_buffered_bytes(self.buffered_bytes as u64);
        self.counters
            .set_open_objects(self.epoch.buffers.len() as u64);
        self.counters.set_ready_objects(self.ready.len() as u64);
    }

    async fn run_actor(mut self, mut io: SinkIo) -> anyhow::Result<()> {
        // This limit is configuration-derived and therefore stable across
        // retries/restarts. Runtime memory pressure may throttle reception, but
        // must never change object boundaries in an exactly-once pipeline.
        self.epoch_byte_limit = self
            .config
            .buffering
            .max_buffered_bytes
            .0
            .min((io.memory.limit() / 2).max(1));
        let mut active: Option<JoinHandle<ActiveUpload>> = None;
        let mut input_closed = false;
        let mut backpressure_started: Option<std::time::Instant> = None;
        loop {
            self.emit_committed(&io.events).await?;
            if active.is_none() {
                active = self.start_upload();
            }
            if input_closed && !self.epoch.buffers.is_empty() {
                self.close_epoch();
                continue;
            }
            if input_closed && active.is_none() && self.ready.is_empty() {
                self.emit_committed(&io.events).await?;
                anyhow::ensure!(
                    self.progress.is_empty(),
                    "S3 sink stopped with incomplete deliveries"
                );
                return Ok(());
            }

            let can_accept =
                !input_closed && self.buffered_bytes < self.config.buffering.max_buffered_bytes.0;
            if !input_closed && !can_accept {
                backpressure_started.get_or_insert_with(std::time::Instant::now);
            } else if let Some(started) = backpressure_started.take() {
                self.counters.add_backpressure(started.elapsed());
            }
            let deadline = self.wall_clock_deadline();
            let mut wall_sleep = wall_clock_sleep(deadline);
            if let Some(mut task) = active.take() {
                tokio::select! {
                    () = io.cancellation.cancelled() => { task.abort(); return Ok(()); }
                    result = &mut task => {
                        let completed = result.map_err(|error| anyhow::anyhow!("S3 upload task failed: {error}"))?;
                        self.complete_upload(completed)?;
                    }
                    delivery = io.deliveries.recv(), if can_accept => {
                        match delivery {
                            Some(delivery) => self.accept(delivery, &io.memory)?,
                            None => input_closed = true,
                        }
                        active = Some(task);
                    }
                    () = &mut wall_sleep, if deadline.is_some() => {
                        self.close_epoch();
                        active = Some(task);
                    }
                }
            } else {
                tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
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
