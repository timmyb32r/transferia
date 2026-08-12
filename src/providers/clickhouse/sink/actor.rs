use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio::time::{Duration, Instant};
use tokio_util::task::AbortOnDropHandle;

use super::transport::{InsertError, InsertTransport};
use super::ClickHouseSinkConfig;
use crate::metrics::SinkCounters;
use crate::pipeline::delivery_tracker::DeliveryTracker;
use crate::pipeline::retry::{jittered_retry_delay, stable_retry_seed};
use crate::pipeline::sink::{Delivery, DeliveryId, Sink, SinkBatch, SinkEvent, SinkIo};
use crate::pipeline::PipelineFailure;
use crate::types::system_columns::SystemColumns;

struct BufferedBatch {
    delivery_id: DeliveryId,
    batch: SinkBatch,
}

struct TableBuffer {
    table: Arc<str>,
    first_seen: Instant,
    rows: usize,
    bytes: usize,
    batches: Vec<BufferedBatch>,
}

struct ActiveInsert {
    table: Arc<str>,
    rows: usize,
    bytes: usize,
    batches: Vec<BufferedBatch>,
}

pub struct ClickHouseSink {
    transport: Arc<dyn InsertTransport>,
    config: ClickHouseSinkConfig,
    counters: Arc<SinkCounters>,
    buffers: HashMap<Arc<str>, TableBuffer>,
    progress: DeliveryTracker,
    partition_retry_seed: u64,
    keep_system_columns: bool,
}

impl ClickHouseSink {
    #[must_use]
    pub fn with_transport(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
    ) -> Self {
        Self::with_transport_for_partition(config, counters, transport, 0)
    }

    #[must_use]
    pub fn with_transport_for_partition(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
        partition_id: i64,
    ) -> Self {
        Self::with_transport_for_partition_and_visibility(
            config,
            counters,
            transport,
            partition_id,
            false,
        )
    }

    pub(super) fn with_transport_for_partition_and_visibility(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
        partition_id: i64,
        keep_system_columns: bool,
    ) -> Self {
        Self {
            transport,
            config,
            counters,
            buffers: HashMap::new(),
            progress: DeliveryTracker::new(),
            partition_retry_seed: stable_retry_seed(&partition_id.to_le_bytes()),
            keep_system_columns,
        }
    }

    fn accept(&mut self, delivery: Delivery) -> anyhow::Result<()> {
        let remaining_outputs = delivery
            .outputs
            .iter()
            .filter(|output| output.batch.num_rows() > 0)
            .count();
        self.progress.accept(
            delivery.id,
            remaining_outputs,
            delivery.meta.source_messages,
        )?;
        for mut batch in delivery
            .outputs
            .into_iter()
            .filter(|output| output.batch.num_rows() > 0)
        {
            if !self.keep_system_columns && !batch.system_columns.is_empty() {
                batch.batch = without_system_columns(&batch.batch, &batch.system_columns)?;
                batch.byte_size = batch.batch.get_array_memory_size();
                batch.system_columns = SystemColumns::default();
            }
            let table = Arc::clone(&batch.table);
            let rows = batch.rows();
            let bytes = batch.bytes();
            let buffer = self
                .buffers
                .entry(Arc::clone(&table))
                .or_insert_with(|| TableBuffer {
                    table,
                    first_seen: Instant::now(),
                    rows: 0,
                    bytes: 0,
                    batches: Vec::new(),
                });
            buffer.rows = buffer.rows.saturating_add(rows);
            buffer.bytes = buffer.bytes.saturating_add(bytes);
            buffer.batches.push(BufferedBatch {
                delivery_id: delivery.id,
                batch,
            });
        }
        Ok(())
    }

    fn next_flush(&self, input_closed: bool, memory_pressure: bool) -> Option<(Arc<str>, Instant)> {
        let interval = Duration::from_millis(self.config.flush_interval_ms);
        self.buffers
            .values()
            .map(|buffer| {
                let full = memory_pressure
                    || buffer.rows >= self.config.max_insert_rows
                    || buffer.bytes >= self.config.max_insert_bytes;
                let wanted = if input_closed || full {
                    Instant::now()
                } else {
                    buffer.first_seen + interval
                };
                (Arc::clone(&buffer.table), wanted)
            })
            .min_by_key(|(_, deadline)| *deadline)
    }

    fn take_insert(&mut self, table: &Arc<str>) -> anyhow::Result<ActiveInsert> {
        let buffer = self
            .buffers
            .remove(table)
            .ok_or_else(|| anyhow::anyhow!("missing ClickHouse buffer for table '{table}'"))?;
        Ok(ActiveInsert {
            table: buffer.table,
            rows: buffer.rows,
            bytes: buffer.bytes,
            batches: buffer.batches,
        })
    }

    fn start_insert(
        &self,
        active: ActiveInsert,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> AbortOnDropHandle<Result<ActiveInsert, PipelineFailure>> {
        let transport = Arc::clone(&self.transport);
        let counters = Arc::clone(&self.counters);
        let config = self.config.clone();
        let retry_seed =
            self.partition_retry_seed.rotate_left(17) ^ stable_retry_seed(active.table.as_bytes());
        AbortOnDropHandle::new(tokio::spawn(async move {
            let mut attempts = 0_u32;
            let max_attempts = config.effective_retry_max_attempts();
            let mut backoff = Duration::from_millis(config.retry_initial_ms);
            loop {
                attempts = attempts.saturating_add(1);
                let batches = active
                    .batches
                    .iter()
                    .map(|buffered| buffered.batch.batch.clone())
                    .collect();
                let started = std::time::Instant::now();
                let result = tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(PipelineFailure::retryable(anyhow::anyhow!(
                            "ClickHouse insert cancelled"
                        )));
                    }
                    result = tokio::time::timeout(
                        config.request_timeout(),
                        transport.insert(Arc::clone(&active.table), batches),
                    ) => result.unwrap_or_else(|_| {
                        Err(InsertError::Transient(anyhow::anyhow!(
                            "ClickHouse INSERT timed out after {} ms; result is ambiguous",
                            config.request_timeout().as_millis(),
                        )))
                    }),
                };
                counters.add_busy(started.elapsed());
                match result {
                    Ok(()) => {
                        counters.add_rows(active.rows as u64);
                        counters.add_bytes(active.bytes as u64);
                        counters.add_flush();
                        return Ok(active);
                    }
                    Err(InsertError::Permanent(error)) => {
                        return Err(PipelineFailure::fatal(error));
                    }
                    Err(InsertError::Transient(error)) => {
                        if attempts >= max_attempts {
                            return Err(PipelineFailure::retryable(
                                error.context("ClickHouse retry limit exhausted"),
                            ));
                        }
                        let delay =
                            jittered_retry_delay(backoff, attempts.saturating_sub(1), retry_seed);
                        tracing::warn!(
                            attempts,
                            delay_ms = delay.as_millis() as u64,
                            "ClickHouse INSERT failed, retrying: {error}"
                        );
                        counters.add_retries(1);
                        tokio::select! {
                            () = cancellation.cancelled() => {
                                return Err(PipelineFailure::retryable(anyhow::anyhow!(
                                    "ClickHouse retry cancelled"
                                )));
                            }
                            () = tokio::time::sleep(delay) => {}
                        }
                        backoff = backoff
                            .saturating_mul(2)
                            .min(Duration::from_millis(config.retry_max_ms));
                    }
                }
            }
        }))
    }

    fn complete_insert(&mut self, active: ActiveInsert) -> anyhow::Result<()> {
        for buffered in active.batches {
            self.progress.complete(buffered.delivery_id, 1)?;
            drop(buffered);
        }
        tracing::info!(rows = active.rows, bytes = active.bytes, table = %active.table, "ClickHouse INSERT completed");
        Ok(())
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

    async fn run_actor(mut self, mut io: SinkIo) -> anyhow::Result<()> {
        let mut active: Option<AbortOnDropHandle<Result<ActiveInsert, PipelineFailure>>> = None;
        let mut input_closed = false;
        loop {
            self.emit_committed(&io.events).await?;

            if let Some(mut task) = active.take() {
                let mut completed = None;
                tokio::select! {
                    () = io.cancellation.cancelled() => {
                        task.abort();
                        let _ = (&mut task).await;
                        return Ok(());
                    }
                    result = &mut task => completed = Some(result),
                    delivery = io.deliveries.recv(), if !input_closed => {
                        match delivery {
                            Some(delivery) => self.accept(delivery)?,
                            None => input_closed = true,
                        }
                    }
                }
                if let Some(result) = completed {
                    match result.map_err(|error| {
                        anyhow::anyhow!("ClickHouse insert task failed: {error}")
                    })? {
                        Ok(insert) => self.complete_insert(insert)?,
                        Err(failure) => return Err(failure.into()),
                    }
                } else {
                    active = Some(task);
                }
                continue;
            }

            if input_closed && self.buffers.is_empty() {
                self.emit_committed(&io.events).await?;
                anyhow::ensure!(
                    self.progress.is_empty(),
                    "sink input closed with incomplete deliveries"
                );
                return Ok(());
            }

            let memory_pressure = io.memory.is_transform_pressured();
            let Some((table, deadline)) = self.next_flush(input_closed, memory_pressure) else {
                tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv(), if !input_closed => {
                        match delivery {
                            Some(delivery) => self.accept(delivery)?,
                            None => input_closed = true,
                        }
                    }
                }
                continue;
            };

            if deadline <= Instant::now() {
                let insert = self.take_insert(&table)?;
                active = Some(self.start_insert(insert, io.cancellation.clone()));
                continue;
            }

            tokio::select! {
                () = io.cancellation.cancelled() => return Ok(()),
                delivery = io.deliveries.recv(), if !input_closed => {
                    match delivery {
                        Some(delivery) => self.accept(delivery)?,
                        None => input_closed = true,
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    let insert = self.take_insert(&table)?;
                    active = Some(self.start_insert(insert, io.cancellation.clone()));
                }
            }
        }
    }
}

pub(super) fn without_system_columns(
    batch: &RecordBatch,
    system_columns: &crate::types::system_columns::SystemColumns,
) -> anyhow::Result<RecordBatch> {
    let mut visible = vec![true; batch.num_columns()];
    for column in system_columns.iter() {
        let field = batch
            .schema()
            .fields()
            .get(column.index)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "system column {:?} points outside a {}-column ClickHouse batch",
                    column.kind,
                    batch.num_columns()
                )
            })?;
        anyhow::ensure!(
            visible[column.index],
            "multiple system columns point to ClickHouse batch column {}",
            column.index
        );
        anyhow::ensure!(
            field.name() == column.kind.name() && field.data_type() == &column.kind.data_type(),
            "system column metadata {:?} does not match ClickHouse batch field '{}' ({:?})",
            column.kind,
            field.name(),
            field.data_type()
        );
        visible[column.index] = false;
    }
    let projection = visible
        .into_iter()
        .enumerate()
        .filter_map(|(index, visible)| visible.then_some(index))
        .collect::<Vec<_>>();
    Ok(batch.project(&projection)?)
}

impl Sink for ClickHouseSink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_actor(io).await })
    }
}
