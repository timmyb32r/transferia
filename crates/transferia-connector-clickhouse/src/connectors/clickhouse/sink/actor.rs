use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt as _;
use tokio::time::{Duration, Instant};
use tokio_util::task::AbortOnDropHandle;

use super::transport::{InsertError, InsertTransport};
use super::ClickHouseSinkConfig;
use crate::metrics::SinkCounters;
use transferia_core::data::changelog::{
    project_sink_batch, ChangelogBatch, ProjectedSinkBatch,
};
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::delivery_tracker::DeliveryTracker;
use transferia_delivery_contracts::retry::{jittered_retry_delay, stable_retry_seed};

struct BufferedBatch {
    delivery_id: DeliveryId,
    batch: RecordBatch,
    rows: usize,
    bytes: usize,
    _memory: Arc<transferia_core::memory::MemoryReservation>,
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
    discovery: Arc<DeliveryDiscovery>,
}

impl ClickHouseSink {
    #[must_use]
    pub fn with_transport(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
        discovery: Arc<DeliveryDiscovery>,
    ) -> Self {
        Self::with_transport_for_partition(config, counters, transport, 0, discovery)
    }

    #[must_use]
    pub fn with_transport_for_partition(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
        partition_id: i64,
        discovery: Arc<DeliveryDiscovery>,
    ) -> Self {
        Self::with_transport_for_partition_and_visibility(
            config,
            counters,
            transport,
            partition_id,
            false,
            discovery,
        )
    }

    pub(super) fn with_transport_for_partition_and_visibility(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
        partition_id: i64,
        _keep_system_columns: bool,
        discovery: Arc<DeliveryDiscovery>,
    ) -> Self {
        Self {
            transport,
            config,
            counters,
            buffers: HashMap::new(),
            progress: DeliveryTracker::new(),
            partition_retry_seed: stable_retry_seed(&partition_id.to_le_bytes()),
            discovery,
        }
    }

    fn accept(&mut self, delivery: Delivery) -> anyhow::Result<()> {
        // Defend the sink boundary even after successful startup discovery:
        // validate the complete delivery before mutating progress or buffers.
        for batch in &delivery.outputs {
            self.config
                .validate_batch(&self.discovery, batch)
                .map_err(DataPlaneFailure::fatal)?;
        }

        let mut prepared = Vec::new();
        for output in delivery.outputs {
            if output.batch.num_rows() == 0 {
                continue;
            }
            let table = Arc::clone(&output.table);
            let projected = project_sink_batch(&self.discovery, &output)?;
            let memory = Arc::new(output.memory);
            let batches = match projected {
                ProjectedSinkBatch::AppendOnly(batch) => vec![batch],
                ProjectedSinkBatch::Changelog(changelog) => {
                    clickhouse_changelog_batches(&changelog)?
                }
            };
            for batch in batches.into_iter().filter(|batch| batch.num_rows() > 0) {
                let rows = batch.num_rows();
                let bytes = batch.get_array_memory_size();
                prepared.push((Arc::clone(&table), batch, rows, bytes, Arc::clone(&memory)));
            }
        }
        let remaining_outputs = prepared.len();
        self.progress.accept(
            delivery.id,
            remaining_outputs,
            delivery.meta.source_messages,
        )?;
        for (table, batch, rows, bytes, memory) in prepared {
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
                rows,
                bytes,
                _memory: memory,
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
                    || buffer.rows >= self.config.insert_target_rows
                    || buffer.bytes >= self.config.insert_target_bytes;
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
            .get_mut(table)
            .ok_or_else(|| anyhow::anyhow!("missing ClickHouse buffer for table '{table}'"))?;
        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut batch_count = 0_usize;
        let first_schema = buffer.batches.first().map(|batch| batch.batch.schema());
        for buffered in &buffer.batches {
            if batch_count > 0
                && (rows >= self.config.insert_target_rows
                    || bytes >= self.config.insert_target_bytes
                    || first_schema
                        .as_ref()
                        .is_some_and(|schema| buffered.batch.schema() != *schema))
            {
                break;
            }
            rows = rows.saturating_add(buffered.rows);
            bytes = bytes.saturating_add(buffered.bytes);
            batch_count += 1;
        }
        let batches = buffer.batches.drain(..batch_count).collect();
        buffer.rows = buffer.rows.saturating_sub(rows);
        buffer.bytes = buffer.bytes.saturating_sub(bytes);
        let table_name = Arc::clone(&buffer.table);
        if buffer.batches.is_empty() {
            self.buffers.remove(table);
        }
        Ok(ActiveInsert {
            table: table_name,
            rows,
            bytes,
            batches,
        })
    }

    fn start_insert(
        &self,
        active: ActiveInsert,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> AbortOnDropHandle<Result<ActiveInsert, DataPlaneFailure>> {
        let transport = Arc::clone(&self.transport);
        let counters = Arc::clone(&self.counters);
        let config = self.config.clone();
        let first_delivery_id = active
            .batches
            .first()
            .map_or(0, |batch| batch.delivery_id.get());
        let retry_seed = self.partition_retry_seed.rotate_left(17)
            ^ stable_retry_seed(active.table.as_bytes())
            ^ stable_retry_seed(&first_delivery_id.to_le_bytes());
        AbortOnDropHandle::new(tokio::spawn(async move {
            let mut attempts = 0_u32;
            let max_attempts = config.effective_retry_max_attempts();
            let mut backoff = Duration::from_millis(config.retry_initial_ms);
            loop {
                attempts = attempts.saturating_add(1);
                let batches = active
                    .batches
                    .iter()
                    .map(|buffered| buffered.batch.clone())
                    .collect();
                let started = std::time::Instant::now();
                let result = tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
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
                        return Err(DataPlaneFailure::fatal(error));
                    }
                    Err(InsertError::Transient(error)) => {
                        if attempts >= max_attempts {
                            return Err(DataPlaneFailure::retryable(
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
                                return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
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
        tracing::debug!(rows = active.rows, bytes = active.bytes, table = %active.table, "ClickHouse INSERT completed");
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
        let mut active =
            FuturesUnordered::<AbortOnDropHandle<Result<ActiveInsert, DataPlaneFailure>>>::new();
        let mut input_closed = false;
        loop {
            self.emit_committed(&io.events).await?;

            while active.len() < self.config.insert_concurrency {
                let memory_pressure = io.memory.is_transform_pressured();
                let Some((table, deadline)) = self.next_flush(input_closed, memory_pressure) else {
                    break;
                };
                if deadline > Instant::now() {
                    break;
                }
                let insert = self.take_insert(&table)?;
                active.push(self.start_insert(insert, io.cancellation.clone()));
            }

            if input_closed && self.buffers.is_empty() && active.is_empty() {
                self.emit_committed(&io.events).await?;
                anyhow::ensure!(
                    self.progress.is_empty(),
                    "sink input closed with incomplete deliveries"
                );
                return Ok(());
            }

            let next_deadline = (active.len() < self.config.insert_concurrency)
                .then(|| {
                    self.next_flush(input_closed, io.memory.is_transform_pressured())
                        .map(|(_, deadline)| deadline)
                })
                .flatten();

            tokio::select! {
                () = io.cancellation.cancelled() => return Ok(()),
                result = active.next(), if !active.is_empty() => {
                    let result = result.ok_or_else(|| anyhow::anyhow!(
                        "ClickHouse active INSERT set ended unexpectedly"
                    ))?;
                    match result
                        .map_err(|error| anyhow::anyhow!("ClickHouse insert task failed: {error}"))?
                    {
                        Ok(insert) => self.complete_insert(insert)?,
                        Err(failure) => return Err(failure.into()),
                    }
                }
                delivery = io.deliveries.recv(), if !input_closed => {
                    match delivery {
                        Some(delivery) => self.accept(delivery)?,
                        None => input_closed = true,
                    }
                }
                () = tokio::time::sleep_until(next_deadline.unwrap_or_else(Instant::now)), if next_deadline.is_some() => {}
            }
        }
    }
}

pub(super) fn clickhouse_changelog_batches(
    changelog: &ChangelogBatch,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for run in changelog.collapsed_runs()? {
        let deleted = run.action == transferia_core::ChangelogAction::Delete;
        batches.push(clickhouse_change_batch(
            &run.batch,
            &run.source_versions,
            deleted,
        )?);
    }
    Ok(batches)
}

fn clickhouse_change_batch(
    base: &RecordBatch,
    versions: &[u64],
    deleted: bool,
) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        base.num_rows() == versions.len(),
        "ClickHouse changelog batch and source versions have different lengths"
    );
    // Zero is the explicit "not deleted" sentinel in delete_time. Shift the
    // nonnegative source position so offset zero remains representable.
    let versions = versions
        .iter()
        .map(|version| {
            version
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("ClickHouse changelog source version overflow"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut arrays = base.columns().to_vec();
    arrays.push(Arc::new(UInt64Array::from(versions.clone())) as ArrayRef);
    arrays.push(Arc::new(UInt64Array::from(if deleted {
        versions.to_vec()
    } else {
        vec![0; versions.len()]
    })) as ArrayRef);
    let mut fields = base.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        super::table::CHANGE_COMMIT_TIME,
        arrow::datatypes::DataType::UInt64,
        false,
    )));
    fields.push(Arc::new(Field::new(
        super::table::CHANGE_DELETE_TIME,
        arrow::datatypes::DataType::UInt64,
        false,
    )));
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

impl Sink for ClickHouseSink {
    fn run(
        self: Box<Self>,
        io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            self.run_actor(io)
                .await
                .map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}
