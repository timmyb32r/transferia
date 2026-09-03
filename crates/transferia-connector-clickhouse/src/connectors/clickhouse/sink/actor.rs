use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt as _;
use tokio::time::{Duration, Instant};
use tokio_util::task::AbortOnDropHandle;

use super::transport::{InsertError, InsertTransport};
use super::ClickHouseSinkConfig;
use crate::metrics::SinkCounters;
use transferia_core::data::changelog::{project_sink_batch, ChangelogBatch, ProjectedSinkBatch};
use transferia_core::data::system_columns::SystemColumnKind;
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

    async fn accept(&mut self, delivery: Delivery) -> anyhow::Result<()> {
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
                    clickhouse_changelog_batches(
                        &changelog,
                        self.transport.as_ref(),
                        &self.config,
                        &table,
                    )
                    .await?
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
        let mut pending_changelog = None;
        let mut serial_changelog_active = false;
        loop {
            self.emit_committed(&io.events).await?;

            if serial_changelog_active && self.buffers.is_empty() && active.is_empty() {
                serial_changelog_active = false;
            }
            if pending_changelog.is_some() && self.buffers.is_empty() && active.is_empty() {
                let delivery = pending_changelog.take().ok_or_else(|| {
                    anyhow::anyhow!("pending ClickHouse changelog delivery disappeared")
                })?;
                self.accept(delivery).await?;
                serial_changelog_active = true;
            }

            while active.len() < self.config.insert_concurrency {
                let memory_pressure = io.memory.is_transform_pressured()
                    || pending_changelog.is_some()
                    || serial_changelog_active;
                let Some((table, deadline)) = self.next_flush(input_closed, memory_pressure) else {
                    break;
                };
                if deadline > Instant::now() {
                    break;
                }
                let insert = self.take_insert(&table)?;
                active.push(self.start_insert(insert, io.cancellation.clone()));
            }

            if input_closed
                && pending_changelog.is_none()
                && !serial_changelog_active
                && self.buffers.is_empty()
                && active.is_empty()
            {
                self.emit_committed(&io.events).await?;
                anyhow::ensure!(
                    self.progress.is_empty(),
                    "sink input closed with incomplete deliveries"
                );
                return Ok(());
            }

            let next_deadline = (active.len() < self.config.insert_concurrency)
                .then(|| {
                    self.next_flush(
                        input_closed,
                        io.memory.is_transform_pressured()
                            || pending_changelog.is_some()
                            || serial_changelog_active,
                    )
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
                delivery = io.deliveries.recv(), if !input_closed && pending_changelog.is_none() && !serial_changelog_active => {
                    match delivery {
                        Some(delivery) if delivery_is_changelog(&delivery) => {
                            pending_changelog = Some(delivery);
                        }
                        Some(delivery) => self.accept(delivery).await?,
                        None => input_closed = true,
                    }
                }
                () = tokio::time::sleep_until(next_deadline.unwrap_or_else(Instant::now)), if next_deadline.is_some() => {}
            }
        }
    }
}

fn delivery_is_changelog(delivery: &Delivery) -> bool {
    delivery.outputs.iter().any(|output| {
        output
            .system_columns
            .contains(SystemColumnKind::ChangeOperation)
    })
}

pub(super) async fn clickhouse_changelog_batches(
    changelog: &ChangelogBatch,
    transport: &dyn InsertTransport,
    config: &ClickHouseSinkConfig,
    table: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for run in changelog.collapsed_runs()? {
        let deleted = run.action == transferia_core::ChangelogAction::Delete;
        let base = if run.operation == transferia_core::ChangeOperation::Update
            && run.batch.num_columns() != changelog.stored_columns.len()
        {
            restore_clickhouse_update(transport, config, table, changelog, &run.batch).await?
        } else {
            run.batch
        };
        batches.push(clickhouse_change_batch(
            &base,
            &run.source_versions,
            deleted,
        )?);
    }
    Ok(batches)
}

async fn restore_clickhouse_update(
    transport: &dyn InsertTransport,
    config: &ClickHouseSinkConfig,
    table: &str,
    changelog: &ChangelogBatch,
    update: &RecordBatch,
) -> anyhow::Result<RecordBatch> {
    let query = current_rows_query(config, table, changelog, update)?;
    let batches = transport.query_all(query).await.map_err(|error| {
        anyhow::Error::new(match error {
            InsertError::Transient(error) => DataPlaneFailure::retryable(error),
            InsertError::Permanent(error) => DataPlaneFailure::fatal(error),
        })
    })?;
    anyhow::ensure!(
        !batches.is_empty(),
        "ClickHouse cannot restore unchanged TOAST columns: no current rows were returned"
    );
    let query_schema = batches[0].schema();
    anyhow::ensure!(
        batches.iter().all(|batch| batch.schema() == query_schema),
        "ClickHouse current-row query returned inconsistent Arrow schemas"
    );
    let fetched = concat_batches(&query_schema, &batches)?;
    anyhow::ensure!(
        fetched.num_columns() == changelog.stored_columns.len(),
        "ClickHouse current-row query returned {} columns, expected {}",
        fetched.num_columns(),
        changelog.stored_columns.len()
    );
    let full_fields = changelog
        .stored_columns
        .iter()
        .map(|column| {
            Arc::new(
                Field::new(&column.name, column.data_type.clone(), column.nullable)
                    .with_metadata(column.arrow_metadata()),
            )
        })
        .collect::<Vec<_>>();
    let fetched_arrays = fetched
        .columns()
        .iter()
        .zip(&changelog.stored_columns)
        .map(|(array, column)| {
            if array.data_type() == &column.data_type {
                Ok(Arc::clone(array))
            } else {
                cast(array, &column.data_type).map_err(Into::into)
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let fetched = RecordBatch::try_new(Arc::new(Schema::new(full_fields.clone())), fetched_arrays)?;

    let key_types = changelog
        .primary_key_columns
        .iter()
        .map(|column| SortField::new(column.data_type.clone()))
        .collect::<Vec<_>>();
    let fetched_keys = changelog
        .primary_key_indexes()
        .iter()
        .map(|index| Arc::clone(fetched.column(*index)))
        .collect::<Vec<_>>();
    let update_keys = changelog
        .primary_keys
        .iter()
        .map(|name| {
            update
                .schema()
                .index_of(name)
                .map(|index| Arc::clone(update.column(index)))
                .map_err(|_| {
                    anyhow::anyhow!("ClickHouse partial update omits primary key '{name}'")
                })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let converter = RowConverter::new(key_types)?;
    let fetched_keys = converter.convert_columns(&fetched_keys)?;
    let update_keys = converter.convert_columns(&update_keys)?;
    let mut fetched_by_key = HashMap::with_capacity(fetched.num_rows());
    for row in 0..fetched.num_rows() {
        anyhow::ensure!(
            fetched_by_key
                .insert(fetched_keys.row(row).as_ref().to_vec(), row)
                .is_none(),
            "ClickHouse current-row query returned duplicate primary keys after FINAL"
        );
    }

    let full_schema = Arc::new(Schema::new(full_fields));
    let mut rows = Vec::with_capacity(update.num_rows());
    for row in 0..update.num_rows() {
        let current_row = fetched_by_key
            .get(update_keys.row(row).as_ref())
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "ClickHouse cannot restore unchanged TOAST columns: primary key has no current destination row"
                )
            })?;
        let arrays = changelog
            .stored_columns
            .iter()
            .enumerate()
            .map(|(column_index, column)| {
                update.schema().index_of(&column.name).map_or_else(
                    |_| fetched.column(column_index).slice(current_row, 1),
                    |index| update.column(index).slice(row, 1),
                )
            })
            .collect();
        rows.push(RecordBatch::try_new(Arc::clone(&full_schema), arrays)?);
    }
    Ok(concat_batches(&full_schema, &rows)?)
}

fn current_rows_query(
    config: &ClickHouseSinkConfig,
    table: &str,
    changelog: &ChangelogBatch,
    update: &RecordBatch,
) -> anyhow::Result<String> {
    let columns = changelog
        .stored_columns
        .iter()
        .map(|column| super::client::quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = (0..update.num_rows())
        .map(|row| {
            changelog
                .primary_key_columns
                .iter()
                .map(|column| {
                    let index = update.schema().index_of(&column.name).map_err(|_| {
                        anyhow::anyhow!(
                            "ClickHouse partial update omits primary key '{}'",
                            column.name
                        )
                    })?;
                    key_predicate(
                        &column.name,
                        update.column(index).as_ref(),
                        row,
                        &column.data_type,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()
                .map(|parts| format!("({})", parts.join(" AND ")))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(" OR ");
    Ok(format!(
        "SELECT {columns} FROM {}.{} FINAL WHERE {} = 0 AND ({predicates})",
        super::client::quote_identifier(&config.database),
        super::client::quote_identifier(table),
        super::client::quote_identifier(super::table::CHANGE_DELETE_TIME),
    ))
}

pub(super) fn key_predicate(
    name: &str,
    array: &dyn Array,
    row: usize,
    data_type: &DataType,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        !array.is_null(row),
        "ClickHouse primary key '{name}' is null"
    );
    let name = super::client::quote_identifier(name);
    macro_rules! primitive {
        ($array:ty) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!("{name} = {}", values.value(row))
        }};
    }
    Ok(match data_type {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!(
                "{name} = {}",
                super::table::quote_string_literal(values.value(row))
            )
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!(
                "{name} = {}",
                super::table::quote_string_literal(values.value(row))
            )
        }
        DataType::Binary => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!("{name} = unhex('{}')", hex(values.value(row)))
        }
        DataType::LargeBinary => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::LargeBinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!("{name} = unhex('{}')", hex(values.value(row)))
        }
        DataType::Int8 => primitive!(arrow::array::Int8Array),
        DataType::Int16 => primitive!(arrow::array::Int16Array),
        DataType::Int32 => primitive!(arrow::array::Int32Array),
        DataType::Int64 => primitive!(arrow::array::Int64Array),
        DataType::UInt8 => primitive!(arrow::array::UInt8Array),
        DataType::UInt16 => primitive!(arrow::array::UInt16Array),
        DataType::UInt32 => primitive!(arrow::array::UInt32Array),
        DataType::UInt64 => primitive!(arrow::array::UInt64Array),
        DataType::Float32 => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!(
                "reinterpretAsUInt32({name}) = {}",
                values.value(row).to_bits()
            )
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!(
                "reinterpretAsUInt64({name}) = {}",
                values.value(row).to_bits()
            )
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!("{name} = {}", u8::from(values.value(row)))
        }
        DataType::Decimal128(precision, scale) => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            let value = decimal_literal(values.value(row), *scale)?;
            format!("{name} = CAST('{value}' AS Decimal({precision}, {scale}))")
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse primary-key Arrow type mismatch"))?;
            format!("toInt32({name}) = {}", values.value(row))
        }
        DataType::Timestamp(unit, _) => {
            let value = timestamp_value(array, row, *unit)?;
            let conversion = match unit {
                TimeUnit::Second => "toUnixTimestamp64Second",
                TimeUnit::Millisecond => "toUnixTimestamp64Milli",
                TimeUnit::Microsecond => "toUnixTimestamp64Micro",
                TimeUnit::Nanosecond => "toUnixTimestamp64Nano",
            };
            format!("{conversion}({name}) = {value}")
        }
        other => {
            anyhow::bail!("ClickHouse cannot restore TOAST values for primary-key type {other:?}")
        }
    })
}

fn timestamp_value(array: &dyn Array, row: usize, unit: TimeUnit) -> anyhow::Result<i64> {
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| {
                    anyhow::anyhow!("ClickHouse timestamp primary-key Arrow type mismatch")
                })?
                .value(row)
        };
    }
    Ok(match unit {
        TimeUnit::Second => value!(arrow::array::TimestampSecondArray),
        TimeUnit::Millisecond => value!(arrow::array::TimestampMillisecondArray),
        TimeUnit::Microsecond => value!(arrow::array::TimestampMicrosecondArray),
        TimeUnit::Nanosecond => value!(arrow::array::TimestampNanosecondArray),
    })
}

fn decimal_literal(value: i128, scale: i8) -> anyhow::Result<String> {
    anyhow::ensure!(
        scale >= 0,
        "ClickHouse Decimal primary-key scale must be nonnegative"
    );
    let scale = usize::from(scale.unsigned_abs());
    let negative = value.is_negative();
    let mut digits = value.unsigned_abs().to_string();
    if scale != 0 {
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
    }
    if negative {
        digits.insert(0, '-');
    }
    Ok(digits)
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    result
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
        versions
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
