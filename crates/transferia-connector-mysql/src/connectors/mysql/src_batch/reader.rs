use std::mem::size_of;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    new_null_array, ArrayRef, BinaryArray, BinaryBuilder, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, StringBuilder,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use mysql_async::prelude::{Query, Queryable, WithParams};
use mysql_async::{BinaryProtocol, Conn, ResultSetStream, Row, TextProtocol, Value};
use transferia_connector_support::external_request::observe_external_request;

use super::config::{MySqlReadProtocol, TableConfig, MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES};
use super::connector::{
    old_value_column_name, ColumnPlan, MySqlColumnKind, MYSQL_REPLICATION_SYSTEM_COLUMNS,
    MYSQL_SOURCE_METADATA_COLUMNS,
};
use crate::connectors::mysql::common::{
    quote_identifier, validate_mysql_client_packet_limit,
};
use crate::connectors::mysql::src_batch_and_stream::MySqlBinlogBoundary;
use crate::connectors::mysql::src_stream::encode_snapshot_boundary_identity;
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};

type TextRowStream = ResultSetStream<'static, 'static, 'static, Row, TextProtocol>;
type BinaryRowStream = ResultSetStream<'static, 'static, 'static, Row, BinaryProtocol>;

enum MySqlRowStream {
    Text(TextRowStream),
    Binary(BinaryRowStream),
}

pub(super) type SnapshotRow = Vec<Option<Value>>;

pub(super) trait RowValueAccess {
    fn value_at(&self, index: usize) -> Option<&Value>;
}

impl RowValueAccess for Row {
    fn value_at(&self, index: usize) -> Option<&Value> {
        self.as_ref(index)
    }
}

impl RowValueAccess for SnapshotRow {
    fn value_at(&self, index: usize) -> Option<&Value> {
        self.get(index).and_then(Option::as_ref)
    }
}

impl MySqlRowStream {
    async fn next(&mut self) -> Option<mysql_async::Result<Row>> {
        match self {
            Self::Text(stream) => stream.next().await,
            Self::Binary(stream) => stream.next().await,
        }
    }
}

pub struct MySqlSource {
    table: TableConfig,
    schema: DatasetSchema,
    columns: Vec<ColumnPlan>,
    batch_rows: usize,
    batch_target_bytes: usize,
    max_row_bytes: usize,
    max_decoded_row_bytes: usize,
    stream: Option<MySqlRowStream>,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
    memory: PipelineMemory,
    snapshot_metadata: Option<MySqlSnapshotMetadata>,
}

#[derive(Clone)]
pub(super) struct MySqlSnapshotMetadata {
    pub partition_id: i64,
    pub database: String,
    pub table: String,
    pub boundary: MySqlBinlogBoundary,
}

impl MySqlSource {
    pub async fn new(
        mut connection: Conn,
        database: String,
        table: TableConfig,
        schema: DatasetSchema,
        columns: Vec<ColumnPlan>,
        batch_rows: usize,
        batch_target_bytes: usize,
        max_row_bytes: usize,
        read_protocol: MySqlReadProtocol,
        counters: Arc<SourceCounters>,
        memory: PipelineMemory,
    ) -> anyhow::Result<Self> {
        connection
            .query_drop("SET SESSION time_zone = '+00:00'")
            .await?;
        connection
            .query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        connection
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        Self::from_started_snapshot(
            connection,
            database,
            table,
            schema,
            columns,
            batch_rows,
            batch_target_bytes,
            max_row_bytes,
            read_protocol,
            counters,
            memory,
            None,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the exact snapshot reader receives its prepared session and immutable schema boundary explicitly"
    )]
    pub(super) async fn from_started_snapshot(
        connection: Conn,
        database: String,
        table: TableConfig,
        schema: DatasetSchema,
        columns: Vec<ColumnPlan>,
        batch_rows: usize,
        batch_target_bytes: usize,
        max_row_bytes: usize,
        read_protocol: MySqlReadProtocol,
        counters: Arc<SourceCounters>,
        memory: PipelineMemory,
        snapshot_metadata: Option<MySqlSnapshotMetadata>,
    ) -> anyhow::Result<Self> {
        validate_snapshot_memory_limits(batch_rows, batch_target_bytes, max_row_bytes)?;
        let max_decoded_row_bytes =
            max_decoded_row_admission_bytes(max_row_bytes, columns.len())?;
        let projection = columns
            .iter()
            .map(|column| column.expression.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT {projection} FROM {}.{}",
            quote_identifier(&database),
            quote_identifier(&table.name)
        );
        let stream = match read_protocol {
            MySqlReadProtocol::Text => MySqlRowStream::Text(
                observe_external_request(
                    "mysql",
                    "start_snapshot_row_stream",
                    query.stream::<Row, _>(connection),
                )
                .await?,
            ),
            MySqlReadProtocol::Binary => MySqlRowStream::Binary(
                observe_external_request(
                    "mysql",
                    "start_snapshot_row_stream",
                    query.with(()).stream::<Row, _>(connection),
                )
                .await?,
            ),
        };
        let actual_columns = match &stream {
            MySqlRowStream::Text(stream) => stream.columns_ref(),
            MySqlRowStream::Binary(stream) => stream.columns_ref(),
        };
        anyhow::ensure!(
            actual_columns.len() == columns.len()
                && actual_columns
                    .iter()
                    .zip(&columns)
                    .all(|(actual, expected)| actual.name_str() == expected.name),
            "MySQL query schema changed after discovery for table '{}.{}'",
            database,
            table.name
        );
        Ok(Self {
            table,
            schema,
            columns,
            batch_rows,
            batch_target_bytes,
            max_row_bytes,
            max_decoded_row_bytes,
            stream: Some(stream),
            offset: 0,
            finished: false,
            counters,
            memory,
            snapshot_metadata,
        })
    }
}

pub(super) fn validate_snapshot_memory_limits(
    batch_rows: usize,
    batch_target_bytes: usize,
    max_row_bytes: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(batch_rows > 0, "MySQL snapshot batch_rows must be positive");
    anyhow::ensure!(
        (1..=MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES).contains(&batch_target_bytes),
        "MySQL snapshot batch_target_bytes must be in 1..={MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES}"
    );
    validate_mysql_client_packet_limit(max_row_bytes)?;
    batch_target_bytes
        .checked_add(max_row_bytes)
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot memory admission overflow"))?;
    Ok(())
}

pub(super) fn max_decoded_row_admission_bytes(
    max_row_bytes: usize,
    column_count: usize,
) -> anyhow::Result<usize> {
    validate_mysql_client_packet_limit(max_row_bytes)?;
    max_row_bytes
        .checked_add(
            column_count
                .checked_mul(size_of::<Option<Value>>())
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL snapshot decoded row value-vector admission overflow")
                })?,
        )
        .and_then(|bytes| bytes.checked_add(size_of::<Row>()))
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot decoded row admission overflow"))
}

pub(super) fn retained_row_value_heap_bytes(row: &SnapshotRow) -> anyhow::Result<usize> {
    let mut bytes = row
        .capacity()
        .checked_mul(size_of::<Option<Value>>())
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot row value memory accounting overflow"))?;
    for value in row.iter().flatten() {
        if let Value::Bytes(value) = value {
            bytes = bytes.checked_add(value.capacity()).ok_or_else(|| {
                anyhow::anyhow!("MySQL snapshot row payload memory accounting overflow")
            })?;
        }
    }
    Ok(bytes)
}

pub(super) fn retained_rows_heap_bytes(
    row_capacity: usize,
    retained_value_bytes: usize,
) -> anyhow::Result<usize> {
    row_capacity
        .checked_mul(size_of::<SnapshotRow>())
        .and_then(|bytes| bytes.checked_add(retained_value_bytes))
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot batch memory accounting overflow"))
}

pub(super) fn next_snapshot_rows_capacity(
    row_len: usize,
    row_capacity: usize,
) -> anyhow::Result<usize> {
    if row_len < row_capacity {
        return Ok(row_capacity);
    }
    if row_capacity == 0 {
        return Ok(4);
    }
    row_capacity
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot row-vector capacity overflow"))
}

fn snapshot_rows_push_peak_bytes(
    row_len: usize,
    row_capacity: usize,
    retained_value_bytes: usize,
) -> anyhow::Result<usize> {
    let next_capacity = next_snapshot_rows_capacity(row_len, row_capacity)?;
    let overlapping_capacity = if next_capacity == row_capacity {
        row_capacity
    } else {
        row_capacity.checked_add(next_capacity).ok_or_else(|| {
            anyhow::anyhow!("MySQL snapshot row-vector reallocation accounting overflow")
        })?
    };
    retained_rows_heap_bytes(overlapping_capacity, retained_value_bytes)
}

pub(super) fn validate_snapshot_batch_growth(
    previous_bytes: usize,
    next_bytes: usize,
    batch_target_bytes: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        previous_bytes < batch_target_bytes,
        "MySQL snapshot read another row after reaching batch_target_bytes"
    );
    anyhow::ensure!(
        next_bytes > previous_bytes,
        "MySQL snapshot retained-row memory did not grow after reading a row"
    );
    Ok(())
}

pub(super) fn should_read_snapshot_row(
    rows: usize,
    retained_row_bytes: usize,
    batch_rows: usize,
    batch_target_bytes: usize,
) -> bool {
    rows < batch_rows && (rows == 0 || retained_row_bytes < batch_target_bytes)
}

fn checked_snapshot_product(left: usize, right: usize, what: &str) -> anyhow::Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot {what} memory accounting overflow"))
}

fn checked_snapshot_sum(left: usize, right: usize, what: &str) -> anyhow::Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot {what} memory accounting overflow"))
}

pub(super) fn estimate_arrow_working_set_bytes(
    rows: &[SnapshotRow],
    columns: &[ColumnPlan],
    snapshot: Option<&MySqlSnapshotMetadata>,
) -> anyhow::Result<usize> {
    let row_count = rows.len();
    let user_cells = checked_snapshot_product(row_count, columns.len(), "Arrow user cells")?;
    let mut byte_payload = 0_usize;
    for (row_index, row) in rows.iter().enumerate() {
        anyhow::ensure!(
            row.len() == columns.len(),
            "MySQL snapshot row {row_index} has {} values, expected {}",
            row.len(),
            columns.len()
        );
        for value in row.iter().flatten() {
            if let Value::Bytes(value) = value {
                byte_payload = checked_snapshot_sum(
                    byte_payload,
                    value.len(),
                    "Arrow source payload",
                )?;
            }
        }
    }

    let (output_columns, generated_payload_per_row) = match snapshot {
        Some(snapshot) => {
            let transaction_identity = encode_snapshot_boundary_identity(&snapshot.boundary)?;
            let changed_mask = columns.len().div_ceil(8);
            let payload = snapshot
                .database
                .len()
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(snapshot.table.len()))
                .and_then(|bytes| bytes.checked_add(transaction_identity.len()))
                .and_then(|bytes| bytes.checked_add(snapshot.boundary.filename.len()))
                .and_then(|bytes| {
                    bytes.checked_add(transferia_core::ChangeOperation::SnapshotRead.code().len())
                })
                .and_then(|bytes| bytes.checked_add(changed_mask))
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL snapshot generated Arrow payload accounting overflow")
                })?;
            (
                columns
                    .len()
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
                    .and_then(|value| value.checked_add(MYSQL_REPLICATION_SYSTEM_COLUMNS.len()))
                    .ok_or_else(|| {
                        anyhow::anyhow!("MySQL snapshot Arrow column count overflow")
                    })?,
                payload,
            )
        }
        None => (
            columns.len().checked_add(4).ok_or_else(|| {
                anyhow::anyhow!("MySQL snapshot Arrow column count overflow")
            })?,
            "mysql".len(),
        ),
    };

    let generated_payload =
        checked_snapshot_product(row_count, generated_payload_per_row, "generated Arrow payload")?;
    let output_cells = checked_snapshot_product(row_count, output_columns, "Arrow output cells")?;
    let cell_storage = checked_snapshot_product(
        output_cells,
        size_of::<Option<u64>>(),
        "Arrow values and validity",
    )?;
    let conversion_refs = checked_snapshot_product(
        user_cells,
        size_of::<Option<&Value>>(),
        "Arrow conversion references",
    )?;
    // Every variable-width builder starts with 1024 values and 1024 payload bytes. This
    // fixed allowance also covers 64-byte Arrow buffer rounding for fixed-width arrays.
    let builder_slack = checked_snapshot_product(output_columns, 8 * 1024, "Arrow builders")?;
    let logical = checked_snapshot_sum(byte_payload, generated_payload, "Arrow payload")?;
    let logical = checked_snapshot_sum(logical, cell_storage, "Arrow cell storage")?;
    let logical = checked_snapshot_sum(logical, conversion_refs, "Arrow conversion")?;
    let logical = checked_snapshot_sum(logical, builder_slack, "Arrow builder slack")?;
    // Vec and Arrow builders grow geometrically; twice the requested logical working set
    // covers the live builder capacity and the temporary conversion vector before finish().
    checked_snapshot_product(logical.max(1), 2, "Arrow working set")
}

pub(super) fn snapshot_row_error(
    error: mysql_async::Error,
    max_row_bytes: usize,
) -> DataPlaneFailure {
    if error.is_packet_too_large() {
        DataPlaneFailure::fatal(anyhow::anyhow!(
            "MySQL snapshot row exceeds configured max_row_bytes={max_row_bytes}"
        ))
    } else {
        DataPlaneFailure::retryable(error.into())
    }
}

impl Source for MySqlSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(SourceBatch::Finished);
            }
            let initial_admission = self
                .batch_target_bytes
                .checked_add(self.max_decoded_row_bytes)
                .ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "MySQL snapshot memory admission overflow"
                    ))
                })?;
            let memory = self
                .memory
                .reserve_progress_source(initial_admission)
                .await;
            let mut rows: Vec<SnapshotRow> = Vec::new();
            let mut retained_value_bytes = 0_usize;
            let mut retained_row_bytes = 0_usize;
            while should_read_snapshot_row(
                rows.len(),
                retained_row_bytes,
                self.batch_rows,
                self.batch_target_bytes,
            ) {
                if !rows.is_empty() {
                    memory
                        .grow_progress_source_to(
                            retained_row_bytes
                                .checked_add(self.max_decoded_row_bytes)
                                .ok_or_else(|| {
                                    DataPlaneFailure::fatal(anyhow::anyhow!(
                                        "MySQL snapshot in-flight row memory accounting overflow"
                                    ))
                                })?,
                        )
                        .map_err(DataPlaneFailure::fatal)?;
                }
                let stream = self.stream.as_mut().ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "MySQL snapshot stream is unavailable before completion"
                    ))
                })?;
                let wait_started = Instant::now();
                let next = stream.next().await;
                self.counters.add_response_wait(wait_started.elapsed());
                match next {
                    Some(Ok(row)) => {
                        let row = row.unwrap_raw();
                        let row_value_bytes = retained_row_value_heap_bytes(&row)
                            .map_err(DataPlaneFailure::fatal)?;
                        let previous_row_bytes = retained_row_bytes;
                        retained_value_bytes = retained_value_bytes
                            .checked_add(row_value_bytes)
                            .ok_or_else(|| {
                                DataPlaneFailure::fatal(anyhow::anyhow!(
                                    "MySQL snapshot batch memory accounting overflow"
                                ))
                            })?;
                        let push_peak_bytes = snapshot_rows_push_peak_bytes(
                            rows.len(),
                            rows.capacity(),
                            retained_value_bytes,
                        )
                        .map_err(DataPlaneFailure::fatal)?;
                        memory
                            .grow_progress_source_to(push_peak_bytes)
                            .map_err(DataPlaneFailure::fatal)?;
                        rows.try_reserve(1).map_err(|error| {
                            DataPlaneFailure::fatal(anyhow::anyhow!(
                                "MySQL snapshot row-vector allocation failed: {error}"
                            ))
                        })?;
                        rows.push(row);
                        retained_row_bytes = retained_rows_heap_bytes(
                            rows.capacity(),
                            retained_value_bytes,
                        )
                        .map_err(DataPlaneFailure::fatal)?;
                        validate_snapshot_batch_growth(
                            previous_row_bytes,
                            retained_row_bytes,
                            self.batch_target_bytes,
                        )
                        .map_err(DataPlaneFailure::fatal)?;
                        memory
                            .grow_progress_source_to(retained_row_bytes)
                            .map_err(DataPlaneFailure::fatal)?;
                        let _ = memory.shrink_to(retained_row_bytes);
                    }
                    Some(Err(error)) => {
                        return Err(snapshot_row_error(error, self.max_row_bytes));
                    }
                    None => break,
                }
            }
            if rows.is_empty() {
                self.stream.take();
                self.finished = true;
                return Ok(SourceBatch::Finished);
            }
            let source_rows = u64::try_from(rows.len())
                .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
            memory
                .grow_progress_source_to(
                    retained_row_bytes
                        .checked_add(self.max_decoded_row_bytes)
                        .ok_or_else(|| {
                            DataPlaneFailure::fatal(anyhow::anyhow!(
                                "MySQL snapshot Arrow estimate admission overflow"
                            ))
                        })?,
                )
                .map_err(DataPlaneFailure::fatal)?;
            let arrow_working_set = estimate_arrow_working_set_bytes(
                &rows,
                &self.columns,
                self.snapshot_metadata.as_ref(),
            )
            .map_err(DataPlaneFailure::fatal)?;
            memory
                .grow_progress_source_to(
                    retained_row_bytes
                        .checked_add(arrow_working_set.max(self.max_decoded_row_bytes))
                        .ok_or_else(|| {
                            DataPlaneFailure::fatal(anyhow::anyhow!(
                                "MySQL snapshot raw and Arrow admission overflow"
                            ))
                        })?,
                )
                .map_err(DataPlaneFailure::fatal)?;
            let batch = match &self.snapshot_metadata {
                Some(metadata) => rows_to_changelog_snapshot_batch(
                    &self.schema,
                    &self.columns,
                    &rows,
                    self.offset,
                    metadata,
                ),
                None => rows_to_batch(&self.schema, &self.columns, &rows, self.offset),
            }
            .map_err(DataPlaneFailure::fatal)?;
            let arrow_bytes = batch.get_array_memory_size();
            if arrow_bytes > arrow_working_set {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "MySQL snapshot Arrow memory {} exceeded its pre-admitted {}-byte working set",
                    arrow_bytes,
                    arrow_working_set
                )));
            }
            drop(rows);
            let _ = memory.shrink_to(arrow_bytes);
            self.offset = self
                .offset
                .checked_add(
                    i64::try_from(source_rows)
                        .map_err(|error| DataPlaneFailure::fatal(error.into()))?,
                )
                .ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!("MySQL source offset overflow"))
                })?;
            self.counters.add_records(source_rows);
            Ok(SourceBatch::Typed {
                tables: vec![TableData::new(
                    Arc::from(self.table.name.as_str()),
                    false,
                    batch,
                    routing_system_columns(
                        if self.snapshot_metadata.is_some() {
                            self.columns
                                .len()
                                .checked_mul(2)
                                .and_then(|value| {
                                    value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len())
                                })
                                .ok_or_else(|| {
                                    DataPlaneFailure::fatal(anyhow::anyhow!(
                                        "MySQL snapshot system-column index overflow"
                                    ))
                                })?
                        } else {
                            self.columns.len()
                        },
                        if self.snapshot_metadata.is_some() {
                            MYSQL_REPLICATION_SYSTEM_COLUMNS
                        } else {
                            &[
                                SystemColumnKind::Topic,
                                SystemColumnKind::Partition,
                                SystemColumnKind::Offset,
                                SystemColumnKind::MessageIndex,
                            ]
                        },
                    ),
                )],
                source_rows,
                commit_marker: Some(CommitMarker::new(self.offset)),
                memory: vec![memory],
            })
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            self.stream.take();
            self.finished = true;
            Ok(())
        })
    }
}

fn rows_to_changelog_snapshot_batch<R: RowValueAccess>(
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    rows: &[R],
    start_offset: i64,
    snapshot: &MySqlSnapshotMetadata,
) -> anyhow::Result<RecordBatch> {
    let (mut fields, mut arrays) = user_fields_and_arrays(discovered_schema, columns, rows, true)?;
    let len = rows.len();
    for (index, column) in discovered_schema.columns.iter().enumerate() {
        fields.push(
            Field::new(old_value_column_name(index), column.data_type.clone(), true)
                .with_metadata(std::collections::HashMap::from([(
                    transferia_core::data::schema::META_OLD_VALUE_OF.to_owned(),
                    column.name.clone(),
                )])),
        );
        arrays.push(new_null_array(&column.data_type, len));
    }
    fields.extend(MYSQL_SOURCE_METADATA_COLUMNS.iter().map(|column| {
        Field::new(column.name, column.data_type.clone(), false).with_metadata(
            SchemaColumn::new(column.name.to_owned(), column.data_type.clone(), false)
                .with_system_role(column.role)
                .arrow_metadata(),
        )
    }));
    fields.extend(MYSQL_REPLICATION_SYSTEM_COLUMNS.iter().map(|kind| {
        let field = Field::new(kind.default_name(), kind.data_type(), false);
        if *kind == SystemColumnKind::ChangeOperation {
            field.with_metadata(std::collections::HashMap::from([(
                transferia_core::data::schema::META_CHANGE_OPERATION.to_owned(),
                "true".to_owned(),
            )]))
        } else {
            field
        }
    }));
    let transaction_identity = encode_snapshot_boundary_identity(&snapshot.boundary)?;
    let source_timestamp_us = snapshot.boundary.source_timestamp_micros;
    let source_timestamp_ms = source_timestamp_us / 1_000;
    let source_timestamp_ns = source_timestamp_us.checked_mul(1_000).ok_or_else(|| {
        anyhow::anyhow!("MySQL snapshot source timestamp nanoseconds overflow")
    })?;
    let event_timestamp_ns = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| anyhow::anyhow!("system clock precedes Unix epoch: {error}"))?
            .as_nanos(),
    )?;
    let event_timestamp_us = event_timestamp_ns / 1_000;
    let event_timestamp_ms = event_timestamp_ns / 1_000_000;
    arrays.extend([
        Arc::new(StringArray::from(vec![snapshot.database.as_str(); len])) as ArrayRef,
        Arc::new(StringArray::from(vec![snapshot.database.as_str(); len])) as ArrayRef,
        Arc::new(StringArray::from(vec![snapshot.table.as_str(); len])) as ArrayRef,
        Arc::new(BinaryArray::from_iter_values(
            std::iter::repeat(transaction_identity.as_slice()).take(len),
        )) as ArrayRef,
        Arc::new(Int64Array::from(vec![source_timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![source_timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![source_timestamp_ns; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_ns; len])) as ArrayRef,
    ]);
    let filename = snapshot.boundary.filename.as_str();
    let position = i64::try_from(snapshot.boundary.position)?;
    let changed = full_changed_columns_mask(discovered_schema.columns.len());
    let len_i64 = i64::try_from(len)?;
    for kind in MYSQL_REPLICATION_SYSTEM_COLUMNS {
        arrays.push(match kind {
            SystemColumnKind::Topic => {
                Arc::new(StringArray::from(vec![filename; len])) as ArrayRef
            }
            SystemColumnKind::Partition => {
                Arc::new(Int64Array::from(vec![snapshot.partition_id; len])) as ArrayRef
            }
            SystemColumnKind::Offset => {
                Arc::new(Int64Array::from(vec![position; len])) as ArrayRef
            }
            SystemColumnKind::MessageIndex => Arc::new(UInt64Array::from_iter_values(
                u64::try_from(start_offset)?
                    ..u64::try_from(start_offset.checked_add(len_i64).ok_or_else(|| {
                        anyhow::anyhow!("MySQL snapshot offset overflow")
                    })?)?,
            )) as ArrayRef,
            SystemColumnKind::ChangeOperation => Arc::new(StringArray::from(vec![
                transferia_core::ChangeOperation::SnapshotRead.code();
                len
            ])) as ArrayRef,
            SystemColumnKind::ChangedColumns => Arc::new(BinaryArray::from_iter_values(
                std::iter::repeat(changed.as_slice()).take(len),
            )) as ArrayRef,
            SystemColumnKind::WriteTimestampMs => {
                anyhow::bail!("MySQL snapshot has no write timestamp")
            }
        });
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn full_changed_columns_mask(columns: usize) -> Vec<u8> {
    let mut mask = vec![0_u8; columns.div_ceil(8)];
    for index in 0..columns {
        mask[index / 8] |= 1 << (index % 8);
    }
    mask
}

pub(super) fn rows_to_batch<R: RowValueAccess>(
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    rows: &[R],
    start_offset: i64,
) -> anyhow::Result<RecordBatch> {
    let (mut fields, mut arrays) = user_fields_and_arrays(discovered_schema, columns, rows, false)?;
    let len = rows.len();
    let len_i64 = i64::try_from(len)?;
    fields.extend([
        Field::new(
            SystemColumnKind::Topic.default_name(),
            DataType::Utf8,
            false,
        ),
        Field::new(
            SystemColumnKind::Partition.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::MessageIndex.default_name(),
            DataType::UInt64,
            false,
        ),
    ]);
    arrays.extend([
        Arc::new(arrow::array::StringArray::from(vec!["mysql"; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0_i64; len])) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            start_offset
                ..start_offset
                    .checked_add(len_i64)
                    .ok_or_else(|| anyhow::anyhow!("MySQL source offset overflow"))?,
        )) as ArrayRef,
        Arc::new(UInt64Array::from(vec![0_u64; len])) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

pub(super) fn user_fields_and_arrays<R: RowValueAccess>(
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    rows: &[R],
    force_nullable: bool,
) -> anyhow::Result<(Vec<Field>, Vec<ArrayRef>)> {
    anyhow::ensure!(
        discovered_schema.columns.len() == columns.len(),
        "MySQL query schema has {} columns, discovery declared {}",
        columns.len(),
        discovered_schema.columns.len()
    );
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for (index, (column, discovered)) in columns.iter().zip(&discovered_schema.columns).enumerate()
    {
        anyhow::ensure!(
            column.name == discovered.name && column.kind.arrow_type() == discovered.data_type,
            "MySQL query schema drifted at column '{}'",
            column.name
        );
        fields.push(
            Field::new(
                &column.name,
                column.kind.arrow_type(),
                force_nullable || column.nullable,
            )
                .with_metadata(discovered.arrow_metadata()),
        );
        arrays.push(column_array(rows, index, column)?);
    }
    Ok((fields, arrays))
}

pub(super) fn column_array<R: RowValueAccess>(
    rows: &[R],
    index: usize,
    column: &ColumnPlan,
) -> anyhow::Result<ArrayRef> {
    let values = rows
        .iter()
        .map(|row| row.value_at(index))
        .collect::<Vec<_>>();
    values_to_array(&values, column)
}

pub(crate) fn optional_value_column_array(
    rows: &[Option<&[Option<Value>]>],
    index: usize,
    column: &ColumnPlan,
) -> anyhow::Result<ArrayRef> {
    let values = rows
        .iter()
        .map(|row| {
            row.as_ref()
                .and_then(|row| row.get(index))
                .and_then(Option::as_ref)
        })
        .collect::<Vec<_>>();
    values_to_array(&values, column)
}

fn values_to_array(values: &[Option<&Value>], column: &ColumnPlan) -> anyhow::Result<ArrayRef> {
    macro_rules! integer_array {
        ($value:ident, $ty:ty, $array:ty) => {{
            let values = values
                .iter()
                .map(|value| match value {
                    Some(Value::NULL) | None => Ok(None),
                    Some(value) => $value(value).map(Some),
                })
                .collect::<anyhow::Result<Vec<Option<$ty>>>>()?;
            Arc::new(<$array>::from(values)) as ArrayRef
        }};
    }
    Ok(match column.kind {
        MySqlColumnKind::Int8 => integer_array!(value_i64, i8, Int8Array),
        MySqlColumnKind::UInt8 => integer_array!(value_u64, u8, UInt8Array),
        MySqlColumnKind::Int16 => integer_array!(value_i64, i16, Int16Array),
        MySqlColumnKind::UInt16 => integer_array!(value_u64, u16, UInt16Array),
        MySqlColumnKind::Int32 => integer_array!(value_i64, i32, Int32Array),
        MySqlColumnKind::UInt32 => integer_array!(value_u64, u32, UInt32Array),
        MySqlColumnKind::Int64 => integer_array!(value_i64, i64, Int64Array),
        MySqlColumnKind::UInt64 => integer_array!(value_u64, u64, UInt64Array),
        MySqlColumnKind::Float32 => integer_array!(value_f64, f32, Float32Array),
        MySqlColumnKind::Float64 => integer_array!(value_f64, f64, Float64Array),
        MySqlColumnKind::Binary => {
            let mut builder = BinaryBuilder::new();
            for value in values {
                match value {
                    Some(Value::NULL) | None => builder.append_null(),
                    Some(Value::Bytes(value)) => builder.append_value(value),
                    Some(value) => anyhow::bail!(
                        "MySQL binary column '{}' returned unexpected value {value:?}",
                        column.name
                    ),
                }
            }
            Arc::new(builder.finish())
        }
        MySqlColumnKind::Utf8 | MySqlColumnKind::Json => {
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    Some(Value::NULL) | None => builder.append_null(),
                    Some(Value::Bytes(value)) => {
                        builder.append_value(std::str::from_utf8(value).map_err(|error| {
                            anyhow::anyhow!(
                                "MySQL text column '{}' is not valid UTF-8: {error}",
                                column.name
                            )
                        })?);
                    }
                    Some(value) => anyhow::bail!(
                        "MySQL text column '{}' returned unexpected value {value:?}",
                        column.name
                    ),
                }
            }
            Arc::new(builder.finish())
        }
        MySqlColumnKind::Date => {
            let values = values
                .iter()
                .map(|value| match value {
                    Some(Value::NULL) | None => Ok(None),
                    Some(value) => value_date32(value).map(Some),
                })
                .collect::<anyhow::Result<Vec<Option<i32>>>>()?;
            Arc::new(Date32Array::from(values))
        }
        MySqlColumnKind::DateTime | MySqlColumnKind::TimestampUtc => {
            let values = values
                .iter()
                .map(|value| match value {
                    Some(Value::NULL) | None => Ok(None),
                    Some(value) => value_timestamp_micros(value).map(Some),
                })
                .collect::<anyhow::Result<Vec<Option<i64>>>>()?;
            let array = TimestampMicrosecondArray::from(values);
            if column.kind == MySqlColumnKind::TimestampUtc {
                Arc::new(array.with_timezone("UTC"))
            } else {
                Arc::new(array)
            }
        }
    })
}

pub(super) fn value_date32(value: &Value) -> anyhow::Result<i32> {
    let (year, month, day) = match value {
        Value::Date(year, month, day, hour, minute, second, micros) => {
            anyhow::ensure!(
                (*hour, *minute, *second, *micros) == (0, 0, 0, 0),
                "MySQL DATE contained a time component"
            );
            (*year, *month, *day)
        }
        Value::Bytes(value) => parse_date_text(value)?,
        other => anyhow::bail!("expected MySQL DATE, got {other:?}"),
    };
    let date = checked_mysql_date(year, month, day)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| anyhow::anyhow!("Unix epoch is not representable"))?;
    Ok(i32::try_from(date.signed_duration_since(epoch).num_days())?)
}

pub(super) fn value_timestamp_micros(value: &Value) -> anyhow::Result<i64> {
    let (year, month, day, hour, minute, second, micros) = match value {
        Value::Date(year, month, day, hour, minute, second, micros) => {
            (*year, *month, *day, *hour, *minute, *second, *micros)
        }
        Value::Bytes(value) => parse_datetime_text(value)?,
        other => anyhow::bail!("expected MySQL DATETIME or TIMESTAMP, got {other:?}"),
    };
    let date = checked_mysql_date(year, month, day)?;
    anyhow::ensure!(micros < 1_000_000, "MySQL temporal microseconds are out of range");
    let datetime = date
        .and_hms_micro_opt(
            u32::from(hour),
            u32::from(minute),
            u32::from(second),
            micros,
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL temporal time component is invalid"))?;
    Ok(datetime.and_utc().timestamp_micros())
}

fn checked_mysql_date(year: u16, month: u8, day: u8) -> anyhow::Result<NaiveDate> {
    anyhow::ensure!(
        (1000..=9999).contains(&year),
        "MySQL temporal year {year} is outside the supported 1000..=9999 range"
    );
    NaiveDate::from_ymd_opt(i32::from(year), u32::from(month), u32::from(day))
        .ok_or_else(|| anyhow::anyhow!("MySQL temporal date component is invalid"))
}

fn parse_date_text(value: &[u8]) -> anyhow::Result<(u16, u8, u8)> {
    anyhow::ensure!(
        value.len() == 10 && value[4] == b'-' && value[7] == b'-',
        "MySQL DATE text does not use YYYY-MM-DD"
    );
    Ok((
        decimal_digits(&value[0..4])?,
        decimal_digits(&value[5..7])?,
        decimal_digits(&value[8..10])?,
    ))
}

fn parse_datetime_text(value: &[u8]) -> anyhow::Result<(u16, u8, u8, u8, u8, u8, u32)> {
    anyhow::ensure!(
        value.len() >= 19
            && value[4] == b'-'
            && value[7] == b'-'
            && value[10] == b' '
            && value[13] == b':'
            && value[16] == b':',
        "MySQL DATETIME/TIMESTAMP text does not use YYYY-MM-DD HH:MM:SS[.ffffff]"
    );
    let micros = match value.get(19..) {
        Some([]) => 0,
        Some(fraction) if fraction[0] == b'.' && (2..=7).contains(&fraction.len()) => {
            let digits = &fraction[1..];
            decimal_digits::<u32>(digits)?
                * 10_u32.pow(6 - u32::try_from(digits.len())?)
        }
        _ => anyhow::bail!(
            "MySQL DATETIME/TIMESTAMP fraction must contain between one and six digits"
        ),
    };
    Ok((
        decimal_digits(&value[0..4])?,
        decimal_digits(&value[5..7])?,
        decimal_digits(&value[8..10])?,
        decimal_digits(&value[11..13])?,
        decimal_digits(&value[14..16])?,
        decimal_digits(&value[17..19])?,
        micros,
    ))
}

fn decimal_digits<T>(value: &[u8]) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    anyhow::ensure!(
        !value.is_empty() && value.iter().all(u8::is_ascii_digit),
        "MySQL temporal component contains a non-decimal character"
    );
    Ok(std::str::from_utf8(value)?.parse()?)
}

pub(super) fn value_i64<T>(value: &Value) -> anyhow::Result<T>
where
    T: TryFrom<i64> + std::str::FromStr,
    T::Error: std::error::Error + Send + Sync + 'static,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    match value {
        Value::Int(value) => Ok(T::try_from(*value)?),
        Value::UInt(value) => Ok(T::try_from(i64::try_from(*value)?)?),
        Value::Bytes(value) => Ok(std::str::from_utf8(value)?.parse()?),
        other => anyhow::bail!("expected signed MySQL integer, got {other:?}"),
    }
}

pub(super) fn value_u64<T>(value: &Value) -> anyhow::Result<T>
where
    T: TryFrom<u64> + std::str::FromStr,
    T::Error: std::error::Error + Send + Sync + 'static,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    match value {
        Value::UInt(value) => Ok(T::try_from(*value)?),
        Value::Int(value) => Ok(T::try_from(u64::try_from(*value)?)?),
        Value::Bytes(value) => Ok(std::str::from_utf8(value)?.parse()?),
        other => anyhow::bail!("expected unsigned MySQL integer, got {other:?}"),
    }
}

pub(super) fn value_f64<T>(value: &Value) -> anyhow::Result<T>
where
    T: From<f32> + std::str::FromStr,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    match value {
        Value::Float(value) => Ok(T::from(*value)),
        Value::Double(value) => Ok(value.to_string().parse()?),
        Value::Bytes(value) => Ok(std::str::from_utf8(value)?.parse()?),
        other => anyhow::bail!("expected MySQL floating-point value, got {other:?}"),
    }
}

fn routing_system_columns(base: usize, kinds: &[SystemColumnKind]) -> SystemColumns {
    SystemColumns::new(
        kinds
            .iter()
            .enumerate()
            .map(|(offset, kind)| SystemColumn {
                kind: *kind,
                name: Arc::from(kind.default_name()),
                index: base + offset,
            })
            .collect::<Vec<_>>(),
    )
}
