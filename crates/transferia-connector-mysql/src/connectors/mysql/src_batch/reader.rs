use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    new_null_array, ArrayRef, BinaryArray, BinaryBuilder, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, StringBuilder, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use mysql_async::prelude::{Query, Queryable, WithParams};
use mysql_async::{BinaryProtocol, Conn, ResultSetStream, Row, TextProtocol, Value};
use transferia_connector_support::external_request::observe_external_request;

use super::config::{MySqlReadProtocol, TableConfig, MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES};
use super::connector::{
    old_value_column_name, validate_snapshot_read_protocol, ColumnPlan, MySqlColumnKind,
    MYSQL_REPLICATION_SYSTEM_COLUMNS, MYSQL_SOURCE_METADATA_COLUMNS,
};
use super::MYSQL_CANONICAL_SNAPSHOT_SQL_MODE;
use crate::connectors::mysql::common::{quote_identifier, validate_mysql_client_packet_limit};
use crate::connectors::mysql::src_batch_and_stream::MySqlBinlogBoundary;
use crate::connectors::mysql::src_stream::encode_snapshot_boundary_identity;
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME,
    META_CHANGE_OPERATION, META_LOW_CARDINALITY, META_MAX_LENGTH, META_OLD_KEY_OF,
    META_OLD_VALUE_OF, META_PRIMARY_KEY, META_SYSTEM_ROLE,
};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::{MemoryReservation, PipelineMemory};
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
    columns: Vec<ColumnPlan>,
    batch_rows: usize,
    batch_target_bytes: usize,
    max_row_bytes: usize,
    max_decoded_row_bytes: usize,
    output_schema: Arc<Schema>,
    output_schema_memory: MemoryReservation,
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
        observe_external_request(
            "mysql",
            "set_snapshot_timezone",
            connection.query_drop("SET SESSION time_zone = '+00:00'"),
        )
        .await?;
        observe_external_request(
            "mysql",
            "set_snapshot_sql_mode",
            connection.query_drop(MYSQL_CANONICAL_SNAPSHOT_SQL_MODE),
        )
        .await?;
        let forbidden_sql_mode = observe_external_request(
            "mysql",
            "verify_snapshot_sql_mode",
            connection.query_first::<u64, _>(
                "SELECT FIND_IN_SET('PAD_CHAR_TO_FULL_LENGTH', @@SESSION.sql_mode)",
            ),
        )
        .await?;
        anyhow::ensure!(
            forbidden_sql_mode == Some(0),
            "MySQL snapshot session retained PAD_CHAR_TO_FULL_LENGTH after canonical setup"
        );
        observe_external_request(
            "mysql",
            "set_snapshot_isolation",
            connection.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        )
        .await?;
        observe_external_request(
            "mysql",
            "start_consistent_snapshot",
            connection.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT"),
        )
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
        validate_snapshot_read_protocol(read_protocol, &columns)?;
        let max_decoded_row_bytes = max_decoded_row_admission_bytes(max_row_bytes, columns.len())?;
        let (output_schema, output_schema_memory) = build_output_schema_with_memory(
            &memory,
            &schema,
            &columns,
            snapshot_metadata.is_some(),
        )
        .await?;
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
            columns,
            batch_rows,
            batch_target_bytes,
            max_row_bytes,
            max_decoded_row_bytes,
            output_schema,
            output_schema_memory,
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

pub(super) const fn should_read_snapshot_row(
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
                byte_payload =
                    checked_snapshot_sum(byte_payload, value.len(), "Arrow source payload")?;
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
                    .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow column count overflow"))?,
                payload,
            )
        }
        None => (
            columns
                .len()
                .checked_add(4)
                .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow column count overflow"))?,
            "mysql".len(),
        ),
    };

    let generated_payload = checked_snapshot_product(
        row_count,
        generated_payload_per_row,
        "generated Arrow payload",
    )?;
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
    // Every variable-width array builder starts with 1024 values and 1024 payload bytes.
    // This allowance covers builder/buffer capacity only; the cached schema has its own
    // exact persistent reservation derived from Arrow's deep Field::size accounting.
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
            let memory = self.memory.reserve_progress_source(initial_admission).await;
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
                        let row_value_bytes =
                            retained_row_value_heap_bytes(&row).map_err(DataPlaneFailure::fatal)?;
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
                        retained_row_bytes =
                            retained_rows_heap_bytes(rows.capacity(), retained_value_bytes)
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
            let source_rows =
                u64::try_from(rows.len()).map_err(|error| DataPlaneFailure::fatal(error.into()))?;
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
                    Arc::clone(&self.output_schema),
                    &self.columns,
                    &rows,
                    self.offset,
                    metadata,
                ),
                None => rows_to_batch(
                    Arc::clone(&self.output_schema),
                    &self.columns,
                    &rows,
                    self.offset,
                ),
            }
            .map_err(DataPlaneFailure::fatal)?;
            let arrow_bytes = batch.get_array_memory_size();
            if arrow_bytes > arrow_working_set {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "MySQL snapshot Arrow memory {arrow_bytes} exceeded its pre-admitted {arrow_working_set}-byte working set"
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
                memory: vec![memory, self.output_schema_memory.clone()],
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

pub(super) fn rows_to_changelog_snapshot_batch<R: RowValueAccess>(
    output_schema: Arc<Schema>,
    columns: &[ColumnPlan],
    rows: &[R],
    start_offset: i64,
    snapshot: &MySqlSnapshotMetadata,
) -> anyhow::Result<RecordBatch> {
    let mut arrays = user_arrays(columns, rows)?;
    let len = rows.len();
    for column in columns {
        arrays.push(new_null_array(&column.kind.arrow_type(), len));
    }
    let transaction_identity = encode_snapshot_boundary_identity(&snapshot.boundary)?;
    let source_timestamp_us = snapshot.boundary.source_timestamp_micros;
    let source_timestamp_ms = source_timestamp_us / 1_000;
    let source_timestamp_ns = source_timestamp_us
        .checked_mul(1_000)
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot source timestamp nanoseconds overflow"))?;
    let filename = snapshot.boundary.filename.as_str();
    let position = i64::try_from(snapshot.boundary.position)?;
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
        Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
            transaction_identity.as_slice(),
            len,
        ))) as ArrayRef,
        Arc::new(Int64Array::from(vec![0_i64; len])) as ArrayRef,
        Arc::new(std::iter::repeat_n(None::<&str>, len).collect::<StringArray>()) as ArrayRef,
        Arc::new(StringArray::from(vec![filename; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![position; len])) as ArrayRef,
        Arc::new(Int32Array::from(vec![0_i32; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![source_timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![source_timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![source_timestamp_ns; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_ns; len])) as ArrayRef,
    ]);
    let changed = full_changed_columns_mask(columns.len());
    let len_i64 = i64::try_from(len)?;
    for kind in MYSQL_REPLICATION_SYSTEM_COLUMNS {
        arrays.push(match kind {
            SystemColumnKind::Topic => Arc::new(StringArray::from(vec![filename; len])) as ArrayRef,
            SystemColumnKind::Partition => {
                Arc::new(Int64Array::from(vec![snapshot.partition_id; len])) as ArrayRef
            }
            SystemColumnKind::Offset => Arc::new(Int64Array::from(vec![position; len])) as ArrayRef,
            SystemColumnKind::MessageIndex => Arc::new(UInt64Array::from_iter_values(
                u64::try_from(start_offset)?
                    ..u64::try_from(
                        start_offset
                            .checked_add(len_i64)
                            .ok_or_else(|| anyhow::anyhow!("MySQL snapshot offset overflow"))?,
                    )?,
            )) as ArrayRef,
            SystemColumnKind::ChangeOperation => Arc::new(StringArray::from(vec![
                transferia_core::ChangeOperation::SnapshotRead.code();
                len
            ])) as ArrayRef,
            SystemColumnKind::ChangedColumns => Arc::new(BinaryArray::from_iter_values(
                std::iter::repeat_n(changed.as_slice(), len),
            )) as ArrayRef,
            SystemColumnKind::WriteTimestampMs => {
                anyhow::bail!("MySQL snapshot has no write timestamp")
            }
        });
    }
    Ok(RecordBatch::try_new(output_schema, arrays)?)
}

fn full_changed_columns_mask(columns: usize) -> Vec<u8> {
    let mut mask = vec![0_u8; columns.div_ceil(8)];
    for index in 0..columns {
        mask[index / 8] |= 1 << (index % 8);
    }
    mask
}

pub(super) fn rows_to_batch<R: RowValueAccess>(
    output_schema: Arc<Schema>,
    columns: &[ColumnPlan],
    rows: &[R],
    start_offset: i64,
) -> anyhow::Result<RecordBatch> {
    let mut arrays = user_arrays(columns, rows)?;
    let len = rows.len();
    let len_i64 = i64::try_from(len)?;
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
    Ok(RecordBatch::try_new(output_schema, arrays)?)
}

pub(super) fn build_output_schema(
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    changelog_snapshot: bool,
) -> anyhow::Result<Arc<Schema>> {
    anyhow::ensure!(
        discovered_schema.columns.len() == columns.len(),
        "MySQL query schema has {} columns, discovery declared {}",
        columns.len(),
        discovered_schema.columns.len()
    );
    let system_columns = if changelog_snapshot {
        MYSQL_REPLICATION_SYSTEM_COLUMNS
    } else {
        &[
            SystemColumnKind::Topic,
            SystemColumnKind::Partition,
            SystemColumnKind::Offset,
            SystemColumnKind::MessageIndex,
        ]
    };
    let capacity = columns
        .len()
        .checked_mul(if changelog_snapshot { 2 } else { 1 })
        .and_then(|value| {
            value.checked_add(if changelog_snapshot {
                MYSQL_SOURCE_METADATA_COLUMNS.len()
            } else {
                0
            })
        })
        .and_then(|value| value.checked_add(system_columns.len()))
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow schema column count overflow"))?;
    let mut fields = Vec::with_capacity(capacity);
    for (column, discovered) in columns.iter().zip(&discovered_schema.columns) {
        anyhow::ensure!(
            column.name == discovered.name && column.kind.arrow_type() == discovered.data_type,
            "MySQL query schema drifted at column '{}'",
            column.name
        );
        fields.push(
            Field::new(
                &column.name,
                column.kind.arrow_type(),
                changelog_snapshot || column.nullable,
            )
            .with_metadata(discovered.arrow_metadata()),
        );
    }
    if changelog_snapshot {
        fields.extend(
            discovered_schema
                .columns
                .iter()
                .enumerate()
                .map(|(index, current)| {
                    let mut metadata = HashMap::new();
                    if let Some(extension_name) = current.arrow_extension_name {
                        metadata.insert(
                            META_ARROW_EXTENSION_NAME.to_owned(),
                            extension_name.to_owned(),
                        );
                    }
                    if let Some(extension_metadata) = &current.arrow_extension_metadata {
                        metadata.insert(
                            META_ARROW_EXTENSION_METADATA.to_owned(),
                            extension_metadata.clone(),
                        );
                    }
                    metadata.insert(META_OLD_VALUE_OF.to_owned(), current.name.clone());
                    Field::new(
                        old_value_column_name(index),
                        current.data_type.clone(),
                        true,
                    )
                    .with_metadata(metadata)
                }),
        );
        fields.extend(MYSQL_SOURCE_METADATA_COLUMNS.iter().map(|column| {
            Field::new(column.name, column.data_type.clone(), column.nullable).with_metadata(
                HashMap::from([(META_SYSTEM_ROLE.to_owned(), column.role.to_owned())]),
            )
        }));
    }
    fields.extend(system_columns.iter().map(|kind| {
        let field = Field::new(kind.default_name(), kind.data_type(), false);
        if *kind == SystemColumnKind::ChangeOperation {
            field.with_metadata(HashMap::from([(
                transferia_core::data::schema::META_CHANGE_OPERATION.to_owned(),
                "true".to_owned(),
            )]))
        } else {
            field
        }
    }));
    Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn output_schema_heap_bytes(schema: &Schema) -> anyhow::Result<usize> {
    size_of::<Schema>()
        .checked_add(schema.fields().size())
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow schema memory accounting overflow"))
}

fn string_capacity_bound(length: usize) -> anyhow::Result<usize> {
    length
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .map(|value| value.max(32))
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow schema string capacity overflow"))
}

fn existing_string_capacity_bound(value: &String) -> anyhow::Result<usize> {
    Ok(value.capacity().max(string_capacity_bound(value.len())?))
}

const fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn metadata_allocation_bound(column: &SchemaColumn) -> anyhow::Result<(usize, usize)> {
    let mut entries = 0_usize;
    let mut strings = 0_usize;
    let mut add = |key: &str, value_capacity: usize| -> anyhow::Result<()> {
        entries = entries
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow field metadata count overflow"))?;
        strings = checked_snapshot_sum(
            strings,
            string_capacity_bound(key.len())?,
            "Arrow metadata keys",
        )?;
        strings = checked_snapshot_sum(strings, value_capacity, "Arrow metadata values")?;
        Ok(())
    };
    if column.primary_key {
        add(META_PRIMARY_KEY, string_capacity_bound("true".len())?)?;
    }
    if column.low_cardinality {
        add(META_LOW_CARDINALITY, string_capacity_bound("true".len())?)?;
    }
    if let Some(max_length) = column.max_length {
        add(
            META_MAX_LENGTH,
            string_capacity_bound(decimal_digits(max_length))?,
        )?;
    }
    if let Some(extension_name) = column.arrow_extension_name {
        add(
            META_ARROW_EXTENSION_NAME,
            string_capacity_bound(extension_name.len())?,
        )?;
    }
    if let Some(extension_metadata) = &column.arrow_extension_metadata {
        add(
            META_ARROW_EXTENSION_METADATA,
            existing_string_capacity_bound(extension_metadata)?,
        )?;
    }
    if let Some(role) = &column.system_role {
        add(META_SYSTEM_ROLE, existing_string_capacity_bound(role)?)?;
    }
    if let Some(current) = &column.old_value_of {
        add(META_OLD_VALUE_OF, existing_string_capacity_bound(current)?)?;
    }
    if let Some(current) = &column.old_key_of {
        add(META_OLD_KEY_OF, existing_string_capacity_bound(current)?)?;
    }
    Ok((entries, strings))
}

fn metadata_map_capacity_bound(entries: usize) -> anyhow::Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    // `arrow_metadata` starts from an empty map. Eight slots per rounded entry is a
    // conservative upper bound over the standard HashMap's geometric growth/load factor.
    entries
        .checked_next_power_of_two()
        .and_then(|value| value.checked_mul(8))
        .map(|value| value.max(16))
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow metadata capacity overflow"))
}

fn field_allocation_bound(
    name_capacity: usize,
    data_type: &DataType,
    metadata_entries: usize,
    metadata_strings: usize,
) -> anyhow::Result<usize> {
    let fixed = size_of::<Field>()
        .checked_sub(size_of::<DataType>())
        .and_then(|value| value.checked_add(data_type.size()))
        .and_then(|value| value.checked_add(size_of::<Arc<Field>>()))
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow field size overflow"))?;
    let metadata_storage = checked_snapshot_product(
        size_of::<(String, String)>(),
        metadata_map_capacity_bound(metadata_entries)?,
        "Arrow metadata map",
    )?;
    let bound = checked_snapshot_sum(fixed, name_capacity, "Arrow field name")?;
    let bound = checked_snapshot_sum(bound, metadata_storage, "Arrow field metadata map")?;
    checked_snapshot_sum(bound, metadata_strings, "Arrow field metadata strings")
}

fn add_schema_column_allocation_bound(
    total: &mut usize,
    column: &SchemaColumn,
    name_capacity: usize,
) -> anyhow::Result<()> {
    let (metadata_entries, metadata_strings) = metadata_allocation_bound(column)?;
    *total = checked_snapshot_sum(
        *total,
        field_allocation_bound(
            name_capacity,
            &column.data_type,
            metadata_entries,
            metadata_strings,
        )?,
        "Arrow schema fields",
    )?;
    Ok(())
}

pub(super) fn output_schema_allocation_bound(
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    changelog_snapshot: bool,
) -> anyhow::Result<usize> {
    anyhow::ensure!(
        discovered_schema.columns.len() == columns.len(),
        "MySQL query schema has {} columns, discovery declared {}",
        columns.len(),
        discovered_schema.columns.len()
    );
    let output_columns = if changelog_snapshot {
        columns
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
            .and_then(|value| value.checked_add(MYSQL_REPLICATION_SYSTEM_COLUMNS.len()))
    } else {
        columns.len().checked_add(4)
    }
    .ok_or_else(|| anyhow::anyhow!("MySQL snapshot Arrow schema column count overflow"))?;
    let transient_field_vector = checked_snapshot_product(
        output_columns,
        size_of::<Field>(),
        "Arrow schema field-vector capacity",
    )?;
    let mut total = checked_snapshot_sum(
        size_of::<Schema>(),
        transient_field_vector,
        "Arrow schema construction",
    )?;
    for (index, (column, discovered)) in columns.iter().zip(&discovered_schema.columns).enumerate()
    {
        add_schema_column_allocation_bound(
            &mut total,
            discovered,
            existing_string_capacity_bound(&column.name)?,
        )?;
        if changelog_snapshot {
            let old_name_length = "_system_old_value_"
                .len()
                .checked_add(decimal_digits(index))
                .ok_or_else(|| anyhow::anyhow!("MySQL old-value field name length overflow"))?;
            let mut metadata_entries = 1_usize;
            let mut metadata_strings = checked_snapshot_sum(
                string_capacity_bound(META_OLD_VALUE_OF.len())?,
                existing_string_capacity_bound(&discovered.name)?,
                "Arrow old-value metadata",
            )?;
            if let Some(extension_name) = discovered.arrow_extension_name {
                metadata_entries = metadata_entries
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("MySQL old-value metadata count overflow"))?;
                metadata_strings = checked_snapshot_sum(
                    metadata_strings,
                    checked_snapshot_sum(
                        string_capacity_bound(META_ARROW_EXTENSION_NAME.len())?,
                        string_capacity_bound(extension_name.len())?,
                        "Arrow old-value extension name",
                    )?,
                    "Arrow old-value metadata",
                )?;
            }
            if let Some(extension_metadata) = &discovered.arrow_extension_metadata {
                metadata_entries = metadata_entries
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("MySQL old-value metadata count overflow"))?;
                metadata_strings = checked_snapshot_sum(
                    metadata_strings,
                    checked_snapshot_sum(
                        string_capacity_bound(META_ARROW_EXTENSION_METADATA.len())?,
                        existing_string_capacity_bound(extension_metadata)?,
                        "Arrow old-value extension payload",
                    )?,
                    "Arrow old-value metadata",
                )?;
            }
            total = checked_snapshot_sum(
                total,
                field_allocation_bound(
                    string_capacity_bound(old_name_length)?,
                    &discovered.data_type,
                    metadata_entries,
                    metadata_strings,
                )?,
                "Arrow old-value fields",
            )?;
        }
    }
    if changelog_snapshot {
        for column in MYSQL_SOURCE_METADATA_COLUMNS {
            let metadata_strings = checked_snapshot_sum(
                string_capacity_bound(META_SYSTEM_ROLE.len())?,
                string_capacity_bound(column.role.len())?,
                "Arrow source-role metadata",
            )?;
            total = checked_snapshot_sum(
                total,
                field_allocation_bound(
                    string_capacity_bound(column.name.len())?,
                    &column.data_type,
                    1,
                    metadata_strings,
                )?,
                "Arrow source metadata fields",
            )?;
        }
    }
    let system_columns = if changelog_snapshot {
        MYSQL_REPLICATION_SYSTEM_COLUMNS
    } else {
        &[
            SystemColumnKind::Topic,
            SystemColumnKind::Partition,
            SystemColumnKind::Offset,
            SystemColumnKind::MessageIndex,
        ]
    };
    for kind in system_columns {
        let (metadata_entries, metadata_strings) = if *kind == SystemColumnKind::ChangeOperation {
            (
                1,
                checked_snapshot_sum(
                    string_capacity_bound(META_CHANGE_OPERATION.len())?,
                    string_capacity_bound("true".len())?,
                    "Arrow change-operation metadata",
                )?,
            )
        } else {
            (0, 0)
        };
        total = checked_snapshot_sum(
            total,
            field_allocation_bound(
                string_capacity_bound(kind.default_name().len())?,
                &kind.data_type(),
                metadata_entries,
                metadata_strings,
            )?,
            "Arrow system fields",
        )?;
    }
    Ok(total)
}

pub(super) async fn build_output_schema_with_memory(
    memory: &PipelineMemory,
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    changelog_snapshot: bool,
) -> anyhow::Result<(Arc<Schema>, MemoryReservation)> {
    let allocation_bound =
        output_schema_allocation_bound(discovered_schema, columns, changelog_snapshot)?;
    let reservation = memory.reserve(allocation_bound).await;
    let schema = build_output_schema(discovered_schema, columns, changelog_snapshot)?;
    let exact = output_schema_heap_bytes(&schema)?;
    anyhow::ensure!(
        exact <= allocation_bound,
        "MySQL snapshot Arrow schema used {exact} bytes after pre-admitting only {allocation_bound} bytes"
    );
    let _ = reservation.shrink_to(exact);
    anyhow::ensure!(
        reservation.bytes() == exact,
        "MySQL snapshot Arrow schema reservation did not shrink to its exact retained size"
    );
    Ok((schema, reservation))
}

fn user_arrays<R: RowValueAccess>(
    columns: &[ColumnPlan],
    rows: &[R],
) -> anyhow::Result<Vec<ArrayRef>> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| column_array(rows, index, column))
        .collect()
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

pub fn optional_value_column_array(
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
        MySqlColumnKind::UInt16 | MySqlColumnKind::EnumOrdinal => {
            integer_array!(value_u64, u16, UInt16Array)
        }
        MySqlColumnKind::Int32 => integer_array!(value_i64, i32, Int32Array),
        MySqlColumnKind::UInt32 => integer_array!(value_u64, u32, UInt32Array),
        MySqlColumnKind::Int64 => integer_array!(value_i64, i64, Int64Array),
        MySqlColumnKind::UInt64 | MySqlColumnKind::SetBits => {
            integer_array!(value_u64, u64, UInt64Array)
        }
        MySqlColumnKind::Float32 => integer_array!(value_f64, f32, Float32Array),
        MySqlColumnKind::Float64 => integer_array!(value_f64, f64, Float64Array),
        MySqlColumnKind::Binary | MySqlColumnKind::TextBytes => {
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
        MySqlColumnKind::Utf8
        | MySqlColumnKind::Json
        | MySqlColumnKind::DecimalText
        | MySqlColumnKind::DateText
        | MySqlColumnKind::DateTimeText
        | MySqlColumnKind::TimestampText
        | MySqlColumnKind::TimeText
        | MySqlColumnKind::YearText => {
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
    })
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
