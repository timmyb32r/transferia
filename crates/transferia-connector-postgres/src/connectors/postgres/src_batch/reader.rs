use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio_postgres::{Column, Statement};
use tracing::Instrument as _;

use super::copy_out::{CopyOutReader, RawCopyRow};
use super::planning::PreparedTable;
use super::queue::{ClaimedChunk, LaneLease, SnapshotQueue};
use crate::connectors::postgres::common::{
    postgres_requires_text_projection, postgres_to_arrow, quote_identifier, OwnedConnection, PostgresCopyFormat,
};
use crate::connectors::postgres::source::{
    discover_table, incoming_user_schema, old_key_column_name, old_value_column_name,
    DiscoveredTable, TableConfig, POSTGRES_REPLICATION_SYSTEM_COLUMNS,
    POSTGRES_SOURCE_METADATA_COLUMNS,
};
use crate::connectors::postgres::src_batch::ExportedSnapshot;
use crate::connectors::postgres::temporal::{
    parse_date, parse_timestamp, postgres_date_to_unix_days, postgres_timestamp_to_unix_micros,
};
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::schema::{
    SchemaColumn, META_CHANGE_OPERATION, META_OLD_KEY_OF, META_OLD_VALUE_OF,
};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};
use transferia_connector_support::external_request::observe_external_request;

pub struct PostgresSource {
    client: OwnedConnection,

    _exported_snapshot: Arc<ExportedSnapshot>,

    partition_id: i64,

    table: TableConfig,

    replica_identity_full: bool,

    statement: Statement,

    copy: Option<CopyOutReader>,

    active: Option<ActiveRead>,

    prepared: Arc<PreparedTable>,

    lease: LaneLease,

    select: String,

    membership_predicate: String,

    span: tracing::Span,

    schema: DatasetSchema,

    database: String,

    batch_rows: usize,

    copy_format: PostgresCopyFormat,

    snapshot_lsn: i64,

    snapshot_transaction_id: u64,

    snapshot_timestamp_ns: i64,

    offset: i64,

    finished: bool,

    counters: Arc<SourceCounters>,

    changelog_snapshot: bool,
}

struct ActiveRead {
    task: ClaimedChunk,
    started: Instant,
    query_start: Duration,
    bytes: u64,
    ended: bool,
}

impl PostgresSource {
    pub(in crate::connectors::postgres) async fn new(
        client: OwnedConnection,
        exported_snapshot: Arc<ExportedSnapshot>,
        partition_id: i64,
        queue: Arc<SnapshotQueue>,
        lane_index: u32,
        prepared: Arc<PreparedTable>,
        discovered: DiscoveredTable,
        database: String,
        batch_rows: usize,
        copy_format: PostgresCopyFormat,
        unsupported_types: crate::connectors::postgres::source::UnsupportedTypePolicy,
        counters: Arc<SourceCounters>,
        changelog_snapshot: bool,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(batch_rows > 0, "PostgreSQL batch_rows must be positive");
        let lease = queue.open_lane(usize::try_from(lane_index)?).map_err(DataPlaneFailure::fatal)?;
        observe_external_request("postgres", "import_snapshot_reader", exported_snapshot.import(&client)).await?;
        exported_snapshot.verify_relation(&discovered.config.schema, &discovered.config.name, discovered.relation_oid).map_err(DataPlaneFailure::fatal)?;
        let current = observe_external_request("postgres", "validate_snapshot_schema", discover_table(&client, discovered.config.clone(), unsupported_types))
            .await.map_err(|error| sanitized_request_failure("validate_snapshot_schema", error, false))?;
        if !discovered_schema_matches(&current.schema, &discovered.schema)
            || current.type_oids != discovered.type_oids
            || current.replica_identity_full != discovered.replica_identity_full
            || current.relation_oid != discovered.relation_oid
        {
            return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "PostgreSQL table '{}.{}' schema differs from the exported snapshot discovery",
                discovered.config.schema,
                discovered.config.name,
            ))
            .into());
        }
        // The keeper retains the relation guards for the whole epoch. Once
        // checked under this imported snapshot, physical identity cannot change
        // between this lane's task requests without violating those guards.
        prepared.validate_identity(&client).await.map_err(DataPlaneFailure::fatal)?;
        let replica_identity_full = discovered.replica_identity_full;
        let table = discovered.config;
        let schema = incoming_user_schema(&discovered.schema);
        let metadata = observe_external_request("postgres", "prepare_snapshot_columns", client
            .prepare(&format!(
                "SELECT * FROM {}.{} LIMIT 0",
                quote_identifier(&table.schema),
                quote_identifier(&table.name)
            )))
            .await.map_err(|error| sanitized_request_failure("prepare_snapshot_columns", error.into(), true))?;
        let projection = source_select_projection(metadata.columns(), unsupported_types)?;
        let query_scope = exported_snapshot.query_scope(&table.schema, &table.name, discovered.relation_oid).map_err(DataPlaneFailure::fatal)?;
        let select = format!(
            "SELECT {projection} FROM {}",
            query_scope.from_sql(&table.schema, &table.name)
        );
        let membership_predicate = query_scope.membership_predicate();
        let statement = observe_external_request("postgres", "prepare_snapshot_projection", client.prepare(&format!("{select} LIMIT 0")))
            .await.map_err(|error| sanitized_request_failure("prepare_snapshot_projection", error.into(), true))?;
        let offset = lease.offset().map_err(DataPlaneFailure::fatal)?;
        let span = tracing::info_span!(target: "transferia.postgres.snapshot", "postgres_snapshot_lane",
            schema = %table.schema, table = %table.name, partition = partition_id,
            lane = lane_index, snapshot_transaction = exported_snapshot.transaction_id);
        Ok(Self {
            client,
            _exported_snapshot: exported_snapshot.clone(),
            partition_id,
            table,
            replica_identity_full,
            statement,
            copy: None,
            active: None,
            prepared,
            lease,
            select,
            membership_predicate,
            span,
            schema,
            database,
            batch_rows,
            copy_format,
            snapshot_lsn: exported_snapshot.lsn,
            snapshot_transaction_id: exported_snapshot.transaction_id,
            snapshot_timestamp_ns: exported_snapshot.timestamp_ns,
            offset,
            finished: false,
            counters,
            changelog_snapshot,
        })
    }
}

pub(super) fn discovered_schema_matches(current: &DatasetSchema, expected: &DatasetSchema) -> bool {
    current.columns.len() == expected.columns.len()
        && current
            .columns
            .iter()
            .zip(&expected.columns)
            .all(|(current, expected)| {
                current.name == expected.name
                    && current.data_type == expected.data_type
                    && current.nullable == expected.nullable
                    && current.primary_key == expected.primary_key
                    && current.low_cardinality == expected.low_cardinality
                    && current.max_length == expected.max_length
                    && current.arrow_extension_name == expected.arrow_extension_name
                    && current.system_role == expected.system_role
                    && current.old_value_of == expected.old_value_of
                    && current.old_key_of == expected.old_key_of
            })
}

/// Build the exact snapshot projection from PostgreSQL statement metadata.
/// The SQL expressions preserve supported values through the source's chosen
/// type policy. Evaluators must reuse this function instead of measuring native
/// COPY representations which the production reader does not consume.
pub fn source_select_projection(
    columns: &[Column],
    policy: crate::connectors::postgres::source::UnsupportedTypePolicy,
) -> anyhow::Result<String> {
    columns
        .iter()
        .map(|column| source_column_expression(column.name(), column.type_(), policy))
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|columns| columns.join(", "))
}

pub(in crate::connectors::postgres) fn source_column_expression(
    name: &str,
    data_type: &tokio_postgres::types::Type,
    policy: crate::connectors::postgres::source::UnsupportedTypePolicy,
) -> anyhow::Result<String> {
    policy.arrow_type(data_type)?;
    let name = quote_identifier(name);
    if postgres_requires_text_projection(data_type) || postgres_to_arrow(data_type).is_err() {
        // The declared source policy means PostgreSQL's text representation,
        // regardless of user-defined types earlier in the session search_path.
        Ok(format!("{name}::pg_catalog.text AS {name}"))
    } else {
        Ok(name)
    }
}

impl Source for PostgresSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        let span = self.span.clone();
        Box::pin(async move {
            let result = self.read_next().await;
            match result {
                Err(error) if !self.lease.retry_allowed() => Err(DataPlaneFailure::fatal(
                    error.into_source().context("snapshot epoch cannot retry after publishing incomplete COPY work"),
                )),
                result => result,
            }
        }.instrument(span))
    }

    fn commit_offsets<'a>(
        &'a mut self,
        markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        let span = self.span.clone();
        Box::pin(async move { self.lease.acknowledge(markers).map_err(DataPlaneFailure::fatal) }.instrument(span))
    }

    fn shutdown(&mut self) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        let span = self.span.clone();
        Box::pin(async move {
            self.copy = None;
            self.active = None;
            // ROLLBACK would queue behind abandoned COPY/lock work. Cancel the
            // PostgreSQL query before closing its socket; TCP closure alone
            // need not wake a backend blocked on a relation lock. Normal EOF
            // already issued a checked COMMIT and only needs a clean close.
            let cleanup = observe_external_request("postgres", "close_snapshot_reader", async {
                if self.finished { self.client.close().await } else { self.client.cancel().await }
            }).await.map_err(|error| sanitized_request_failure("close_snapshot_reader", error, false));
            self.finished = true;
            self.lease.close().map_err(DataPlaneFailure::fatal)?;
            cleanup
        }.instrument(span))
    }
}

impl PostgresSource {
    async fn read_next(&mut self) -> transferia_core::failure::DataPlaneResult<SourceBatch> {
        if self.finished { return Ok(SourceBatch::Finished); }
        if self.active.as_ref().is_some_and(|active| active.ended) {
            return self.finish_task().await;
        }
        if self.active.is_none() {
            let Some(task) = self.lease.claim().map_err(DataPlaneFailure::fatal)? else {
                observe_external_request("postgres", "commit_snapshot_reader", self.client.batch_execute("COMMIT"))
                    .await.map_err(|error| sanitized_request_failure("commit_snapshot_reader", error.into(), true))?;
                self.client.disarm_cancellation();
                self.finished = true;
                // The pipeline closes parser input and drains outstanding task
                // markers after this EOF, then calls shutdown to close the lease.
                return Ok(SourceBatch::Finished);
            };
            let predicate = self.prepared.predicate(&task.chunk).map_err(DataPlaneFailure::fatal)?;
            let format = match self.copy_format { PostgresCopyFormat::Binary => "BINARY", PostgresCopyFormat::Text => "TEXT" };
            let query = format!("COPY ({} WHERE ({}) AND ({predicate})) TO STDOUT (FORMAT {format})", self.select, self.membership_predicate);
            let started = Instant::now();
            let stream = observe_external_request("postgres", "start_snapshot_chunk", self.client.copy_out(&query))
                .await.map_err(|error| sanitized_request_failure("start_snapshot_chunk", error.into(), true))?;
            tracing::debug!(target: "transferia.postgres.snapshot", schema = %self.table.schema, table = %self.table.name, partition = self.partition_id, task = task.id, "snapshot chunk COPY started");
            self.active = Some(ActiveRead { task, started, query_start: started.elapsed(), bytes: 0, ended: false });
            self.copy = Some(CopyOutReader::new(stream, self.copy_format, self.statement.columns().len()));
        }
        let rows = observe_external_request("postgres", "read_snapshot_chunk_batch", async {
            let mut rows = Vec::with_capacity(self.batch_rows);
            while rows.len() < self.batch_rows {
                let copy = self.copy.as_mut().ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("PostgreSQL COPY reader is missing for an owned task")))?;
                let row = copy.next_row(&self.counters).await.map_err(|error| {
                    let retryable = error.is_retryable();
                    sanitized_request_failure("read_snapshot_chunk", error.into_source(), retryable)
                })?;
                let Some(row) = row else {
                    self.copy = None;
                    self.active.as_mut().ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("snapshot task disappeared while reading COPY")))?.ended = true;
                    break;
                };
                rows.push(row);
            }
            Ok::<_, DataPlaneFailure>(rows)
        }).await?;
        if rows.is_empty() { return self.finish_task().await; }
        let source_rows = u64::try_from(rows.len()).map_err(|error| DataPlaneFailure::fatal(error.into()))?;
        let bytes = rows.iter().flat_map(|row| &row.fields).flatten().try_fold(0_u64, |total, field| {
            total.checked_add(u64::try_from(field.len()).ok()?)
        }).ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("snapshot payload byte count overflow")))?;
        let batch = rows_to_batch(
            &self.schema, &self.statement, &rows, self.copy_format, self.offset, self.partition_id,
            SnapshotMetadata {
                database: &self.database, schema: &self.table.schema, table: &self.table.name,
                lsn: self.snapshot_lsn, transaction_id: self.snapshot_transaction_id,
                timestamp_ns: self.snapshot_timestamp_ns,
            }, self.replica_identity_full, self.changelog_snapshot,
        ).map_err(DataPlaneFailure::fatal)?;
        let active = self.active.as_mut().ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("snapshot task disappeared before publication")))?;
        let total_bytes = active.bytes.checked_add(bytes).ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("snapshot payload byte count overflow")))?;
        let (offset, marker) = self.lease.emit_rows(&active.task, self.offset, source_rows).map_err(DataPlaneFailure::fatal)?;
        active.bytes = total_bytes;
        self.offset = offset;
        self.counters.add_records(source_rows);
        let system_kinds = snapshot_system_columns(self.changelog_snapshot);
        let system_start = batch.schema().fields().len() - system_kinds.len();
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(Arc::from(self.table.name.as_str()), false, batch, routing_system_columns(system_start, system_kinds))
                .with_namespace(Arc::from(self.table.schema.as_str()))],
            source_rows, commit_marker: Some(marker), memory: Vec::new(),
        })
    }

    async fn finish_task(&mut self) -> transferia_core::failure::DataPlaneResult<SourceBatch> {
        let active = self.active.as_ref().ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("snapshot task EOF has no owned task")))?;
        if !active.ended { return Err(DataPlaneFailure::fatal(anyhow::anyhow!("snapshot task is not at COPY EOF"))); }
        // tokio-postgres COPY streams expose CopyDone before final SQL cleanup.
        // Within this explicit transaction, a checked next command detects an
        // aborted statement before we publish the task-completion marker.
        observe_external_request("postgres", "finish_snapshot_chunk", self.client.simple_query("SELECT 1"))
            .await.map_err(|error| sanitized_request_failure("finish_snapshot_chunk", error.into(), true))?;
        let marker = self.lease.finish_read(&active.task, active.started.elapsed(), active.query_start, active.bytes)
            .map_err(DataPlaneFailure::fatal)?;
        self.active = None;
        Ok(SourceBatch::Typed { tables: Vec::new(), source_rows: 0, commit_marker: Some(marker), memory: Vec::new() })
    }
}

fn sanitized_request_failure(operation: &'static str, error: anyhow::Error, retryable: bool) -> DataPlaneFailure {
    let code = error.chain().find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .and_then(tokio_postgres::Error::as_db_error).map(|database| database.code().code());
    let message = match code {
        Some(code) => anyhow::anyhow!("PostgreSQL snapshot request '{operation}' failed (SQLSTATE {code})"),
        None => anyhow::anyhow!("PostgreSQL snapshot request '{operation}' failed"),
    };
    // Retrying malformed SQL, permissions, schema/type changes, or an expired
    // snapshot cannot repair the same immutable task. Connection failures and
    // explicitly transient server conditions may retry before publication.
    let retryable = retryable && code.is_none_or(|code| {
        code.starts_with("08") || matches!(code, "40001" | "40P01" | "55P03" | "57014" | "57P01" | "57P02" | "57P03" | "53300")
    });
    if retryable { DataPlaneFailure::retryable(message) } else { DataPlaneFailure::fatal(message) }
}

#[cfg(test)]
#[path = "tests/reader.rs"]
mod tests;

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

const fn snapshot_system_columns(changelog: bool) -> &'static [SystemColumnKind] {
    if changelog {
        POSTGRES_REPLICATION_SYSTEM_COLUMNS
    } else {
        const SNAPSHOT: &[SystemColumnKind] = &[
            SystemColumnKind::Topic,
            SystemColumnKind::Partition,
            SystemColumnKind::Offset,
            SystemColumnKind::MessageIndex,
        ];
        SNAPSHOT
    }
}

fn rows_to_batch(
    discovered_schema: &DatasetSchema,
    statement: &Statement,
    rows: &[RawCopyRow],
    copy_format: PostgresCopyFormat,
    start_offset: i64,
    partition_id: i64,
    snapshot: SnapshotMetadata<'_>,
    replica_identity_full: bool,
    changelog_snapshot: bool,
) -> anyhow::Result<RecordBatch> {
    let old_columns = if !changelog_snapshot {
        0
    } else if replica_identity_full {
        discovered_schema.columns.len()
    } else {
        discovered_schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .count()
    };
    let system_columns = snapshot_system_columns(changelog_snapshot);
    let output_columns = statement
        .columns()
        .len()
        .checked_add(old_columns)
        .and_then(|count| count.checked_add(POSTGRES_SOURCE_METADATA_COLUMNS.len()))
        .and_then(|count| count.checked_add(system_columns.len()))
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL source column count overflow"))?;
    let mut fields = Vec::with_capacity(output_columns);
    let mut arrays = Vec::with_capacity(output_columns);
    anyhow::ensure!(
        discovered_schema.columns.len() == statement.columns().len(),
        "PostgreSQL query schema has {} columns, discovery declared {}",
        statement.columns().len(),
        discovered_schema.columns.len()
    );
    for (index, (column, discovered)) in statement
        .columns()
        .iter()
        .zip(&discovered_schema.columns)
        .enumerate()
    {
        fields.push(source_user_field(discovered, changelog_snapshot));
        arrays.push(discovered_column_array(rows, index, column, discovered, copy_format)?);
    }
    let len = rows.len();
    let len_i64 = i64::try_from(len)?;
    if changelog_snapshot {
        let old = discovered_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| replica_identity_full || column.primary_key);
        for (index, column) in old {
            let (name, metadata) = if replica_identity_full {
                (
                    old_value_column_name(index),
                    HashMap::from([(META_OLD_VALUE_OF.to_owned(), column.name.clone())]),
                )
            } else {
                (
                    old_key_column_name(index),
                    HashMap::from([(META_OLD_KEY_OF.to_owned(), column.name.clone())]),
                )
            };
            fields.push(Field::new(name, column.data_type.clone(), true).with_metadata(metadata));
            arrays.push(new_null_array(&column.data_type, len));
        }
    }
    fields.extend(snapshot_metadata_fields());
    fields.extend(system_columns.iter().map(|kind| {
        let field = Field::new(kind.default_name(), kind.data_type(), false);
        if *kind == SystemColumnKind::ChangeOperation {
            field.with_metadata(HashMap::from([(
                META_CHANGE_OPERATION.to_owned(),
                "true".to_owned(),
            )]))
        } else {
            field
        }
    }));
    arrays.extend(snapshot_metadata_arrays(snapshot, len));
    let changed_mask = full_changed_columns_mask(discovered_schema.columns.len());
    for kind in system_columns {
        arrays.push(match kind {
            SystemColumnKind::Topic => {
                Arc::new(StringArray::from(vec!["postgres"; len])) as ArrayRef
            }
            SystemColumnKind::Partition => {
                Arc::new(Int64Array::from(vec![partition_id; len])) as ArrayRef
            }
            SystemColumnKind::Offset => {
                Arc::new(Int64Array::from(vec![snapshot.lsn; len])) as ArrayRef
            }
            SystemColumnKind::MessageIndex => {
                Arc::new(UInt64Array::from_iter_values(
                    u64::try_from(start_offset)?
                        ..u64::try_from(start_offset.checked_add(len_i64).ok_or_else(|| {
                            anyhow::anyhow!("PostgreSQL source offset overflow")
                        })?)?,
                )) as ArrayRef
            }
            SystemColumnKind::ChangeOperation => Arc::new(StringArray::from(vec![
                transferia_core::ChangeOperation::SnapshotRead.code();
                len
            ])) as ArrayRef,
            SystemColumnKind::ChangedColumns => Arc::new(BinaryArray::from_iter_values(
                std::iter::repeat_n(changed_mask.as_slice(), len),
            )) as ArrayRef,
            SystemColumnKind::WriteTimestampMs => {
                anyhow::bail!("PostgreSQL snapshot has no write timestamp")
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

pub(super) fn source_user_field(column: &SchemaColumn, nullable: bool) -> Field {
    Field::new(
        column.name.as_str(),
        column.data_type.clone(),
        nullable || column.nullable,
    )
    .with_metadata(column.arrow_metadata())
}

#[derive(Clone, Copy)]
struct SnapshotMetadata<'a> {
    database: &'a str,
    schema: &'a str,
    table: &'a str,
    lsn: i64,
    transaction_id: u64,
    timestamp_ns: i64,
}

fn snapshot_metadata_fields() -> Vec<Field> {
    POSTGRES_SOURCE_METADATA_COLUMNS
        .iter()
        .map(|column| {
            Field::new(column.name, column.data_type.clone(), false).with_metadata(
                SchemaColumn::new(column.name.to_owned(), column.data_type.clone(), false)
                    .with_system_role(column.role)
                    .arrow_metadata(),
            )
        })
        .collect()
}

fn snapshot_metadata_arrays(snapshot: SnapshotMetadata<'_>, len: usize) -> Vec<ArrayRef> {
    let timestamp_us = snapshot.timestamp_ns / 1_000;
    let timestamp_ms = snapshot.timestamp_ns / 1_000_000;
    vec![
        Arc::new(StringArray::from(vec![snapshot.database; len])) as ArrayRef,
        Arc::new(StringArray::from(vec![snapshot.schema; len])) as ArrayRef,
        Arc::new(StringArray::from(vec![snapshot.table; len])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![snapshot.transaction_id; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![snapshot.timestamp_ns; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![snapshot.timestamp_ns; len])) as ArrayRef,
    ]
}

/// The query uses text only for types deliberately projected as text. Its wire
/// descriptor cannot carry NUMERIC typmods; authoritative discovery owns them.
pub(super) fn discovered_column_array(
    rows: &[RawCopyRow], index: usize, column: &Column, discovered: &SchemaColumn,
    copy_format: PostgresCopyFormat,
) -> anyhow::Result<ArrayRef> {
    let wire_type = postgres_to_arrow(column.type_())?;
    let decimal = matches!(discovered.data_type, DataType::Decimal128(..));
    anyhow::ensure!(column.name() == discovered.name && (wire_type == discovered.data_type
        || (decimal && *column.type_() == tokio_postgres::types::Type::TEXT)),
        "PostgreSQL query schema drifted at column '{}': discovered {:?}, query returned {:?}",
        column.name(), discovered.data_type, wire_type);
    if let DataType::Decimal128(precision, scale) = discovered.data_type {
        let values = rows.iter().map(|row| row.fields[index].as_deref()
            .map(|value| crate::connectors::postgres::numeric::parse(decode_string(value)?, precision, scale))
            .transpose()).collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Arc::new(Decimal128Array::from(values).with_precision_and_scale(precision, scale)?))
    } else { column_array(rows, index, column.type_(), copy_format) }
}

pub(super) fn column_array(
    rows: &[RawCopyRow],
    index: usize,
    data_type: &tokio_postgres::types::Type,
    copy_format: PostgresCopyFormat,
) -> anyhow::Result<ArrayRef> {
    macro_rules! primitive {
        ($ty:ty, $array:ty, $decode:ident) => {
            Arc::new(<$array>::from(
                rows.iter()
                    .map(|row| {
                        row.fields[index]
                            .as_deref()
                            .map(|value| $decode(value, copy_format))
                            .transpose()
                    })
                    .collect::<anyhow::Result<Vec<Option<$ty>>>>()?,
            )) as ArrayRef
        };
    }
    Ok(match *data_type {
        tokio_postgres::types::Type::BOOL => primitive!(bool, BooleanArray, decode_bool),
        tokio_postgres::types::Type::CHAR => primitive!(i8, Int8Array, decode_i8),
        tokio_postgres::types::Type::INT2 => primitive!(i16, Int16Array, decode_i16),
        tokio_postgres::types::Type::INT4 => primitive!(i32, Int32Array, decode_i32),
        tokio_postgres::types::Type::INT8 => primitive!(i64, Int64Array, decode_i64),
        tokio_postgres::types::Type::OID => primitive!(u32, UInt32Array, decode_u32),
        tokio_postgres::types::Type::FLOAT4 => primitive!(f32, Float32Array, decode_f32),
        tokio_postgres::types::Type::FLOAT8 => primitive!(f64, Float64Array, decode_f64),
        tokio_postgres::types::Type::BYTEA => {
            let values = rows
                .iter()
                .map(|row| {
                    row.fields[index]
                        .as_deref()
                        .map(|value| decode_binary(value, copy_format))
                        .transpose()
                })
                .collect::<anyhow::Result<Vec<Option<Vec<u8>>>>>()?;
            Arc::new(BinaryArray::from(
                values.iter().map(Option::as_deref).collect::<Vec<_>>(),
            )) as ArrayRef
        }
        tokio_postgres::types::Type::TEXT
        | tokio_postgres::types::Type::VARCHAR
        | tokio_postgres::types::Type::BPCHAR
        | tokio_postgres::types::Type::NAME => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.fields[index].as_deref().map(decode_string).transpose())
                .collect::<anyhow::Result<Vec<Option<&str>>>>()?,
        )) as ArrayRef,
        tokio_postgres::types::Type::DATE => primitive!(i32, Date32Array, decode_date),
        tokio_postgres::types::Type::TIMESTAMP => {
            primitive!(i64, TimestampMicrosecondArray, decode_timestamp)
        }
        tokio_postgres::types::Type::TIMESTAMPTZ => {
            let values = rows
                .iter()
                .map(|row| {
                    row.fields[index]
                        .as_deref()
                        .map(|value| decode_timestamptz(value, copy_format))
                        .transpose()
                })
                .collect::<anyhow::Result<Vec<Option<i64>>>>()?;
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef
        }
        _ => anyhow::bail!("unsupported PostgreSQL type '{}'", data_type.name()),
    })
}

pub(super) fn decode_date(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<i32> {
    match format {
        PostgresCopyFormat::Binary => {
            postgres_date_to_unix_days(decode_i32(value, PostgresCopyFormat::Binary)?)
        }
        PostgresCopyFormat::Text => parse_date(decode_string(value)?),
    }
}

pub(super) fn decode_timestamp(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<i64> {
    match format {
        PostgresCopyFormat::Binary => {
            postgres_timestamp_to_unix_micros(decode_i64(value, PostgresCopyFormat::Binary)?)
        }
        PostgresCopyFormat::Text => parse_timestamp(decode_string(value)?, false),
    }
}

pub(super) fn decode_timestamptz(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<i64> {
    match format {
        PostgresCopyFormat::Binary => {
            postgres_timestamp_to_unix_micros(decode_i64(value, PostgresCopyFormat::Binary)?)
        }
        PostgresCopyFormat::Text => parse_timestamp(decode_string(value)?, true),
    }
}

fn decode_bool(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<bool> {
    match format {
        PostgresCopyFormat::Binary => {
            anyhow::ensure!(value.len() == 1, "invalid PostgreSQL binary boolean length");
            match value[0] {
                0 => Ok(false),
                1 => Ok(true),
                other => anyhow::bail!("invalid PostgreSQL binary boolean value {other}"),
            }
        }
        PostgresCopyFormat::Text => match value {
            b"t" => Ok(true),
            b"f" => Ok(false),
            _ => anyhow::bail!("invalid PostgreSQL text boolean value"),
        },
    }
}

pub(super) fn decode_i8(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<i8> {
    match format {
        PostgresCopyFormat::Binary => {
            anyhow::ensure!(value.len() == 1, "invalid PostgreSQL binary char length");
            Ok(i8::from_be_bytes([value[0]]))
        }
        PostgresCopyFormat::Text if value.is_empty() => Ok(0),
        PostgresCopyFormat::Text if value.len() == 1 => Ok(i8::from_ne_bytes([value[0]])),
        PostgresCopyFormat::Text
            if value.len() == 4
                && value[0] == b'\\'
                && value[1..].iter().all(|byte| matches!(byte, b'0'..=b'7')) =>
        {
            let decoded = value[1..]
                .iter()
                .fold(0_u16, |decoded, byte| decoded * 8 + u16::from(*byte - b'0'));
            anyhow::ensure!(
                u8::try_from(decoded).is_ok(),
                "invalid PostgreSQL text char octal value"
            );
            Ok(i8::from_ne_bytes([u8::try_from(decoded)?]))
        }
        PostgresCopyFormat::Text => anyhow::bail!("invalid PostgreSQL text char value"),
    }
}

macro_rules! fixed_number_decoder {
    ($name:ident, $ty:ty, $length:literal) => {
        fn $name(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<$ty> {
            match format {
                PostgresCopyFormat::Binary => {
                    let bytes: [u8; $length] = value.try_into().map_err(|_| {
                        anyhow::anyhow!(
                            "invalid PostgreSQL binary {} length {}",
                            stringify!($ty),
                            value.len()
                        )
                    })?;
                    Ok(<$ty>::from_be_bytes(bytes))
                }
                PostgresCopyFormat::Text => Ok(decode_string(value)?.parse::<$ty>()?),
            }
        }
    };
}

fixed_number_decoder!(decode_i16, i16, 2);
fixed_number_decoder!(decode_i32, i32, 4);
fixed_number_decoder!(decode_i64, i64, 8);
fixed_number_decoder!(decode_u32, u32, 4);

fn decode_f32(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<f32> {
    match format {
        PostgresCopyFormat::Binary => Ok(f32::from_bits(decode_u32(
            value,
            PostgresCopyFormat::Binary,
        )?)),
        PostgresCopyFormat::Text => parse_float(decode_string(value)?),
    }
}

fn decode_f64(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<f64> {
    match format {
        PostgresCopyFormat::Binary => {
            let bytes: [u8; 8] = value.try_into().map_err(|_| {
                anyhow::anyhow!("invalid PostgreSQL binary f64 length {}", value.len())
            })?;
            Ok(f64::from_bits(u64::from_be_bytes(bytes)))
        }
        PostgresCopyFormat::Text => parse_float(decode_string(value)?),
    }
}

fn parse_float<T>(value: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match value {
        "Infinity" => "inf".parse::<T>().map_err(Into::into),
        "-Infinity" => "-inf".parse::<T>().map_err(Into::into),
        other => other.parse::<T>().map_err(Into::into),
    }
}

fn decode_binary(value: &[u8], format: PostgresCopyFormat) -> anyhow::Result<Vec<u8>> {
    if format == PostgresCopyFormat::Binary {
        return Ok(value.to_vec());
    }
    let hex = value
        .strip_prefix(b"\\x")
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL text bytea is not in forced hex format"))?;
    anyhow::ensure!(
        hex.len() % 2 == 0,
        "PostgreSQL text bytea has an odd number of hex digits"
    );
    hex.chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(text, 16)?)
        })
        .collect()
}

fn decode_string(value: &[u8]) -> anyhow::Result<&str> {
    Ok(std::str::from_utf8(value)?)
}
