use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio_postgres::{Client, Column, Statement};

use super::copy_out::{CopyOutReader, RawCopyRow};
use crate::connectors::postgres::common::{
    postgres_requires_text_projection, postgres_to_arrow, quote_identifier, PostgresCopyFormat,
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

pub struct PostgresSource {
    client: Client,

    _exported_snapshot: Arc<ExportedSnapshot>,

    partition_id: i64,

    table: TableConfig,

    replica_identity_full: bool,

    statement: Statement,

    copy: Option<CopyOutReader>,

    schema: DatasetSchema,

    database: String,

    batch_rows: usize,

    copy_format: PostgresCopyFormat,

    snapshot_lsn: i64,

    snapshot_transaction_id: u64,

    snapshot_timestamp_ns: i64,

    offset: i64,

    copy_done: bool,

    finished: bool,

    counters: Arc<SourceCounters>,

    changelog_snapshot: bool,
}

impl PostgresSource {
    pub async fn new(
        client: Client,
        exported_snapshot: Arc<ExportedSnapshot>,
        partition_id: i64,
        discovered: DiscoveredTable,
        database: String,
        batch_rows: usize,
        copy_format: PostgresCopyFormat,
        counters: Arc<SourceCounters>,
        changelog_snapshot: bool,
    ) -> anyhow::Result<Self> {
        exported_snapshot.import(&client).await?;
        let current = discover_table(&client, discovered.config.clone()).await?;
        if !discovered_schema_matches(&current.schema, &discovered.schema)
            || current.type_oids != discovered.type_oids
            || current.replica_identity_full != discovered.replica_identity_full
        {
            return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "PostgreSQL table '{}.{}' schema differs from the exported snapshot discovery",
                discovered.config.schema,
                discovered.config.name,
            ))
            .into());
        }
        let replica_identity_full = discovered.replica_identity_full;
        let table = discovered.config;
        let schema = incoming_user_schema(&discovered.schema);
        let metadata = client
            .prepare(&format!(
                "SELECT * FROM {}.{} LIMIT 0",
                quote_identifier(&table.schema),
                quote_identifier(&table.name)
            ))
            .await?;
        let projection = source_select_projection(metadata.columns())?;
        let select = format!(
            "SELECT {projection} FROM {}.{}",
            quote_identifier(&table.schema),
            quote_identifier(&table.name)
        );
        let statement = client.prepare(&format!("{select} LIMIT 0")).await?;
        let format = match copy_format {
            PostgresCopyFormat::Binary => "BINARY",
            PostgresCopyFormat::Text => "TEXT",
        };
        let stream = client
            .copy_out(&format!("COPY ({select}) TO STDOUT (FORMAT {format})"))
            .await?;
        let copy = CopyOutReader::new(stream, copy_format, statement.columns().len());
        Ok(Self {
            client,
            _exported_snapshot: exported_snapshot.clone(),
            partition_id,
            table,
            replica_identity_full,
            statement,
            copy: Some(copy),
            schema,
            database,
            batch_rows,
            copy_format,
            snapshot_lsn: exported_snapshot.lsn,
            snapshot_transaction_id: exported_snapshot.transaction_id,
            snapshot_timestamp_ns: exported_snapshot.timestamp_ns,
            offset: 0,
            copy_done: false,
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

pub(super) fn source_select_projection(columns: &[Column]) -> anyhow::Result<String> {
    columns
        .iter()
        .map(|column| source_column_expression(column.name(), column.type_()))
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|columns| columns.join(", "))
}

pub(super) fn source_column_expression(
    name: &str,
    data_type: &tokio_postgres::types::Type,
) -> anyhow::Result<String> {
    postgres_to_arrow(data_type)?;
    let name = quote_identifier(name);
    if postgres_requires_text_projection(data_type) {
        Ok(format!("{name}::text AS {name}"))
    } else {
        Ok(name)
    }
}

impl Source for PostgresSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(SourceBatch::Finished);
            }
            let mut rows = Vec::with_capacity(self.batch_rows);
            while rows.len() < self.batch_rows && !self.copy_done {
                let copy = self.copy.as_mut().ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "PostgreSQL COPY reader is unavailable before snapshot completion"
                    ))
                })?;
                match copy.next_row(&self.counters).await? {
                    Some(row) => rows.push(row),
                    None => {
                        self.copy_done = true;
                        self.copy = None;
                    }
                }
            }
            if rows.is_empty() {
                self.client
                    .batch_execute("COMMIT")
                    .await
                    .map_err(|error| DataPlaneFailure::retryable(error.into()))?;
                self.finished = true;
                return Ok(SourceBatch::Finished);
            }
            let source_rows = rows.len() as u64;
            let batch = rows_to_batch(
                &self.schema,
                &self.statement,
                &rows,
                self.copy_format,
                self.offset,
                self.partition_id,
                SnapshotMetadata {
                    database: &self.database,
                    schema: &self.table.schema,
                    table: &self.table.name,
                    lsn: self.snapshot_lsn,
                    transaction_id: self.snapshot_transaction_id,
                    timestamp_ns: self.snapshot_timestamp_ns,
                },
                self.replica_identity_full,
                self.changelog_snapshot,
            )
            .map_err(DataPlaneFailure::fatal)?;
            self.offset = self
                .offset
                .checked_add(
                    i64::try_from(rows.len())
                        .map_err(|error| DataPlaneFailure::fatal(error.into()))?,
                )
                .ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!("PostgreSQL source offset overflow"))
                })?;
            self.counters.add_records(source_rows);
            let system_kinds = snapshot_system_columns(self.changelog_snapshot);
            let system_start = batch.schema().fields().len() - system_kinds.len();
            Ok(SourceBatch::Typed {
                tables: vec![TableData::new(
                    Arc::from(self.table.name.as_str()),
                    false,
                    batch,
                    routing_system_columns(system_start, system_kinds),
                )],
                source_rows,
                commit_marker: Some(CommitMarker::new(self.offset)),
                memory: Vec::new(),
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
            self.copy = None;
            if self.finished {
                return Ok(());
            }
            self.client
                .batch_execute("ROLLBACK")
                .await
                .map_err(|error| DataPlaneFailure::retryable(error.into()))?;
            self.finished = true;
            Ok(())
        })
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

fn snapshot_system_columns(changelog: bool) -> &'static [SystemColumnKind] {
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
        let data_type = postgres_to_arrow(column.type_())?;
        anyhow::ensure!(
            column.name() == discovered.name && data_type == discovered.data_type,
            "PostgreSQL query schema drifted at column '{}': discovered {:?}, query returned {:?}",
            column.name(),
            discovered.data_type,
            data_type
        );
        fields.push(source_user_field(discovered, changelog_snapshot));
        arrays.push(column_array(rows, index, column.type_(), copy_format)?);
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
                std::iter::repeat(changed_mask.as_slice()).take(len),
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

fn column_array(
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
                decoded <= u16::from(u8::MAX),
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
