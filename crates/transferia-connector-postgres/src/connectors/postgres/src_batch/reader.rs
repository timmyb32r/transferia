use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio_postgres::{Client, Column, Statement};

use crate::connectors::postgres::common::{
    postgres_requires_text_projection, postgres_to_arrow, quote_identifier, PostgresCopyFormat,
};
use crate::connectors::postgres::source::{TableConfig, POSTGRES_SOURCE_METADATA_COLUMNS};
use crate::connectors::postgres::temporal::{
    parse_date, parse_timestamp, postgres_date_to_unix_days,
    postgres_timestamp_to_unix_micros,
};
use crate::metrics::SourceCounters;
use super::copy_out::{CopyOutReader, RawCopyRow};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::schema::SchemaColumn;
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};

pub struct PostgresSource {
    client: Client,
    table: TableConfig,

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
}

impl PostgresSource {
    pub async fn new(
        client: Client,
        table: TableConfig,
        schema: DatasetSchema,
        database: String,
        batch_rows: usize,
        copy_format: PostgresCopyFormat,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
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
        client
            .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        client
            .batch_execute(
                "SET LOCAL DateStyle = 'ISO, YMD';\
                 SET LOCAL IntervalStyle = 'postgres';\
                 SET LOCAL TimeZone = 'UTC';\
                 SET LOCAL bytea_output = 'hex';\
                 SET LOCAL extra_float_digits = 3;",
            )
            .await?;
        let snapshot = client
            .query_one(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, txid_current()::text, (extract(epoch FROM clock_timestamp()) * 1000000000)::bigint",
                &[],
            )
            .await?;
        let snapshot_lsn = snapshot.try_get::<_, i64>(0)?;
        let snapshot_transaction_id = snapshot.try_get::<_, &str>(1)?.parse::<u64>()?;
        let snapshot_timestamp_ns = snapshot.try_get::<_, i64>(2)?;
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
            table,
            statement,
            copy: Some(copy),
            schema,
            database,
            batch_rows,
            copy_format,
            snapshot_lsn,
            snapshot_transaction_id,
            snapshot_timestamp_ns,
            offset: 0,
            copy_done: false,
            finished: false,
            counters,
        })
    }
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
                SnapshotMetadata {
                    database: &self.database,
                    schema: &self.table.schema,
                    table: &self.table.name,
                    lsn: self.snapshot_lsn,
                    transaction_id: self.snapshot_transaction_id,
                    timestamp_ns: self.snapshot_timestamp_ns,
                },
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
            Ok(SourceBatch::Typed {
                tables: vec![TableData::new(
                    Arc::from(self.table.name.as_str()),
                    false,
                    batch,
                    routing_system_columns(
                        self.statement.columns().len() + POSTGRES_SOURCE_METADATA_COLUMNS.len(),
                    ),
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

fn routing_system_columns(base: usize) -> SystemColumns {
    SystemColumns::new(vec![
        SystemColumn {
            kind: SystemColumnKind::Topic,
            name: Arc::from(SystemColumnKind::Topic.default_name()),
            index: base,
        },
        SystemColumn {
            kind: SystemColumnKind::Partition,
            name: Arc::from(SystemColumnKind::Partition.default_name()),
            index: base + 1,
        },
        SystemColumn {
            kind: SystemColumnKind::Offset,
            name: Arc::from(SystemColumnKind::Offset.default_name()),
            index: base + 2,
        },
        SystemColumn {
            kind: SystemColumnKind::MessageIndex,
            name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
            index: base + 3,
        },
    ])
}

fn rows_to_batch(
    discovered_schema: &DatasetSchema,
    statement: &Statement,
    rows: &[RawCopyRow],
    copy_format: PostgresCopyFormat,
    start_offset: i64,
    snapshot: SnapshotMetadata<'_>,
) -> anyhow::Result<RecordBatch> {
    let output_columns = statement
        .columns()
        .len()
        .checked_add(POSTGRES_SOURCE_METADATA_COLUMNS.len() + 4)
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
        fields.push(source_user_field(discovered));
        arrays.push(column_array(rows, index, column.type_(), copy_format)?);
    }
    let len = rows.len();
    let len_i64 = i64::try_from(len)?;
    fields.extend(snapshot_metadata_fields());
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
    arrays.extend(snapshot_metadata_arrays(snapshot, len));
    arrays.extend([
        Arc::new(StringArray::from(vec!["postgres"; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0_i64; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![snapshot.lsn; len])) as ArrayRef,
        Arc::new(UInt64Array::from_iter_values(
            u64::try_from(start_offset)?
                ..u64::try_from(
                    start_offset
                        .checked_add(len_i64)
                        .ok_or_else(|| anyhow::anyhow!("PostgreSQL source offset overflow"))?,
                )?,
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

pub(super) fn source_user_field(column: &SchemaColumn) -> Field {
    Field::new(
        column.name.as_str(),
        column.data_type.clone(),
        column.nullable,
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
                .map(|row| {
                    row.fields[index]
                        .as_deref()
                        .map(decode_string)
                        .transpose()
                })
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
            let decoded = value[1..].iter().fold(0_u16, |decoded, byte| {
                decoded * 8 + u16::from(*byte - b'0')
            });
            anyhow::ensure!(decoded <= u16::from(u8::MAX), "invalid PostgreSQL text char octal value");
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
