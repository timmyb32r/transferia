use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio_postgres::{Client, Column, Row, Statement};

use crate::connectors::postgres::common::{
    postgres_requires_text_projection, postgres_to_arrow, quote_identifier,
};
use crate::connectors::postgres::source::{TableConfig, POSTGRES_SOURCE_METADATA_COLUMNS};
use crate::metrics::SourceCounters;
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
    schema: DatasetSchema,
    database: String,
    snapshot_lsn: i64,
    snapshot_transaction_id: u64,
    snapshot_timestamp_ns: i64,
    offset: i64,
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
        let query = format!(
            "DECLARE transferia_source_cursor NO SCROLL CURSOR FOR SELECT {projection} FROM {}.{}",
            quote_identifier(&table.schema),
            quote_identifier(&table.name)
        );
        client
            .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
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
        client.batch_execute(&query).await?;
        let statement = client
            .prepare(&format!(
                "FETCH FORWARD {batch_rows} FROM transferia_source_cursor"
            ))
            .await?;
        Ok(Self {
            client,
            table,
            statement,
            schema,
            database,
            snapshot_lsn,
            snapshot_transaction_id,
            snapshot_timestamp_ns,
            offset: 0,
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
            let rows = self
                .client
                .query(&self.statement, &[])
                .await
                .map_err(|error| DataPlaneFailure::retryable(error.into()))?;
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
            self.counters
                .add_network_decoded_bytes(batch.get_array_memory_size() as u64);
            Ok(SourceBatch::Typed {
                tables: vec![TableData::new(
                    Arc::from(self.table.name.as_str()),
                    false,
                    batch,
                    routing_system_columns(self.statement.columns().len()),
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
    rows: &[Row],
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
        fields.push(Field::new(column.name(), data_type, discovered.nullable));
        arrays.push(column_array(rows, index, column.type_())?);
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

fn snapshot_metadata_arrays(
    snapshot: SnapshotMetadata<'_>,
    len: usize,
) -> Vec<ArrayRef> {
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
    rows: &[Row],
    index: usize,
    data_type: &tokio_postgres::types::Type,
) -> anyhow::Result<ArrayRef> {
    macro_rules! primitive {
        ($ty:ty, $array:ty) => {
            Arc::new(<$array>::from(
                rows.iter()
                    .map(|row| row.get::<_, Option<$ty>>(index))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    Ok(match *data_type {
        tokio_postgres::types::Type::BOOL => primitive!(bool, BooleanArray),
        tokio_postgres::types::Type::CHAR => primitive!(i8, Int8Array),
        tokio_postgres::types::Type::INT2 => primitive!(i16, Int16Array),
        tokio_postgres::types::Type::INT4 => primitive!(i32, Int32Array),
        tokio_postgres::types::Type::INT8 => primitive!(i64, Int64Array),
        tokio_postgres::types::Type::OID => primitive!(u32, UInt32Array),
        tokio_postgres::types::Type::FLOAT4 => primitive!(f32, Float32Array),
        tokio_postgres::types::Type::FLOAT8 => primitive!(f64, Float64Array),
        tokio_postgres::types::Type::BYTEA => Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row.get::<_, Option<&[u8]>>(index))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        tokio_postgres::types::Type::TEXT
        | tokio_postgres::types::Type::VARCHAR
        | tokio_postgres::types::Type::BPCHAR
        | tokio_postgres::types::Type::NAME => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.get::<_, Option<&str>>(index))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        _ => anyhow::bail!("unsupported PostgreSQL type '{}'", data_type.name()),
    })
}
