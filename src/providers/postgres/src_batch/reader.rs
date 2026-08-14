use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Datelike as _;
use futures_util::future::BoxFuture;
use tokio_postgres::{Client, Row, Statement};

use super::config::TableConfig;
use crate::metrics::SourceCounters;
use crate::pipeline::source::{CommitMarker, Source};
use crate::providers::postgres::common::quote_identifier;
use crate::types::message::SourceBatch;
use crate::types::schema::DatasetSchema;
use crate::types::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use crate::types::table_data::TableData;

pub struct PostgresSource {
    client: Client,
    table: TableConfig,
    statement: Statement,
    schema: DatasetSchema,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

impl PostgresSource {
    pub async fn new(
        client: Client,
        table: TableConfig,
        schema: DatasetSchema,
        batch_rows: usize,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        let query = format!(
            "DECLARE transferia_source_cursor NO SCROLL CURSOR FOR SELECT * FROM {}.{}",
            quote_identifier(&table.schema),
            quote_identifier(&table.name)
        );
        client
            .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
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
            offset: 0,
            finished: false,
            counters,
        })
    }
}

impl Source for PostgresSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(SourceBatch::Finished);
            }
            let rows = self.client.query(&self.statement, &[]).await?;
            if rows.is_empty() {
                self.client.batch_execute("COMMIT").await?;
                self.finished = true;
                return Ok(SourceBatch::Finished);
            }
            let source_rows = rows.len() as u64;
            let batch = rows_to_batch(&self.schema, &self.statement, &rows, self.offset)?;
            self.offset = self
                .offset
                .checked_add(i64::try_from(rows.len())?)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL source offset overflow"))?;
            self.counters.add_messages(source_rows);
            self.counters
                .add_decompressed_bytes(batch.get_array_memory_size() as u64);
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
    ) -> BoxFuture<'a, anyhow::Result<()>> {
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
) -> anyhow::Result<RecordBatch> {
    let mut fields = Vec::with_capacity(statement.columns().len() + 4);
    let mut arrays = Vec::with_capacity(statement.columns().len() + 4);
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
        let data_type = crate::providers::postgres::common::postgres_to_arrow(column.type_())?;
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
        Arc::new(StringArray::from(vec!["postgres"; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0_i64; len])) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            start_offset
                ..start_offset
                    .checked_add(len_i64)
                    .ok_or_else(|| anyhow::anyhow!("PostgreSQL source offset overflow"))?,
        )) as ArrayRef,
        Arc::new(UInt64Array::from(vec![0_u64; len])) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
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
        tokio_postgres::types::Type::INT2 => primitive!(i16, Int16Array),
        tokio_postgres::types::Type::INT4 => primitive!(i32, Int32Array),
        tokio_postgres::types::Type::INT8 => primitive!(i64, Int64Array),
        tokio_postgres::types::Type::FLOAT4 => primitive!(f32, Float32Array),
        tokio_postgres::types::Type::FLOAT8 => primitive!(f64, Float64Array),
        tokio_postgres::types::Type::TEXT
        | tokio_postgres::types::Type::VARCHAR
        | tokio_postgres::types::Type::BPCHAR => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.get::<_, Option<&str>>(index))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        tokio_postgres::types::Type::DATE => Arc::new(Date32Array::from(
            rows.iter()
                .map(|row| {
                    row.get::<_, Option<chrono::NaiveDate>>(index)
                        .map(|date| date.num_days_from_ce() - 719_163)
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        tokio_postgres::types::Type::TIMESTAMP => {
            Arc::new(arrow::array::TimestampMicrosecondArray::from(
                rows.iter()
                    .map(|row| {
                        row.get::<_, Option<chrono::NaiveDateTime>>(index)
                            .map(|value| value.and_utc().timestamp_micros())
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        }
        _ => anyhow::bail!("unsupported PostgreSQL type '{}'", data_type.name()),
    })
}
