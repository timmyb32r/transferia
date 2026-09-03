use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringBuilder, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use mysql_async::prelude::{Query, Queryable, WithParams};
use mysql_async::{BinaryProtocol, Conn, ResultSetStream, Row, TextProtocol, Value};

use super::config::{MySqlReadProtocol, TableConfig};
use super::connector::{ColumnPlan, MySqlColumnKind};
use crate::connectors::mysql::common::quote_identifier;
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};

type TextRowStream = ResultSetStream<'static, 'static, 'static, Row, TextProtocol>;
type BinaryRowStream = ResultSetStream<'static, 'static, 'static, Row, BinaryProtocol>;

enum MySqlRowStream {
    Text(TextRowStream),
    Binary(BinaryRowStream),
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
    stream: MySqlRowStream,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

impl MySqlSource {
    pub async fn new(
        mut connection: Conn,
        database: String,
        table: TableConfig,
        schema: DatasetSchema,
        columns: Vec<ColumnPlan>,
        batch_rows: usize,
        read_protocol: MySqlReadProtocol,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        connection
            .query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        connection
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
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
            MySqlReadProtocol::Text => {
                MySqlRowStream::Text(query.stream::<Row, _>(connection).await?)
            }
            MySqlReadProtocol::Binary => {
                MySqlRowStream::Binary(query.with(()).stream::<Row, _>(connection).await?)
            }
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
            stream,
            offset: 0,
            finished: false,
            counters,
        })
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
            let mut rows = Vec::with_capacity(self.batch_rows);
            while rows.len() < self.batch_rows {
                match self.stream.next().await {
                    Some(Ok(row)) => rows.push(row),
                    Some(Err(error)) => {
                        return Err(DataPlaneFailure::retryable(error.into()));
                    }
                    None => break,
                }
            }
            if rows.is_empty() {
                self.finished = true;
                return Ok(SourceBatch::Finished);
            }
            let source_rows = rows.len() as u64;
            let batch = rows_to_batch(&self.schema, &self.columns, &rows, self.offset)
                .map_err(DataPlaneFailure::fatal)?;
            self.offset = self
                .offset
                .checked_add(
                    i64::try_from(rows.len())
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
                    routing_system_columns(self.columns.len()),
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

pub(super) fn rows_to_batch(
    discovered_schema: &DatasetSchema,
    columns: &[ColumnPlan],
    rows: &[Row],
    start_offset: i64,
) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        discovered_schema.columns.len() == columns.len(),
        "MySQL query schema has {} columns, discovery declared {}",
        columns.len(),
        discovered_schema.columns.len()
    );
    let mut fields = Vec::with_capacity(columns.len() + 4);
    let mut arrays = Vec::with_capacity(columns.len() + 4);
    for (index, (column, discovered)) in columns.iter().zip(&discovered_schema.columns).enumerate()
    {
        anyhow::ensure!(
            column.name == discovered.name && column.kind.arrow_type() == discovered.data_type,
            "MySQL query schema drifted at column '{}'",
            column.name
        );
        fields.push(
            Field::new(&column.name, column.kind.arrow_type(), column.nullable)
                .with_metadata(discovered.arrow_metadata()),
        );
        arrays.push(column_array(rows, index, column)?);
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

fn column_array(rows: &[Row], index: usize, column: &ColumnPlan) -> anyhow::Result<ArrayRef> {
    macro_rules! integer_array {
        ($value:ident, $ty:ty, $array:ty) => {{
            let values = rows
                .iter()
                .map(|row| match row.as_ref(index) {
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
            for row in rows {
                match row.as_ref(index) {
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
            for row in rows {
                match row.as_ref(index) {
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
