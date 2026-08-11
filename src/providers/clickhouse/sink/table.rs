use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_arrow::{ArrowFormat, Client};

use super::client::{connect_once, quote_identifier};
use super::ClickHouseSinkConfig;
use crate::providers::traits::SinkPrepare;
use crate::types::schema::{DatasetSchema, SchemaColumn};

pub(super) async fn prepare_tables(
    config: &ClickHouseSinkConfig,
    request: &SinkPrepare,
) -> anyhow::Result<()> {
    for key in &config.sorting_key {
        anyhow::ensure!(
            request
                .schema
                .columns
                .iter()
                .any(|column| &column.name == key),
            "clickhouse.sorting_key column '{key}' is absent from the dataset schema"
        );
    }

    let client = connect_once(config)
        .await
        .map_err(|error| anyhow::anyhow!("ClickHouse admin connection failed: {error}"))?;
    client
        .execute("SELECT 1", None)
        .await
        .map_err(|error| anyhow::anyhow!("ClickHouse admin health check failed: {error}"))?;

    create_table(
        &client,
        &request.table,
        &request.schema,
        &config.sorting_key,
        config.recreate_tables,
    )
    .await?;
    create_table(
        &client,
        &request.dlq_table,
        &request.dlq_schema,
        &[],
        config.recreate_tables,
    )
    .await
}

async fn create_table(
    client: &Client<ArrowFormat>,
    name: &str,
    schema: &DatasetSchema,
    sorting_key: &[String],
    recreate: bool,
) -> anyhow::Result<()> {
    if recreate {
        tracing::warn!(table = name, "dropping table before recreation");
        client
            .execute(
                &format!("DROP TABLE IF EXISTS {}", quote_identifier(name)),
                None,
            )
            .await
            .map_err(|error| anyhow::anyhow!("Failed to drop table '{name}': {error}"))?;
    }

    let columns = schema
        .columns
        .iter()
        .map(column_definition)
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    let sorting_key = sorting_key
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = if sorting_key.is_empty() {
        "tuple()"
    } else {
        &sorting_key
    };
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} ({columns}) ENGINE = MergeTree ORDER BY ({order_by})",
        quote_identifier(name),
    );
    client
        .execute(&ddl, None)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create table '{name}': {error}"))?;
    Ok(())
}

fn column_definition(column: &SchemaColumn) -> anyhow::Result<String> {
    let data_type = clickhouse_type(&column.data_type)?;
    let data_type = if column.nullable {
        format!("Nullable({data_type})")
    } else {
        data_type.into()
    };
    Ok(format!("{} {data_type}", quote_identifier(&column.name)))
}

fn clickhouse_type(data_type: &DataType) -> anyhow::Result<&'static str> {
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => "String",
        DataType::Int8 => "Int8",
        DataType::Int16 => "Int16",
        DataType::Int32 => "Int32",
        DataType::Int64 => "Int64",
        DataType::UInt8 => "UInt8",
        DataType::UInt16 => "UInt16",
        DataType::UInt32 => "UInt32",
        DataType::UInt64 => "UInt64",
        DataType::Float32 => "Float32",
        DataType::Float64 => "Float64",
        DataType::Boolean => "Bool",
        DataType::Date32 => "Date32",
        DataType::Date64 => "DateTime64(3)",
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => "DateTime",
            TimeUnit::Millisecond => "DateTime64(3)",
            TimeUnit::Microsecond => "DateTime64(6)",
            TimeUnit::Nanosecond => "DateTime64(9)",
        },
        other => anyhow::bail!("No ClickHouse type mapping for Arrow type {other:?}"),
    })
}
