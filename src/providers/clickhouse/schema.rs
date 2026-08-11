use arrow::datatypes::{DataType, TimeUnit};

use crate::types::schema::SchemaColumn;

pub(super) fn schema_columns(columns: &[SchemaColumn]) -> anyhow::Result<Vec<(String, String)>> {
    columns
        .iter()
        .map(|column| {
            let mut data_type = arrow_to_clickhouse(&column.data_type)?;
            if column.nullable {
                data_type = format!("Nullable({data_type})");
            }
            Ok((column.name.clone(), data_type))
        })
        .collect()
}

fn arrow_to_clickhouse(data_type: &DataType) -> anyhow::Result<String> {
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => "String".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Boolean => "Bool".into(),
        DataType::Date32 => "Date32".into(),
        DataType::Date64 => "DateTime64(3)".into(),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => "DateTime".into(),
            TimeUnit::Millisecond => "DateTime64(3)".into(),
            TimeUnit::Microsecond => "DateTime64(6)".into(),
            TimeUnit::Nanosecond => "DateTime64(9)".into(),
        },
        other => anyhow::bail!("No ClickHouse type mapping for Arrow type {other:?}"),
    })
}
