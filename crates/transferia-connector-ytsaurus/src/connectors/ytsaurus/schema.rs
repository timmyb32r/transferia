use arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;
use serde_json::Value;

use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

pub const MAX_COLUMN_NAME_CHARS: usize = 256;
pub const MAX_COLUMNS: usize = 32_768;

#[derive(Deserialize)]
struct SchemaEnvelope {
    #[serde(rename = "$attributes", default)]
    attributes: SchemaAttributes,

    #[serde(rename = "$value")]
    columns: Vec<YtColumn>,
}

#[derive(Default, Deserialize)]
struct SchemaAttributes {
    #[serde(default)]
    unique_keys: bool,
}

#[derive(Deserialize)]
struct YtColumn {
    name: String,
    #[serde(rename = "type")]
    legacy_type: String,
    required: bool,
    #[serde(default)]
    sort_order: Option<YtSortOrder>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum YtSortOrder {
    Ascending,
    Descending,
}

pub fn parse_schema(value: Value) -> anyhow::Result<DatasetSchema> {
    let envelope: SchemaEnvelope = serde_json::from_value(value)
        .map_err(|error| anyhow::anyhow!("invalid YTsaurus schema response: {error}"))?;
    anyhow::ensure!(
        !envelope.columns.is_empty(),
        "YTsaurus table schema is empty"
    );
    anyhow::ensure!(
        envelope.columns.len() <= MAX_COLUMNS,
        "YTsaurus table schema exceeds {MAX_COLUMNS} columns"
    );
    let unique_keys = envelope.attributes.unique_keys;
    let mut names = std::collections::HashSet::new();
    let columns = envelope
        .columns
        .into_iter()
        .map(|column| {
            validate_column_name(&column.name)?;
            anyhow::ensure!(
                names.insert(column.name.clone()),
                "YTsaurus schema repeats column '{}'",
                column.name
            );
            Ok(
                SchemaColumn::new(
                    column.name,
                    yt_to_arrow(&column.legacy_type)?,
                    !column.required,
                )
                .with_constraints(unique_keys && column.sort_order.is_some(), false, None),
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(DatasetSchema::new(columns))
}

pub fn validate_column_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "YTsaurus column name must not be empty");
    anyhow::ensure!(
        name.chars().count() <= MAX_COLUMN_NAME_CHARS,
        "YTsaurus column name '{name}' exceeds {MAX_COLUMN_NAME_CHARS} characters"
    );
    anyhow::ensure!(
        !name.starts_with('@'),
        "YTsaurus column name '{name}' uses the reserved '@' prefix"
    );
    Ok(())
}

pub fn yt_to_arrow(name: &str) -> anyhow::Result<DataType> {
    Ok(match name {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" | "interval" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "string" => DataType::Binary,
        "utf8" => DataType::Utf8,
        "date" => DataType::Date32,
        "datetime" => DataType::Date64,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => anyhow::bail!("unsupported YTsaurus primitive type '{name}'"),
    })
}

pub fn schema_to_yt(schema: &DatasetSchema) -> anyhow::Result<Value> {
    Ok(Value::Array(
        schema
            .columns
            .iter()
            .map(|column| {
                validate_column_name(&column.name)?;
                Ok(serde_json::json!({
                    "name": column.name,
                    "type": arrow_to_yt(&column.data_type)?,
                    "required": !column.nullable,
                }))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
    ))
}

pub fn arrow_to_yt(data_type: &DataType) -> anyhow::Result<&'static str> {
    Ok(match data_type {
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Boolean => "boolean",
        DataType::Binary | DataType::LargeBinary => "string",
        DataType::Utf8 | DataType::LargeUtf8 => "utf8",
        DataType::Date32 => "date",
        DataType::Date64 => "datetime",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp",
        _ => anyhow::bail!("Arrow type {data_type:?} is not supported by YTsaurus static tables"),
    })
}

pub fn schemas_equal(left: &DatasetSchema, right: &DatasetSchema) -> bool {
    left.columns.len() == right.columns.len()
        && left
            .columns
            .iter()
            .zip(&right.columns)
            .all(|(left, right)| {
                left.name == right.name
                    && left.data_type == right.data_type
                    && left.nullable == right.nullable
            })
}
