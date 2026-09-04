use arrow::datatypes::{DataType, TimeUnit};
use serde_json::{json, Map, Value};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};

use super::document::{document_shape, stable_type_tag, DocumentShape};

pub(super) fn destination_type(column: &SchemaColumn) -> anyhow::Result<String> {
    Ok(mapping_for(column)?
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("object")
        .to_owned())
}

pub(super) fn strict_mapping(schema: &DatasetSchema, owner: Option<&str>) -> anyhow::Result<Value> {
    anyhow::ensure!(
        document_shape(schema) == DocumentShape::Flat,
        "OpenSearch cannot create an index for an opaque _source envelope; provide an existing index with its original mapping"
    );
    let mut properties = Map::new();
    for column in &schema.columns {
        if column.name == "_id" || column.name == "_routing" {
            continue;
        }
        properties.insert(column.name.clone(), mapping_for(column)?);
    }
    let mut mapping = json!({
        "dynamic": "strict",
        "properties": properties,
        "_meta": {
            "transferia_schema_version": 1
        }
    });
    if let Some(owner) = owner {
        mapping["_meta"]["transferia_speedtest_owner"] = Value::String(owner.to_owned());
    }
    Ok(mapping)
}

pub(super) fn create_index_body(
    schema: &DatasetSchema,
    owner: Option<&str>,
) -> anyhow::Result<Value> {
    Ok(json!({
        "settings": {
            "index": {
                "translog": { "durability": "request" }
            }
        },
        "mappings": strict_mapping(schema, owner)?
    }))
}

pub(super) fn validate_index_description(
    index: &str,
    description: &Value,
    schema: &DatasetSchema,
    owner: Option<&str>,
) -> anyhow::Result<()> {
    let root = description.get(index).ok_or_else(|| {
        anyhow::anyhow!("OpenSearch omitted index '{index}' from its description")
    })?;
    let durability = root
        .pointer("/settings/index/translog/durability")
        .and_then(Value::as_str);
    anyhow::ensure!(
        durability == Some("request"),
        "OpenSearch index '{index}' must use index.translog.durability=request"
    );
    if document_shape(schema) == DocumentShape::Envelope {
        if let Some(owner) = owner {
            anyhow::ensure!(
                root.pointer("/mappings/_meta/transferia_speedtest_owner")
                    .and_then(Value::as_str)
                    == Some(owner),
                "OpenSearch speedtest index '{index}' has a foreign owner"
            );
        }
        return Ok(());
    }
    let expected = strict_mapping(schema, owner)?;
    let actual = root
        .get("mappings")
        .ok_or_else(|| anyhow::anyhow!("OpenSearch index '{index}' has no mappings"))?;
    anyhow::ensure!(
        actual == &expected,
        "OpenSearch index '{index}' mapping does not exactly match the discovered Arrow schema"
    );
    Ok(())
}

fn mapping_for(column: &SchemaColumn) -> anyhow::Result<Value> {
    if column.arrow_extension_name == Some(ARROW_JSON_EXTENSION_NAME) {
        anyhow::bail!(
            "OpenSearch cannot derive a strict mapping for arbitrary arrow.json column '{}'",
            column.name
        );
    }
    anyhow::ensure!(
        column.arrow_extension_name.is_none(),
        "unsupported Arrow extension {:?} for OpenSearch field '{}'",
        column.arrow_extension_name,
        column.name
    );
    let arrow = stable_type_tag(&column.data_type, column.arrow_extension_name)?;
    let mut value = match &column.data_type {
        DataType::Boolean => json!({ "type": "boolean" }),
        DataType::Int8 => json!({ "type": "byte" }),
        DataType::Int16 | DataType::UInt8 => json!({ "type": "short" }),
        DataType::Int32 | DataType::UInt16 => json!({ "type": "integer" }),
        DataType::Int64 | DataType::UInt32 => json!({ "type": "long" }),
        DataType::UInt64 => json!({ "type": "unsigned_long" }),
        DataType::Float32 => json!({ "type": "float" }),
        DataType::Float64 => json!({ "type": "double" }),
        DataType::Utf8 | DataType::LargeUtf8 => json!({ "type": "keyword" }),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            json!({ "type": "binary" })
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            json!({ "type": "keyword" })
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Duration(_) => {
            json!({ "type": "long" })
        }
        other => anyhow::bail!(
            "unsupported Arrow type {other:?} for OpenSearch field '{}'",
            column.name
        ),
    };
    value["meta"] = json!({ "arrow_type": arrow });
    if let DataType::Timestamp(unit, timezone) = &column.data_type {
        value["meta"]["time_unit"] = Value::String(time_unit(*unit).to_owned());
        if let Some(timezone) = timezone {
            value["meta"]["timezone"] = Value::String(timezone.to_string());
        }
    }
    if let DataType::Duration(unit) = &column.data_type {
        value["meta"]["time_unit"] = Value::String(time_unit(*unit).to_owned());
    }
    Ok(value)
}

const fn time_unit(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "second",
        TimeUnit::Millisecond => "millisecond",
        TimeUnit::Microsecond => "microsecond",
        TimeUnit::Nanosecond => "nanosecond",
    }
}
