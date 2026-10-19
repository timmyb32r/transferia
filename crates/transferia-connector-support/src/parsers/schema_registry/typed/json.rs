use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

pub(in crate::parsers::schema_registry) fn record(definition: &Value, value: &Value) -> anyhow::Result<RecordBatch> {
    let data_type = data_type(definition)?;
    let DataType::Struct(fields) = data_type else {
        anyhow::bail!("Schema Registry JSON output requires an object schema");
    };
    let array = array(&DataType::Struct(fields.clone()), value)?;
    let array = array.as_any().downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow::anyhow!("JSON object did not produce an Arrow struct"))?;
    Ok(RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), array.columns().to_vec(), &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)))?)
}

fn nullable(definition: &Value) -> bool {
    definition["type"].as_array().is_some_and(|types| types.iter().any(|value| value == "null"))
        || definition["type"] == "null"
}

fn data_type(definition: &Value) -> anyhow::Result<DataType> {
    let kind = if let Some(types) = definition["type"].as_array() {
        let kinds = types.iter().filter_map(Value::as_str).filter(|kind| *kind != "null").collect::<Vec<_>>();
        anyhow::ensure!(kinds.len() == 1, "JSON Schema unions with multiple non-null types have no supported Arrow representation");
        kinds[0]
    } else {
        definition["type"].as_str().ok_or_else(|| anyhow::anyhow!("JSON Schema must declare an explicit type for every Arrow field"))?
    };
    Ok(match kind {
        "null" => DataType::Null,
        "boolean" => DataType::Boolean,
        "integer" => DataType::Int64,
        "number" => anyhow::bail!("unbounded JSON Schema number has no lossless Arrow numeric representation"),
        "string" => DataType::Utf8,
        "array" => DataType::List(Arc::new(Field::new("item", data_type(&definition["items"])?, nullable(&definition["items"])))),
        "object" => {
            anyhow::ensure!(definition["additionalProperties"] == false, "JSON Schema objects must disallow additional properties to define a complete Arrow schema");
            let properties = definition["properties"].as_object().ok_or_else(|| anyhow::anyhow!("JSON Schema object requires properties"))?;
            DataType::Struct(properties.iter().map(|(name, definition)| {
                Ok(Field::new(name, data_type(definition)?, nullable(definition)))
            }).collect::<anyhow::Result<Vec<_>>>()?.into())
        }
        _ => anyhow::bail!("JSON Schema type '{kind}' has no supported Arrow representation"),
    })
}

fn array(data_type: &DataType, value: &Value) -> anyhow::Result<ArrayRef> {
    if value.is_null() { return Ok(new_null_array(data_type, 1)); }
    Ok(match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(vec![value.as_bool().ok_or_else(|| anyhow::anyhow!("expected JSON boolean"))?])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![value.as_i64().ok_or_else(|| anyhow::anyhow!("JSON integer does not fit Arrow Int64 without loss"))?])),
        DataType::Utf8 => Arc::new(StringArray::from(vec![value.as_str().ok_or_else(|| anyhow::anyhow!("expected JSON string"))?])),
        DataType::List(field) => {
            let values = value.as_array().ok_or_else(|| anyhow::anyhow!("expected JSON array"))?;
            let arrays = values.iter().map(|value| {
                anyhow::ensure!(field.is_nullable() || !value.is_null(), "JSON array contains a null in a non-nullable item");
                array(field.data_type(), value)
            }).collect::<anyhow::Result<Vec<_>>>()?;
            super::list(Arc::clone(field), arrays)?
        }
        DataType::Struct(fields) => {
            let object = value.as_object().ok_or_else(|| anyhow::anyhow!("expected JSON object"))?;
            anyhow::ensure!(object.len() == fields.len(), "JSON object fields differ from its Arrow schema; absent fields cannot be silently replaced with NULL");
            let arrays = fields.iter().map(|field| {
                let value = object.get(field.name()).ok_or_else(|| anyhow::anyhow!("JSON field '{}' is absent", field.name()))?;
                anyhow::ensure!(field.is_nullable() || !value.is_null(), "JSON field '{}' is not nullable", field.name());
                array(field.data_type(), value)
            }).collect::<anyhow::Result<Vec<_>>>()?;
            if fields.is_empty() { Arc::new(StructArray::new_empty_fields(1, None)) } else { Arc::new(StructArray::try_new(fields.clone(), arrays, None)?) }
        }
        _ => anyhow::bail!("JSON value does not match its Arrow type"),
    })
}

/// Reject duplicate keys at every object depth before constructing an Arrow
/// row. Deserializing directly into Value would silently keep the last value.
pub(in crate::parsers::schema_registry) fn parse(payload: &[u8]) -> anyhow::Result<Value> {
    let value: UniqueValue = serde_json::from_slice(payload)?;
    Ok(value.0)
}

struct UniqueValue(Value);

impl<'de> serde::Deserialize<'de> for UniqueValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON with unique object keys")
            }
            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> { Ok(UniqueValue(Value::Bool(value))) }
            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> { Ok(UniqueValue(Value::Number(value.into()))) }
            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> { Ok(UniqueValue(Value::Number(value.into()))) }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
                let number = serde_json::Number::from_f64(value).ok_or_else(|| E::custom("non-finite JSON number"))?;
                Ok(UniqueValue(Value::Number(number)))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> { Ok(UniqueValue(Value::String(value.into()))) }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> { Ok(UniqueValue(Value::String(value))) }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> { Ok(UniqueValue(Value::Null)) }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> { Ok(UniqueValue(Value::Null)) }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueValue>()? { values.push(value.0); }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut object = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if object.contains_key(&key) { return Err(serde::de::Error::custom("duplicate JSON object key")); }
                    let value = map.next_value::<UniqueValue>()?;
                    object.insert(key, value.0);
                }
                Ok(UniqueValue(Value::Object(object)))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}
