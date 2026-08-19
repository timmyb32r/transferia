use std::collections::HashMap;

use apache_avro::{types::Value as AvroValue, Schema};
use base64::Engine as _;
use serde_json::{Number, Value};

pub fn avro_to_json(value: AvroValue) -> anyhow::Result<Value> {
    Ok(match value {
        AvroValue::Null => Value::Null,
        AvroValue::Boolean(value) => Value::Bool(value),
        AvroValue::Int(value) | AvroValue::Date(value) | AvroValue::TimeMillis(value) => {
            Value::Number(value.into())
        }
        AvroValue::Long(value)
        | AvroValue::TimeMicros(value)
        | AvroValue::TimestampMillis(value)
        | AvroValue::TimestampMicros(value)
        | AvroValue::TimestampNanos(value)
        | AvroValue::LocalTimestampMillis(value)
        | AvroValue::LocalTimestampMicros(value)
        | AvroValue::LocalTimestampNanos(value) => Value::Number(value.into()),
        AvroValue::Float(value) => Number::from_f64(f64::from(value))
            .map(Value::Number)
            .ok_or_else(|| anyhow::anyhow!("Avro float is not finite"))?,
        AvroValue::Double(value) => Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| anyhow::anyhow!("Avro double is not finite"))?,
        AvroValue::Bytes(value) | AvroValue::Fixed(_, value) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(value))
        }
        AvroValue::String(value) | AvroValue::Enum(_, value) => Value::String(value),
        AvroValue::Array(values) => Value::Array(
            values
                .into_iter()
                .map(avro_to_json)
                .collect::<anyhow::Result<_>>()?,
        ),
        AvroValue::Map(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, avro_to_json(value)?)))
                .collect::<anyhow::Result<_>>()?,
        ),
        AvroValue::Union(_, value) => avro_to_json(*value)?,
        AvroValue::Record(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| Ok((key, avro_to_json(value)?)))
                .collect::<anyhow::Result<_>>()?,
        ),
        AvroValue::Decimal(value) => Value::String(
            base64::engine::general_purpose::STANDARD.encode(Vec::<u8>::try_from(&value)?),
        ),
        AvroValue::Uuid(value) => Value::String(value.to_string()),
        AvroValue::BigDecimal(value) => Value::String(value.to_string()),
        AvroValue::Duration(_) => {
            anyhow::bail!("Avro duration has no lossless JSON representation")
        }
    })
}

pub fn json_to_avro(schema: &Schema, value: &Value) -> anyhow::Result<AvroValue> {
    Ok(match schema {
        Schema::Null => {
            anyhow::ensure!(value.is_null(), "expected null");
            AvroValue::Null
        }
        Schema::Boolean => AvroValue::Boolean(
            value
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("expected boolean"))?,
        ),
        Schema::Int => AvroValue::Int(i32::try_from(
            value
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("expected signed integer"))?,
        )?),
        Schema::Long => AvroValue::Long(
            value
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("expected signed integer"))?,
        ),
        Schema::Float => AvroValue::Float(number(value)? as f32),
        Schema::Double => AvroValue::Double(number(value)?),
        Schema::Bytes => AvroValue::Bytes(decode_base64(value)?),
        Schema::String => AvroValue::String(string(value)?.to_owned()),
        Schema::Array(array) => AvroValue::Array(
            value
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("expected array"))?
                .iter()
                .map(|value| json_to_avro(&array.items, value))
                .collect::<anyhow::Result<_>>()?,
        ),
        Schema::Map(map) => AvroValue::Map(
            value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("expected object"))?
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_avro(&map.types, value)?)))
                .collect::<anyhow::Result<HashMap<_, _>>>()?,
        ),
        Schema::Union(union) => {
            let mut failures = Vec::new();
            let mut selected = None;
            for (index, variant) in union.variants().iter().enumerate() {
                match json_to_avro(variant, value) {
                    Ok(value) => {
                        selected = Some(AvroValue::Union(u32::try_from(index)?, Box::new(value)));
                        break;
                    }
                    Err(error) => failures.push(error.to_string()),
                }
            }
            selected.ok_or_else(|| {
                anyhow::anyhow!(
                    "value matches no Avro union variant: {}",
                    failures.join("; ")
                )
            })?
        }
        Schema::Record(record) => {
            let object = value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("expected object"))?;
            anyhow::ensure!(
                object.len() == record.fields.len(),
                "Avro record fields differ from schema"
            );
            AvroValue::Record(
                record
                    .fields
                    .iter()
                    .map(|field| {
                        let value = object.get(&field.name).ok_or_else(|| {
                            anyhow::anyhow!("missing Avro field '{}'", field.name)
                        })?;
                        Ok((field.name.clone(), json_to_avro(&field.schema, value)?))
                    })
                    .collect::<anyhow::Result<_>>()?,
            )
        }
        Schema::Enum(schema) => {
            let symbol = string(value)?;
            let index = schema
                .symbols
                .iter()
                .position(|candidate| candidate == symbol)
                .ok_or_else(|| anyhow::anyhow!("unknown Avro enum symbol '{symbol}'"))?;
            AvroValue::Enum(u32::try_from(index)?, symbol.to_owned())
        }
        Schema::Fixed(schema) => {
            let bytes = decode_base64(value)?;
            anyhow::ensure!(
                bytes.len() == schema.size,
                "Avro fixed value has wrong size"
            );
            AvroValue::Fixed(schema.size, bytes)
        }
        Schema::Date => AvroValue::Date(integer32(value)?),
        Schema::TimeMillis => AvroValue::TimeMillis(integer32(value)?),
        Schema::TimeMicros => AvroValue::TimeMicros(integer64(value)?),
        Schema::TimestampMillis => AvroValue::TimestampMillis(integer64(value)?),
        Schema::TimestampMicros => AvroValue::TimestampMicros(integer64(value)?),
        Schema::TimestampNanos => AvroValue::TimestampNanos(integer64(value)?),
        Schema::LocalTimestampMillis => AvroValue::LocalTimestampMillis(integer64(value)?),
        Schema::LocalTimestampMicros => AvroValue::LocalTimestampMicros(integer64(value)?),
        Schema::LocalTimestampNanos => AvroValue::LocalTimestampNanos(integer64(value)?),
        Schema::Uuid => AvroValue::Uuid(string(value)?.parse()?),
        Schema::Decimal(_) | Schema::BigDecimal | Schema::Duration | Schema::Ref { .. } => {
            anyhow::bail!("Avro schema type is not supported by the Arrow serializer")
        }
    })
}

fn number(value: &Value) -> anyhow::Result<f64> {
    value
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("expected number"))
}

fn integer32(value: &Value) -> anyhow::Result<i32> {
    Ok(i32::try_from(integer64(value)?)?)
}

fn integer64(value: &Value) -> anyhow::Result<i64> {
    value
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("expected signed integer"))
}

fn string(value: &Value) -> anyhow::Result<&str> {
    value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("expected string"))
}

fn decode_base64(value: &Value) -> anyhow::Result<Vec<u8>> {
    Ok(base64::engine::general_purpose::STANDARD.decode(string(value)?)?)
}
