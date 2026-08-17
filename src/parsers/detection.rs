use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParserDetection {
    pub key: String,

    pub label: String,

    pub config: Value,
}

pub trait ParserDetector: Send + Sync {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>>;
}

#[must_use]
pub fn detect(payload: &[u8]) -> Vec<ParserDetection> {
    std::iter::once(&JsonDetector as &dyn ParserDetector)
        .filter_map(|detector| detector.try_parse(payload).ok().flatten())
        .collect()
}

struct JsonDetector;

impl ParserDetector for JsonDetector {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>> {
        let (framing, records) = if let Ok(document) = serde_json::from_slice::<Value>(payload) {
            match document {
                Value::Array(records) => ("json_array", records),
                record => ("single_document", vec![record]),
            }
        } else {
            let lines = payload
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
                .collect::<Vec<_>>();
            if lines.len() <= 1 {
                return Ok(None);
            }
            let parsed = lines
                .iter()
                .map(|line| serde_json::from_slice::<Value>(line))
                .collect::<Result<Vec<_>, _>>();
            match parsed {
                Ok(records) => ("json_lines", records),
                Err(_) => return Ok(None),
            }
        };
        let columns = infer_columns(&records);
        Ok(Some(ParserDetection {
            key: "json_parser".to_owned(),
            label: "JSON parser".to_owned(),
            config: serde_json::json!({
                "common": {
                    "table_naming": { "type": "from_config", "name": "" },
                    "system_columns": {}
                },
                "json_parser": {
                    "json_framing": framing,
                    "columns": columns,
                    "conversion_error": "dlq",
                    "unknown_fields": {
                        "action": "send_to_column",
                        "column_name": "additional_properties"
                    },
                    "keys": []
                }
            }),
        }))
    }
}

fn infer_columns(records: &[Value]) -> Vec<Value> {
    let mut fields = BTreeSet::<&str>::new();
    for record in records {
        let Value::Object(object) = record else {
            continue;
        };
        for key in object.keys().filter(|key| is_identifier(key)) {
            fields.insert(key);
        }
    }
    fields
        .into_iter()
        .filter_map(|key| infer_column(key, records))
        .collect()
}

fn infer_column(key: &str, records: &[Value]) -> Option<Value> {
    let values = records
        .iter()
        .map(|record| record.as_object().and_then(|object| object.get(key)))
        .collect::<Vec<_>>();
    let nullable = values.iter().any(|value| value.is_none_or(Value::is_null));
    let present = values
        .into_iter()
        .flatten()
        .filter(|value| !value.is_null());
    let (json_type, arrow_type) = infer_type(present)?;
    Some(serde_json::json!({
        "jsonpath": format!("$.{key}"),
        "column_name": key,
        "json_data_type": json_type,
        "arrow_type": arrow_type,
        "nullable": nullable,
        "time_conversion": null,
        "low_cardinality": false,
        "max_length": null
    }))
}

fn infer_type<'a>(
    mut values: impl Iterator<Item = &'a Value>,
) -> Option<(&'static str, &'static str)> {
    let first = values.next()?;
    let kind = value_kind(first)?;
    values
        .all(|value| value_kind(value) == Some(kind))
        .then_some(kind)
}

fn value_kind(value: &Value) -> Option<(&'static str, &'static str)> {
    match value {
        Value::String(_) => Some(("string", "Utf8")),
        Value::Bool(_) => Some(("boolean", "Boolean")),
        Value::Number(number) if number.is_i64() => Some(("number", "Int64")),
        Value::Number(number) if number.is_u64() => Some(("number", "UInt64")),
        Value::Number(_) => Some(("number", "Float64")),
        _ => None,
    }
}

fn is_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
