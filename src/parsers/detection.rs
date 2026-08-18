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

    pub inferred_columns: Vec<InferredColumn>,

    pub preview_tabs: Vec<ParserPreviewTab>,

    pub sampled_messages: usize,

    pub sampled_rows: usize,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InferredColumn {
    pub name: String,

    pub source_type: String,

    pub arrow_type: String,

    pub nullable: bool,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParserPreviewTab {
    pub key: String,

    pub label: String,

    pub content: String,

    pub truncated: bool,
}

pub trait ParserDetector: Send + Sync {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>>;

    fn try_parse_samples(
        &self,
        payloads: &[&[u8]],
        _max_rows: usize,
    ) -> anyhow::Result<Option<ParserDetection>> {
        payloads
            .first()
            .map_or_else(|| Ok(None), |payload| self.try_parse(payload))
    }
}

#[must_use]
pub fn detect(payload: &[u8]) -> Vec<ParserDetection> {
    detect_samples(&[payload], 1_000)
}

#[must_use]
pub fn detect_samples(payloads: &[&[u8]], max_rows: usize) -> Vec<ParserDetection> {
    std::iter::once(&JsonDetector as &dyn ParserDetector)
        .filter_map(|detector| {
            detect_with_samples(detector, payloads, max_rows)
                .ok()
                .flatten()
        })
        .collect()
}

fn detect_with_samples(
    detector: &dyn ParserDetector,
    payloads: &[&[u8]],
    max_rows: usize,
) -> anyhow::Result<Option<ParserDetection>> {
    detector.try_parse_samples(payloads, max_rows)
}

struct JsonDetector;

impl ParserDetector for JsonDetector {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>> {
        self.try_parse_samples(&[payload], usize::MAX)
    }

    fn try_parse_samples(
        &self,
        payloads: &[&[u8]],
        max_rows: usize,
    ) -> anyhow::Result<Option<ParserDetection>> {
        let mut framing = None;
        let mut records = Vec::new();
        let mut sampled_messages = 0;
        for payload in payloads {
            let Some((payload_framing, mut payload_records)) = parse_json_payload(payload) else {
                continue;
            };
            if let Some(expected) = framing {
                if expected != payload_framing {
                    continue;
                }
            } else {
                framing = Some(payload_framing);
            }
            sampled_messages += 1;
            let remaining = max_rows.saturating_sub(records.len());
            records.extend(payload_records.drain(..remaining.min(payload_records.len())));
            if records.len() >= max_rows {
                break;
            }
        }
        let Some(framing) = framing else {
            return Ok(None);
        };
        let columns = infer_columns(&records);
        let inferred_columns = columns
            .iter()
            .filter_map(|column| {
                Some(InferredColumn {
                    name: column.get("column_name")?.as_str()?.to_owned(),
                    source_type: column.get("json_data_type")?.as_str()?.to_owned(),
                    arrow_type: column.get("arrow_type")?.as_str()?.to_owned(),
                    nullable: column.get("nullable")?.as_bool()?,
                })
            })
            .collect();
        let preview_tabs = payloads
            .first()
            .and_then(|payload| pretty_json(payload))
            .into_iter()
            .collect();
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
            inferred_columns,
            preview_tabs,
            sampled_messages,
            sampled_rows: records.len(),
        }))
    }
}

fn parse_json_payload(payload: &[u8]) -> Option<(&'static str, Vec<Value>)> {
    let parsed = if let Ok(document) = serde_json::from_slice::<Value>(payload) {
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
            return None;
        }
        let parsed = lines
            .iter()
            .map(|line| serde_json::from_slice::<Value>(line))
            .collect::<Result<Vec<_>, _>>();
        match parsed {
            Ok(records) => ("json_lines", records),
            Err(_) => return None,
        }
    };
    Some(parsed)
}

fn pretty_json(payload: &[u8]) -> Option<ParserPreviewTab> {
    const MAX_PRETTY_PRINT_BYTES: usize = 64 * 1024;
    let (framing, mut records) = parse_json_payload(payload)?;
    let value = if framing == "single_document" {
        records.pop()?
    } else {
        Value::Array(records)
    };
    let content = serde_json::to_string_pretty(&value).ok()?;
    let truncated = content.len() > MAX_PRETTY_PRINT_BYTES;
    let content = if truncated {
        let mut end = MAX_PRETTY_PRINT_BYTES;
        while !content.is_char_boundary(end) {
            end -= 1;
        }
        content[..end].to_owned()
    } else {
        content
    };
    Some(ParserPreviewTab {
        key: "json_pretty_print".to_owned(),
        label: "Pretty print".to_owned(),
        content,
        truncated,
    })
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
