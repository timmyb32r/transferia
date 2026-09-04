use std::collections::BTreeSet;

use serde_json::Value;

pub use transferia_delivery_contracts::parser::{
    InferredColumn, ParserDetection, ParserPreviewTab,
};

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
    [
        &JsonDetector as &dyn ParserDetector,
        &TskvDetector as &dyn ParserDetector,
        &super::protoscope::CloudEventsWireDetector as &dyn ParserDetector,
        &super::protoscope::ProtobufWireDetector as &dyn ParserDetector,
    ]
        .into_iter()
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

struct TskvDetector;

impl ParserDetector for TskvDetector {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>> {
        self.try_parse_samples(&[payload], usize::MAX)
    }

    fn try_parse_samples(
        &self,
        payloads: &[&[u8]],
        max_rows: usize,
    ) -> anyhow::Result<Option<ParserDetection>> {
        let mut records = Vec::new();
        for payload in payloads {
            let Ok(fields) = super::tskv::parse_record(payload) else {
                continue;
            };
            records.push(fields);
            if records.len() >= max_rows {
                break;
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
        let mut names = BTreeSet::new();
        for record in &records {
            names.extend(record.keys().cloned());
        }
        let columns = names
            .iter()
            .map(|name| {
                let values = records
                    .iter()
                    .filter_map(|record| record.get(name))
                    .collect::<Vec<_>>();
                let arrow_type = infer_tskv_arrow_type(&values);
                serde_json::json!({
                    "column_name": name,
                    "arrow_type": arrow_type,
                    "nullable": values.len() != records.len(),
                    "time_conversion": null,
                    "low_cardinality": false,
                    "max_length": null
                })
            })
            .collect::<Vec<_>>();
        let inferred_columns = columns
            .iter()
            .filter_map(|column| {
                Some(InferredColumn {
                    name: column.get("column_name")?.as_str()?.to_owned(),
                    source_type: "string".to_owned(),
                    arrow_type: column.get("arrow_type")?.as_str()?.to_owned(),
                    nullable: column.get("nullable")?.as_bool()?,
                })
            })
            .collect();
        let sample_rows = records
            .iter()
            .map(|record| {
                Value::Object(
                    record
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        let content = sample_rows
            .first()
            .and_then(|row| serde_json::to_string_pretty(row).ok())
            .unwrap_or_default();
        Ok(Some(ParserDetection {
            key: "tskv".to_owned(),
            label: "TSKV parser".to_owned(),
            config: serde_json::json!({
                "common": {
                    "table_naming": { "type": "from_config", "name": "" },
                    "system_columns": {}
                },
                "tskv": {
                    "columns": columns,
                    "unknown_fields": {
                        "action": "send_to_column",
                        "column_name": "additional_properties"
                    },
                    "keys": []
                }
            }),
            inferred_columns,
            sample_rows,
            preview_tabs: vec![ParserPreviewTab {
                key: "tskv_pretty_print".to_owned(),
                label: "Pretty print".to_owned(),
                content,
                truncated: false,
            }],
            sampled_messages: records.len(),
            sampled_rows: records.len(),
        }))
    }
}

fn infer_tskv_arrow_type(values: &[&String]) -> &'static str {
    if values.iter().all(|value| matches!(value.as_str(), "true" | "false")) {
        "Boolean"
    } else if values.iter().all(|value| value.parse::<i64>().is_ok()) {
        "Int64"
    } else if values
        .iter()
        .all(|value| value.parse::<u64>().is_ok())
    {
        "UInt64"
    } else if values.iter().all(|value| {
        value
            .parse::<f64>()
            .is_ok_and(|number| number.is_finite())
    }) {
        "Float64"
    } else {
        "Utf8"
    }
}

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
            sample_rows: records.clone(),
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
