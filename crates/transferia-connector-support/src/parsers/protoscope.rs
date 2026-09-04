use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde_json::json;

use super::detection::{ParserDetection, ParserDetector, ParserPreviewTab};
use super::protobuf::PROTOSEQ_MAGIC;

const MAX_PREVIEW_BYTES: usize = 64 * 1024;
const MAX_NESTING_DEPTH: usize = 64;
const MAX_FIELD_NUMBER: u32 = (1 << 29) - 1;

pub struct ProtobufWireDetector;

pub struct CloudEventsWireDetector;

impl ParserDetector for CloudEventsWireDetector {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>> {
        let Some(fields) = decode_message(payload, 0) else {
            return Ok(None);
        };
        if !looks_like_cloud_event(&fields) {
            return Ok(None);
        }
        let Some(mut detection) = ProtobufWireDetector.try_parse(payload)? else {
            return Ok(None);
        };
        "cloud_events".clone_into(&mut detection.key);
        "CloudEvents parser".clone_into(&mut detection.label);
        detection.config["protobuf"]["message_name"] =
            serde_json::Value::String("io.cloudevents.v1.CloudEvent".to_owned());
        Ok(Some(detection))
    }
}

fn looks_like_cloud_event(fields: &[WireField]) -> bool {
    let required_text = [1_u32, 2, 3, 4].into_iter().all(|number| {
        fields.iter().any(|field| {
            field.number == number
                && matches!(
                    &field.value,
                    WireValue::LengthDelimited(value)
                        if !value.is_empty() && std::str::from_utf8(value).is_ok()
                )
        })
    });
    let has_data = fields.iter().any(|field| {
        matches!(field.number, 6..=8) && matches!(field.value, WireValue::LengthDelimited(_))
    });
    required_text
        && has_data
        && fields.iter().all(|field| {
            matches!(field.number, 1..=8) && matches!(field.value, WireValue::LengthDelimited(_))
        })
}

impl ParserDetector for ProtobufWireDetector {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>> {
        self.try_parse_samples(&[payload], usize::MAX)
    }

    fn try_parse_samples(
        &self,
        payloads: &[&[u8]],
        max_rows: usize,
    ) -> anyhow::Result<Option<ParserDetection>> {
        let mut framing = None;
        let mut sampled_messages = 0;
        let mut sampled_rows = 0;
        let mut first_preview = None;
        for payload in payloads {
            let Some(decoded) = decode_payload(payload) else {
                continue;
            };
            if framing.is_some_and(|expected| expected != decoded.framing) {
                continue;
            }
            framing.get_or_insert(decoded.framing);
            sampled_messages += 1;
            let remaining = max_rows.saturating_sub(sampled_rows);
            sampled_rows += decoded.messages.len().min(remaining);
            first_preview.get_or_insert_with(|| render_preview(&decoded));
            if sampled_rows >= max_rows {
                break;
            }
        }
        let (Some(framing), Some(preview)) = (framing, first_preview) else {
            return Ok(None);
        };

        Ok(Some(ParserDetection {
            key: "protobuf".to_owned(),
            label: "Protobuf parser".to_owned(),
            config: json!({
                "common": {
                    "table_naming": { "type": "from_config", "name": "" },
                    "system_columns": {}
                },
                "protobuf": {
                    "descriptor": { "type": "inline_base64", "value": "" },
                    "message_name": "",
                    "package_type": framing.config_value(),
                    "include_columns": [],
                    "primary_key": [],
                    "null_keys_allowed": false,
                    "not_fill_empty_fields": false,
                    "unknown_fields": "fail"
                }
            }),
            inferred_columns: Vec::new(),
            sample_rows: Vec::new(),
            preview_tabs: vec![ParserPreviewTab {
                key: "protobuf_protoscope".to_owned(),
                label: "Protoscope".to_owned(),
                content: preview.content,
                truncated: preview.truncated,
            }],
            sampled_messages,
            sampled_rows,
        }))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Framing {
    SingleMessage,
    Protoseq,
}

impl Framing {
    const fn label(self) -> &'static str {
        match self {
            Self::SingleMessage => "single message",
            Self::Protoseq => "protoseq",
        }
    }

    const fn config_value(self) -> &'static str {
        match self {
            Self::SingleMessage => "single_message",
            Self::Protoseq => "protoseq",
        }
    }
}

struct DecodedPayload {
    framing: Framing,
    messages: Vec<Vec<WireField>>,
}

fn decode_payload(payload: &[u8]) -> Option<DecodedPayload> {
    if let Some(messages) = decode_protoseq(payload) {
        return Some(DecodedPayload {
            framing: Framing::Protoseq,
            messages,
        });
    }
    let fields = decode_message(payload, 0)?;
    Some(DecodedPayload {
        framing: Framing::SingleMessage,
        messages: vec![fields],
    })
}

fn decode_protoseq(mut input: &[u8]) -> Option<Vec<Vec<WireField>>> {
    let mut messages = Vec::new();
    while !input.is_empty() {
        let size = usize::try_from(u32::from_le_bytes(input.get(..4)?.try_into().ok()?)).ok()?;
        let frame_end = 4_usize
            .checked_add(size)?
            .checked_add(PROTOSEQ_MAGIC.len())?;
        let frame = input.get(4..4 + size)?;
        if input.get(4 + size..frame_end)? != PROTOSEQ_MAGIC {
            return None;
        }
        messages.push(decode_message(frame, 0)?);
        input = input.get(frame_end..)?;
    }
    (!messages.is_empty()).then_some(messages)
}

#[derive(Clone)]
struct WireField {
    number: u32,
    value: WireValue,
}

#[derive(Clone)]
enum WireValue {
    Varint(u64),
    Fixed64(u64),
    LengthDelimited(Vec<u8>),
    Group(Vec<WireField>),
    Fixed32(u32),
}

fn decode_message(input: &[u8], depth: usize) -> Option<Vec<WireField>> {
    if input.is_empty() || depth > MAX_NESTING_DEPTH {
        return None;
    }
    let (fields, consumed) = decode_fields(input, depth, None)?;
    (consumed == input.len() && !fields.is_empty()).then_some(fields)
}

fn decode_fields(
    input: &[u8],
    depth: usize,
    expected_end_group: Option<u32>,
) -> Option<(Vec<WireField>, usize)> {
    if depth > MAX_NESTING_DEPTH {
        return None;
    }
    let mut fields = Vec::new();
    let mut offset = 0;
    while offset < input.len() {
        let (tag, tag_bytes) = decode_varint(input.get(offset..)?)?;
        offset = offset.checked_add(tag_bytes)?;
        let number = u32::try_from(tag >> 3).ok()?;
        if number == 0 || number > MAX_FIELD_NUMBER {
            return None;
        }
        match u8::try_from(tag & 0b111).ok()? {
            0 => {
                let (value, bytes) = decode_varint(input.get(offset..)?)?;
                offset = offset.checked_add(bytes)?;
                fields.push(WireField {
                    number,
                    value: WireValue::Varint(value),
                });
            }
            1 => {
                let end = offset.checked_add(8)?;
                let bytes: [u8; 8] = input.get(offset..end)?.try_into().ok()?;
                offset = end;
                fields.push(WireField {
                    number,
                    value: WireValue::Fixed64(u64::from_le_bytes(bytes)),
                });
            }
            2 => {
                let (length, length_bytes) = decode_varint(input.get(offset..)?)?;
                offset = offset.checked_add(length_bytes)?;
                let length = usize::try_from(length).ok()?;
                let end = offset.checked_add(length)?;
                let bytes = input.get(offset..end)?.to_vec();
                offset = end;
                fields.push(WireField {
                    number,
                    value: WireValue::LengthDelimited(bytes),
                });
            }
            3 => {
                let (nested, bytes) = decode_fields(input.get(offset..)?, depth + 1, Some(number))?;
                offset = offset.checked_add(bytes)?;
                fields.push(WireField {
                    number,
                    value: WireValue::Group(nested),
                });
            }
            4 if expected_end_group == Some(number) => return Some((fields, offset)),
            5 => {
                let end = offset.checked_add(4)?;
                let bytes: [u8; 4] = input.get(offset..end)?.try_into().ok()?;
                offset = end;
                fields.push(WireField {
                    number,
                    value: WireValue::Fixed32(u32::from_le_bytes(bytes)),
                });
            }
            _ => return None,
        }
    }
    expected_end_group.is_none().then_some((fields, offset))
}

fn decode_varint(input: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    for (index, byte) in input.iter().copied().take(10).enumerate() {
        if index == 9 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
    }
    None
}

struct RenderedPreview {
    content: String,
    truncated: bool,
}

fn render_preview(decoded: &DecodedPayload) -> RenderedPreview {
    let mut content = format!("framing: {}\n", decoded.framing.label());
    for (index, fields) in decoded.messages.iter().enumerate() {
        if decoded.messages.len() > 1 {
            let _ = writeln!(content, "message {}:", index + 1);
        }
        render_fields(
            &mut content,
            fields,
            usize::from(decoded.messages.len() > 1),
        );
    }
    let truncated = content.len() > MAX_PREVIEW_BYTES;
    if truncated {
        let mut end = MAX_PREVIEW_BYTES;
        while !content.is_char_boundary(end) {
            end -= 1;
        }
        content.truncate(end);
    }
    RenderedPreview { content, truncated }
}

fn render_fields(output: &mut String, fields: &[WireField], depth: usize) {
    let mut counts = BTreeMap::<u32, usize>::new();
    for field in fields {
        *counts.entry(field.number).or_default() += 1;
    }
    for field in fields {
        let indent = "  ".repeat(depth);
        let repeated =
            (counts[&field.number] > 1).then(|| format!(", repeated ×{}", counts[&field.number]));
        let repeated = repeated.as_deref().unwrap_or_default();
        match &field.value {
            WireValue::Varint(value) => {
                let _ = writeln!(
                    output,
                    "{indent}{}: varint{repeated} = {value}",
                    field.number
                );
            }
            WireValue::Fixed64(value) => {
                let _ = writeln!(
                    output,
                    "{indent}{}: fixed64{repeated} = 0x{value:016x}",
                    field.number
                );
            }
            WireValue::Fixed32(value) => {
                let _ = writeln!(
                    output,
                    "{indent}{}: fixed32{repeated} = 0x{value:08x}",
                    field.number
                );
            }
            WireValue::Group(nested) => {
                let _ = writeln!(output, "{indent}{}: group{repeated} {{", field.number);
                render_fields(output, nested, depth + 1);
                let _ = writeln!(output, "{indent}}}");
            }
            WireValue::LengthDelimited(bytes) => {
                if let Some(nested) = decode_message(bytes, depth + 1) {
                    let _ = writeln!(
                        output,
                        "{indent}{}: length-delimited{repeated} ({} bytes, embedded message) {{",
                        field.number,
                        bytes.len()
                    );
                    render_fields(output, &nested, depth + 1);
                    let _ = writeln!(output, "{indent}}}");
                } else if let Some(text) = std::str::from_utf8(bytes).ok().filter(|text| {
                    text.chars().all(|character| {
                        !character.is_control() || matches!(character, '\n' | '\r' | '\t')
                    })
                }) {
                    let _ = writeln!(
                        output,
                        "{indent}{}: length-delimited{repeated} ({} bytes, UTF-8) = {text:?}",
                        field.number,
                        bytes.len()
                    );
                } else {
                    let _ = writeln!(
                        output,
                        "{indent}{}: length-delimited{repeated} ({} bytes) = {}",
                        field.number,
                        bytes.len(),
                        hex_preview(bytes)
                    );
                }
            }
        }
    }
}

fn hex_preview(bytes: &[u8]) -> String {
    const SHOWN_BYTES: usize = 32;
    let mut output = String::from("0x");
    for byte in bytes.iter().take(SHOWN_BYTES) {
        let _ = write!(output, "{byte:02x}");
    }
    if bytes.len() > SHOWN_BYTES {
        output.push('…');
    }
    output
}

#[cfg(test)]
#[path = "protoscope/tests.rs"]
mod tests;
