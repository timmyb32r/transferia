use bytes::Bytes;

use super::config::{ConversionErrorPolicy, JsonFramingMode};
use transferia_core::data::message::Message;

pub(super) fn frame_json_arrays(
    framing: JsonFramingMode,
    error_policy: ConversionErrorPolicy,
    messages: Vec<Message>,
) -> anyhow::Result<Vec<Message>> {
    if framing != JsonFramingMode::JsonArray {
        return Ok(messages);
    }
    let mut framed_messages = Vec::with_capacity(messages.len());
    for message in messages {
        let values: Vec<serde_json::Value> = match serde_json::from_slice(&message.value) {
            Ok(values) => values,
            Err(error) => match error_policy {
                ConversionErrorPolicy::Dlq => {
                    framed_messages.push(message);
                    continue;
                }
                ConversionErrorPolicy::Drop => continue,
                ConversionErrorPolicy::Fail => {
                    return Err(anyhow::anyhow!("invalid JSON array: {error}"));
                }
            },
        };
        let mut framed = Vec::with_capacity(message.value.len());
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                framed.push(b'\n');
            }
            serde_json::to_writer(&mut framed, value)?;
        }
        framed_messages.push(Message {
            value: Bytes::from(framed),
            tombstone: false,
            key: message.key,
            headers: message.headers,
            meta: message.meta,
        });
    }
    Ok(framed_messages)
}
