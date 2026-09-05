use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use transferia_core::data::{message::Message, schema::{DatasetSchema, SchemaColumn}, table_data::{dlq_name, TableData}};
use transferia_core::data::system_columns::SystemColumns;

/// Explicit handling of malformed records, never transport or resource failures.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnParseError {
    #[default]
    #[schemars(title = "Fail delivery")]
    Fail,
    #[schemars(title = "Send to DLQ")]
    Dlq,
    #[schemars(title = "Drop")]
    Drop,
}

pub(crate) fn message_dlq_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("raw_base64".to_owned(), DataType::Utf8, false),
        SchemaColumn::new("error_message".to_owned(), DataType::Utf8, false),
        SchemaColumn::new("source_write_timestamp_ms".to_owned(), DataType::Int64, true),
        SchemaColumn::new("key_base64".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("headers_json".to_owned(), DataType::Utf8, false),
        SchemaColumn::new("tombstone".to_owned(), DataType::Boolean, false),
        SchemaColumn::new("topic".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("partition".to_owned(), DataType::Int64, true),
        SchemaColumn::new("offset".to_owned(), DataType::Int64, true),
    ])
}

pub(crate) fn message_dlq_bound(messages: &[Message]) -> usize {
    messages.iter().fold(0usize, |sum, message| {
        let payload = message.value.len().saturating_add(message.key.as_ref().map_or(0, |key| key.len()));
        let headers = message.headers.iter().fold(0usize, |sum, header| {
            sum.saturating_add(header.key.len().saturating_mul(6))
                .saturating_add(header.value.as_ref().map_or(0, |value| value.len().saturating_mul(2)))
                .saturating_add(16)
        });
        sum.saturating_add(payload.saturating_mul(4)).saturating_add(headers.saturating_mul(3))
            .saturating_add(message.meta.topic.as_ref().map_or(0, |topic| topic.len()))
            .saturating_add(512)
    })
}

/// Retain the complete queue envelope, including ordered duplicate/null headers.
pub(crate) fn rejected_messages(table: &str, messages: &[Message], memory_limit: usize) -> anyhow::Result<Option<TableData>> {
    if messages.is_empty() { return Ok(None); }
    anyhow::ensure!(message_dlq_bound(messages) <= memory_limit, "parser DLQ exceeds pipeline memory budget");
    let base64 = base64::engine::general_purpose::STANDARD;
    let raw = messages.iter().map(|m| base64.encode(&m.value)).collect::<Vec<_>>();
    let keys = messages.iter().map(|m| m.key.as_ref().map(|key| base64.encode(key))).collect::<Vec<_>>();
    let headers = messages.iter().map(|m| serde_json::to_string(&m.headers.iter().map(|h| {
        (h.key.as_ref(), h.value.as_ref().map(|value| base64.encode(value)))
    }).collect::<Vec<_>>())).collect::<Result<Vec<_>, _>>()?;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(raw.iter())),
        Arc::new(StringArray::from_iter_values(messages.iter().map(|_| "Message decoding failed"))),
        Arc::new(Int64Array::from_iter(messages.iter().map(|m| m.meta.write_timestamp_ms))),
        Arc::new(StringArray::from_iter(keys.iter().map(|k| k.as_deref()))),
        Arc::new(StringArray::from_iter_values(headers.iter())),
        Arc::new(BooleanArray::from_iter(messages.iter().map(|m| Some(m.tombstone)))),
        Arc::new(StringArray::from_iter(messages.iter().map(|m| m.meta.topic.as_deref()))),
        Arc::new(Int64Array::from_iter(messages.iter().map(|m| m.meta.partition))),
        Arc::new(Int64Array::from_iter(messages.iter().map(|m| m.meta.offset))),
    ];
    let fields = message_dlq_schema().columns.into_iter().map(|c| Field::new(c.name, c.data_type, c.nullable)).collect::<Vec<_>>();
    Ok(Some(TableData::new(Arc::from(dlq_name(table)), true,
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?, SystemColumns::default())))
}

impl OnParseError {
    /// Returns whether the caller must retain the original record in DLQ.
    pub fn retain_in_dlq(self, error: anyhow::Error) -> anyhow::Result<bool> {
        match self {
            Self::Fail => Err(error),
            Self::Dlq => Ok(true),
            Self::Drop => Ok(false),
        }
    }
}
