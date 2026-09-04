use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use transferia_core::data::message::{Message, MessageHeader, MessageMeta};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::data::table_data::{dlq_name, TableData};

use crate::parsers::{ParserFactory, ParserSession};

pub const PRIMARY_KEY: [&str; 3] = ["topic", "partition", "offset"];

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawValueType {
    #[default]
    #[schemars(title = "Bytes")]
    Bytes,
    #[schemars(title = "String")]
    String,
    #[schemars(title = "JSON")]
    Json,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RawToTableParserConfig {
    #[schemars(title = "Key type", default)]
    pub key_type: RawValueType,

    #[schemars(title = "Value type", default)]
    pub value_type: RawValueType,

    #[schemars(
        title = "Add message key",
        description = "Disabling this explicitly discards source message keys",
        default = "default_true"
    )]
    pub preserve_key: bool,

    #[schemars(
        title = "Add headers",
        description = "Disabling this explicitly discards source message headers",
        default = "default_true"
    )]
    pub preserve_headers: bool,

    #[schemars(
        title = "Add write timestamp",
        description = "Disabling this explicitly discards source message write timestamps",
        default = "default_true"
    )]
    pub preserve_write_timestamp: bool,
}

impl Default for RawToTableParserConfig {
    fn default() -> Self {
        Self {
            key_type: RawValueType::Bytes,
            value_type: RawValueType::Bytes,
            preserve_key: true,
            preserve_headers: true,
            preserve_write_timestamp: true,
        }
    }
}

const fn default_true() -> bool {
    true
}

impl RawToTableParserConfig {
    #[must_use]
    pub fn dataset_schema(&self) -> DatasetSchema {
        let mut columns = primary_key_columns();
        if self.preserve_write_timestamp {
            columns.push(SchemaColumn::new(
                "timestamp".into(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ));
        }
        if self.preserve_headers {
            columns.push(
                SchemaColumn::new("headers".into(), DataType::Utf8, false)
                    .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
            );
        }
        if self.preserve_key {
            let mut key = SchemaColumn::new("key".into(), self.key_type.arrow_type(), true);
            if matches!(self.key_type, RawValueType::Json) {
                key = key.with_arrow_extension(ARROW_JSON_EXTENSION_NAME);
            }
            columns.push(key);
        }
        columns.push(SchemaColumn::new(
            "tombstone".into(),
            DataType::Boolean,
            false,
        ));
        let mut value = SchemaColumn::new("value".into(), self.value_type.arrow_type(), true);
        if matches!(self.value_type, RawValueType::Json) {
            value = value.with_arrow_extension(ARROW_JSON_EXTENSION_NAME);
        }
        columns.push(value);
        DatasetSchema::new(columns)
    }
}

impl RawValueType {
    const fn arrow_type(self) -> DataType {
        match self {
            Self::Bytes => DataType::Binary,
            Self::String | Self::Json => DataType::Utf8,
        }
    }

    fn validate(self, value: &[u8], field: &str) -> Result<(), String> {
        match self {
            Self::Bytes => Ok(()),
            Self::String => std::str::from_utf8(value)
                .map(|_| ())
                .map_err(|_| format!("raw_to_table {field} is not valid UTF-8")),
            Self::Json => {
                let mut deserializer = serde_json::Deserializer::from_slice(value);
                serde::de::IgnoredAny::deserialize(&mut deserializer)
                    .and_then(|_| deserializer.end())
                    .map_err(|error| format!("raw_to_table {field} is not valid JSON: {error}"))
            }
        }
    }
}

pub struct RawToTableParser {
    config: RawToTableParserConfig,
    table: Arc<str>,
    arrow_schema: Arc<Schema>,
    dlq_schema: Arc<Schema>,
}

impl RawToTableParser {
    pub fn new(config: &RawToTableParserConfig, table: Arc<str>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !table.is_empty(),
            "raw_to_table table name must not be empty"
        );
        let schema = config.dataset_schema();
        let arrow_schema = dataset_arrow_schema(&schema);
        let dlq_schema = dataset_arrow_schema(&dlq_dataset_schema());
        Ok(Self {
            config: config.clone(),
            table,
            arrow_schema,
            dlq_schema,
        })
    }
}

impl ParserFactory for RawToTableParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(RawToTableParserSession {
            parser: self,
            memory_limit_bytes,
        })
    }
}

struct RawToTableParserSession {
    parser: Arc<RawToTableParser>,
    memory_limit_bytes: usize,
}

impl ParserSession for RawToTableParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        messages.iter().fold(0_usize, |total, message| {
            let headers = message.headers.iter().fold(0_usize, |size, header| {
                size.saturating_add(header.key.len().saturating_mul(6))
                    .saturating_add(
                        header
                            .value
                            .as_ref()
                            .map_or(4, |value| value.len().saturating_mul(2)),
                    )
                    .saturating_add(32)
            });
            total
                .saturating_add(message.value.len())
                .saturating_add(message.key.as_ref().map_or(0, bytes::Bytes::len))
                .saturating_add(message.meta.topic.as_ref().map_or(0, |topic| topic.len()))
                .saturating_add(headers)
                .saturating_add(512)
        })
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let memory_bound = self.output_memory_bound(&messages);
        anyhow::ensure!(
            memory_bound <= self.memory_limit_bytes,
            "raw_to_table output memory bound {memory_bound} exceeds pipeline memory limit {}",
            self.memory_limit_bytes
        );
        parse_messages(&self.parser, &messages)
    }
}

fn parse_messages(
    parser: &RawToTableParser,
    messages: &[Message],
) -> anyhow::Result<(TableData, Option<TableData>)> {
    let mut topic = StringBuilder::new();
    let mut partition = Int64Builder::new();
    let mut offset = Int64Builder::new();
    let mut write_timestamp = parser
        .config
        .preserve_write_timestamp
        .then(TimestampMillisecondBuilder::new);
    let mut headers = parser.config.preserve_headers.then(StringBuilder::new);
    let mut key = parser
        .config
        .preserve_key
        .then(|| RawBuilder::new(parser.config.key_type));
    let mut tombstone = BooleanBuilder::new();
    let mut value = RawBuilder::new(parser.config.value_type);
    let mut rejected = Vec::<(usize, String)>::new();
    let mut headers_scratch = Vec::new();

    for (index, message) in messages.iter().enumerate() {
        let (message_topic, message_partition, message_offset) = required_metadata(&message.meta)?;
        let validation = (!message.tombstone || message.value.is_empty())
            .then_some(())
            .ok_or_else(|| "raw_to_table tombstone carries a nonempty value".to_owned())
            .and_then(|()| {
                if message.tombstone {
                    Ok(())
                } else {
                    parser.config.value_type.validate(&message.value, "value")
                }
            })
            .and_then(|()| {
                if parser.config.preserve_key {
                    if let Some(message_key) = &message.key {
                        parser.config.key_type.validate(message_key, "key")?;
                    }
                }
                Ok(())
            });
        if let Err(error) = validation {
            rejected.push((index, error));
            continue;
        }

        topic.append_value(message_topic);
        partition.append_value(message_partition);
        offset.append_value(message_offset);
        if let Some(builder) = &mut write_timestamp {
            builder.append_option(message.meta.write_timestamp_ms);
        }
        if let Some(builder) = &mut headers {
            write_headers_json(&message.headers, &mut headers_scratch)?;
            builder.append_value(std::str::from_utf8(&headers_scratch)?);
        }
        if let Some(builder) = &mut key {
            builder.append_option(message.key.as_deref())?;
        }
        tombstone.append_value(message.tombstone);
        if message.tombstone {
            value.append_option(None)?;
        } else {
            value.append(&message.value)?;
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(topic.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
    ];
    if let Some(builder) = &mut write_timestamp {
        arrays.push(Arc::new(builder.finish()));
    }
    if let Some(builder) = &mut headers {
        arrays.push(Arc::new(builder.finish()));
    }
    if let Some(builder) = &mut key {
        arrays.push(builder.finish());
    }
    arrays.push(Arc::new(tombstone.finish()));
    arrays.push(value.finish());
    let main = TableData::new(
        Arc::clone(&parser.table),
        false,
        RecordBatch::try_new(Arc::clone(&parser.arrow_schema), arrays)?,
        SystemColumns::default(),
    );

    let dlq = if rejected.is_empty() {
        None
    } else {
        Some(build_dlq(parser, messages, &rejected)?)
    };
    Ok((main, dlq))
}

fn build_dlq(
    parser: &RawToTableParser,
    messages: &[Message],
    rejected: &[(usize, String)],
) -> anyhow::Result<TableData> {
    let mut topic = StringBuilder::new();
    let mut partition = Int64Builder::new();
    let mut offset = Int64Builder::new();
    let mut write_timestamp = TimestampMillisecondBuilder::new();
    let mut headers = StringBuilder::new();
    let mut key = BinaryBuilder::new();
    let mut tombstone = BooleanBuilder::new();
    let mut value = BinaryBuilder::new();
    let mut failure_reason = StringBuilder::new();
    let mut headers_scratch = Vec::new();
    for (index, reason) in rejected {
        let message = &messages[*index];
        let (message_topic, message_partition, message_offset) = required_metadata(&message.meta)?;
        topic.append_value(message_topic);
        partition.append_value(message_partition);
        offset.append_value(message_offset);
        write_timestamp.append_option(message.meta.write_timestamp_ms);
        write_headers_json(&message.headers, &mut headers_scratch)?;
        headers.append_value(std::str::from_utf8(&headers_scratch)?);
        key.append_option(message.key.as_deref());
        tombstone.append_value(message.tombstone);
        value.append_value(&message.value);
        failure_reason.append_value(reason);
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(topic.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
        Arc::new(write_timestamp.finish()),
        Arc::new(headers.finish()),
        Arc::new(key.finish()),
        Arc::new(tombstone.finish()),
        Arc::new(value.finish()),
        Arc::new(failure_reason.finish()),
    ];
    Ok(TableData::new(
        dlq_name(&parser.table).into(),
        true,
        RecordBatch::try_new(Arc::clone(&parser.dlq_schema), arrays)?,
        SystemColumns::default(),
    ))
}

fn required_metadata(meta: &MessageMeta) -> anyhow::Result<(&str, i64, i64)> {
    let topic = meta
        .topic
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("raw_to_table requires source topic metadata"))?;
    let partition = meta
        .partition
        .ok_or_else(|| anyhow::anyhow!("raw_to_table requires source partition metadata"))?;
    let offset = meta
        .offset
        .ok_or_else(|| anyhow::anyhow!("raw_to_table requires source offset metadata"))?;
    Ok((topic, partition, offset))
}

enum RawBuilder {
    Binary(BinaryBuilder),
    String(StringBuilder),
}

impl RawBuilder {
    fn new(value_type: RawValueType) -> Self {
        match value_type {
            RawValueType::Bytes => Self::Binary(BinaryBuilder::new()),
            RawValueType::String | RawValueType::Json => Self::String(StringBuilder::new()),
        }
    }

    fn append(&mut self, value: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Binary(builder) => builder.append_value(value),
            Self::String(builder) => builder.append_value(std::str::from_utf8(value)?),
        }
        Ok(())
    }

    fn append_option(&mut self, value: Option<&[u8]>) -> anyhow::Result<()> {
        match (self, value) {
            (Self::Binary(builder), value) => builder.append_option(value),
            (Self::String(builder), Some(value)) => {
                builder.append_value(std::str::from_utf8(value)?);
            }
            (Self::String(builder), None) => builder.append_null(),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
        }
    }
}

fn write_headers_json(headers: &[MessageHeader], output: &mut Vec<u8>) -> anyhow::Result<()> {
    output.clear();
    output.push(b'[');
    for (index, header) in headers.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        output.extend_from_slice(b"{\"key\":");
        serde_json::to_writer(&mut *output, header.key.as_ref())?;
        output.extend_from_slice(b",\"value_base64\":");
        if let Some(value) = &header.value {
            output.push(b'\"');
            let encoded_len = value
                .len()
                .checked_add(2)
                .and_then(|length| length.checked_div(3))
                .and_then(|length| length.checked_mul(4))
                .ok_or_else(|| anyhow::anyhow!("raw_to_table header encoding size overflow"))?;
            let start = output.len();
            let end = start
                .checked_add(encoded_len)
                .ok_or_else(|| anyhow::anyhow!("raw_to_table header encoding size overflow"))?;
            output.resize(end, 0);
            let written = base64::engine::general_purpose::STANDARD
                .encode_slice(value, &mut output[start..])
                .map_err(|error| anyhow::anyhow!("raw_to_table header encoding failed: {error}"))?;
            anyhow::ensure!(
                written == encoded_len,
                "raw_to_table header encoder returned an unexpected size"
            );
            output.push(b'\"');
        } else {
            output.extend_from_slice(b"null");
        }
        output.push(b'}');
    }
    output.push(b']');
    Ok(())
}

fn primary_key_columns() -> Vec<SchemaColumn> {
    vec![
        SchemaColumn::new("topic".into(), DataType::Utf8, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("partition".into(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("offset".into(), DataType::Int64, false)
            .with_constraints(true, false, None),
    ]
}

#[must_use]
pub fn dlq_dataset_schema() -> DatasetSchema {
    let mut columns = primary_key_columns();
    columns.extend([
        SchemaColumn::new(
            "timestamp".into(),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        SchemaColumn::new("headers".into(), DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
        SchemaColumn::new("key".into(), DataType::Binary, true),
        SchemaColumn::new("tombstone".into(), DataType::Boolean, false),
        SchemaColumn::new("value".into(), DataType::Binary, false),
        SchemaColumn::new("failure_reason".into(), DataType::Utf8, false),
    ]);
    DatasetSchema::new(columns)
}

fn dataset_arrow_schema(schema: &DatasetSchema) -> Arc<Schema> {
    Arc::new(Schema::new(
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(&column.name, column.data_type.clone(), column.nullable)
                    .with_metadata(column.arrow_metadata())
            })
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
#[path = "tests/raw_to_table.rs"]
mod tests;
