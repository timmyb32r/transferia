mod decoder;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::json_parser::{
    ColumnMapping, ConversionErrorPolicy, JsonDataType, JsonFramingMode, JsonParser,
    JsonParserConfig, UnknownFieldPolicy,
};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use crate::schema_registry::{RegistryClient, SchemaRegistryConnection};
use transferia_core::data::message::Message;
use transferia_core::data::table_data::TableData;

pub use decoder::SchemaDecoder;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryParserConfig {
    #[serde(default)]
    #[schemars(title = "On Parse Error", extend("x-ui" = { "labels": {
        "fail": "Fail delivery", "dlq": "Send to DLQ", "drop": "Drop"
    } }))]
    pub on_parse_error: super::error_policy::OnParseError,
    pub connection: SchemaRegistryConnection,
}

pub(crate) fn decoded_record_projection() -> JsonParserConfig {
    JsonParserConfig {
        json_framing: JsonFramingMode::SingleDocument,
        columns: vec![ColumnMapping {
            jsonpath: "$.data".to_owned(),
            column_name: "data".to_owned(),
            json_data_type: JsonDataType::Json,
            arrow_type: "Json".to_owned(),
            decimal_precision: None,
            decimal_scale: None,
            nullable: false,
            time_conversion: None,
            low_cardinality: false,
            max_length: None,
        }],
        conversion_error: ConversionErrorPolicy::Fail,
        unknown_fields: UnknownFieldPolicy::Fail,
        keys: Vec::new(),
    }
}

pub struct SchemaRegistryParser {
    on_parse_error: super::error_policy::OnParseError,
    registry: RegistryClient,
    json: Arc<JsonParser>,
}

impl SchemaRegistryParser {
    pub fn new(
        config: &SchemaRegistryParserConfig,
        system_config: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        let projection = decoded_record_projection();
        Ok(Self {
            on_parse_error: config.on_parse_error,
            registry: RegistryClient::new(&config.connection)?,
            json: Arc::new(JsonParser::new(&projection, system_config, table)?),
        })
    }
}

impl ParserFactory for SchemaRegistryParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(SchemaRegistryParserSession {
            parser: Arc::clone(&self),
            registry: self.registry.clone(),
            decoder: SchemaDecoder::default(),
            json: Arc::clone(&self.json).create_session(memory_limit_bytes),
            runtime: None,
            memory_limit_bytes,
        })
    }
}

struct SchemaRegistryParserSession {
    parser: Arc<SchemaRegistryParser>,
    registry: RegistryClient,
    decoder: SchemaDecoder,
    json: Box<dyn ParserSession>,
    runtime: Option<tokio::runtime::Runtime>,
    memory_limit_bytes: usize,
}

impl SchemaRegistryParserSession {
    fn runtime(&mut self) -> anyhow::Result<&tokio::runtime::Runtime> {
        if self.runtime.is_none() {
            self.runtime = Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            );
        }
        self.runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Schema Registry parser runtime was not initialized"))
    }
}

impl ParserSession for SchemaRegistryParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        let dlq_bound = if self.parser.on_parse_error == super::error_policy::OnParseError::Dlq {
            super::error_policy::message_dlq_bound(messages)
        } else { 0 };
        self.json.output_memory_bound(messages).saturating_add(dlq_bound).saturating_add(
            messages
                .iter()
                .map(|message| message.value.len())
                .sum::<usize>(),
        )
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let mut rejected = Vec::new();
        let mut valid = Vec::with_capacity(messages.len());
        let mut schema_ids = HashSet::new();
        for message in messages {
            match crate::schema_registry::ConfluentEnvelope::decode(&message.value) {
                Ok(envelope) => { schema_ids.insert(envelope.schema_id); valid.push(message); }
                Err(error) => if self.parser.on_parse_error.retain_in_dlq(error)? { rejected.push(message); },
            }
        }
        let registry = self.registry.clone();
        let schemas = self.runtime()?.block_on(async move {
            let mut schemas = HashMap::with_capacity(schema_ids.len());
            for schema_id in schema_ids {
                schemas.insert(schema_id, registry.schema_by_id(schema_id).await?);
            }
            anyhow::Ok(schemas)
        })?;

        let mut decoded = Vec::with_capacity(valid.len());
        let mut decoded_bytes = 0_usize;
        for message in valid {
            let envelope = crate::schema_registry::ConfluentEnvelope::decode(&message.value)?;
            let schema = schemas.get(&envelope.schema_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "prefetched Schema Registry schema id {} is absent",
                    envelope.schema_id
                )
            })?;
            let data = match self.decoder.decode(schema, envelope.payload) {
                Ok(data) => data,
                Err(error) => {
                    if self.parser.on_parse_error.retain_in_dlq(error)? { rejected.push(message); }
                    continue;
                }
            };
            let value = serde_json::json!({ "data": data });
            let value = serde_json::to_vec(&value)?;
            decoded_bytes = decoded_bytes
                .checked_add(value.len())
                .ok_or_else(|| anyhow::anyhow!("decoded Schema Registry delivery size overflow"))?;
            anyhow::ensure!(
                decoded_bytes <= self.memory_limit_bytes,
                "decoded Schema Registry delivery exceeds pipeline memory budget: decoded_bytes={decoded_bytes}, pipeline_memory_limit_bytes={}",
                self.memory_limit_bytes
            );
            decoded.push(Message {
                value: Bytes::from(value),
                tombstone: false,
                key: message.key,
                headers: message.headers,
                meta: message.meta,
            });
        }
        let (main, unexpected_dlq) = self.json.parse_into(decoded)?;
        anyhow::ensure!(unexpected_dlq.is_none(), "normalized Schema Registry projection produced DLQ");
        let dlq = super::error_policy::rejected_messages(&main.table, &rejected, self.memory_limit_bytes)?;
        Ok((main, dlq))
    }
}

#[cfg(test)]
mod tests;
