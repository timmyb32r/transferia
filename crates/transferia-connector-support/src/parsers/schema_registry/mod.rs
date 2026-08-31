mod decoder;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::json_parser::{JsonParser, JsonParserConfig};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use crate::schema_registry::{RegistryClient, SchemaRegistryConnection};
use transferia_core::data::message::Message;
use transferia_core::data::table_data::TableData;

pub use decoder::SchemaDecoder;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryParserConfig {
    pub connection: SchemaRegistryConnection,

    pub json_parser: JsonParserConfig,
}

pub struct SchemaRegistryParser {
    registry: RegistryClient,
    json: Arc<JsonParser>,
}

impl SchemaRegistryParser {
    pub fn new(
        config: &SchemaRegistryParserConfig,
        system_config: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            registry: RegistryClient::new(&config.connection)?,
            json: Arc::new(JsonParser::new(&config.json_parser, system_config, table)?),
        })
    }
}

impl ParserFactory for SchemaRegistryParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(SchemaRegistryParserSession {
            registry: self.registry.clone(),
            decoder: SchemaDecoder::default(),
            json: Arc::clone(&self.json).create_session(memory_limit_bytes),
            runtime: None,
            memory_limit_bytes,
        })
    }
}

struct SchemaRegistryParserSession {
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
        self.json.output_memory_bound(messages).saturating_add(
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
        let schema_ids = messages
            .iter()
            .map(|message| {
                crate::schema_registry::ConfluentEnvelope::decode(&message.value)
                    .map(|envelope| envelope.schema_id)
            })
            .collect::<anyhow::Result<HashSet<_>>>()?;
        let registry = self.registry.clone();
        let schemas = self.runtime()?.block_on(async move {
            let mut schemas = HashMap::with_capacity(schema_ids.len());
            for schema_id in schema_ids {
                schemas.insert(schema_id, registry.schema_by_id(schema_id).await?);
            }
            anyhow::Ok(schemas)
        })?;

        let mut decoded = Vec::with_capacity(messages.len());
        let mut decoded_bytes = 0_usize;
        for message in messages {
            let envelope = crate::schema_registry::ConfluentEnvelope::decode(&message.value)?;
            let schema = schemas.get(&envelope.schema_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "prefetched Schema Registry schema id {} is absent",
                    envelope.schema_id
                )
            })?;
            let value = self.decoder.decode(&schema, envelope.payload)?;
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
                key: message.key,
                headers: message.headers,
                meta: message.meta,
            });
        }
        self.json.parse_into(decoded)
    }
}

#[cfg(test)]
mod tests;
