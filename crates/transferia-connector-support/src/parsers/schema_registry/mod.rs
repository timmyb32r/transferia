mod decoder;
mod typed;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use schemars::JsonSchema;
use serde::Deserialize;

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

pub struct SchemaRegistryParser {
    on_parse_error: super::error_policy::OnParseError,
    registry: RegistryClient,
    table: Arc<str>,
    system_config: SystemColumnsConfig,
}

impl SchemaRegistryParser {
    pub fn new(
        config: &SchemaRegistryParserConfig,
        system_config: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            on_parse_error: config.on_parse_error,
            registry: RegistryClient::new(&config.connection)?,
            table,
            system_config: system_config.clone(),
        })
    }
}

impl ParserFactory for SchemaRegistryParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(SchemaRegistryParserSession {
            parser: Arc::clone(&self),
            registry: self.registry.clone(),
            decoder: SchemaDecoder::default(),
            runtime: None,
            memory_limit_bytes,
        })
    }
}

struct SchemaRegistryParserSession {
    parser: Arc<SchemaRegistryParser>,
    registry: RegistryClient,
    decoder: SchemaDecoder,
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
        let dlq = if self.parser.on_parse_error == super::error_policy::OnParseError::Dlq {
            super::error_policy::message_dlq_bound(messages)
        } else { 0 };
        messages.iter().fold(dlq, |bytes, message| {
            bytes.saturating_add(message.value.len().saturating_mul(64)).saturating_add(1024)
        })
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(Vec<TableData>, Option<TableData>)> {
        let mut rejected = Vec::new();
        let mut valid = Vec::with_capacity(messages.len());
        let mut schema_ids = HashSet::new();
        for message in messages {
            match crate::schema_registry::ConfluentEnvelope::decode(&message.value) {
                Ok(envelope) => {
                    schema_ids.insert(envelope.schema_id);
                    valid.push(message);
                }
                Err(error) => {
                    if self.parser.on_parse_error.retain_in_dlq(error)? {
                        rejected.push(message);
                    }
                }
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

        let mut groups: Vec<Vec<arrow::record_batch::RecordBatch>> = Vec::new();
        let mut decoded_bytes = 0_usize;
        for message in valid {
            let envelope = crate::schema_registry::ConfluentEnvelope::decode(&message.value)?;
            let schema = schemas.get(&envelope.schema_id).ok_or_else(|| anyhow::anyhow!("prefetched Registry schema is absent"))?;
            let batch = match self.decoder.decode_arrow(schema, envelope.payload) {
                Ok(batch) => batch,
                Err(error) => {
                    if self.parser.on_parse_error.retain_in_dlq(error)? { rejected.push(message); }
                    continue;
                }
            };
            let batch = with_system_columns(batch, &message, &self.parser.system_config)?;
            decoded_bytes = decoded_bytes.checked_add(batch.get_array_memory_size())
                .ok_or_else(|| anyhow::anyhow!("decoded Registry batch size overflow"))?;
            anyhow::ensure!(decoded_bytes <= self.memory_limit_bytes, "decoded Registry batches exceed pipeline memory budget");
            if let Some(group) = groups.last_mut().filter(|group| group.first().is_some_and(|previous| previous.schema() == batch.schema())) {
                group.push(batch);
            } else { groups.push(vec![batch]); }
        }
        let mut tables = Vec::with_capacity(groups.len());
        let mut live_bytes = decoded_bytes;
        for mut group in groups {
            let batch = if group.len() == 1 {
                group.pop().ok_or_else(|| anyhow::anyhow!("empty Registry schema group"))?
            } else {
                let retained = group.iter().try_fold(0_usize, |bytes, batch| bytes.checked_add(batch.get_array_memory_size()))
                    .ok_or_else(|| anyhow::anyhow!("Registry compaction size overflow"))?;
                let peak = live_bytes.checked_add(retained).ok_or_else(|| anyhow::anyhow!("Registry compaction peak overflow"))?;
                anyhow::ensure!(peak <= self.memory_limit_bytes, "Registry batch compaction exceeds pipeline memory budget");
                let first = group.first().ok_or_else(|| anyhow::anyhow!("empty Registry schema group"))?;
                let batch = arrow::compute::concat_batches(&first.schema(), &group)?;
                drop(group);
                live_bytes = live_bytes.checked_sub(retained).and_then(|bytes| bytes.checked_add(batch.get_array_memory_size()))
                    .ok_or_else(|| anyhow::anyhow!("Registry compacted size overflow"))?;
                batch
            };
            let system_columns = system_columns(&batch, &self.parser.system_config)?;
            tables.push(TableData::new(Arc::clone(&self.parser.table), false, batch, system_columns)?);
        }
        let dlq = super::error_policy::rejected_messages(&self.parser.table, &rejected, self.memory_limit_bytes)?;
        Ok((tables, dlq))
    }
}

fn system_columns(batch: &arrow::record_batch::RecordBatch, config: &SystemColumnsConfig) -> anyhow::Result<transferia_core::SystemColumns> {
    let columns = config.enabled().map(|kind| {
        let name = config.name(kind);
        Ok(transferia_core::SystemColumn { kind, index: batch.schema().index_of(name)?, name: Arc::from(name) })
    }).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(transferia_core::SystemColumns::new(columns))
}

fn with_system_columns(batch: arrow::record_batch::RecordBatch, message: &Message, config: &SystemColumnsConfig) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use transferia_core::SystemColumnKind;
    if config.enabled().next().is_none() { return Ok(batch); }
    let mut fields = batch.schema().fields().to_vec();
    let mut arrays = batch.columns().to_vec();
    for kind in config.enabled() {
        let name = config.name(kind);
        anyhow::ensure!(fields.iter().all(|field| field.name() != name), "Registry schema field conflicts with configured system column '{name}'");
        let missing = || anyhow::anyhow!("Registry message lacks required metadata for {kind:?}");
        let array: ArrayRef = match kind {
            SystemColumnKind::Topic => Arc::new(StringArray::from(vec![message.meta.topic.as_deref().ok_or_else(missing)?])),
            SystemColumnKind::Partition => Arc::new(Int64Array::from(vec![message.meta.partition.ok_or_else(missing)?])),
            SystemColumnKind::Offset => Arc::new(Int64Array::from(vec![message.meta.offset.ok_or_else(missing)?])),
            SystemColumnKind::WriteTimestampMs => Arc::new(Int64Array::from(vec![message.meta.write_timestamp_ms.ok_or_else(missing)?])),
            _ => anyhow::bail!("unsupported Schema Registry system column {kind:?}"),
        };
        fields.push(Arc::new(arrow::datatypes::Field::new(name, kind.data_type(), false)));
        arrays.push(array);
    }
    Ok(arrow::record_batch::RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays)?)
}

#[cfg(test)]
mod tests;
