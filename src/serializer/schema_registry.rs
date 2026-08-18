use apache_avro::Schema;
use prost::Message as _;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::core::delivery::{DeliveryDiscovery, SinkLimits};
use crate::core::sink::Delivery;
use crate::schema_registry::{
    encode_message_indexes, ConfluentEnvelope, RegistryClient, RegistrySchema, SchemaFormat,
    SchemaRegistryConnection,
};

use super::JsonBatchEncoder;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SerializerConfig {
    #[schemars(title = "JSON")]
    Json,

    #[schemars(title = "Schema Registry")]
    SchemaRegistry {
        connection: SchemaRegistryConnection,

        #[serde(default = "default_message_indexes")]
        protobuf_message_indexes: Vec<i32>,
    },
}

fn default_message_indexes() -> Vec<i32> {
    vec![0]
}

impl SerializerConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Json => Ok(()),
            Self::SchemaRegistry {
                connection,
                protobuf_message_indexes,
            } => {
                connection.validate()?;
                anyhow::ensure!(
                    connection.format != SchemaFormat::Protobuf
                        || !protobuf_message_indexes.is_empty(),
                    "schema_registry protobuf_message_indexes must not be empty"
                );
                anyhow::ensure!(
                    protobuf_message_indexes.iter().all(|index| *index >= 0),
                    "schema_registry protobuf_message_indexes must be nonnegative"
                );
                Ok(())
            }
        }
    }
}

pub struct DeliverySerializer {
    kind: SerializerKind,
}

enum SerializerKind {
    Json,
    SchemaRegistry {
        registry: RegistryClient,
        message_indexes: Vec<i32>,
        schema: Box<Option<CompiledWriterSchema>>,
    },
}

enum CompiledWriterSchema {
    Avro {
        id: i32,
        schema: Schema,
    },
    Json {
        id: i32,
        validator: jsonschema::Validator,
    },
    Protobuf {
        id: i32,
        descriptor: MessageDescriptor,
    },
}

impl DeliverySerializer {
    pub fn new(config: &SerializerConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let kind = match config {
            SerializerConfig::Json => SerializerKind::Json,
            SerializerConfig::SchemaRegistry {
                connection,
                protobuf_message_indexes,
            } => SerializerKind::SchemaRegistry {
                registry: RegistryClient::new(connection)?,
                message_indexes: protobuf_message_indexes.clone(),
                schema: Box::new(None),
            },
        };
        Ok(Self { kind })
    }

    pub async fn serialize(
        &mut self,
        delivery: &Delivery,
        discovery: &DeliveryDiscovery,
        limits: &dyn SinkLimits,
        message_size_limit: usize,
    ) -> anyhow::Result<(Vec<Vec<u8>>, u64)> {
        if let SerializerKind::SchemaRegistry {
            registry,
            message_indexes,
            schema,
        } = &mut self.kind
        {
            if schema.is_none() {
                **schema = Some(compile_writer_schema(
                    &registry.latest_schema().await?,
                    message_indexes,
                )?);
            }
        }

        let mut payloads = Vec::new();
        let mut rows = 0_u64;
        for batch in &delivery.outputs {
            limits.validate_batch(discovery, batch)?;
            let encoder = JsonBatchEncoder::new(&batch.batch, |index| {
                !batch
                    .system_columns
                    .iter()
                    .any(|column| column.index == index)
            })?;
            for row in 0..batch.rows() {
                let mut json = Vec::new();
                encoder.write_row(row, &mut json);
                let newline = json.pop();
                anyhow::ensure!(
                    newline == Some(b'\n'),
                    "JSON serializer omitted row delimiter"
                );
                let output = match &self.kind {
                    SerializerKind::Json => {
                        json.push(b'\n');
                        json
                    }
                    SerializerKind::SchemaRegistry {
                        message_indexes,
                        schema,
                        ..
                    } => encode_registered(
                        schema.as_ref().as_ref().ok_or_else(|| {
                            anyhow::anyhow!("Schema Registry writer schema was not initialized")
                        })?,
                        message_indexes,
                        &json,
                    )?,
                };
                anyhow::ensure!(
                    output.len() <= message_size_limit,
                    "Logbroker serialized message exceeds configured transport limit: message_bytes={}, transport_limit_bytes={message_size_limit}",
                    output.len()
                );
                payloads.push(output);
            }
            rows = rows
                .checked_add(batch.rows() as u64)
                .ok_or_else(|| anyhow::anyhow!("Logbroker sink row counter overflow"))?;
        }
        Ok((payloads, rows))
    }
}

fn compile_writer_schema(
    schema: &RegistrySchema,
    message_indexes: &[i32],
) -> anyhow::Result<CompiledWriterSchema> {
    match schema.format {
        SchemaFormat::Avro => Ok(CompiledWriterSchema::Avro {
            id: schema.id,
            schema: Schema::parse_str(&schema.definition)?,
        }),
        SchemaFormat::JsonSchema => {
            let definition = serde_json::from_str::<serde_json::Value>(&schema.definition)?;
            Ok(CompiledWriterSchema::Json {
                id: schema.id,
                validator: jsonschema::validator_for(&definition)?,
            })
        }
        SchemaFormat::Protobuf => {
            let (pool, file_name) =
                crate::schema_registry::protobuf_descriptor_pool(&schema.definition)?;
            let file = pool.get_file_by_name(&file_name).ok_or_else(|| {
                anyhow::anyhow!("Protobuf schema file is absent from descriptor pool")
            })?;
            let descriptor = message_at_indexes(file.messages().collect(), message_indexes)?;
            Ok(CompiledWriterSchema::Protobuf {
                id: schema.id,
                descriptor,
            })
        }
    }
}

fn encode_registered(
    schema: &CompiledWriterSchema,
    message_indexes: &[i32],
    json: &[u8],
) -> anyhow::Result<Vec<u8>> {
    match schema {
        CompiledWriterSchema::Avro { id, schema } => {
            let value: serde_json::Value = serde_json::from_slice(json)?;
            let value = crate::schema_registry::json_to_avro(schema, &value)?;
            ConfluentEnvelope::encode(*id, &apache_avro::to_avro_datum(schema, value)?)
        }
        CompiledWriterSchema::Json { id, validator } => {
            let value = serde_json::from_slice(json)?;
            validator
                .validate(&value)
                .map_err(|error| anyhow::anyhow!("JSON Schema validation failed: {error}"))?;
            ConfluentEnvelope::encode(*id, json)
        }
        CompiledWriterSchema::Protobuf { id, descriptor } => {
            let mut deserializer = serde_json::Deserializer::from_slice(json);
            let message = DynamicMessage::deserialize(descriptor.clone(), &mut deserializer)?;
            let mut payload = Vec::new();
            encode_message_indexes(message_indexes, &mut payload)?;
            message.encode(&mut payload)?;
            ConfluentEnvelope::encode(*id, &payload)
        }
    }
}

fn message_at_indexes(
    mut messages: Vec<MessageDescriptor>,
    indexes: &[i32],
) -> anyhow::Result<MessageDescriptor> {
    let mut selected = None;
    for index in indexes {
        let index = usize::try_from(*index)?;
        let message = messages
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Protobuf message index {index} is out of range"))?;
        messages = message.child_messages().collect();
        selected = Some(message);
    }
    selected.ok_or_else(|| anyhow::anyhow!("Protobuf message-index array is empty"))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
