use apache_avro::Schema;
use prost::Message as _;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::schema_registry::{
    encode_message_indexes, ConfluentEnvelope, RegistryClient, RegistrySchema, SchemaFormat,
    SchemaRegistryConnection,
};
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::data::schema::{
    SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA,
    SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::sink::Delivery;

use super::debezium::DebeziumJsonEncoder;
use super::{
    JsonBatchEncoder, QueueMessageMode, SerializedBatch, SerializedDelivery, SerializedMessage,
};

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SerializerConfig {
    #[schemars(title = "JSON")]
    Json,

    #[schemars(title = "Debezium JSON")]
    DebeziumJson {
        #[schemars(title = "Logical source name")]
        logical_name: String,
    },

    #[schemars(title = "Schema Registry")]
    SchemaRegistry {
        #[schemars(title = "Connection")]
        connection: SchemaRegistryConnection,

        #[schemars(title = "Subject")]
        subject: String,

        #[schemars(title = "Format")]
        format: SchemaFormat,

        #[serde(default = "default_message_indexes")]
        #[schemars(
            title = "Protobuf message indexes",
            extend("x-ui" = { "section": "advanced" })
        )]
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
            Self::DebeziumJson { logical_name } => {
                anyhow::ensure!(
                    logical_name.trim() == logical_name && !logical_name.is_empty(),
                    "debezium_json.logical_name must be nonempty and must not contain leading or trailing whitespace"
                );
                Ok(())
            }
            Self::SchemaRegistry {
                connection,
                subject,
                format,
                protobuf_message_indexes,
            } => {
                connection.validate()?;
                anyhow::ensure!(
                    subject.trim() == subject && !subject.is_empty(),
                    "schema_registry.subject must be nonempty and must not contain leading or trailing whitespace"
                );
                anyhow::ensure!(
                    *format != SchemaFormat::Protobuf || !protobuf_message_indexes.is_empty(),
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

    pub fn destination_type(
        &self,
        data_type: &arrow::datatypes::DataType,
    ) -> anyhow::Result<String> {
        match self {
            Self::Json => Ok(format!("JSON {}", json_type_name(data_type)?)),
            Self::DebeziumJson { .. } => {
                Ok(format!("Debezium JSON {}", json_type_name(data_type)?))
            }
            Self::SchemaRegistry {
                subject, format, ..
            } => Ok(format!(
                "{} ({})",
                match format {
                    SchemaFormat::Avro => "Avro",
                    SchemaFormat::JsonSchema => "JSON Schema",
                    SchemaFormat::Protobuf => "Protobuf",
                },
                subject
            )),
        }
    }

    #[must_use]
    pub const fn supports_changelog(&self) -> bool {
        matches!(self, Self::DebeziumJson { .. })
    }

    pub fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        if !self.supports_changelog() {
            return Ok(());
        }
        let datasets = discovery
            .datasets
            .iter()
            .filter(|dataset| dataset.role == transferia_core::delivery::DatasetRole::Main)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !datasets.is_empty(),
            "Debezium serializer requires at least one main dataset"
        );
        for dataset in datasets {
            let primary_keys = dataset
                .incoming_schema
                .columns
                .iter()
                .filter(|column| column.primary_key)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                !primary_keys.is_empty(),
                "Debezium dataset '{}' requires at least one primary-key column",
                dataset.name
            );
            for key in primary_keys {
                let mappings = dataset
                    .incoming_schema
                    .columns
                    .iter()
                    .filter(|column| {
                        column.old_key_of.as_deref() == Some(key.name.as_str())
                            || column.old_value_of.as_deref() == Some(key.name.as_str())
                    })
                    .count();
                anyhow::ensure!(
                    mappings == 1,
                    "Debezium dataset '{}' primary key '{}' must have exactly one old-key or old-value mapping, found {mappings}",
                    dataset.name,
                    key.name
                );
            }
            for kind in [
                SystemColumnKind::Offset,
                SystemColumnKind::ChangeOperation,
                SystemColumnKind::ChangedColumns,
            ] {
                anyhow::ensure!(
                    dataset.system_columns.iter().any(|column| column.kind == kind),
                    "Debezium dataset '{}' is missing required {kind:?} metadata",
                    dataset.name
                );
            }
            for (role, data_type) in [
                (SYSTEM_ROLE_SOURCE_DATABASE, arrow::datatypes::DataType::Utf8),
                (SYSTEM_ROLE_SOURCE_SCHEMA, arrow::datatypes::DataType::Utf8),
                (SYSTEM_ROLE_SOURCE_TABLE, arrow::datatypes::DataType::Utf8),
                (
                    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                    arrow::datatypes::DataType::UInt64,
                ),
                (SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, arrow::datatypes::DataType::Int64),
                (SYSTEM_ROLE_SOURCE_TIMESTAMP_US, arrow::datatypes::DataType::Int64),
                (SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, arrow::datatypes::DataType::Int64),
                (SYSTEM_ROLE_EVENT_TIMESTAMP_MS, arrow::datatypes::DataType::Int64),
                (SYSTEM_ROLE_EVENT_TIMESTAMP_US, arrow::datatypes::DataType::Int64),
                (SYSTEM_ROLE_EVENT_TIMESTAMP_NS, arrow::datatypes::DataType::Int64),
            ] {
                let matches = dataset
                    .incoming_schema
                    .columns
                    .iter()
                    .filter(|column| column.system_role.as_deref() == Some(role))
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    matches.len() == 1,
                    "Debezium dataset '{}' must have exactly one '{role}' control column, found {}",
                    dataset.name,
                    matches.len()
                );
                anyhow::ensure!(
                    matches[0].data_type == data_type && !matches[0].nullable,
                    "Debezium dataset '{}' control role '{role}' must be non-nullable {data_type:?}",
                    dataset.name,
                );
            }
        }
        Ok(())
    }
}

fn json_type_name(data_type: &arrow::datatypes::DataType) -> anyhow::Result<&'static str> {
    use arrow::datatypes::DataType;
    Ok(match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _) => "string",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => "number",
        DataType::Boolean => "boolean",
        other => anyhow::bail!("Arrow type {other:?} has no JSON serializer representation"),
    })
}

pub struct DeliverySerializer {
    kind: SerializerKind,
}

enum SerializerKind {
    Json,
    DebeziumJson(DebeziumJsonEncoder),
    SchemaRegistry {
        registry: RegistryClient,
        subject: String,
        format: SchemaFormat,
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
    pub fn new(config: &SerializerConfig, mode: QueueMessageMode) -> anyhow::Result<Self> {
        config.validate()?;
        let kind = match config {
            SerializerConfig::Json => SerializerKind::Json,
            SerializerConfig::DebeziumJson { logical_name } => {
                SerializerKind::DebeziumJson(DebeziumJsonEncoder::new(logical_name.clone(), mode))
            }
            SerializerConfig::SchemaRegistry {
                connection,
                subject,
                format,
                protobuf_message_indexes,
            } => SerializerKind::SchemaRegistry {
                registry: RegistryClient::new(connection)?,
                subject: subject.clone(),
                format: *format,
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
    ) -> anyhow::Result<SerializedDelivery> {
        if let SerializerKind::SchemaRegistry {
            registry,
            subject,
            format,
            message_indexes,
            schema,
        } = &mut self.kind
        {
            if schema.is_none() {
                **schema = Some(compile_writer_schema(
                    &registry.latest_schema(subject, *format).await?,
                    message_indexes,
                )?);
            }
        }

        let mut batches = Vec::with_capacity(delivery.outputs.len());
        let mut rows = 0_u64;
        for batch in &delivery.outputs {
            limits.validate_batch(discovery, batch)?;
            if let SerializerKind::DebeziumJson(encoder) = &self.kind {
                batches.push(encoder.encode_batch(batch, message_size_limit)?);
                rows = rows
                    .checked_add(u64::try_from(batch.rows())?)
                    .ok_or_else(|| anyhow::anyhow!("queue sink row counter overflow"))?;
                continue;
            }
            let encoder = JsonBatchEncoder::new(&batch.batch, |index| {
                !batch
                    .system_columns
                    .iter()
                    .any(|column| column.index == index)
            })?;
            let mut payloads = Vec::with_capacity(batch.rows());
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
                    SerializerKind::DebeziumJson(_) => {
                        unreachable!("Debezium batches are handled before row serialization")
                    }
                };
                validate_payload_size(&output, message_size_limit)?;
                payloads.push(SerializedMessage {
                    key: None,
                    value: Some(output),
                });
            }
            batches.push(SerializedBatch {
                table: std::sync::Arc::clone(&batch.table),
                messages: payloads,
            });
            rows = rows
                .checked_add(batch.rows() as u64)
                .ok_or_else(|| anyhow::anyhow!("queue sink row counter overflow"))?;
        }
        Ok(SerializedDelivery {
            batches,
            source_rows: rows,
        })
    }
}

fn validate_payload_size(payload: &[u8], message_size_limit: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        payload.len() <= message_size_limit,
        "serialized queue message exceeds configured transport limit: message_bytes={}, transport_limit_bytes={message_size_limit}",
        payload.len()
    );
    Ok(())
}

fn compile_writer_schema(
    schema: &RegistrySchema,
    message_indexes: &[i32],
) -> anyhow::Result<CompiledWriterSchema> {
    match schema.format {
        SchemaFormat::Avro => Ok(CompiledWriterSchema::Avro {
            id: schema.id,
            schema: {
                let definitions = schema
                    .references
                    .iter()
                    .map(|reference| reference.definition.as_str())
                    .chain(std::iter::once(schema.definition.as_str()))
                    .collect::<Vec<_>>();
                Schema::parse_list(definitions)?
                    .pop()
                    .ok_or_else(|| anyhow::anyhow!("Schema Registry returned no Avro schema"))?
            },
        }),
        SchemaFormat::JsonSchema => {
            let definition = serde_json::from_str::<serde_json::Value>(&schema.definition)?;
            let resources = schema
                .references
                .iter()
                .map(|reference| {
                    let definition = serde_json::from_str(&reference.definition)?;
                    let resource = jsonschema::Resource::from_contents(definition)?;
                    Ok((reference.name.clone(), resource))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(CompiledWriterSchema::Json {
                id: schema.id,
                validator: jsonschema::options()
                    .with_resources(resources.into_iter())
                    .build(&definition)?,
            })
        }
        SchemaFormat::Protobuf => {
            let (pool, file_name) = crate::schema_registry::protobuf_descriptor_pool(
                &schema.definition,
                &schema.references,
            )?;
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
