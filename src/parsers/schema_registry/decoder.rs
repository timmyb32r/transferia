use std::collections::HashMap;

use apache_avro::Schema;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde_json::Value;

use crate::schema_registry::{decode_message_indexes, RegistrySchema, SchemaFormat};

#[derive(Default)]
pub struct SchemaDecoder {
    schemas: HashMap<i32, CompiledSchema>,
}

enum CompiledSchema {
    Avro(Schema),
    Json(jsonschema::Validator),
    Protobuf(ProtobufSchema),
}

struct ProtobufSchema {
    pool: DescriptorPool,
    file_name: String,
}

impl SchemaDecoder {
    pub fn decode(&mut self, schema: &RegistrySchema, payload: &[u8]) -> anyhow::Result<Value> {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.schemas.entry(schema.id) {
            entry.insert(compile(schema)?);
        }
        match self
            .schemas
            .get(&schema.id)
            .ok_or_else(|| anyhow::anyhow!("compiled schema disappeared from cache"))?
        {
            CompiledSchema::Avro(schema) => {
                let mut payload = payload;
                let value = apache_avro::from_avro_datum(schema, &mut payload, None)?;
                anyhow::ensure!(payload.is_empty(), "Avro payload contains trailing bytes");
                crate::schema_registry::avro_to_json(value)
            }
            CompiledSchema::Json(validator) => {
                let value = serde_json::from_slice(payload)?;
                validator
                    .validate(&value)
                    .map_err(|error| anyhow::anyhow!("JSON Schema validation failed: {error}"))?;
                Ok(value)
            }
            CompiledSchema::Protobuf(schema) => decode_protobuf(schema, payload),
        }
    }
}

fn compile(schema: &RegistrySchema) -> anyhow::Result<CompiledSchema> {
    match schema.format {
        SchemaFormat::Avro => Ok(CompiledSchema::Avro(Schema::parse_str(&schema.definition)?)),
        SchemaFormat::JsonSchema => {
            let definition = serde_json::from_str::<Value>(&schema.definition)?;
            Ok(CompiledSchema::Json(jsonschema::validator_for(
                &definition,
            )?))
        }
        SchemaFormat::Protobuf => {
            let (pool, file_name) =
                crate::schema_registry::protobuf_descriptor_pool(&schema.definition)?;
            Ok(CompiledSchema::Protobuf(ProtobufSchema { pool, file_name }))
        }
    }
}

fn decode_protobuf(schema: &ProtobufSchema, payload: &[u8]) -> anyhow::Result<Value> {
    let (indexes, payload) = decode_message_indexes(payload)?;
    let file = schema
        .pool
        .get_file_by_name(&schema.file_name)
        .ok_or_else(|| anyhow::anyhow!("Protobuf schema file is absent from descriptor pool"))?;
    let descriptor = message_at_indexes(file.messages().collect(), &indexes)?;
    let message = DynamicMessage::decode(descriptor, payload)?;
    Ok(serde_json::to_value(message)?)
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
