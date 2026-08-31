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

    pub fn decode_named_protobuf(
        &mut self,
        schema: &RegistrySchema,
        message_name: &str,
        payload: &[u8],
    ) -> anyhow::Result<Value> {
        anyhow::ensure!(
            schema.format == SchemaFormat::Protobuf,
            "named Protobuf decoding requires a PROTOBUF schema"
        );
        anyhow::ensure!(
            !message_name.is_empty(),
            "Protobuf message name must not be empty"
        );
        if let std::collections::hash_map::Entry::Vacant(entry) = self.schemas.entry(schema.id) {
            entry.insert(compile(schema)?);
        }
        let CompiledSchema::Protobuf(compiled) = self
            .schemas
            .get(&schema.id)
            .ok_or_else(|| anyhow::anyhow!("compiled schema disappeared from cache"))?
        else {
            anyhow::bail!("named Protobuf decoding requires a PROTOBUF schema");
        };
        let descriptor = compiled
            .pool
            .get_message_by_name(message_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Protobuf schema id {} does not declare message '{message_name}'",
                    schema.id
                )
            })?;
        let message = DynamicMessage::decode(descriptor, payload)?;
        Ok(serde_json::to_value(message)?)
    }
}

fn compile(schema: &RegistrySchema) -> anyhow::Result<CompiledSchema> {
    match schema.format {
        SchemaFormat::Avro => {
            let definitions = schema
                .references
                .iter()
                .map(|reference| reference.definition.as_str())
                .chain(std::iter::once(schema.definition.as_str()))
                .collect::<Vec<_>>();
            let mut schemas = Schema::parse_list(definitions)?;
            let schema = schemas
                .pop()
                .ok_or_else(|| anyhow::anyhow!("Schema Registry returned no Avro schema"))?;
            Ok(CompiledSchema::Avro(schema))
        }
        SchemaFormat::JsonSchema => {
            let definition = serde_json::from_str::<Value>(&schema.definition)?;
            let resources = schema
                .references
                .iter()
                .map(|reference| {
                    let definition = serde_json::from_str(&reference.definition)?;
                    let resource = jsonschema::Resource::from_contents(definition)?;
                    Ok((reference.name.clone(), resource))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(CompiledSchema::Json(
                jsonschema::options()
                    .with_resources(resources.into_iter())
                    .build(&definition)?,
            ))
        }
        SchemaFormat::Protobuf => {
            let (pool, file_name) = crate::schema_registry::protobuf_descriptor_pool(
                &schema.definition,
                &schema.references,
            )?;
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
