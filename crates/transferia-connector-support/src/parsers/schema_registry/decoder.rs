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
    Avro(AvroSchema),
    Json(jsonschema::Validator),
    Protobuf(ProtobufSchema),
}

struct AvroSchema {
    writer: Schema,
    references: Vec<Schema>,
    native: Option<Schema>,
}

impl AvroSchema {
    fn decode(&self, payload: &[u8]) -> anyhow::Result<apache_avro::types::Value> {
        let schemata = self.references.iter().chain(std::iter::once(&self.writer)).collect();
        let mut payload = payload;
        let value = apache_avro::from_avro_datum_schemata(&self.writer, schemata, &mut payload, None)?;
        anyhow::ensure!(payload.is_empty(), "Avro payload contains trailing bytes");
        Ok(value)
    }

    fn native_schema(&mut self) -> anyhow::Result<&Schema> {
        if self.native.is_none() {
            let schemata = self.references.iter().chain(std::iter::once(&self.writer)).collect::<Vec<_>>();
            let resolved = apache_avro::schema::ResolvedSchema::try_from(schemata)?;
            self.native = Some(expand_avro(&self.writer, &resolved, &mut Vec::new())?);
        }
        self.native.as_ref().ok_or_else(|| anyhow::anyhow!("native Avro schema is absent"))
    }
}

fn expand_avro(schema: &Schema, resolved: &apache_avro::schema::ResolvedSchema<'_>, ancestors: &mut Vec<apache_avro::schema::Name>) -> anyhow::Result<Schema> {
    Ok(match schema {
        Schema::Ref { name } => {
            let schema = resolved.get_names().get(name).ok_or_else(|| anyhow::anyhow!("Avro named reference is absent"))?;
            expand_avro(schema, resolved, ancestors)?
        }
        Schema::Record(record) => {
            anyhow::ensure!(!ancestors.contains(&record.name), "recursive Avro records have no finite Arrow schema");
            ancestors.push(record.name.clone());
            let mut record = record.clone();
            for field in &mut record.fields { field.schema = expand_avro(&field.schema, resolved, ancestors)?; }
            ancestors.pop();
            Schema::Record(record)
        }
        Schema::Array(array) => {
            let mut array = array.clone();
            array.items = Box::new(expand_avro(&array.items, resolved, ancestors)?);
            Schema::Array(array)
        }
        Schema::Map(map) => {
            let mut map = map.clone();
            map.types = Box::new(expand_avro(&map.types, resolved, ancestors)?);
            Schema::Map(map)
        }
        Schema::Union(union) => Schema::Union(apache_avro::schema::UnionSchema::new(union.variants().iter().map(|schema| expand_avro(schema, resolved, ancestors)).collect::<anyhow::Result<Vec<_>>>()?)?),
        _ => schema.clone(),
    })
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
                crate::schema_registry::avro_to_json(schema.decode(payload)?)
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

    /// Decode using the message's own writer schema. No comparison with any
    /// earlier schema or destination is performed by the parser.
    pub fn decode_arrow(&mut self, schema: &RegistrySchema, payload: &[u8]) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.schemas.entry(schema.id) {
            entry.insert(compile(schema)?);
        }
        match self.schemas.get_mut(&schema.id).ok_or_else(|| anyhow::anyhow!("compiled schema is absent"))? {
            CompiledSchema::Avro(writer) => {
                let value = writer.decode(payload)?;
                super::typed::avro_record(writer.native_schema()?, &value)
            }
            CompiledSchema::Json(validator) => {
                let value = super::typed::parse_json(payload)?;
                validator.validate(&value).map_err(|_| anyhow::anyhow!("JSON Schema validation failed"))?;
                let definition = serde_json::from_str(&schema.definition)?;
                super::typed::json_record(&definition, &value)
            }
            CompiledSchema::Protobuf(compiled) => {
                let (indexes, payload) = decode_message_indexes(payload)?;
                let file = compiled.pool.get_file_by_name(&compiled.file_name)
                    .ok_or_else(|| anyhow::anyhow!("Protobuf schema file is absent"))?;
                let descriptor = message_at_indexes(file.messages().collect(), &indexes)?;
                let message = DynamicMessage::decode(descriptor, payload)?;
                super::typed::protobuf_record(&message)
            }
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
            Ok(CompiledSchema::Avro(AvroSchema { writer: schema, references: schemas, native: None }))
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
