use super::*;
use base64::Engine as _;

#[test]
fn protobuf_serializer_rejects_empty_message_path() {
    let config = SerializerConfig::SchemaRegistry {
        connection: crate::schema_registry::SchemaRegistryConnection {
            urls: vec!["http://registry".to_owned()],
            subject: "topic-value".to_owned(),
            format: crate::schema_registry::SchemaFormat::Protobuf,
            request_timeout_ms: 1_000,
            auth: crate::schema_registry::SchemaRegistryAuth::None,
        },
        protobuf_message_indexes: Vec::new(),
    };
    assert!(config.validate().is_err());
}

#[test]
fn json_schema_serializer_rejects_values_outside_contract() -> anyhow::Result<()> {
    let schema = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 11,
            definition: r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}},"additionalProperties":false}"#.to_owned(),
            format: crate::schema_registry::SchemaFormat::JsonSchema,
        },
        &[0],
    )?;
    assert!(encode_registered(&schema, &[0], br#"{"id":1}"#).is_ok());
    assert!(encode_registered(&schema, &[0], br#"{"id":"bad"}"#).is_err());
    Ok(())
}

#[test]
fn avro_and_protobuf_serializers_emit_confluent_envelopes() -> anyhow::Result<()> {
    let avro_definition = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}]}"#;
    let avro = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 12,
            definition: avro_definition.to_owned(),
            format: crate::schema_registry::SchemaFormat::Avro,
        },
        &[0],
    )?;
    let encoded = encode_registered(&avro, &[0], br#"{"id":7,"name":null}"#)?;
    let envelope = crate::schema_registry::ConfluentEnvelope::decode(&encoded)?;
    assert_eq!(envelope.schema_id, 12);
    let schema = apache_avro::Schema::parse_str(avro_definition)?;
    let mut payload = envelope.payload;
    let value = apache_avro::from_avro_datum(&schema, &mut payload, None)?;
    let value: serde_json::Value = apache_avro::from_value(&value)?;
    assert_eq!(value["id"], 7);
    assert!(value["name"].is_null());

    let protobuf_definition = protobuf_definition()?;
    let protobuf = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 13,
            definition: protobuf_definition,
            format: crate::schema_registry::SchemaFormat::Protobuf,
        },
        &[0],
    )?;
    let encoded = encode_registered(
        &protobuf,
        &[0],
        br#"{"id":"8","name":"event","enabled":true}"#,
    )?;
    let envelope = crate::schema_registry::ConfluentEnvelope::decode(&encoded)?;
    assert_eq!(envelope.schema_id, 13);
    let (indexes, payload) = crate::schema_registry::decode_message_indexes(envelope.payload)?;
    assert_eq!(indexes, [0]);
    assert!(!payload.is_empty());
    Ok(())
}

fn protobuf_definition() -> anyhow::Result<String> {
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };

    let fields = [
        ("id", Type::Int64),
        ("name", Type::String),
        ("enabled", Type::Bool),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (name, r#type))| {
        Ok(FieldDescriptorProto {
            name: Some(name.to_owned()),
            number: Some(i32::try_from(index + 1)?),
            label: Some(Label::Optional as i32),
            r#type: Some(r#type as i32),
            json_name: Some(name.to_owned()),
            ..Default::default()
        })
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
    let file = FileDescriptorProto {
        name: Some("event.proto".to_owned()),
        package: Some("demo".to_owned()),
        syntax: Some("proto3".to_owned()),
        message_type: vec![DescriptorProto {
            name: Some("Event".to_owned()),
            field: fields,
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut encoded = Vec::new();
    file.encode(&mut encoded)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(encoded))
}
