use apache_avro::types::Value as AvroValue;
use base64::Engine as _;
use prost::Message as _;

use super::decoder::SchemaDecoder;
use crate::schema_registry::{RegistrySchema, SchemaFormat};

#[test]
fn schema_registry_runtime_config_is_explicit_but_not_published_to_the_ui() {
    let public_schema = serde_json::to_value(schemars::schema_for!(
        crate::parsers::config::ParserSchema
    ))
    .expect("public parser schema serializes");
    assert!(!public_schema.to_string().contains("schema_registry"));

    let runtime_schema = serde_json::to_value(schemars::schema_for!(
        super::SchemaRegistryParserConfig
    ))
    .expect("runtime parser schema serializes");
    let connection = runtime_schema
        .pointer("/$defs/SchemaRegistryConnection/properties")
        .and_then(serde_json::Value::as_object)
        .expect("runtime schema contains Schema Registry connection properties");
    for field in ["url", "auth", "ca_certificate"] {
        assert!(
            connection.contains_key(field),
            "runtime schema is missing {field}"
        );
    }
    assert!(!connection.contains_key("subject"));
    assert!(!connection.contains_key("format"));
    assert_eq!(
        runtime_schema.pointer("/properties/json_parser/x-ui/widget"),
        None
    );
}

#[test]
fn avro_decoder_preserves_supported_scalar_and_nullable_values() -> anyhow::Result<()> {
    let definition = r#"{
      "type":"record","name":"Event","fields":[
        {"name":"text","type":"string"},
        {"name":"i32","type":"int"},
        {"name":"i64","type":"long"},
        {"name":"f32","type":"float"},
        {"name":"f64","type":"double"},
        {"name":"flag","type":"boolean"},
        {"name":"blob","type":"bytes"},
        {"name":"optional","type":["null","string"],"default":null}
      ]
    }"#;
    let avro_schema = apache_avro::Schema::parse_str(definition)?;
    let datum = apache_avro::to_avro_datum(
        &avro_schema,
        AvroValue::Record(vec![
            ("text".to_owned(), AvroValue::String("hello".to_owned())),
            ("i32".to_owned(), AvroValue::Int(7)),
            ("i64".to_owned(), AvroValue::Long(8)),
            ("f32".to_owned(), AvroValue::Float(1.5)),
            ("f64".to_owned(), AvroValue::Double(2.5)),
            ("flag".to_owned(), AvroValue::Boolean(true)),
            ("blob".to_owned(), AvroValue::Bytes(vec![1, 2, 3])),
            (
                "optional".to_owned(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
        ]),
    )?;
    let schema = RegistrySchema {
        id: 1,
        definition: definition.to_owned(),
        format: SchemaFormat::Avro,
    };
    let decoded = SchemaDecoder::default().decode(&schema, &datum)?;
    assert_eq!(decoded["text"], "hello");
    assert_eq!(decoded["i32"], 7);
    assert_eq!(decoded["i64"], 8);
    assert_eq!(decoded["flag"], true);
    assert!(decoded["optional"].is_null());
    Ok(())
}

#[test]
fn json_schema_decoder_enforces_constraints() -> anyhow::Result<()> {
    let schema = RegistrySchema {
        id: 2,
        definition: r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}},"additionalProperties":false}"#.to_owned(),
        format: SchemaFormat::JsonSchema,
    };
    let mut decoder = SchemaDecoder::default();
    assert_eq!(decoder.decode(&schema, br#"{"id":42}"#)?["id"], 42);
    assert!(decoder.decode(&schema, br#"{"id":"wrong"}"#).is_err());
    Ok(())
}

#[test]
fn protobuf_decoder_supports_scalar_repeated_and_bytes_fields() -> anyhow::Result<()> {
    let (definition, descriptor) = protobuf_schema()?;
    let mut deserializer = serde_json::Deserializer::from_str(
        r#"{"text":"hello","count":"9","ratio":1.25,"flag":true,"blob":"AQID","tags":["a","b"]}"#,
    );
    let message = prost_reflect::DynamicMessage::deserialize(descriptor, &mut deserializer)?;
    let mut payload = vec![0];
    message.encode(&mut payload)?;
    let schema = RegistrySchema {
        id: 3,
        definition,
        format: SchemaFormat::Protobuf,
    };
    let decoded = SchemaDecoder::default().decode(&schema, &payload)?;
    assert_eq!(decoded["text"], "hello");
    assert_eq!(decoded["count"], "9");
    assert_eq!(decoded["flag"], true);
    assert_eq!(decoded["tags"], serde_json::json!(["a", "b"]));
    Ok(())
}

fn protobuf_schema() -> anyhow::Result<(String, prost_reflect::MessageDescriptor)> {
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };

    let fields = [
        ("text", 1, Label::Optional, Type::String),
        ("count", 2, Label::Optional, Type::Int64),
        ("ratio", 3, Label::Optional, Type::Double),
        ("flag", 4, Label::Optional, Type::Bool),
        ("blob", 5, Label::Optional, Type::Bytes),
        ("tags", 6, Label::Repeated, Type::String),
    ]
    .into_iter()
    .map(|(name, number, label, r#type)| FieldDescriptorProto {
        name: Some(name.to_owned()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(r#type as i32),
        json_name: Some(name.to_owned()),
        ..Default::default()
    })
    .collect();
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
    let definition = base64::engine::general_purpose::STANDARD.encode(encoded);
    let mut pool = prost_reflect::DescriptorPool::new();
    pool.add_file_descriptor_proto(file)?;
    let descriptor = pool
        .get_message_by_name("demo.Event")
        .ok_or_else(|| anyhow::anyhow!("test descriptor is missing"))?;
    Ok((definition, descriptor))
}
