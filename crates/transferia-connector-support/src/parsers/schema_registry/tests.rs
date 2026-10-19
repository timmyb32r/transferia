use apache_avro::types::Value as AvroValue;
use base64::Engine as _;
use prost::Message as _;

use super::decoder::SchemaDecoder;
use crate::schema_registry::{RegistrySchema, RegistrySchemaReference, SchemaFormat};

#[test]
fn schema_registry_public_config_is_complete_and_editable() {
    let public_schema =
        serde_json::to_value(schemars::schema_for!(crate::parsers::config::ParserSchema))
            .expect("public parser schema serializes");
    let public = public_schema.to_string();
    assert!(public.contains("schema_registry"));
    assert!(public.contains("Schema Registry parser"));
    assert!(!public.contains("Confluent Schema Registry parser"));

    let runtime_schema =
        serde_json::to_value(schemars::schema_for!(super::SchemaRegistryParserConfig))
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
    let properties = runtime_schema["properties"]
        .as_object()
        .expect("runtime parser properties are an object");
    assert_eq!(properties.len(), 2);
    assert!(properties.contains_key("on_parse_error"));
    assert!(properties.contains_key("connection"));
    for removed in [
        "json_parser",
        "json_framing",
        "columns",
        "keys",
        "unknown_fields",
    ] {
        assert!(
            !properties.contains_key(removed),
            "Confluent runtime config must not expose {removed}"
        );
    }

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
        references: Arc::from([]),
    };
    let decoded = SchemaDecoder::default().decode(&schema, &datum)?;
    let typed = SchemaDecoder::default().decode_arrow(&schema, &datum)?;
    assert_eq!(typed.schema().field_with_name("i32")?.data_type(), &arrow::datatypes::DataType::Int32);
    assert_eq!(typed.schema().field_with_name("i64")?.data_type(), &arrow::datatypes::DataType::Int64);
    assert_eq!(typed.schema().field_with_name("blob")?.data_type(), &arrow::datatypes::DataType::Binary);
    assert!(typed.column_by_name("optional").unwrap().is_null(0));

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
        references: Arc::from([]),
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
        references: Arc::from([]),
    };
    let decoded = SchemaDecoder::default().decode(&schema, &payload)?;
    assert_eq!(decoded["text"], "hello");
    assert_eq!(decoded["count"], "9");
    assert_eq!(decoded["flag"], true);
    assert_eq!(decoded["tags"], serde_json::json!(["a", "b"]));
    Ok(())
}

#[test]
fn protobuf_decoder_resolves_recursive_schema_registry_references() -> anyhow::Result<()> {
    let common = r#"syntax = "proto3"; package demo; message Meta { string source = 1; }"#;
    let middle = r#"syntax = "proto3"; package demo; import "common.proto"; message Envelope { Meta meta = 1; }"#;
    let root = r#"syntax = "proto3"; package demo; import "middle.proto"; message Event { Envelope envelope = 1; string value = 2; }"#;
    let schema = RegistrySchema {
        id: 4,
        definition: root.to_owned(),
        format: SchemaFormat::Protobuf,
        references: Arc::from([
            RegistrySchemaReference {
                name: "common.proto".to_owned(),
                definition: common.to_owned(),
                format: SchemaFormat::Protobuf,
            },
            RegistrySchemaReference {
                name: "middle.proto".to_owned(),
                definition: middle.to_owned(),
                format: SchemaFormat::Protobuf,
            },
        ]),
    };
    let (pool, file_name) =
        crate::schema_registry::protobuf_descriptor_pool(&schema.definition, &schema.references)?;
    let descriptor = pool
        .get_file_by_name(&file_name)
        .and_then(|file| file.messages().next())
        .ok_or_else(|| anyhow::anyhow!("test event descriptor is missing"))?;
    let mut deserializer = serde_json::Deserializer::from_str(
        r#"{"envelope":{"meta":{"source":"logbroker"}},"value":"payload"}"#,
    );
    let message = prost_reflect::DynamicMessage::deserialize(descriptor, &mut deserializer)?;
    let mut payload = vec![0];
    message.encode(&mut payload)?;
    let decoded = SchemaDecoder::default().decode(&schema, &payload)?;
    assert_eq!(decoded["envelope"]["meta"]["source"], "logbroker");
    assert_eq!(decoded["value"], "payload");
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
use std::sync::Arc;

#[test]
fn typed_protobuf_preserves_int64_binary_and_repeated_fields() -> anyhow::Result<()> {
    use arrow::array::{BinaryArray, Int64Array, ListArray};
    let (definition, descriptor) = protobuf_schema()?;
    let mut message = prost_reflect::DynamicMessage::new(descriptor);
    message.set_field_by_name("count", prost_reflect::Value::I64(i64::MAX));
    message.set_field_by_name("blob", prost_reflect::Value::Bytes(bytes::Bytes::from_static(&[0, 255])));
    message.set_field_by_name("tags", prost_reflect::Value::List(vec![prost_reflect::Value::String("a".into()), prost_reflect::Value::String("b".into())]));
    let mut payload = vec![0];
    message.encode(&mut payload)?;
    let schema = RegistrySchema { id: 41, definition, format: SchemaFormat::Protobuf, references: Arc::from([]) };
    let batch = SchemaDecoder::default().decode_arrow(&schema, &payload)?;
    assert_eq!(batch.column_by_name("count").unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0), i64::MAX);
    assert_eq!(batch.column_by_name("blob").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap().value(0), &[0, 255]);
    assert_eq!(batch.column_by_name("tags").unwrap().as_any().downcast_ref::<ListArray>().unwrap().value_length(0), 2);
    Ok(())
}

#[test]
fn typed_avro_preserves_float_bits_and_empty_records() -> anyhow::Result<()> {
    use arrow::array::Float32Array;
    let definition = r#"{"type":"record","name":"Float","fields":[{"name":"value","type":"float"}]}"#;
    let schema = RegistrySchema { id: 42, definition: definition.into(), format: SchemaFormat::Avro, references: Arc::from([]) };
    let writer = apache_avro::Schema::parse_str(definition)?;
    for value in [f32::NEG_INFINITY, f32::INFINITY, -0.0, f32::NAN] {
        let payload = apache_avro::to_avro_datum(&writer, AvroValue::Record(vec![("value".into(), AvroValue::Float(value))]))?;
        let batch = SchemaDecoder::default().decode_arrow(&schema, &payload)?;
        let actual = batch.column(0).as_any().downcast_ref::<Float32Array>().unwrap().value(0);
        assert_eq!(actual.to_bits(), value.to_bits());
    }
    let empty = RegistrySchema { id: 43, definition: r#"{"type":"record","name":"Empty","fields":[]}"#.into(), format: SchemaFormat::Avro, references: Arc::from([]) };
    let batch = SchemaDecoder::default().decode_arrow(&empty, &[])?;
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 0);
    Ok(())
}

#[tokio::test]
async fn typed_parser_keeps_incompatible_writer_schemas_in_message_order() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use super::{SchemaRegistryParser, SchemaRegistryParserConfig};
    use crate::parsers::{ParserFactory, SystemColumnsConfig};
    use crate::schema_registry::{ConfluentEnvelope, SchemaRegistryConnection, SchemaRegistryAuth};
    use transferia_core::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await?;
                anyhow::ensure!(read > 0, "mock Registry request ended before its headers");
                request.extend_from_slice(&buffer[..read]);
            }
            let request = std::str::from_utf8(&request)?;
            let kind = if request.starts_with("GET /schemas/ids/1 ") { "integer" }
                else if request.starts_with("GET /schemas/ids/2 ") { "string" }
                else { anyhow::bail!("unexpected Registry request path") };
            let schema = serde_json::json!({"type":"object", "required":["id"], "properties":{"id":{"type":kind}}, "additionalProperties":false});
            let body = serde_json::to_vec(&serde_json::json!({"schemaType":"JSON", "schema":schema.to_string()}))?;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await?;
            stream.write_all(&body).await?;
        }
        anyhow::Ok(())
    });
    let parser = Arc::new(SchemaRegistryParser::new(&SchemaRegistryParserConfig {
        on_parse_error: Default::default(),
        connection: SchemaRegistryConnection { url: format!("http://{address}"), request_timeout_ms: 5000, auth: SchemaRegistryAuth::None, ca_certificate: None },
    }, &SystemColumnsConfig::default(), Arc::from("events"))?);
    let parsed = tokio::task::spawn_blocking(move || {
        let mut session = parser.create_session(1024 * 1024);
        let messages = [(1, br#"{"id":42}"#.as_slice()), (2, br#"{"id":"forty-two"}"#.as_slice()), (1, br#"{"id":43}"#.as_slice())]
            .into_iter().map(|(id, payload)| ConfluentEnvelope::encode(id, payload).map(|value| Message::new(value.into())))
            .collect::<anyhow::Result<Vec<_>>>()?;
        session.parse_into(messages)
    }).await??;
    server.await??;
    let (tables, dlq) = parsed;
    assert!(dlq.is_none());
    assert_eq!(tables.len(), 3);
    assert_eq!(tables.iter().map(|table| table.schema.columns[0].data_type.clone()).collect::<Vec<_>>(), [arrow::datatypes::DataType::Int64, arrow::datatypes::DataType::Utf8, arrow::datatypes::DataType::Int64]);
    assert_eq!(tables[0].batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 42);
    assert_eq!(tables[1].batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(0), "forty-two");
    assert_eq!(tables[2].batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 43);
    Ok(())
}

#[test]
fn typed_avro_resolves_named_references_without_json_conversion() -> anyhow::Result<()> {
    use arrow::array::{Int64Array, StructArray};
    let reference = r#"{"type":"record","name":"Meta","namespace":"demo","fields":[{"name":"value","type":"long"}]}"#;
    let definition = r#"{"type":"record","name":"Event","namespace":"demo","fields":[{"name":"meta","type":"demo.Meta"}]}"#;
    let schemas = apache_avro::Schema::parse_list([reference, definition])?;
    let payload = apache_avro::to_avro_datum_schemata(&schemas[1], schemas.iter().collect(), AvroValue::Record(vec![
        ("meta".into(), AvroValue::Record(vec![("value".into(), AvroValue::Long(i64::MIN))])),
    ]))?;
    let schema = RegistrySchema {
        id: 44, definition: definition.into(), format: SchemaFormat::Avro,
        references: Arc::from([RegistrySchemaReference { name: "demo.Meta".into(), definition: reference.into(), format: SchemaFormat::Avro }]),
    };
    let batch = SchemaDecoder::default().decode_arrow(&schema, &payload)?;
    let nested = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(nested.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), i64::MIN);
    Ok(())
}

#[test]
fn typed_avro_preserves_decimal_scale_and_signed_value() -> anyhow::Result<()> {
    use arrow::array::Decimal128Array;
    let definition = r#"{"type":"record","name":"Event","fields":[{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":3}}]}"#;
    let writer = apache_avro::Schema::parse_str(definition)?;
    let value = AvroValue::Decimal(apache_avro::Decimal::from((-12345_i128).to_be_bytes().to_vec()));
    let payload = apache_avro::to_avro_datum(&writer, AvroValue::Record(vec![("amount".into(), value)]))?;
    let schema = RegistrySchema { id: 45, definition: definition.into(), format: SchemaFormat::Avro, references: Arc::from([]) };
    let batch = SchemaDecoder::default().decode_arrow(&schema, &payload)?;
    let amount = batch.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(amount.value(0), -12345);
    assert_eq!(amount.scale(), 3);
    assert_eq!(amount.precision(), 8);
    Ok(())
}

#[test]
fn typed_json_rejects_duplicate_keys_and_integer_overflow() -> anyhow::Result<()> {
    let schema = RegistrySchema {
        id: 46, format: SchemaFormat::JsonSchema, references: Arc::from([]),
        definition: r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}"#.into(),
    };
    let mut decoder = SchemaDecoder::default();
    assert!(decoder.decode_arrow(&schema, br#"{"id":1,"id":2}"#).is_err());
    assert!(decoder.decode_arrow(&schema, br#"{"id":18446744073709551615}"#).is_err());
    let batch = decoder.decode_arrow(&schema, br#"{"id":9223372036854775807}"#)?;
    assert_eq!(batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), i64::MAX);
    Ok(())
}

#[test]
fn registry_discovery_defers_main_schema_instead_of_declaring_a_json_column() -> anyhow::Result<()> {
    let config: crate::parsers::ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: {}\nschema_registry:\n  connection: { url: 'http://127.0.0.1:1', request_timeout_ms: 5000 }\n",
    )?;
    let plan = crate::parsers::ParserPlan::from_config(&config, "events")?;
    let discovery = plan.delivery_discovery(Arc::from("kafka"), transferia_core::SourceTopology::StaticPartitions(vec![0]),
        transferia_core::DeliveryDiscoveryRequest { keep_system_columns: false })?;
    assert_eq!(discovery.schema_origin, transferia_core::SchemaOrigin::ParserRuntime);
    assert!(discovery.datasets.iter().all(|dataset| dataset.role == transferia_core::DatasetRole::DeadLetterQueue));
    Ok(())
}
