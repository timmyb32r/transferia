use apache_avro::Schema as AvroSchema;
use prost::Message as _;
use prost_reflect::DynamicMessage;

use super::*;
use crate::schema_registry::{
    encode_message_indexes, json_to_avro, protobuf_descriptor_pool, RegistrySchema, SchemaFormat,
    SchemaRegistryAuth,
};

#[test]
fn editor_exposes_schema_registry_and_parse_error_policy() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(DebeziumParserConfig))?;
    let properties = schema["properties"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Debezium config properties are absent"))?;
    assert_eq!(properties.keys().collect::<Vec<_>>(), ["connection", "on_parse_error"]);
    assert_eq!(properties["on_parse_error"]["title"], "On Parse Error");
    assert_eq!(properties["connection"]["title"], "Schema Registry");
    Ok(())
}

#[test]
fn schemas_preserve_complete_key_and_row_without_manual_projection() -> anyhow::Result<()> {
    let (incoming, stored) = config().schemas()?;
    assert_eq!(
        stored
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["message_key_base64", "data"]
    );
    assert!(stored.columns[0].primary_key);
    assert!(!stored.columns[0].nullable);
    assert_eq!(stored.columns[1].data_type, DataType::Utf8);
    assert_eq!(incoming.columns.len(), 14);
    assert_eq!(
        incoming.columns[2].old_value_of.as_deref(),
        Some("message_key_base64")
    );
    Ok(())
}

#[test]
fn all_schema_registry_formats_preserve_their_decoded_row_and_common_metadata() -> anyhow::Result<()>
{
    let value = envelope(
        "u",
        &row(7, &Value::from("old")),
        &row(7, &Value::from("new")),
        42,
    );
    for (format, decoded) in registry_decoded_values(&value)? {
        let actual = normalize_envelope(&decoded, br#"{"id":7}"#)?;
        assert_eq!(actual.0["current"][0], "eyJpZCI6N30=", "{format:?}");
        assert_eq!(actual.0["current"][1], decoded["after"], "{format:?}");
        assert_eq!(actual.0["before"][1], decoded["before"], "{format:?}");
        assert_eq!(actual.0["source"]["lsn"], 42, "{format:?}");
        assert_eq!(actual.0["op"], "u", "{format:?}");
        assert_eq!(actual.1, [0b11], "{format:?}");
    }
    Ok(())
}

#[test]
fn normalization_keeps_full_images_and_injective_message_key() -> anyhow::Result<()> {
    let input = envelope(
        "u",
        &serde_json::json!({"id": 1, "nested": {"old": true}}),
        &serde_json::json!({"id": 1, "nested": {"new": true}}),
        1,
    );
    let (normalized, mask) = normalize_envelope(&input, &[0, 255, 65])?;
    assert_eq!(normalized["current"][0], "AP9B");
    assert_eq!(normalized["current"][1], input["after"]);
    assert_eq!(normalized["before"][1], input["before"]);
    assert_eq!(mask, [0b11]);
    Ok(())
}

#[test]
fn normalization_rejects_invalid_shapes_and_unavailable_values() {
    let invalid = envelope("u", &Value::Null, &row(1, &Value::Null), 1);
    assert!(normalize_envelope(&invalid, b"key")
        .unwrap_err()
        .to_string()
        .contains("update before"));
    let unavailable = envelope(
        "c",
        &Value::Null,
        &row(1, &Value::from(UNAVAILABLE_VALUE)),
        1,
    );
    assert!(normalize_envelope(&unavailable, b"key")
        .unwrap_err()
        .to_string()
        .contains("unavailable"));
}

#[test]
fn message_derived_table_is_revalidated_for_every_envelope() {
    let value = envelope("c", &Value::Null, &row(1, &Value::Null), 1);
    validate_message_table(&value, "accounts").unwrap();
    assert!(validate_message_table(&value, "other")
        .unwrap_err()
        .to_string()
        .contains("does not match discovered table"));
}

fn config() -> DebeziumParserConfig {
    DebeziumParserConfig {
        on_parse_error: Default::default(),
        connection: SchemaRegistryConnection {
            url: "http://registry.invalid".to_owned(),
            request_timeout_ms: 1_000,
            auth: SchemaRegistryAuth::None,
            ca_certificate: None,
        },
    }
}

fn row(id: i64, payload: &Value) -> Value {
    serde_json::json!({"id": id, "payload": payload})
}

fn envelope(operation: &str, before: &Value, after: &Value, lsn: i64) -> Value {
    serde_json::json!({
        "before": before,
        "after": after,
        "source": {
            "version": "transferia",
            "connector": "postgresql",
            "name": "inventory",
            "ts_ms": 1_000,
            "ts_us": 1_000_000,
            "ts_ns": 1_000_000_000,
            "db": "postgres",
            "schema": "public",
            "table": "accounts",
            "txId": 9,
            "lsn": lsn
        },
        "op": operation,
        "ts_ms": 2_000,
        "ts_us": 2_000_000,
        "ts_ns": 2_000_000_000
    })
}

fn registry_decoded_values(value: &Value) -> anyhow::Result<Vec<(SchemaFormat, Value)>> {
    let json_schema = RegistrySchema {
        id: 1,
        definition: r#"{"type":"object"}"#.to_owned(),
        format: SchemaFormat::JsonSchema,
        references: Arc::from([]),
    };
    let avro_definition = r#"
        {"type":"record","name":"Envelope","fields":[
          {"name":"before","type":["null",{"type":"record","name":"BeforeRow","fields":[{"name":"id","type":"long"},{"name":"payload","type":["null","string"]}]}]},
          {"name":"after","type":["null",{"type":"record","name":"AfterRow","fields":[{"name":"id","type":"long"},{"name":"payload","type":["null","string"]}]}]},
          {"name":"source","type":{"type":"record","name":"Source","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"db","type":"string"},{"name":"schema","type":"string"},{"name":"table","type":"string"},{"name":"txId","type":"long"},{"name":"lsn","type":"long"},{"name":"ts_ms","type":"long"},{"name":"ts_us","type":"long"},{"name":"ts_ns","type":"long"}]}},
          {"name":"op","type":"string"},{"name":"ts_ms","type":"long"},{"name":"ts_us","type":"long"},{"name":"ts_ns","type":"long"}
        ]}
    "#;
    let avro_schema = RegistrySchema {
        id: 2,
        definition: avro_definition.to_owned(),
        format: SchemaFormat::Avro,
        references: Arc::from([]),
    };
    let protobuf_definition = r#"
        syntax = "proto3";
        package demo;
        message Row { int64 id = 1; string payload = 2; }
        message Source { string version = 1; string connector = 2; string name = 3; string db = 4; string schema = 5; string table = 6; uint64 txId = 7; int64 lsn = 8; int64 ts_ms = 9; int64 ts_us = 10; int64 ts_ns = 11; }
        message Envelope { Row before = 1; Row after = 2; Source source = 3; string op = 4; int64 ts_ms = 5; int64 ts_us = 6; int64 ts_ns = 7; }
    "#;
    let protobuf_schema = RegistrySchema {
        id: 3,
        definition: protobuf_definition.to_owned(),
        format: SchemaFormat::Protobuf,
        references: Arc::from([]),
    };

    let json_payload = serde_json::to_vec(value)?;
    let avro_parsed = AvroSchema::parse_str(avro_definition)?;
    let avro_value = json_to_avro(&avro_parsed, value)?;
    let avro_payload = apache_avro::to_avro_datum(&avro_parsed, avro_value)?;

    let (pool, file_name) = protobuf_descriptor_pool(protobuf_definition, &[])?;
    let file = pool
        .get_file_by_name(&file_name)
        .ok_or_else(|| anyhow::anyhow!("test Protobuf file missing"))?;
    let descriptor = file
        .messages()
        .find(|message| message.name() == "Envelope")
        .ok_or_else(|| anyhow::anyhow!("test Protobuf Envelope missing"))?;
    let protobuf_json = protobuf_json(value.clone());
    let protobuf_json = serde_json::to_vec(&protobuf_json)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&protobuf_json);
    let protobuf = DynamicMessage::deserialize(descriptor, &mut deserializer)?;
    let mut protobuf_payload = Vec::new();
    encode_message_indexes(&[2], &mut protobuf_payload)?;
    protobuf.encode(&mut protobuf_payload)?;

    let mut decoder = SchemaDecoder::default();
    Ok(vec![
        (
            SchemaFormat::JsonSchema,
            decoder.decode(&json_schema, &json_payload)?,
        ),
        (
            SchemaFormat::Avro,
            decoder.decode(&avro_schema, &avro_payload)?,
        ),
        (
            SchemaFormat::Protobuf,
            decoder.decode(&protobuf_schema, &protobuf_payload)?,
        ),
    ])
}

fn protobuf_json(mut value: Value) -> Value {
    fn convert(object: &mut Map<String, Value>, field: &str) {
        if let Some(value) = object.get_mut(field) {
            if let Some(number) = value.as_i64() {
                *value = Value::String(number.to_string());
            } else if let Some(number) = value.as_u64() {
                *value = Value::String(number.to_string());
            }
        }
    }
    let object = value.as_object_mut().unwrap();
    for field in ["ts_ms", "ts_us", "ts_ns"] {
        convert(object, field);
    }
    for row_name in ["before", "after"] {
        if let Some(row) = object.get_mut(row_name).and_then(Value::as_object_mut) {
            convert(row, "id");
        }
    }
    let source = object.get_mut("source").unwrap().as_object_mut().unwrap();
    for field in ["txId", "lsn", "ts_ms", "ts_us", "ts_ns"] {
        convert(source, field);
    }
    value
}
