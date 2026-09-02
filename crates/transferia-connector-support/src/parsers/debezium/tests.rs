use anyhow::Context as _;
use apache_avro::Schema as AvroSchema;
use arrow::array::{Array as _, BinaryArray, Int64Array, StringArray};
use prost::Message as _;
use prost_reflect::DynamicMessage;

use transferia_core::data::changelog::{project_sink_batch, ChangelogAction, ProjectedSinkBatch};
use transferia_core::data::message::Message;
use transferia_core::delivery::{DeliveryDiscoveryRequest, SourceTopology};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::SinkBatch;
use transferia_delivery_contracts::semantics::{RecordSemantics, SourceBehavior};

use super::*;
use crate::parsers::{CommonParserConfig, ParserPlan, TableNaming};
use crate::schema_registry::{
    encode_message_indexes, json_to_avro, protobuf_descriptor_pool, RegistrySchema, SchemaFormat,
};

#[test]
fn primary_keys_are_an_explicit_required_editor_field() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(DebeziumParserConfig))?;
    let keys = &schema["properties"]["keys"];
    assert_eq!(keys["minItems"], 1);
    assert!(keys.get("x-ui").is_none());
    assert_eq!(keys["title"], "Primary key columns");
    Ok(())
}

#[tokio::test]
async fn json_parser_preserves_changelog_controls_to_sink_projection() -> anyhow::Result<()> {
    let config = config(DebeziumInput::Json);
    let common = CommonParserConfig {
        table_naming: TableNaming::FromConfig {
            name: "accounts".to_owned(),
        },
        system_columns: SystemColumnsConfig::default(),
    };
    let plan = ParserPlan::from_debezium_config(&common, &config, "topic")?;
    assert_eq!(plan.record_semantics(), RecordSemantics::Changelog);
    assert_eq!(plan.source_behavior(), SourceBehavior::ChangelogRows);

    let discovery = plan.delivery_discovery(
        "topic".into(),
        SourceTopology::DynamicWorkerLanes,
        DeliveryDiscoveryRequest {
            keep_system_columns: false,
        },
    )?;
    let dataset = &discovery.datasets[0];
    assert_eq!(
        dataset
            .stored_schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "payload"]
    );
    assert_eq!(dataset.incoming_schema.columns.len(), 17);
    assert_eq!(
        dataset.incoming_schema.columns[2].old_value_of.as_deref(),
        Some("id")
    );

    let parser = plan.parser();
    let mut session = parser.create_session(4 * 1024 * 1024);
    let messages = vec![
        message(&envelope(
            "c",
            &Value::Null,
            &row(1, &Value::from("alpha")),
            10,
        )),
        message(&envelope(
            "u",
            &row(1, &Value::from("alpha")),
            &row(1, &Value::from(UNAVAILABLE_VALUE)),
            11,
        )),
        message(&envelope(
            "d",
            &row(1, &Value::from("alpha")),
            &Value::Null,
            12,
        )),
        Message {
            value: Bytes::new(),
            tombstone: true,
            key: Some(Bytes::from_static(br#"{"id":1}"#)),
            headers: Arc::from([]),
            meta: transferia_core::data::message::MessageMeta::default(),
        },
    ];
    let (main, dlq) = session.parse_into(messages)?;
    assert!(dlq.is_none());
    assert_eq!(main.batch.num_rows(), 3);
    let id = main
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 1, 1]);
    let payload = main
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(payload.is_null(1));
    let changed = main
        .batch
        .column(main.batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(changed.value(0), &[0b11]);
    assert_eq!(changed.value(1), &[0b01]);

    let byte_size = main.batch.get_array_memory_size();
    let sink = SinkBatch {
        table: main.table,
        is_dlq: false,
        batch: main.batch,
        byte_size,
        memory: PipelineMemory::new(byte_size.max(1))
            .reserve(byte_size)
            .await,
        system_columns: main.system_columns,
    };
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &sink)? else {
        anyhow::bail!("Debezium parser output was not recognized as changelog")
    };
    assert_eq!(changelog.operations().len(), 3);
    let runs = changelog.collapsed_runs()?;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].action, ChangelogAction::Delete);
    Ok(())
}

#[test]
fn json_and_all_schema_registry_formats_normalize_identically() -> anyhow::Result<()> {
    let value = envelope(
        "u",
        &row(7, &Value::from("old")),
        &row(7, &Value::from("new")),
        42,
    );
    let paths = [
        jsonpath_lib::Compiled::compile("$.id").map_err(anyhow::Error::msg)?,
        jsonpath_lib::Compiled::compile("$.payload").map_err(anyhow::Error::msg)?,
    ];
    let types = [JsonDataType::Number, JsonDataType::String];
    let fields = ["id".to_owned(), "payload".to_owned()];
    let expected = normalize_envelope(&value, &paths, &types, &fields)?;

    for (format, decoded) in registry_decoded_values(&value)? {
        let actual = normalize_envelope(&decoded, &paths, &types, &fields)
            .with_context(|| format!("normalizing {format:?} Debezium envelope"))?;
        assert_eq!(actual, expected, "{format:?}");
    }
    Ok(())
}

#[test]
fn parser_rejects_unknown_user_fields_and_invalid_event_shapes() -> anyhow::Result<()> {
    let paths = [jsonpath_lib::Compiled::compile("$.id").map_err(anyhow::Error::msg)?];
    let types = [JsonDataType::Number];
    let fields = ["id".to_owned()];
    let unknown = envelope(
        "c",
        &Value::Null,
        &serde_json::json!({"id": 1, "lost": "must fail"}),
        1,
    );
    assert!(normalize_envelope(&unknown, &paths, &types, &fields)
        .unwrap_err()
        .to_string()
        .contains("unmapped field 'lost'"));
    let invalid = envelope("u", &Value::Null, &row(1, &Value::Null), 1);
    assert!(normalize_envelope(&invalid, &paths, &types, &fields)
        .unwrap_err()
        .to_string()
        .contains("update before"));
    Ok(())
}

#[test]
fn parser_requires_a_nonnullable_primary_key() {
    let mut missing_keys = config(DebeziumInput::Json);
    missing_keys.keys.clear();
    assert!(missing_keys
        .schemas()
        .unwrap_err()
        .to_string()
        .contains("primary-key"));

    let mut nullable_key = config(DebeziumInput::Json);
    nullable_key.columns[0].nullable = true;
    assert!(nullable_key
        .schemas()
        .unwrap_err()
        .to_string()
        .contains("must be non-nullable"));
}

#[test]
fn unavailable_nonnullable_value_is_nullable_only_in_the_incoming_schema() -> anyhow::Result<()> {
    let mut config = config(DebeziumInput::Json);
    config.columns[1].nullable = false;
    let (incoming, stored) = config.schemas()?;
    assert!(incoming.columns[1].nullable);
    assert!(!stored.columns[1].nullable);

    let parser = Arc::new(DebeziumParser::new(&config, Arc::from("accounts"))?);
    let mut session = parser.create_session(4 * 1024 * 1024);
    let (batch, _) = session.parse_into(vec![message(&envelope(
        "u",
        &row(1, &Value::from("old")),
        &row(1, &Value::from(UNAVAILABLE_VALUE)),
        11,
    ))])?;
    assert!(batch.batch.column(1).is_null(0));
    Ok(())
}

fn config(input: DebeziumInput) -> DebeziumParserConfig {
    DebeziumParserConfig {
        input,
        columns: vec![
            mapping("$.id", "id", JsonDataType::Number, "Int64", false),
            mapping("$.payload", "payload", JsonDataType::String, "Utf8", true),
        ],
        keys: vec!["id".to_owned()],
    }
}

fn mapping(
    jsonpath: &str,
    column_name: &str,
    json_data_type: JsonDataType,
    arrow_type: &str,
    nullable: bool,
) -> ColumnMapping {
    ColumnMapping {
        jsonpath: jsonpath.to_owned(),
        column_name: column_name.to_owned(),
        json_data_type,
        arrow_type: arrow_type.to_owned(),
        decimal_precision: None,
        decimal_scale: None,
        nullable,
        time_conversion: None,
        low_cardinality: false,
        max_length: None,
    }
}

fn message(value: &Value) -> Message {
    Message::new(Bytes::from(serde_json::to_vec(&value).unwrap()))
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
