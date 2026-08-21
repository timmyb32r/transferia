use std::sync::Arc;
use std::time::Duration;

use apache_avro::types::Value as AvroValue;
use arrow::array::{BooleanArray, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use prost::Message as _;
use serde::Deserialize;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use transferia_core::data::message::Message;
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{DeliveryDiscoveryRequest, SourceTopology, NO_LIMITS};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch};
use transferia_connector_support::parsers::{ParserConfig, ParserPlan};
use transferia_connector_support::schema_registry::{
    ConfluentEnvelope, SchemaFormat, SchemaRegistryAuth, SchemaRegistryConnection,
};
use transferia_connector_support::serializer::{DeliverySerializer, SerializerConfig};

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str = "v24.3.18";

#[derive(Deserialize)]
struct RegisteredSchema {
    id: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProtoEvent {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(bool, tag = "3")]
    enabled: bool,
}

#[tokio::test]
async fn schema_registry_round_trips_all_confluent_formats() -> anyhow::Result<()> {
    let container = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
        .with_exposed_port(8081.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/subjects")
                .with_port(8081.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_startup_timeout(Duration::from_mins(3))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--memory",
            "1G",
            "--reserve-memory",
            "0M",
            "--kafka-addr",
            "0.0.0.0:9092",
            "--advertise-kafka-addr",
            "127.0.0.1:9092",
            "--schema-registry-addr",
            "0.0.0.0:8081",
        ])
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(8081.tcp()).await?;
    let base_url = format!("http://{host}:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let schemas = [
        (
            SchemaFormat::Avro,
            "AVRO",
            r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"name","type":["null","string"],"default":null},{"name":"enabled","type":"boolean"}]}"#,
        ),
        (
            SchemaFormat::JsonSchema,
            "JSON",
            r#"{"type":"object","required":["id","name","enabled"],"properties":{"id":{"type":"integer"},"name":{"type":["string","null"]},"enabled":{"type":"boolean"}},"additionalProperties":false}"#,
        ),
        (
            SchemaFormat::Protobuf,
            "PROTOBUF",
            "syntax = \"proto3\"; package demo; message Event { int32 id = 1; string name = 2; bool enabled = 3; }",
        ),
    ];

    for (index, (format, registry_format, definition)) in schemas.into_iter().enumerate() {
        let subject = format!("events-{index}-value");
        let id = register(&client, &base_url, &subject, registry_format, definition).await?;
        let raw = encode_source(format, id, definition)?;
        let parser = parser_config(&base_url)?;
        let plan = ParserPlan::from_config(&parser, "events")?;
        let (table, dlq) = parse_one(&plan, raw).await?;
        assert!(dlq.is_none());
        assert_row(&table.batch);

        let discovery = plan.delivery_discovery(
            Arc::from("schema-registry"),
            SourceTopology::StaticPartitions(vec![0]),
            DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
        )?;
        let memory = PipelineMemory::new(16 * 1024 * 1024);
        let byte_size = table.batch.get_array_memory_size();
        let delivery = Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from("events"),
                is_dlq: false,
                batch: table.batch,
                byte_size,
                memory: memory.reserve_transform(byte_size),
                system_columns: SystemColumns::default(),
            }],
            meta: DeliveryMeta { source_messages: 1 },
        };
        let mut serializer = DeliverySerializer::new(&SerializerConfig::SchemaRegistry {
            connection: connection(&base_url),
            subject: subject.clone(),
            format,
            protobuf_message_indexes: vec![0],
        })?;
        let (payloads, rows) = serializer
            .serialize(&delivery, &discovery, &NO_LIMITS, 16 * 1024 * 1024)
            .await?;
        assert_eq!(rows, 1);
        assert_eq!(payloads.len(), 1);
        let (round_trip, dlq) = parse_one(&plan, payloads[0].clone()).await?;
        assert!(dlq.is_none());
        assert_row(&round_trip.batch);
    }
    Ok(())
}

async fn parse_one(
    plan: &ParserPlan,
    payload: Vec<u8>,
) -> anyhow::Result<(
    transferia_core::data::table_data::TableData,
    Option<transferia_core::data::table_data::TableData>,
)> {
    let mut session = plan.parser().create_session(16 * 1024 * 1024);
    tokio::task::spawn_blocking(move || session.parse_into(vec![Message::new(payload.into())]))
        .await?
}

async fn register(
    client: &reqwest::Client,
    base_url: &str,
    subject: &str,
    schema_type: &str,
    schema: &str,
) -> anyhow::Result<i32> {
    let response = client
        .post(format!("{base_url}/subjects/{subject}/versions"))
        .json(&serde_json::json!({ "schemaType": schema_type, "schema": schema }))
        .send()
        .await?
        .error_for_status()?
        .json::<RegisteredSchema>()
        .await?;
    Ok(response.id)
}

fn connection(base_url: &str) -> SchemaRegistryConnection {
    SchemaRegistryConnection {
        url: base_url.to_owned(),
        request_timeout_ms: 30_000,
        auth: SchemaRegistryAuth::None,
        ca_certificate: None,
    }
}

fn parser_config(base_url: &str) -> anyhow::Result<ParserConfig> {
    Ok(serde_yaml::from_value(serde_yaml::to_value(
        serde_json::json!({
            "common": {
                "table_naming": { "type": "from_config", "name": "events" },
                "system_columns": {}
            },
            "schema_registry": {
                "connection": connection(base_url),
                "json_parser": {
                    "json_framing": "single_document",
                    "columns": [
                        { "jsonpath": "$.id", "column_name": "id", "json_data_type": "number", "arrow_type": "Int32", "nullable": false },
                        { "jsonpath": "$.name", "column_name": "name", "json_data_type": "string", "arrow_type": "Utf8", "nullable": false },
                        { "jsonpath": "$.enabled", "column_name": "enabled", "json_data_type": "boolean", "arrow_type": "Boolean", "nullable": false }
                    ],
                    "conversion_error": "fail",
                    "unknown_fields": { "action": "drop" },
                    "keys": ["id"]
                }
            }
        }),
    )?)?)
}

fn encode_source(format: SchemaFormat, id: i32, definition: &str) -> anyhow::Result<Vec<u8>> {
    let payload = match format {
        SchemaFormat::Avro => {
            let schema = apache_avro::Schema::parse_str(definition)?;
            apache_avro::to_avro_datum(
                &schema,
                AvroValue::Record(vec![
                    ("id".to_owned(), AvroValue::Int(42)),
                    (
                        "name".to_owned(),
                        AvroValue::Union(1, Box::new(AvroValue::String("answer".to_owned()))),
                    ),
                    ("enabled".to_owned(), AvroValue::Boolean(true)),
                ]),
            )?
        }
        SchemaFormat::JsonSchema => br#"{"id":42,"name":"answer","enabled":true}"#.to_vec(),
        SchemaFormat::Protobuf => {
            let mut payload = vec![0];
            ProtoEvent {
                id: 42,
                name: "answer".to_owned(),
                enabled: true,
            }
            .encode(&mut payload)?;
            payload
        }
    };
    ConfluentEnvelope::encode(id, &payload)
}

fn assert_row(batch: &RecordBatch) {
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| array.value(0)),
        Some(42)
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(0)),
        Some("answer")
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| array.value(0)),
        Some(true)
    );
}
