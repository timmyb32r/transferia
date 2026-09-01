use std::time::Duration;

use apache_avro::types::Value as AvroValue;
use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use prost::Message as _;
use serde::Deserialize;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use transferia_connector_support::parsers::{ParserConfig, ParserPlan};
use transferia_connector_support::schema_registry::{
    ConfluentEnvelope, SchemaFormat, SchemaRegistryAuth, SchemaRegistryConnection,
};
use transferia_core::data::message::Message;

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
async fn schema_registry_decodes_all_confluent_formats_losslessly() -> anyhow::Result<()> {
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

    register(
        &client,
        &base_url,
        "common-value",
        "PROTOBUF",
        "syntax = \"proto3\"; package common; message Meta { string source = 1; }",
        &[],
    )
    .await?;

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
            "syntax = \"proto3\"; package demo; import \"common.proto\"; message Event { int32 id = 1; string name = 2; bool enabled = 3; common.Meta meta = 4; }",
        ),
    ];

    for (index, (format, registry_format, definition)) in schemas.into_iter().enumerate() {
        let subject = format!("events-{index}-value");
        let references = if format == SchemaFormat::Protobuf {
            vec![serde_json::json!({
                "name": "common.proto",
                "subject": "common-value",
                "version": 1
            })]
        } else {
            Vec::new()
        };
        let id = register(
            &client,
            &base_url,
            &subject,
            registry_format,
            definition,
            &references,
        )
        .await?;
        let raw = encode_source(format, id, definition)?;
        let parser = parser_config(&base_url)?;
        let plan = ParserPlan::from_config(&parser, "events")?;
        let (table, dlq) = parse_one(&plan, raw).await?;
        assert!(dlq.is_none());
        assert_row(&table.batch);
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
    references: &[serde_json::Value],
) -> anyhow::Result<i32> {
    let response = client
        .post(format!("{base_url}/subjects/{subject}/versions"))
        .json(&serde_json::json!({
            "schemaType": schema_type,
            "schema": schema,
            "references": references
        }))
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
                "connection": connection(base_url)
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
    let data = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("decoded data is an Arrow JSON string");
    let value: serde_json::Value =
        serde_json::from_str(data.value(0)).expect("decoded data is valid JSON");
    assert_eq!(value["id"], 42);
    assert_eq!(value["name"], "answer");
    assert_eq!(value["enabled"], true);
}
