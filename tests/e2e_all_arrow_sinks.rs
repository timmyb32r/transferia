//! Whole-preset startup contracts: unsupported schemas must fail before any
//! destination connection. These negative E2Es complement each connector's real
//! wire/storage E2Es; they do not claim unsupported types can be written.
//! Discard, which accepts the whole preset, runs the real partition pipeline.
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use transferia::core::memory::PipelineMemory;
use transferia::delivery::config::yaml::{Config, DeliveryType};
use transferia::delivery::execution::run_partition_pipeline;
use transferia::delivery::preparation::build_delivery_plan_with;
use transferia::extension::Transferia;
use transferia::metrics::{ParseCounters, SinkCounters};
use transferia::registry::{SinkBuildContext, SourceBuildContext, SourcePhase};

fn sink_config(kind: &str, port: u16) -> Value {
    let connection =
        json!({"type":"on_premise", "host":"127.0.0.1", "port":port, "trusted_plaintext":true});
    let hosts =
        json!({"type":"on_premise", "hosts":["127.0.0.1"], "port":port, "trusted_plaintext":true});
    let storage = json!({"type":"s3", "bucket":"arrow-e2e", "region":"us-east-1", "endpoint":format!("http://127.0.0.1:{port}"), "credentials":{"access_key":"test", "secret_key":"test"}, "path_style_access":true});
    match kind {
        "discard" => json!({}),
        "postgres" | "mysql" => {
            json!({"installation":connection, "database":"arrow_e2e", "username":"test", "password":"test", "create_tables":true})
        }
        "clickhouse" => {
            json!({"installation":{"http_port":port, "type":"on_premise", "hosts":["127.0.0.1"], "port":port, "trusted_plaintext":true}, "database":"default", "username":"default"})
        }
        "opensearch" => {
            json!({"installation":hosts, "auth":{"type":"anonymous"}, "create_indices":true})
        }
        "kafka" => {
            json!({"installation":{"type":"on_premise", "brokers":[format!("127.0.0.1:{port}")], "security":{"type":"plaintext"}}, "topic":{"type":"topic", "topic":"arrow-e2e"}, "serializer":{"type":"json"}})
        }
        "logbroker" => {
            json!({"installation":connection, "auth":{"type":"token", "token":"test"}, "topic":{"type":"topic", "topic_path":"/Root/arrow-e2e"}, "serializer":{"type":"json"}, "driver":"ydb"})
        }
        "s3" => {
            json!({"installation":{"type":"on_premise", "bucket":"arrow-e2e", "endpoint":format!("http://127.0.0.1:{port}"), "region":"us-east-1", "credentials":{"access_key":"test", "secret_key":"test"}}, "path_prefix":"e2e", "format":{"type":"parquet"}})
        }
        "iceberg" => {
            json!({"installation":{"type":"on_premise", "storage":storage}, "catalog":{"uri":format!("http://127.0.0.1:{port}"), "auth":{"type":"none"}}, "namespace":"default", "create_if_missing":true})
        }
        "ydb" => {
            json!({"installation":{"type":"on_premise", "endpoint":format!("grpc://127.0.0.1:{port}"), "trusted_plaintext":true}, "database":"/local", "auth":{"type":"anonymous"}, "tables":[{"path":"/local/events"}]})
        }
        "ytsaurus" => {
            json!({"installation":{"type":"on_premise", "host":"127.0.0.1", "port":port, "trusted_plaintext":true, "trusted_native_rpc_plaintext":false}, "auth":{"type":"token", "token":"test"}, "tables":{"type":"static_tables", "path":"//tmp/arrow-e2e", "replace_tables":false}})
        }
        _ => unreachable!("test sink must be enumerated"),
    }
}

async fn exercise(kind: &str, expected_error: Option<&str>) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    // The listening socket catches accidental destination I/O during preflight.
    // It is deliberately not a fake successful service: any connection fails.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let config: Config = serde_json::from_value(json!({
        "delivery_id":format!("all-arrow-{kind}"), "delivery_name":"All Arrow E2E",
        "delivery_type":"batch", "pipeline_memory_limit_bytes":268435456,
        "durable_storage":{"type":"local_file", "path":directory.path()},
        "source":{"data_generator":{"table_name":"events", "preset":{"type":"all_arrow_datatypes"}, "amount":{"type":"rows", "row_count":17}}},
        "sink":{kind:sink_config(kind, port)}
    }))?;
    let composition = Transferia::public()?;
    let result = tokio::select! {
        connection = listener.accept() => anyhow::bail!("{kind} contacted destination before rejecting unsupported schema: {connection:?}"),
        result = tokio::time::timeout(Duration::from_secs(30), build_delivery_plan_with(config, CancellationToken::new(), &composition)) => result?,
    };
    if let Some(expected) = expected_error {
        let error = match result {
            Ok(_) => anyhow::bail!("{kind} unexpectedly accepted the complete Arrow preset; add a real successful write/read E2E before changing this contract"),
            Err(error) => format!("{error:#}"),
        };
        let boundary = if kind == "s3" {
            "incompatible source/sink configuration"
        } else {
            "delivery violates sink limits"
        };
        assert!(
            error.contains(boundary) && error.contains(expected),
            "{kind}: {error}"
        );
        return Ok(());
    }
    let plan = result?;
    let pipeline = plan.primary()?;
    assert_eq!(
        pipeline.discovery.datasets[0].stored_schema.columns.len(),
        56
    );
    let memory = PipelineMemory::new(1048576);
    let source = pipeline
        .source_connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: DeliveryType::Batch,
            phase: SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: CancellationToken::new(),
            memory: memory.clone(),
            durable: pipeline.durable.clone(),
        })
        .await?;
    let counters = Arc::new(SinkCounters::new());
    let sink = pipeline
        .sink_connector
        .build_sink(SinkBuildContext {
            delivery_name: "All Arrow E2E".into(),
            durable: pipeline.durable.clone(),
            partition_id: 0,
            replay_identity: None,
            finite_source: true,
            counters: Arc::clone(&counters),
            keep_system_columns: true,
            discovery: Arc::clone(&pipeline.discovery),
        })
        .await?;
    tokio::time::timeout(
        Duration::from_secs(30),
        run_partition_pipeline(
            source,
            pipeline.source_connector.parser(),
            Arc::new(Vec::new()),
            sink,
            memory,
            CancellationToken::new(),
            0,
            Arc::new(ParseCounters::new()),
        ),
    )
    .await??;
    assert_eq!(counters.rows_total(), 17);
    Ok(())
}

macro_rules! sink_cases {
    ($($name:ident, $kind:literal, $expected:expr);+ $(;)?) => {
        const SINKS: &[&str] = &[$($kind),+];
        $(
        #[tokio::test]
        async fn $name() -> anyhow::Result<()> { exercise($kind, $expected).await }
        )+
    };
}

sink_cases! {
    all_arrow_to_clickhouse, "clickhouse", Some("Float16");
    all_arrow_to_discard, "discard", None;
    all_arrow_to_iceberg, "iceberg", Some("Date64");
    all_arrow_to_kafka, "kafka", Some("Float16");
    all_arrow_to_logbroker, "logbroker", Some("Float16");
    all_arrow_to_mysql, "mysql", Some("Float16");
    all_arrow_to_opensearch, "opensearch", Some("Float16");
    all_arrow_to_postgres, "postgres", Some("Float16");
    all_arrow_to_s3, "s3", Some("MissingSystemColumn");
    all_arrow_to_ydb, "ydb", Some("Float16");
    all_arrow_to_ytsaurus, "ytsaurus", Some("Float16");
}

#[test]
fn every_registered_sink_has_an_all_arrow_case() -> anyhow::Result<()> {
    let composition = Transferia::public()?;
    let mut actual: Vec<_> = composition
        .composition()
        .connector_definitions()
        .iter()
        .filter(|entry| entry.sink.is_some())
        .map(|entry| entry.key)
        .collect();
    actual.sort_unstable();
    assert_eq!(actual, SINKS);
    Ok(())
}
