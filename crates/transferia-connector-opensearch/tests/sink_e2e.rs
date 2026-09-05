#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_connector_opensearch::opensearch::sink::{
    OpenSearchSinkConfig, OpenSearchSinkConnector, RoutedIdentity,
};
use transferia_connector_opensearch::opensearch::{OpenSearchAuth, OpenSearchConnectionConfig};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_registry::{SinkBuildContext, SinkConnector as _, SinkPrepare};

const IMAGE: &str = "opensearchproject/opensearch";
const TAG: &str = "3.2.0";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

fn schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false),
    ])
}

fn discovery(name: &str) -> Arc<DeliveryDiscovery> {
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("opensearch-sink-e2e"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            namespace: None,
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name: Arc::from(name),
            incoming_schema: schema(),
            stored_schema: schema(),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    })
}

fn sink_batch(
    memory: &PipelineMemory,
    ids: Vec<i64>,
    payloads: Vec<&str>,
) -> anyhow::Result<SinkBatch> {
    let id = SchemaColumn::new("id".to_owned(), DataType::Int64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(id.arrow_metadata()),
            Field::new("payload", DataType::Utf8, false).with_metadata(payload.arrow_metadata()),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: memory.reserve_transform(bytes),
        system_columns: SystemColumns::default(),
    })
}

fn envelope_discovery() -> Arc<DeliveryDiscovery> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false).with_constraints(
            true,
            false,
            Some(512),
        ),
        SchemaColumn::new("_routing".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("_source".to_owned(), DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
        SchemaColumn::new("_routing_key".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, None),
    ]);
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("opensearch-source-e2e"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            namespace: None,
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name: Arc::from("roundtrip"),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    })
}

fn envelope_batch(memory: &PipelineMemory) -> anyhow::Result<SinkBatch> {
    let discovery = envelope_discovery();
    let fields = discovery.datasets[0]
        .stored_schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(StringArray::from(vec!["raw-1", "raw-1"])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("route-a"), Some("route-b")])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                r#"{ "v": 7, "tenant": "a" }"#,
                r#"{ "v": 8, "tenant": "b" }"#,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec!["route-a", "route-b"])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(SinkBatch {
        table: Arc::from("roundtrip"),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: memory.reserve_transform(bytes),
        system_columns: SystemColumns::default(),
    })
}

#[tokio::test]
async fn opensearch_sink_commits_strict_bulk_rejects_duplicates_and_preserves_routed_identity(
) -> anyhow::Result<()> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9200.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/")
                .with_port(9200.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .with_startup_timeout(Duration::from_mins(3))
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(9200.tcp()).await?;
    let config = OpenSearchSinkConfig {
        connection: OpenSearchConnectionConfig {
            hosts: vec![host.clone()],
            port,
            trusted_plaintext: true,
            tls_ca_file: None,
            auth: OpenSearchAuth::Anonymous,
            request_timeout_ms: 30_000,
            max_response_bytes: 16 * 1024 * 1024,
        },
        create_indices: true,
        routed_identity: RoutedIdentity::Fail,
        bulk_target_rows: 2,
        bulk_target_bytes: 1024 * 1024,
        bulk_concurrency: 2,
        flush_interval_ms: 10,
        retry_initial_ms: 10,
        retry_max_ms: 1_000,
        retry_max_attempts: 5,
    };
    let connection = config.connection.clone();
    let connector = OpenSearchSinkConnector::from_config(config)?;
    let discovery = discovery("events");
    connector.limits().validate_discovery(&discovery)?;
    connector
        .prepare(SinkPrepare::from_discovery(&discovery, true, "e2e", None)?.expect("dataset"))
        .await?;

    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            partition_id: 0,
            delivery_name: "test delivery".into(),
            replay_identity: None,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(2);
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![sink_batch(&memory, vec![1, 2], vec!["one", "two"])?],
            meta: DeliveryMeta { source_messages: 1 },
        })
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(2),
            outputs: vec![sink_batch(
                &memory,
                vec![1, 1],
                vec!["must-not-overwrite", "must-not-overwrite-either"],
            )?],
            meta: DeliveryMeta { source_messages: 1 },
        })
        .await?;
    drop(delivery_tx);
    let duplicate = task
        .await?
        .expect_err("duplicate primary key must fail closed");
    assert!(!duplicate.is_retryable());

    let http = reqwest::Client::new();
    http.post(format!("http://{host}:{port}/events/_refresh"))
        .send()
        .await?
        .error_for_status()?;
    let result: serde_json::Value = http
        .get(format!("http://{host}:{port}/events/_search"))
        .query(&[("size", "10")])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(result["hits"]["total"]["value"], 2);
    let mut rows = result["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| {
            (
                hit["_source"]["id"].as_i64().expect("id"),
                hit["_source"]["payload"]
                    .as_str()
                    .expect("payload")
                    .to_owned(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, [(1, "one".to_owned()), (2, "two".to_owned())]);

    http.put(format!("http://{host}:{port}/roundtrip"))
        .json(&serde_json::json!({
            "settings": { "index": { "translog": { "durability": "request" } } },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "v": { "type": "integer" },
                    "tenant": { "type": "keyword" }
                }
            }
        }))
        .send()
        .await?
        .error_for_status()?;
    let envelope_connector = OpenSearchSinkConnector::from_config(OpenSearchSinkConfig {
        connection,
        create_indices: false,
        routed_identity: RoutedIdentity::EncodeIdentity,
        bulk_target_rows: 1,
        bulk_target_bytes: 1024 * 1024,
        bulk_concurrency: 1,
        flush_interval_ms: 10,
        retry_initial_ms: 10,
        retry_max_ms: 1_000,
        retry_max_attempts: 5,
    })?;
    let envelope_discovery = envelope_discovery();
    envelope_connector
        .limits()
        .validate_discovery(&envelope_discovery)?;
    envelope_connector
        .prepare(
            SinkPrepare::from_discovery(&envelope_discovery, true, "roundtrip", None)?
                .expect("dataset"),
        )
        .await?;
    let envelope_sink = envelope_connector
        .build_sink(SinkBuildContext {
            partition_id: 0,
            delivery_name: "test delivery".into(),
            replay_identity: None,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: envelope_discovery,
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let task = tokio::spawn(envelope_sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![envelope_batch(&memory)?],
            meta: DeliveryMeta { source_messages: 1 },
        })
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    drop(delivery_tx);
    task.await??;
    http.post(format!("http://{host}:{port}/roundtrip/_refresh"))
        .send()
        .await?
        .error_for_status()?;
    let roundtrip: serde_json::Value = http
        .get(format!("http://{host}:{port}/roundtrip/_search"))
        .query(&[("size", "10")])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(roundtrip["hits"]["total"]["value"], 2);
    let hits = roundtrip["hits"]["hits"].as_array().expect("hits");
    assert_ne!(hits[0]["_id"], hits[1]["_id"]);
    assert!(hits.iter().all(|hit| hit["_id"] != "raw-1"));
    let mut sources = hits
        .iter()
        .map(|hit| hit["_source"].clone())
        .collect::<Vec<_>>();
    sources.sort_by_key(|source| source["v"].as_i64());
    assert_eq!(
        sources,
        [
            serde_json::json!({ "v": 7, "tenant": "a" }),
            serde_json::json!({ "v": 8, "tenant": "b" }),
        ]
    );
    Ok(())
}
