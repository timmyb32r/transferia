#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::ObjectStore as _;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use transferia::connectors::clickhouse::{ClickHouseSinkConfig, ClickHouseSinkConnector};
use transferia::connectors::discard::connector::DiscardSinkConnector;
use transferia::connectors::s3::sink::{S3SinkConfig, S3SinkConnector};
use transferia::core::data::schema::{DatasetSchema, SchemaColumn};
use transferia::core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia::core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin};
use transferia::core::memory::PipelineMemory;
use transferia::core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia::metrics::SinkCounters;
use transferia::registry::{SinkBuildContext, SinkConnector as _, SinkPrepare};

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.8.28.1";
const LOCALSTACK_IMAGE: &str = "localstack/localstack";
const LOCALSTACK_TAG: &str = "4.14.0";

fn dataset_schema(columns: &[(&str, DataType, bool)]) -> DatasetSchema {
    DatasetSchema::new(
        columns
            .iter()
            .map(|(name, data_type, nullable)| {
                SchemaColumn::new((*name).to_owned(), data_type.clone(), *nullable)
            })
            .collect(),
    )
}

fn discovery(
    source_name: &str,
    incoming: DatasetSchema,
    stored: DatasetSchema,
    system_columns: &[SystemColumnKind],
    keep_system_columns: bool,
) -> Arc<DeliveryDiscovery> {
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from(source_name),
        source_topology: transferia::core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns,
        datasets: vec![
            DiscoveredDataset {
                role: DatasetRole::Main,
                name: Arc::from("events"),
                incoming_schema: incoming.clone(),
                stored_schema: stored.clone(),
                system_columns: system_columns.iter().copied().map(Into::into).collect(),
            },
            DiscoveredDataset {
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from("events_dlq"),
                incoming_schema: incoming,
                stored_schema: stored,
                system_columns: system_columns.iter().copied().map(Into::into).collect(),
            },
        ],
        performance_advice: Vec::new(),
    })
}

async fn run_one_delivery(
    sink: Box<dyn transferia::core::sink::Sink>,
    memory: PipelineMemory,
    delivery: Delivery,
) -> anyhow::Result<()> {
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory,
        cancellation,
    }));

    delivery_tx.send(delivery).await?;
    drop(delivery_tx);
    let event = tokio::time::timeout(core::time::Duration::from_secs(30), event_rx.recv()).await?;
    let Some(event) = event else {
        task.await??;
        anyhow::bail!("sink event channel closed without an actor error before commit");
    };
    assert_eq!(event, SinkEvent::CommittedThrough(DeliveryId::new(1)));
    task.await??;
    Ok(())
}

#[tokio::test]
async fn discard_sink_runs_through_the_connector_and_actor_boundary() -> anyhow::Result<()> {
    let connector = DiscardSinkConnector::new();
    let discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("benchmark-source"),
        source_topology: transferia::core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: Vec::new(),
        performance_advice: Vec::new(),
    });
    connector.limits().validate_discovery(&discovery)?;
    let sink = connector
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
        })
        .await?;
    run_one_delivery(
        sink,
        PipelineMemory::new(1024),
        Delivery {
            id: DeliveryId::new(1),
            outputs: Vec::new(),
            meta: DeliveryMeta { source_messages: 1 },
        },
    )
    .await
}

async fn wait_for_tcp(host: &str, port: u16) -> anyhow::Result<()> {
    let address = format!("{host}:{port}");
    tokio::time::timeout(core::time::Duration::from_secs(30), async {
        loop {
            if tokio::net::TcpStream::connect(&address).await.is_ok() {
                return;
            }
            tokio::time::sleep(core::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("service did not accept TCP connections on {address}"))
}

#[tokio::test]
async fn clickhouse_sink_writes_to_a_real_native_server() -> anyhow::Result<()> {
    let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .start()
        .await?;
    let host = container.get_host().await?;
    let host = if host.to_string() == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host.to_string()
    };
    let native_port = container.get_host_port_ipv4(9000.tcp()).await?;
    let http_port = container.get_host_port_ipv4(8123.tcp()).await?;
    wait_for_tcp(&host, native_port).await?;

    let config: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "hosts: ['{host}']\nport: {native_port}\ntrusted_plaintext: true\ndatabase: default\nusername: default\nflush_interval_ms: 10\n"
    ))?;
    let checked = ClickHouseSinkConnector::check_connection(config.clone()).await?;
    let transferia::connectors::clickhouse::ClickHouseConnectionCheck::Verified { shard_groups } =
        checked
    else {
        anyhow::bail!("complete ClickHouse credentials must be fully verified")
    };
    assert!(shard_groups.is_empty() || shard_groups.iter().all(|group| !group.is_empty()));
    let connector = ClickHouseSinkConnector::from_config(config)?;
    let schema = dataset_schema(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, false),
    ]);
    let discovery = discovery("topic-a", schema.clone(), schema.clone(), &[], false);
    connector.limits().validate_discovery(&discovery)?;
    let mut last_prepare_error = None;
    for _ in 0..50 {
        match connector
            .prepare(SinkPrepare::from_discovery(&discovery)?.expect("row discovery"))
            .await
        {
            Ok(()) => {
                last_prepare_error = None;
                break;
            }
            Err(error) => {
                last_prepare_error = Some(error);
                tokio::time::sleep(core::time::Duration::from_millis(100)).await;
            }
        }
    }
    if let Some(error) = last_prepare_error {
        return Err(error.context("ClickHouse native endpoint never became ready"));
    }

    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
        })
        .await?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef,
            Arc::new(StringArray::from(vec!["seven", "eight"])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    run_one_delivery(
        sink,
        memory.clone(),
        Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from("events"),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve_transform(bytes),
                system_columns: SystemColumns::default(),
            }],
            meta: DeliveryMeta { source_messages: 2 },
        },
    )
    .await?;

    let response = reqwest::Client::new()
        .get(format!("http://{host}:{http_port}"))
        .query(&[(
            "query",
            "SELECT id, name FROM events ORDER BY id FORMAT JSONEachRow",
        )])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert_eq!(
        response,
        "{\"id\":7,\"name\":\"seven\"}\n{\"id\":8,\"name\":\"eight\"}\n"
    );
    Ok(())
}

#[tokio::test]
async fn s3_sink_writes_to_a_real_s3_api() -> anyhow::Result<()> {
    let container = GenericImage::new(LOCALSTACK_IMAGE, LOCALSTACK_TAG)
        .with_exposed_port(4566.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/_localstack/health")
                .with_port(4566.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("SERVICES", "s3")
        .start()
        .await?;
    let host = container.get_host().await?;
    let host = if host.to_string() == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host.to_string()
    };
    let port = container.get_host_port_ipv4(4566.tcp()).await?;
    let mut create_bucket = container
        .exec(ExecCommand::new([
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "transferia-e2e",
            "--region",
            "us-east-1",
        ]))
        .await?;
    let stderr = String::from_utf8(create_bucket.stderr_to_vec().await?)?;
    anyhow::ensure!(
        create_bucket.exit_code().await? == Some(0),
        "LocalStack bucket creation failed: {stderr}"
    );

    let yaml = format!(
        "bucket: transferia-e2e\nobject_layout_version: 5\npath_prefix: e2e\nregion: us-east-1\nendpoint: 'http://{host}:{port}'\ncredentials: {{ access_key: test, secret_key: test }}\nrotation: {{ max_rows: 1 }}\n"
    );
    let config: S3SinkConfig = serde_yaml::from_str(&yaml)?;
    config.check_connection().await?;
    let connector = S3SinkConnector::from_config(config)?;
    let system_kinds = vec![
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
    ];
    let incoming = dataset_schema(&[
        ("id", DataType::Int64, false),
        (
            SystemColumnKind::Topic.default_name(),
            DataType::Utf8,
            false,
        ),
        (
            SystemColumnKind::Partition.default_name(),
            DataType::Int64,
            false,
        ),
        (
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        ),
        (
            SystemColumnKind::MessageIndex.default_name(),
            DataType::UInt64,
            false,
        ),
    ]);
    let stored = dataset_schema(&[("id", DataType::Int64, false)]);
    let discovery = discovery("topic-a", incoming, stored, &system_kinds, false);
    connector.limits().validate_discovery(&discovery)?;
    connector
        .prepare(SinkPrepare::from_discovery(&discovery)?.expect("row discovery"))
        .await?;

    let durable_root =
        std::env::temp_dir().join(format!("transferia-s3-e2e-durable-{}", std::process::id()));
    let durable = transferia::durable::DurableStorageConfig::LocalFile {
        path: durable_root.clone(),
    }
    .build("s3-e2e")?;
    let memory = PipelineMemory::new(256 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            durable: durable.clone(),
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
        })
        .await?;
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            SystemColumnKind::Topic.default_name(),
            DataType::Utf8,
            false,
        ),
        Field::new(
            SystemColumnKind::Partition.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::MessageIndex.default_name(),
            DataType::UInt64,
            false,
        ),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            Arc::new(StringArray::from(vec!["topic-a"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![11])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    let replay_batch = batch.clone();
    let replay_system_columns = SystemColumns::new(vec![
        SystemColumn {
            kind: SystemColumnKind::Topic,
            name: Arc::from(SystemColumnKind::Topic.default_name()),
            index: 1,
        },
        SystemColumn {
            kind: SystemColumnKind::Partition,
            name: Arc::from(SystemColumnKind::Partition.default_name()),
            index: 2,
        },
        SystemColumn {
            kind: SystemColumnKind::Offset,
            name: Arc::from(SystemColumnKind::Offset.default_name()),
            index: 3,
        },
        SystemColumn {
            kind: SystemColumnKind::MessageIndex,
            name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
            index: 4,
        },
    ]);
    run_one_delivery(
        sink,
        memory.clone(),
        Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from("events"),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve_transform(bytes),
                system_columns: replay_system_columns.clone(),
            }],
            meta: DeliveryMeta { source_messages: 1 },
        },
    )
    .await?;

    let config: S3SinkConfig = serde_yaml::from_str(&yaml)?;
    let store = config.build_store()?;
    let mut objects = store.list(Some(&Path::from("e2e/events")));
    let object = objects
        .next()
        .await
        .transpose()?
        .expect("S3 sink must create one object");
    assert!(
        objects.next().await.is_none(),
        "expected exactly one object"
    );
    let payload = store.get(&object.location).await?.bytes().await?;
    assert_eq!(payload.as_ref(), b"{\"id\":42}\n");
    assert_eq!(
        object.location.as_ref(),
        "e2e/events/topic=topic-a/partition=0/topic-a+0+11.json"
    );
    let modified_before_replay = object.last_modified;

    // Recreate the production connector and local durable-storage handle exactly as a process
    // restart would. CLOSED state must recover source commit without issuing another PUT.
    tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    let replay_connector = S3SinkConnector::from_config(serde_yaml::from_str(&yaml)?)?;
    let replay_sink = replay_connector
        .build_sink(SinkBuildContext {
            durable: transferia::durable::DurableStorageConfig::LocalFile {
                path: durable_root.clone(),
            }
            .build("s3-e2e")?,
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
        })
        .await?;
    let replay_memory = PipelineMemory::new(256 * 1024 * 1024);
    run_one_delivery(
        replay_sink,
        replay_memory.clone(),
        Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from("events"),
                is_dlq: false,
                batch: replay_batch,
                byte_size: bytes,
                memory: replay_memory.reserve_transform(bytes),
                system_columns: replay_system_columns,
            }],
            meta: DeliveryMeta { source_messages: 1 },
        },
    )
    .await?;
    let after_replay = store.head(&object.location).await?;
    assert_eq!(
        after_replay.last_modified, modified_before_replay,
        "CLOSED recovery must not rewrite the real S3 object"
    );
    std::fs::remove_dir_all(durable_root)?;
    Ok(())
}
