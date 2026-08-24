#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
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

use transferia::connectors::clickhouse::ClickHouseSinkConnector;
use transferia::connectors::postgres::{PostgresSinkConnector, PostgresSourceConnector};
use transferia::connectors::s3::sink::{S3SinkConfig, S3SinkConnector};
use transferia::core::data::schema::{DatasetSchema, SchemaColumn};
use transferia::core::data::system_columns::SystemColumns;
use transferia::core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
};
use transferia::core::memory::PipelineMemory;
use transferia::core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia::delivery::execution::run_partition_pipeline;
use transferia::metrics::{MetricsRegistry, ParseCounters, SinkCounters};
use transferia::registry::{
    SinkBuildContext, SinkConnector as _, SinkPrepare, SourceBuildContext, SourceConnector as _,
    SourceDiscoveryContext,
};

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "17.6-bookworm";
const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.8.28.1";
const LOCALSTACK_IMAGE: &str = "localstack/localstack";
const LOCALSTACK_TAG: &str = "4.14.0";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

async fn wait_for_postgres(connection: &str) -> anyhow::Result<tokio_postgres::Client> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match tokio_postgres::connect(connection, tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        drop(connection.await);
                    });
                    return client;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("PostgreSQL testcontainer did not become ready"))
}

async fn wait_for_tcp(host: &str, port: u16) -> anyhow::Result<()> {
    let address = format!("{host}:{port}");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if tokio::net::TcpStream::connect(&address).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("service did not accept TCP connections on {address}"))
}

async fn run_pipeline(
    source: &PostgresSourceConnector,
    sink: &dyn transferia::registry::SinkConnector,
    discovery: Arc<DeliveryDiscovery>,
) -> anyhow::Result<()> {
    sink.limits().validate_discovery(&discovery)?;
    if let Some(prepare) = SinkPrepare::from_discovery(&discovery)? {
        sink.prepare(prepare).await?;
    }
    let memory = PipelineMemory::new(256 * 1024 * 1024);
    let source_actor = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: memory.clone(),
            durable: support::durable_context(),
        })
        .await?;
    let sink_actor = sink
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
        })
        .await?;
    tokio::time::timeout(
        Duration::from_secs(30),
        run_partition_pipeline(
            source_actor,
            source.parser(),
            Arc::new(Vec::new()),
            sink_actor,
            memory,
            CancellationToken::new(),
            0,
            Arc::new(ParseCounters::new()),
        ),
    )
    .await??;
    Ok(())
}

async fn run_one_delivery(
    sink: Box<dyn transferia::core::sink::Sink>,
    memory: PipelineMemory,
    delivery: Delivery,
) -> anyhow::Result<()> {
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory,
        cancellation: CancellationToken::new(),
    }));
    delivery_tx.send(delivery).await?;
    drop(delivery_tx);
    let event = tokio::time::timeout(Duration::from_secs(30), event_rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL sink closed without a commit event"))?;
    assert_eq!(event, SinkEvent::CommittedThrough(DeliveryId::new(1)));
    task.await??;
    Ok(())
}

#[tokio::test]
async fn postgres_source_without_primary_key_reaches_clickhouse_and_s3_and_binary_copy_is_real(
) -> anyhow::Result<()> {
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start()
        .await?;
    let pg_host = reachable_host(&postgres.get_host().await?);
    let pg_port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let pg_connection =
        format!("host={pg_host} port={pg_port} user=postgres password=test dbname=transferia");
    let pg = wait_for_postgres(&pg_connection).await?;
    transferia::connectors::postgres::check_connection(
        &transferia::connectors::postgres::PostgresConnectionConfig {
            host: pg_host.clone(),
            port: pg_port,
            database: "transferia".to_owned(),
            username: "postgres".to_owned(),
            password: "test".to_owned(),
            trusted_plaintext: true,
            tls_ca_file: None,
        },
    )
    .await?;
    pg.batch_execute(
        "CREATE TABLE events (id bigint NOT NULL, name text NULL, active boolean NOT NULL);\
         INSERT INTO events VALUES (1, 'one', true), (2, NULL, false);",
    )
    .await?;

    let source = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{pg_host}'\nport: {pg_port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\nbatch_rows: 1\ntables:\n  - name: events\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = Arc::new(
        source
            .delivery_discovery(SourceDiscoveryContext {
                request: DeliveryDiscoveryRequest {
                    keep_system_columns: false,
                },
                cancellation: CancellationToken::new(),
            })
            .await?,
    );

    let clickhouse = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
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
    let ch_host = reachable_host(&clickhouse.get_host().await?);
    let ch_native = clickhouse.get_host_port_ipv4(9000.tcp()).await?;
    let ch_http = clickhouse.get_host_port_ipv4(8123.tcp()).await?;
    wait_for_tcp(&ch_host, ch_native).await?;
    let clickhouse_sink = ClickHouseSinkConnector::from_config(serde_yaml::from_str(&format!(
        "hosts: ['{ch_host}']\nport: {ch_native}\ntrusted_plaintext: true\ndatabase: default\nusername: default\nflush_interval_ms: 10\n"
    ))?)?;
    let mut last_error = None;
    for _ in 0..50 {
        match run_pipeline(&source, &clickhouse_sink, Arc::clone(&discovery)).await {
            Ok(()) => {
                last_error = None;
                break;
            }
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    if let Some(error) = last_error {
        return Err(error.context("ClickHouse native endpoint never became ready"));
    }
    let ch_rows = reqwest::Client::new()
        .get(format!("http://{ch_host}:{ch_http}"))
        .query(&[(
            "query",
            "SELECT id, name, active FROM events FORMAT JSONEachRow",
        )])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let mut ch_rows = ch_rows
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    ch_rows.sort_by_key(|row| row["id"].as_i64());
    assert_eq!(
        ch_rows,
        vec![
            serde_json::json!({"id": 1, "name": "one", "active": true}),
            serde_json::json!({"id": 2, "name": null, "active": false}),
        ]
    );

    let localstack = GenericImage::new(LOCALSTACK_IMAGE, LOCALSTACK_TAG)
        .with_exposed_port(4566.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/_localstack/health")
                .with_port(4566.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("SERVICES", "s3")
        .start()
        .await?;
    let s3_host = reachable_host(&localstack.get_host().await?);
    let s3_port = localstack.get_host_port_ipv4(4566.tcp()).await?;
    let mut create_bucket = localstack
        .exec(ExecCommand::new([
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "postgres-source-e2e",
            "--region",
            "us-east-1",
        ]))
        .await?;
    let create_bucket_stderr = String::from_utf8(create_bucket.stderr_to_vec().await?)?;
    anyhow::ensure!(
        create_bucket.exit_code().await? == Some(0),
        "LocalStack bucket creation failed: {create_bucket_stderr}"
    );
    let s3_yaml = format!(
        "bucket: postgres-source-e2e\nobject_layout_version: 5\nprefix: pg\nregion: us-east-1\nhost: '{s3_host}'\nport: {s3_port}\nallow_http: true\ncredentials: {{ access_key: test, secret_key: test }}\nrotation: {{ max_rows: 100 }}\n"
    );
    let s3_sink = S3SinkConnector::from_config(serde_yaml::from_str(&s3_yaml)?)?;
    run_pipeline(&source, &s3_sink, Arc::clone(&discovery)).await?;
    let store = serde_yaml::from_str::<S3SinkConfig>(&s3_yaml)?.build_store()?;
    let mut objects = store.list(Some(&Path::from("pg/events")));
    let object = objects
        .next()
        .await
        .transpose()?
        .expect("PostgreSQL source must create an S3 object");
    assert!(objects.next().await.is_none());
    let json = store.get(&object.location).await?.bytes().await?;
    let mut rows = json
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by_key(|row| row["id"].as_i64());
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 1, "name": "one", "active": true}),
            serde_json::json!({"id": 2, "name": null, "active": false}),
        ]
    );

    let copy_schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("name".into(), DataType::Utf8, true),
        SchemaColumn::new("day".into(), DataType::Date32, false),
        SchemaColumn::new(
            "created_at".into(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        ),
    ]);
    let copy_discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("typed-e2e"),
        source_topology: transferia::core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("copy_target"),
            incoming_schema: copy_schema.clone(),
            stored_schema: copy_schema,
            system_columns: Vec::new(),
        }],
    });
    let postgres_sink = PostgresSinkConnector::from_config(serde_yaml::from_str(&format!(
        "host: '{pg_host}'\nport: {pg_port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncreate_tables: true\n"
    ))?)?;
    postgres_sink.limits().validate_discovery(&copy_discovery)?;
    postgres_sink
        .prepare(SinkPrepare::from_discovery(&copy_discovery)?.expect("dataset"))
        .await?;
    let sink = postgres_sink
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: copy_discovery,
        })
        .await?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("day", DataType::Date32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![9])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(Date32Array::from(vec![19_723])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![1_704_067_200_123_456])) as ArrayRef,
        ],
    )?;
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let bytes = batch.get_array_memory_size();
    run_one_delivery(
        sink,
        memory.clone(),
        Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from("copy_target"),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve_transform(bytes),
                system_columns: SystemColumns::default(),
            }],
            meta: DeliveryMeta { source_messages: 1 },
        },
    )
    .await?;
    let copied = pg
        .query_one(
            "SELECT id, name, day::text, created_at::text FROM copy_target",
            &[],
        )
        .await?;
    assert_eq!(copied.get::<_, i64>(0), 9);
    assert_eq!(copied.get::<_, Option<String>>(1), None);
    assert_eq!(copied.get::<_, String>(2), "2024-01-01");
    assert_eq!(copied.get::<_, String>(3), "2024-01-01 00:00:00.123456");
    Ok(())
}
