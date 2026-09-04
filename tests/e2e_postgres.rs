#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
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
use transferia::core::data::message::SourceBatch;
use transferia::core::data::schema::{DatasetSchema, SchemaColumn};
use transferia::core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
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
    if let Some(prepare) =
        SinkPrepare::from_discovery(&discovery, true, "test-transfer", None)?
    {
        sink.prepare(prepare).await?;
    }
    let memory = PipelineMemory::new(256 * 1024 * 1024);
    let source_actor = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Batch,
            phase: transferia::registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: CancellationToken::new(),
            memory: memory.clone(),
            durable: support::durable_context(),
        })
        .await?;
    let sink_actor = sink
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            replay_identity: None,
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

fn changelog_discovery(table: &str) -> DeliveryDiscovery {
    let id =
        SchemaColumn::new("id".into(), DataType::Int64, false).with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".into(), DataType::Utf8, false);
    let incoming_payload = SchemaColumn::new("payload".into(), DataType::Utf8, true);
    let operation = SchemaColumn::new(
        SystemColumnKind::ChangeOperation.default_name().into(),
        SystemColumnKind::ChangeOperation.data_type(),
        false,
    );
    let offset = SchemaColumn::new(
        SystemColumnKind::Offset.default_name().into(),
        SystemColumnKind::Offset.data_type(),
        false,
    );
    let changed = SchemaColumn::new(
        SystemColumnKind::ChangedColumns.default_name().into(),
        SystemColumnKind::ChangedColumns.data_type(),
        false,
    );
    DeliveryDiscovery {
        source_name: Arc::from("postgres-cdc-e2e"),
        source_topology: transferia::core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from(table),
            incoming_schema: DatasetSchema::new(vec![
                id.clone(),
                incoming_payload,
                operation,
                offset,
                changed,
            ]),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: vec![
                SystemColumnKind::ChangeOperation.into(),
                SystemColumnKind::Offset.into(),
                SystemColumnKind::ChangedColumns.into(),
            ],
        }],
        performance_advice: Vec::new(),
    }
}

async fn changelog_delivery(
    memory: &PipelineMemory,
    delivery_id: u64,
    operations: Vec<&str>,
    ids: Vec<i64>,
    payloads: Vec<Option<&str>>,
    lsn: i64,
) -> anyhow::Result<Delivery> {
    let discovery = changelog_discovery("cdc_target");
    let fields = discovery.datasets[0]
        .incoming_schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let rows = ids.len();
    let changed = operations
        .iter()
        .zip(&payloads)
        .map(|(operation, payload)| {
            if *operation == "d" || (*operation == "u" && payload.is_none()) {
                &[0b01_u8][..]
            } else {
                &[0b11_u8][..]
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
            Arc::new(StringArray::from(operations)) as ArrayRef,
            Arc::new(Int64Array::from(vec![lsn; rows])) as ArrayRef,
            Arc::new(arrow::array::BinaryArray::from_iter_values(changed)) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(Delivery {
        id: DeliveryId::new(delivery_id),
        outputs: vec![SinkBatch {
            table: Arc::from("cdc_target"),
            is_dlq: false,
            batch,
            byte_size: bytes,
            memory: memory.reserve_transform(bytes),
            system_columns: SystemColumns::new(vec![
                SystemColumn {
                    kind: SystemColumnKind::ChangeOperation,
                    name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
                    index: 2,
                },
                SystemColumn {
                    kind: SystemColumnKind::Offset,
                    name: Arc::from(SystemColumnKind::Offset.default_name()),
                    index: 3,
                },
                SystemColumn {
                    kind: SystemColumnKind::ChangedColumns,
                    name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
                    index: 4,
                },
            ]),
        }],
        meta: DeliveryMeta {
            source_messages: rows as u64,
        },
    })
}

#[tokio::test]
async fn postgres_sink_applies_changelog_atomically_and_replay_is_idempotent() -> anyhow::Result<()>
{
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start()
        .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let connection =
        format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let pg = wait_for_postgres(&connection).await?;
    let discovery = Arc::new(changelog_discovery("cdc_target"));
    let connector = PostgresSinkConnector::from_config(serde_yaml::from_str(&format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncreate_tables: true\n"
    ))?)?;
    pg.batch_execute("CREATE TABLE cdc_wrong_key (id bigint NOT NULL, payload text NOT NULL)")
        .await?;
    let wrong_key = changelog_discovery("cdc_wrong_key");
    let error = connector
        .prepare(
            SinkPrepare::from_discovery(&wrong_key, false, "test-transfer", None)?
                .expect("wrong-key dataset"),
        )
        .await
        .expect_err("an existing changelog table without the declared key must fail at startup");
    assert!(
        error.to_string().contains("has primary key []"),
        "{error:#}"
    );
    connector.limits().validate_discovery(&discovery)?;
    connector
        .prepare(
            SinkPrepare::from_discovery(&discovery, false, "test-transfer", None)?
                .expect("dataset"),
        )
        .await?;
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            replay_identity: None,
            finite_source: false,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(3);
    let (event_tx, mut event_rx) = mpsc::channel(3);
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    for delivery in [
        changelog_delivery(
            &memory,
            1,
            vec!["c", "u", "c", "d"],
            vec![1, 1, 2, 2],
            vec![Some("old"), Some("current"), Some("deleted"), None],
            42,
        )
        .await?,
        changelog_delivery(
            &memory,
            2,
            vec!["c", "u", "c", "d"],
            vec![1, 1, 2, 2],
            vec![Some("old"), Some("current"), Some("deleted"), None],
            42,
        )
        .await?,
        changelog_delivery(
            &memory,
            3,
            vec!["d", "c"],
            vec![1, 3],
            vec![None, Some("three")],
            43,
        )
        .await?,
        changelog_delivery(&memory, 4, vec!["u"], vec![3], vec![None], 44).await?,
    ] {
        let id = delivery.id;
        delivery_tx.send(delivery).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
            Some(SinkEvent::CommittedThrough(id))
        );
    }
    drop(delivery_tx);
    task.await??;

    let rows = pg
        .query("SELECT id, payload FROM cdc_target ORDER BY id", &[])
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 3);
    assert_eq!(rows[0].get::<_, String>(1), "three");
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
                delivery_type: transferia::delivery::config::yaml::DeliveryType::Batch,
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
        "bucket: postgres-source-e2e\nobject_layout_version: 5\npath_prefix: pg\nregion: us-east-1\nendpoint: 'http://{s3_host}:{s3_port}'\ncredentials: {{ access_key: test, secret_key: test }}\nformat: {{ type: json }}\nrotation: {{ max_rows: 100 }}\n"
    );
    let s3_sink = S3SinkConnector::from_config(serde_yaml::from_str(&s3_yaml)?)?;
    run_pipeline(&source, &s3_sink, Arc::clone(&discovery)).await?;
    let store = serde_yaml::from_str::<S3SinkConfig>(&s3_yaml)?.build_store()?;
    let objects = store
        .list(Some(&Path::from("pg/events")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        objects.len(),
        1,
        "expected one S3 data object, got {:?}",
        objects
            .iter()
            .map(|object| object.location.as_ref())
            .collect::<Vec<_>>()
    );
    let object = &objects[0];
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
        performance_advice: Vec::new(),
    });
    let postgres_sink = PostgresSinkConnector::from_config(serde_yaml::from_str(&format!(
        "host: '{pg_host}'\nport: {pg_port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncreate_tables: true\n"
    ))?)?;
    postgres_sink.limits().validate_discovery(&copy_discovery)?;
    postgres_sink
        .prepare(
            SinkPrepare::from_discovery(&copy_discovery, true, "test-transfer", None)?
                .expect("dataset"),
        )
        .await?;
    let sink = postgres_sink
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            replay_identity: None,
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

#[tokio::test]
async fn postgres_source_reads_builtin_and_user_defined_types_losslessly() -> anyhow::Result<()> {
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let connection =
        format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let pg = wait_for_postgres(&connection).await?;
    pg.batch_execute(
        r#"
        CREATE TYPE transferia_mood AS ENUM ('calm', 'busy');
        CREATE DOMAIN transferia_positive AS integer CHECK (VALUE > 0);
        CREATE TYPE transferia_pair AS (left_value numeric, right_value text);
        CREATE TABLE all_types (
            bool_value boolean NOT NULL,
            char_value "char" NOT NULL,
            int2_value smallint NOT NULL,
            int4_value integer NOT NULL,
            int8_value bigint NOT NULL,
            oid_value oid NOT NULL,
            float4_value real NOT NULL,
            float8_value double precision NOT NULL,
            bytea_value bytea NOT NULL,
            text_value text NOT NULL,
            varchar_value varchar(20) NOT NULL,
            bpchar_value character(4) NOT NULL,
            name_value name NOT NULL,
            numeric_value numeric NOT NULL,
            money_value money NOT NULL,
            date_value date NOT NULL,
            time_value time NOT NULL,
            timetz_value timetz NOT NULL,
            timestamp_value timestamp NOT NULL,
            timestamptz_value timestamptz NOT NULL,
            interval_value interval NOT NULL,
            json_value json NOT NULL,
            jsonb_value jsonb NOT NULL,
            xml_value xml NOT NULL,
            uuid_value uuid NOT NULL,
            inet_value inet NOT NULL,
            cidr_value cidr NOT NULL,
            macaddr_value macaddr NOT NULL,
            macaddr8_value macaddr8 NOT NULL,
            bit_value bit(8) NOT NULL,
            varbit_value varbit NOT NULL,
            point_value point NOT NULL,
            line_value line NOT NULL,
            lseg_value lseg NOT NULL,
            box_value box NOT NULL,
            path_value path NOT NULL,
            polygon_value polygon NOT NULL,
            circle_value circle NOT NULL,
            range_value int4range NOT NULL,
            multirange_value int4multirange NOT NULL,
            int_array integer[] NOT NULL,
            text_array text[] NOT NULL,
            enum_value transferia_mood NOT NULL,
            domain_value transferia_positive NOT NULL,
            composite_value transferia_pair NOT NULL
        );
        CREATE PUBLICATION transferia_all_types FOR TABLE all_types
            WITH (publish = 'insert, update, delete');
        "#,
    )
    .await?;
    pg.query_one(
        "SELECT * FROM pg_create_logical_replication_slot('transferia_all_types', 'pgoutput')",
        &[],
    )
    .await?;
    pg.batch_execute(
        r#"
        INSERT INTO all_types VALUES (
            true, 'A', -2, 3, 4, 5, 1.25, -2.5,
            decode('00ff10', 'hex'), 'text', 'varchar', 'xy', 'identifier',
            '123456789012345678901234567890.12345678901234567890', '$12.34',
            'infinity', '24:00:00', '04:05:06.123456-08', '-infinity',
            '2004-10-19 10:23:54+02', '1 year 2 mons 3 days 04:05:06.789',
            '{ "preserve" : "spacing" }', '{"canonical": true}',
            '<root attr="value">text</root>', '550e8400-e29b-41d4-a716-446655440000',
            '192.168.1.5/24', '10.0.0.0/8', '08:00:2b:01:02:03',
            '08:00:2b:01:02:03:04:05', B'10101111', B'10101',
            '(1,2)', '{1,2,3}', '[(1,2),(3,4)]', '((1,1),(3,3))',
            '[(1,1),(2,2)]', '((0,0),(1,0),(1,1))', '<(1,2),3>',
            '[1,5)', '{[1,5),[8,10)}', '[0:1]={10,20}', ARRAY['NULL', NULL],
            'busy', 7, ROW(0.99, 'value')
        );
        "#,
    )
    .await?;

    let source = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\nbatch_rows: 128\ntables:\n  - name: all_types\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = source
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Batch,
        })
        .await?;
    let dataset = &discovery.datasets[0];
    let data_type = |name: &str| {
        dataset
            .stored_schema
            .columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| &column.data_type)
            .expect("column must be discovered")
    };
    assert_eq!(data_type("bool_value"), &DataType::Boolean);
    assert_eq!(data_type("char_value"), &DataType::Int8);
    assert_eq!(data_type("int2_value"), &DataType::Int16);
    assert_eq!(data_type("int4_value"), &DataType::Int32);
    assert_eq!(data_type("int8_value"), &DataType::Int64);
    assert_eq!(data_type("oid_value"), &DataType::UInt32);
    assert_eq!(data_type("float4_value"), &DataType::Float32);
    assert_eq!(data_type("float8_value"), &DataType::Float64);
    assert_eq!(data_type("bytea_value"), &DataType::Binary);
    assert_eq!(data_type("domain_value"), &DataType::Int32);
    for column in &dataset.stored_schema.columns {
        if [
            "bool_value",
            "char_value",
            "int2_value",
            "int4_value",
            "int8_value",
            "oid_value",
            "float4_value",
            "float8_value",
            "bytea_value",
            "domain_value",
        ]
        .contains(&column.name.as_str())
        {
            continue;
        }
        assert_eq!(
            column.data_type,
            DataType::Utf8,
            "{} should use PostgreSQL's lossless canonical text representation",
            column.name
        );
    }

    let mut actor = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Batch,
            phase: transferia::registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(256 * 1024 * 1024),
            durable: support::durable_context(),
        })
        .await?;
    let SourceBatch::Typed { tables, .. } = actor.read_batch().await? else {
        anyhow::bail!("PostgreSQL all-types source returned no data batch");
    };
    let batch = &tables[0].batch;
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("bytea_value")
            .expect("bytea")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary")
            .value(0),
        &[0, 255, 16]
    );
    let text = |name: &str| {
        batch
            .column_by_name(name)
            .expect("textual column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8")
            .value(0)
    };
    assert_eq!(
        text("numeric_value"),
        "123456789012345678901234567890.12345678901234567890"
    );
    assert_eq!(text("date_value"), "infinity");
    assert_eq!(text("timestamp_value"), "-infinity");
    assert_eq!(text("int_array"), "[0:1]={10,20}");
    assert_eq!(text("text_array"), "{\"NULL\",NULL}");
    assert_eq!(text("enum_value"), "busy");
    assert_eq!(text("composite_value"), "(0.99,value)");

    let replication = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\nbatch_rows: 128\ntables:\n  - name: all_types\nreplication:\n  slot: transferia_all_types\n  decoder: {{ type: pgoutput, publication: transferia_all_types }}\n  poll_interval_ms: 10\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    replication
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Stream,
        })
        .await?;
    let mut replication = replication
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Stream,
            phase: transferia::registry::SourcePhase::Stream,
            replay_identity: Some(Arc::from("postgres-all-types-revision-1")),
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(256 * 1024 * 1024),
            durable: support::durable_context(),
        })
        .await?;
    let SourceBatch::Typed {
        tables: replication_tables,
        ..
    } = tokio::time::timeout(Duration::from_secs(10), replication.read_batch()).await??
    else {
        anyhow::bail!("PostgreSQL all-types replication returned no typed batch");
    };
    let replication_batch = &replication_tables[0].batch;
    let replication_schema = replication_batch.schema();
    let snapshot_schema = batch.schema();
    for column in &dataset.stored_schema.columns {
        let name = &column.name;
        let snapshot_field = snapshot_schema
            .field_with_name(name)
            .expect("snapshot field");
        let replication_field = replication_schema
            .field_with_name(name)
            .expect("replication field");
        assert_eq!(
            snapshot_field, replication_field,
            "Arrow field mismatch for {name}"
        );
        assert_eq!(
            batch
                .column_by_name(name)
                .expect("snapshot column")
                .to_data(),
            replication_batch
                .column_by_name(name)
                .expect("replication column")
                .to_data(),
            "Arrow value mismatch for {name}"
        );
    }
    Ok(())
}
