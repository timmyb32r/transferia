#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use arrow::array::{Array as _, Int64Array, StringArray};
use mysql_async::prelude::Queryable as _;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ExecCommand, IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use transferia::connectors::iceberg::{
    IcebergSinkConnector, IcebergSourceConfig, IcebergSourceConnector,
};
use transferia::connectors::mysql::{
    connect as connect_mysql, MySqlConnectionConfig, MySqlSourceConnector,
};
use transferia::connectors::postgres::PostgresSourceConnector;
use transferia::core::data::message::SourceBatch;
use transferia::core::data::schema::{
    META_SYSTEM_ROLE, SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_US,
};
use transferia::core::data::table_data::TableData;
use transferia::core::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use transferia::core::memory::{MemoryReservation, PipelineMemory};
use transferia::core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia::core::source::{CommitMarker, Source};
use transferia::delivery::config::yaml::DeliveryType;
use transferia::durable::DurableContext;
use transferia::metrics::{MetricsRegistry, SinkCounters};
use transferia::registry::{
    SinkBuildContext, SinkConnector as _, SinkPrepare, SourceBuildContext, SourceConnector,
    SourceDiscoveryContext, SourceExecutionContext, SourcePhase,
};

const MYSQL_PORT: u16 = 3_306;
const POSTGRES_PORT: u16 = 5_432;
const SOURCE_TIMEOUT: Duration = Duration::from_secs(20);

struct IcebergFixture {
    _s3: ContainerAsync<GenericImage>,
    _catalog: ContainerAsync<GenericImage>,
    catalog_uri: String,
    storage_yaml: String,
}

impl IcebergFixture {
    async fn start() -> anyhow::Result<Self> {
        let s3 = GenericImage::new("localstack/localstack", "4.14.0")
            .with_exposed_port(4566.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/_localstack/health")
                    .with_port(4566.tcp())
                    .with_expected_status_code(200_u16),
            ))
            .with_env_var("SERVICES", "s3")
            .start()
            .await?;
        let s3_host = reachable_host(&s3.get_host().await?);
        let s3_port = s3.get_host_port_ipv4(4566.tcp()).await?;
        let mut create_bucket = s3
            .exec(ExecCommand::new([
                "awslocal",
                "s3api",
                "create-bucket",
                "--bucket",
                "iceberg-warehouse",
                "--region",
                "us-east-1",
            ]))
            .await?;
        let stderr = String::from_utf8(create_bucket.stderr_to_vec().await?)?;
        anyhow::ensure!(
            create_bucket.exit_code().await? == Some(0),
            "LocalStack bucket creation failed: {stderr}"
        );

        let catalog = GenericImage::new("tabulario/iceberg-rest", "1.6.0")
            .with_exposed_port(8181.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/v1/config")
                    .with_port(8181.tcp())
                    .with_expected_status_code(200_u16),
            ))
            .with_env_var("AWS_ACCESS_KEY_ID", "test")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "test")
            .with_env_var("AWS_REGION", "us-east-1")
            .with_env_var("CATALOG_WAREHOUSE", "s3://iceberg-warehouse/")
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var(
                "CATALOG_S3_ENDPOINT",
                format!("http://host.docker.internal:{s3_port}"),
            )
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .start()
            .await?;
        let catalog_host = reachable_host(&catalog.get_host().await?);
        let catalog_port = catalog.get_host_port_ipv4(8181.tcp()).await?;
        let catalog_uri = format!("http://{catalog_host}:{catalog_port}");
        reqwest::Client::new()
            .post(format!("{catalog_uri}/v1/namespaces"))
            .json(&serde_json::json!({ "namespace": ["default"], "properties": {} }))
            .send()
            .await?
            .error_for_status()?;
        Ok(Self {
            _s3: s3,
            _catalog: catalog,
            catalog_uri,
            storage_yaml: format!(
                "type: s3\nbucket: iceberg-warehouse\nregion: us-east-1\nendpoint: http://{s3_host}:{s3_port}\ncredentials:\n  access_key: test\n  secret_key: test\npath_style_access: true\n"
            ),
        })
    }

    fn sink(&self) -> anyhow::Result<IcebergSinkConnector> {
        IcebergSinkConnector::from_config(serde_yaml::from_str(&format!(
            "catalog:\n  uri: {}\n  auth: {{ type: none }}\nstorage:\n{}namespace: default\ncreate_if_missing: true\ntarget_file_size_bytes: 1048576\ncommit_target_size_bytes: 1048576\nwrite_concurrency: 2\n",
            self.catalog_uri,
            indent(&self.storage_yaml, 2),
        ))?)
    }

    async fn current_snapshot_id(&self, table: &str) -> anyhow::Result<i64> {
        let response = reqwest::Client::new()
            .get(format!(
                "{}/v1/namespaces/default/tables/{table}",
                self.catalog_uri
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        response
            .pointer("/metadata/current-snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("REST catalog response has no current snapshot id"))
    }

    async fn rows(&self, table: &str) -> anyhow::Result<Vec<(i64, String)>> {
        let config: IcebergSourceConfig = serde_yaml::from_str(&format!(
            "catalog:\n  uri: {}\n  auth: {{ type: none }}\nstorage:\n{}namespace: default\ntable_names: [{table}]\n",
            self.catalog_uri,
            indent(&self.storage_yaml, 2),
        ))?;
        let connector =
            IcebergSourceConnector::from_config(config, Arc::new(MetricsRegistry::new()))?;
        connector
            .delivery_discovery(SourceDiscoveryContext {
                request: DeliveryDiscoveryRequest {
                    keep_system_columns: false,
                },
                cancellation: CancellationToken::new(),
                delivery_type: DeliveryType::Batch,
            })
            .await?;
        let mut source = connector
            .build_source(SourceBuildContext {
                partition_id: 0,
                delivery_type: DeliveryType::Batch,
                phase: SourcePhase::Snapshot,
                replay_identity: None,
                cancellation: CancellationToken::new(),
                memory: PipelineMemory::new(128 * 1024 * 1024),
                durable: support::durable_context(),
            })
            .await?;
        let mut rows = Vec::new();
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed { tables, .. } => {
                    for data in tables {
                        let ids = data
                            .batch
                            .column_by_name("id")
                            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                            .context("Iceberg replica id must be Int64")?;
                        let values = data
                            .batch
                            .column_by_name("value")
                            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                            .context("Iceberg replica value must be Utf8")?;
                        for row in 0..data.batch.num_rows() {
                            rows.push((ids.value(row), values.value(row).to_owned()));
                        }
                    }
                }
                SourceBatch::Finished => break,
                other => anyhow::bail!("unexpected Iceberg source batch: {other:?}"),
            }
        }
        rows.sort();
        Ok(rows)
    }
}

struct TypedRead {
    tables: Vec<TableData>,
    marker: CommitMarker,
    _memory: Vec<MemoryReservation>,
}

async fn read_typed(source: &mut Box<dyn Source>) -> anyhow::Result<TypedRead> {
    tokio::time::timeout(SOURCE_TIMEOUT, async {
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    commit_marker: Some(marker),
                    memory,
                    ..
                } if !tables.is_empty() => {
                    return Ok(TypedRead {
                        tables,
                        marker,
                        _memory: memory,
                    });
                }
                SourceBatch::Typed {
                    tables,
                    commit_marker: Some(_),
                    ..
                } if tables.is_empty() => {
                    anyhow::bail!("unexpected filtered transaction checkpoint")
                }
                SourceBatch::Finished => anyhow::bail!("replication source finished"),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for replication rows"))?
}

fn assert_same_tables(left: &[TableData], right: &[TableData]) {
    assert_eq!(left.len(), right.len());
    for (left, right) in left.iter().zip(right) {
        assert_eq!(left.table, right.table);
        assert_eq!(
            left.system_columns.iter().cloned().collect::<Vec<_>>(),
            right.system_columns.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!(left.batch.schema(), right.batch.schema());
        assert_eq!(left.batch.num_columns(), right.batch.num_columns());
        let schema = left.batch.schema();
        for column in 0..left.batch.num_columns() {
            let field = schema.field(column);
            if matches!(
                field.metadata().get(META_SYSTEM_ROLE).map(String::as_str),
                Some(
                    SYSTEM_ROLE_EVENT_TIMESTAMP_MS
                        | SYSTEM_ROLE_EVENT_TIMESTAMP_US
                        | SYSTEM_ROLE_EVENT_TIMESTAMP_NS
                )
            ) {
                continue;
            }
            assert_eq!(
                left.batch.column(column).to_data(),
                right.batch.column(column).to_data(),
                "replayed field '{}' changed",
                field.name()
            );
        }
    }
}

async fn prepare_iceberg(
    fixture: &IcebergFixture,
    discovery: &Arc<DeliveryDiscovery>,
    durable: &DurableContext,
    replay_identity: &Arc<str>,
) -> anyhow::Result<()> {
    let connector = fixture.sink()?;
    connector.limits().validate_discovery(discovery)?;
    connector
        .prepare(
            SinkPrepare::from_discovery(
                discovery,
                false,
                Arc::clone(&durable.delivery_id),
                Some(Arc::clone(replay_identity)),
            )?
            .context("replica discovery has no datasets")?,
        )
        .await
}

async fn deliver_to_iceberg(
    fixture: &IcebergFixture,
    discovery: Arc<DeliveryDiscovery>,
    durable: DurableContext,
    replay_identity: Arc<str>,
    tables: &[TableData],
) -> anyhow::Result<()> {
    let connector = fixture.sink()?;
    let memory = PipelineMemory::new(128 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            partition_id: 0,
            delivery_name: "test delivery".into(),
            replay_identity: Some(replay_identity),
            finite_source: false,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: true,
            discovery,
            durable,
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation,
    }));
    let bytes = tables
        .iter()
        .map(|table| table.batch.get_array_memory_size())
        .sum::<usize>();
    let lease = memory.reserve_transform(bytes.max(1));
    let outputs = tables
        .iter()
        .map(|table| SinkBatch {
            table: Arc::clone(&table.table),
            is_dlq: table.is_dlq,
            batch: table.batch.clone(),
            byte_size: table.batch.get_array_memory_size(),
            memory: lease.clone(),
            system_columns: table.system_columns.clone(),
        })
        .collect();
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(0),
            outputs,
            meta: DeliveryMeta {
                source_messages: tables
                    .iter()
                    .map(|table| table.batch.num_rows() as u64)
                    .sum(),
            },
        })
        .await?;
    drop(delivery_tx);
    let event = event_rx.recv().await;
    task.await??;
    assert_eq!(event, Some(SinkEvent::CommittedThrough(DeliveryId::new(0))));
    Ok(())
}

#[tokio::test]
async fn postgres_and_mysql_replicate_to_iceberg_exactly_once() -> anyhow::Result<()> {
    let iceberg = IcebergFixture::start().await?;
    exercise_mysql(&iceberg).await?;
    exercise_postgres(&iceberg).await?;
    Ok(())
}

async fn exercise_mysql(iceberg: &IcebergFixture) -> anyhow::Result<()> {
    let mysql = GenericImage::new("mysql", "8.4.6")
        .with_exposed_port(MYSQL_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "test")
        .with_env_var("MYSQL_DATABASE", "transferia")
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
            "--binlog-transaction-compression=OFF",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--sync-binlog=1",
            "--binlog-expire-logs-seconds=0",
        ])
        .start()
        .await?;
    let host = reachable_host(&mysql.get_host().await?);
    let port = mysql.get_host_port_ipv4(MYSQL_PORT.tcp()).await?;
    let admin = mysql_connection(&host, port, "root", "test");
    wait_for_mysql(&admin).await?.disconnect().await?;
    let mut connection = connect_mysql(&admin).await?;
    connection
        .query_drop("CREATE USER 'iceberg_source'@'%' IDENTIFIED BY 'source-test'")
        .await?;
    connection
        .query_drop("GRANT SELECT, LOCK TABLES ON transferia.* TO 'iceberg_source'@'%'")
        .await?;
    connection
        .query_drop(
            "GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'iceberg_source'@'%'",
        )
        .await?;
    connection
        .query_drop(
            "CREATE TABLE mysql_replica (id BIGINT PRIMARY KEY, value VARCHAR(128) NOT NULL)",
        )
        .await?;
    connection.disconnect().await?;
    let source_connection = mysql_connection(&host, port, "iceberg_source", "source-test");
    let config = mysql_source_yaml(&source_connection);
    let durable = support::durable_context();
    let replay_identity: Arc<str> = Arc::from("mysql-iceberg-replica-v1");

    let connector = mysql_connector(&config)?;
    let discovery = Arc::new(discover_stream(&connector).await?);
    prepare_iceberg(iceberg, &discovery, &durable, &replay_identity).await?;
    prepare_stream(&connector, &durable, &replay_identity).await?;
    let mut source = build_stream(&connector, &durable, &replay_identity).await?;
    let mut writer = connect_mysql(&admin).await?;
    writer.query_drop("START TRANSACTION").await?;
    writer
        .query_drop("INSERT INTO mysql_replica VALUES (1, 'one'), (2, 'two')")
        .await?;
    writer.query_drop("COMMIT").await?;
    let seed = read_typed(&mut source)
        .await
        .context("waiting for initial MySQL replication rows")?;
    deliver_to_iceberg(
        iceberg,
        Arc::clone(&discovery),
        durable.clone(),
        Arc::clone(&replay_identity),
        &seed.tables,
    )
    .await?;
    source
        .commit_offsets(std::slice::from_ref(&seed.marker))
        .await?;
    drop(seed);
    assert_eq!(
        iceberg.rows("mysql_replica").await?,
        vec![(1, "one".to_owned()), (2, "two".to_owned())]
    );

    writer.query_drop("START TRANSACTION").await?;
    writer
        .query_drop("UPDATE mysql_replica SET value='one-updated' WHERE id=1")
        .await?;
    writer
        .query_drop("DELETE FROM mysql_replica WHERE id=2")
        .await?;
    writer.query_drop("COMMIT").await?;
    writer.disconnect().await?;
    let first = read_typed(&mut source)
        .await
        .context("waiting for uncommitted MySQL update/delete rows")?;
    deliver_to_iceberg(
        iceberg,
        Arc::clone(&discovery),
        durable.clone(),
        Arc::clone(&replay_identity),
        &first.tables,
    )
    .await?;
    let committed_snapshot = iceberg.current_snapshot_id("mysql_replica").await?;
    source.shutdown().await?;
    drop(source);
    drop(connector);

    let replay_connector = mysql_connector(&config)?;
    prepare_stream(&replay_connector, &durable, &replay_identity).await?;
    let mut replay_source = build_stream(&replay_connector, &durable, &replay_identity).await?;
    let replay = read_typed(&mut replay_source)
        .await
        .context("waiting for replayed MySQL update/delete rows")?;
    assert_same_tables(&first.tables, &replay.tables);
    deliver_to_iceberg(
        iceberg,
        Arc::clone(&discovery),
        durable.clone(),
        Arc::clone(&replay_identity),
        &replay.tables,
    )
    .await?;
    assert_eq!(
        iceberg.current_snapshot_id("mysql_replica").await?,
        committed_snapshot,
        "replayed MySQL transaction created a duplicate Iceberg snapshot"
    );
    replay_source
        .commit_offsets(std::slice::from_ref(&replay.marker))
        .await?;
    replay_source.shutdown().await?;
    assert_eq!(
        iceberg.rows("mysql_replica").await?,
        vec![(1, "one-updated".to_owned())]
    );
    Ok(())
}

async fn exercise_postgres(iceberg: &IcebergFixture) -> anyhow::Result<()> {
    let postgres = GenericImage::new(
        "souravbiswassanto/postgres",
        concat!(
            "17-wal2json@sha256:",
            "3ee36414cc936dbbf5640a8e8671141815af1d1fb49d465aeeb85b4a4e412879"
        ),
    )
    .with_exposed_port(POSTGRES_PORT.tcp())
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
        "max_replication_slots=10",
        "-c",
        "max_wal_senders=10",
    ])
    .with_platform("linux/amd64")
    .start()
    .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let connection =
        format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let client = connect_postgres(&connection).await?;
    client
        .batch_execute(
            "CREATE TABLE postgres_replica (id bigint PRIMARY KEY, value text NOT NULL);\
             ALTER TABLE postgres_replica REPLICA IDENTITY FULL;\
             CREATE PUBLICATION iceberg_publication FOR TABLE postgres_replica \
                 WITH (publish = 'insert, update, delete');",
        )
        .await?;
    let config = postgres_source_yaml(&host, port);
    let mut durable = support::durable_context();
    durable.delivery_id = Arc::from("iceberg_replica_slot");
    let replay_identity: Arc<str> = Arc::from("postgres-iceberg-replica-v1");
    let connector = postgres_connector(&config)?;
    let discovery = Arc::new(discover_stream(&connector).await?);
    prepare_iceberg(iceberg, &discovery, &durable, &replay_identity).await?;
    prepare_stream(&connector, &durable, &replay_identity).await?;
    let mut source = build_stream(&connector, &durable, &replay_identity).await?;
    client
        .batch_execute(
            "BEGIN;\
             INSERT INTO postgres_replica VALUES (1, 'one'), (2, 'two');\
             COMMIT;",
        )
        .await?;
    let seed = read_typed(&mut source)
        .await
        .context("waiting for initial PostgreSQL replication rows")?;
    deliver_to_iceberg(
        iceberg,
        Arc::clone(&discovery),
        durable.clone(),
        Arc::clone(&replay_identity),
        &seed.tables,
    )
    .await?;
    source
        .commit_offsets(std::slice::from_ref(&seed.marker))
        .await?;
    drop(seed);
    assert_eq!(
        iceberg.rows("postgres_replica").await?,
        vec![(1, "one".to_owned()), (2, "two".to_owned())]
    );

    client
        .batch_execute(
            "BEGIN;\
             UPDATE postgres_replica SET value='one-updated' WHERE id=1;\
             DELETE FROM postgres_replica WHERE id=2;\
             COMMIT;",
        )
        .await?;
    let first = read_typed(&mut source)
        .await
        .context("waiting for uncommitted PostgreSQL update/delete rows")?;
    deliver_to_iceberg(
        iceberg,
        Arc::clone(&discovery),
        durable.clone(),
        Arc::clone(&replay_identity),
        &first.tables,
    )
    .await?;
    let committed_snapshot = iceberg.current_snapshot_id("postgres_replica").await?;
    source.shutdown().await?;
    drop(source);
    drop(connector);

    let replay_connector = postgres_connector(&config)?;
    prepare_stream(&replay_connector, &durable, &replay_identity).await?;
    let mut replay_source = build_stream(&replay_connector, &durable, &replay_identity).await?;
    let replay = read_typed(&mut replay_source)
        .await
        .context("waiting for replayed PostgreSQL update/delete rows")?;
    assert_same_tables(&first.tables, &replay.tables);
    deliver_to_iceberg(
        iceberg,
        Arc::clone(&discovery),
        durable.clone(),
        Arc::clone(&replay_identity),
        &replay.tables,
    )
    .await?;
    assert_eq!(
        iceberg.current_snapshot_id("postgres_replica").await?,
        committed_snapshot,
        "replayed PostgreSQL transaction created a duplicate Iceberg snapshot"
    );
    replay_source
        .commit_offsets(std::slice::from_ref(&replay.marker))
        .await?;
    replay_source.shutdown().await?;
    assert_eq!(
        iceberg.rows("postgres_replica").await?,
        vec![(1, "one-updated".to_owned())]
    );
    Ok(())
}

async fn discover_stream(connector: &dyn SourceConnector) -> anyhow::Result<DeliveryDiscovery> {
    connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: CancellationToken::new(),
            delivery_type: DeliveryType::Stream,
        })
        .await
}

async fn prepare_stream(
    connector: &dyn SourceConnector,
    durable: &DurableContext,
    replay_identity: &Arc<str>,
) -> anyhow::Result<()> {
    connector
        .prepare_execution(SourceExecutionContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: CancellationToken::new(),
            delivery_type: DeliveryType::Stream,
            replay_identity: Some(Arc::clone(replay_identity)),
            durable: durable.clone(),
        })
        .await?;
    Ok(())
}

async fn build_stream(
    connector: &dyn SourceConnector,
    durable: &DurableContext,
    replay_identity: &Arc<str>,
) -> anyhow::Result<Box<dyn Source>> {
    connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: DeliveryType::Stream,
            phase: SourcePhase::Stream,
            replay_identity: Some(Arc::clone(replay_identity)),
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(128 * 1024 * 1024),
            durable: durable.clone(),
        })
        .await
}

fn mysql_connector(config: &str) -> anyhow::Result<MySqlSourceConnector> {
    MySqlSourceConnector::from_config(
        serde_yaml::from_str(config)?,
        Arc::new(MetricsRegistry::new()),
    )
}

fn postgres_connector(config: &str) -> anyhow::Result<PostgresSourceConnector> {
    PostgresSourceConnector::from_config(
        serde_yaml::from_str(config)?,
        Arc::new(MetricsRegistry::new()),
    )
}

fn mysql_source_yaml(connection: &MySqlConnectionConfig) -> String {
    format!(
        "host: '{}'\nport: {}\ndatabase: transferia\nusername: {}\npassword: {}\ntrusted_plaintext: true\ntables:\n  - name: mysql_replica\nreplication:\n  server_id: 454545\n  max_events: 1024\n  poll_interval_ms: 10\n  bootstrap_timeout_ms: 10000\n",
        connection.host, connection.port, connection.username, connection.password,
    )
}

fn postgres_source_yaml(host: &str, port: u16) -> String {
    format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - {{ schema: public, name: postgres_replica }}\nreplication:\n  plugin: {{ type: pgoutput, publication: iceberg_publication }}\n  poll_interval_ms: 10\n"
    )
}

fn mysql_connection(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> MySqlConnectionConfig {
    MySqlConnectionConfig {
        host: host.to_owned(),
        port,
        database: "transferia".to_owned(),
        username: username.to_owned(),
        password: password.to_owned(),
        trusted_plaintext: true,
        tls_ca_file: None,
    }
}

async fn wait_for_mysql(config: &MySqlConnectionConfig) -> anyhow::Result<mysql_async::Conn> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match connect_mysql(config).await {
                Ok(connection) => return connection,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("MySQL container did not become ready"))
}

async fn connect_postgres(connection: &str) -> anyhow::Result<tokio_postgres::Client> {
    tokio::time::timeout(Duration::from_secs(60), async {
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
    .map_err(|_| anyhow::anyhow!("PostgreSQL container did not become ready"))
}

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

fn indent(value: &str, spaces: usize) -> String {
    let prefix = " ".repeat(spaces);
    value.lines().fold(String::new(), |mut output, line| {
        output.push_str(&prefix);
        output.push_str(line);
        output.push('\n');
        output
    })
}
