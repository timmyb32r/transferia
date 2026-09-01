#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use transferia_connector_postgres::metrics::MetricsRegistry;
use transferia_connector_postgres::postgres::PostgresSourceConnector;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::{SystemColumnKind, SystemColumns};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};
use transferia_registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext};

const POSTGRES_PORT: u16 = 5_432;
const POSTGRES_IMAGE: &str = "souravbiswassanto/postgres";
const POSTGRES_TAG: &str = concat!(
    "17-wal2json@sha256:",
    "3ee36414cc936dbbf5640a8e8671141815af1d1fb49d465aeeb85b4a4e412879"
);

#[tokio::test]
async fn pgoutput_and_wal2json_emit_identical_committable_arrow_changes() -> anyhow::Result<()> {
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
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
    let host = postgres.get_host().await?.to_string();
    let host = if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    };
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let connection =
        format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let client = connect_with_retry(&connection).await?;
    client
        .batch_execute(
            "CREATE TABLE accounts (\
                 id integer PRIMARY KEY,\
                 name text NOT NULL,\
                 balance bigint NOT NULL\
             );\
             CREATE TABLE ignored (id integer PRIMARY KEY);\
             CREATE PUBLICATION transferia_publication FOR TABLE accounts, ignored;",
        )
        .await?;
    client
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot('transferia_pgoutput', 'pgoutput')",
            &[],
        )
        .await?;
    client
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot('transferia_wal2json', 'wal2json')",
            &[],
        )
        .await?;

    let mut pgoutput = source(&host, port, "pgoutput").await?;
    let mut wal2json = source(&host, port, "wal2json").await?;
    let pgoutput_before = slot_lsn(&client, "transferia_pgoutput").await?;
    let wal2json_before = slot_lsn(&client, "transferia_wal2json").await?;

    client
        .batch_execute(
            "BEGIN;\
             INSERT INTO accounts VALUES (1, 'alice', 10);\
             UPDATE accounts SET name = 'alice-2', balance = 11 WHERE id = 1;\
             DELETE FROM accounts WHERE id = 1;\
             COMMIT;",
        )
        .await?;

    let pgoutput_batch = read_changes(&mut pgoutput).await?;
    let wal2json_batch = read_changes(&mut wal2json).await?;
    assert_same_change_rows(&pgoutput_batch, &wal2json_batch);
    assert_eq!(operations(&pgoutput_batch), ["c", "u", "d"]);

    pgoutput
        .commit_offsets(std::slice::from_ref(&pgoutput_batch.marker))
        .await?;
    wal2json
        .commit_offsets(std::slice::from_ref(&wal2json_batch.marker))
        .await?;
    assert!(slot_lsn(&client, "transferia_pgoutput").await? > pgoutput_before);
    assert!(slot_lsn(&client, "transferia_wal2json").await? > wal2json_before);

    client
        .execute("INSERT INTO ignored VALUES (1)", &[])
        .await?;
    let pgoutput_marker = read_filtered_transaction(&mut pgoutput).await?;
    let wal2json_marker = read_filtered_transaction(&mut wal2json).await?;
    pgoutput
        .commit_offsets(std::slice::from_ref(&pgoutput_marker))
        .await?;
    wal2json
        .commit_offsets(std::slice::from_ref(&wal2json_marker))
        .await?;

    assert!(tokio::time::timeout(Duration::from_millis(400), pgoutput.read_batch())
        .await
        .is_err());
    assert!(tokio::time::timeout(Duration::from_millis(400), wal2json.read_batch())
        .await
        .is_err());
    Ok(())
}

struct ChangeBatch {
    batch: RecordBatch,
    system_columns: SystemColumns,
    marker: CommitMarker,
}

async fn source(
    host: &str,
    port: u16,
    decoder: &str,
) -> anyhow::Result<Box<dyn Source>> {
    let decoder = match decoder {
        "pgoutput" => "{ type: pgoutput, publication: transferia_publication }",
        "wal2json" => "{ type: wal2_json }",
        other => anyhow::bail!("unsupported test decoder {other}"),
    };
    let connector = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            r#"host: '{host}'
port: {port}
database: transferia
username: postgres
password: test
trusted_plaintext: true
tables:
  - {{ schema: public, name: accounts }}
replication:
  slot: transferia_{decoder_name}
  decoder: {decoder}
  poll_interval_ms: 10
"#,
            decoder_name = if decoder.contains("pgoutput") {
                "pgoutput"
            } else {
                "wal2json"
            }
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    connector
        .delivery_discovery(SourceDiscoveryContext {
            request: transferia_core::delivery::DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(64 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await
}

async fn read_changes(source: &mut Box<dyn Source>) -> anyhow::Result<ChangeBatch> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    mut tables,
                    source_rows,
                    commit_marker,
                    ..
                } if source_rows > 0 => {
                    anyhow::ensure!(tables.len() == 1, "expected one changed table");
                    let table = tables.pop().expect("one table");
                    return Ok(ChangeBatch {
                        batch: table.batch,
                        system_columns: table.system_columns,
                        marker: commit_marker
                            .ok_or_else(|| anyhow::anyhow!("CDC batch has no commit marker"))?,
                    });
                }
                SourceBatch::Typed { .. } => {}
                SourceBatch::Raw { .. } | SourceBatch::Finished => {
                    anyhow::bail!("PostgreSQL CDC returned a non-typed or finite batch")
                }
            }
        }
    })
    .await?
}

async fn read_filtered_transaction(source: &mut Box<dyn Source>) -> anyhow::Result<CommitMarker> {
    let batch = tokio::time::timeout(Duration::from_secs(10), source.read_batch()).await??;
    match batch {
        SourceBatch::Typed {
            tables,
            source_rows: 0,
            commit_marker: Some(marker),
            ..
        } if tables.is_empty() => Ok(marker),
        SourceBatch::Typed { .. } | SourceBatch::Raw { .. } | SourceBatch::Finished => {
            anyhow::bail!("filtered PostgreSQL transaction did not retain its commit marker")
        }
    }
}

fn assert_same_change_rows(left: &ChangeBatch, right: &ChangeBatch) {
    assert_eq!(left.batch.num_rows(), right.batch.num_rows());
    assert_eq!(left.batch.num_columns(), right.batch.num_columns());
    for index in 0..=3 {
        assert_eq!(left.batch.column(index).to_data(), right.batch.column(index).to_data());
    }
}

fn operations(batch: &ChangeBatch) -> Vec<&str> {
    let operation = batch
        .system_columns
        .get(SystemColumnKind::ChangeOperation)
        .expect("operation system column");
    let values = batch
        .batch
        .column(operation.index)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("operation string array");
    values.iter().map(Option::unwrap).collect()
}

async fn connect_with_retry(connection: &str) -> anyhow::Result<tokio_postgres::Client> {
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

async fn slot_lsn(client: &tokio_postgres::Client, slot: &str) -> anyhow::Result<u64> {
    let value: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
        .get(0);
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid LSN {value}"))?;
    Ok((u64::from_str_radix(high, 16)? << 32) | u64::from_str_radix(low, 16)?)
}
