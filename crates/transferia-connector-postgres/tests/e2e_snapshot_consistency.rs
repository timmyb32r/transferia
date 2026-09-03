#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array as _, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use transferia_connector_postgres::metrics::MetricsRegistry;
use transferia_connector_postgres::postgres::PostgresSourceConnector;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source;
use transferia_registry::{
    SourceBuildContext, SourceConnector as _, SourceDiscoveryContext,
};

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "17.6-bookworm";

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotIdentity {
    database: String,
    schema: String,
    transaction_id: u64,
    source_timestamp_ms: i64,
    source_timestamp_us: i64,
    source_timestamp_ns: i64,
    event_timestamp_ms: i64,
    event_timestamp_us: i64,
    event_timestamp_ns: i64,
    wal_lsn: i64,
}

#[derive(Debug, Eq, PartialEq)]
struct SnapshotResult {
    rows: Vec<(i64, String)>,
    identity: SnapshotIdentity,
}

#[tokio::test]
async fn all_table_partitions_share_one_pre_mutation_snapshot_for_binary_and_text_copy(
) -> anyhow::Result<()> {
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5_432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start()
        .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(5_432.tcp()).await?;
    let connection = connection_string(&host, port);
    let client = connect_with_retry(&connection).await?;
    client
        .batch_execute(
            "CREATE TABLE snapshot_a (id bigint PRIMARY KEY, payload text NOT NULL);\
             CREATE TABLE snapshot_b (id bigint PRIMARY KEY, payload text NOT NULL);",
        )
        .await?;

    for format in ["binary", "text"] {
        restore_initial_rows(&client).await?;
        let (first, second) = run_snapshot(&host, port, format).await?;

        assert_eq!(
            first.rows,
            vec![(1, "a-one".to_owned()), (2, "a-two".to_owned())],
            "partition 0 observed post-snapshot mutations in {format} COPY mode"
        );
        assert_eq!(
            second.rows,
            vec![(10, "b-ten".to_owned()), (20, "b-twenty".to_owned())],
            "partition 1 did not import partition 0's snapshot in {format} COPY mode"
        );
        assert_eq!(
            first.identity, second.identity,
            "table partitions used different snapshot metadata in {format} COPY mode"
        );
        assert_current_rows_are_post_mutation(&client).await?;
        assert_no_snapshot_owner(&client).await?;
    }
    Ok(())
}

#[tokio::test]
async fn terminated_snapshot_owner_is_a_non_retryable_source_build_failure() -> anyhow::Result<()> {
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5_432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start()
        .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(5_432.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute("CREATE TABLE snapshot_a (id bigint PRIMARY KEY)")
        .await?;
    assert_expired_snapshot_sqlstate(&connection_string(&host, port)).await?;
    let connector = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - {{ schema: public, name: snapshot_a }}\n"
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

    let terminated = client
        .query_one(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = current_database() AND state = 'idle in transaction' \
               AND pid <> pg_backend_pid() ORDER BY backend_start LIMIT 1",
            &[],
        )
        .await?
        .try_get::<_, bool>(0)?;
    assert!(terminated, "the exported snapshot owner was not terminated");

    let error = match build_partition(&connector, 0).await {
        Ok(_) => panic!("an expired exported snapshot must not construct a reader"),
        Err(error) => error,
    };
    let failure = error
        .downcast::<transferia_core::failure::DataPlaneFailure>()
        .map_err(|error| anyhow::anyhow!("source build failure lost its disposition: {error}"))?;
    assert!(
        !failure.is_retryable(),
        "an expired shared snapshot cannot be repaired by retrying one partition: {failure:?}"
    );
    Ok(())
}

async fn assert_expired_snapshot_sqlstate(connection: &str) -> anyhow::Result<()> {
    let owner = connect_with_retry(connection).await?;
    owner
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    let snapshot_id = owner
        .query_one("SELECT pg_export_snapshot()::text", &[])
        .await?
        .try_get::<_, String>(0)?;
    owner.batch_execute("ROLLBACK").await?;

    let importer = connect_with_retry(connection).await?;
    importer
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    let error = importer
        .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{snapshot_id}'"))
        .await
        .expect_err("a snapshot must expire when its exporting transaction ends");
    assert_eq!(
        error.as_db_error().map(|error| error.code().code()),
        Some("42704"),
        "PostgreSQL must classify an expired exported snapshot as an undefined object"
    );
    drop(importer);
    Ok(())
}

async fn run_snapshot(
    host: &str,
    port: u16,
    format: &str,
) -> anyhow::Result<(SnapshotResult, SnapshotResult)> {
    let connector = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - {{ schema: public, name: snapshot_a }}\n  - {{ schema: public, name: snapshot_b }}\nbatch_rows: 1\ncopy_to_format: {format}\n"
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

    let mut first = build_partition(&connector, 0).await?;
    mutate_after_first_partition_opens(&connection_string(host, port)).await?;
    let mut second = build_partition(&connector, 1).await?;

    let first_result = drain_partition(&mut first, "snapshot_a", 0).await?;
    let second_result = drain_partition(&mut second, "snapshot_b", 1).await?;
    first.shutdown().await?;
    second.shutdown().await?;
    Ok((first_result, second_result))
}

async fn build_partition(
    connector: &PostgresSourceConnector,
    partition_id: i64,
) -> anyhow::Result<Box<dyn Source>> {
    connector
        .build_source(SourceBuildContext {
            partition_id,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(16 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await
}

async fn drain_partition(
    source: &mut Box<dyn Source>,
    expected_table: &str,
    expected_partition: i64,
) -> anyhow::Result<SnapshotResult> {
    let mut rows = Vec::new();
    let mut identity = None;
    let mut expected_message_index = 0_u64;
    loop {
        match source.read_batch().await? {
            SourceBatch::Typed {
                tables,
                source_rows,
                ..
            } => {
                assert_eq!(tables.len(), 1);
                let table = &tables[0];
                assert_eq!(table.table.as_ref(), expected_table);
                assert_eq!(source_rows, table.batch.num_rows() as u64);
                let batch_identity = batch_identity(&table.batch)?;
                if let Some(expected) = &identity {
                    assert_eq!(expected, &batch_identity);
                } else {
                    identity = Some(batch_identity);
                }

                let ids = array::<Int64Array>(&table.batch, 0);
                let payloads = array::<StringArray>(&table.batch, 1);
                let topic = system_array::<StringArray>(
                    &table.batch,
                    &table.system_columns,
                    SystemColumnKind::Topic,
                );
                let partitions = system_array::<Int64Array>(
                    &table.batch,
                    &table.system_columns,
                    SystemColumnKind::Partition,
                );
                let message_indexes = system_array::<UInt64Array>(
                    &table.batch,
                    &table.system_columns,
                    SystemColumnKind::MessageIndex,
                );
                for row in 0..table.batch.num_rows() {
                    assert_eq!(topic.value(row), "postgres");
                    assert_eq!(partitions.value(row), expected_partition);
                    assert_eq!(message_indexes.value(row), expected_message_index);
                    expected_message_index += 1;
                    rows.push((ids.value(row), payloads.value(row).to_owned()));
                }
            }
            SourceBatch::Finished => break,
            SourceBatch::Raw { .. } => panic!("PostgreSQL snapshot must emit typed batches"),
        }
    }
    rows.sort_by_key(|row| row.0);
    Ok(SnapshotResult {
        rows,
        identity: identity.expect("snapshot emitted at least one row"),
    })
}

fn batch_identity(batch: &RecordBatch) -> anyhow::Result<SnapshotIdentity> {
    let identity = SnapshotIdentity {
        database: array_by_name::<StringArray>(batch, "_system_source_database")?.value(0).into(),
        schema: array_by_name::<StringArray>(batch, "_system_source_schema")?.value(0).into(),
        transaction_id: array_by_name::<UInt64Array>(batch, "_system_source_transaction_id")?
            .value(0),
        source_timestamp_ms: array_by_name::<Int64Array>(batch, "_system_source_timestamp_ms")?
            .value(0),
        source_timestamp_us: array_by_name::<Int64Array>(batch, "_system_source_timestamp_us")?
            .value(0),
        source_timestamp_ns: array_by_name::<Int64Array>(batch, "_system_source_timestamp_ns")?
            .value(0),
        event_timestamp_ms: array_by_name::<Int64Array>(batch, "_system_event_timestamp_ms")?
            .value(0),
        event_timestamp_us: array_by_name::<Int64Array>(batch, "_system_event_timestamp_us")?
            .value(0),
        event_timestamp_ns: array_by_name::<Int64Array>(batch, "_system_event_timestamp_ns")?
            .value(0),
        wal_lsn: array_by_name::<Int64Array>(batch, "_system_offset")?.value(0),
    };
    assert_eq!(identity.database, "transferia");
    assert_eq!(identity.schema, "public");
    assert_eq!(
        identity.source_timestamp_ms,
        identity.source_timestamp_ns / 1_000_000
    );
    assert_eq!(
        identity.source_timestamp_us,
        identity.source_timestamp_ns / 1_000
    );
    assert_eq!(identity.event_timestamp_ms, identity.source_timestamp_ms);
    assert_eq!(identity.event_timestamp_us, identity.source_timestamp_us);
    assert_eq!(identity.event_timestamp_ns, identity.source_timestamp_ns);
    Ok(identity)
}

fn system_array<'a, T: arrow::array::Array + 'static>(
    batch: &'a RecordBatch,
    columns: &transferia_core::data::system_columns::SystemColumns,
    kind: SystemColumnKind,
) -> &'a T {
    let column = columns.get(kind).expect("required snapshot system column");
    array(batch, column.index)
}

fn array_by_name<'a, T: arrow::array::Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> anyhow::Result<&'a T> {
    let index = batch.schema().index_of(name)?;
    Ok(array(batch, index))
}

fn array<T: arrow::array::Array + 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch.column(index).as_any().downcast_ref().unwrap()
}

async fn restore_initial_rows(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    client
        .batch_execute(
            "TRUNCATE snapshot_a, snapshot_b;\
             INSERT INTO snapshot_a VALUES (1, 'a-one'), (2, 'a-two');\
             INSERT INTO snapshot_b VALUES (10, 'b-ten'), (20, 'b-twenty');",
        )
        .await?;
    Ok(())
}

async fn mutate_after_first_partition_opens(connection: &str) -> anyhow::Result<()> {
    let client = connect_with_retry(connection).await?;
    client
        .batch_execute(
            "UPDATE snapshot_a SET payload = 'a-after' WHERE id = 1;\
             DELETE FROM snapshot_a WHERE id = 2;\
             INSERT INTO snapshot_a VALUES (3, 'a-new');\
             UPDATE snapshot_b SET payload = 'b-after' WHERE id = 10;\
             DELETE FROM snapshot_b WHERE id = 20;\
             INSERT INTO snapshot_b VALUES (30, 'b-new');",
        )
        .await?;
    Ok(())
}

async fn assert_current_rows_are_post_mutation(
    client: &tokio_postgres::Client,
) -> anyhow::Result<()> {
    let a: String = client
        .query_one(
            "SELECT string_agg(id::text || ':' || payload, ',' ORDER BY id) FROM snapshot_a",
            &[],
        )
        .await?
        .try_get(0)?;
    let b: String = client
        .query_one(
            "SELECT string_agg(id::text || ':' || payload, ',' ORDER BY id) FROM snapshot_b",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(a, "1:a-after,3:a-new");
    assert_eq!(b, "10:b-after,30:b-new");
    Ok(())
}

async fn assert_no_snapshot_owner(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let owners = client
                .query_one(
                    "SELECT count(*)::bigint FROM pg_stat_activity \
                     WHERE datname = current_database() AND state = 'idle in transaction'",
                    &[],
                )
                .await?
                .try_get::<_, i64>(0)?;
            if owners == 0 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("PostgreSQL snapshot owner transaction was not released"))??;
    Ok(())
}

fn connection_string(host: &str, port: u16) -> String {
    format!("host={host} port={port} user=postgres password=test dbname=transferia")
}

fn reachable_host(host: &impl ToString) -> String {
    match host.to_string().as_str() {
        "localhost" => "127.0.0.1".to_owned(),
        host => host.to_owned(),
    }
}

async fn connect_with_retry(connection: &str) -> anyhow::Result<tokio_postgres::Client> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok((client, connection)) =
                tokio_postgres::connect(connection, tokio_postgres::NoTls).await
            {
                tokio::spawn(async move {
                    drop(connection.await);
                });
                return client;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("PostgreSQL testcontainer did not become ready"))
}
