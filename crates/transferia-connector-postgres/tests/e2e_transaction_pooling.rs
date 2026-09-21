#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used, reason = "test assertions intentionally fail fast")]

#[path = "support/transaction_pool.rs"]
mod transaction_pool;

use std::sync::Arc;
use std::time::Duration;
use anyhow::Context as _;
use testcontainers::{GenericImage, ImageExt as _};
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use tokio_util::sync::CancellationToken;
use transferia_connector_postgres::{metrics::MetricsRegistry, postgres::PostgresSourceConnector};
use transferia_core::{data::message::SourceBatch, delivery::DeliveryDiscoveryRequest, memory::PipelineMemory, source::Source};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext, SourceExecutionContext, SourcePhase};
use transferia_registry::durable::DurableContext;

#[tokio::test]
async fn cdc_survives_idle_session_resets_and_lost_lease_never_acknowledges() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(90), verify_pooling()).await?
}

async fn verify_pooling() -> anyhow::Result<()> {
    let postgres = GenericImage::new("souravbiswassanto/postgres", "17-wal2json@sha256:3ee36414cc936dbbf5640a8e8671141815af1d1fb49d465aeeb85b4a4e412879")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("database system is ready to accept connections"))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .with_cmd(["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"])
        .with_platform("linux/amd64")
        .start().await?;
    let host = postgres.get_host().await?.to_string();
    let host = if host == "localhost" { "127.0.0.1".to_owned() } else { host };
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let admin = connect(&host, port).await?;
    admin.batch_execute("CREATE TABLE pooled (id bigint PRIMARY KEY, payload text NOT NULL, amount numeric(20,4) NOT NULL); CREATE PUBLICATION pooled_publication FOR TABLE pooled").await?;
    let pool = transaction_pool::TransactionPool::start(host, port).await?;
    let raw = connect("127.0.0.1", pool.port()).await?;
    // Prove this fixture deterministically reproduces the observed failure.
    let old_error = raw.query_one("SELECT 1::integer", &[]).await.unwrap_err();
    assert_eq!(old_error.code(), Some(&tokio_postgres::error::SqlState::INVALID_SQL_STATEMENT_NAME));
    drop(raw);

    for (index, plugin) in ["{ type: pgoutput, publication: pooled_publication }", "{ type: wal2json }", "{ type: auto }"].iter().enumerate() {
        let slot = format!("pooled_{index}");
        let durable = transferia_test_support::durable_contexts(&[&slot]).remove(0);
        let connector = make_connector(pool.port(), plugin)?;
        discover_and_prepare(&connector, durable.clone()).await?;
        let mut source = build(&connector, durable.clone()).await?;
        // A separate durable store must not bypass PostgreSQL's source-wide
        // lock. DISCARD ALL must not release the dedicated transaction lease.
        let competitor = make_connector(pool.port(), plugin)?;
        let other_store = transferia_test_support::durable_contexts(&[&slot]).remove(0);
        let error = discover_and_prepare(&competitor, other_store).await.unwrap_err();
        assert!(format!("{error:#}").contains("already active on the exact source"));
        let id = i64::try_from(index)?;
        admin.execute("INSERT INTO pooled VALUES ($1, 'before lease failure', 1844674407370955.1615)", &[&id]).await?;
        let marker = read_row(source.as_mut(), id, "before lease failure", 18_446_744_073_709_551_615).await?;
        source.commit_offsets(std::slice::from_ref(&marker)).await?;
        let committed: String = admin.query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", &[&slot]).await?.get(0);
        let durable_key = format!("postgres-replication-{slot}");
        let durable_before = durable.storage.read(&durable_key).await?.context("committed CDC state is absent")?;
        let id = id + 10;
        admin.execute("INSERT INTO pooled VALUES ($1, 'after committed record', -1844674407370955.1615)", &[&id]).await?;
        let uncommitted = read_row(source.as_mut(), id, "after committed record", -18_446_744_073_709_551_615).await?;
        // Only this connector has a lease now: the failed competitor was
        // rolled back on drop. Terminate its exact advisory-lock backend.
        let leases = admin.query("SELECT DISTINCT pid FROM pg_locks WHERE locktype = 'advisory' AND granted", &[]).await?;
        assert_eq!(leases.len(), 1);
        let pid: i32 = leases[0].get(0);
        admin.query_one("SELECT pg_terminate_backend($1)", &[&pid]).await?;
        let error = source.commit_offsets(std::slice::from_ref(&uncommitted)).await.unwrap_err();
        assert!(format!("{error:#}").contains("execution lease was lost"));
        assert!(!error.is_retryable());
        let read_error = source.read_batch().await.err().context("lost lease still allowed reads")?;
        assert!(!read_error.is_retryable());
        assert!(format!("{read_error:#}").contains("execution lease was lost"));
        let after: String = admin.query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", &[&slot]).await?.get(0);
        assert_eq!(after, committed, "lost ownership must not advance the slot");
        let durable_after = durable.storage.read(&durable_key).await?.context("committed CDC state disappeared")?;
        assert_eq!(durable_after.revision, durable_before.revision);
        assert_eq!(durable_after.payload, durable_before.payload);
        drop(source);
        drop(connector);
        drop(competitor);
        admin.query_one("SELECT pg_drop_replication_slot($1)", &[&slot]).await?;
    }
    // The plugin must preserve NaN as a string until the Decimal contract can
    // reject it. wal2json's default numeric JSON mode silently emits null here.
    let slot = "pooled_numeric_invalid";
    let durable = transferia_test_support::durable_contexts(&[slot]).remove(0);
    let connector = make_connector(pool.port(), "{ type: wal2json }")?;
    discover_and_prepare(&connector, durable.clone()).await?;
    let mut source = build(&connector, durable.clone()).await?;
    let before: String = admin.query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", &[&slot]).await?.get(0);
    let durable_key = format!("postgres-replication-{slot}");
    let state_before = durable.storage.read(&durable_key).await?;
    admin.batch_execute("INSERT INTO pooled VALUES (99, 'non-finite NUMERIC', 'NaN'::numeric)").await?;
    let error = source.read_batch().await.err().context("wal2json converted NUMERIC NaN to a published null")?;
    assert!(!error.is_retryable());
    assert!(format!("{error:#}").contains("non-finite PostgreSQL NUMERIC"));
    let after: String = admin.query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", &[&slot]).await?.get(0);
    assert_eq!(before, after);
    assert_eq!(durable.storage.read(&durable_key).await?, state_before);
    drop(source);
    drop(connector);
    admin.query_one("SELECT pg_drop_replication_slot($1)", &[&slot]).await?;
    assert!(pool.resets() >= 30, "CDC did not exercise repeated idle backend resets");
    Ok(())
}

fn make_connector(port: u16, plugin: &str) -> anyhow::Result<PostgresSourceConnector> {
    PostgresSourceConnector::from_config(serde_yaml::from_str(&format!(
        "host: 127.0.0.1\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.pooled\nreplication:\n  plugin: {plugin}\n  poll_interval_ms: 10\n"
    ))?, Arc::new(MetricsRegistry::new()))
}

async fn discover_and_prepare(connector: &PostgresSourceConnector, durable: DurableContext) -> anyhow::Result<()> {
    connector.delivery_discovery(SourceDiscoveryContext { request: DeliveryDiscoveryRequest { keep_system_columns: true }, cancellation: CancellationToken::new(), delivery_type: DeliveryType::Stream }).await?;
    connector.prepare_execution(SourceExecutionContext { request: DeliveryDiscoveryRequest { keep_system_columns: true }, cancellation: CancellationToken::new(), delivery_type: DeliveryType::Stream, replay_identity: Some(Arc::from("pooling-regression-v1")), durable }).await?;
    Ok(())
}

async fn build(connector: &PostgresSourceConnector, durable: DurableContext) -> anyhow::Result<Box<dyn Source>> {
    connector.build_source(SourceBuildContext { partition_id: 0, delivery_type: DeliveryType::Stream, phase: SourcePhase::Stream, replay_identity: Some(Arc::from("pooling-regression-v1")), cancellation: CancellationToken::new(), memory: PipelineMemory::new(64 * 1024 * 1024), durable }).await
}

async fn read_row(source: &mut dyn Source, id: i64, payload: &str, coefficient: i128) -> anyhow::Result<transferia_core::source::CommitMarker> {
    loop {
        match source.read_batch().await? {
            SourceBatch::Typed { tables, source_rows, commit_marker, .. } if source_rows > 0 => {
                assert_eq!(source_rows, 1);
                assert_eq!(tables.len(), 1);
                let batch = &tables[0].batch;
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.column_by_name("id").unwrap().as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), id);
                assert_eq!(batch.column_by_name("payload").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(0), payload);
                let amounts = batch.column_by_name("amount").unwrap().as_any().downcast_ref::<arrow::array::Decimal128Array>().unwrap();
                assert_eq!(amounts.value(0), coefficient);
                assert_eq!(amounts.precision(), 20);
                assert_eq!(amounts.scale(), 4);
                return commit_marker.context("CDC record lacks commit marker");
            }
            SourceBatch::Typed { commit_marker: Some(marker), .. } => source.commit_offsets(&[marker]).await?,
            SourceBatch::Typed { .. } => {}
            _ => anyhow::bail!("expected CDC records"),
        }
    }
}

async fn connect(host: &str, port: u16) -> anyhow::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(&format!("host={host} port={port} user=postgres password=test dbname=transferia"), tokio_postgres::NoTls).await.context("connecting pooling regression fixture")?;
    tokio::spawn(async move { drop(connection.await); });
    Ok(client)
}
