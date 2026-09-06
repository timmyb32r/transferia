#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use arrow::array::{Array as _, BinaryArray, Int64Array, StringArray};
use arrow::datatypes::Schema;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use transferia_connector_postgres::metrics::MetricsRegistry;
use transferia_connector_postgres::postgres::PostgresSourceConnector;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::DeliveryDiscoveryRequest;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source;
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::{
    SourceBuildContext, SourceConnector as _, SourceDiscoveryContext, SourceExecutionContext,
    SourcePhase,
};

const POSTGRES_PORT: u16 = 5_432;
const POSTGRES_IMAGE: &str = "souravbiswassanto/postgres";
const POSTGRES_TAG: &str = concat!(
    "17-wal2json@sha256:",
    "3ee36414cc936dbbf5640a8e8671141815af1d1fb49d465aeeb85b4a4e412879"
);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObservedRow {
    table: String,
    operation: String,
    id: i64,
    payload: String,
}

#[derive(Debug)]
struct ObservedPhase {
    rows: Vec<ObservedRow>,
    schemas: BTreeMap<String, Arc<Schema>>,
    offsets: Vec<i64>,
}

#[tokio::test]
async fn exact_slot_snapshot_hands_off_and_resumes_for_each_decoder_and_copy_format(
) -> anyhow::Result<()> {
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
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute(
            "CREATE TABLE exact_a (id bigint PRIMARY KEY, payload text NOT NULL);\
             ALTER TABLE exact_a REPLICA IDENTITY FULL;\
             CREATE TABLE exact_b (id bigint PRIMARY KEY, payload text NOT NULL);\
             ALTER TABLE exact_b REPLICA IDENTITY FULL;\
             CREATE PUBLICATION transferia_exact_publication FOR TABLE exact_a, exact_b \
                 WITH (publish = 'insert, update, delete');",
        )
        .await
        .context("creating exact snapshot-and-replication test schema")?;

    for (decoder_name, decoder) in [
        (
            "pgoutput",
            "{ type: pgoutput, publication: transferia_exact_publication }",
        ),
        ("wal2json", "{ type: wal2json }"),
        ("auto", "{ type: auto }"),
    ] {
        for copy_format in ["binary", "text"] {
            restore_initial_rows(&client).await?;
            let slot = format!("dttexact_{decoder_name}_{copy_format}");
            verify_exact_boundary(&host, port, &slot, decoder, copy_format, &client)
                .await
                .with_context(|| {
                    format!("verifying {decoder_name} batch_and_stream with {copy_format} COPY")
                })?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn automatic_plugin_works_without_wal2json_and_refuses_foreign_publications(
) -> anyhow::Result<()> {
    let postgres = GenericImage::new("postgres", "17.6-bookworm")
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
        ])
        .start()
        .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute(
            "CREATE TABLE exact_a (id bigint PRIMARY KEY, payload text NOT NULL);\
         ALTER TABLE exact_a REPLICA IDENTITY FULL;\
         CREATE TABLE exact_b (id bigint PRIMARY KEY, payload text NOT NULL);\
         ALTER TABLE exact_b REPLICA IDENTITY FULL;\
         CREATE PUBLICATION dttforeign FOR TABLE exact_a, exact_b;",
        )
        .await?;
    let config = format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.exact_a\n    - include: public.exact_b\nreplication: {{}}\n"
    );
    let connector = postgres_connector(&config)?;
    let cancellation = CancellationToken::new();
    let durable = transferia_test_support::durable_contexts(&["dttforeign"]).remove(0);
    let before: u32 = client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = 'dttforeign'",
            &[],
        )
        .await?
        .get(0);
    let error = connector
        .prepare_execution(combined_execution_context(&durable, &cancellation))
        .await
        .err()
        .context("auto must refuse an unrelated existing publication")?;
    assert!(format!("{error:#}").contains("not owned by this transfer"));
    let after: u32 = client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = 'dttforeign'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(before, after, "a foreign publication was replaced");
    assert_eq!(replication_slot_count(&client, "dttforeign").await?, 0);
    let probes: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE temporary",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(probes, 0);
    restore_initial_rows(&client).await?;
    verify_exact_boundary(
        &host,
        port,
        "dttauto_pgonly",
        "{ type: auto }",
        "binary",
        &client,
    )
    .await?;
    client
        .batch_execute("DROP PUBLICATION dttauto_pgonly")
        .await?;
    let durable = transferia_test_support::durable_contexts(&["dttauto_pgonly"]).remove(0);
    let error = postgres_connector(&config)?
        .prepare_execution(combined_execution_context(&durable, &cancellation))
        .await
        .err()
        .context("a publication missing behind an existing slot must not be recreated")?;
    assert!(format!("{error:#}").contains("refusing to recreate"));
    let publications: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = 'dttauto_pgonly'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(publications, 0);
    Ok(())
}

#[tokio::test]
async fn pgoutput_publication_that_can_omit_changes_fails_before_slot_or_reader_creation(
) -> anyhow::Result<()> {
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
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute(
            "CREATE TABLE publication_a (id bigint PRIMARY KEY, payload text NOT NULL);\
             CREATE TABLE publication_b (id bigint PRIMARY KEY, payload text NOT NULL);\
             CREATE PUBLICATION publication_missing_table FOR TABLE publication_a \
                 WITH (publish = 'insert, update, delete');\
             CREATE PUBLICATION publication_missing_delete FOR TABLE publication_a, publication_b \
                 WITH (publish = 'insert, update');\
             CREATE PUBLICATION publication_row_filter FOR TABLE \
                 publication_a WHERE (id > 0), publication_b \
                 WITH (publish = 'insert, update, delete');\
             CREATE PUBLICATION publication_column_list FOR TABLE \
                 publication_a (id), publication_b \
                 WITH (publish = 'insert, update, delete');",
        )
        .await
        .context("creating PostgreSQL publication validation fixtures")?;

    for (case, publication, required_diagnostic) in [
        (
            "missing_table",
            "publication_missing_table",
            "publication_b",
        ),
        ("missing_delete", "publication_missing_delete", "delete"),
        ("row_filter", "publication_row_filter", "row filter"),
        ("column_list", "publication_column_list", "column"),
    ] {
        assert_publication_rejected_before_execution(
            &host,
            port,
            case,
            publication,
            required_diagnostic,
            &client,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn pgoutput_runtime_publication_drift_is_fatal_without_offset_progress() -> anyhow::Result<()>
{
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
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute(
            "CREATE TABLE drift_events (id bigint PRIMARY KEY, payload text NOT NULL);\
             INSERT INTO drift_events VALUES (1, 'before-boundary');\
             CREATE PUBLICATION publication_runtime_drift FOR TABLE drift_events \
                 WITH (publish = 'insert, update, delete');",
        )
        .await
        .context("creating PostgreSQL runtime publication-drift fixture")?;

    let slot = "runtime_publication_drift";
    let connector = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.drift_events\nreplication:\n  plugin: {{ type: pgoutput, publication: publication_runtime_drift }}\n  poll_interval_ms: 10\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let cancellation = CancellationToken::new();
    let durable = transferia_test_support::durable_contexts(&[slot]).remove(0);
    connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
        })
        .await?;
    connector
        .prepare_execution(SourceExecutionContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
            replay_identity: Some(Arc::from("postgres-combined-e2e-revision-1")),
            durable: durable.clone(),
        })
        .await?
        .expect("valid publication must establish exact execution state");

    let snapshot = read_single_snapshot_partition(&connector, &durable, &cancellation).await?;
    assert_eq!(
        snapshot.rows,
        vec![row("drift_events", "r", 1, "before-boundary")]
    );
    connector
        .complete_execution_phase(
            SourcePhase::Snapshot,
            durable.clone(),
            cancellation.child_token(),
        )
        .await?;
    let mut stream = connector
        .build_source(build_context(
            0,
            SourcePhase::Stream,
            &durable,
            &cancellation,
        ))
        .await?;
    let durable_key = format!("postgres-replication-{slot}");
    let durable_before = durable
        .storage
        .read(&durable_key)
        .await?
        .expect("stream construction must persist its exact start offset");
    let slot_before = replication_slot_confirmed_lsn(&client, slot).await?;

    client
        .batch_execute(
            "ALTER PUBLICATION publication_runtime_drift DROP TABLE drift_events;\
             INSERT INTO drift_events VALUES (2, 'after-drift');",
        )
        .await?;
    let failure = match tokio::time::timeout(Duration::from_secs(10), stream.read_batch()).await {
        Ok(Err(failure)) => failure,
        Ok(Ok(_)) => panic!("publication drift emitted a batch instead of failing closed"),
        Err(error) => {
            panic!("publication drift was not detected before the read deadline: {error}")
        }
    };
    assert!(
        !failure.is_retryable(),
        "publication contract drift must be a fatal source failure: {failure}"
    );
    assert!(
        failure
            .to_string()
            .to_lowercase()
            .contains("publication_runtime_drift"),
        "publication drift diagnostic did not identify the changed publication: {failure}"
    );
    assert_eq!(
        durable.storage.read(&durable_key).await?,
        Some(durable_before),
        "fatal publication drift advanced the durable replication offset"
    );
    assert_eq!(
        replication_slot_confirmed_lsn(&client, slot).await?,
        slot_before,
        "fatal publication drift advanced the server replication slot"
    );
    let persisted: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM drift_events WHERE id = 2",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(
        persisted, 1,
        "source-side DML must remain intact after drift"
    );
    stream.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn interrupted_snapshot_stays_failed_closed_after_the_user_drops_the_exact_slot(
) -> anyhow::Result<()> {
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
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute(
            "CREATE TABLE manual_reset_events (id bigint PRIMARY KEY, payload text NOT NULL);\
             INSERT INTO manual_reset_events VALUES (1, 'before-boundary');\
             CREATE PUBLICATION manual_reset_publication FOR TABLE manual_reset_events \
                 WITH (publish = 'insert, update, delete');",
        )
        .await?;

    let slot = "manual_reset_slot";
    let config = format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.manual_reset_events\nreplication:\n  plugin: {{ type: pgoutput, publication: manual_reset_publication }}\n  poll_interval_ms: 10\n"
    );
    let cancellation = CancellationToken::new();
    let durable = transferia_test_support::durable_contexts(&[slot]).remove(0);
    let state_key = format!("postgres-snapshot-stream-{slot}");

    let interrupted = postgres_connector(&config)?;
    let preview = interrupted
        .delivery_discovery(combined_discovery_context(&cancellation))
        .await?;
    let preview_phases = interrupted.execution_phases(DeliveryType::BatchAndStream, &preview)?;
    let prepared = tokio::time::timeout(
        Duration::from_secs(10),
        interrupted.prepare_execution(combined_execution_context(&durable, &cancellation)),
    )
    .await
    .context("timed out establishing the interrupted exact snapshot")??
    .expect("combined execution must create an exact snapshot");
    assert_eq!(prepared.remaining_phases, preview_phases);
    assert_eq!(replication_slot_count(&client, slot).await?, 1);
    assert_replication_owner_count(&client, 1).await?;
    let interrupted_state = durable
        .storage
        .read(&state_key)
        .await?
        .expect("exact snapshot preparation must persist its phase");

    drop(interrupted);
    assert_replication_owner_count(&client, 0).await?;

    let blocked = postgres_connector(&config)?;
    blocked
        .delivery_discovery(combined_discovery_context(&cancellation))
        .await?;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        blocked.prepare_execution(combined_execution_context(&durable, &cancellation)),
    )
    .await
    .context("timed out checking the interrupted slot")?
    .expect_err("an interrupted snapshot with its slot intact must fail closed");
    let diagnostic = format!("{error:#}");
    assert!(diagnostic.contains(slot), "{diagnostic}");
    assert!(
        diagnostic.contains("remove that exact slot deliberately"),
        "{diagnostic}"
    );
    assert_eq!(
        replication_slot_count(&client, slot).await?,
        1,
        "the blocked retry must not drop or replace the user's slot"
    );
    assert_eq!(
        durable.storage.read(&state_key).await?,
        Some(interrupted_state.clone()),
        "the blocked retry must not rewrite the interrupted durable phase"
    );
    assert_replication_owner_count(&client, 0).await?;
    drop(blocked);

    drop_replication_slot(&client, slot).await?;
    assert_eq!(replication_slot_count(&client, slot).await?, 0);

    let absent_slot_retry = postgres_connector(&config)?;
    absent_slot_retry
        .delivery_discovery(combined_discovery_context(&cancellation))
        .await?;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        absent_slot_retry.prepare_execution(combined_execution_context(&durable, &cancellation)),
    )
    .await
    .context("timed out checking slot-free interrupted snapshot state")?
    .expect_err("an interrupted Snapshot must not be recycled after slot removal");
    let diagnostic = format!("{error:#}");
    assert_eq!(
        durable.storage.read(&state_key).await?,
        Some(interrupted_state),
        "slot removal must not rewrite the interrupted Snapshot phase"
    );
    assert!(
        diagnostic.contains("destination may contain rows"),
        "{diagnostic}"
    );
    assert_eq!(replication_slot_count(&client, slot).await?, 0);
    drop(absent_slot_retry);
    Ok(())
}

#[tokio::test]
async fn global_slot_owner_fences_a_different_delivery_concurrently_and_sequentially(
) -> anyhow::Result<()> {
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
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let client = connect_with_retry(&connection_string(&host, port)).await?;
    client
        .batch_execute(
            "CREATE TABLE lease_events (id bigint PRIMARY KEY, payload text NOT NULL);\
             CREATE PUBLICATION lease_publication FOR TABLE lease_events \
                 WITH (publish = 'insert, update, delete');",
        )
        .await?;

    let slot = "connector_lease_slot";
    let config = format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.lease_events\nreplication:\n  plugin: {{ type: pgoutput, publication: lease_publication }}\n  poll_interval_ms: 10\n"
    );
    let cancellation = CancellationToken::new();
    let mut durables = transferia_test_support::durable_contexts(&[slot, slot]).into_iter();
    let owner_durable = durables.next().expect("owner durable context");
    let contender_durable = durables.next().expect("contender durable context");
    let state_key = format!("postgres-snapshot-stream-{slot}");

    let owner = postgres_connector(&config)?;
    owner
        .delivery_discovery(combined_discovery_context(&cancellation))
        .await?;
    tokio::time::timeout(
        Duration::from_secs(10),
        owner.prepare_execution(combined_execution_context(&owner_durable, &cancellation)),
    )
    .await
    .context("timed out preparing the lease-owning connector")??
    .expect("lease owner must establish an exact snapshot execution");
    let state_before = owner_durable
        .storage
        .read(&state_key)
        .await?
        .expect("lease owner must persist its exact snapshot phase");
    let (system_identifier, database_oid) = postgres_source_identity(&client).await?;
    let resource_key = format!("postgres-replication-{system_identifier}-{database_oid}-{slot}");

    let mut isolated_durable = transferia_test_support::durable_context();
    isolated_durable.delivery_id = Arc::from(slot);
    let isolated_contender = postgres_connector(&config)?;
    isolated_contender
        .delivery_discovery(combined_discovery_context(&cancellation))
        .await?;
    let isolated_error = tokio::time::timeout(
        Duration::from_secs(5),
        isolated_contender
            .prepare_execution(combined_execution_context(&isolated_durable, &cancellation)),
    )
    .await
    .context("timed out waiting for PostgreSQL-side slot fencing")?
    .expect_err("an independent durable root must still be fenced by PostgreSQL");
    assert!(
        isolated_error
            .to_string()
            .contains("execution is already active on the exact source"),
        "unexpected PostgreSQL-side fencing diagnostic: {isolated_error:#}"
    );
    assert_eq!(isolated_durable.storage.read(&state_key).await?, None);
    assert_eq!(
        isolated_durable
            .resource_storage
            .read(&resource_key)
            .await?,
        None,
        "a PostgreSQL-fenced independent durable root persisted ownership"
    );

    let resource_owner = contender_durable
        .resource_storage
        .read(&resource_key)
        .await?
        .expect("slot ownership must be visible in the global resource namespace");
    let resource_owner_json: serde_json::Value = serde_json::from_slice(&resource_owner.payload)?;
    assert_eq!(resource_owner_json["version"], 1);
    assert_eq!(resource_owner_json["delivery_id"], slot);
    assert_eq!(
        resource_owner_json["source"]["system_identifier"],
        system_identifier
    );
    assert_eq!(resource_owner_json["source"]["database"], "transferia");
    assert_eq!(
        resource_owner_json["source"]["database_oid"],
        u64::from(database_oid)
    );
    assert_eq!(resource_owner_json["slot"], slot);
    assert_eq!(replication_slot_count(&client, slot).await?, 1);
    assert_replication_owner_count(&client, 1).await?;

    let contender = postgres_connector(&config)?;
    contender
        .delivery_discovery(combined_discovery_context(&cancellation))
        .await?;
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        contender.prepare_execution(combined_execution_context(
            &contender_durable,
            &cancellation,
        )),
    )
    .await
    .context("timed out waiting for connector lease fencing")?
    .expect_err("a second connector must be fenced while the first holds the execution lease");
    assert!(
        error
            .to_string()
            .contains("execution is already active on the exact source"),
        "unexpected lease-fencing diagnostic: {error:#}"
    );
    assert_eq!(
        owner_durable.storage.read(&state_key).await?,
        Some(state_before),
        "the fenced connector modified durable snapshot state"
    );
    assert_eq!(
        contender_durable.storage.read(&state_key).await?,
        None,
        "the fenced delivery created delivery-local snapshot state"
    );
    assert_eq!(
        contender_durable
            .resource_storage
            .read(&resource_key)
            .await?,
        Some(resource_owner.clone()),
        "the concurrently fenced delivery rewrote global slot ownership"
    );
    assert_eq!(
        replication_slot_count(&client, slot).await?,
        1,
        "the fenced connector modified the owned replication slot"
    );
    assert_replication_owner_count(&client, 1).await?;

    drop(owner);
    assert_replication_owner_count(&client, 0).await?;

    let error = tokio::time::timeout(
        Duration::from_secs(10),
        contender.prepare_execution(combined_execution_context(
            &contender_durable,
            &cancellation,
        )),
    )
    .await
    .context("timed out checking persistent cross-delivery slot ownership")?
    .expect_err("a different durable history must remain fenced after the live lease is released");
    let diagnostic = format!("{error:#}");
    assert!(
        diagnostic.contains("without matching batch_and_stream durable ownership"),
        "{diagnostic}"
    );
    assert_eq!(contender_durable.storage.read(&state_key).await?, None);
    assert_eq!(
        contender_durable
            .resource_storage
            .read(&resource_key)
            .await?,
        Some(resource_owner),
        "the sequentially fenced delivery rewrote global slot ownership"
    );
    assert_eq!(replication_slot_count(&client, slot).await?, 1);

    drop(contender);
    drop_replication_slot(&client, slot).await?;
    Ok(())
}

async fn read_single_snapshot_partition(
    connector: &PostgresSourceConnector,
    durable: &transferia_registry::durable::DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<ObservedPhase> {
    let mut source = connector
        .build_source(build_context(
            0,
            SourcePhase::Snapshot,
            durable,
            cancellation,
        ))
        .await?;
    let mut observed = ObservedPhase {
        rows: Vec::new(),
        schemas: BTreeMap::new(),
        offsets: Vec::new(),
    };
    loop {
        match source.read_batch().await? {
            SourceBatch::Typed {
                tables,
                commit_marker,
                ..
            } => {
                observe_tables(&mut observed, tables, Some("r"))?;
                if let Some(marker) = commit_marker {
                    source.commit_offsets(&[marker]).await?;
                }
            }
            SourceBatch::Finished => break,
            SourceBatch::Dataset { .. } | SourceBatch::Raw { .. } => {
                anyhow::bail!("PostgreSQL exact snapshot emitted a raw batch")
            }
        }
    }
    source.shutdown().await?;
    observed.rows.sort();
    Ok(observed)
}

async fn assert_publication_rejected_before_execution(
    host: &str,
    port: u16,
    case: &str,
    publication: &str,
    required_diagnostic: &str,
    client: &tokio_postgres::Client,
) -> anyhow::Result<()> {
    let slot = format!("rejected_{case}");
    let connector = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.publication_a\n    - include: public.publication_b\nreplication:\n  plugin: {{ type: pgoutput, publication: {publication} }}\n  poll_interval_ms: 10\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let cancellation = CancellationToken::new();
    let durable = transferia_test_support::durable_contexts(&[&slot]).remove(0);
    let phase_key = format!("postgres-snapshot-stream-{slot}");
    let offset_key = format!("postgres-replication-{slot}");
    let (system_identifier, database_oid) = postgres_source_identity(client).await?;
    let resource_key = format!("postgres-replication-{system_identifier}-{database_oid}-{slot}");
    assert_eq!(durable.storage.read(&phase_key).await?, None);
    assert_eq!(durable.storage.read(&offset_key).await?, None);
    assert_eq!(durable.resource_storage.read(&resource_key).await?, None);

    let error = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
        })
        .await
        .err()
        .unwrap_or_else(|| panic!("publication case '{case}' must fail during delivery discovery"));
    let diagnostic = format!("{error:#}").to_lowercase();
    assert!(
        diagnostic.contains(&publication.to_lowercase()),
        "publication failure did not identify '{publication}': {diagnostic}"
    );
    assert!(
        diagnostic.contains(required_diagnostic),
        "publication failure did not explain '{required_diagnostic}': {diagnostic}"
    );

    for phase in [SourcePhase::Snapshot, SourcePhase::Stream] {
        let source = connector
            .build_source(build_context(0, phase, &durable, &cancellation))
            .await;
        assert!(
            source.is_err(),
            "publication case '{case}' constructed a {phase:?} reader after discovery rejection"
        );
    }
    assert_eq!(
        replication_slot_count(client, &slot).await?,
        0,
        "invalid publication created a permanent replication slot"
    );
    assert_replication_owner_count(client, 0).await?;
    assert_eq!(
        durable.resource_storage.read(&resource_key).await?,
        None,
        "invalid publication persisted global slot ownership"
    );
    assert_eq!(
        durable.storage.read(&phase_key).await?,
        None,
        "invalid publication persisted connector-local phase state"
    );
    assert_eq!(
        durable.storage.read(&offset_key).await?,
        None,
        "invalid publication persisted connector-local replication offset"
    );
    Ok(())
}

async fn verify_exact_boundary(
    host: &str,
    port: u16,
    slot: &str,
    decoder: &str,
    copy_format: &str,
    client: &tokio_postgres::Client,
) -> anyhow::Result<()> {
    let config = format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.exact_a\n    - include: public.exact_b\nbatch_rows: 1\ncopy_to_format: {copy_format}\nreplication:\n  plugin: {decoder}\n  poll_interval_ms: 10\n"
    );
    let config = if decoder.contains("auto") {
        config.replace(
            "replication:\n  plugin: { type: auto }\n  poll_interval_ms: 10\n",
            "",
        )
    } else {
        config
    };
    let connector = postgres_connector(&config)?;
    let cancellation = CancellationToken::new();
    let durable = transferia_test_support::durable_contexts(&[slot]).remove(0);
    let preview = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
        })
        .await?;
    let preview_phases = connector.execution_phases(DeliveryType::BatchAndStream, &preview)?;
    assert_eq!(preview_phases.len(), 2);
    assert_eq!(preview_phases[0].phase, SourcePhase::Snapshot);
    assert!(preview_phases[0].finite);
    assert_eq!(preview_phases[1].phase, SourcePhase::Stream);
    assert!(!preview_phases[1].finite);

    let authoritative = connector
        .prepare_execution(SourceExecutionContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
            replay_identity: Some(Arc::from("postgres-combined-e2e-revision-1")),
            durable: durable.clone(),
        })
        .await?
        .expect("batch_and_stream must return slot-snapshot discovery");
    assert_eq!(
        preview.source_topology,
        authoritative.discovery.source_topology
    );
    assert_eq!(
        authoritative.remaining_phases, preview_phases,
        "a fresh exact snapshot must retain the complete authoritative phase plan"
    );
    assert_replication_owner_count(client, 1).await?;

    if decoder.contains("auto") {
        let row = client
            .query_one(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await?;
        assert_eq!(
            row.get::<_, String>(0),
            "pgoutput",
            "auto must prefer pgoutput when both plugins exist"
        );
        let published = client
            .query(
                "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY tablename",
                &[&slot],
            )
            .await?;
        assert_eq!(
            published
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>(),
            ["exact_a", "exact_b"]
        );
        let row = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE temporary",
                &[],
            )
            .await?;
        assert_eq!(
            row.get::<_, i64>(0),
            0,
            "plugin probes must not leave temporary slots"
        );
    }

    mutate_after_boundary(client).await?;
    let snapshot = read_snapshot_phase(&connector, &durable, &cancellation).await?;
    assert_eq!(
        snapshot.rows,
        expected_snapshot_rows(),
        "snapshot included a post-boundary mutation"
    );
    assert_eq!(
        snapshot
            .offsets
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "all snapshot rows must carry one exact slot boundary LSN"
    );
    let boundary_lsn = *snapshot
        .offsets
        .first()
        .expect("the exact snapshot contains rows");

    let premature = connector
        .build_source(build_context(
            0,
            SourcePhase::Stream,
            &durable,
            &cancellation,
        ))
        .await;
    assert!(
        premature.is_err(),
        "replication must not start before successful snapshot-phase completion"
    );

    connector
        .complete_execution_phase(
            SourcePhase::Snapshot,
            durable.clone(),
            cancellation.child_token(),
        )
        .await?;
    assert_replication_owner_count(client, 0).await?;

    drop(connector);
    let resumed_connector = postgres_connector(&config)?;
    let resumed_preview = resumed_connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
        })
        .await?;
    let resumed_preview_phases =
        resumed_connector.execution_phases(DeliveryType::BatchAndStream, &resumed_preview)?;
    let resumed = resumed_connector
        .prepare_execution(SourceExecutionContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::BatchAndStream,
            replay_identity: Some(Arc::from("postgres-combined-e2e-revision-1")),
            durable: durable.clone(),
        })
        .await?
        .expect("persisted exact boundary must prepare a stream-only resume");
    assert_eq!(
        resumed.remaining_phases,
        vec![resumed_preview_phases[1].clone()],
        "persisted Streaming(P) state must skip the completed snapshot phase"
    );
    let snapshot_replay = resumed_connector
        .build_source(build_context(
            0,
            SourcePhase::Snapshot,
            &durable,
            &cancellation,
        ))
        .await
        .err()
        .expect("a resumed connector must not rebuild the completed snapshot");
    assert!(
        snapshot_replay.to_string().contains("owner is unavailable"),
        "unexpected snapshot-replay rejection: {snapshot_replay:#}"
    );

    let stream = read_stream_phase(&resumed_connector, &durable, &cancellation).await?;
    assert_eq!(
        stream.rows,
        expected_stream_rows(),
        "replication has a gap, overlap, or duplicate at the snapshot boundary"
    );
    assert!(
        stream.offsets.iter().all(|offset| *offset > boundary_lsn),
        "post-boundary changes must have commit LSNs after the exact snapshot boundary"
    );
    assert_eq!(
        snapshot.schemas, stream.schemas,
        "snapshot and replication phases must expose one stable Arrow schema"
    );
    assert_projected_final_state(&snapshot.rows, &stream.rows);
    assert_current_rows(client).await?;
    Ok(())
}

fn postgres_connector(config: &str) -> anyhow::Result<PostgresSourceConnector> {
    PostgresSourceConnector::from_config(
        serde_yaml::from_str(config)?,
        Arc::new(MetricsRegistry::new()),
    )
}

async fn read_snapshot_phase(
    connector: &PostgresSourceConnector,
    durable: &transferia_registry::durable::DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<ObservedPhase> {
    let mut observed = ObservedPhase {
        rows: Vec::new(),
        schemas: BTreeMap::new(),
        offsets: Vec::new(),
    };
    for partition_id in [0, 1] {
        let mut source = connector
            .build_source(build_context(
                partition_id,
                SourcePhase::Snapshot,
                durable,
                cancellation,
            ))
            .await?;
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    commit_marker,
                    ..
                } => {
                    observe_tables(&mut observed, tables, Some("r"))?;
                    if let Some(marker) = commit_marker {
                        source.commit_offsets(&[marker]).await?;
                    }
                }
                SourceBatch::Finished => break,
                SourceBatch::Dataset { .. } | SourceBatch::Raw { .. } => {
                    anyhow::bail!("PostgreSQL exact snapshot emitted a raw batch")
                }
            }
        }
        source.shutdown().await?;
    }
    observed.rows.sort();
    Ok(observed)
}

async fn read_stream_phase(
    connector: &PostgresSourceConnector,
    durable: &transferia_registry::durable::DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<ObservedPhase> {
    let mut source = connector
        .build_source(build_context(0, SourcePhase::Stream, durable, cancellation))
        .await?;
    let mut observed = ObservedPhase {
        rows: Vec::new(),
        schemas: BTreeMap::new(),
        offsets: Vec::new(),
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while observed.rows.len() < 6 {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    source_rows,
                    commit_marker,
                    ..
                } => {
                    observe_tables(&mut observed, tables, None)?;
                    if let Some(marker) = commit_marker {
                        source.commit_offsets(&[marker]).await?;
                    } else {
                        anyhow::ensure!(
                            source_rows == 0,
                            "non-empty PostgreSQL CDC batch has no commit marker"
                        );
                    }
                    anyhow::ensure!(
                        observed.rows.len() <= 6,
                        "PostgreSQL CDC emitted unexpected extra rows"
                    );
                }
                SourceBatch::Finished => {
                    anyhow::bail!("PostgreSQL replication ended before all changes arrived")
                }
                SourceBatch::Dataset { .. } | SourceBatch::Raw { .. } => {
                    anyhow::bail!("PostgreSQL replication emitted a raw batch")
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for exact post-boundary changes"))??;
    source.shutdown().await?;
    observed.rows.sort();
    Ok(observed)
}

fn observe_tables(
    observed: &mut ObservedPhase,
    tables: Vec<TableData>,
    expected_operation: Option<&str>,
) -> anyhow::Result<()> {
    for table in tables {
        let table_name = table.table.to_string();
        let prior = observed
            .schemas
            .insert(table_name.clone(), table.batch.schema());
        if let Some(prior) = prior {
            anyhow::ensure!(
                prior == table.batch.schema(),
                "table '{table_name}' changed schema within one source phase"
            );
        }
        let ids = array_by_name::<Int64Array>(&table, "id")?;
        let payloads = array_by_name::<StringArray>(&table, "payload")?;
        let operations = system_array::<StringArray>(&table, SystemColumnKind::ChangeOperation);
        let changed = system_array::<BinaryArray>(&table, SystemColumnKind::ChangedColumns);
        let offsets = system_array::<Int64Array>(&table, SystemColumnKind::Offset);
        for row in 0..table.batch.num_rows() {
            let operation = operations.value(row);
            if let Some(expected) = expected_operation {
                anyhow::ensure!(
                    operation == expected,
                    "snapshot row used operation '{operation}' instead of '{expected}'"
                );
                anyhow::ensure!(
                    changed.value(row) == [0b11],
                    "snapshot row did not mark every user column as changed"
                );
            } else {
                anyhow::ensure!(
                    matches!(operation, "c" | "u" | "d"),
                    "stream emitted non-change operation '{operation}'"
                );
            }
            anyhow::ensure!(!ids.is_null(row), "change row lost its primary key");
            anyhow::ensure!(!payloads.is_null(row), "change row lost its payload");
            observed.rows.push(ObservedRow {
                table: table_name.clone(),
                operation: operation.to_owned(),
                id: ids.value(row),
                payload: payloads.value(row).to_owned(),
            });
            observed.offsets.push(offsets.value(row));
        }
    }
    Ok(())
}

fn array_by_name<'a, T: arrow::array::Array + 'static>(
    table: &'a TableData,
    name: &str,
) -> anyhow::Result<&'a T> {
    let index = table.batch.schema().index_of(name)?;
    table
        .batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| anyhow::anyhow!("column '{name}' has an unexpected Arrow type"))
}

fn system_array<T: arrow::array::Array + 'static>(table: &TableData, kind: SystemColumnKind) -> &T {
    let column = table
        .system_columns
        .get(kind)
        .expect("required batch_and_stream system column");
    table
        .batch
        .column(column.index)
        .as_any()
        .downcast_ref()
        .unwrap()
}

fn build_context(
    partition_id: i64,
    phase: SourcePhase,
    durable: &transferia_registry::durable::DurableContext,
    cancellation: &CancellationToken,
) -> SourceBuildContext {
    SourceBuildContext {
        partition_id,
        delivery_type: DeliveryType::BatchAndStream,
        phase,
        replay_identity: Some(Arc::from("postgres-combined-e2e-revision-1")),
        cancellation: cancellation.child_token(),
        memory: PipelineMemory::new(64 * 1024 * 1024),
        durable: durable.clone(),
    }
}

fn combined_discovery_context(cancellation: &CancellationToken) -> SourceDiscoveryContext {
    SourceDiscoveryContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type: DeliveryType::BatchAndStream,
    }
}

fn combined_execution_context(
    durable: &transferia_registry::durable::DurableContext,
    cancellation: &CancellationToken,
) -> SourceExecutionContext {
    SourceExecutionContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type: DeliveryType::BatchAndStream,
        replay_identity: Some(Arc::from("postgres-combined-e2e-revision-1")),
        durable: durable.clone(),
    }
}

async fn restore_initial_rows(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    client
        .batch_execute(
            "TRUNCATE exact_a, exact_b;\
             INSERT INTO exact_a VALUES (1, 'a-one'), (2, 'a-two');\
             INSERT INTO exact_b VALUES (10, 'b-ten'), (20, 'b-twenty');",
        )
        .await?;
    Ok(())
}

async fn mutate_after_boundary(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    client
        .batch_execute(
            "BEGIN;\
             INSERT INTO exact_a VALUES (3, 'a-three');\
             UPDATE exact_a SET payload = 'a-updated' WHERE id = 1;\
             DELETE FROM exact_a WHERE id = 2;\
             INSERT INTO exact_b VALUES (30, 'b-thirty');\
             UPDATE exact_b SET payload = 'b-updated' WHERE id = 10;\
             DELETE FROM exact_b WHERE id = 20;\
             COMMIT;",
        )
        .await?;
    Ok(())
}

fn expected_snapshot_rows() -> Vec<ObservedRow> {
    vec![
        row("exact_a", "r", 1, "a-one"),
        row("exact_a", "r", 2, "a-two"),
        row("exact_b", "r", 10, "b-ten"),
        row("exact_b", "r", 20, "b-twenty"),
    ]
}

fn expected_stream_rows() -> Vec<ObservedRow> {
    let mut rows = vec![
        row("exact_a", "c", 3, "a-three"),
        row("exact_a", "u", 1, "a-updated"),
        row("exact_a", "d", 2, "a-two"),
        row("exact_b", "c", 30, "b-thirty"),
        row("exact_b", "u", 10, "b-updated"),
        row("exact_b", "d", 20, "b-twenty"),
    ];
    rows.sort();
    rows
}

fn row(table: &str, operation: &str, id: i64, payload: &str) -> ObservedRow {
    ObservedRow {
        table: table.to_owned(),
        operation: operation.to_owned(),
        id,
        payload: payload.to_owned(),
    }
}

fn assert_projected_final_state(snapshot: &[ObservedRow], stream: &[ObservedRow]) {
    let mut state = snapshot
        .iter()
        .map(|row| ((row.table.clone(), row.id), row.payload.clone()))
        .collect::<BTreeMap<_, _>>();
    for row in stream {
        match row.operation.as_str() {
            "c" | "u" => {
                state.insert((row.table.clone(), row.id), row.payload.clone());
            }
            "d" => {
                assert!(
                    state.remove(&(row.table.clone(), row.id)).is_some(),
                    "delete addressed a row absent from the exact snapshot"
                );
            }
            operation => panic!("unexpected projected operation '{operation}'"),
        }
    }
    assert_eq!(
        state,
        BTreeMap::from([
            (("exact_a".to_owned(), 1), "a-updated".to_owned()),
            (("exact_a".to_owned(), 3), "a-three".to_owned()),
            (("exact_b".to_owned(), 10), "b-updated".to_owned()),
            (("exact_b".to_owned(), 30), "b-thirty".to_owned()),
        ])
    );
}

async fn assert_current_rows(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    for (table, expected) in [
        ("exact_a", "1:a-updated,3:a-three"),
        ("exact_b", "10:b-updated,30:b-thirty"),
    ] {
        let query =
            format!("SELECT string_agg(id::text || ':' || payload, ',' ORDER BY id) FROM {table}");
        let actual: String = client.query_one(&query, &[]).await?.try_get(0)?;
        assert_eq!(actual, expected);
    }
    Ok(())
}

async fn replication_slot_count(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<i64> {
    Ok(client
        .query_one(
            "SELECT count(*)::bigint FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
        .try_get(0)?)
}

async fn replication_slot_confirmed_lsn(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<String> {
    Ok(client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
        .try_get(0)?)
}

async fn postgres_source_identity(client: &tokio_postgres::Client) -> anyhow::Result<(u64, u32)> {
    let row = client
        .query_one(
            "SELECT system_identifier::text, \
                    (SELECT oid FROM pg_database WHERE datname = current_database()) \
             FROM pg_control_system()",
            &[],
        )
        .await?;
    Ok((row.try_get::<_, &str>(0)?.parse()?, row.try_get(1)?))
}

async fn drop_replication_slot(client: &tokio_postgres::Client, slot: &str) -> anyhow::Result<()> {
    client
        .query_one("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await?;
    Ok(())
}

async fn assert_replication_owner_count(
    client: &tokio_postgres::Client,
    expected: i64,
) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let owners = client
                .query_one(
                    "SELECT count(*)::bigint FROM pg_stat_activity \
                     WHERE datname = current_database() AND backend_type = 'walsender'",
                    &[],
                )
                .await?
                .try_get::<_, i64>(0)?;
            if owners == expected {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!("PostgreSQL replication snapshot owner count did not become {expected}")
    })??;
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
