use std::sync::Arc;

use arrow::datatypes::DataType;

use super::{
    decode_state, is_replication_safety_violation, recreate_slot_query,
    recreated_slot_recovery_plan, validate_slot_database, RecreatedSlotRecoveryPlan,
    ReplicationOffsetState, ReplicationSlotTracker, EXISTING_SLOT_QUERY, STATE_VERSION,
};
use crate::connectors::postgres::source::{DiscoveredTable, TableConfig};
use crate::connectors::postgres::src_stream::identity::{
    authoritative_table_identities, PostgresSourceIdentity,
};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

const REPLAY_IDENTITY: &str = "delivery-revision-1";

fn source() -> PostgresSourceIdentity {
    PostgresSourceIdentity {
        system_identifier: 7_412_345_678_901_234_567,
        database: "inventory".to_owned(),
        database_oid: 16_384,
    }
}

fn discovered_tables() -> Vec<DiscoveredTable> {
    vec![DiscoveredTable {
        config: TableConfig {
            schema: "public".to_owned(),
            name: "accounts".to_owned(),
        },
        schema: DatasetSchema::new(vec![
            SchemaColumn::new("id".to_owned(), DataType::Int64, false)
                .with_constraints(true, false, None),
            SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
        ]),
        type_oids: vec![20, 25],
        replica_identity_full: false,
        replica_identity: "d".to_owned(),
        relation_oid: 16_385,
    }]
}

#[test]
fn durable_replication_offset_is_strict_and_bound_to_slot_and_plugin() {
    let source = source();
    let tables = authoritative_table_identities(&discovered_tables()).unwrap();
    let payload = serde_json::to_vec(&ReplicationOffsetState {
        version: STATE_VERSION,
        replay_identity: REPLAY_IDENTITY.to_owned(),
        slot: "transferia_slot".into(),
        plugin: "pgoutput".into(),
        publication: Some("transferia_publication".into()),
        source: source.clone(),
        authoritative_tables: tables.clone(),
        committed_lsn: 0x1234,
    })
    .unwrap();
    assert_eq!(
        decode_state(
            &payload,
            "transferia_slot",
            "pgoutput",
            Some("transferia_publication"),
            &source,
            &tables,
            REPLAY_IDENTITY,
        )
        .unwrap(),
        0x1234
    );
    let identity_error = decode_state(
        &payload,
        "other",
        "pgoutput",
        Some("transferia_publication"),
        &source,
        &tables,
        REPLAY_IDENTITY,
    )
    .expect_err("a different slot identity must fail");
    assert!(is_replication_safety_violation(&identity_error));
    let plugin_error = decode_state(
        &payload,
        "transferia_slot",
        "wal2json",
        None,
        &source,
        &tables,
        REPLAY_IDENTITY,
    )
    .expect_err("a different decoder plugin must fail");
    assert!(is_replication_safety_violation(&plugin_error));
    let publication_error = decode_state(
        &payload,
        "transferia_slot",
        "pgoutput",
        Some("replacement_publication"),
        &source,
        &tables,
        REPLAY_IDENTITY,
    )
    .expect_err("a different publication must fail");
    assert!(is_replication_safety_violation(&publication_error));
    let replay_error = decode_state(
        &payload,
        "transferia_slot",
        "pgoutput",
        Some("transferia_publication"),
        &source,
        &tables,
        "delivery-revision-2",
    )
    .expect_err("a different replay-affecting delivery revision must fail");
    assert!(is_replication_safety_violation(&replay_error));
    let corrupt_error = decode_state(
        b"{}",
        "transferia_slot",
        "pgoutput",
        Some("transferia_publication"),
        &source,
        &tables,
        REPLAY_IDENTITY,
    )
    .expect_err("corrupt durable state must fail");
    assert!(is_replication_safety_violation(&corrupt_error));

    let future = serde_json::to_vec(&ReplicationOffsetState {
        version: STATE_VERSION + 1,
        replay_identity: REPLAY_IDENTITY.to_owned(),
        slot: "transferia_slot".into(),
        plugin: "pgoutput".into(),
        publication: Some("transferia_publication".into()),
        source: source.clone(),
        authoritative_tables: tables.clone(),
        committed_lsn: 0x1234,
    })
    .unwrap();
    let version_error = decode_state(
        &future,
        "transferia_slot",
        "pgoutput",
        Some("transferia_publication"),
        &source,
        &tables,
        REPLAY_IDENTITY,
    )
    .expect_err("an unsupported durable state version must fail");
    assert!(is_replication_safety_violation(&version_error));
}

#[test]
fn durable_offset_rejects_cluster_database_oid_and_schema_drift_before_slot_io() {
    let source = source();
    let tables = authoritative_table_identities(&discovered_tables()).unwrap();
    let payload = serde_json::to_vec(&ReplicationOffsetState {
        version: STATE_VERSION,
        replay_identity: REPLAY_IDENTITY.to_owned(),
        slot: "transferia_slot".into(),
        plugin: "pgoutput".into(),
        publication: Some("transferia_publication".into()),
        source: source.clone(),
        authoritative_tables: tables.clone(),
        committed_lsn: 0x1234,
    })
    .unwrap();

    let mut identities = [source.clone(), source.clone(), source.clone()];
    identities[0].system_identifier += 1;
    identities[1].database = "replacement".to_owned();
    identities[2].database_oid += 1;
    for changed in identities {
        assert!(decode_state(
            &payload,
            "transferia_slot",
            "pgoutput",
            Some("transferia_publication"),
            &changed,
            &tables,
            REPLAY_IDENTITY,
        )
        .is_err());
    }

    let mut oid_drift = discovered_tables();
    oid_drift[0].type_oids[1] = 1043;
    let mut schema_drift = discovered_tables();
    schema_drift[0].schema.columns[1].nullable = false;
    let mut relation_drift = discovered_tables();
    relation_drift[0].relation_oid += 1;
    let mut replica_identity_drift = discovered_tables();
    replica_identity_drift[0].replica_identity = "n".to_owned();
    for changed in [
        oid_drift,
        schema_drift,
        relation_drift,
        replica_identity_drift,
    ] {
        let changed = authoritative_table_identities(&changed).unwrap();
        assert!(decode_state(
            &payload,
            "transferia_slot",
            "pgoutput",
            Some("transferia_publication"),
            &source,
            &changed,
            REPLAY_IDENTITY,
        )
        .is_err());
    }
}

#[test]
fn pg_tm_aux_schema_is_always_quoted_as_one_identifier() {
    assert_eq!(
        recreate_slot_query("tm_aux"),
        "SELECT slot_name, lsn::text FROM \"tm_aux\".\"pg_create_logical_replication_slot_lsn\"($1, $2, false, $3::pg_lsn)"
    );
    assert_eq!(
        recreate_slot_query("evil\".public; DROP SCHEMA public; --"),
        "SELECT slot_name, lsn::text FROM \"evil\"\".public; DROP SCHEMA public; --\".\"pg_create_logical_replication_slot_lsn\"($1, $2, false, $3::pg_lsn)"
    );
}

#[test]
fn recreated_slot_catches_up_and_verifies_before_recovery_can_finish() {
    assert_eq!(
        recreated_slot_recovery_plan(90, 100).unwrap(),
        RecreatedSlotRecoveryPlan::CatchUpThenVerifyExact,
        "a slot recreated behind durable progress must not be exposed before catch-up and verification"
    );
    assert_eq!(
        recreated_slot_recovery_plan(100, 100).unwrap(),
        RecreatedSlotRecoveryPlan::VerifyExact,
        "even an exact recreation must be verified against the server catalog"
    );

    let error = recreated_slot_recovery_plan(101, 100)
        .expect_err("a slot recreated ahead of durable progress must fail closed");
    assert!(is_replication_safety_violation(&error));
    assert!(error.to_string().contains("ahead of durable LSN"));
}

#[test]
fn slot_owned_by_another_database_fails_closed_before_recreation() {
    assert_eq!(
        EXISTING_SLOT_QUERY,
        "SELECT plugin, confirmed_flush_lsn::text, database::text, datoid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        "slot lookup must be cluster-global so a same-name slot in another database cannot look absent",
    );
    validate_slot_database(
        "transferia_slot",
        Some("inventory"),
        Some(16_384),
        "inventory",
        16_384,
    )
    .unwrap();

    for (actual_database, actual_database_oid) in [
        (Some("other_database"), Some(16_384)),
        (Some("inventory"), Some(16_385)),
        (None, None),
    ] {
        let error = validate_slot_database(
            "transferia_slot",
            actual_database,
            actual_database_oid,
            "inventory",
            16_384,
        )
        .expect_err("a slot outside the exact database must not look absent");
        assert!(is_replication_safety_violation(&error));
        assert!(error.to_string().contains("belongs to database"));
    }
}

#[tokio::test]
async fn durable_replication_offset_has_single_writer_cas_semantics() {
    let durable = transferia_test_support::durable_context();
    let source = source();
    let tables = authoritative_table_identities(&discovered_tables()).unwrap();
    let mut first = ReplicationSlotTracker {
        storage: Arc::clone(&durable.storage),
        key: "postgres-replication-slot".into(),
        revision: None,
        replay_identity: Arc::from(REPLAY_IDENTITY),
        slot: "slot".into(),
        plugin: "pgoutput".into(),
        publication: Some("transferia_publication".into()),
        source: source.clone(),
        authoritative_tables: tables.clone(),
    };
    let mut competing = ReplicationSlotTracker {
        storage: Arc::clone(&durable.storage),
        key: "postgres-replication-slot".into(),
        revision: None,
        replay_identity: Arc::from(REPLAY_IDENTITY),
        slot: "slot".into(),
        plugin: "pgoutput".into(),
        publication: Some("transferia_publication".into()),
        source: source.clone(),
        authoritative_tables: tables.clone(),
    };

    first.store(100).await.unwrap();
    assert!(competing.store(200).await.is_err());
    first.store(300).await.unwrap();
    let stored = durable
        .storage
        .read("postgres-replication-slot")
        .await
        .unwrap()
        .unwrap();
    let stored_json: serde_json::Value = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!(stored_json["replay_identity"], REPLAY_IDENTITY);
    assert_eq!(
        decode_state(
            &stored.payload,
            "slot",
            "pgoutput",
            Some("transferia_publication"),
            &source,
            &tables,
            REPLAY_IDENTITY,
        )
        .unwrap(),
        300
    );
}
