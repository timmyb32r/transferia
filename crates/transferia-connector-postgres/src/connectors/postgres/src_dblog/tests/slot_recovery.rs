use std::sync::Arc;

use super::{
    decode_state, recreate_slot_query, ReplicationOffsetState, ReplicationSlotTracker,
    STATE_VERSION,
};

#[test]
fn durable_replication_offset_is_strict_and_bound_to_slot_and_plugin() {
    let payload = serde_json::to_vec(&ReplicationOffsetState {
        version: STATE_VERSION,
        slot: "transferia_slot".into(),
        plugin: "pgoutput".into(),
        committed_lsn: 0x1234,
    })
    .unwrap();
    assert_eq!(
        decode_state(&payload, "transferia_slot", "pgoutput").unwrap(),
        0x1234
    );
    assert!(decode_state(&payload, "other", "pgoutput").is_err());
    assert!(decode_state(&payload, "transferia_slot", "wal2json").is_err());
    assert!(decode_state(b"{}", "transferia_slot", "pgoutput").is_err());

    let future = serde_json::to_vec(&ReplicationOffsetState {
        version: STATE_VERSION + 1,
        slot: "transferia_slot".into(),
        plugin: "pgoutput".into(),
        committed_lsn: 0x1234,
    })
    .unwrap();
    assert!(decode_state(&future, "transferia_slot", "pgoutput").is_err());
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

#[tokio::test]
async fn durable_replication_offset_has_single_writer_cas_semantics() {
    let durable = transferia_test_support::durable_context();
    let mut first = ReplicationSlotTracker {
        storage: Arc::clone(&durable.storage),
        key: "postgres-replication-slot".into(),
        revision: None,
        slot: "slot".into(),
        plugin: "pgoutput".into(),
    };
    let mut competing = ReplicationSlotTracker {
        storage: Arc::clone(&durable.storage),
        key: "postgres-replication-slot".into(),
        revision: None,
        slot: "slot".into(),
        plugin: "pgoutput".into(),
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
    assert_eq!(decode_state(&stored.payload, "slot", "pgoutput").unwrap(), 300);
}
