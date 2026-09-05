use super::*;
use crate::connectors::postgres::source::DiscoveredTable;
use crate::connectors::postgres::src_stream::is_replication_safety_violation;
use arrow::datatypes::DataType;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

const REPLAY_IDENTITY: &str = "delivery-revision-1";

fn source() -> PostgresSourceIdentity {
    PostgresSourceIdentity {
        system_identifier: 7_412_345_678_901_234_567,
        database: "inventory".to_owned(),
        database_oid: 16_384,
    }
}

fn config() -> LogicalDecoder {
    LogicalDecoder::Pgoutput {
        publication: "transferia_publication".to_owned(),
    }
}

fn durable_context() -> transferia_registry::durable::DurableContext {
    transferia_test_support::durable_contexts(&["transferia_exact_boundary"]).remove(0)
}

fn tables() -> Vec<TableConfig> {
    vec![
        TableConfig {
            schema: "public".to_owned(),
            name: "accounts".to_owned(),
        },
        TableConfig {
            schema: "audit".to_owned(),
            name: "events".to_owned(),
        },
    ]
}

fn discovered_tables() -> Vec<DiscoveredTable> {
    tables()
        .into_iter()
        .enumerate()
        .map(|(index, config)| DiscoveredTable {
            config,
            schema: DatasetSchema::new(vec![
                SchemaColumn::new("id".to_owned(), DataType::Int64, false)
                    .with_constraints(true, false, None),
                SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
            ]),
            type_oids: vec![20, 25],
            replica_identity_full: index == 1,
            replica_identity: if index == 1 { "f" } else { "d" }.to_owned(),
            relation_oid: 16_385 + u32::try_from(index).unwrap(),
        })
        .collect()
}

#[tokio::test]
async fn exact_boundary_becomes_the_only_resumable_stream_start() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must claim snapshot creation");
    };

    tracker
        .mark_snapshot_ready(0xA_BCDE, &discovered_tables())
        .await
        .unwrap();
    assert_eq!(tracker.streaming_lsn(), None);
    assert_eq!(tracker.mark_streaming().await.unwrap(), 0xA_BCDE);
    assert_eq!(tracker.streaming_lsn(), Some(0xA_BCDE));
    drop(tracker);

    let SnapshotStreamPreparation::Streaming { tracker, start_lsn } =
        SnapshotStreamTracker::claim_or_resume(
            &config(),
            &tables(),
            &source(),
            durable,
            true,
            REPLAY_IDENTITY,
        )
        .await
        .unwrap()
    else {
        panic!("committed boundary must resume as streaming");
    };
    assert_eq!(start_lsn, 0xA_BCDE);
    assert_eq!(tracker.streaming_lsn(), Some(0xA_BCDE));
}

#[tokio::test]
async fn claimed_or_snapshot_state_never_invents_a_new_snapshot() {
    for mark_snapshot in [false, true] {
        let durable = durable_context();
        let SnapshotStreamPreparation::Create(mut tracker) =
            SnapshotStreamTracker::claim_or_resume(
                &config(),
                &tables(),
                &source(),
                durable.clone(),
                false,
                REPLAY_IDENTITY,
            )
            .await
            .unwrap()
        else {
            panic!("fresh state must be claimed");
        };
        if mark_snapshot {
            tracker
                .mark_snapshot_ready(42, &discovered_tables())
                .await
                .unwrap();
        }
        drop(tracker);

        let error = SnapshotStreamTracker::claim_or_resume(
            &config(),
            &tables(),
            &source(),
            durable,
            true,
            REPLAY_IDENTITY,
        )
        .await
        .err()
        .expect("an exported snapshot cannot survive process loss");
        assert!(is_replication_safety_violation(&error));
        let message = error.to_string();
        assert!(message.contains("interrupted"), "{message}");
        assert!(
            message.contains("remove that exact slot deliberately"),
            "{message}"
        );
    }
}

#[tokio::test]
async fn durable_identity_rejects_replay_source_or_table_changes() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must be claimed");
    };
    tracker
        .mark_snapshot_ready(91, &discovered_tables())
        .await
        .unwrap();
    tracker.mark_streaming().await.unwrap();
    drop(tracker);

    let changed_config = LogicalDecoder::Pgoutput {
        publication: "other_publication".to_owned(),
    };
    let changed_tables = vec![TableConfig {
        schema: "public".to_owned(),
        name: "other_table".to_owned(),
    }];
    for result in [
        SnapshotStreamTracker::claim_or_resume(
            &changed_config,
            &tables(),
            &source(),
            durable.clone(),
            true,
            REPLAY_IDENTITY,
        )
        .await,
        SnapshotStreamTracker::claim_or_resume(
            &config(),
            &changed_tables,
            &source(),
            durable.clone(),
            true,
            REPLAY_IDENTITY,
        )
        .await,
        SnapshotStreamTracker::claim_or_resume(
            &config(),
            &tables(),
            &source(),
            durable.clone(),
            true,
            "delivery-revision-2",
        )
        .await,
    ] {
        let error = result.err().expect("identity drift must fail");
        assert!(is_replication_safety_violation(&error));
        let message = error.to_string();
        assert!(message.contains("different replay-affecting"), "{message}");
    }
}

#[tokio::test]
async fn slot_free_claimed_state_is_recycled_before_destination_delivery_can_start() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must be claimed");
    };
    drop(tracker);

    let SnapshotStreamPreparation::Create(recycled) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable,
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("slot-free claimed state must be recycled");
    };
    assert_eq!(recycled.streaming_lsn(), None);
}

#[tokio::test]
async fn slot_free_snapshot_state_fails_closed_before_a_new_boundary_is_created() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must be claimed");
    };
    tracker
        .mark_snapshot_ready(42, &discovered_tables())
        .await
        .unwrap();
    drop(tracker);

    let error = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable,
        false,
        REPLAY_IDENTITY,
    )
    .await
    .err()
    .expect("a missing slot cannot prove that prior destination rows were rolled back");
    assert!(is_replication_safety_violation(&error));
    let message = error.to_string();
    assert!(
        message.contains("refusing to bootstrap a new snapshot"),
        "{message}"
    );
    assert!(
        message.contains("destination may contain rows"),
        "{message}"
    );
}

#[tokio::test]
async fn unowned_existing_slot_is_never_claimed() {
    let error = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable_context(),
        true,
        REPLAY_IDENTITY,
    )
    .await
    .err()
    .expect("an existing slot without durable ownership must fail");
    assert!(is_replication_safety_violation(&error));
    let message = error.to_string();
    assert!(message.contains("without matching"), "{message}");
    assert!(message.contains("refusing to replace"), "{message}");
}

#[tokio::test]
async fn durable_phase_rejects_cluster_database_and_database_oid_drift() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must be claimed");
    };
    tracker
        .mark_snapshot_ready(91, &discovered_tables())
        .await
        .unwrap();
    tracker.mark_streaming().await.unwrap();
    drop(tracker);

    let mut identities = [source(), source(), source()];
    identities[0].system_identifier += 1;
    identities[1].database = "replacement".to_owned();
    identities[2].database_oid += 1;
    for identity in identities {
        let error = SnapshotStreamTracker::claim_or_resume(
            &config(),
            &tables(),
            &identity,
            durable.clone(),
            true,
            REPLAY_IDENTITY,
        )
        .await
        .err()
        .expect("PostgreSQL source identity drift must fail");
        assert!(is_replication_safety_violation(&error));
        let message = error.to_string();
        assert!(message.contains("different replay-affecting"), "{message}");
    }
}

#[tokio::test]
async fn durable_phase_rejects_authoritative_oid_schema_and_replica_identity_drift() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must be claimed");
    };
    tracker
        .mark_snapshot_ready(91, &discovered_tables())
        .await
        .unwrap();
    tracker.mark_streaming().await.unwrap();
    drop(tracker);

    let SnapshotStreamPreparation::Streaming { tracker, .. } =
        SnapshotStreamTracker::claim_or_resume(
            &config(),
            &tables(),
            &source(),
            durable,
            true,
            REPLAY_IDENTITY,
        )
        .await
        .unwrap()
    else {
        panic!("committed boundary must resume as streaming");
    };

    let mut oid_drift = discovered_tables();
    oid_drift[0].type_oids[1] = 1043;
    let mut schema_drift = discovered_tables();
    schema_drift[0].schema.columns[1].nullable = false;
    let mut relation_drift = discovered_tables();
    relation_drift[0].relation_oid += 1;
    let mut replica_identity_drift = discovered_tables();
    replica_identity_drift[1].replica_identity_full = false;
    replica_identity_drift[1].replica_identity = "d".to_owned();
    for changed in [
        oid_drift,
        schema_drift,
        relation_drift,
        replica_identity_drift,
    ] {
        let error = tracker
            .validate_authoritative_tables(&changed)
            .expect_err("authoritative schema drift must fail");
        assert!(is_replication_safety_violation(&error));
        let message = error.to_string();
        assert!(
            message.contains("authoritative table schema changed"),
            "{message}"
        );
    }
}

#[tokio::test]
async fn execution_lease_fences_a_concurrent_phase_claim() {
    let durable = durable_context();
    let lease = durable
        .storage
        .acquire_execution_lease("postgres-replication-transferia_exact_boundary")
        .await
        .unwrap();
    let message = durable
        .storage
        .acquire_execution_lease("postgres-replication-transferia_exact_boundary")
        .await
        .err()
        .expect("a second execution must be fenced before its durable claim")
        .to_string();
    assert!(message.contains("already owns"), "{message}");

    let result = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable,
        false,
        REPLAY_IDENTITY,
    )
    .await;
    assert!(matches!(result, Ok(SnapshotStreamPreparation::Create(_))));
    drop(lease);
}

#[tokio::test]
async fn durable_phase_payload_contains_identity_and_boundary_but_no_snapshot_token() {
    let durable = durable_context();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        &config(),
        &tables(),
        &source(),
        durable.clone(),
        false,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap() else {
        panic!("fresh state must be claimed");
    };
    tracker
        .mark_snapshot_ready(123, &discovered_tables())
        .await
        .unwrap();
    let value = durable
        .storage
        .read("postgres-snapshot-stream-transferia_exact_boundary")
        .await
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&value.payload).unwrap();
    assert_eq!(json["state"]["phase"], "snapshot");
    assert_eq!(json["state"]["consistent_lsn"], 123);
    assert_eq!(json["replay_identity"], REPLAY_IDENTITY);
    assert_eq!(
        json["source"]["system_identifier"],
        source().system_identifier
    );
    assert_eq!(json["source"]["database"], "inventory");
    assert_eq!(json["source"]["database_oid"], 16_384);
    assert_eq!(
        json["authoritative_tables"][0]["columns"][0]["postgres_type_oid"],
        20
    );
    assert_eq!(json["authoritative_tables"][0]["relation_oid"], 16_385);
    assert_eq!(json["authoritative_tables"][1]["replica_identity"], "f");
    assert!(json.get("snapshot_name").is_none());
    assert!(!String::from_utf8(value.payload)
        .unwrap()
        .contains("password"));
}
