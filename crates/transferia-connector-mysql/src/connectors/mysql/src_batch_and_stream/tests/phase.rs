#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "durable phase tests intentionally fail fast"
)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use transferia_registry::durable::{
    CompareExchangeResult, DurableContext, DurableStorage, DurableValue,
};

use super::*;
use crate::connectors::mysql::src_batch::TableConfig;
use crate::connectors::mysql::src_batch_and_stream::is_replication_safety_violation;

const REPLAY_IDENTITY: &str = "delivery-revision-17";

#[derive(Default)]
struct MemoryDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
    conflict_next_cas: AtomicBool,
}

impl DurableStorage for MemoryDurableStorage {
    fn read<'a>(&'a self, key: &'a str) -> BoxFuture<'a, anyhow::Result<Option<DurableValue>>> {
        Box::pin(async move {
            Ok(self
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .cloned())
        })
    }

    fn compare_exchange<'a>(
        &'a self,
        key: &'a str,
        expected_revision: Option<u64>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<CompareExchangeResult>> {
        Box::pin(async move {
            let mut values = self
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = values.get(key).cloned();
            if self.conflict_next_cas.swap(false, Ordering::AcqRel)
                || current.as_ref().map(|value| value.revision) != expected_revision
            {
                return Ok(CompareExchangeResult::Conflict(current));
            }
            let value = DurableValue {
                revision: expected_revision.map_or(1, |revision| revision + 1),
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            Ok(CompareExchangeResult::Applied(value))
        })
    }
}

fn durable() -> (DurableContext, Arc<MemoryDurableStorage>) {
    let storage = Arc::new(MemoryDurableStorage::default());
    (
        DurableContext {
            delivery_id: Arc::from("mysql-phase-tests"),
            storage: storage.clone(),
            resource_storage: storage.clone(),
        },
        storage,
    )
}

fn source() -> MySqlSourceIdentity {
    MySqlSourceIdentity {
        server_uuid: "24bc7856-9a41-11ee-b9d1-0242ac120002".to_owned(),
        database: "inventory".to_owned(),
    }
}

fn tables() -> Vec<TableConfig> {
    vec![
        TableConfig {
            name: "accounts".to_owned(),
        },
        TableConfig {
            name: "events".to_owned(),
        },
    ]
}

fn boundary() -> MySqlBinlogBoundary {
    MySqlBinlogBoundary {
        filename: "mysql-bin.000042".to_owned(),
        position: 9_271,
        gtid_executed: "24bc7856-9a41-11ee-b9d1-0242ac120002:1-71".to_owned(),
        source_timestamp_micros: 1_731_234_567_890_123,
    }
}

fn authoritative_tables() -> Vec<AuthoritativeTableIdentity> {
    vec![
        AuthoritativeTableIdentity {
            database: "inventory".to_owned(),
            table: "accounts".to_owned(),
            engine: "InnoDB".to_owned(),
            columns: vec![authoritative_column("id", "bigint", false, Some(1))],
        },
        AuthoritativeTableIdentity {
            database: "inventory".to_owned(),
            table: "events".to_owned(),
            engine: "InnoDB".to_owned(),
            columns: vec![
                authoritative_column("id", "bigint", false, Some(1)),
                authoritative_column("body", "json", true, None),
            ],
        },
    ]
}

fn authoritative_column(
    name: &str,
    column_type: &str,
    nullable: bool,
    primary_key_ordinal: Option<u64>,
) -> AuthoritativeColumnIdentity {
    AuthoritativeColumnIdentity {
        name: name.to_owned(),
        column_type: column_type.to_owned(),
        nullable,
        character_set: None,
        collation: None,
        collation_id: None,
        extra: String::new(),
        generation_expression: Some(String::new()),
        primary_key_ordinal,
        primary_key_prefix_length: None,
        primary_key_direction: primary_key_ordinal.map(|_| "A".to_owned()),
    }
}

#[tokio::test]
async fn exact_boundary_and_schema_become_the_only_resumable_stream_start() {
    let (durable, storage) = durable();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        91_001,
        &tables(),
        &source(),
        durable.clone(),
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("fresh state must claim snapshot creation");
    };
    tracker
        .mark_snapshot_ready(&boundary(), &authoritative_tables())
        .await
        .unwrap();
    assert_eq!(tracker.streaming_boundary(), None);
    assert_eq!(tracker.mark_streaming().await.unwrap(), boundary());
    assert_eq!(tracker.streaming_boundary(), Some(&boundary()));
    drop(tracker);

    let SnapshotStreamPreparation::Streaming {
        tracker,
        start_boundary,
    } = SnapshotStreamTracker::claim_or_resume(
        91_001,
        &tables(),
        &source(),
        durable,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("streaming state must resume at its persisted boundary");
    };
    assert_eq!(start_boundary, boundary());
    tracker
        .validate_authoritative_tables(&authoritative_tables())
        .unwrap();

    let payload = storage
        .read(SNAPSHOT_STREAM_STATE_KEY)
        .await
        .unwrap()
        .unwrap()
        .payload;
    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(json["replay_identity"], REPLAY_IDENTITY);
    assert_eq!(json["server_id"], 91_001);
    assert_eq!(json["source"]["server_uuid"], source().server_uuid);
    assert_eq!(json["state"]["phase"], "streaming");
    assert_eq!(json["state"]["start_boundary"]["position"], 9_271);
    assert_eq!(
        json["state"]["start_boundary"]["source_timestamp_micros"],
        1_731_234_567_890_123_i64
    );
    assert_eq!(
        json["authoritative_tables"][0]["columns"][0]["column_type"],
        authoritative_tables()[0].columns[0].column_type
    );
}

#[tokio::test]
async fn claimed_state_is_recyclable_only_before_snapshot_side_effects() {
    let (durable, _) = durable();
    let SnapshotStreamPreparation::Create(tracker) = SnapshotStreamTracker::claim_or_resume(
        7,
        &tables(),
        &source(),
        durable.clone(),
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("fresh state must be claimed");
    };
    drop(tracker);

    let SnapshotStreamPreparation::Create(mut recycled) = SnapshotStreamTracker::claim_or_resume(
        7,
        &tables(),
        &source(),
        durable.clone(),
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("pre-side-effect claim must recycle");
    };
    recycled
        .mark_snapshot_ready(&boundary(), &authoritative_tables())
        .await
        .unwrap();
    drop(recycled);

    let error = SnapshotStreamTracker::claim_or_resume(
        7,
        &tables(),
        &source(),
        durable,
        REPLAY_IDENTITY,
    )
    .await
    .err()
    .expect("connection-owned snapshot must not recycle after process loss");
    assert!(is_replication_safety_violation(&error));
    assert!(error.to_string().contains("cannot survive process loss"));
}

#[tokio::test]
async fn claim_requires_exact_replay_and_source_identity_before_writing_state() {
    for (server_id, replay_identity, source) in [
        (0, REPLAY_IDENTITY, source()),
        (7, "", source()),
        (
            7,
            REPLAY_IDENTITY,
            MySqlSourceIdentity {
                server_uuid: String::new(),
                database: "inventory".to_owned(),
            },
        ),
    ] {
        let (durable, storage) = durable();
        let error = SnapshotStreamTracker::claim_or_resume(
            server_id,
            &tables(),
            &source,
            durable,
            replay_identity,
        )
        .await
        .err()
        .expect("incomplete replay identity must fail");
        assert!(is_replication_safety_violation(&error));
        assert!(storage
            .read(SNAPSHOT_STREAM_STATE_KEY)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn persisted_state_rejects_replay_server_source_and_table_drift() {
    let (durable, _) = durable();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        7,
        &tables(),
        &source(),
        durable.clone(),
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("fresh state must be claimed");
    };
    tracker
        .mark_snapshot_ready(&boundary(), &authoritative_tables())
        .await
        .unwrap();
    tracker.mark_streaming().await.unwrap();
    drop(tracker);

    let changed_source = MySqlSourceIdentity {
        server_uuid: "c74d57d5-3ead-4ab1-9921-eaee6f7032e5".to_owned(),
        database: "inventory".to_owned(),
    };
    let changed_tables = vec![TableConfig {
        name: "accounts".to_owned(),
    }];
    for result in [
        SnapshotStreamTracker::claim_or_resume(
            8,
            &tables(),
            &source(),
            durable.clone(),
            REPLAY_IDENTITY,
        )
        .await,
        SnapshotStreamTracker::claim_or_resume(
            7,
            &tables(),
            &changed_source,
            durable.clone(),
            REPLAY_IDENTITY,
        )
        .await,
        SnapshotStreamTracker::claim_or_resume(
            7,
            &changed_tables,
            &source(),
            durable.clone(),
            REPLAY_IDENTITY,
        )
        .await,
        SnapshotStreamTracker::claim_or_resume(
            7,
            &tables(),
            &source(),
            durable.clone(),
            "delivery-revision-18",
        )
        .await,
    ] {
        let error = result.err().expect("durable identity drift must fail");
        assert!(is_replication_safety_violation(&error));
        assert!(error.to_string().contains("different replay-affecting"));
    }
}

#[tokio::test]
async fn boundary_and_authoritative_schema_are_validated_before_snapshot_cas() {
    let (durable, storage) = durable();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        7,
        &tables(),
        &source(),
        durable,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("fresh state must be claimed");
    };
    let mut bad_boundary = boundary();
    bad_boundary.position = 3;
    let error = tracker
        .mark_snapshot_ready(&bad_boundary, &authoritative_tables())
        .await
        .unwrap_err();
    assert!(is_replication_safety_violation(&error));

    let mut bad_schema = authoritative_tables();
    bad_schema[0].columns.clear();
    let error = tracker
        .mark_snapshot_ready(&boundary(), &bad_schema)
        .await
        .unwrap_err();
    assert!(is_replication_safety_violation(&error));

    let payload = storage
        .read(SNAPSHOT_STREAM_STATE_KEY)
        .await
        .unwrap()
        .unwrap()
        .payload;
    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(json["state"]["phase"], "claimed");
    assert!(json["authoritative_tables"].is_null());
}

#[tokio::test]
async fn every_phase_completion_requires_its_expected_revision() {
    let (durable, storage) = durable();
    let SnapshotStreamPreparation::Create(mut tracker) = SnapshotStreamTracker::claim_or_resume(
        7,
        &tables(),
        &source(),
        durable,
        REPLAY_IDENTITY,
    )
    .await
    .unwrap()
    else {
        panic!("fresh state must be claimed");
    };
    storage.conflict_next_cas.store(true, Ordering::Release);
    let error = tracker
        .mark_snapshot_ready(&boundary(), &authoritative_tables())
        .await
        .unwrap_err();
    assert!(is_replication_safety_violation(&error));
    assert!(error.to_string().contains("another execution"));
    assert_eq!(tracker.streaming_boundary(), None);
}

#[test]
fn authoritative_identity_rejects_schema_drift_without_hashes_or_aliases() {
    let expected = persisted_state(7, &tables(), &source(), REPLAY_IDENTITY);
    let mut drift = authoritative_tables();
    drift[1].columns[1].column_type = "longtext".to_owned();
    validate_authoritative_identity(&expected, &authoritative_tables()).unwrap();

    let mut persisted = expected;
    persisted.authoritative_tables = Some(authoritative_tables());
    persisted.state = PersistedPhase::Streaming {
        start_boundary: boundary(),
    };
    let tracker = SnapshotStreamTracker {
        storage: durable().0.storage,
        revision: 1,
        identity: persisted,
    };
    let error = tracker
        .validate_authoritative_tables(&drift)
        .unwrap_err();
    assert!(is_replication_safety_violation(&error));
}

#[test]
fn volatile_auto_increment_counter_is_not_persisted_as_schema_identity() {
    let before_show_create = "CREATE TABLE `accounts` (`id` bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=2";
    let after_show_create = "CREATE TABLE `accounts` (`id` bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=900";
    assert_ne!(before_show_create, after_show_create);

    let mut identity = authoritative_tables()[0].clone();
    identity.columns[0].extra = "auto_increment".to_owned();
    let persisted = serde_json::to_string(&identity).unwrap();
    assert!(persisted.contains("\"extra\":\"auto_increment\""));
    assert!(!persisted.contains("AUTO_INCREMENT=2"));
    assert!(!persisted.contains("AUTO_INCREMENT=900"));
}

#[test]
fn nullable_generation_expression_is_preserved_without_normalization() {
    let mut maria_db = authoritative_tables()[0].columns[0].clone();
    maria_db.generation_expression = None;
    validate_authoritative_column(&maria_db).unwrap();
    let maria_db_json = serde_json::to_value(&maria_db).unwrap();
    assert!(maria_db_json["generation_expression"].is_null());

    let mut mysql = maria_db.clone();
    mysql.generation_expression = Some(String::new());
    validate_authoritative_column(&mysql).unwrap();
    let mysql_json = serde_json::to_value(&mysql).unwrap();
    assert_eq!(mysql_json["generation_expression"], "");
    assert_ne!(maria_db, mysql);
}
