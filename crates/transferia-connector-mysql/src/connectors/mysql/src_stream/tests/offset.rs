use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use transferia_registry::durable::{
    CompareExchangeResult, DurableContext, DurableStorage, DurableValue,
};

use super::super::offset::{inspect_existing_replication_offset, MySqlReplicationOffsetTracker};
use super::super::MySqlReplicationConfig;
use crate::connectors::mysql::src_batch_and_stream::{
    AuthoritativeColumnIdentity, AuthoritativeTableIdentity, MySqlBinlogBoundary,
    MySqlColumnGeneration, MySqlColumnVisibility, MySqlSourceIdentity,
};

#[derive(Default)]
struct MemoryDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
    compare_exchanges: AtomicUsize,
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
            self.compare_exchanges.fetch_add(1, Ordering::AcqRel);
            let mut values = self
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = values.get(key).cloned();
            if current.as_ref().map(|value| value.revision) != expected_revision {
                return Ok(CompareExchangeResult::Conflict(current));
            }
            let value = DurableValue {
                revision: expected_revision.map_or(1, |revision| revision + 1),
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            drop(values);
            Ok(CompareExchangeResult::Applied(value))
        })
    }
}

#[tokio::test]
async fn offset_inspection_never_initializes_or_updates_durable_state() {
    let (durable, storage) = durable();
    let absent = inspect_existing_replication_offset(
        &config(),
        &source(),
        &tables(),
        &durable,
        None,
        &super::super::GtidSet::default(),
        &super::super::GtidSet::default(),
        "revision-7",
    )
    .await
    .unwrap();
    assert_eq!(absent, None);
    assert_eq!(storage.compare_exchanges.load(Ordering::Acquire), 0);

    let (_, expected, expected_gtids) = MySqlReplicationOffsetTracker::prepare(
        &config(),
        &source(),
        &tables(),
        durable.clone(),
        Some(&boundary()),
        &gtids("24bc7856-9a41-11ee-b9d1-0242ac120002:1"),
        &super::super::GtidSet::default(),
        Arc::from("revision-7"),
    )
    .await
    .unwrap();
    assert_eq!(storage.compare_exchanges.load(Ordering::Acquire), 1);

    let inspected = inspect_existing_replication_offset(
        &config(),
        &source(),
        &tables(),
        &durable,
        Some(&boundary()),
        &expected_gtids,
        &super::super::GtidSet::default(),
        "revision-7",
    )
    .await
    .unwrap();
    assert_eq!(inspected, Some(expected));
    assert_eq!(storage.compare_exchanges.load(Ordering::Acquire), 1);

    assert!(inspect_existing_replication_offset(
        &config(),
        &source(),
        &tables(),
        &durable,
        Some(&boundary()),
        &super::super::GtidSet::default(),
        &super::super::GtidSet::default(),
        "revision-7",
    )
    .await
    .is_err());
    assert!(inspect_existing_replication_offset(
        &config(),
        &source(),
        &tables(),
        &durable,
        Some(&boundary()),
        &gtids("24bc7856-9a41-11ee-b9d1-0242ac120002:1-2"),
        &gtids("24bc7856-9a41-11ee-b9d1-0242ac120002:2"),
        "revision-7",
    )
    .await
    .is_err());
    assert_eq!(storage.compare_exchanges.load(Ordering::Acquire), 1);
}

fn durable() -> (DurableContext, Arc<MemoryDurableStorage>) {
    let storage = Arc::new(MemoryDurableStorage::default());
    (
        DurableContext {
            delivery_id: Arc::from("mysql-offset-tests"),
            storage: storage.clone(),
            resource_storage: storage.clone(),
        },
        storage,
    )
}

#[tokio::test]
async fn table_membership_and_create_position_are_committed_atomically() {
    use super::super::offset::inspect_replication_membership;
    let (durable, storage) = durable();
    let initial_gtids = gtids("24bc7856-9a41-11ee-b9d1-0242ac120002:1");
    let (mut tracker, _, _) = MySqlReplicationOffsetTracker::prepare(
        &config(),
        &source(),
        &tables(),
        durable.clone(),
        Some(&boundary()),
        &initial_gtids,
        &super::super::GtidSet::default(),
        Arc::from("revision-7"),
    )
    .await
    .unwrap();
    let mut new_table = tables().remove(0);
    new_table.database = "another_database".into();
    new_table.table = "new_table".into();
    let next = super::super::MySqlBinlogPosition::new(b"mysql-bin.000007".to_vec(), 8192).unwrap();
    let next_gtids = gtids("24bc7856-9a41-11ee-b9d1-0242ac120002:1-2");
    let before = storage.compare_exchanges.load(Ordering::Acquire);
    tracker
        .store_admission(&next, &next_gtids, &[new_table.clone()])
        .await
        .unwrap();
    assert_eq!(
        storage.compare_exchanges.load(Ordering::Acquire),
        before + 1
    );
    let mut expected_tables = tables();
    expected_tables.push(new_table);
    assert_eq!(
        inspect_replication_membership(&config(), &source(), &durable, "revision-7")
            .await
            .unwrap(),
        Some(expected_tables.clone())
    );
    assert_eq!(
        inspect_existing_replication_offset(
            &config(),
            &source(),
            &expected_tables,
            &durable,
            None,
            &next_gtids,
            &super::super::GtidSet::default(),
            "revision-7"
        )
        .await
        .unwrap(),
        Some(next)
    );
    assert!(
        inspect_replication_membership(&config(), &source(), &durable, "another-revision")
            .await
            .is_err()
    );
}

fn config() -> MySqlReplicationConfig {
    MySqlReplicationConfig {
        server_id: 91,
        max_events: 100,
        max_transaction_bytes: 1 << 20,
        poll_interval_ms: 10,
        bootstrap_timeout_ms: 1_000,
    }
}

fn source() -> MySqlSourceIdentity {
    MySqlSourceIdentity {
        server_uuid: "24bc7856-9a41-11ee-b9d1-0242ac120002".to_owned(),
        database: "inventory".to_owned(),
    }
}

fn boundary() -> MySqlBinlogBoundary {
    MySqlBinlogBoundary {
        filename: "mysql-bin.000007".to_owned(),
        position: 4_096,
        gtid_executed: "24bc7856-9a41-11ee-b9d1-0242ac120002:1".to_owned(),
        source_timestamp_micros: 1_731_234_567_890_123,
    }
}

fn gtids(value: &str) -> super::super::GtidSet {
    super::super::GtidSet::parse_mysql(value).unwrap()
}

fn tables() -> Vec<AuthoritativeTableIdentity> {
    vec![AuthoritativeTableIdentity {
        database: "inventory".to_owned(),
        table: "items".to_owned(),
        engine: "InnoDB".to_owned(),
        columns: vec![AuthoritativeColumnIdentity {
            name: "id".to_owned(),
            data_type: "bigint".to_owned(),
            column_type: "bigint".to_owned(),
            unsigned: false,
            zerofill: false,
            auto_increment: false,
            nullable: false,
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: Some(19),
            numeric_scale: Some(0),
            datetime_precision: None,
            character_set: None,
            collation: None,
            collation_id: None,
            collation_padding: None,
            enum_set_values: None,
            srs_id: None,
            visibility: MySqlColumnVisibility::Visible,
            generation: MySqlColumnGeneration::None,
            extra: String::new(),
            generation_expression: Some(String::new()),
            primary_key_ordinal: Some(1),
            primary_key_prefix_length: None,
            primary_key_direction: Some("A".to_owned()),
        }],
    }]
}
