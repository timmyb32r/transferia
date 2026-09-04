#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use arrow::array::{Array as _, BinaryArray, Int32Array, Int64Array, StringArray, UInt64Array};
use mysql_async::prelude::Queryable as _;
use serde_json::{json, Value};
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use transferia_connector_mysql::metrics::MetricsRegistry;
use transferia_connector_mysql::mysql::{connect, MySqlConnectionConfig, MySqlSourceConnector};
use transferia_connector_support::serializer::{
    DeliverySerializer, QueueMessageMode, SerializedDelivery, SerializedMessage, SerializerConfig,
};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    META_SYSTEM_ROLE, SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_BINLOG_FILE,
    SYSTEM_ROLE_SOURCE_BINLOG_POSITION, SYSTEM_ROLE_SOURCE_BINLOG_ROW,
    SYSTEM_ROLE_SOURCE_GTID, SYSTEM_ROLE_SOURCE_SERVER_ID, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    validate_batch_against_discovery, DeliveryDiscovery, DeliveryDiscoveryRequest, NO_LIMITS,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch};
use transferia_core::source::{CommitMarker, Source};
use transferia_core::{project_sink_batch, ProjectedSinkBatch};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::durable::{
    CompareExchangeResult, DurableContext, DurableLease, DurableStorage, DurableValue,
};
use transferia_registry::{
    SourceBuildContext, SourceConnector as _, SourceDiscoveryContext, SourceExecutionContext,
    SourcePhase,
};

const MYSQL_IMAGE: &str = "mysql";
const MYSQL_TAG: &str = "8.4.6";
const MYSQL_PORT: u16 = 3_306;
const ROOT_PASSWORD: &str = "test";
const DATABASE: &str = "transferia";
const SOURCE_USER: &str = "transferia_source";
const SOURCE_PASSWORD: &str = "source-test";
const REPLAY_IDENTITY: &str = "mysql-debezium-e2e-revision-1";
const REPLICA_SERVER_ID: u32 = 454_545;
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

struct MySqlFixture {
    _container: ContainerAsync<GenericImage>,
    admin: MySqlConnectionConfig,
    source: MySqlConnectionConfig,
}

impl MySqlFixture {
    async fn start() -> anyhow::Result<Self> {
        let container = GenericImage::new(MYSQL_IMAGE, MYSQL_TAG)
            .with_exposed_port(MYSQL_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
            .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .with_env_var("MYSQL_DATABASE", DATABASE)
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
        let host = reachable_host(&container.get_host().await?);
        let port = container.get_host_port_ipv4(MYSQL_PORT.tcp()).await?;
        let admin = connection_config(&host, port, "root", ROOT_PASSWORD);
        wait_for_mysql(&admin).await?.disconnect().await?;

        let mut connection = connect(&admin).await?;
        connection
            .query_drop(format!(
                "CREATE USER '{SOURCE_USER}'@'%' IDENTIFIED BY '{SOURCE_PASSWORD}'"
            ))
            .await?;
        connection
            .query_drop(format!(
                "GRANT SELECT, LOCK TABLES ON `{DATABASE}`.* TO '{SOURCE_USER}'@'%'"
            ))
            .await?;
        connection
            .query_drop(format!(
                "GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{SOURCE_USER}'@'%'"
            ))
            .await?;
        connection.disconnect().await?;

        Ok(Self {
            _container: container,
            source: connection_config(&host, port, SOURCE_USER, SOURCE_PASSWORD),
            admin,
        })
    }
}

#[derive(Default)]
struct TestDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
    leases: Arc<Mutex<HashSet<String>>>,
}

impl TestDurableStorage {
    fn snapshot(&self) -> BTreeMap<String, DurableValue> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl DurableStorage for TestDurableStorage {
    fn read<'a>(
        &'a self,
        key: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Option<DurableValue>>> {
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
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<CompareExchangeResult>> {
        Box::pin(async move {
            let mut values = self
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = values.get(key).cloned();
            if current.as_ref().map(|value| value.revision) != expected_revision {
                return Ok(CompareExchangeResult::Conflict(current));
            }
            let revision = expected_revision.map_or(Ok(0), |revision| {
                revision
                    .checked_add(1)
                    .context("durable revision overflow")
            })?;
            let value = DurableValue {
                revision,
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            Ok(CompareExchangeResult::Applied(value))
        })
    }

    fn acquire_execution_lease<'a>(
        &'a self,
        key: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<DurableLease>> {
        Box::pin(async move {
            let mut leases = self
                .leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            anyhow::ensure!(
                leases.insert(key.to_owned()),
                "another execution already owns durable lease '{key}'"
            );
            drop(leases);
            Ok(DurableLease::new(TestDurableLease {
                key: key.to_owned(),
                leases: Arc::clone(&self.leases),
            }))
        })
    }
}

struct TestDurableLease {
    key: String,
    leases: Arc<Mutex<HashSet<String>>>,
}

impl Drop for TestDurableLease {
    fn drop(&mut self) {
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

struct TestDurable {
    context: DurableContext,
    local: Arc<TestDurableStorage>,
}

impl TestDurable {
    fn new() -> Self {
        let local = Arc::new(TestDurableStorage::default());
        let resources = Arc::new(TestDurableStorage::default());
        Self {
            context: DurableContext {
                delivery_id: Arc::from("mysql-debezium-e2e"),
                storage: local.clone(),
                resource_storage: resources,
            },
            local,
        }
    }
}

struct TypedSourceBatch {
    tables: Vec<TableData>,
    source_rows: u64,
    marker: CommitMarker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PhysicalRow {
    server_id: i64,
    gtid: Option<String>,
    file: String,
    position: i64,
    row: i32,
    topic: String,
    partition: i64,
    offset: i64,
    message_index: u64,
    source_timestamp_ms: i64,
    source_timestamp_us: i64,
    source_timestamp_ns: i64,
    event_timestamp_ms: i64,
    event_timestamp_us: i64,
    event_timestamp_ns: i64,
}

#[tokio::test]
async fn locked_snapshot_and_cdc_serialize_as_lossless_mysql_debezium_with_ack_replay(
) -> anyhow::Result<()> {
    let fixture = MySqlFixture::start().await?;
    let mut admin = connect(&fixture.admin).await?;
    exec_all(
        &mut admin,
        &[
            "SET SESSION sql_mode = ''",
            "SET SESSION time_zone = '+00:00'",
            "CREATE TABLE debezium_events (\
                id BIGINT NOT NULL, \
                payload VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL, \
                u64_value BIGINT UNSIGNED NOT NULL, \
                decimal_value DECIMAL(65,30) NOT NULL, \
                binary_value VARBINARY(8) NOT NULL, \
                latin1_value VARCHAR(8) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL, \
                json_value JSON NOT NULL, \
                date_value DATE NOT NULL, \
                datetime_value DATETIME(6) NOT NULL, \
                timestamp_value TIMESTAMP(6) NOT NULL, \
                time_value TIME(6) NOT NULL, \
                PRIMARY KEY (id)\
            ) ENGINE=InnoDB",
            "INSERT INTO debezium_events VALUES (\
                1, 'snapshot', 18446744073709551615, \
                12345678901234567890123456789012345.123456789012345678901234567890, \
                0x00FF41, _latin1 0x80FF, \
                JSON_OBJECT('array', JSON_ARRAY(1, NULL), 'scalar', 'text'), \
                '2024-02-29', '2024-02-29 23:59:58.123456', \
                '2038-01-19 03:14:07.499999', '-123:45:56.123456'\
            )",
        ],
    )
    .await?;

    let config = source_yaml(&fixture.source);
    let connector = mysql_connector(&config)?;
    let durable = TestDurable::new();
    let cancellation = CancellationToken::new();
    let preview = connector
        .delivery_discovery(discovery_context(&cancellation))
        .await?;
    let phases = connector.execution_phases(DeliveryType::BatchAndStream, &preview)?;
    assert_eq!(
        phases.iter().map(|phase| phase.phase).collect::<Vec<_>>(),
        vec![SourcePhase::Snapshot, SourcePhase::Stream]
    );
    let prepared = tokio::time::timeout(
        TEST_TIMEOUT,
        connector.prepare_execution(execution_context(&durable.context, &cancellation)),
    )
    .await
    .context("timed out establishing the locked MySQL snapshot boundary")??
    .context("batch_and_stream did not return authoritative discovery")?;
    assert_eq!(prepared.remaining_phases, phases);

    let serializer_config: SerializerConfig = serde_json::from_value(json!({
        "type": "debezium",
        "logical_name": "inventory",
        "format": { "type": "json" }
    }))?;
    serializer_config.validate_discovery(&prepared.discovery)?;
    let mut serializer = DeliverySerializer::new(
        &serializer_config,
        QueueMessageMode::KeyedWithTombstones,
    )?;

    exec_all(
        &mut admin,
        &[
            "START TRANSACTION",
            "INSERT INTO debezium_events SELECT \
                2, 'created', u64_value, decimal_value, binary_value, latin1_value, \
                json_value, date_value, datetime_value, timestamp_value, time_value \
             FROM debezium_events WHERE id = 1",
            "UPDATE debezium_events SET payload = 'updated' WHERE id = 1",
            "UPDATE debezium_events SET id = 3, payload = 'renamed' WHERE id = 2",
            "DELETE FROM debezium_events WHERE id = 1",
            "COMMIT",
        ],
    )
    .await?;

    let partitions = prepared
        .discovery
        .source_topology
        .static_partitions()
        .context("MySQL snapshot unexpectedly used dynamic partitions")?;
    anyhow::ensure!(
        partitions.len() == 1,
        "single-table fixture exposed {} snapshot partitions",
        partitions.len()
    );
    let mut snapshot_source = connector
        .build_source(build_context(
            SourcePhase::Snapshot,
            partitions[0],
            &durable.context,
            &cancellation,
        ))
        .await?;
    let snapshot = read_nonempty(&mut snapshot_source).await?;
    anyhow::ensure!(snapshot.source_rows == 1, "snapshot did not contain exactly one row");
    let snapshot_serialized = serialize_tables(
        &mut serializer,
        &prepared.discovery,
        &snapshot.tables,
        1,
    )
    .await?;
    assert_snapshot_delivery(&snapshot.tables, &snapshot_serialized)?;
    snapshot_source.commit_offsets(&[snapshot.marker]).await?;
    anyhow::ensure!(
        matches!(snapshot_source.read_batch().await?, SourceBatch::Finished),
        "single-row snapshot emitted more than one batch"
    );
    snapshot_source.shutdown().await?;
    connector
        .complete_execution_phase(
            SourcePhase::Snapshot,
            durable.context.clone(),
            cancellation.child_token(),
        )
        .await?;

    let mut stream = connector
        .build_source(build_context(
            SourcePhase::Stream,
            0,
            &durable.context,
            &cancellation,
        ))
        .await?;
    let durable_before_read = durable.local.snapshot();
    let first = read_nonempty(&mut stream).await?;
    anyhow::ensure!(
        first.source_rows == 4,
        "post-boundary transaction emitted {} rows instead of four",
        first.source_rows
    );
    let first_serialized =
        serialize_tables(&mut serializer, &prepared.discovery, &first.tables, 2).await?;
    assert_cdc_delivery(&first.tables, &first_serialized)?;
    anyhow::ensure!(
        durable.local.snapshot() == durable_before_read,
        "serialization advanced the source cursor before sink acknowledgement"
    );
    let first_tables = first.tables.clone();
    let first_semantic = semantic_delivery(&first_serialized)?;
    stream.shutdown().await?;
    drop(stream);

    let mut replay_source = connector
        .build_source(build_context(
            SourcePhase::Stream,
            0,
            &durable.context,
            &cancellation,
        ))
        .await?;
    let replay = read_nonempty(&mut replay_source).await?;
    assert_same_replayed_tables(&first_tables, &replay.tables);
    let replay_serialized =
        serialize_tables(&mut serializer, &prepared.discovery, &replay.tables, 3).await?;
    anyhow::ensure!(
        semantic_delivery(&replay_serialized)? == first_semantic,
        "restart changed the semantic Debezium replay"
    );
    anyhow::ensure!(
        durable.local.snapshot() == durable_before_read,
        "replay advanced the source cursor before sink acknowledgement"
    );
    replay_source.commit_offsets(&[replay.marker]).await?;
    let durable_after_ack = durable.local.snapshot();
    anyhow::ensure!(
        durable_after_ack != durable_before_read,
        "source commit did not persist the acknowledged cursor"
    );
    replay_source.shutdown().await?;
    drop(replay_source);
    drop(connector);

    let resumed = mysql_connector(&config)?;
    resumed
        .delivery_discovery(discovery_context(&cancellation))
        .await?;
    let resumed_prepared = resumed
        .prepare_execution(execution_context(&durable.context, &cancellation))
        .await?
        .context("completed snapshot did not resume in stream phase")?;
    anyhow::ensure!(
        resumed_prepared.remaining_phases.len() == 1
            && resumed_prepared.remaining_phases[0].phase == SourcePhase::Stream,
        "restart attempted to repeat the completed snapshot phase"
    );
    serializer_config.validate_discovery(&resumed_prepared.discovery)?;
    let mut resumed_source = resumed
        .build_source(build_context(
            SourcePhase::Stream,
            0,
            &durable.context,
            &cancellation,
        ))
        .await?;
    admin
        .query_drop(
            "INSERT INTO debezium_events SELECT \
                4, 'after-ack', u64_value, decimal_value, binary_value, latin1_value, \
                json_value, date_value, datetime_value, timestamp_value, time_value \
             FROM debezium_events WHERE id = 3",
        )
        .await?;
    let after_ack = read_nonempty(&mut resumed_source).await?;
    anyhow::ensure!(
        after_ack.source_rows == 1,
        "restart replayed acknowledged rows before the new transaction"
    );
    let after_ack_serialized = serialize_tables(
        &mut serializer,
        &resumed_prepared.discovery,
        &after_ack.tables,
        4,
    )
    .await?;
    let messages = only_messages(&after_ack_serialized)?;
    anyhow::ensure!(messages.len() == 1, "new create emitted tombstones or duplicates");
    assert_key_id(messages[0].key.as_deref(), 4)?;
    let value = message_value(messages[0].value.as_deref())?;
    anyhow::ensure!(value["op"] == "c", "post-ACK transaction was not a create");
    anyhow::ensure!(value["after"]["payload"] == "after-ack");
    resumed_source.commit_offsets(&[after_ack.marker]).await?;
    let durable_before_invalid = durable.local.snapshot();
    admin
        .query_drop(
            "INSERT INTO debezium_events SELECT \
                5, 'invalid-temporal', u64_value, decimal_value, binary_value, latin1_value, \
                json_value, \
                '2024-00-15', '2024-00-15 12:34:56.000001', \
                '0000-00-00 00:00:00.000000', time_value \
             FROM debezium_events WHERE id = 3",
        )
        .await?;
    let invalid = read_nonempty(&mut resumed_source).await?;
    let error = serialize_tables(
        &mut serializer,
        &resumed_prepared.discovery,
        &invalid.tables,
        5,
    )
    .await
    .expect_err("zero/partial MySQL temporal value was serialized lossily");
    let diagnostic = format!("{error:#}").to_lowercase();
    anyhow::ensure!(
        diagnostic.contains("temporal")
            || diagnostic.contains("date")
            || diagnostic.contains("timestamp"),
        "unexpected zero/partial temporal diagnostic: {error:#}"
    );
    anyhow::ensure!(
        durable.local.snapshot() == durable_before_invalid,
        "invalid temporal row advanced the source cursor"
    );
    resumed_source.shutdown().await?;
    admin.disconnect().await?;
    Ok(())
}

async fn serialize_tables(
    serializer: &mut DeliverySerializer,
    discovery: &DeliveryDiscovery,
    tables: &[TableData],
    delivery_id: u64,
) -> anyhow::Result<SerializedDelivery> {
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let mut outputs = Vec::with_capacity(tables.len());
    for table in tables {
        let byte_size = table.batch.get_array_memory_size();
        let batch = SinkBatch {
            table: Arc::clone(&table.table),
            is_dlq: table.is_dlq,
            batch: table.batch.clone(),
            byte_size,
            memory: memory.reserve_transform(byte_size),
            system_columns: table.system_columns.clone(),
        };
        validate_batch_against_discovery(discovery, &batch)?;
        let ProjectedSinkBatch::Changelog(projected) = project_sink_batch(discovery, &batch)? else {
            anyhow::bail!("MySQL batch_and_stream emitted append-only data")
        };
        anyhow::ensure!(
            projected.rows().num_rows() == batch.rows(),
            "normal changelog projection changed the source row count"
        );
        outputs.push(batch);
    }
    let source_messages = tables
        .iter()
        .map(|table| u64::try_from(table.batch.num_rows()))
        .try_fold(0_u64, |sum, rows| {
            sum.checked_add(rows?)
                .context("source message count overflow")
        })?;
    serializer
        .serialize(
            &Delivery {
                id: DeliveryId::new(delivery_id),
                outputs,
                meta: DeliveryMeta { source_messages },
            },
            discovery,
            &NO_LIMITS,
            1024 * 1024,
        )
        .await
}

fn assert_snapshot_delivery(
    tables: &[TableData],
    serialized: &SerializedDelivery,
) -> anyhow::Result<()> {
    anyhow::ensure!(serialized.source_rows == 1);
    let table = only_table(tables)?;
    anyhow::ensure!(table.batch.num_rows() == 1, "snapshot batch was not one row");
    let physical = physical_rows(table)?;
    let transaction_ids = role_array::<BinaryArray>(table, SYSTEM_ROLE_SOURCE_TRANSACTION_ID)?;
    anyhow::ensure!(physical.len() == 1);
    anyhow::ensure!(!transaction_ids.is_null(0) && !transaction_ids.value(0).is_empty());
    anyhow::ensure!(physical[0].server_id == 0);
    anyhow::ensure!(physical[0].gtid.is_none());
    anyhow::ensure!(physical[0].row == 0);
    anyhow::ensure!(physical[0].file == physical[0].topic);
    anyhow::ensure!(physical[0].position == physical[0].offset);
    anyhow::ensure!(physical[0].message_index == 0);

    let messages = only_messages(serialized)?;
    anyhow::ensure!(messages.len() == 1, "snapshot did not serialize to one message");
    assert_key_id(messages[0].key.as_deref(), 1)?;
    let value = message_value(messages[0].value.as_deref())?;
    anyhow::ensure!(value["op"] == "r");
    anyhow::ensure!(value["before"].is_null());
    assert_physical_payload(&value["after"], 1, "snapshot")?;
    assert_mysql_source(&value, &physical[0], true)?;
    Ok(())
}

fn assert_cdc_delivery(
    tables: &[TableData],
    serialized: &SerializedDelivery,
) -> anyhow::Result<()> {
    anyhow::ensure!(serialized.source_rows == 4);
    let table = only_table(tables)?;
    anyhow::ensure!(table.batch.num_rows() == 4, "CDC batch was not one transaction");
    let operations = system_array::<StringArray>(table, SystemColumnKind::ChangeOperation)?;
    anyhow::ensure!(
        (0..4).map(|row| operations.value(row)).collect::<Vec<_>>()
            == vec!["c", "u", "u", "d"],
        "source transaction changed row-event ordering"
    );
    let physical = physical_rows(table)?;
    let transaction_ids = role_array::<BinaryArray>(table, SYSTEM_ROLE_SOURCE_TRANSACTION_ID)?;
    anyhow::ensure!(physical.len() == 4);
    anyhow::ensure!(
        (0..transaction_ids.len()).all(|row| {
            !transaction_ids.is_null(row)
                && !transaction_ids.value(row).is_empty()
                && transaction_ids.value(row) == transaction_ids.value(0)
        }),
        "one MySQL transaction emitted different opaque transaction identities"
    );
    let transaction_gtid = physical[0]
        .gtid
        .as_deref()
        .context("stream create omitted GTID")?;
    assert_canonical_gtid(transaction_gtid)?;
    anyhow::ensure!(
        physical
            .iter()
            .all(|row| row.server_id == 1 && row.gtid.as_deref() == Some(transaction_gtid)),
        "one MySQL transaction emitted inconsistent server or GTID identity"
    );
    anyhow::ensure!(
        physical.windows(2).all(|rows| rows[0].position < rows[1].position),
        "separate rows events did not retain their distinct physical positions"
    );
    for (index, row) in physical.iter().enumerate() {
        anyhow::ensure!(row.file == row.topic);
        anyhow::ensure!(row.partition == 0);
        anyhow::ensure!(row.offset > row.position);
        anyhow::ensure!(row.row == 0, "one-row rows event used a nonzero row ordinal");
        anyhow::ensure!(row.message_index == u64::try_from(index)?);
        anyhow::ensure!(
            row.position != row.offset || i64::from(row.row) != i64::try_from(row.message_index)?,
            "physical rows-event coordinates collapsed into durable cursor coordinates"
        );
    }
    anyhow::ensure!(
        physical.iter().map(|row| row.offset).collect::<HashSet<_>>().len() == 1,
        "one transaction received multiple durable offsets"
    );

    let messages = only_messages(serialized)?;
    anyhow::ensure!(messages.len() == 7, "c/u/PK-change/d did not emit seven records");
    assert_key_id(messages[0].key.as_deref(), 2)?;
    assert_key_id(messages[1].key.as_deref(), 1)?;
    assert_key_id(messages[2].key.as_deref(), 2)?;
    assert_key_id(messages[3].key.as_deref(), 2)?;
    assert_key_id(messages[4].key.as_deref(), 3)?;
    assert_key_id(messages[5].key.as_deref(), 1)?;
    assert_key_id(messages[6].key.as_deref(), 1)?;
    anyhow::ensure!(messages[3].value.is_none() && messages[6].value.is_none());

    let create = message_value(messages[0].value.as_deref())?;
    anyhow::ensure!(create["op"] == "c" && create["before"].is_null());
    assert_physical_payload(&create["after"], 2, "created")?;

    let update = message_value(messages[1].value.as_deref())?;
    anyhow::ensure!(update["op"] == "u");
    assert_physical_payload(&update["before"], 1, "snapshot")?;
    assert_physical_payload(&update["after"], 1, "updated")?;

    let key_delete = message_value(messages[2].value.as_deref())?;
    anyhow::ensure!(key_delete["op"] == "d" && key_delete["after"].is_null());
    assert_physical_payload(&key_delete["before"], 2, "created")?;

    let key_create = message_value(messages[4].value.as_deref())?;
    anyhow::ensure!(key_create["op"] == "c" && key_create["before"].is_null());
    assert_physical_payload(&key_create["after"], 3, "renamed")?;

    let delete = message_value(messages[5].value.as_deref())?;
    anyhow::ensure!(delete["op"] == "d" && delete["after"].is_null());
    assert_physical_payload(&delete["before"], 1, "updated")?;

    let envelopes = [create, update, key_delete, key_create, delete];
    let source_rows = [0_usize, 1, 2, 2, 3];
    for (envelope, row) in envelopes.iter().zip(source_rows) {
        assert_mysql_source(envelope, &physical[row], false)?;
        anyhow::ensure!(
            !serde_json::to_string(envelope)?.contains("__debezium_unavailable_value"),
            "full MySQL row image was replaced with an unavailable placeholder"
        );
    }
    Ok(())
}

fn assert_physical_payload(value: &Value, id: i64, payload: &str) -> anyhow::Result<()> {
    anyhow::ensure!(value["id"] == id);
    anyhow::ensure!(value["payload"] == payload);
    anyhow::ensure!(value["u64_value"] == "AP//////////");
    anyhow::ensure!(
        value["decimal_value"] == "HgK8HpeFi9xsuVBY80JNfTp/7HsD4maOPwrS"
    );
    anyhow::ensure!(value["binary_value"] == "AP9B");
    anyhow::ensure!(value["latin1_value"] == "€ÿ");
    anyhow::ensure!(value["date_value"] == 19_782);
    anyhow::ensure!(value["datetime_value"] == 1_709_251_198_123_456_i64);
    anyhow::ensure!(value["timestamp_value"] == "2038-01-19T03:14:07.499999Z");
    anyhow::ensure!(value["time_value"] == -445_556_123_456_i64);
    let json_text = value["json_value"]
        .as_str()
        .context("MySQL JSON was not serialized as its exact JSON text")?;
    let decoded: Value = serde_json::from_str(json_text)?;
    anyhow::ensure!(decoded == json!({"array": [1, null], "scalar": "text"}));
    Ok(())
}

fn assert_mysql_source(
    envelope: &Value,
    physical: &PhysicalRow,
    snapshot: bool,
) -> anyhow::Result<()> {
    let source = envelope["source"]
        .as_object()
        .context("Debezium envelope omitted source object")?;
    let expected_fields = [
        "version",
        "connector",
        "name",
        "ts_ms",
        "snapshot",
        "db",
        "sequence",
        "ts_us",
        "ts_ns",
        "table",
        "server_id",
        "gtid",
        "file",
        "pos",
        "row",
        "thread",
        "query",
    ];
    anyhow::ensure!(
        source.keys().map(String::as_str).collect::<HashSet<_>>()
            == expected_fields.into_iter().collect::<HashSet<_>>(),
        "MySQL source block fields changed: {:?}",
        source.keys().collect::<Vec<_>>()
    );
    anyhow::ensure!(source["version"] == "transferia");
    anyhow::ensure!(source["connector"] == "mysql");
    anyhow::ensure!(source["name"] == "inventory");
    anyhow::ensure!(source["db"] == DATABASE);
    anyhow::ensure!(source["table"] == "debezium_events");
    anyhow::ensure!(source["snapshot"] == if snapshot { "true" } else { "false" });
    anyhow::ensure!(source["sequence"].is_null());
    anyhow::ensure!(source["thread"].is_null() && source["query"].is_null());
    for postgres_only in ["schema", "txId", "lsn", "xmin"] {
        anyhow::ensure!(
            !source.contains_key(postgres_only),
            "MySQL source block exposed PostgreSQL-only field '{postgres_only}'"
        );
    }
    anyhow::ensure!(source["server_id"] == physical.server_id);
    match &physical.gtid {
        Some(gtid) => anyhow::ensure!(source["gtid"].as_str() == Some(gtid.as_str())),
        None => anyhow::ensure!(source["gtid"].is_null()),
    }
    anyhow::ensure!(source["file"].as_str() == Some(physical.file.as_str()));
    anyhow::ensure!(source["pos"] == physical.position);
    anyhow::ensure!(source["row"] == i64::from(physical.row));
    anyhow::ensure!(source["ts_ms"] == physical.source_timestamp_ms);
    anyhow::ensure!(source["ts_us"] == physical.source_timestamp_us);
    anyhow::ensure!(source["ts_ns"] == physical.source_timestamp_ns);
    anyhow::ensure!(
        physical.source_timestamp_ms == physical.source_timestamp_us.div_euclid(1_000)
            && physical.source_timestamp_ns
                == physical
                    .source_timestamp_us
                    .checked_mul(1_000)
                    .context("MySQL source timestamp nanoseconds overflow")?,
        "Debezium source timestamp did not retain the binlog event time"
    );
    if !snapshot {
        anyhow::ensure!(
            physical.source_timestamp_us
                == physical
                    .source_timestamp_ms
                    .checked_mul(1_000)
                    .context("MySQL stream source timestamp microseconds overflow")?,
            "MySQL stream source timestamp was not the exact binlog-header second"
        );
    }
    anyhow::ensure!(envelope["ts_ms"] == physical.event_timestamp_ms);
    anyhow::ensure!(envelope["ts_us"] == physical.event_timestamp_us);
    anyhow::ensure!(envelope["ts_ns"] == physical.event_timestamp_ns);
    anyhow::ensure!(
        physical.event_timestamp_us != physical.source_timestamp_us
            || physical.event_timestamp_ns != physical.source_timestamp_ns,
        "serializer collapsed the processing timestamp into the binlog source timestamp"
    );
    Ok(())
}

fn physical_rows(table: &TableData) -> anyhow::Result<Vec<PhysicalRow>> {
    let server_ids = role_array::<Int64Array>(table, SYSTEM_ROLE_SOURCE_SERVER_ID)?;
    let gtids = role_array::<StringArray>(table, SYSTEM_ROLE_SOURCE_GTID)?;
    let files = role_array::<StringArray>(table, SYSTEM_ROLE_SOURCE_BINLOG_FILE)?;
    let positions = role_array::<Int64Array>(table, SYSTEM_ROLE_SOURCE_BINLOG_POSITION)?;
    let rows = role_array::<Int32Array>(table, SYSTEM_ROLE_SOURCE_BINLOG_ROW)?;
    let source_ms = role_array::<Int64Array>(table, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS)?;
    let source_us = role_array::<Int64Array>(table, SYSTEM_ROLE_SOURCE_TIMESTAMP_US)?;
    let source_ns = role_array::<Int64Array>(table, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS)?;
    let event_ms = role_array::<Int64Array>(table, SYSTEM_ROLE_EVENT_TIMESTAMP_MS)?;
    let event_us = role_array::<Int64Array>(table, SYSTEM_ROLE_EVENT_TIMESTAMP_US)?;
    let event_ns = role_array::<Int64Array>(table, SYSTEM_ROLE_EVENT_TIMESTAMP_NS)?;
    let topics = system_array::<StringArray>(table, SystemColumnKind::Topic)?;
    let partitions = system_array::<Int64Array>(table, SystemColumnKind::Partition)?;
    let offsets = system_array::<Int64Array>(table, SystemColumnKind::Offset)?;
    let indexes = system_array::<UInt64Array>(table, SystemColumnKind::MessageIndex)?;
    (0..table.batch.num_rows())
        .map(|index| {
            for (name, array) in [
                ("server_id", server_ids as &dyn arrow::array::Array),
                ("file", files),
                ("position", positions),
                ("row", rows),
                ("source timestamp ms", source_ms),
                ("source timestamp us", source_us),
                ("source timestamp ns", source_ns),
                ("event timestamp ms", event_ms),
                ("event timestamp us", event_us),
                ("event timestamp ns", event_ns),
                ("topic", topics),
                ("partition", partitions),
                ("offset", offsets),
                ("message index", indexes),
            ] {
                anyhow::ensure!(!array.is_null(index), "MySQL {name} is null");
            }
            Ok(PhysicalRow {
                server_id: server_ids.value(index),
                gtid: (!gtids.is_null(index)).then(|| gtids.value(index).to_owned()),
                file: files.value(index).to_owned(),
                position: positions.value(index),
                row: rows.value(index),
                topic: topics.value(index).to_owned(),
                partition: partitions.value(index),
                offset: offsets.value(index),
                message_index: indexes.value(index),
                source_timestamp_ms: source_ms.value(index),
                source_timestamp_us: source_us.value(index),
                source_timestamp_ns: source_ns.value(index),
                event_timestamp_ms: event_ms.value(index),
                event_timestamp_us: event_us.value(index),
                event_timestamp_ns: event_ns.value(index),
            })
        })
        .collect()
}

fn assert_canonical_gtid(gtid: &str) -> anyhow::Result<()> {
    let parts = gtid.split(':').collect::<Vec<_>>();
    anyhow::ensure!(
        matches!(parts.len(), 2 | 3),
        "GTID '{gtid}' is neither canonical UUID:GNO nor UUID:TAG:GNO"
    );
    let uuid = parts[0];
    anyhow::ensure!(uuid.len() == 36 && uuid == uuid.to_ascii_lowercase());
    anyhow::ensure!(
        [8_usize, 13, 18, 23]
            .into_iter()
            .all(|index| uuid.as_bytes().get(index) == Some(&b'-')),
        "GTID SID '{uuid}' is not canonical UUID text"
    );
    anyhow::ensure!(
        uuid.bytes()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit()),
        "GTID SID '{uuid}' contains a non-hex digit"
    );
    if parts.len() == 3 {
        anyhow::ensure!(!parts[1].is_empty(), "tagged GTID has an empty tag");
    }
    anyhow::ensure!(parts.last().unwrap().parse::<u64>()? > 0);
    Ok(())
}

type SemanticDelivery = Vec<(String, Vec<(Option<Value>, Option<Value>)>)>;

fn semantic_delivery(serialized: &SerializedDelivery) -> anyhow::Result<SemanticDelivery> {
    serialized
        .batches
        .iter()
        .map(|batch| {
            let messages = batch
                .messages
                .iter()
                .map(|message| {
                    let key: Option<Value> = message
                        .key
                        .as_deref()
                        .map(serde_json::from_slice)
                        .transpose()?;
                    let value = message
                        .value
                        .as_deref()
                        .map(serde_json::from_slice::<Value>)
                        .transpose()?
                        .map(|mut value| {
                            let object = value
                                .as_object_mut()
                                .expect("Debezium value must be an object");
                            object.remove("ts_ms");
                            object.remove("ts_us");
                            object.remove("ts_ns");
                            value
                        });
                    Ok((key, value))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok((batch.table.to_string(), messages))
        })
        .collect()
}

fn assert_same_replayed_tables(expected: &[TableData], actual: &[TableData]) {
    assert_eq!(expected.len(), actual.len());
    for (expected, actual) in expected.iter().zip(actual) {
        assert_eq!(expected.table, actual.table);
        assert_eq!(expected.batch.schema(), actual.batch.schema());
        let schema = expected.batch.schema();
        for index in 0..expected.batch.num_columns() {
            let field = schema.field(index);
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
                expected.batch.column(index).to_data(),
                actual.batch.column(index).to_data(),
                "replayed MySQL field '{}' changed",
                field.name()
            );
        }
    }
}

fn only_table(tables: &[TableData]) -> anyhow::Result<&TableData> {
    anyhow::ensure!(tables.len() == 1, "expected one table batch, got {}", tables.len());
    anyhow::ensure!(tables[0].table.as_ref() == "debezium_events");
    Ok(&tables[0])
}

fn only_messages(serialized: &SerializedDelivery) -> anyhow::Result<&[SerializedMessage]> {
    anyhow::ensure!(
        serialized.batches.len() == 1,
        "expected one serialized table batch, got {}",
        serialized.batches.len()
    );
    anyhow::ensure!(serialized.batches[0].table.as_ref() == "debezium_events");
    Ok(&serialized.batches[0].messages)
}

fn assert_key_id(key: Option<&[u8]>, expected: i64) -> anyhow::Result<()> {
    let key: Value = serde_json::from_slice(key.context("keyed Debezium message omitted its key")?)?;
    anyhow::ensure!(key == json!({"id": expected}), "unexpected Debezium key {key}");
    Ok(())
}

fn message_value(value: Option<&[u8]>) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(
        value.context("Debezium data record was a tombstone")?,
    )?)
}

fn role_array<'a, T: arrow::array::Array + 'static>(
    table: &'a TableData,
    role: &str,
) -> anyhow::Result<&'a T> {
    let matches = table
        .batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.metadata().get(META_SYSTEM_ROLE).map(String::as_str) == Some(role)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    anyhow::ensure!(matches.len() == 1, "expected one '{role}' role column");
    table.batch.column(matches[0]).as_any().downcast_ref().with_context(|| {
        format!("MySQL role '{role}' column has an unexpected Arrow type")
    })
}

fn system_array<T: arrow::array::Array + 'static>(
    table: &TableData,
    kind: SystemColumnKind,
) -> anyhow::Result<&T> {
    let column = table
        .system_columns
        .get(kind)
        .with_context(|| format!("missing {kind:?} system column"))?;
    table
        .batch
        .column(column.index)
        .as_any()
        .downcast_ref()
        .with_context(|| format!("{kind:?} system column has an unexpected Arrow type"))
}

async fn read_nonempty(source: &mut Box<dyn Source>) -> anyhow::Result<TypedSourceBatch> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    source_rows,
                    commit_marker,
                    ..
                } if source_rows > 0 => {
                    return Ok(TypedSourceBatch {
                        tables,
                        source_rows,
                        marker: commit_marker
                            .context("non-empty MySQL batch omitted its commit marker")?,
                    });
                }
                SourceBatch::Typed { .. } => {}
                SourceBatch::Raw { .. } => anyhow::bail!("MySQL source emitted raw data"),
                SourceBatch::Finished => anyhow::bail!("MySQL source finished before emitting data"),
            }
        }
    })
    .await
    .context("timed out waiting for MySQL data")?
}

fn mysql_connector(config: &str) -> anyhow::Result<MySqlSourceConnector> {
    MySqlSourceConnector::from_config(
        serde_yaml::from_str(config)?,
        Arc::new(MetricsRegistry::new()),
    )
}

fn source_yaml(connection: &MySqlConnectionConfig) -> String {
    format!(
        "host: '{}'\nport: {}\ndatabase: {}\nusername: {}\npassword: {}\ntrusted_plaintext: true\nbatch_rows: 1\nread_protocol: binary\ntables:\n  - name: debezium_events\nreplication:\n  server_id: {}\n  max_events: 1024\n  poll_interval_ms: 10\n  bootstrap_timeout_ms: 10000\n",
        connection.host,
        connection.port,
        connection.database,
        connection.username,
        connection.password,
        REPLICA_SERVER_ID,
    )
}

fn discovery_context(cancellation: &CancellationToken) -> SourceDiscoveryContext {
    SourceDiscoveryContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type: DeliveryType::BatchAndStream,
    }
}

fn execution_context(
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> SourceExecutionContext {
    SourceExecutionContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type: DeliveryType::BatchAndStream,
        replay_identity: Some(Arc::from(REPLAY_IDENTITY)),
        durable: durable.clone(),
    }
}

fn build_context(
    phase: SourcePhase,
    partition_id: i64,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> SourceBuildContext {
    SourceBuildContext {
        partition_id,
        delivery_type: DeliveryType::BatchAndStream,
        phase,
        replay_identity: Some(Arc::from(REPLAY_IDENTITY)),
        cancellation: cancellation.child_token(),
        memory: PipelineMemory::new(64 * 1024 * 1024),
        durable: durable.clone(),
    }
}

fn connection_config(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> MySqlConnectionConfig {
    MySqlConnectionConfig {
        host: host.to_owned(),
        port,
        database: DATABASE.to_owned(),
        username: username.to_owned(),
        password: password.to_owned(),
        trusted_plaintext: true,
        tls_ca_file: None,
    }
}

async fn wait_for_mysql(config: &MySqlConnectionConfig) -> anyhow::Result<mysql_async::Conn> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match connect(config).await {
                Ok(connection) => return connection,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("MySQL container did not become ready"))
}

async fn exec_all(connection: &mut mysql_async::Conn, statements: &[&str]) -> anyhow::Result<()> {
    for statement in statements {
        connection.query_drop(*statement).await?;
    }
    Ok(())
}

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}
