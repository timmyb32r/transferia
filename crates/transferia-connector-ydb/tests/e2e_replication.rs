#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use arrow::array::{
    Array as _, BinaryArray, Date32Array, FixedSizeBinaryArray, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::Schema;
use prost::Message;
use testcontainers::core::{Healthcheck, IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use transferia_connector_support::serializer::{
    DeliverySerializer, QueueMessageMode, SerializerConfig,
};
use transferia_connector_ydb::metrics::MetricsRegistry;
use transferia_connector_ydb::ydb::{
    self, YdbAuth, YdbConnectionConfig, YdbSourceConfig, YdbSourceConnector,
};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    META_OLD_VALUE_OF, META_SYSTEM_ROLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    validate_batch_against_discovery, DeliveryDiscovery, DeliveryDiscoveryRequest, SourceTopology,
    NO_LIMITS,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch};
use transferia_core::source::{CommitMarker, Source};
use transferia_core::{project_sink_batch, ProjectedSinkBatch};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::durable::DurableContext;
use transferia_registry::{
    SourceBuildContext, SourceConnector as _, SourceDiscoveryContext, SourceExecutionContext,
    SourcePhase,
};
use ydb_grpc::ydb_proto::coordination::v1::coordination_service_client::CoordinationServiceClient;
use ydb_grpc::ydb_proto::coordination::{
    Config as CoordinationConfig, ConsistencyMode, CreateNodeRequest, RateLimiterCountersMode,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use ydb_grpc::ydb_proto::table::{
    self, changefeed_description, changefeed_format, changefeed_mode, AlterTableRequest,
    Changefeed, CreateSessionRequest, CreateSessionResult, DeleteSessionRequest,
    DescribeTableRequest, DescribeTableResult, ExecuteDataQueryRequest, ExecuteQueryResult,
    ExecuteSchemeQueryRequest, Query, QueryCachePolicy, SerializableModeSettings,
    TransactionControl, TransactionSettings,
};
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use ydb_grpc::ydb_proto::topic::{
    AlterTopicRequest, AutoPartitioningSettings, AutoPartitioningStrategy, Codec, Consumer,
    DescribeConsumerRequest, DescribeConsumerResult, DescribeTopicRequest, DescribeTopicResult,
    PartitioningSettings, SupportedCodecs,
};
use ydb_grpc::ydb_proto::{operations::Operation, table::query, table::transaction_control};

const YDB_IMAGE: &str = "ydbplatform/local-ydb";
const YDB_TAG: &str = "25.4.1";
const YDB_PORT: u16 = 2_136;
const EVENTS_TABLE: &str = "/local/replication_events";
const JSON_TABLE: &str = "/local/nullable_json_events";
const CHANGEFEED: &str = "transferia_cdc";
const CONSUMER: &str = "transferia_e2e";
const JSON_CONSUMER: &str = "transferia_nullable_json_e2e";
const COORDINATION_NODE: &str = "/local/transferia_replication_coordination";
const MAIN_DELIVERY_ID: &str = "integration-test";
const REPLAY_IDENTITY: &str = "ydb-replication-e2e-v1";
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const EVENT_DATE_DAYS: i32 = 19_782;
const EVENT_DATETIME_SECONDS: i64 = 1_709_210_096;
const EVENT_TIMESTAMP_MICROS: i64 = 1_709_210_096_123_456;
const EVENT_RAW_BYTES: &[u8] = &[0, 255, 65];

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedRow {
    operation: String,
    tenant: String,
    id: u64,
    payload: Option<String>,
    generation: Option<u64>,
    event_date: Option<i32>,
    event_datetime: Option<i64>,
    event_timestamp: Option<i64>,
    raw_bytes: Option<Vec<u8>>,
    old_tenant: Option<String>,
    old_id: Option<u64>,
    old_payload: Option<String>,
    old_generation: Option<u64>,
    old_event_date: Option<i32>,
    old_event_datetime: Option<i64>,
    old_event_timestamp: Option<i64>,
    old_raw_bytes: Option<Vec<u8>>,
    topic: String,
    partition: i64,
    offset: i64,
    message_index: u64,
    write_timestamp_ms: i64,
    source_timestamp_ms: i64,
    changed_columns: Vec<u8>,
    transaction_identity: Vec<u8>,
}

#[derive(Default)]
struct ObservedChanges {
    rows: Vec<ObservedRow>,
    schemas: BTreeMap<String, Arc<Schema>>,
    markers: Vec<CommitMarker>,
    serialized: SerializedMessages,
}

type SerializedMessage = (Option<Vec<u8>>, Option<Vec<u8>>);
type SerializedMessages = Vec<SerializedMessage>;

impl ObservedChanges {
    fn next_offsets(&self) -> BTreeMap<i64, i64> {
        let mut offsets = BTreeMap::<i64, i64>::new();
        for row in &self.rows {
            offsets
                .entry(row.partition)
                .and_modify(|offset| *offset = (*offset).max(row.offset + 1))
                .or_insert(row.offset + 1);
        }
        offsets
    }
}

#[tokio::test]
async fn ydb_changefeed_replays_until_ack_and_fails_closed_on_schema_drift() -> anyhow::Result<()> {
    let container = GenericImage::new(YDB_IMAGE, YDB_TAG)
        .with_exposed_port(YDB_PORT.tcp())
        .with_wait_for(WaitFor::healthcheck())
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("YDB_USE_IN_MEMORY_PDISKS", "true")
        .with_env_var("GRPC_PORT", YDB_PORT.to_string())
        .with_health_check(
            Healthcheck::cmd(["/health_check"])
                .with_interval(Duration::from_secs(1))
                .with_timeout(Duration::from_secs(3))
                .with_retries(90),
        )
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(YDB_PORT.tcp()).await?;
    let connection = YdbConnectionConfig {
        endpoint: format!("grpc://{host}:{port}"),
        database: "/local".to_owned(),
        trusted_plaintext: true,
        auth: YdbAuth::Anonymous,
        request_timeout_ms: 10_000,
        max_rpc_message_bytes: 256 * 1024 * 1024,
    };
    wait_for_ydb(&connection).await?;

    let mut admin = TestYdbAdmin::connect(&connection).await?;
    admin.create_coordination_node(COORDINATION_NODE).await?;
    admin
        .execute_scheme(format!(
            r"--!syntax_v1
CREATE TABLE `{EVENTS_TABLE}` (
    tenant Utf8 NOT NULL,
    id Uint64 NOT NULL,
    payload Utf8,
    generation Uint64,
    event_date Date NOT NULL,
    event_datetime Datetime NOT NULL,
    event_timestamp Timestamp NOT NULL,
    raw_bytes String NOT NULL,
    PRIMARY KEY (tenant, id)
);",
        ))
        .await?;
    admin
        .execute_scheme(format!(
            r"--!syntax_v1
CREATE TABLE `{JSON_TABLE}` (
    id Uint64 NOT NULL,
    document Json,
    PRIMARY KEY (id)
);",
        ))
        .await?;

    admin.add_exact_changefeed(EVENTS_TABLE, CHANGEFEED).await?;
    admin
        .wait_for_exact_changefeed(EVENTS_TABLE, CHANGEFEED)
        .await?;
    let topic = topic_path(EVENTS_TABLE, CHANGEFEED);
    let physical_partition_id = admin.assert_single_fixed_topic(&topic).await?;
    assert_eq!(
        physical_partition_id, 0,
        "pinned local-ydb did not assign the sole changefeed partition id 0"
    );
    admin.add_consumer(&topic, CONSUMER).await?;
    admin.add_exact_changefeed(JSON_TABLE, CHANGEFEED).await?;
    admin
        .wait_for_exact_changefeed(JSON_TABLE, CHANGEFEED)
        .await?;
    let json_topic = topic_path(JSON_TABLE, CHANGEFEED);
    admin.assert_single_fixed_topic(&json_topic).await?;
    admin.add_consumer(&json_topic, JSON_CONSUMER).await?;

    let cancellation = CancellationToken::new();
    let nullable_json = connector(&connection, JSON_TABLE, CHANGEFEED, JSON_CONSUMER)?;
    let json_error = nullable_json
        .delivery_discovery(discovery_context(&cancellation))
        .await
        .expect_err("nullable YDB Json reached source execution");
    let diagnostic = format!("{json_error:#}").to_lowercase();
    assert!(diagnostic.contains("json"), "{diagnostic}");
    assert!(diagnostic.contains("nullable"), "{diagnostic}");
    assert!(
        diagnostic.contains("lossless")
            || diagnostic.contains("ambiguous")
            || diagnostic.contains("distinguish"),
        "{diagnostic}"
    );

    let mut shared_root = transferia_test_support::durable_contexts(&[
        MAIN_DELIVERY_ID,
        "ydb-replication-other-delivery",
    ]);
    let other_delivery = shared_root
        .pop()
        .context("missing contender durable context")?;
    let durable = shared_root.pop().context("missing owner durable context")?;
    let other_root = transferia_test_support::durable_context();
    let baseline_offsets = admin.consumer_offsets(&topic, CONSUMER).await?;
    let (connector, discovery) = prepare_stream(&connection, &durable, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable, &cancellation).await?;

    for (name, contender) in [
        ("different delivery", &other_delivery),
        ("independent durable root", &other_root),
    ] {
        let error = reject_contender(&connection, contender, &cancellation).await?;
        let diagnostic = error.to_lowercase();
        assert!(
            diagnostic.contains("lease")
                || diagnostic.contains("semaphore")
                || diagnostic.contains("owner")
                || diagnostic.contains("another execution")
                || diagnostic.contains("different delivery")
                || diagnostic.contains("delivery_id")
                || diagnostic.contains("acquire")
                || diagnostic.contains("fenc"),
            "unexpected {name} fencing diagnostic: {diagnostic}"
        );
        assert_eq!(
            admin.consumer_offsets(&topic, CONSUMER).await?,
            baseline_offsets,
            "rejected {name} contender mutated the YDB consumer cursor"
        );
    }

    admin
        .execute_data(format!(
            r#"--!syntax_v1
UPSERT INTO `{EVENTS_TABLE}`
    (tenant, id, payload, generation, event_date, event_datetime, event_timestamp, raw_bytes)
VALUES (
    Utf8("acme"),
    Uint64("1"),
    Utf8("one"),
    Uint64("1"),
    Date("2024-02-29"),
    Datetime("2024-02-29T12:34:56Z"),
    Timestamp("2024-02-29T12:34:56.123456Z"),
    "\x00\xff\x41"
);"#,
        ))
        .await?;
    admin
        .execute_data(format!(
            r#"--!syntax_v1
UPDATE `{EVENTS_TABLE}`
SET payload = Utf8("one-updated"), generation = Uint64("2")
WHERE tenant = Utf8("acme") AND id = Uint64("1");"#,
        ))
        .await?;
    admin
        .execute_data(format!(
            r#"--!syntax_v1
UPSERT INTO `{EVENTS_TABLE}`
    (tenant, id, payload, generation, event_date, event_datetime, event_timestamp, raw_bytes)
VALUES (
    Utf8("other"),
    Uint64("1"),
    Utf8("two"),
    Uint64("1"),
    Date("2024-02-29"),
    Datetime("2024-02-29T12:34:56Z"),
    Timestamp("2024-02-29T12:34:56.123456Z"),
    "\x00\xff\x41"
);"#,
        ))
        .await?;
    admin
        .execute_data(format!(
            r#"--!syntax_v1
DELETE FROM `{EVENTS_TABLE}`
WHERE tenant = Utf8("acme") AND id = Uint64("1");"#,
        ))
        .await?;

    let first = read_changes(&mut stream, &discovery, 4).await?;
    assert_expected_changes(&first.rows)?;
    assert_debezium_changes(&first.serialized)?;
    assert_eq!(
        admin.consumer_offsets(&topic, CONSUMER).await?,
        baseline_offsets,
        "reading without a source commit advanced the real YDB consumer"
    );
    stream.shutdown().await?;
    assert_eq!(
        admin.consumer_offsets(&topic, CONSUMER).await?,
        baseline_offsets,
        "source shutdown committed offsets without downstream acknowledgement"
    );
    drop(stream);
    drop(connector);

    let (connector, discovery) = prepare_stream(&connection, &durable, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable, &cancellation).await?;
    let replay = read_changes(&mut stream, &discovery, 4).await?;
    assert_eq!(
        replay.rows, first.rows,
        "restart did not replay the exact YDB topic offsets"
    );
    assert_eq!(
        replay
            .rows
            .iter()
            .map(|row| {
                (
                    row.topic.as_str(),
                    row.partition,
                    row.offset,
                    row.message_index,
                )
            })
            .collect::<Vec<_>>(),
        first
            .rows
            .iter()
            .map(|row| {
                (
                    row.topic.as_str(),
                    row.partition,
                    row.offset,
                    row.message_index,
                )
            })
            .collect::<Vec<_>>(),
        "YDB reconnect changed the stable source-message replay tuple"
    );
    assert_eq!(
        replay.schemas, first.schemas,
        "restart changed the emitted Arrow schema"
    );
    assert_eq!(
        replay.serialized, first.serialized,
        "restart changed the exact YDB Debezium messages before acknowledgement"
    );

    stream.commit_offsets(&replay.markers).await?;
    let committed_offsets = admin.consumer_offsets(&topic, CONSUMER).await?;
    assert_ne!(
        committed_offsets, baseline_offsets,
        "commit_offsets returned without advancing the real YDB consumer"
    );
    assert_offsets_cover_rows(&committed_offsets, &replay)?;
    stream.shutdown().await?;
    drop(stream);
    drop(connector);

    let (connector, discovery) = prepare_stream(&connection, &durable, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable, &cancellation).await?;
    admin
        .execute_data(format!(
            r#"--!syntax_v1
UPSERT INTO `{EVENTS_TABLE}`
    (tenant, id, payload, generation, event_date, event_datetime, event_timestamp, raw_bytes)
VALUES (
    Utf8("after-ack"),
    Uint64("7"),
    Utf8("new"),
    Uint64("1"),
    Date("2024-02-29"),
    Datetime("2024-02-29T12:34:56Z"),
    Timestamp("2024-02-29T12:34:56.123456Z"),
    "\x00\xff\x41"
);"#,
        ))
        .await?;
    let after_ack = read_changes(&mut stream, &discovery, 1).await?;
    assert_eq!(after_ack.rows.len(), 1);
    assert_eq!(after_ack.rows[0].tenant, "after-ack");
    assert_eq!(after_ack.rows[0].id, 7);
    assert_eq!(after_ack.rows[0].operation, "c");
    assert_eq!(after_ack.rows[0].partition, 0);
    assert_wire_values(&after_ack.rows)?;
    anyhow::ensure!(
        after_ack.serialized.len() == 1 && debezium_value(&after_ack.serialized[0])?["op"] == "c",
        "post-ack YDB change did not serialize as one Debezium create"
    );
    assert!(
        after_ack.rows[0].offset
            >= committed_offsets
                .get(&after_ack.rows[0].partition)
                .copied()
                .context("new YDB change arrived on an unknown partition")?,
        "restart replayed a server-acknowledged offset"
    );
    stream.commit_offsets(&after_ack.markers).await?;
    let before_drift = admin.consumer_offsets(&topic, CONSUMER).await?;

    admin
        .execute_scheme(format!(
            r"--!syntax_v1
ALTER TABLE `{EVENTS_TABLE}` ADD COLUMN added Utf8;",
        ))
        .await?;
    admin
        .execute_data(format!(
            r#"--!syntax_v1
UPSERT INTO `{EVENTS_TABLE}`
    (tenant, id, payload, generation, event_date, event_datetime, event_timestamp, raw_bytes, added)
VALUES (
    Utf8("drift"),
    Uint64("9"),
    Utf8("must-fail"),
    Uint64("1"),
    Date("2024-02-29"),
    Datetime("2024-02-29T12:34:56Z"),
    Timestamp("2024-02-29T12:34:56.123456Z"),
    "\x00\xff\x41",
    Utf8("unknown")
);"#,
        ))
        .await?;

    let failure = read_fatal_schema_drift(&mut stream).await?;
    assert!(
        !failure.is_retryable(),
        "schema drift was retryable: {failure}"
    );
    let diagnostic = failure.to_string().to_lowercase();
    assert!(
        diagnostic.contains("unknown") && diagnostic.contains("added"),
        "{diagnostic}"
    );
    assert_eq!(
        admin.consumer_offsets(&topic, CONSUMER).await?,
        before_drift,
        "fatal schema drift advanced the real YDB consumer cursor"
    );
    drop(stream.shutdown().await);
    Ok(())
}

fn assert_offsets_cover_rows(
    committed: &BTreeMap<i64, i64>,
    observed: &ObservedChanges,
) -> anyhow::Result<()> {
    for (partition, required_next_offset) in observed.next_offsets() {
        let actual = committed
            .get(&partition)
            .copied()
            .with_context(|| format!("YDB omitted committed offset for partition {partition}"))?;
        anyhow::ensure!(
            actual >= required_next_offset,
            "commit_offsets returned before YDB acknowledged partition {partition}: expected at \
             least {required_next_offset}, got {actual}"
        );
    }
    Ok(())
}

fn assert_expected_changes(rows: &[ObservedRow]) -> anyhow::Result<()> {
    anyhow::ensure!(
        rows.iter().all(|row| {
            row.topic == topic_path(EVENTS_TABLE, CHANGEFEED).trim_start_matches('/')
        }),
        "YDB change rows did not preserve the physical changefeed topic"
    );
    anyhow::ensure!(
        rows.iter().all(|row| row.write_timestamp_ms > 0),
        "YDB change rows omitted their broker write timestamp"
    );
    anyhow::ensure!(
        rows.iter().all(|row| row.partition == 0),
        "YDB source Partition system column did not preserve the sole physical partition id 0"
    );
    anyhow::ensure!(
        rows.iter().all(|row| row.transaction_identity.len() == 16),
        "YDB change rows omitted the 128-bit virtual transaction identity"
    );
    for row in rows {
        anyhow::ensure!(
            row.message_index == 0,
            "one-row YDB changefeed message had message_index={}",
            row.message_index
        );
        let transaction_step = transaction_step(&row.transaction_identity)?;
        anyhow::ensure!(
            row.source_timestamp_ms == transaction_step,
            "source timestamp {} differs from CDC transaction step {transaction_step}",
            row.source_timestamp_ms
        );
    }
    assert_eq!(
        rows.iter()
            .map(|row| row.changed_columns.as_slice())
            .collect::<Vec<_>>(),
        vec![
            &[0b1111_1111][..],
            &[0b1111_1111][..],
            &[0b1111_1111][..],
            &[0b0000_0011][..],
        ],
        "YDB CDC changed-column masks did not distinguish full images from erase"
    );
    let positions = rows
        .iter()
        .map(|row| (row.partition, row.offset, row.message_index))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "YDB changes were not emitted in topic order: {positions:?}"
    );
    let without_positions = rows
        .iter()
        .map(|row| {
            (
                row.operation.as_str(),
                row.tenant.as_str(),
                row.id,
                row.payload.as_deref(),
                row.generation,
                row.old_tenant.as_deref(),
                row.old_id,
                row.old_payload.as_deref(),
                row.old_generation,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        without_positions,
        vec![
            ("c", "acme", 1, Some("one"), Some(1), None, None, None, None,),
            (
                "u",
                "acme",
                1,
                Some("one-updated"),
                Some(2),
                Some("acme"),
                Some(1),
                Some("one"),
                Some(1),
            ),
            (
                "c",
                "other",
                1,
                Some("two"),
                Some(1),
                None,
                None,
                None,
                None,
            ),
            (
                "d",
                "acme",
                1,
                Some("one-updated"),
                Some(2),
                Some("acme"),
                Some(1),
                Some("one-updated"),
                Some(2),
            ),
        ]
    );
    assert_wire_values(rows)?;
    Ok(())
}

fn assert_debezium_changes(messages: &[SerializedMessage]) -> anyhow::Result<()> {
    anyhow::ensure!(
        messages.len() == 5,
        "YDB c/u/c/d sequence produced {} Debezium messages instead of four values plus one delete tombstone",
        messages.len()
    );
    let operations = messages
        .iter()
        .filter_map(|message| message.1.as_deref())
        .map(|value| {
            serde_json::from_slice::<serde_json::Value>(value)
                .map(|json| json["op"].as_str().unwrap_or_default().to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(operations == ["c", "u", "c", "d"]);
    anyhow::ensure!(messages[4].1.is_none(), "YDB delete omitted its tombstone");
    anyhow::ensure!(
        serde_json::from_slice::<serde_json::Value>(
            messages[0]
                .0
                .as_deref()
                .context("YDB create omitted its key")?
        )? == serde_json::json!({"tenant":"acme","id":1}),
        "YDB Debezium create key lost its composite primary key"
    );

    let create = debezium_value(&messages[0])?;
    anyhow::ensure!(create["before"].is_null());
    anyhow::ensure!(create["after"]["payload"] == "one");
    anyhow::ensure!(create["after"]["raw_bytes"] == "AP9B");
    anyhow::ensure!(create["after"]["event_date"] == EVENT_DATE_DAYS);
    anyhow::ensure!(create["after"]["event_datetime"] == EVENT_DATETIME_SECONDS * 1_000);
    anyhow::ensure!(create["after"]["event_timestamp"] == EVENT_TIMESTAMP_MICROS);

    let update = debezium_value(&messages[1])?;
    anyhow::ensure!(update["before"]["payload"] == "one");
    anyhow::ensure!(update["after"]["payload"] == "one-updated");
    anyhow::ensure!(
        update["after"]["event_timestamp"] != "__debezium_unavailable_value",
        "YDB full new image was mistaken for a PostgreSQL TOAST mask"
    );

    let delete = debezium_value(&messages[3])?;
    anyhow::ensure!(delete["before"]["payload"] == "one-updated");
    anyhow::ensure!(delete["after"].is_null());
    anyhow::ensure!(messages[3].0 == messages[4].0);

    for message in messages.iter().filter(|message| message.1.is_some()) {
        let value = debezium_value(message)?;
        let source = &value["source"];
        anyhow::ensure!(source["version"] == "1.0.0");
        anyhow::ensure!(source["connector"] == "ydb");
        anyhow::ensure!(source["name"] == "inventory");
        anyhow::ensure!(source["snapshot"] == "false");
        anyhow::ensure!(source["db"] == "/local");
        anyhow::ensure!(source["table"] == EVENTS_TABLE);
        anyhow::ensure!(source["step"] == source["ts_ms"]);
        anyhow::ensure!(source["txId"].as_u64().is_some());
        anyhow::ensure!(value["ts_ms"].as_i64().is_some_and(|value| value > 0));
        for foreign in [
            "schema",
            "lsn",
            "xmin",
            "server_id",
            "gtid",
            "file",
            "pos",
            "row",
            "thread",
            "query",
            "ts_us",
            "ts_ns",
        ] {
            anyhow::ensure!(
                source.get(foreign).is_none(),
                "YDB source contains '{foreign}'"
            );
        }
        anyhow::ensure!(value.get("ts_us").is_none() && value.get("ts_ns").is_none());
    }
    Ok(())
}

fn debezium_value(
    message: &(Option<Vec<u8>>, Option<Vec<u8>>),
) -> anyhow::Result<serde_json::Value> {
    serde_json::from_slice(
        message
            .1
            .as_deref()
            .context("expected a YDB Debezium value message")?,
    )
    .map_err(Into::into)
}

fn assert_wire_values(rows: &[ObservedRow]) -> anyhow::Result<()> {
    for row in rows {
        anyhow::ensure!(
            row.event_date == Some(EVENT_DATE_DAYS),
            "YDB Date did not round-trip as exact Arrow Date32 days"
        );
        anyhow::ensure!(
            row.event_datetime == Some(EVENT_DATETIME_SECONDS),
            "YDB Datetime did not round-trip as exact Arrow TimestampSecond"
        );
        anyhow::ensure!(
            row.event_timestamp == Some(EVENT_TIMESTAMP_MICROS),
            "YDB Timestamp did not round-trip as exact Arrow TimestampMicrosecond"
        );
        anyhow::ensure!(
            row.raw_bytes.as_deref() == Some(EVENT_RAW_BYTES),
            "YDB String did not preserve the exact non-UTF8/control bytes"
        );
        let expected_old = (row.operation == "u" || row.operation == "d").then_some((
            EVENT_DATE_DAYS,
            EVENT_DATETIME_SECONDS,
            EVENT_TIMESTAMP_MICROS,
        ));
        anyhow::ensure!(
            row.old_event_date == expected_old.map(|values| values.0)
                && row.old_event_datetime == expected_old.map(|values| values.1)
                && row.old_event_timestamp == expected_old.map(|values| values.2),
            "YDB temporal old image did not match NEW_AND_OLD_IMAGES semantics for operation '{}'",
            row.operation
        );
        let expected_old_bytes =
            (row.operation == "u" || row.operation == "d").then_some(EVENT_RAW_BYTES);
        anyhow::ensure!(
            row.old_raw_bytes.as_deref() == expected_old_bytes,
            "YDB String old image did not preserve the exact bytes for operation '{}'",
            row.operation
        );
    }
    Ok(())
}

async fn prepare_stream(
    connection: &YdbConnectionConfig,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<(YdbSourceConnector, DeliveryDiscovery)> {
    let connector = connector(connection, EVENTS_TABLE, CHANGEFEED, CONSUMER)?;
    connector
        .delivery_discovery(discovery_context(cancellation))
        .await?;
    let prepared = connector
        .prepare_execution(SourceExecutionContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::Stream,
            replay_identity: Some(Arc::from(REPLAY_IDENTITY)),
            durable: durable.clone(),
        })
        .await?
        .context("YDB replication preparation returned no execution plan")?;
    anyhow::ensure!(
        prepared.discovery.source_topology == SourceTopology::StaticPartitions(vec![0]),
        "YDB replication did not expose one logical worker partition"
    );
    Ok((connector, prepared.discovery))
}

async fn reject_contender(
    connection: &YdbConnectionConfig,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<String> {
    let contender = connector(connection, EVENTS_TABLE, CHANGEFEED, CONSUMER)?;
    contender
        .delivery_discovery(discovery_context(cancellation))
        .await?;
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        contender.prepare_execution(SourceExecutionContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.child_token(),
            delivery_type: DeliveryType::Stream,
            replay_identity: Some(Arc::from(REPLAY_IDENTITY)),
            durable: durable.clone(),
        }),
    )
    .await
    .context("timed out waiting for YDB replication contender fencing")?;
    result
        .err()
        .map(|error| format!("{error:#}"))
        .with_context(|| "a concurrent YDB replication owner was accepted")
}

async fn build_stream(
    connector: &YdbSourceConnector,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<Box<dyn Source>> {
    connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: DeliveryType::Stream,
            phase: SourcePhase::Stream,
            replay_identity: Some(Arc::from(REPLAY_IDENTITY)),
            cancellation: cancellation.child_token(),
            memory: PipelineMemory::new(128 * 1024 * 1024),
            durable: durable.clone(),
        })
        .await
}

fn connector(
    connection: &YdbConnectionConfig,
    table: &str,
    changefeed: &str,
    consumer: &str,
) -> anyhow::Result<YdbSourceConnector> {
    let config: YdbSourceConfig = serde_json::from_value(serde_json::json!({
        "endpoint": connection.endpoint,
        "database": connection.database,
        "trusted_plaintext": connection.trusted_plaintext,
        "auth": { "type": "anonymous" },
        "request_timeout_ms": connection.request_timeout_ms,
        "tables": [{ "path": table }],
        "batch_rows": 1_024,
        "session_shutdown_timeout_ms": 30_000,
        "session_shutdown_retry_initial_ms": 50,
        "replication": {
            "changefeed_name": changefeed,
            "consumer_name": consumer,
            "read_buffer_bytes": 8 * 1024 * 1024,
            "max_message_bytes": 64 * 1024,
            "max_batch_bytes": 96 * 1024,
            "max_response_bytes": 120 * 1024,
            "commit_timeout_ms": 10_000,
            "coordination_node_path": COORDINATION_NODE
        }
    }))?;
    YdbSourceConnector::from_config(config, Arc::new(MetricsRegistry::new()))
}

fn discovery_context(cancellation: &CancellationToken) -> SourceDiscoveryContext {
    SourceDiscoveryContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type: DeliveryType::Stream,
    }
}

async fn read_changes(
    source: &mut Box<dyn Source>,
    discovery: &DeliveryDiscovery,
    expected_rows: usize,
) -> anyhow::Result<ObservedChanges> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut observed = ObservedChanges::default();
        let serializer_config: SerializerConfig = serde_json::from_value(serde_json::json!({
            "type": "debezium",
            "format": { "type": "json" }
        }))?;
        serializer_config.validate_discovery(discovery)?;
        let mut serializer = DeliverySerializer::new(
            &serializer_config,
            QueueMessageMode::KeyedWithTombstones,
            "Inventory delivery",
        )?;
        let mut delivery_id = 0_u64;
        while observed.rows.len() < expected_rows {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    commit_marker,
                    ..
                } => {
                    let serialized =
                        serialize_tables(&mut serializer, discovery, &tables, delivery_id).await?;
                    delivery_id = delivery_id
                        .checked_add(1)
                        .context("YDB Debezium test delivery id overflow")?;
                    observed.serialized.extend(
                        serialized
                            .batches
                            .into_iter()
                            .flat_map(|batch| batch.messages)
                            .map(|message| (message.key, message.value)),
                    );
                    observe_tables(&mut observed, discovery, tables)?;
                    if let Some(marker) = commit_marker {
                        observed.markers.push(marker);
                    }
                }
                SourceBatch::Raw { .. } | SourceBatch::Finished => {
                    anyhow::bail!("YDB replication returned raw or finite data")
                }
            }
        }
        anyhow::ensure!(
            observed.rows.len() == expected_rows,
            "YDB returned {} rows while exactly {expected_rows} were expected",
            observed.rows.len()
        );
        anyhow::ensure!(
            !observed.markers.is_empty(),
            "YDB replication omitted commit markers"
        );
        Ok(observed)
    })
    .await
    .context("timed out waiting for YDB changefeed rows")?
}

async fn serialize_tables(
    serializer: &mut DeliverySerializer,
    discovery: &DeliveryDiscovery,
    tables: &[TableData],
    delivery_id: u64,
) -> anyhow::Result<transferia_connector_support::serializer::SerializedDelivery> {
    let source_messages = tables
        .iter()
        .map(|table| u64::try_from(table.batch.num_rows()))
        .try_fold(0_u64, |total, rows| {
            total
                .checked_add(rows?)
                .context("YDB source message count overflow")
        })?;
    let mut outputs = Vec::with_capacity(tables.len());
    for table in tables {
        let batch = sink_batch(table);
        validate_batch_against_discovery(discovery, &batch)?;
        let ProjectedSinkBatch::Changelog(projected) = project_sink_batch(discovery, &batch)?
        else {
            anyhow::bail!("YDB CDC batch crossed the Debezium boundary as append-only data")
        };
        anyhow::ensure!(
            projected.rows().num_rows() == table.batch.num_rows(),
            "YDB Debezium sink projection changed the row count"
        );
        outputs.push(batch);
    }
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

fn observe_tables(
    observed: &mut ObservedChanges,
    discovery: &DeliveryDiscovery,
    tables: Vec<TableData>,
) -> anyhow::Result<()> {
    for table in tables {
        validate_changelog_table(discovery, &table)?;
        anyhow::ensure!(
            table.table.as_ref() == "replication_events",
            "unexpected YDB dataset '{}'",
            table.table
        );
        let table_name = table.table.to_string();
        if let Some(previous) = observed
            .schemas
            .insert(table_name.clone(), table.batch.schema())
        {
            anyhow::ensure!(
                previous == table.batch.schema(),
                "YDB changed the Arrow schema within one read"
            );
        }
        let tenants = array_by_name::<StringArray>(&table, "tenant")?;
        let ids = array_by_name::<UInt64Array>(&table, "id")?;
        let payloads = array_by_name::<StringArray>(&table, "payload")?;
        let generations = array_by_name::<UInt64Array>(&table, "generation")?;
        let event_dates = array_by_name::<Date32Array>(&table, "event_date")?;
        let event_datetimes = array_by_name::<TimestampSecondArray>(&table, "event_datetime")?;
        let event_timestamps =
            array_by_name::<TimestampMicrosecondArray>(&table, "event_timestamp")?;
        let raw_bytes = array_by_name::<BinaryArray>(&table, "raw_bytes")?;
        let old_tenants = old_array::<StringArray>(&table, "tenant")?;
        let old_ids = old_array::<UInt64Array>(&table, "id")?;
        let old_payloads = old_array::<StringArray>(&table, "payload")?;
        let old_generations = old_array::<UInt64Array>(&table, "generation")?;
        let old_event_dates = old_array::<Date32Array>(&table, "event_date")?;
        let old_event_datetimes = old_array::<TimestampSecondArray>(&table, "event_datetime")?;
        let old_event_timestamps =
            old_array::<TimestampMicrosecondArray>(&table, "event_timestamp")?;
        let old_raw_bytes = old_array::<BinaryArray>(&table, "raw_bytes")?;
        let operations = system_array::<StringArray>(&table, SystemColumnKind::ChangeOperation)?;
        let topics = system_array::<StringArray>(&table, SystemColumnKind::Topic)?;
        let partitions = system_array::<Int64Array>(&table, SystemColumnKind::Partition)?;
        let offsets = system_array::<Int64Array>(&table, SystemColumnKind::Offset)?;
        let message_indexes = system_array::<UInt64Array>(&table, SystemColumnKind::MessageIndex)?;
        let write_timestamps =
            system_array::<Int64Array>(&table, SystemColumnKind::WriteTimestampMs)?;
        let changed_columns =
            system_array::<BinaryArray>(&table, SystemColumnKind::ChangedColumns)?;
        let transaction_identities =
            role_array::<FixedSizeBinaryArray>(&table, SYSTEM_ROLE_SOURCE_TRANSACTION_ID)?;
        let source_timestamps = role_array::<Int64Array>(&table, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS)?;
        for row in 0..table.batch.num_rows() {
            anyhow::ensure!(
                !tenants.is_null(row),
                "YDB change lost composite key tenant"
            );
            anyhow::ensure!(!ids.is_null(row), "YDB change lost composite key id");
            let message_index = message_indexes.value(row);
            let write_timestamp_ms = write_timestamps.value(row);
            let source_timestamp_ms = source_timestamps.value(row);
            let transaction_identity = transaction_identities.value(row).to_vec();
            anyhow::ensure!(
                message_index == 0,
                "one-row YDB changefeed message had message_index={message_index}"
            );
            anyhow::ensure!(
                source_timestamp_ms == transaction_step(&transaction_identity)?,
                "YDB source timestamp does not match its checked CDC transaction step"
            );
            anyhow::ensure!(
                source_timestamp_ms != write_timestamp_ms,
                "YDB source timestamp was replaced by the broker write timestamp"
            );
            observed.rows.push(ObservedRow {
                operation: required_string(operations, row, "change operation")?,
                tenant: required_string(tenants, row, "tenant")?,
                id: ids.value(row),
                payload: optional_string(payloads, row),
                generation: optional_u64(generations, row),
                event_date: optional_date32(event_dates, row),
                event_datetime: optional_timestamp_second(event_datetimes, row),
                event_timestamp: optional_timestamp_microsecond(event_timestamps, row),
                raw_bytes: optional_binary(raw_bytes, row),
                old_tenant: optional_string(old_tenants, row),
                old_id: optional_u64(old_ids, row),
                old_payload: optional_string(old_payloads, row),
                old_generation: optional_u64(old_generations, row),
                old_event_date: optional_date32(old_event_dates, row),
                old_event_datetime: optional_timestamp_second(old_event_datetimes, row),
                old_event_timestamp: optional_timestamp_microsecond(old_event_timestamps, row),
                old_raw_bytes: optional_binary(old_raw_bytes, row),
                topic: required_string(topics, row, "topic")?,
                partition: partitions.value(row),
                offset: offsets.value(row),
                message_index,
                write_timestamp_ms,
                source_timestamp_ms,
                changed_columns: required_binary(changed_columns, row, "changed columns")?,
                transaction_identity,
            });
        }
    }
    Ok(())
}

fn validate_changelog_table(
    discovery: &DeliveryDiscovery,
    table: &TableData,
) -> anyhow::Result<()> {
    let batch = sink_batch(table);
    validate_batch_against_discovery(discovery, &batch)?;
    let ProjectedSinkBatch::Changelog(projected) = project_sink_batch(discovery, &batch)? else {
        anyhow::bail!("YDB CDC batch crossed the sink boundary as append-only data")
    };
    anyhow::ensure!(
        projected.rows().num_rows() == table.batch.num_rows(),
        "YDB CDC sink projection changed the row count"
    );
    Ok(())
}

fn sink_batch(table: &TableData) -> SinkBatch {
    let byte_size = table.batch.get_array_memory_size();
    let memory = PipelineMemory::new(byte_size.max(1));
    SinkBatch {
        table: Arc::clone(&table.table),
        is_dlq: table.is_dlq,
        batch: table.batch.clone(),
        byte_size,
        memory: memory.reserve_transform(byte_size),
        system_columns: table.system_columns.clone(),
    }
}

fn transaction_step(identity: &[u8]) -> anyhow::Result<i64> {
    let encoded = identity
        .get(..8)
        .context("YDB transaction identity has no 64-bit step")?;
    let step = u64::from_be_bytes(encoded.try_into()?);
    i64::try_from(step).context("YDB transaction step does not fit source timestamp metadata")
}

async fn read_fatal_schema_drift(
    source: &mut Box<dyn Source>,
) -> anyhow::Result<transferia_core::failure::DataPlaneFailure> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match source.read_batch().await {
                Err(failure) => return Ok(failure),
                Ok(SourceBatch::Typed { tables, .. }) if tables.is_empty() => {}
                Ok(batch) => anyhow::bail!("schema drift emitted {batch:?} instead of failing"),
            }
        }
    })
    .await
    .context("schema drift was not detected before the read deadline")?
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
        .with_context(|| format!("column '{name}' has an unexpected Arrow type"))
}

fn old_array<'a, T: arrow::array::Array + 'static>(
    table: &'a TableData,
    current_name: &str,
) -> anyhow::Result<&'a T> {
    let schema = table.batch.schema();
    let index = schema
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get(META_OLD_VALUE_OF)
                .is_some_and(|name| name == current_name)
        })
        .with_context(|| format!("missing old-image field for '{current_name}'"))?;
    table
        .batch
        .column(index)
        .as_any()
        .downcast_ref()
        .with_context(|| format!("old image of '{current_name}' has an unexpected Arrow type"))
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

fn role_array<'a, T: arrow::array::Array + 'static>(
    table: &'a TableData,
    role: &str,
) -> anyhow::Result<&'a T> {
    let schema = table.batch.schema();
    let index = schema
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get(META_SYSTEM_ROLE)
                .is_some_and(|actual| actual == role)
        })
        .with_context(|| format!("missing '{role}' source metadata column"))?;
    table
        .batch
        .column(index)
        .as_any()
        .downcast_ref()
        .with_context(|| format!("'{role}' source metadata has an unexpected Arrow type"))
}

fn required_string(array: &StringArray, row: usize, name: &str) -> anyhow::Result<String> {
    anyhow::ensure!(!array.is_null(row), "YDB change has no {name}");
    Ok(array.value(row).to_owned())
}

fn required_binary(array: &BinaryArray, row: usize, name: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(!array.is_null(row), "YDB change has no {name}");
    Ok(array.value(row).to_vec())
}

fn optional_string(array: &StringArray, row: usize) -> Option<String> {
    (!array.is_null(row)).then(|| array.value(row).to_owned())
}

fn optional_u64(array: &UInt64Array, row: usize) -> Option<u64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_binary(array: &BinaryArray, row: usize) -> Option<Vec<u8>> {
    (!array.is_null(row)).then(|| array.value(row).to_vec())
}

fn optional_date32(array: &Date32Array, row: usize) -> Option<i32> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_timestamp_second(array: &TimestampSecondArray, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_timestamp_microsecond(array: &TimestampMicrosecondArray, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

fn topic_path(table: &str, changefeed: &str) -> String {
    format!("{table}/{changefeed}")
}

#[allow(
    deprecated,
    reason = "the pinned fixture sets every partition-count field exposed by YDB 25.4.1"
)]
const fn fixed_topic_partitioning() -> PartitioningSettings {
    PartitioningSettings {
        min_active_partitions: 1,
        max_active_partitions: 1,
        partition_count_limit: 1,
        auto_partitioning_settings: Some(AutoPartitioningSettings {
            strategy: AutoPartitioningStrategy::Disabled as i32,
            partition_write_speed: None,
        }),
    }
}

async fn wait_for_ydb(config: &YdbConnectionConfig) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            match ydb::check_connection(config).await {
                Ok(()) => return Ok(()),
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("YDB testcontainer did not become ready"))?
}

struct TestYdbAdmin {
    table: TableServiceClient<Channel>,
    topic: TopicServiceClient<Channel>,
    coordination: CoordinationServiceClient<Channel>,
    database: AsciiMetadataValue,
    timeout: Duration,
}

impl TestYdbAdmin {
    async fn connect(config: &YdbConnectionConfig) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(config.tonic_endpoint()?)?
            .connect_timeout(config.request_timeout())
            .timeout(config.request_timeout())
            .connect()
            .await?;
        Ok(Self {
            table: TableServiceClient::new(channel.clone()),
            topic: TopicServiceClient::new(channel.clone()),
            coordination: CoordinationServiceClient::new(channel),
            database: AsciiMetadataValue::try_from(config.database.clone())?,
            timeout: config.request_timeout(),
        })
    }

    fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("x-ydb-database", self.database.clone());
        request
    }

    async fn create_session(&mut self) -> anyhow::Result<String> {
        let request = self.request(CreateSessionRequest {
            operation_params: None,
        });
        let response = tokio::time::timeout(self.timeout, self.table.create_session(request))
            .await??
            .into_inner();
        Ok(
            decode_operation::<CreateSessionResult>(response.operation, "CreateSession")?
                .session_id,
        )
    }

    async fn create_coordination_node(&mut self, path: &str) -> anyhow::Result<()> {
        let request = self.request(CreateNodeRequest {
            path: path.to_owned(),
            config: Some(CoordinationConfig {
                path: String::new(),
                self_check_period_millis: 1_000,
                session_grace_period_millis: 1_000,
                read_consistency_mode: ConsistencyMode::Strict.into(),
                attach_consistency_mode: ConsistencyMode::Strict.into(),
                rate_limiter_counters_mode: RateLimiterCountersMode::Detailed.into(),
            }),
            operation_params: None,
        });
        let response = tokio::time::timeout(self.timeout, self.coordination.create_node(request))
            .await??
            .into_inner();
        ensure_operation(response.operation, "CreateCoordinationNode")?;
        Ok(())
    }

    async fn delete_session(&mut self, session_id: String) {
        let request = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let _ignored = self.table.delete_session(request).await;
    }

    async fn execute_scheme(&mut self, yql_text: String) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(ExecuteSchemeQueryRequest {
            session_id: session_id.clone(),
            yql_text,
            operation_params: None,
        });
        let response = tokio::time::timeout(self.timeout, self.table.execute_scheme_query(request))
            .await??
            .into_inner();
        ensure_operation(response.operation, "ExecuteSchemeQuery")?;
        self.delete_session(session_id).await;
        Ok(())
    }

    async fn execute_data(&mut self, yql_text: String) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(ExecuteDataQueryRequest {
            session_id: session_id.clone(),
            tx_control: Some(TransactionControl {
                commit_tx: true,
                tx_selector: Some(transaction_control::TxSelector::BeginTx(
                    TransactionSettings {
                        tx_mode: Some(table::transaction_settings::TxMode::SerializableReadWrite(
                            SerializableModeSettings {},
                        )),
                    },
                )),
            }),
            query: Some(Query {
                query: Some(query::Query::YqlText(yql_text)),
            }),
            parameters: HashMap::new(),
            query_cache_policy: Some(QueryCachePolicy {
                keep_in_cache: false,
            }),
            operation_params: None,
            collect_stats: table::query_stats_collection::Mode::StatsCollectionNone.into(),
        });
        let response = tokio::time::timeout(self.timeout, self.table.execute_data_query(request))
            .await??
            .into_inner();
        decode_operation::<ExecuteQueryResult>(response.operation, "ExecuteDataQuery")?;
        self.delete_session(session_id).await;
        Ok(())
    }

    async fn add_exact_changefeed(
        &mut self,
        table_path: &str,
        changefeed_name: &str,
    ) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(AlterTableRequest {
            session_id: session_id.clone(),
            path: table_path.to_owned(),
            add_changefeeds: vec![Changefeed {
                name: changefeed_name.to_owned(),
                mode: changefeed_mode::Mode::NewAndOldImages.into(),
                format: changefeed_format::Format::Json.into(),
                retention_period: None,
                virtual_timestamps: true,
                initial_scan: false,
                attributes: HashMap::new(),
                aws_region: String::new(),
                resolved_timestamps_interval: None,
                topic_partitioning_settings: Some(fixed_topic_partitioning()),
                schema_changes: false,
            }],
            ..Default::default()
        });
        let response = tokio::time::timeout(self.timeout, self.table.alter_table(request))
            .await??
            .into_inner();
        ensure_operation(response.operation, "AlterTable(AddChangefeed)")?;
        self.delete_session(session_id).await;
        Ok(())
    }

    async fn describe_table(&mut self, table_path: &str) -> anyhow::Result<DescribeTableResult> {
        let session_id = self.create_session().await?;
        let request = self.request(DescribeTableRequest {
            session_id: session_id.clone(),
            path: table_path.to_owned(),
            operation_params: None,
            include_shard_key_bounds: false,
            include_table_stats: false,
            include_partition_stats: false,
            include_set_val: false,
            include_shard_nodes_info: false,
        });
        let response = tokio::time::timeout(self.timeout, self.table.describe_table(request))
            .await??
            .into_inner();
        let result = decode_operation(response.operation, "DescribeTable")?;
        self.delete_session(session_id).await;
        Ok(result)
    }

    async fn describe_topic(&mut self, topic_path: &str) -> anyhow::Result<DescribeTopicResult> {
        let request = self.request(DescribeTopicRequest {
            operation_params: None,
            path: topic_path.to_owned(),
            include_stats: false,
            include_location: false,
        });
        let response = tokio::time::timeout(self.timeout, self.topic.describe_topic(request))
            .await??
            .into_inner();
        decode_operation(response.operation, "DescribeTopic")
    }

    async fn assert_single_fixed_topic(&mut self, topic_path: &str) -> anyhow::Result<i64> {
        let description = self.describe_topic(topic_path).await?;
        let partitioning = description.partitioning_settings.with_context(|| {
            format!("YDB changefeed topic '{topic_path}' omitted partitioning settings")
        })?;
        let auto_partitioning = partitioning.auto_partitioning_settings.with_context(|| {
            format!("YDB changefeed topic '{topic_path}' omitted auto-partitioning settings")
        })?;
        let strategy = AutoPartitioningStrategy::try_from(auto_partitioning.strategy)
            .with_context(|| {
                format!(
                    "YDB changefeed topic '{topic_path}' returned unknown auto-partitioning strategy {}",
                    auto_partitioning.strategy
                )
            })?;
        anyhow::ensure!(
            strategy == AutoPartitioningStrategy::Disabled,
            "YDB changefeed topic '{topic_path}' enabled auto-partitioning with strategy {}",
            strategy.as_str_name()
        );
        anyhow::ensure!(
            description.partitions.len() == 1,
            "YDB changefeed topic '{topic_path}' has {} partitions instead of exactly one",
            description.partitions.len()
        );
        let partition = description
            .partitions
            .first()
            .context("single YDB changefeed partition disappeared")?;
        anyhow::ensure!(
            partition.partition_id >= 0,
            "YDB changefeed topic '{topic_path}' has negative partition id {}",
            partition.partition_id
        );
        anyhow::ensure!(
            partition.active,
            "YDB changefeed topic '{topic_path}' sole partition {} is inactive",
            partition.partition_id
        );
        anyhow::ensure!(
            partition.parent_partition_ids.is_empty() && partition.child_partition_ids.is_empty(),
            "YDB changefeed topic '{topic_path}' sole partition {} has split/merge ancestry",
            partition.partition_id
        );
        Ok(partition.partition_id)
    }

    async fn wait_for_exact_changefeed(
        &mut self,
        table_path: &str,
        changefeed_name: &str,
    ) -> anyhow::Result<()> {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let description = self.describe_table(table_path).await?;
                let changefeed = description
                    .changefeeds
                    .iter()
                    .find(|changefeed| changefeed.name == changefeed_name)
                    .with_context(|| {
                        format!("YDB table '{table_path}' omitted changefeed '{changefeed_name}'")
                    })?;
                anyhow::ensure!(
                    changefeed.mode == i32::from(changefeed_mode::Mode::NewAndOldImages),
                    "YDB changefeed was not NEW_AND_OLD_IMAGES"
                );
                anyhow::ensure!(
                    changefeed.format == i32::from(changefeed_format::Format::Json),
                    "YDB changefeed was not JSON"
                );
                anyhow::ensure!(
                    changefeed.virtual_timestamps,
                    "YDB changefeed omitted VIRTUAL_TIMESTAMPS"
                );
                anyhow::ensure!(
                    !changefeed.schema_changes,
                    "YDB changefeed unexpectedly enabled SCHEMA_CHANGES"
                );
                anyhow::ensure!(
                    changefeed.attributes.is_empty(),
                    "YDB JSON changefeed unexpectedly has attributes"
                );
                anyhow::ensure!(
                    changefeed.aws_region.is_empty(),
                    "YDB JSON changefeed unexpectedly has an AWS region"
                );
                anyhow::ensure!(
                    changefeed.resolved_timestamps_interval.is_none(),
                    "YDB changefeed unexpectedly enabled resolved timestamps"
                );
                anyhow::ensure!(
                    changefeed.initial_scan_progress.is_none(),
                    "YDB changefeed unexpectedly enabled INITIAL_SCAN"
                );
                if changefeed.state == i32::from(changefeed_description::State::Enabled) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("YDB changefeed did not become enabled")?
    }

    async fn add_consumer(&mut self, topic_path: &str, consumer: &str) -> anyhow::Result<()> {
        let request = self.request(AlterTopicRequest {
            path: topic_path.to_owned(),
            add_consumers: vec![Consumer {
                name: consumer.to_owned(),
                important: true,
                read_from: None,
                supported_codecs: Some(SupportedCodecs {
                    codecs: vec![Codec::Raw.into()],
                }),
                attributes: HashMap::new(),
                consumer_stats: None,
                availability_period: None,
            }],
            ..Default::default()
        });
        let response = tokio::time::timeout(self.timeout, self.topic.alter_topic(request))
            .await??
            .into_inner();
        ensure_operation(response.operation, "AlterTopic(AddConsumer)")?;
        Ok(())
    }

    async fn consumer_offsets(
        &mut self,
        topic_path: &str,
        consumer: &str,
    ) -> anyhow::Result<BTreeMap<i64, i64>> {
        let request = self.request(DescribeConsumerRequest {
            operation_params: None,
            path: topic_path.to_owned(),
            consumer: consumer.to_owned(),
            include_stats: true,
            include_location: false,
        });
        let response = tokio::time::timeout(self.timeout, self.topic.describe_consumer(request))
            .await??
            .into_inner();
        let result: DescribeConsumerResult =
            decode_operation(response.operation, "DescribeConsumer")?;
        result
            .partitions
            .into_iter()
            .map(|partition| {
                let stats = partition.partition_consumer_stats.with_context(|| {
                    format!(
                        "YDB omitted consumer stats for partition {}",
                        partition.partition_id
                    )
                })?;
                Ok((partition.partition_id, stats.committed_offset))
            })
            .collect()
    }
}

fn decode_operation<T: Message + Default>(
    operation: Option<Operation>,
    name: &str,
) -> anyhow::Result<T> {
    let operation = ensure_operation(operation, name)?;
    let result = operation
        .result
        .with_context(|| format!("YDB {name} returned no result"))?;
    Ok(T::decode(result.value.as_slice())?)
}

fn ensure_operation(operation: Option<Operation>, name: &str) -> anyhow::Result<Operation> {
    let operation = operation.with_context(|| format!("YDB {name} returned no operation"))?;
    anyhow::ensure!(
        operation.ready,
        "YDB {name} returned an asynchronous operation"
    );
    let status = StatusCode::try_from(operation.status).unwrap_or(StatusCode::Unspecified);
    anyhow::ensure!(
        status == StatusCode::Success,
        "YDB {name} failed with {status:?}: {:?}",
        operation.issues
    );
    Ok(operation)
}
