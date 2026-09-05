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
use arrow::array::{Array as _, BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Schema};
use mysql_async::prelude::Queryable as _;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use transferia_connector_mysql::metrics::MetricsRegistry;
use transferia_connector_mysql::mysql::{connect, MySqlConnectionConfig, MySqlSourceConnector};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME, META_CHANGE_OPERATION,
    META_OLD_KEY_OF, META_OLD_VALUE_OF, META_SYSTEM_ROLE, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};
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
const MARIADB_IMAGE: &str = "mariadb";
const MARIADB_TAG: &str = "11.8.3";
const MYSQL_PORT: u16 = 3_306;
const ROOT_PASSWORD: &str = "test";
const DATABASE: &str = "transferia";
const SOURCE_USER: &str = "transferia_source";
const SOURCE_PASSWORD: &str = "source-test";
const REPLAY_IDENTITY: &str = "mysql-replication-e2e-revision-1";
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
enum MySqlServerMode {
    ReplicationReady,
    BinlogDisabled,
}

struct MySqlFixture {
    _container: ContainerAsync<GenericImage>,
    admin: MySqlConnectionConfig,
    source: MySqlConnectionConfig,
}

impl MySqlFixture {
    async fn mysql(mode: MySqlServerMode) -> anyhow::Result<Self> {
        let image = GenericImage::new(MYSQL_IMAGE, MYSQL_TAG)
            .with_exposed_port(MYSQL_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
            .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .with_env_var("MYSQL_DATABASE", DATABASE);
        let container = match mode {
            MySqlServerMode::ReplicationReady => {
                image
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
                    .await?
            }
            MySqlServerMode::BinlogDisabled => {
                image
                    .with_cmd([
                        "--skip-log-bin",
                        "--gtid-mode=ON",
                        "--enforce-gtid-consistency=ON",
                    ])
                    .start()
                    .await?
            }
        };
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
            admin,
            source: connection_config(&host, port, SOURCE_USER, SOURCE_PASSWORD),
        })
    }

    async fn mariadb() -> anyhow::Result<Self> {
        let container = GenericImage::new(MARIADB_IMAGE, MARIADB_TAG)
            .with_exposed_port(MYSQL_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
            .with_env_var("MARIADB_ROOT_PASSWORD", ROOT_PASSWORD)
            .with_env_var("MARIADB_DATABASE", DATABASE)
            .with_cmd([
                "--server-id=1",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--sync-binlog=1",
                "--binlog-expire-logs-seconds=0",
            ])
            .start()
            .await?;
        let host = reachable_host(&container.get_host().await?);
        let port = container.get_host_port_ipv4(MYSQL_PORT.tcp()).await?;
        let admin = connection_config(&host, port, "root", ROOT_PASSWORD);
        wait_for_mysql(&admin).await?.disconnect().await?;
        Ok(Self {
            _container: container,
            source: admin.clone(),
            admin,
        })
    }

    async fn connection(&self) -> anyhow::Result<mysql_async::Conn> {
        connect(&self.admin).await
    }
}

#[derive(Default)]
struct RecordingDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
    leases: Arc<Mutex<HashSet<String>>>,
}

impl RecordingDurableStorage {
    fn snapshot(&self) -> BTreeMap<String, DurableValue> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl DurableStorage for RecordingDurableStorage {
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
            let revision = match expected_revision {
                Some(revision) => revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("durable revision overflow"))?,
                None => 0,
            };
            let value = DurableValue {
                revision,
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            drop(values);
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
            Ok(DurableLease::new(RecordingDurableLease {
                key: key.to_owned(),
                leases: Arc::clone(&self.leases),
            }))
        })
    }
}

struct RecordingDurableLease {
    key: String,
    leases: Arc<Mutex<HashSet<String>>>,
}

impl Drop for RecordingDurableLease {
    fn drop(&mut self) {
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

struct TestDurable {
    context: DurableContext,
    local: Arc<RecordingDurableStorage>,
    resources: Arc<RecordingDurableStorage>,
}

impl TestDurable {
    fn new(delivery_id: &str) -> Self {
        let local = Arc::new(RecordingDurableStorage::default());
        let resources = Arc::new(RecordingDurableStorage::default());
        Self {
            context: DurableContext {
                delivery_id: Arc::from(delivery_id),
                storage: local.clone(),
                resource_storage: resources.clone(),
            },
            local,
            resources,
        }
    }

    fn is_empty(&self) -> bool {
        self.local.snapshot().is_empty() && self.resources.snapshot().is_empty()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObservedRow {
    table: String,
    operation: String,
    id: i64,
    payload: String,
    old_id: Option<i64>,
    old_payload: Option<String>,
}

#[derive(Default)]
struct ObservedPhase {
    rows: Vec<ObservedRow>,
    schemas: BTreeMap<String, Arc<Schema>>,
}

struct TypedSourceBatch {
    tables: Vec<TableData>,
    source_rows: u64,
    marker: CommitMarker,
}

#[tokio::test]
async fn exact_snapshot_boundary_has_no_gap_overlap_or_duplicate() -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    exec_all(
        &mut admin,
        &[
            "CREATE TABLE exact_accounts (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "CREATE TABLE exact_aux (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "CREATE TABLE exact_ignored (id BIGINT NOT NULL, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "INSERT INTO exact_accounts VALUES (1, 'a-one'), (2, 'a-two')",
            "INSERT INTO exact_aux VALUES (10, 'b-ten')",
        ],
    )
    .await?;

    let config = source_yaml(&fixture.source, &["exact_accounts", "exact_aux"], true);
    let connector = mysql_connector(&config)?;
    let durable = TestDurable::new("mysql-exact-boundary");
    let cancellation = CancellationToken::new();
    let preview = connector
        .delivery_discovery(discovery_context(
            DeliveryType::BatchAndStream,
            &cancellation,
        ))
        .await?;
    let phases = connector.execution_phases(DeliveryType::BatchAndStream, &preview)?;
    assert_eq!(phases.len(), 2);
    assert_eq!(phases[0].phase, SourcePhase::Snapshot);
    assert!(phases[0].finite);
    assert_eq!(phases[1].phase, SourcePhase::Stream);
    assert!(!phases[1].finite);

    let prepared = tokio::time::timeout(
        TEST_TIMEOUT,
        connector.prepare_execution(execution_context(
            DeliveryType::BatchAndStream,
            &durable.context,
            &cancellation,
        )),
    )
    .await
    .context("timed out establishing the exact MySQL snapshot")??
    .expect("batch_and_stream must return authoritative snapshot discovery");
    assert_eq!(prepared.remaining_phases, phases);

    exec_all(
        &mut admin,
        &[
            "START TRANSACTION",
            "INSERT INTO exact_accounts VALUES (3, 'a-three')",
            "UPDATE exact_accounts SET id = 11, payload = 'a-eleven' WHERE id = 1",
            "DELETE FROM exact_accounts WHERE id = 2",
            "INSERT INTO exact_aux VALUES (20, 'b-twenty'), (21, 'b-twenty-one')",
            "INSERT INTO exact_ignored VALUES (100, 'ignored')",
            "COMMIT",
        ],
    )
    .await?;

    let snapshot = read_snapshot(
        &connector,
        &prepared.discovery,
        &durable.context,
        &cancellation,
    )
    .await?;
    assert_eq!(
        snapshot.rows,
        sorted_rows([
            row("exact_accounts", "r", 1, "a-one", None, None),
            row("exact_accounts", "r", 2, "a-two", None, None),
            row("exact_aux", "r", 10, "b-ten", None, None),
        ]),
        "snapshot included a post-boundary mutation or omitted a pre-boundary row"
    );

    let premature = connector
        .build_source(build_context(
            DeliveryType::BatchAndStream,
            SourcePhase::Stream,
            0,
            &durable.context,
            &cancellation,
        ))
        .await;
    assert!(
        premature.is_err(),
        "stream started before the finite snapshot phase completed"
    );

    connector
        .complete_execution_phase(
            SourcePhase::Snapshot,
            durable.context.clone(),
            cancellation.child_token(),
        )
        .await?;
    drop(connector);

    let resumed = mysql_connector(&config)?;
    let resumed_preview = resumed
        .delivery_discovery(discovery_context(
            DeliveryType::BatchAndStream,
            &cancellation,
        ))
        .await?;
    let prepared = resumed
        .prepare_execution(execution_context(
            DeliveryType::BatchAndStream,
            &durable.context,
            &cancellation,
        ))
        .await?
        .expect("completed exact snapshot must resume as stream-only");
    let resumed_phases =
        resumed.execution_phases(DeliveryType::BatchAndStream, &resumed_preview)?;
    assert_eq!(prepared.remaining_phases, vec![resumed_phases[1].clone()]);
    assert!(
        resumed
            .build_source(build_context(
                DeliveryType::BatchAndStream,
                SourcePhase::Snapshot,
                0,
                &durable.context,
                &cancellation,
            ))
            .await
            .is_err(),
        "completed snapshot was replayable"
    );

    let mut stream = resumed
        .build_source(build_context(
            DeliveryType::BatchAndStream,
            SourcePhase::Stream,
            0,
            &durable.context,
            &cancellation,
        ))
        .await?;
    let delivery = read_nonempty(&mut stream).await?;
    assert_eq!(
        delivery.source_rows, 5,
        "one multi-table transaction was split, filtered incorrectly, or counted ignored rows"
    );
    let transaction_identities = transaction_identities(&delivery.tables)?;
    assert_eq!(
        transaction_identities.iter().collect::<HashSet<_>>().len(),
        1,
        "one source transaction emitted different immutable transaction identities"
    );
    let mut changes = ObservedPhase::default();
    observe_tables(&mut changes, delivery.tables)?;
    changes.rows.sort();
    assert_eq!(
        changes.rows,
        sorted_rows([
            row("exact_accounts", "c", 3, "a-three", None, None),
            row(
                "exact_accounts",
                "u",
                11,
                "a-eleven",
                Some(1),
                Some("a-one"),
            ),
            row("exact_accounts", "d", 2, "a-two", Some(2), Some("a-two"),),
            row("exact_aux", "c", 20, "b-twenty", None, None),
            row("exact_aux", "c", 21, "b-twenty-one", None, None),
        ]),
        "stream has a gap, overlap, duplicate, or incorrect before image at the exact boundary"
    );
    assert_eq!(snapshot.schemas, changes.schemas);
    stream.commit_offsets(&[delivery.marker]).await?;
    stream.shutdown().await?;
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn exhaustive_physical_types_match_snapshot_and_binlog_exactly() -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    exec_all(
        &mut admin,
        &[
            "SET SESSION sql_mode = ''",
            "SET SESSION time_zone = '+00:00'",
            "SET GLOBAL sql_mode = 'PAD_CHAR_TO_FULL_LENGTH'",
            "CREATE TABLE type_parity (\
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, \
                i8 TINYINT NOT NULL, u8 TINYINT UNSIGNED ZEROFILL NOT NULL, \
                i16 SMALLINT NOT NULL, u16 SMALLINT UNSIGNED NOT NULL, \
                i24 MEDIUMINT NOT NULL, u24 MEDIUMINT UNSIGNED NOT NULL, \
                i32 INT NOT NULL, u32 INT UNSIGNED NOT NULL, \
                i64 BIGINT NOT NULL, u64 BIGINT UNSIGNED NOT NULL, \
                f32 FLOAT NOT NULL, f64 DOUBLE NOT NULL, \
                dec65 DECIMAL(65,30) NOT NULL, \
                bit1 BIT(1) NOT NULL, bit9 BIT(9) NOT NULL, bit64 BIT(64) NOT NULL, \
                bin_fixed BINARY(4) NOT NULL, var_bin VARBINARY(8) NOT NULL, \
                blob_value BLOB NOT NULL, \
                utf8_value VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, \
                latin1_value VARCHAR(8) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL, \
                char_utf8 CHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, \
                char_latin1 CHAR(4) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL, \
                date_valid DATE NOT NULL, date_zero DATE NOT NULL, date_partial DATE NOT NULL, \
                datetime_valid DATETIME(6) NOT NULL, datetime_zero DATETIME(6) NOT NULL, \
                datetime_partial DATETIME(6) NOT NULL, timestamp_valid TIMESTAMP(6) NOT NULL, \
                timestamp_zero TIMESTAMP(6) NOT NULL, \
                time_negative TIME(6) NOT NULL, year_zero YEAR NOT NULL, year_valid YEAR NOT NULL, \
                enum_utf8 ENUM('', 'plain', 'quote''value') CHARACTER SET utf8mb4 NOT NULL, \
                enum_empty ENUM('', 'plain') CHARACTER SET utf8mb4 NOT NULL, \
                enum_bytes ENUM('plain', 'ÿ') CHARACTER SET latin1 COLLATE latin1_bin NOT NULL, \
                set_utf8 SET('alpha', 'beta', 'quote''value') CHARACTER SET utf8mb4 NOT NULL, \
                set_empty SET('alpha', 'beta') CHARACTER SET utf8mb4 NOT NULL, \
                json_value JSON NOT NULL, \
                point_value POINT SRID 4326 NOT NULL, \
                stored_generated BIGINT GENERATED ALWAYS AS (i32 + 1) STORED, \
                secret VARBINARY(4) INVISIBLE NOT NULL, nullable_utf8 VARCHAR(8) NULL, \
                PRIMARY KEY (id)\
            ) ENGINE=InnoDB",
            "CREATE TABLE unsupported_virtual (\
                id BIGINT NOT NULL PRIMARY KEY, value INT NOT NULL, \
                derived INT GENERATED ALWAYS AS (value + 1) VIRTUAL\
            ) ENGINE=InnoDB",
            "INSERT INTO type_parity (\
                id, i8, u8, i16, u16, i24, u24, i32, u32, i64, u64, f32, f64, dec65, \
                bit1, bit9, bit64, bin_fixed, var_bin, blob_value, utf8_value, latin1_value, \
                char_utf8, char_latin1, \
                date_valid, date_zero, date_partial, datetime_valid, datetime_zero, \
                datetime_partial, timestamp_valid, timestamp_zero, time_negative, year_zero, year_valid, \
                enum_utf8, enum_empty, enum_bytes, set_utf8, set_empty, json_value, \
                point_value, secret, nullable_utf8\
            ) VALUES (\
                1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, \
                -9223372036854775808, 18446744073709551615, 1.5, -3.25, \
                12345678901234567890123456789012345.123456789012345678901234567890, \
                b'1', b'101010101', 0xFEDCBA9876543210, 0x00FF, 0x00FF7F, 0x000102FF, \
                _utf8mb4'text 🙂', _latin1 0x80FF, _utf8mb4'a b', _latin1 0x80, \
                '2024-02-29', '0000-00-00', '2024-00-15', \
                '2024-02-29 23:59:58.123456', '0000-00-00 00:00:00.000000', \
                '2024-00-15 12:34:56.000001', '2038-01-19 03:14:07.499999', \
                '0000-00-00 00:00:00.000000', \
                '-123:45:56.123456', 0, 2155, 'quote''value', '', _latin1 0xFF, \
                'alpha,quote''value', '', \
                JSON_OBJECT(\
                    'array', JSON_ARRAY(1, TRUE, NULL, _utf8mb4'🙂'), \
                    'double_fixed_high', CAST(1e15 AS DOUBLE), \
                    'double_scientific_high', CAST(1e16 AS DOUBLE), \
                    'double_fixed_low', CAST(1e-15 AS DOUBLE), \
                    'double_scientific_low', CAST(1e-16 AS DOUBLE), \
                    'double_large', CAST(1e200 AS DOUBLE), \
                    'double_negative_zero', CAST('-0' AS DOUBLE), \
                    'decimal', CAST('12345678901234567890.12345678901234567890' AS DECIMAL(40,20)), \
                    'date', CAST('2024-02-29' AS DATE), \
                    'datetime', CAST('2024-02-29 23:59:58.123456' AS DATETIME(6)), \
                    'time', CAST('-123:45:56.123456' AS TIME(6)), \
                    'opaque_binary', CAST(_binary 0x5100FF AS BINARY)\
                ), \
                ST_SRID(POINT(-7.25, 12.5), 4326), 0x00FF7F, NULL\
            )",
        ],
    )
    .await?;

    let config = source_yaml(&fixture.source, &["type_parity"], true);
    let connector = mysql_connector(&config)?;
    let durable = TestDurable::new("mysql-exhaustive-type-parity");
    let cancellation = CancellationToken::new();
    let preview = connector
        .delivery_discovery(discovery_context(
            DeliveryType::BatchAndStream,
            &cancellation,
        ))
        .await?;
    let phases = connector.execution_phases(DeliveryType::BatchAndStream, &preview)?;
    let prepared = connector
        .prepare_execution(execution_context(
            DeliveryType::BatchAndStream,
            &durable.context,
            &cancellation,
        ))
        .await?
        .context("type parity source did not prepare an exact snapshot")?;
    assert_eq!(prepared.remaining_phases, phases);

    admin
        .query_drop(
            "INSERT INTO type_parity (\
                id, i8, u8, i16, u16, i24, u24, i32, u32, i64, u64, f32, f64, dec65, \
                bit1, bit9, bit64, bin_fixed, var_bin, blob_value, utf8_value, latin1_value, \
                char_utf8, char_latin1, \
                date_valid, date_zero, date_partial, datetime_valid, datetime_zero, \
                datetime_partial, timestamp_valid, timestamp_zero, time_negative, year_zero, year_valid, \
                enum_utf8, enum_empty, enum_bytes, set_utf8, set_empty, json_value, \
                point_value, secret, nullable_utf8\
            ) SELECT \
                2, i8, u8, i16, u16, i24, u24, i32, u32, i64, u64, f32, f64, dec65, \
                bit1, bit9, bit64, bin_fixed, var_bin, blob_value, utf8_value, latin1_value, \
                char_utf8, char_latin1, \
                date_valid, date_zero, date_partial, datetime_valid, datetime_zero, \
                datetime_partial, timestamp_valid, timestamp_zero, time_negative, year_zero, year_valid, \
                enum_utf8, enum_empty, enum_bytes, set_utf8, set_empty, json_value, \
                point_value, secret, nullable_utf8 \
            FROM type_parity WHERE id = 1",
        )
        .await?;

    let snapshot = read_one_snapshot_table(
        &connector,
        &prepared.discovery,
        &durable.context,
        &cancellation,
        "type_parity",
    )
    .await?;
    assert_mysql_extension(
        &snapshot,
        "id",
        &DataType::UInt64,
        "transferia.mysql.unsigned_integer",
        &[("unsigned", serde_json::Value::Bool(true))],
    )?;
    assert_mysql_extension(
        &snapshot,
        "u8",
        &DataType::UInt8,
        "transferia.mysql.unsigned_integer",
        &[("zerofill", serde_json::Value::Bool(true))],
    )?;
    assert_mysql_extension(
        &snapshot,
        "dec65",
        &DataType::Utf8,
        "transferia.mysql.decimal",
        &[
            ("numeric_precision", serde_json::Value::from(65_u64)),
            ("numeric_scale", serde_json::Value::from(30_u64)),
        ],
    )?;
    assert_mysql_extension(
        &snapshot,
        "latin1_value",
        &DataType::Binary,
        "transferia.mysql.text_bytes",
        &[(
            "character_set",
            serde_json::Value::String("latin1".to_owned()),
        )],
    )?;
    assert_mysql_extension(
        &snapshot,
        "char_utf8",
        &DataType::Utf8,
        "transferia.mysql.text",
        &[(
            "collation_padding",
            serde_json::Value::String("pad_space".to_owned()),
        )],
    )?;
    assert_mysql_extension(
        &snapshot,
        "char_latin1",
        &DataType::Binary,
        "transferia.mysql.text_bytes",
        &[(
            "collation_padding",
            serde_json::Value::String("pad_space".to_owned()),
        )],
    )?;
    assert_mysql_extension(
        &snapshot,
        "date_zero",
        &DataType::Utf8,
        "transferia.mysql.date",
        &[],
    )?;
    assert_mysql_extension(&snapshot, "json_value", &DataType::Utf8, "arrow.json", &[])?;
    assert_mysql_extension(
        &snapshot,
        "enum_empty",
        &DataType::UInt16,
        "transferia.mysql.enum",
        &[("enum_set_values", serde_json::json!(["", "plain"]))],
    )?;
    assert_mysql_extension(
        &snapshot,
        "set_utf8",
        &DataType::UInt64,
        "transferia.mysql.set",
        &[(
            "enum_set_values",
            serde_json::json!(["alpha", "beta", "quote'value"]),
        )],
    )?;
    assert_mysql_extension(
        &snapshot,
        "point_value",
        &DataType::Binary,
        "transferia.mysql.binary",
        &[("srs_id", serde_json::Value::from(4_326_u64))],
    )?;
    assert_mysql_extension(
        &snapshot,
        "stored_generated",
        &DataType::Int64,
        "transferia.mysql.signed_integer",
        &[("generation", serde_json::Value::String("stored".to_owned()))],
    )?;
    assert_mysql_extension(
        &snapshot,
        "secret",
        &DataType::Binary,
        "transferia.mysql.binary",
        &[(
            "visibility",
            serde_json::Value::String("invisible".to_owned()),
        )],
    )?;
    connector
        .complete_execution_phase(
            SourcePhase::Snapshot,
            durable.context.clone(),
            cancellation.child_token(),
        )
        .await?;
    let mut stream = connector
        .build_source(build_context(
            DeliveryType::BatchAndStream,
            SourcePhase::Stream,
            0,
            &durable.context,
            &cancellation,
        ))
        .await?;

    let inserted = read_nonempty(&mut stream).await?;
    let inserted_table = take_only_table(inserted.tables, "type_parity")?;
    anyhow::ensure!(
        snapshot.batch.schema() == inserted_table.batch.schema(),
        "MySQL snapshot and binlog emitted different Arrow schemas"
    );
    assert_user_columns_equal(&snapshot, 0, &inserted_table, 0, &["id"])?;
    assert_old_values_are_null_and_typed(&inserted_table, 0)?;
    assert_operation(&inserted_table, 0, "c")?;
    stream.commit_offsets(&[inserted.marker]).await?;

    admin
        .query_drop("UPDATE type_parity SET id = 3 WHERE id = 2")
        .await?;
    let updated = read_nonempty(&mut stream).await?;
    let updated_table = take_only_table(updated.tables, "type_parity")?;
    assert_user_columns_equal(&inserted_table, 0, &updated_table, 0, &["id"])?;
    assert_old_values_equal_current(&inserted_table, 0, &updated_table, 0)?;
    assert_operation(&updated_table, 0, "u")?;
    stream.commit_offsets(&[updated.marker]).await?;

    admin
        .query_drop("DELETE FROM type_parity WHERE id = 3")
        .await?;
    let deleted = read_nonempty(&mut stream).await?;
    let deleted_table = take_only_table(deleted.tables, "type_parity")?;
    assert_user_columns_equal(&updated_table, 0, &deleted_table, 0, &[])?;
    assert_old_values_equal_current(&updated_table, 0, &deleted_table, 0)?;
    assert_operation(&deleted_table, 0, "d")?;
    stream.commit_offsets(&[deleted.marker]).await?;

    stream.shutdown().await?;
    let unsupported = source_yaml(&fixture.source, &["unsupported_virtual"], true);
    let unsupported_durable = TestDurable::new("mysql-unsupported-virtual-column");
    let diagnostic = reject_before_execution(
        &unsupported,
        DeliveryType::BatchAndStream,
        &unsupported_durable,
        &cancellation,
    )
    .await?
    .to_lowercase();
    assert!(diagnostic.contains("virtual"), "{diagnostic}");
    assert!(diagnostic.contains("generated"), "{diagnostic}");
    assert!(unsupported_durable.is_empty());
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn durable_offset_replays_until_commit_and_filtered_transactions_checkpoint(
) -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    exec_all(
        &mut admin,
        &[
            "CREATE TABLE replay_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "CREATE TABLE replay_ignored (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        ],
    )
    .await?;
    let config = source_yaml(&fixture.source, &["replay_events"], true);
    let durable = TestDurable::new("mysql-replay");
    let cancellation = CancellationToken::new();

    let connector = prepare_stream(&config, &durable.context, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable.context, &cancellation).await?;
    let initial_state = durable.local.snapshot();
    admin
        .query_drop("INSERT INTO replay_events VALUES (1, 'uncommitted')")
        .await?;
    let first = read_nonempty(&mut stream).await?;
    assert_eq!(durable.local.snapshot(), initial_state);
    let first_transaction_identities = transaction_identities(&first.tables)?;
    let first_tables = first.tables.clone();
    stream.shutdown().await?;
    drop(stream);

    let mut stream = build_stream(&connector, &durable.context, &cancellation).await?;
    let replay = read_nonempty(&mut stream).await?;
    assert_eq!(
        transaction_identities(&replay.tables)?,
        first_transaction_identities,
        "restart changed the transaction identities attached to replayed rows"
    );
    assert_same_deterministic_tables(&first_tables, &replay.tables);
    assert_eq!(
        durable.local.snapshot(),
        initial_state,
        "reading a replay advanced durable state before sink acknowledgement"
    );
    stream.commit_offsets(&[replay.marker]).await?;
    let committed_state = durable.local.snapshot();
    assert_ne!(committed_state, initial_state);
    stream.shutdown().await?;
    drop(stream);
    drop(connector);

    let local_before_mismatch = durable.local.snapshot();
    let resources_before_mismatch = durable.resources.snapshot();
    let mismatched = mysql_connector(&config)?;
    mismatched
        .delivery_discovery(discovery_context(DeliveryType::Stream, &cancellation))
        .await?;
    let mismatch = mismatched
        .prepare_execution(execution_context_with_replay_identity(
            DeliveryType::Stream,
            &durable.context,
            &cancellation,
            "mysql-replication-e2e-revision-2",
        ))
        .await
        .expect_err("a changed replay identity was allowed to read the existing binlog state");
    let diagnostic = format!("{mismatch:#}").to_lowercase();
    assert!(
        diagnostic.contains("replay") && diagnostic.contains("identity"),
        "unexpected replay-identity diagnostic: {mismatch:#}"
    );
    assert_eq!(durable.local.snapshot(), local_before_mismatch);
    assert_eq!(durable.resources.snapshot(), resources_before_mismatch);
    drop(mismatched);

    let resumed = prepare_stream(&config, &durable.context, &cancellation).await?;
    let mut stream = build_stream(&resumed, &durable.context, &cancellation).await?;
    admin
        .query_drop("INSERT INTO replay_ignored VALUES (10, 'filtered')")
        .await?;
    let filtered = read_empty_checkpoint(&mut stream).await?;
    assert_eq!(durable.local.snapshot(), committed_state);
    stream.commit_offsets(&[filtered]).await?;
    let filtered_state = durable.local.snapshot();
    assert_ne!(filtered_state, committed_state);
    stream.shutdown().await?;
    drop(stream);
    drop(resumed);

    let resumed = prepare_stream(&config, &durable.context, &cancellation).await?;
    let mut stream = build_stream(&resumed, &durable.context, &cancellation).await?;
    admin
        .query_drop("INSERT INTO replay_events VALUES (2, 'after-checkpoint')")
        .await?;
    let after = read_nonempty(&mut stream).await?;
    let mut observed = ObservedPhase::default();
    observe_tables(&mut observed, after.tables)?;
    assert_eq!(
        observed.rows,
        vec![row("replay_events", "c", 2, "after-checkpoint", None, None,)],
        "restart replayed a committed selected or filtered transaction"
    );
    stream.commit_offsets(&[after.marker]).await?;
    stream.shutdown().await?;
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_schema_drift_is_fatal_without_offset_progress() -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    admin
        .query_drop(
            "CREATE TABLE drift_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        )
        .await?;
    let config = source_yaml(&fixture.source, &["drift_events"], true);
    let durable = TestDurable::new("mysql-schema-drift");
    let cancellation = CancellationToken::new();
    let connector = prepare_stream(&config, &durable.context, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable.context, &cancellation).await?;

    admin
        .query_drop("INSERT INTO drift_events VALUES (1, 'baseline')")
        .await?;
    let baseline = read_nonempty(&mut stream).await?;
    stream.commit_offsets(&[baseline.marker]).await?;
    let durable_before = durable.local.snapshot();

    exec_all(
        &mut admin,
        &[
            "ALTER TABLE drift_events ADD COLUMN added BIGINT NOT NULL DEFAULT 0",
            "INSERT INTO drift_events (id, payload, added) VALUES (2, 'after-drift', 9)",
        ],
    )
    .await?;
    let failure = match tokio::time::timeout(TEST_TIMEOUT, stream.read_batch()).await {
        Ok(Err(failure)) => failure,
        Ok(Ok(batch)) => panic!("schema drift emitted {batch:?} instead of failing closed"),
        Err(error) => panic!("schema drift was not detected before the read deadline: {error}"),
    };
    assert!(
        !failure.is_retryable(),
        "schema drift was retryable: {failure}"
    );
    let diagnostic = failure.to_string().to_lowercase();
    assert!(
        diagnostic.contains("unsupportedstatement") && diagnostic.contains("schema"),
        "{diagnostic}"
    );
    assert_eq!(
        durable.local.snapshot(),
        durable_before,
        "schema drift advanced the durable binlog offset"
    );
    let persisted: u64 = admin
        .query_first("SELECT COUNT(*) FROM drift_events WHERE id = 2")
        .await?
        .expect("count row");
    assert_eq!(persisted, 1, "source-side data was modified by CDC failure");
    drop(stream.shutdown().await);
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn rotation_resumes_exactly_and_purged_required_history_is_fatal() -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    admin
        .query_drop(
            "CREATE TABLE rotate_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        )
        .await?;
    let config = source_yaml(&fixture.source, &["rotate_events"], true);
    let durable = TestDurable::new("mysql-rotation");
    let cancellation = CancellationToken::new();

    let connector = prepare_stream(&config, &durable.context, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable.context, &cancellation).await?;
    admin
        .query_drop("INSERT INTO rotate_events VALUES (1, 'before-rotate')")
        .await?;
    let before_rotate = read_nonempty(&mut stream).await?;
    stream.commit_offsets(&[before_rotate.marker]).await?;
    stream.shutdown().await?;
    drop(stream);
    drop(connector);

    let first_log = current_binary_log(&mut admin).await?;
    admin.query_drop("FLUSH BINARY LOGS").await?;
    let second_log = current_binary_log(&mut admin).await?;
    assert_ne!(first_log, second_log);
    admin
        .query_drop(format!("PURGE BINARY LOGS TO '{second_log}'"))
        .await?;

    let connector = prepare_stream(&config, &durable.context, &cancellation).await?;
    let mut stream = build_stream(&connector, &durable.context, &cancellation).await?;
    admin
        .query_drop("INSERT INTO rotate_events VALUES (2, 'after-rotate')")
        .await?;
    let after_rotate = read_nonempty(&mut stream).await?;
    let mut observed = ObservedPhase::default();
    observe_tables(&mut observed, after_rotate.tables)?;
    assert_eq!(
        observed.rows,
        vec![row("rotate_events", "c", 2, "after-rotate", None, None,)],
        "restart across rotation replayed a committed transaction or skipped the new file"
    );
    stream.commit_offsets(&[after_rotate.marker]).await?;
    stream.shutdown().await?;
    drop(stream);
    drop(connector);

    admin
        .query_drop("INSERT INTO rotate_events VALUES (3, 'must-not-be-skipped')")
        .await?;
    admin.query_drop("FLUSH BINARY LOGS").await?;
    let third_log = current_binary_log(&mut admin).await?;
    assert_ne!(second_log, third_log);
    admin
        .query_drop(format!("PURGE BINARY LOGS TO '{third_log}'"))
        .await?;
    let durable_before = durable.local.snapshot();
    let resources_before = durable.resources.snapshot();

    let restarted = mysql_connector(&config)?;
    restarted
        .delivery_discovery(discovery_context(DeliveryType::Stream, &cancellation))
        .await?;
    let error = restarted
        .prepare_execution(execution_context(
            DeliveryType::Stream,
            &durable.context,
            &cancellation,
        ))
        .await
        .expect_err("a purged required transaction was silently skipped");
    let diagnostic = format!("{error:#}").to_lowercase();
    assert!(
        diagnostic.contains("purged")
            || diagnostic.contains("binary log")
            || diagnostic.contains("history"),
        "unexpected purged-history diagnostic: {error:#}"
    );
    assert_eq!(durable.local.snapshot(), durable_before);
    assert_eq!(durable.resources.snapshot(), resources_before);
    let persisted: u64 = admin
        .query_first("SELECT COUNT(*) FROM rotate_events WHERE id = 3")
        .await?
        .expect("count row");
    assert_eq!(persisted, 1);
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn mysql_named_lock_fences_independent_durable_roots_without_disrupting_owner(
) -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    admin
        .query_drop(
            "CREATE TABLE fenced_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        )
        .await?;
    let config = source_yaml(&fixture.source, &["fenced_events"], true);
    let cancellation = CancellationToken::new();
    let owner_durable = TestDurable::new("mysql-lock-owner");
    let contender_durable = TestDurable::new("mysql-lock-owner");
    let owner = prepare_stream(&config, &owner_durable.context, &cancellation).await?;
    let mut owner_stream = build_stream(&owner, &owner_durable.context, &cancellation).await?;

    let contender = mysql_connector(&config)?;
    contender
        .delivery_discovery(discovery_context(DeliveryType::Stream, &cancellation))
        .await?;
    let error = tokio::time::timeout(
        TEST_TIMEOUT,
        contender.prepare_execution(execution_context(
            DeliveryType::Stream,
            &contender_durable.context,
            &cancellation,
        )),
    )
    .await
    .context("timed out waiting for MySQL-side ownership fencing")?
    .expect_err("an independent durable root acquired an already-active replica server id");
    let diagnostic = format!("{error:#}").to_lowercase();
    assert!(
        diagnostic.contains("active")
            || diagnostic.contains("owner")
            || diagnostic.contains("lock"),
        "unexpected ownership diagnostic: {error:#}"
    );
    assert!(
        contender_durable.is_empty(),
        "server-fenced contender persisted local or resource state"
    );

    admin
        .query_drop("INSERT INTO fenced_events VALUES (1, 'owner-alive')")
        .await?;
    let batch = read_nonempty(&mut owner_stream).await?;
    let mut observed = ObservedPhase::default();
    observe_tables(&mut observed, batch.tables)?;
    assert_eq!(
        observed.rows,
        vec![row("fenced_events", "c", 1, "owner-alive", None, None,)],
        "contender disconnected or stole the active binlog reader"
    );
    owner_stream.commit_offsets(&[batch.marker]).await?;
    owner_stream.shutdown().await?;
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_server_contracts_fail_before_durable_state() -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();

    let no_binlog = MySqlFixture::mysql(MySqlServerMode::BinlogDisabled).await?;
    let mut admin = no_binlog.connection().await?;
    admin
        .query_drop(
            "CREATE TABLE no_binlog_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        )
        .await?;
    let durable = TestDurable::new("mysql-no-binlog");
    let config = source_yaml(&no_binlog.source, &["no_binlog_events"], true);
    let error =
        reject_before_execution(&config, DeliveryType::Stream, &durable, &cancellation).await?;
    assert!(error.to_lowercase().contains("log_bin"), "{error}");
    assert!(durable.is_empty());
    admin.disconnect().await?;

    let invalid_image = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = invalid_image.connection().await?;
    exec_all(
        &mut admin,
        &[
            "CREATE TABLE minimal_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "SET GLOBAL binlog_row_image = 'MINIMAL'",
        ],
    )
    .await?;
    let durable = TestDurable::new("mysql-minimal-row-image");
    let config = source_yaml(&invalid_image.source, &["minimal_events"], true);
    let error = reject_before_execution(
        &config,
        DeliveryType::BatchAndStream,
        &durable,
        &cancellation,
    )
    .await?;
    let diagnostic = error.to_lowercase();
    assert!(diagnostic.contains("binlog_row_image"), "{diagnostic}");
    assert!(diagnostic.contains("full"), "{diagnostic}");
    assert!(durable.is_empty());

    admin
        .query_drop("SET GLOBAL binlog_row_image = 'FULL'")
        .await?;
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = 'ON'")
        .await?;
    let durable = TestDurable::new("mysql-compressed-transactions");
    let config = source_yaml(&invalid_image.source, &["minimal_events"], true);
    let error =
        reject_before_execution(&config, DeliveryType::Stream, &durable, &cancellation).await?;
    let diagnostic = error.to_lowercase();
    assert!(
        diagnostic.contains("binlog_transaction_compression"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("off"), "{diagnostic}");
    assert!(durable.is_empty());

    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = 'OFF'")
        .await?;
    admin
        .query_drop(
            "CREATE TABLE no_primary_key (id BIGINT NOT NULL, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        )
        .await?;
    let durable = TestDurable::new("mysql-no-primary-key");
    let config = source_yaml(&invalid_image.source, &["no_primary_key"], true);
    let error =
        reject_before_execution(&config, DeliveryType::Stream, &durable, &cancellation).await?;
    let diagnostic = error.to_lowercase();
    assert!(diagnostic.contains("primary key"), "{diagnostic}");
    assert!(durable.is_empty());
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn interrupted_snapshot_is_not_silently_restarted() -> anyhow::Result<()> {
    let fixture = MySqlFixture::mysql(MySqlServerMode::ReplicationReady).await?;
    let mut admin = fixture.connection().await?;
    exec_all(
        &mut admin,
        &[
            "CREATE TABLE interrupted_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "INSERT INTO interrupted_events VALUES (1, 'possibly-persisted'), (2, 'not-read-yet')",
        ],
    )
    .await?;
    let config = source_yaml(&fixture.source, &["interrupted_events"], true);
    let durable = TestDurable::new("mysql-interrupted-snapshot");
    let cancellation = CancellationToken::new();
    let connector = mysql_connector(&config)?;
    let preview = connector
        .delivery_discovery(discovery_context(
            DeliveryType::BatchAndStream,
            &cancellation,
        ))
        .await?;
    let prepared = connector
        .prepare_execution(execution_context(
            DeliveryType::BatchAndStream,
            &durable.context,
            &cancellation,
        ))
        .await?
        .expect("combined execution must establish its exact snapshot");
    let partition = snapshot_partitions(&prepared.discovery)?[0];
    let mut snapshot = connector
        .build_source(build_context(
            DeliveryType::BatchAndStream,
            SourcePhase::Snapshot,
            partition,
            &durable.context,
            &cancellation,
        ))
        .await?;
    let delivered = read_nonempty(&mut snapshot).await?;
    snapshot.commit_offsets(&[delivered.marker]).await?;
    let interrupted_state = durable.local.snapshot();
    assert!(!interrupted_state.is_empty());
    snapshot.shutdown().await?;
    drop(snapshot);
    drop(connector);

    let restarted = mysql_connector(&config)?;
    restarted
        .delivery_discovery(discovery_context(
            DeliveryType::BatchAndStream,
            &cancellation,
        ))
        .await?;
    let error = restarted
        .prepare_execution(execution_context(
            DeliveryType::BatchAndStream,
            &durable.context,
            &cancellation,
        ))
        .await
        .expect_err("interrupted snapshot was silently recycled");
    let diagnostic = format!("{error:#}").to_lowercase();
    assert!(diagnostic.contains("snapshot"), "{diagnostic}");
    assert!(
        diagnostic.contains("destination may contain rows")
            || diagnostic.contains("manual")
            || diagnostic.contains("reset"),
        "{diagnostic}"
    );
    assert_eq!(durable.local.snapshot(), interrupted_state);
    assert_eq!(
        connector_phases(&restarted, &preview)?,
        vec![SourcePhase::Snapshot, SourcePhase::Stream]
    );
    admin.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn mariadb_rejects_replication_early_while_batch_snapshot_remains_supported(
) -> anyhow::Result<()> {
    let fixture = MySqlFixture::mariadb().await?;
    let mut admin = fixture.connection().await?;
    exec_all(
        &mut admin,
        &[
            "CREATE TABLE mariadb_events (id BIGINT PRIMARY KEY, payload VARCHAR(255) NOT NULL) ENGINE=InnoDB",
            "INSERT INTO mariadb_events VALUES (1, 'batch-supported')",
        ],
    )
    .await?;
    let cancellation = CancellationToken::new();

    let batch_config = source_yaml(&fixture.source, &["mariadb_events"], false);
    let batch = mysql_connector(&batch_config)?;
    let discovery = batch
        .delivery_discovery(discovery_context(DeliveryType::Batch, &cancellation))
        .await?;
    let partition = snapshot_partitions(&discovery)?[0];
    let durable = TestDurable::new("mariadb-batch");
    let mut source = batch
        .build_source(build_context(
            DeliveryType::Batch,
            SourcePhase::Snapshot,
            partition,
            &durable.context,
            &cancellation,
        ))
        .await?;
    let batch = read_nonempty(&mut source).await?;
    let ids = batch.tables[0]
        .batch
        .column_by_name("id")
        .expect("id column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("BIGINT Arrow column");
    assert_eq!(ids.len(), 1);
    assert_eq!(ids.value(0), 1);
    source.shutdown().await?;

    let replication_config = source_yaml(&fixture.source, &["mariadb_events"], true);
    for delivery_type in [DeliveryType::Stream, DeliveryType::BatchAndStream] {
        let durable = TestDurable::new(match delivery_type {
            DeliveryType::Stream => "mariadb-stream",
            DeliveryType::BatchAndStream => "mariadb-combined",
            DeliveryType::Batch => panic!("batch is not part of the replication test matrix"),
        });
        let error =
            reject_before_execution(&replication_config, delivery_type, &durable, &cancellation)
                .await?;
        let diagnostic = error.to_lowercase();
        assert!(diagnostic.contains("mariadb"), "{diagnostic}");
        assert!(
            diagnostic.contains("unsupported") || diagnostic.contains("not supported"),
            "{diagnostic}"
        );
        assert!(durable.is_empty());
    }
    admin.disconnect().await?;
    Ok(())
}

async fn prepare_stream(
    config: &str,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<MySqlSourceConnector> {
    let connector = mysql_connector(config)?;
    connector
        .delivery_discovery(discovery_context(DeliveryType::Stream, cancellation))
        .await?;
    connector
        .prepare_execution(execution_context(
            DeliveryType::Stream,
            durable,
            cancellation,
        ))
        .await?;
    Ok(connector)
}

async fn build_stream(
    connector: &MySqlSourceConnector,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<Box<dyn Source>> {
    connector
        .build_source(build_context(
            DeliveryType::Stream,
            SourcePhase::Stream,
            0,
            durable,
            cancellation,
        ))
        .await
}

async fn reject_before_execution(
    config: &str,
    delivery_type: DeliveryType,
    durable: &TestDurable,
    cancellation: &CancellationToken,
) -> anyhow::Result<String> {
    let connector = mysql_connector(config)?;
    match connector
        .delivery_discovery(discovery_context(delivery_type, cancellation))
        .await
    {
        Err(error) => Ok(format!("{error:#}")),
        Ok(_) => connector
            .prepare_execution(execution_context(
                delivery_type,
                &durable.context,
                cancellation,
            ))
            .await
            .err()
            .map(|error| format!("{error:#}"))
            .context("invalid MySQL replication contract reached source execution"),
    }
}

async fn read_snapshot(
    connector: &MySqlSourceConnector,
    discovery: &DeliveryDiscovery,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> anyhow::Result<ObservedPhase> {
    let mut observed = ObservedPhase::default();
    for partition in snapshot_partitions(discovery)? {
        let mut source = connector
            .build_source(build_context(
                DeliveryType::BatchAndStream,
                SourcePhase::Snapshot,
                partition,
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
                    observe_tables(&mut observed, tables)?;
                    if let Some(marker) = commit_marker {
                        source.commit_offsets(&[marker]).await?;
                    }
                }
                SourceBatch::Finished => break,
                SourceBatch::Raw { .. } => anyhow::bail!("MySQL snapshot emitted raw data"),
            }
        }
        source.shutdown().await?;
    }
    observed.rows.sort();
    Ok(observed)
}

async fn read_one_snapshot_table(
    connector: &MySqlSourceConnector,
    discovery: &DeliveryDiscovery,
    durable: &DurableContext,
    cancellation: &CancellationToken,
    expected_table: &str,
) -> anyhow::Result<TableData> {
    let partitions = snapshot_partitions(discovery)?;
    anyhow::ensure!(
        partitions.len() == 1,
        "single-table MySQL fixture exposed {} snapshot partitions",
        partitions.len()
    );
    let mut source = connector
        .build_source(build_context(
            DeliveryType::BatchAndStream,
            SourcePhase::Snapshot,
            partitions[0],
            durable,
            cancellation,
        ))
        .await?;
    let table = match source.read_batch().await? {
        SourceBatch::Typed {
            tables,
            source_rows: 1,
            commit_marker,
            ..
        } => {
            if let Some(marker) = commit_marker {
                source.commit_offsets(&[marker]).await?;
            }
            take_only_table(tables, expected_table)?
        }
        SourceBatch::Typed { source_rows, .. } => {
            anyhow::bail!("type-parity snapshot returned {source_rows} rows instead of one")
        }
        SourceBatch::Raw { .. } | SourceBatch::Finished => {
            anyhow::bail!("type-parity snapshot returned raw or empty data")
        }
    };
    anyhow::ensure!(
        matches!(source.read_batch().await?, SourceBatch::Finished),
        "type-parity snapshot returned more than one batch"
    );
    source.shutdown().await?;
    Ok(table)
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
                } => {
                    if source_rows > 0 {
                        return Ok(TypedSourceBatch {
                            tables,
                            source_rows,
                            marker: commit_marker
                                .context("non-empty MySQL CDC batch omitted its commit marker")?,
                        });
                    }
                    if commit_marker.is_some() {
                        anyhow::bail!(
                            "an unexpected filtered GTID checkpoint preceded the selected transaction"
                        );
                    }
                }
                SourceBatch::Raw { .. } | SourceBatch::Finished => {
                    anyhow::bail!("MySQL replication returned raw or finite data")
                }
            }
        }
    })
    .await
    .context("timed out waiting for MySQL changes")?
}

async fn read_empty_checkpoint(source: &mut Box<dyn Source>) -> anyhow::Result<CommitMarker> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    source_rows: 0,
                    commit_marker: Some(marker),
                    ..
                } if tables.is_empty() => return Ok(marker),
                SourceBatch::Typed { source_rows: 0, .. } => {}
                SourceBatch::Typed { .. } => {
                    anyhow::bail!("filtered transaction unexpectedly emitted selected rows")
                }
                SourceBatch::Raw { .. } | SourceBatch::Finished => {
                    anyhow::bail!("MySQL replication returned raw or finite data")
                }
            }
        }
    })
    .await
    .context("timed out waiting for filtered-transaction checkpoint")?
}

fn observe_tables(observed: &mut ObservedPhase, tables: Vec<TableData>) -> anyhow::Result<()> {
    for table in tables {
        let table_name = table.table.to_string();
        if let Some(prior) = observed
            .schemas
            .insert(table_name.clone(), table.batch.schema())
        {
            anyhow::ensure!(
                prior == table.batch.schema(),
                "table '{table_name}' changed Arrow schema within one phase"
            );
        }
        let ids = array_by_name::<Int64Array>(&table, "id")?;
        let payloads = array_by_name::<StringArray>(&table, "payload")?;
        let operations = system_array::<StringArray>(&table, SystemColumnKind::ChangeOperation)?;
        let old_id = old_array::<Int64Array>(&table, "id")?;
        let old_payload = old_array::<StringArray>(&table, "payload")?;
        for index in 0..table.batch.num_rows() {
            anyhow::ensure!(!ids.is_null(index), "change row lost its primary key");
            anyhow::ensure!(!payloads.is_null(index), "change row lost its payload");
            observed.rows.push(ObservedRow {
                table: table_name.clone(),
                operation: operations.value(index).to_owned(),
                id: ids.value(index),
                payload: payloads.value(index).to_owned(),
                old_id: old_id
                    .filter(|array| !array.is_null(index))
                    .map(|array| array.value(index)),
                old_payload: old_payload
                    .filter(|array| !array.is_null(index))
                    .map(|array| array.value(index).to_owned()),
            });
        }
    }
    Ok(())
}

fn assert_same_deterministic_tables(expected: &[TableData], actual: &[TableData]) {
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

fn take_only_table(mut tables: Vec<TableData>, expected_table: &str) -> anyhow::Result<TableData> {
    anyhow::ensure!(
        tables.len() == 1,
        "expected one '{expected_table}' table batch, got {}",
        tables.len()
    );
    let table = tables.pop().context("one-table batch was empty")?;
    anyhow::ensure!(
        table.table.to_string() == expected_table,
        "expected table '{expected_table}', got '{}'",
        table.table
    );
    Ok(table)
}

fn assert_user_columns_equal(
    expected: &TableData,
    expected_row: usize,
    actual: &TableData,
    actual_row: usize,
    excluded_names: &[&str],
) -> anyhow::Result<()> {
    let expected_schema = expected.batch.schema();
    let actual_schema = actual.batch.schema();
    for (expected_index, expected_field) in expected_schema.fields().iter().enumerate() {
        if !is_mysql_user_current_field(expected_field)
            || excluded_names.contains(&expected_field.name().as_str())
        {
            continue;
        }
        let actual_index = actual_schema.index_of(expected_field.name())?;
        let actual_field = actual_schema.field(actual_index);
        anyhow::ensure!(
            expected_field.as_ref() == actual_field,
            "MySQL field '{}' changed between snapshot and binlog: expected {expected_field:?}, got {actual_field:?}",
            expected_field.name()
        );
        anyhow::ensure!(
            expected_field
                .metadata()
                .contains_key(META_ARROW_EXTENSION_NAME)
                && expected_field
                    .metadata()
                    .contains_key(META_ARROW_EXTENSION_METADATA),
            "MySQL user field '{}' omitted its physical extension metadata",
            expected_field.name()
        );
        let expected_value = expected
            .batch
            .column(expected_index)
            .slice(expected_row, 1)
            .to_data();
        let actual_value = actual
            .batch
            .column(actual_index)
            .slice(actual_row, 1)
            .to_data();
        anyhow::ensure!(
            expected_value == actual_value,
            "MySQL field '{}' changed value between snapshot and binlog: expected {expected_value:?}, got {actual_value:?}",
            expected_field.name()
        );
    }
    Ok(())
}

fn assert_mysql_extension(
    table: &TableData,
    column: &str,
    data_type: &DataType,
    extension_name: &str,
    expected_metadata: &[(&str, serde_json::Value)],
) -> anyhow::Result<()> {
    let schema = table.batch.schema();
    let field = schema.field_with_name(column)?;
    anyhow::ensure!(
        field.data_type() == data_type,
        "MySQL column '{column}' uses Arrow {:?}, expected {data_type:?}",
        field.data_type()
    );
    anyhow::ensure!(
        field
            .metadata()
            .get(META_ARROW_EXTENSION_NAME)
            .map(String::as_str)
            == Some(extension_name),
        "MySQL column '{column}' has the wrong Arrow extension name"
    );
    let payload = field
        .metadata()
        .get(META_ARROW_EXTENSION_METADATA)
        .with_context(|| format!("MySQL column '{column}' omitted extension metadata"))?;
    let payload: serde_json::Value = serde_json::from_str(payload)
        .with_context(|| format!("MySQL column '{column}' extension metadata is not JSON"))?;
    anyhow::ensure!(
        payload.get("version") == Some(&serde_json::Value::from(1_u64)),
        "MySQL column '{column}' has an unknown extension metadata version"
    );
    for (key, expected) in expected_metadata {
        anyhow::ensure!(
            payload.get(*key) == Some(expected),
            "MySQL column '{column}' extension metadata key '{key}' is {:?}, expected {expected:?}",
            payload.get(*key)
        );
    }
    Ok(())
}

fn assert_old_values_are_null_and_typed(table: &TableData, row: usize) -> anyhow::Result<()> {
    let schema = table.batch.schema();
    for current in schema
        .fields()
        .iter()
        .filter(|field| is_mysql_user_current_field(field))
    {
        let old_index = old_value_index(table, current.name())?;
        let old = schema.field(old_index);
        assert_old_field_matches_current(current, old)?;
        anyhow::ensure!(
            table.batch.column(old_index).is_null(row),
            "MySQL INSERT unexpectedly carried an old value for '{}'",
            current.name()
        );
    }
    Ok(())
}

fn assert_old_values_equal_current(
    expected_current: &TableData,
    expected_row: usize,
    actual: &TableData,
    actual_row: usize,
) -> anyhow::Result<()> {
    let expected_schema = expected_current.batch.schema();
    let actual_schema = actual.batch.schema();
    for (current_index, current) in expected_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| is_mysql_user_current_field(field))
    {
        let old_index = old_value_index(actual, current.name())?;
        let old = actual_schema.field(old_index);
        assert_old_field_matches_current(current, old)?;
        anyhow::ensure!(
            expected_current
                .batch
                .column(current_index)
                .slice(expected_row, 1)
                .to_data()
                == actual
                    .batch
                    .column(old_index)
                    .slice(actual_row, 1)
                    .to_data(),
            "MySQL old value for '{}' differs from the prior current value",
            current.name()
        );
    }
    Ok(())
}

fn assert_old_field_matches_current(
    current: &arrow::datatypes::Field,
    old: &arrow::datatypes::Field,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        current.data_type() == old.data_type(),
        "old value of '{}' changed Arrow storage type",
        current.name()
    );
    for key in [META_ARROW_EXTENSION_NAME, META_ARROW_EXTENSION_METADATA] {
        anyhow::ensure!(
            current.metadata().get(key) == old.metadata().get(key),
            "old value of '{}' changed extension metadata key '{key}'",
            current.name()
        );
    }
    anyhow::ensure!(
        old.metadata().get(META_OLD_VALUE_OF).map(String::as_str) == Some(current.name().as_str()),
        "old-value field is not paired with '{}'",
        current.name()
    );
    Ok(())
}

fn old_value_index(table: &TableData, current_name: &str) -> anyhow::Result<usize> {
    table
        .batch
        .schema()
        .fields()
        .iter()
        .position(|field| {
            field
                .metadata()
                .get(META_OLD_VALUE_OF)
                .is_some_and(|name| name == current_name)
        })
        .with_context(|| format!("missing old-value field for '{current_name}'"))
}

fn assert_operation(table: &TableData, row: usize, expected: &str) -> anyhow::Result<()> {
    let operations = system_array::<StringArray>(table, SystemColumnKind::ChangeOperation)?;
    anyhow::ensure!(
        !operations.is_null(row) && operations.value(row) == expected,
        "expected MySQL operation '{expected}', got {:?}",
        (!operations.is_null(row)).then(|| operations.value(row))
    );
    Ok(())
}

fn is_mysql_user_current_field(field: &arrow::datatypes::Field) -> bool {
    field.metadata().contains_key(META_ARROW_EXTENSION_NAME)
        && field.metadata().contains_key(META_ARROW_EXTENSION_METADATA)
        && !field.metadata().contains_key(META_SYSTEM_ROLE)
        && !field.metadata().contains_key(META_CHANGE_OPERATION)
        && !field.metadata().contains_key(META_OLD_VALUE_OF)
        && !field.metadata().contains_key(META_OLD_KEY_OF)
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

fn old_array<'a, T: arrow::array::Array + 'static>(
    table: &'a TableData,
    current_name: &str,
) -> anyhow::Result<Option<&'a T>> {
    let schema = table.batch.schema();
    let Some(index) = schema.fields().iter().position(|field| {
        field
            .metadata()
            .get(META_OLD_VALUE_OF)
            .is_some_and(|name| name == current_name)
    }) else {
        return Ok(None);
    };
    Ok(Some(
        table
            .batch
            .column(index)
            .as_any()
            .downcast_ref()
            .with_context(|| format!("old value of '{current_name}' has an unexpected type"))?,
    ))
}

fn transaction_identities(tables: &[TableData]) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut identities = Vec::new();
    for table in tables {
        let schema = table.batch.schema();
        let index = schema
            .fields()
            .iter()
            .position(|field| {
                field.metadata().get(META_SYSTEM_ROLE).map(String::as_str)
                    == Some(SYSTEM_ROLE_SOURCE_TRANSACTION_ID)
            })
            .context("MySQL change batch omitted its source transaction identity")?;
        let values = table
            .batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("MySQL source transaction identity is not binary")?;
        for row in 0..values.len() {
            anyhow::ensure!(
                !values.is_null(row),
                "MySQL change row has no source transaction identity"
            );
            identities.push(values.value(row).to_vec());
        }
    }
    anyhow::ensure!(
        !identities.is_empty(),
        "MySQL change batch had no transaction identities"
    );
    Ok(identities)
}

fn snapshot_partitions(discovery: &DeliveryDiscovery) -> anyhow::Result<Vec<i64>> {
    discovery
        .source_topology
        .static_partitions()
        .map(<[i64]>::to_vec)
        .context("MySQL snapshot returned dynamic worker lanes")
}

fn connector_phases(
    connector: &MySqlSourceConnector,
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<Vec<SourcePhase>> {
    Ok(connector
        .execution_phases(DeliveryType::BatchAndStream, discovery)?
        .into_iter()
        .map(|phase| phase.phase)
        .collect())
}

fn row(
    table: &str,
    operation: &str,
    id: i64,
    payload: &str,
    old_id: Option<i64>,
    old_payload: Option<&str>,
) -> ObservedRow {
    ObservedRow {
        table: table.to_owned(),
        operation: operation.to_owned(),
        id,
        payload: payload.to_owned(),
        old_id,
        old_payload: old_payload.map(str::to_owned),
    }
}

fn sorted_rows<const N: usize>(rows: [ObservedRow; N]) -> Vec<ObservedRow> {
    let mut rows = rows.into_iter().collect::<Vec<_>>();
    rows.sort();
    rows
}

fn mysql_connector(config: &str) -> anyhow::Result<MySqlSourceConnector> {
    MySqlSourceConnector::from_config(
        serde_yaml::from_str(config)?,
        Arc::new(MetricsRegistry::new()),
    )
}

fn source_yaml(
    connection: &MySqlConnectionConfig,
    tables: &[&str],
    replication_limits: bool,
) -> String {
    let tables = tables
        .iter()
        .map(|table| format!("  - name: {table}"))
        .collect::<Vec<_>>()
        .join("\n");
    let replication = if replication_limits {
        "replication:\n  max_events: 1024\n  poll_interval_ms: 10\n  bootstrap_timeout_ms: 10000\n"
    } else {
        ""
    };
    format!(
        "host: '{}'\nport: {}\ndatabase: {}\nusername: {}\npassword: {}\ntrusted_plaintext: true\nbatch_rows: 1\nread_protocol: binary\ntables:\n{tables}\n{replication}",
        connection.host,
        connection.port,
        connection.database,
        connection.username,
        connection.password,
    )
}

fn discovery_context(
    delivery_type: DeliveryType,
    cancellation: &CancellationToken,
) -> SourceDiscoveryContext {
    SourceDiscoveryContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type,
    }
}

fn execution_context(
    delivery_type: DeliveryType,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> SourceExecutionContext {
    execution_context_with_replay_identity(delivery_type, durable, cancellation, REPLAY_IDENTITY)
}

fn execution_context_with_replay_identity(
    delivery_type: DeliveryType,
    durable: &DurableContext,
    cancellation: &CancellationToken,
    replay_identity: &str,
) -> SourceExecutionContext {
    SourceExecutionContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: cancellation.child_token(),
        delivery_type,
        replay_identity: Some(Arc::from(replay_identity)),
        durable: durable.clone(),
    }
}

fn build_context(
    delivery_type: DeliveryType,
    phase: SourcePhase,
    partition_id: i64,
    durable: &DurableContext,
    cancellation: &CancellationToken,
) -> SourceBuildContext {
    SourceBuildContext {
        partition_id,
        delivery_type,
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
    tokio::time::timeout(Duration::from_mins(1), async {
        loop {
            match connect(config).await {
                Ok(connection) => return connection,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("MySQL-compatible container did not become ready"))
}

async fn exec_all(connection: &mut mysql_async::Conn, statements: &[&str]) -> anyhow::Result<()> {
    for statement in statements {
        connection.query_drop(*statement).await?;
    }
    Ok(())
}

async fn current_binary_log(connection: &mut mysql_async::Conn) -> anyhow::Result<String> {
    let row: mysql_async::Row = connection
        .query_first("SHOW BINARY LOG STATUS")
        .await?
        .context("SHOW BINARY LOG STATUS returned no row")?;
    row.get("File")
        .context("SHOW BINARY LOG STATUS omitted File")
}

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}
