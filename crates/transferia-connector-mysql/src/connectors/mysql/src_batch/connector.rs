use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

use super::config::{MySqlReadProtocol, MySqlSourceConfig, TableConfig};
use super::reader::{MySqlSnapshotMetadata, MySqlSource};
use crate::connectors::mysql::common::{
    connect, connect_with_max_allowed_packet, quote_identifier, validate_identifier,
};
use crate::connectors::mysql::src_batch_and_stream::{
    acquire_execution_lock, begin_locked_snapshot, inspect_mysql8_gtid_source,
    is_replication_safety_violation, replication_safety_violation, AuthoritativeColumnIdentity,
    AuthoritativeTableIdentity, MySqlBinlogBoundary, MySqlCollationPadding, MySqlColumnGeneration,
    MySqlColumnVisibility, MySqlExecutionLock, MySqlGtidState, MySqlSnapshotSession,
    MySqlSourceIdentity, SnapshotStreamPreparation, SnapshotStreamTracker,
};
use crate::connectors::mysql::src_stream::{
    inspect_existing_replication_offset, validate_replication_column_plan, MySqlBinlogPosition,
    MySqlReplicationSource,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_BINLOG_FILE,
    SYSTEM_ROLE_SOURCE_BINLOG_POSITION, SYSTEM_ROLE_SOURCE_BINLOG_ROW, SYSTEM_ROLE_SOURCE_DATABASE,
    SYSTEM_ROLE_SOURCE_GTID, SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_SERVER_ID,
    SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::{
    PreparedSourceExecution, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
    SourceExecutionContext, SourceExecutionPhase, SourcePhase,
};

pub const MYSQL_REPLICATION_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
    SystemColumnKind::ChangeOperation,
    SystemColumnKind::ChangedColumns,
];

const MYSQL_DECIMAL_EXTENSION_NAME: &str = "transferia.mysql.decimal";
const MYSQL_DATE_EXTENSION_NAME: &str = "transferia.mysql.date";
const MYSQL_DATETIME_EXTENSION_NAME: &str = "transferia.mysql.datetime";
const MYSQL_TIMESTAMP_EXTENSION_NAME: &str = "transferia.mysql.timestamp";
const MYSQL_TIME_EXTENSION_NAME: &str = "transferia.mysql.time";
const MYSQL_YEAR_EXTENSION_NAME: &str = "transferia.mysql.year";
const MYSQL_ENUM_EXTENSION_NAME: &str = "transferia.mysql.enum";
const MYSQL_SET_EXTENSION_NAME: &str = "transferia.mysql.set";
const MYSQL_TEXT_BYTES_EXTENSION_NAME: &str = "transferia.mysql.text_bytes";
const MYSQL_SIGNED_INTEGER_EXTENSION_NAME: &str = "transferia.mysql.signed_integer";
const MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME: &str = "transferia.mysql.unsigned_integer";
const MYSQL_FLOAT_EXTENSION_NAME: &str = "transferia.mysql.float";
const MYSQL_BINARY_EXTENSION_NAME: &str = "transferia.mysql.binary";
const MYSQL_TEXT_EXTENSION_NAME: &str = "transferia.mysql.text";

const MYSQL_SNAPSHOT_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

pub struct MySqlSourceMetadataColumn {
    pub(crate) name: &'static str,
    pub(crate) role: &'static str,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
}

pub const MYSQL_SOURCE_METADATA_COLUMNS: &[MySqlSourceMetadataColumn] = &[
    MySqlSourceMetadataColumn {
        name: "_system_source_database",
        role: SYSTEM_ROLE_SOURCE_DATABASE,
        data_type: DataType::Utf8,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_schema",
        role: SYSTEM_ROLE_SOURCE_SCHEMA,
        data_type: DataType::Utf8,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_table",
        role: SYSTEM_ROLE_SOURCE_TABLE,
        data_type: DataType::Utf8,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_transaction_id",
        role: SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
        data_type: DataType::Binary,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_server_id",
        role: SYSTEM_ROLE_SOURCE_SERVER_ID,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_gtid",
        role: SYSTEM_ROLE_SOURCE_GTID,
        data_type: DataType::Utf8,
        nullable: true,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_binlog_file",
        role: SYSTEM_ROLE_SOURCE_BINLOG_FILE,
        data_type: DataType::Utf8,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_binlog_position",
        role: SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_binlog_row",
        role: SYSTEM_ROLE_SOURCE_BINLOG_ROW,
        data_type: DataType::Int32,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_timestamp_ms",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_timestamp_us",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_timestamp_ns",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_event_timestamp_ms",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_event_timestamp_us",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        data_type: DataType::Int64,
        nullable: false,
    },
    MySqlSourceMetadataColumn {
        name: "_system_event_timestamp_ns",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
        data_type: DataType::Int64,
        nullable: false,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MySqlColumnKind {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Binary,
    Utf8,
    TextBytes,
    Json,
    DecimalText,
    DateText,
    DateTimeText,
    TimestampText,
    TimeText,
    YearText,
    EnumOrdinal,
    SetBits,
}

impl MySqlColumnKind {
    pub(crate) const fn arrow_type(self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::UInt8 => DataType::UInt8,
            Self::Int16 => DataType::Int16,
            Self::UInt16 | Self::EnumOrdinal => DataType::UInt16,
            Self::Int32 => DataType::Int32,
            Self::UInt32 => DataType::UInt32,
            Self::Int64 => DataType::Int64,
            Self::UInt64 | Self::SetBits => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Binary | Self::TextBytes => DataType::Binary,
            Self::Utf8
            | Self::Json
            | Self::DecimalText
            | Self::DateText
            | Self::DateTimeText
            | Self::TimestampText
            | Self::TimeText
            | Self::YearText => DataType::Utf8,
        }
    }

    pub(crate) const fn arrow_extension_name(self) -> &'static str {
        match self {
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 => {
                MYSQL_SIGNED_INTEGER_EXTENSION_NAME
            }
            Self::UInt8 | Self::UInt16 | Self::UInt32 | Self::UInt64 => {
                MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME
            }
            Self::Float32 | Self::Float64 => MYSQL_FLOAT_EXTENSION_NAME,
            Self::Binary => MYSQL_BINARY_EXTENSION_NAME,
            Self::Utf8 => MYSQL_TEXT_EXTENSION_NAME,
            Self::TextBytes => MYSQL_TEXT_BYTES_EXTENSION_NAME,
            Self::Json => ARROW_JSON_EXTENSION_NAME,
            Self::DecimalText => MYSQL_DECIMAL_EXTENSION_NAME,
            Self::DateText => MYSQL_DATE_EXTENSION_NAME,
            Self::DateTimeText => MYSQL_DATETIME_EXTENSION_NAME,
            Self::TimestampText => MYSQL_TIMESTAMP_EXTENSION_NAME,
            Self::TimeText => MYSQL_TIME_EXTENSION_NAME,
            Self::YearText => MYSQL_YEAR_EXTENSION_NAME,
            Self::EnumOrdinal => MYSQL_ENUM_EXTENSION_NAME,
            Self::SetBits => MYSQL_SET_EXTENSION_NAME,
        }
    }
}

#[derive(Clone)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "the flags preserve independent authoritative MySQL column modifiers"
)]
pub struct ColumnPlan {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) kind: MySqlColumnKind,
    pub(crate) unsigned: bool,
    pub(crate) zerofill: bool,
    pub(crate) auto_increment: bool,
    pub(crate) nullable: bool,
    pub(crate) primary_key: bool,
    pub(crate) character_maximum_length: Option<usize>,
    pub(crate) character_octet_length: Option<usize>,
    pub(crate) numeric_precision: Option<u64>,
    pub(crate) numeric_scale: Option<u64>,
    pub(crate) datetime_precision: Option<u64>,
    pub(crate) max_length: Option<usize>,
    pub(crate) expression: String,
    pub(crate) column_type: String,
    pub(crate) character_set: Option<String>,
    pub(crate) collation: Option<String>,
    pub(crate) collation_id: Option<u16>,
    pub(crate) collation_padding: Option<MySqlCollationPadding>,
    pub(crate) enum_set_values: Option<Vec<String>>,
    pub(crate) srs_id: Option<u32>,
    pub(crate) visibility: MySqlColumnVisibility,
    pub(crate) generation: MySqlColumnGeneration,
    pub(crate) extra: String,
    pub(crate) generation_expression: Option<String>,
    pub(crate) primary_key_ordinal: Option<u64>,
    pub(crate) primary_key_prefix_length: Option<u64>,
    pub(crate) primary_key_direction: Option<String>,
}

#[derive(serde::Serialize)]
struct MySqlArrowExtensionMetadata<'a> {
    version: u8,
    data_type: &'a str,
    column_type: &'a str,
    unsigned: bool,
    zerofill: bool,
    auto_increment: bool,
    character_maximum_length: Option<usize>,
    character_octet_length: Option<usize>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
    datetime_precision: Option<u64>,
    character_set: Option<&'a str>,
    collation: Option<&'a str>,
    collation_id: Option<u16>,
    collation_padding: Option<MySqlCollationPadding>,
    enum_set_values: Option<&'a [String]>,
    srs_id: Option<u32>,
    visibility: MySqlColumnVisibility,
    generation: MySqlColumnGeneration,
    extra: &'a str,
    generation_expression: Option<&'a str>,
    primary_key_ordinal: Option<u64>,
    primary_key_prefix_length: Option<u64>,
    primary_key_direction: Option<&'a str>,
}

impl ColumnPlan {
    pub(crate) fn arrow_extension_metadata(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&MySqlArrowExtensionMetadata {
            version: 1,
            data_type: &self.data_type,
            column_type: &self.column_type,
            unsigned: self.unsigned,
            zerofill: self.zerofill,
            auto_increment: self.auto_increment,
            character_maximum_length: self.character_maximum_length,
            character_octet_length: self.character_octet_length,
            numeric_precision: self.numeric_precision,
            numeric_scale: self.numeric_scale,
            datetime_precision: self.datetime_precision,
            character_set: self.character_set.as_deref(),
            collation: self.collation.as_deref(),
            collation_id: self.collation_id,
            collation_padding: self.collation_padding,
            enum_set_values: self.enum_set_values.as_deref(),
            srs_id: self.srs_id,
            visibility: self.visibility,
            generation: self.generation,
            extra: &self.extra,
            generation_expression: self.generation_expression.as_deref(),
            primary_key_ordinal: self.primary_key_ordinal,
            primary_key_prefix_length: self.primary_key_prefix_length,
            primary_key_direction: self.primary_key_direction.as_deref(),
        })?)
    }
}

#[derive(Clone)]
pub struct DiscoveredTable {
    pub(crate) config: TableConfig,
    pub(crate) schema: DatasetSchema,
    pub(crate) columns: Vec<ColumnPlan>,
    pub(crate) engine: String,
}

pub struct MySqlSourceConnector {
    delivery_type: std::sync::OnceLock<DeliveryType>,
    config: MySqlSourceConfig,
    resolved_tables: tokio::sync::OnceCell<Vec<TableConfig>>,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    snapshot_stream: tokio::sync::OnceCell<Arc<MySqlSnapshotStreamExecution>>,
    stream: tokio::sync::OnceCell<Arc<MySqlStreamExecution>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

struct MySqlSnapshotStreamExecution {
    replay_identity: Arc<str>,
    tables: Arc<Vec<DiscoveredTable>>,
    source_identity: MySqlSourceIdentity,
    authoritative_tables: Vec<AuthoritativeTableIdentity>,
    tracker: tokio::sync::Mutex<SnapshotStreamTracker>,
    boundary: MySqlBinlogBoundary,
    start_boundary: Mutex<Option<MySqlBinlogBoundary>>,
    snapshot_sessions: Mutex<Vec<Option<MySqlSnapshotSession>>>,
    execution_lock: tokio::sync::Mutex<Option<MySqlExecutionLock>>,
}

struct MySqlStreamExecution {
    replay_identity: Arc<str>,
    tables: Arc<Vec<DiscoveredTable>>,
    source_identity: MySqlSourceIdentity,
    authoritative_tables: Vec<AuthoritativeTableIdentity>,
    exact_start_boundary: Option<MySqlBinlogBoundary>,
    execution_lock: tokio::sync::Mutex<Option<MySqlExecutionLock>>,
}

impl MySqlSourceConnector {
    pub fn from_config(
        config: MySqlSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            delivery_type: std::sync::OnceLock::new(),
            config,
            resolved_tables: tokio::sync::OnceCell::new(),
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            snapshot_stream: tokio::sync::OnceCell::new(),
            stream: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    fn bind_mode(&self, mode: DeliveryType) -> anyhow::Result<()> {
        anyhow::ensure!(
            *self.delivery_type.get_or_init(|| mode) == mode,
            "MySQL connector cannot be reused across delivery modes"
        );
        Ok(())
    }

    fn replication_enabled(&self) -> bool {
        matches!(
            self.delivery_type.get(),
            Some(DeliveryType::Stream | DeliveryType::BatchAndStream)
        )
    }

    async fn discovered_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async { self.load_discovered_tables().await.map(Arc::new) })
            .await
            .map(Arc::clone)
    }

    async fn resolved_tables(&self) -> anyhow::Result<&[TableConfig]> {
        self.resolved_tables.get_or_try_init(|| async {
            let catalog = crate::connectors::mysql::common::list_tables(&self.config.connection).await?;
            Ok(self.config.resolve_tables(catalog)?.into_iter().map(|table| TableConfig {
                database: table.namespace,
                name: table.name,
            }).collect())
        }).await.map(Vec::as_slice)
    }

    async fn load_discovered_tables(&self) -> anyhow::Result<Vec<DiscoveredTable>> {
        self.load_selected_tables(self.resolved_tables().await?).await
    }

    async fn load_selected_tables(&self, selected: &[TableConfig]) -> anyhow::Result<Vec<DiscoveredTable>> {
        // Also validate restored snapshot/replication membership before any
        // database request; never silently drop a durably recorded table.
        for table in selected {
            anyhow::ensure!(self.config.includes_database(&table.database),
                "MySQL Hide system tables excludes selected table {:?}. Disable the filter to retain this table; changing durably recorded membership requires a new delivery revision",
                transferia_registry::TableIdentity {
                    namespace: table.database.clone(), name: table.name.clone(),
                }.qualified_name());
        }
        let mut connection = observe_external_request(
            "mysql",
            "connect_source_discovery",
            connect(&self.config.connection),
        )
        .await?;
        let mut tables = Vec::with_capacity(selected.len());
        for table in selected {
            tables.push(
                discover_table(
                    &mut connection,
                    &table.database,
                    table.clone(),
                    self.replication_enabled(),
                    self.config.read_protocol,
                )
                .await?,
            );
        }
        observe_mysql_request("disconnect_source_discovery", connection.disconnect()).await?;
        Ok(tables)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the retry handoff revalidates every durable and source-side replication identity before transferring the lock-owned connection"
    )]
    async fn acquire_stream_handoff(
        &self,
        expected_source: &MySqlSourceIdentity,
        expected_authoritative_tables: &[AuthoritativeTableIdentity],
        exact_start_boundary: Option<&MySqlBinlogBoundary>,
        replay_identity: &str,
        durable: &transferia_registry::durable::DurableContext,
        cancellation: &tokio_util::sync::CancellationToken,
        execution_lock: &tokio::sync::Mutex<Option<MySqlExecutionLock>>,
    ) -> anyhow::Result<(Conn, MySqlGtidState)> {
        let replication = &self.config.replication.for_delivery(&durable.delivery_id)?;
        let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
        let preflight =
            inspect_mysql8_gtid_source(&self.config.connection, timeout, cancellation).await?;
        if &preflight.source != expected_source {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL source identity changed before replication stream handoff"
            )));
        }
        let selected = expected_authoritative_tables.iter().map(|table| TableConfig {
            database: table.database.clone(), name: table.table.clone(),
        }).collect::<Vec<_>>();
        let current_tables = self.load_selected_tables(&selected).await?;
        let current_authoritative_tables =
            authoritative_table_identities(&current_tables);
        if current_authoritative_tables != expected_authoritative_tables {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL authoritative table identity changed before replication stream handoff"
            )));
        }

        let prepared_lock = execution_lock.lock().await.take();
        let mut prepared_lock = match prepared_lock {
            Some(prepared_lock) => prepared_lock,
            None => {
                acquire_execution_lock(
                    &self.config.connection,
                    replication.server_id,
                    &preflight,
                    timeout,
                    timeout,
                    cancellation,
                )
                .await?
            }
        };
        let gtid_state = prepared_lock.read_gtid_state(timeout, cancellation).await?;
        let existing_position = inspect_existing_replication_offset(
            replication,
            expected_source,
            expected_authoritative_tables,
            durable,
            exact_start_boundary,
            &gtid_state.executed,
            &gtid_state.purged,
            replay_identity,
        )
        .await?;
        if existing_position.is_none() {
            boundary_position(exact_start_boundary.ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL replication has neither a durable offset nor an exact start boundary"
                ))
            })?)?;
        }
        let connection = prepared_lock.into_connection()?;
        Ok((connection, gtid_state))
    }

    fn counters(&self, partition: i64) -> Arc<SourceCounters> {
        Arc::clone(
            self.counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(partition)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }

    async fn prepare_snapshot_stream(
        &self,
        durable: transferia_registry::durable::DurableContext,
        cancellation: tokio_util::sync::CancellationToken,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<Arc<MySqlSnapshotStreamExecution>> {
        self.snapshot_stream
            .get_or_try_init(|| async move {
                let replication = &self.config.replication.for_delivery(&durable.delivery_id)?;
                let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
                let preflight = inspect_mysql8_gtid_source(
                    &self.config.connection,
                    timeout,
                    &cancellation,
                )
                .await?;
                let recorded = SnapshotStreamTracker::recorded_tables(
                    replication.server_id, &preflight.source, &durable, &replay_identity,
                ).await?;
                let selected = match recorded {
                    Some(tables) => tables,
                    None => self.resolved_tables().await?.to_vec(),
                };
                let preview_tables = Arc::new(self.load_selected_tables(&selected).await?);
                let preparation = SnapshotStreamTracker::claim_or_resume(
                    replication.server_id,
                    &selected,
                    &preflight.source,
                    durable.clone(),
                    &replay_identity,
                )
                .await?;
                let authoritative = authoritative_table_identities(
                    &preview_tables,
                );
                let (tracker, boundary, sessions, execution_lock, streaming) = match preparation {
                    SnapshotStreamPreparation::Create(mut tracker) => {
                        let bootstrap = begin_locked_snapshot(
                            &self.config.connection,
                            &selected,
                            &self.config,
                            replication.server_id,
                            &preflight,
                            self.config.max_row_bytes,
                            timeout,
                            timeout,
                            timeout,
                            &cancellation,
                        )
                        .await?;
                        if bootstrap.source() != &preflight.source
                            || bootstrap.authoritative_tables() != authoritative
                        {
                            bootstrap.abort(timeout).await?;
                            return Err(replication_safety_violation(anyhow::anyhow!(
                                "MySQL table schema changed between discovery and the exact snapshot boundary"
                            )));
                        }
                        let prepared = bootstrap
                            .persist_and_unlock(
                                &mut tracker,
                                timeout,
                                timeout,
                                &cancellation,
                            )
                            .await?;
                        (
                            tracker,
                            prepared.boundary,
                            prepared.sessions.into_iter().map(Some).collect(),
                            prepared.execution_lock,
                            false,
                        )
                    }
                    SnapshotStreamPreparation::Streaming {
                        tracker,
                        start_boundary,
                    } => {
                        tracker.validate_authoritative_tables(&authoritative)?;
                        let mut execution_lock = acquire_execution_lock(
                            &self.config.connection,
                            replication.server_id,
                            &preflight,
                            timeout,
                            timeout,
                            &cancellation,
                        )
                        .await?;
                        let gtid_state = execution_lock
                            .read_gtid_state(timeout, &cancellation)
                            .await?;
                        let membership = crate::connectors::mysql::src_stream::inspect_replication_membership(
                            replication, &preflight.source, &durable, &replay_identity,
                        ).await?.unwrap_or_else(|| authoritative.clone());
                        let _resume_position = inspect_existing_replication_offset(
                            replication,
                            &preflight.source,
                            &membership,
                            &durable,
                            Some(&start_boundary),
                            &gtid_state.executed,
                            &gtid_state.purged,
                            &replay_identity,
                        )
                        .await?
                        .unwrap_or(boundary_position(&start_boundary)?);
                        (
                            tracker,
                            start_boundary.clone(),
                            Vec::new(),
                            execution_lock,
                            true,
                        )
                    }
                };
                Ok::<_, anyhow::Error>(Arc::new(MySqlSnapshotStreamExecution {
                    replay_identity,
                    tables: preview_tables,
                    source_identity: preflight.source,
                    authoritative_tables: authoritative,
                    tracker: tokio::sync::Mutex::new(tracker),
                    boundary: boundary.clone(),
                    start_boundary: Mutex::new(streaming.then_some(boundary)),
                    snapshot_sessions: Mutex::new(sessions),
                    execution_lock: tokio::sync::Mutex::new(Some(execution_lock)),
                }))
            })
            .await
            .map(Arc::clone)
    }

    async fn prepare_stream(
        &self,
        durable: transferia_registry::durable::DurableContext,
        cancellation: tokio_util::sync::CancellationToken,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<Arc<MySqlStreamExecution>> {
        self.stream
            .get_or_try_init(|| async move {
                let replication = &self.config.replication.for_delivery(&durable.delivery_id)?;
                let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
                let preflight =
                    inspect_mysql8_gtid_source(&self.config.connection, timeout, &cancellation)
                        .await?;
                let recorded = crate::connectors::mysql::src_stream::inspect_replication_membership(
                    replication, &preflight.source, &durable, &replay_identity,
                ).await?;
                let tables = Arc::new(if let Some(recorded) = recorded {
                    let selection = self.config.tables.compile()?;
                    for table in &recorded {
                        let identity = transferia_registry::TableIdentity {
                            namespace: table.database.clone(), name: table.table.clone(),
                        };
                        let matches = selection.classify(&identity);
                        anyhow::ensure!(matches.selected_by.len() == 1 && matches.issues.is_empty(),
                            "MySQL table rules no longer unambiguously select durably recorded table {:?}; start a new delivery revision instead of changing running membership", identity.qualified_name());
                    }
                    let selected = recorded.iter().map(|table| TableConfig {
                        database: table.database.clone(), name: table.table.clone(),
                    }).collect::<Vec<_>>();
                    self.load_selected_tables(&selected).await?
                } else {
                    self.load_discovered_tables().await?
                });
                let authoritative_tables = authoritative_table_identities(&tables);
                let mut execution_lock = acquire_execution_lock(
                    &self.config.connection,
                    replication.server_id,
                    &preflight,
                    timeout,
                    timeout,
                    &cancellation,
                )
                .await?;
                let gtid_state = execution_lock
                    .read_gtid_state(timeout, &cancellation)
                    .await?;
                let existing = inspect_existing_replication_offset(
                    replication,
                    &preflight.source,
                    &authoritative_tables,
                    &durable,
                    None,
                    &gtid_state.executed,
                    &gtid_state.purged,
                    &replay_identity,
                )
                .await?;
                let exact_start_boundary = match existing {
                    Some(_) => None,
                    None => {
                        let boundary = execution_lock
                            .capture_boundary(
                                &preflight,
                                self.resolved_tables().await?,
                                &self.config,
                                &authoritative_tables,
                                timeout,
                                &cancellation,
                            )
                            .await?;
                        Some(boundary)
                    }
                };
                Ok::<_, anyhow::Error>(Arc::new(MySqlStreamExecution {
                    replay_identity,
                    tables,
                    source_identity: preflight.source,
                    authoritative_tables,
                    exact_start_boundary,
                    execution_lock: tokio::sync::Mutex::new(Some(execution_lock)),
                }))
            })
            .await
            .map(Arc::clone)
    }
}

impl SourceConnector for MySqlSourceConnector {
    fn can_add_datasets(&self, delivery_type: DeliveryType) -> bool {
        delivery_type != DeliveryType::Batch
            && self.config.new_tables == super::NewTables::IncludeAutomatically
    }

    fn compatibility(
        &self,
        delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::MySql(SourceDescriptor {
            behavior: if delivery_type == DeliveryType::Batch {
                SourceBehavior::FiniteAppendOnlyRows
            } else {
                SourceBehavior::ChangelogRows
            },
            delivery_modes: SourceDeliveryModes::BATCH_AND_STREAM,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let SourceDiscoveryContext {
                request,
                cancellation,
                delivery_type,
            } = context;
            self.bind_mode(delivery_type)?;
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("MySQL discovery cancelled"),
                tables = self.discovered_tables() => tables?,
            };
            build_delivery_discovery(self.replication_enabled(), delivery_type, request, &tables)
        })
    }

    fn prepare_execution(
        &self,
        context: SourceExecutionContext,
    ) -> BoxFuture<'_, anyhow::Result<Option<PreparedSourceExecution>>> {
        Box::pin(async move {
            self.bind_mode(context.delivery_type)?;
            if !self.replication_enabled() {
                return Ok(None);
            }
            let replay_identity = require_replication_replay_identity(context.replay_identity)
                .map_err(classify_replication_error)?;
            anyhow::ensure!(
                context.delivery_type != DeliveryType::Batch,
                "MySQL replication configuration does not support batch-only delivery"
            );
            if context.delivery_type == DeliveryType::Stream {
                let execution = self
                    .prepare_stream(
                        context.durable,
                        context.cancellation,
                        Arc::clone(&replay_identity),
                    )
                    .await
                    .map_err(classify_replication_error)?;
                let discovery = build_delivery_discovery(
                    true,
                    DeliveryType::Stream,
                    context.request,
                    &execution.tables,
                )?;
                let remaining_phases = self.execution_phases(DeliveryType::Stream, &discovery)?;
                return Ok(Some(PreparedSourceExecution {
                    discovery,
                    remaining_phases,
                }));
            }
            let execution = self
                .prepare_snapshot_stream(
                    context.durable,
                    context.cancellation,
                    Arc::clone(&replay_identity),
                )
                .await
                .map_err(classify_replication_error)?;
            let discovery = build_delivery_discovery(
                true,
                DeliveryType::BatchAndStream,
                context.request,
                &execution.tables,
            )?;
            let mut remaining = self.execution_phases(DeliveryType::BatchAndStream, &discovery)?;
            if execution
                .start_boundary
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
            {
                remaining.remove(0);
            }
            Ok(Some(PreparedSourceExecution {
                discovery,
                remaining_phases: remaining,
            }))
        })
    }

    fn execution_phases(
        &self,
        delivery_type: DeliveryType,
        discovery: &DeliveryDiscovery,
    ) -> anyhow::Result<Vec<SourceExecutionPhase>> {
        Ok(match delivery_type {
            DeliveryType::Batch => vec![SourceExecutionPhase {
                phase: SourcePhase::Snapshot,
                topology: discovery.source_topology.clone(),
                finite: true,
            }],
            DeliveryType::Stream => vec![SourceExecutionPhase {
                phase: SourcePhase::Stream,
                topology: discovery.source_topology.clone(),
                finite: false,
            }],
            DeliveryType::BatchAndStream => vec![
                SourceExecutionPhase {
                    phase: SourcePhase::Snapshot,
                    topology: discovery.source_topology.clone(),
                    finite: true,
                },
                SourceExecutionPhase {
                    phase: SourcePhase::Stream,
                    topology: SourceTopology::CoLocatedStaticPartitions(vec![0]),
                    finite: false,
                },
            ],
        })
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the execution lock mutex must stay held while the retained MySQL connection is queried"
    )]
    fn complete_execution_phase(
        &self,
        phase: SourcePhase,
        durable: transferia_registry::durable::DurableContext,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if phase != SourcePhase::Snapshot || !self.replication_enabled() {
                return Ok(());
            }
            let execution = self
                .snapshot_stream
                .get()
                .ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "MySQL batch_and_stream execution was not prepared"
                    ))
                })
                .map_err(classify_replication_error)?;
            if !execution
                .snapshot_sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .all(Option::is_none)
            {
                return Err(classify_replication_error(replication_safety_violation(
                    anyhow::anyhow!(
                        "MySQL snapshot phase completed before every prepared table session was consumed"
                    ),
                )));
            }
            let replication = &self.config.replication.for_delivery(&durable.delivery_id)?;
            let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
            let non_cancellable = tokio_util::sync::CancellationToken::new();
            let gtid_state = {
                let mut execution_lock = execution.execution_lock.lock().await;
                let execution_lock = execution_lock.as_mut().ok_or_else(|| {
                    classify_replication_error(replication_safety_violation(anyhow::anyhow!(
                        "MySQL replication execution lock is unavailable at the snapshot completion barrier"
                    )))
                })?;
                execution_lock
                    .read_gtid_state(timeout, &non_cancellable)
                    .await
                    .map_err(classify_replication_error)?
            };
            let _resume_position = inspect_existing_replication_offset(
                replication,
                &execution.source_identity,
                &execution.authoritative_tables,
                &durable,
                Some(&execution.boundary),
                &gtid_state.executed,
                &gtid_state.purged,
                &execution.replay_identity,
            )
            .await
            .map_err(classify_replication_error)?;
            let boundary = {
                let mut tracker = execution.tracker.lock().await;
                match tracker.streaming_boundary() {
                    Some(boundary) => boundary.clone(),
                    None => tracker
                        .mark_streaming()
                        .await
                        .map_err(classify_replication_error)?,
                }
            };
            *execution
                .start_boundary
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(boundary);
            Ok(())
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            self.bind_mode(context.delivery_type)?;
            let partition_id = context.partition_id;
            if context.phase == SourcePhase::Stream {
                anyhow::ensure!(
                    partition_id == 0,
                    "MySQL replication stream has only partition 0"
                );
                let replay_identity =
                    require_replication_replay_identity(context.replay_identity.clone())
                        .map_err(classify_replication_error)?;
                let (
                    expected_replay_identity,
                    tables,
                    source_identity,
                    authoritative_tables,
                    exact_start_boundary,
                    execution_lock,
                ) = match context.delivery_type {
                    DeliveryType::Stream => {
                        let execution = self.stream.get().ok_or_else(|| {
                            replication_safety_violation(anyhow::anyhow!(
                                "MySQL stream execution was not prepared"
                            ))
                        })?;
                        (
                            &execution.replay_identity,
                            &execution.tables,
                            &execution.source_identity,
                            &execution.authoritative_tables,
                            execution.exact_start_boundary.clone(),
                            &execution.execution_lock,
                        )
                    }
                    DeliveryType::BatchAndStream => {
                        let execution = self.snapshot_stream.get().ok_or_else(|| {
                            replication_safety_violation(anyhow::anyhow!(
                                "MySQL batch_and_stream execution was not prepared"
                            ))
                        })?;
                        let start_boundary = execution
                            .start_boundary
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone()
                            .ok_or_else(|| {
                                replication_safety_violation(anyhow::anyhow!(
                                    "MySQL replication stream cannot start before the snapshot completion barrier"
                                ))
                            })?;
                        (
                            &execution.replay_identity,
                            &execution.tables,
                            &execution.source_identity,
                            &execution.authoritative_tables,
                            Some(start_boundary),
                            &execution.execution_lock,
                        )
                    }
                    DeliveryType::Batch => {
                        return Err(classify_replication_error(replication_safety_violation(
                            anyhow::anyhow!(
                                "MySQL batch-only delivery cannot build a replication stream"
                            ),
                        )));
                    }
                };
                if expected_replay_identity.as_ref() != replay_identity.as_ref() {
                    return Err(classify_replication_error(replication_safety_violation(
                        anyhow::anyhow!(
                            "MySQL replication source was built under a different replay-affecting delivery configuration"
                        ),
                    )));
                }
                let replication = &self
                    .config
                    .replication
                    .for_delivery(&context.durable.delivery_id)?;
                // The prepared execution predates any admitted CREATEs. On
                // every retry, restore membership from the same checkpoint as
                // the binlog position, never from today's wildcard catalog.
                let recorded = crate::connectors::mysql::src_stream::inspect_replication_membership(
                    replication, source_identity, &context.durable, &replay_identity,
                ).await.map_err(classify_replication_error)?;
                let restored_tables;
                let restored_authoritative;
                let (tables, authoritative_tables) = if let Some(recorded) = recorded {
                    let selected = recorded.iter().map(|table| TableConfig {
                        database: table.database.clone(), name: table.table.clone(),
                    }).collect::<Vec<_>>();
                    restored_tables = self.load_selected_tables(&selected).await
                        .map_err(classify_replication_error)?;
                    restored_authoritative = authoritative_table_identities(&restored_tables);
                    if restored_authoritative != recorded {
                        return Err(classify_replication_error(replication_safety_violation(anyhow::anyhow!(
                            "MySQL durable table schemas changed before replication restart"))));
                    }
                    (restored_tables.as_slice(), &restored_authoritative)
                } else { (tables.as_slice(), authoritative_tables) };
                let (connection, gtid_state) = self
                    .acquire_stream_handoff(
                        source_identity,
                        authoritative_tables,
                        exact_start_boundary.as_ref(),
                        &replay_identity,
                        &context.durable,
                        &context.cancellation,
                        execution_lock,
                    )
                    .await
                    .map_err(classify_replication_error)?;
                let counters = self.counters(partition_id);
                self.metrics
                    .register_source(partition_id, Arc::clone(&counters));
                let source = MySqlReplicationSource::new(
                    connection,
                    replication.clone(),
                    self.config.clone(),
                    source_identity.clone(),
                    tables.to_vec(),
                    authoritative_tables.clone(),
                    counters,
                    context.cancellation,
                    context.durable,
                    context.memory,
                    exact_start_boundary,
                    gtid_state.executed,
                    gtid_state.purged,
                    replay_identity,
                )
                .await
                .map_err(classify_replication_error)?;
                return Ok(Box::new(source) as Box<dyn Source>);
            }
            let snapshot_stream = if context.delivery_type == DeliveryType::BatchAndStream {
                Some(self.snapshot_stream.get().ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "MySQL batch_and_stream execution was not prepared"
                    ))
                })?)
            } else {
                None
            };
            if let Some(execution) = snapshot_stream {
                let replay_identity = require_replication_replay_identity(context.replay_identity)
                    .map_err(classify_replication_error)?;
                if execution.replay_identity.as_ref() != replay_identity.as_ref() {
                    return Err(classify_replication_error(replication_safety_violation(
                        anyhow::anyhow!(
                            "MySQL batch_and_stream source was built under a different replay-affecting delivery configuration"
                        ),
                    )));
                }
                anyhow::ensure!(
                    context.phase == SourcePhase::Snapshot,
                    "MySQL batch_and_stream requested an invalid source phase"
                );
            } else {
                anyhow::ensure!(
                    !self.replication_enabled(),
                    "MySQL replication source execution is not prepared"
                );
            }
            let tables = match snapshot_stream {
                Some(execution) => Arc::clone(&execution.tables),
                None => self.discovered_tables().await?,
            };
            let table = tables
                .get(usize::try_from(partition_id)?)
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL source partition {partition_id} does not exist")
                })?
                .clone();
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            let (connection, snapshot_metadata) = match snapshot_stream {
                Some(execution) => {
                    let session = execution
                        .snapshot_sessions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_mut(usize::try_from(partition_id)?)
                        .and_then(Option::take)
                        .ok_or_else(|| {
                            replication_safety_violation(anyhow::anyhow!(
                                "MySQL prepared snapshot session {partition_id} is unavailable"
                            ))
                        })?;
                    let (session_table, _session_connection_id, session_max_row_bytes, connection) =
                        session.into_parts();
                    anyhow::ensure!(
                        session_table.name == table.config.name && session_table.database == table.config.database,
                        "MySQL prepared snapshot session belongs to a different table"
                    );
                    anyhow::ensure!(
                        session_max_row_bytes == self.config.max_row_bytes,
                        "MySQL prepared snapshot session uses a different max_row_bytes limit"
                    );
                    (
                        connection,
                        Some(MySqlSnapshotMetadata {
                            partition_id,
                            database: table.config.database.clone(),
                            table: table.config.name.clone(),
                            boundary: execution.boundary.clone(),
                        }),
                    )
                }
                None => (
                    observe_external_request(
                        "mysql",
                        "connect_snapshot_source",
                        connect_with_max_allowed_packet(
                            &self.config.connection,
                            self.config.max_row_bytes,
                        ),
                    )
                    .await?,
                    None,
                ),
            };
            let source = match snapshot_metadata {
                Some(snapshot_metadata) => {
                    MySqlSource::from_started_snapshot(
                        connection,
                        table.config.database.clone(),
                        table.config,
                        table.schema,
                        table.columns,
                        self.config.batch_rows,
                        self.config.batch_target_bytes,
                        self.config.max_row_bytes,
                        self.config.read_protocol,
                        counters,
                        context.memory,
                        Some(snapshot_metadata),
                    )
                    .await?
                }
                None => {
                    MySqlSource::new(
                        connection,
                        table.config.database.clone(),
                        table.config,
                        table.schema,
                        table.columns,
                        self.config.batch_rows,
                        self.config.batch_target_bytes,
                        self.config.max_row_bytes,
                        self.config.read_protocol,
                        counters,
                        context.memory,
                    )
                    .await?
                }
            };
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }

    fn build_speedtest_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        if context.delivery_type != DeliveryType::Batch {
            return Box::pin(async {
                anyhow::bail!(
                    "MySQL replication speedtest requires an isolated binlog execution boundary"
                )
            });
        }
        self.build_source(context)
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

pub(crate) fn build_delivery_discovery(
    replication: bool,
    delivery_type: DeliveryType,
    request: transferia_core::delivery::DeliveryDiscoveryRequest,
    tables: &[DiscoveredTable],
) -> anyhow::Result<DeliveryDiscovery> {
    let system_columns = if replication {
        MYSQL_REPLICATION_SYSTEM_COLUMNS
    } else {
        MYSQL_SNAPSHOT_SYSTEM_COLUMNS
    };
    let discovered_system_columns = system_columns
        .iter()
        .copied()
        .map(Into::into)
        .collect::<Vec<_>>();
    let datasets = tables
        .iter()
        .map(|table| {
            let mut incoming = table.schema.clone();
            if replication {
                for column in &mut incoming.columns {
                    column.nullable = true;
                }
                incoming.columns.extend(
                    table
                        .schema
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(index, column)| old_value_schema_column(index, column)),
                );
                incoming
                    .columns
                    .extend(MYSQL_SOURCE_METADATA_COLUMNS.iter().map(|column| {
                        SchemaColumn::new(
                            column.name.to_owned(),
                            column.data_type.clone(),
                            column.nullable,
                        )
                        .with_system_role(column.role)
                    }));
            }
            incoming.columns.extend(system_columns.iter().map(|kind| {
                SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)
            }));
            let mut stored = table.schema.clone();
            if request.keep_system_columns {
                stored.columns.extend(
                    system_columns
                        .iter()
                        .filter(|kind| {
                            !matches!(
                                kind,
                                SystemColumnKind::ChangeOperation
                                    | SystemColumnKind::ChangedColumns
                            )
                        })
                        .map(|kind| {
                            SchemaColumn::new(
                                kind.default_name().to_owned(),
                                kind.data_type(),
                                false,
                            )
                        }),
                );
            }
            DiscoveredDataset {
                namespace: Some(Arc::from(table.config.database.as_str())),
                update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                role: DatasetRole::Main,
                name: Arc::from(table.config.name.as_str()),
                incoming_schema: incoming,
                stored_schema: stored,
                system_columns: discovered_system_columns.clone(),
            }
        })
        .collect();
    let partitions = || {
        (0..tables.len())
            .map(i64::try_from)
            .collect::<Result<Vec<_>, _>>()
    };
    let source_topology = match (replication, delivery_type) {
        (false, DeliveryType::Batch) => SourceTopology::StaticPartitions(partitions()?),
        (true, DeliveryType::BatchAndStream) => {
            SourceTopology::CoLocatedStaticPartitions(partitions()?)
        }
        (true, DeliveryType::Stream) => SourceTopology::StaticPartitions(vec![0]),
        _ => anyhow::bail!(
            "MySQL source configuration does not support delivery type '{}'",
            delivery_type.label()
        ),
    };
    Ok(DeliveryDiscovery {
        source_name: Arc::from("mysql"),
        source_topology,
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: request.keep_system_columns,
        datasets,
        performance_advice: Vec::new(),
    })
}

pub(crate) fn authoritative_table_identities(
    tables: &[DiscoveredTable],
) -> Vec<AuthoritativeTableIdentity> {
    tables
        .iter()
        .map(|table| AuthoritativeTableIdentity {
            database: table.config.database.clone(),
            table: table.config.name.clone(),
            engine: table.engine.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| AuthoritativeColumnIdentity {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    column_type: column.column_type.clone(),
                    unsigned: column.unsigned,
                    zerofill: column.zerofill,
                    auto_increment: column.auto_increment,
                    nullable: column.nullable,
                    character_maximum_length: column.character_maximum_length,
                    character_octet_length: column.character_octet_length,
                    numeric_precision: column.numeric_precision,
                    numeric_scale: column.numeric_scale,
                    datetime_precision: column.datetime_precision,
                    character_set: column.character_set.clone(),
                    collation: column.collation.clone(),
                    collation_id: column.collation_id,
                    collation_padding: column.collation_padding,
                    enum_set_values: column.enum_set_values.clone(),
                    srs_id: column.srs_id,
                    visibility: column.visibility,
                    generation: column.generation,
                    extra: column.extra.clone(),
                    generation_expression: column.generation_expression.clone(),
                    primary_key_ordinal: column.primary_key_ordinal,
                    primary_key_prefix_length: column.primary_key_prefix_length,
                    primary_key_direction: column.primary_key_direction.clone(),
                })
                .collect(),
        })
        .collect()
}

fn boundary_position(boundary: &MySqlBinlogBoundary) -> anyhow::Result<MySqlBinlogPosition> {
    MySqlBinlogPosition::new(boundary.filename.as_bytes().to_vec(), boundary.position)
        .map_err(|error| replication_safety_violation(error.into()))
}

fn require_replication_replay_identity(
    replay_identity: Option<Arc<str>>,
) -> anyhow::Result<Arc<str>> {
    let replay_identity = replay_identity.ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL replication requires a non-secret replay identity bound to the complete replay-affecting delivery configuration"
        ))
    })?;
    if replay_identity.is_empty() {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL replication replay identity must not be empty"
        )));
    }
    Ok(replay_identity)
}

fn classify_replication_error(error: anyhow::Error) -> anyhow::Error {
    if is_replication_safety_violation(&error) {
        anyhow::Error::new(DataPlaneFailure::fatal(error))
    } else {
        error
    }
}

pub(crate) async fn discover_table(
    connection: &mut Conn,
    database: &str,
    table: TableConfig,
    replication: bool,
    read_protocol: MySqlReadProtocol,
) -> anyhow::Result<DiscoveredTable> {
    let table_identity: Option<(String, String, String)> = observe_mysql_request(
        "discover_table_identity",
        connection.exec_first(
            "SELECT ENGINE, TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'",
            (database, table.name.as_str()),
        ),
    )
    .await?;
    let (engine, actual_database, actual_table) = table_identity
        .ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL table '{}.{}' does not exist or is not a base table",
                database,
                table.name
            )
        })
        .map_err(|error| classify_discovery_contract_error(replication, error))?;
    if actual_database != database || actual_table != table.name {
        return Err(classify_discovery_contract_error(
            replication,
            anyhow::anyhow!(
                "MySQL resolved configured table '{}.{}' to '{}.{}'; replication and snapshot discovery require exact identifier identity",
                database,
                table.name,
                actual_database,
                actual_table
            ),
        ));
    }
    let mysql8 = connection.server_version().0 == 8;
    let (srs_id_projection, collation_id_projection, collation_padding_projection) = if mysql8 {
        (
            "c.SRS_ID",
            "col.ID AS COLLATION_ID",
            "col.PAD_ATTRIBUTE AS COLLATION_PADDING",
        )
    } else {
        // MariaDB snapshot discovery does not expose the MySQL 8 identity
        // fields used by FULL row-binlog metadata.
        (
            "NULL AS SRS_ID",
            "NULL AS COLLATION_ID",
            "NULL AS COLLATION_PADDING",
        )
    };
    let rows: Vec<Row> = observe_mysql_request(
        "discover_column_identities",
        connection.exec(
            format!(
                "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.IS_NULLABLE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, {collation_id_projection}, {collation_padding_projection}, c.EXTRA, c.GENERATION_EXPRESSION, c.CHARACTER_MAXIMUM_LENGTH, c.CHARACTER_OCTET_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.DATETIME_PRECISION, {srs_id_projection}, s.SEQ_IN_INDEX, s.SUB_PART, s.COLLATION FROM information_schema.COLUMNS AS c LEFT JOIN information_schema.COLLATIONS AS col ON col.COLLATION_NAME = c.COLLATION_NAME LEFT JOIN information_schema.STATISTICS AS s ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.INDEX_NAME = 'PRIMARY' AND s.COLUMN_NAME = c.COLUMN_NAME WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? ORDER BY c.ORDINAL_POSITION"
            ),
            (database, table.name.as_str()),
        ),
    )
    .await?;
    if rows.is_empty() {
        return Err(classify_discovery_contract_error(
            replication,
            anyhow::anyhow!(
                "MySQL table '{}.{}' does not exist or has no columns",
                database,
                table.name
            ),
        ));
    }
    let columns = rows
        .iter()
        .map(|row| column_plan(row, mysql8))
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|error| classify_discovery_contract_error(replication, error))?;
    validate_snapshot_read_protocol(read_protocol, &columns)
        .map_err(|error| classify_discovery_contract_error(replication, error))?;
    let schema = DatasetSchema::new(
        columns
            .iter()
            .map(|column| {
                Ok(SchemaColumn::new(
                    column.name.clone(),
                    column.kind.arrow_type(),
                    column.nullable,
                )
                .with_constraints(column.primary_key, false, column.max_length)
                .with_arrow_extension_metadata(
                    column.kind.arrow_extension_name(),
                    column.arrow_extension_metadata()?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
    );
    if replication {
        let validate = || -> anyhow::Result<()> {
            anyhow::ensure!(
                engine.eq_ignore_ascii_case("InnoDB"),
                "MySQL replication table '{}.{}' uses storage engine '{}'; replication requires InnoDB",
                database,
                table.name,
                engine
            );
            anyhow::ensure!(
                schema.columns.iter().any(|column| column.primary_key),
                "MySQL replication table '{}.{}' must have a primary key",
                database,
                table.name
            );
            for column in &columns {
                validate_replication_column_plan(column).map_err(|error| {
                    anyhow::anyhow!(
                        "MySQL replication table '{}.{}' column '{}' is unsupported: {error}",
                        database,
                        table.name,
                        column.name
                    )
                })?;
            }
            validate_reserved_replication_column_names(database, &table, &schema)
        };
        validate().map_err(replication_safety_violation)?;
    }
    Ok(DiscoveredTable {
        config: table,
        schema,
        columns,
        engine,
    })
}

fn classify_discovery_contract_error(replication: bool, error: anyhow::Error) -> anyhow::Error {
    if replication {
        replication_safety_violation(error)
    } else {
        error
    }
}

fn validate_reserved_replication_column_names(
    database: &str,
    table: &TableConfig,
    schema: &DatasetSchema,
) -> anyhow::Result<()> {
    let reserved = MYSQL_SOURCE_METADATA_COLUMNS
        .iter()
        .map(|metadata| metadata.name.to_owned())
        .chain(
            MYSQL_REPLICATION_SYSTEM_COLUMNS
                .iter()
                .map(|kind| kind.default_name().to_owned()),
        )
        .chain((0..schema.columns.len()).map(old_value_column_name))
        .collect::<Vec<_>>();
    for column in &schema.columns {
        for reserved in &reserved {
            anyhow::ensure!(
                column.name != *reserved,
                "MySQL table '{}.{}' column '{}' conflicts with a reserved CDC control column name",
                database,
                table.name,
                column.name
            );
        }
    }
    Ok(())
}

#[must_use]
pub fn old_value_column_name(index: usize) -> String {
    format!("_system_old_value_{index}")
}

#[must_use]
pub fn old_value_schema_column(index: usize, current: &SchemaColumn) -> SchemaColumn {
    let mut old_value = SchemaColumn::new(
        old_value_column_name(index),
        current.data_type.clone(),
        true,
    )
    .with_old_value_of(current.name.clone());
    old_value.arrow_extension_name = current.arrow_extension_name;
    old_value
        .arrow_extension_metadata
        .clone_from(&current.arrow_extension_metadata);
    old_value
}

fn column_plan(row: &Row, require_collation_padding: bool) -> anyhow::Result<ColumnPlan> {
    let name = required::<String>(row, "COLUMN_NAME")?;
    validate_identifier("column", &name)?;
    let data_type = required::<String>(row, "DATA_TYPE")?.to_ascii_lowercase();
    let column_type = required::<String>(row, "COLUMN_TYPE")?;
    let nullable = match required::<String>(row, "IS_NULLABLE")?.as_str() {
        "YES" => true,
        "NO" => false,
        value => anyhow::bail!(
            "MySQL metadata returned invalid nullability '{value}' for column '{name}'"
        ),
    };
    let primary_key_ordinal = required::<Option<u64>>(row, "SEQ_IN_INDEX")?;
    let primary_key = primary_key_ordinal.is_some();
    let character_maximum_length = required::<Option<u64>>(row, "CHARACTER_MAXIMUM_LENGTH")?
        .map(usize::try_from)
        .transpose()?;
    let character_octet_length = required::<Option<u64>>(row, "CHARACTER_OCTET_LENGTH")?
        .map(usize::try_from)
        .transpose()?;
    let numeric_precision = required::<Option<u64>>(row, "NUMERIC_PRECISION")?;
    let numeric_scale = required::<Option<u64>>(row, "NUMERIC_SCALE")?;
    let datetime_precision = required::<Option<u64>>(row, "DATETIME_PRECISION")?;
    let character_set = required::<Option<String>>(row, "CHARACTER_SET_NAME")?;
    let collation = required::<Option<String>>(row, "COLLATION_NAME")?;
    let collation_id = required::<Option<u64>>(row, "COLLATION_ID")?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!(
                "MySQL metadata returned a collation id outside the binlog protocol range for column '{name}'"
            )
        })?;
    let collation_padding = required::<Option<String>>(row, "COLLATION_PADDING")?
        .map(|value| parse_collation_padding(&name, &value))
        .transpose()?;
    anyhow::ensure!(
        collation_identity_is_consistent(
            character_set.is_some(),
            collation.is_some(),
            collation_id.is_some(),
            collation_padding.is_some(),
            require_collation_padding,
        ),
        "MySQL metadata returned inconsistent character-set, collation, numeric collation-id, and padding identity for column '{name}'"
    );
    let extra = required::<String>(row, "EXTRA")?;
    let generation_expression = required::<Option<String>>(row, "GENERATION_EXPRESSION")?;
    let srs_id = required::<Option<u64>>(row, "SRS_ID")?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!(
                "MySQL metadata returned an SRS id outside the u32 range for column '{name}'"
            )
        })?;
    let primary_key_prefix_length = required::<Option<u64>>(row, "SUB_PART")?;
    let primary_key_direction = required::<Option<String>>(row, "COLLATION")?;
    let unsigned = has_column_type_modifier(&column_type, "unsigned");
    let zerofill = has_column_type_modifier(&column_type, "zerofill");
    let auto_increment = has_extra_modifier(&extra, "auto_increment");
    anyhow::ensure!(
        !zerofill || unsigned,
        "MySQL metadata returned ZEROFILL without UNSIGNED for column '{name}'"
    );
    let enum_set_values = parse_enum_set_values(&data_type, &column_type)?;
    let visibility = column_visibility(&extra);
    let generation = column_generation(&extra, generation_expression.as_deref())?;
    validate_structured_column_metadata(
        &name,
        &data_type,
        character_maximum_length,
        character_octet_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        character_set.as_deref(),
        srs_id,
        enum_set_values.as_deref(),
    )?;
    let kind = mysql_column_kind(&data_type, unsigned, character_set.as_deref()).map_err(
        |error| {
            anyhow::anyhow!(
                "unsupported MySQL/MariaDB column type '{data_type}' ({column_type}) for column '{name}': {error}"
            )
        },
    )?;
    let max_length = if matches!(kind, MySqlColumnKind::Binary | MySqlColumnKind::TextBytes) {
        character_octet_length
    } else if matches!(
        kind,
        MySqlColumnKind::EnumOrdinal | MySqlColumnKind::SetBits
    ) {
        None
    } else {
        character_maximum_length
    };
    let expression = snapshot_expression(&name, &data_type, kind);
    Ok(ColumnPlan {
        name,
        data_type,
        kind,
        unsigned,
        zerofill,
        auto_increment,
        nullable,
        primary_key,
        character_maximum_length,
        character_octet_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        max_length,
        expression,
        column_type,
        character_set,
        collation,
        collation_id,
        collation_padding,
        enum_set_values,
        srs_id,
        visibility,
        generation,
        extra,
        generation_expression,
        primary_key_ordinal,
        primary_key_prefix_length,
        primary_key_direction,
    })
}

#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the helper compares presence bits for four independently nullable information_schema fields"
)]
pub(super) const fn collation_identity_is_consistent(
    has_character_set: bool,
    has_collation: bool,
    has_collation_id: bool,
    has_collation_padding: bool,
    require_extended_identity: bool,
) -> bool {
    has_character_set == has_collation
        && (!has_collation_id || has_collation)
        && (!has_collation_padding || has_collation)
        && (!require_extended_identity
            || (has_collation == has_collation_id && has_collation == has_collation_padding))
}

pub fn mysql_column_kind(
    data_type: &str,
    unsigned: bool,
    character_set: Option<&str>,
) -> anyhow::Result<MySqlColumnKind> {
    let text_is_utf8 = || match character_set {
        Some("ascii" | "utf8mb3" | "utf8mb4") => Ok(true),
        Some(_) => Ok(false),
        None => anyhow::bail!("textual type has no declared character set"),
    };
    Ok(match data_type {
        "tinyint" => {
            if unsigned {
                MySqlColumnKind::UInt8
            } else {
                MySqlColumnKind::Int8
            }
        }
        "smallint" => {
            if unsigned {
                MySqlColumnKind::UInt16
            } else {
                MySqlColumnKind::Int16
            }
        }
        "mediumint" | "int" | "integer" => {
            if unsigned {
                MySqlColumnKind::UInt32
            } else {
                MySqlColumnKind::Int32
            }
        }
        "bigint" => {
            if unsigned {
                MySqlColumnKind::UInt64
            } else {
                MySqlColumnKind::Int64
            }
        }
        "float" => MySqlColumnKind::Float32,
        "double" | "real" => MySqlColumnKind::Float64,
        "bit" | "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
        | "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "vector" => MySqlColumnKind::Binary,
        "json" => MySqlColumnKind::Json,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "inet4"
        | "inet6" | "uuid" => {
            if text_is_utf8()? {
                MySqlColumnKind::Utf8
            } else {
                MySqlColumnKind::TextBytes
            }
        }
        "enum" => MySqlColumnKind::EnumOrdinal,
        "set" => MySqlColumnKind::SetBits,
        "decimal" | "numeric" => MySqlColumnKind::DecimalText,
        "date" => MySqlColumnKind::DateText,
        "datetime" => MySqlColumnKind::DateTimeText,
        "timestamp" => MySqlColumnKind::TimestampText,
        "time" => MySqlColumnKind::TimeText,
        "year" => MySqlColumnKind::YearText,
        unsupported => anyhow::bail!("unknown physical family '{unsupported}'"),
    })
}

pub fn snapshot_expression(name: &str, data_type: &str, kind: MySqlColumnKind) -> String {
    let quoted = quote_identifier(name);
    match kind {
        MySqlColumnKind::TextBytes if data_type == "char" => {
            format!("CAST(RTRIM({quoted}) AS BINARY) AS {quoted}")
        }
        MySqlColumnKind::TextBytes => format!("CAST({quoted} AS BINARY) AS {quoted}"),
        MySqlColumnKind::EnumOrdinal | MySqlColumnKind::SetBits => {
            format!("CAST({quoted} AS UNSIGNED) AS {quoted}")
        }
        MySqlColumnKind::DecimalText
        | MySqlColumnKind::DateText
        | MySqlColumnKind::DateTimeText
        | MySqlColumnKind::TimestampText
        | MySqlColumnKind::TimeText
        | MySqlColumnKind::YearText => format!("CAST({quoted} AS CHAR) AS {quoted}"),
        _ => quoted,
    }
}

fn parse_collation_padding(
    column_name: &str,
    value: &str,
) -> anyhow::Result<MySqlCollationPadding> {
    match value {
        "PAD SPACE" => Ok(MySqlCollationPadding::PadSpace),
        "NO PAD" => Ok(MySqlCollationPadding::NoPad),
        other => anyhow::bail!(
            "MySQL metadata returned unknown collation padding attribute '{other}' for column '{column_name}'"
        ),
    }
}

pub fn validate_snapshot_read_protocol(
    read_protocol: MySqlReadProtocol,
    columns: &[ColumnPlan],
) -> anyhow::Result<()> {
    if read_protocol == MySqlReadProtocol::Text {
        if let Some(column) = columns
            .iter()
            .find(|column| column.kind == MySqlColumnKind::Float32)
        {
            anyhow::bail!(
                "MySQL text snapshot protocol cannot preserve every exact IEEE-754 FLOAT value in column '{}'; use read_protocol='binary'",
                column.name
            );
        }
    }
    Ok(())
}

pub fn has_column_type_modifier(column_type: &str, modifier: &str) -> bool {
    column_type
        .split_ascii_whitespace()
        .any(|token| token.eq_ignore_ascii_case(modifier))
}

pub fn has_extra_modifier(extra: &str, modifier: &str) -> bool {
    extra
        .split_ascii_whitespace()
        .any(|token| token.eq_ignore_ascii_case(modifier))
}

pub fn column_visibility(extra: &str) -> MySqlColumnVisibility {
    if has_extra_modifier(extra, "invisible") {
        MySqlColumnVisibility::Invisible
    } else {
        MySqlColumnVisibility::Visible
    }
}

pub fn column_generation(
    extra: &str,
    generation_expression: Option<&str>,
) -> anyhow::Result<MySqlColumnGeneration> {
    let tokens = extra.split_ascii_whitespace().collect::<Vec<_>>();
    let has_pair = |storage: &str| {
        tokens.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case(storage) && pair[1].eq_ignore_ascii_case("generated")
        })
    };
    let virtual_generated = has_pair("virtual");
    let stored_generated = has_pair("stored");
    anyhow::ensure!(
        !(virtual_generated && stored_generated),
        "MySQL metadata marks one column as both VIRTUAL and STORED generated"
    );
    let expression_present = generation_expression.is_some_and(|value| !value.is_empty());
    anyhow::ensure!(
        expression_present == (virtual_generated || stored_generated),
        "MySQL generated-column expression and EXTRA modifiers disagree"
    );
    Ok(if virtual_generated {
        MySqlColumnGeneration::Virtual
    } else if stored_generated {
        MySqlColumnGeneration::Stored
    } else {
        MySqlColumnGeneration::None
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "validates the exact INFORMATION_SCHEMA column tuple"
)]
pub fn validate_structured_column_metadata(
    name: &str,
    data_type: &str,
    character_maximum_length: Option<usize>,
    character_octet_length: Option<usize>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
    datetime_precision: Option<u64>,
    character_set: Option<&str>,
    srs_id: Option<u32>,
    enum_set_values: Option<&[String]>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !data_type.is_empty(),
        "MySQL column '{name}' has an empty DATA_TYPE"
    );
    let is_textual = matches!(
        data_type,
        "char"
            | "varchar"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "enum"
            | "set"
            | "inet4"
            | "inet6"
            | "uuid"
    );
    anyhow::ensure!(
        is_textual == character_set.is_some(),
        "MySQL column '{name}' has inconsistent textual character-set metadata"
    );
    let has_character_capacity = matches!(
        data_type,
        "char"
            | "varchar"
            | "binary"
            | "varbinary"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
            | "enum"
            | "set"
            | "inet4"
            | "inet6"
            | "uuid"
    );
    if has_character_capacity {
        anyhow::ensure!(
            character_maximum_length.is_some() && character_octet_length.is_some(),
            "MySQL string column '{name}' omits character or octet length"
        );
    }
    if let (Some(characters), Some(octets)) = (character_maximum_length, character_octet_length) {
        anyhow::ensure!(
            octets >= characters,
            "MySQL column '{name}' has CHARACTER_OCTET_LENGTH below CHARACTER_MAXIMUM_LENGTH"
        );
    }
    anyhow::ensure!(
        numeric_scale.is_none() || numeric_precision.is_some(),
        "MySQL numeric column '{name}' has scale without precision"
    );
    let is_numeric = matches!(
        data_type,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "decimal"
            | "numeric"
            | "float"
            | "double"
            | "real"
            | "bit"
    );
    anyhow::ensure!(
        is_numeric == numeric_precision.is_some(),
        "MySQL column '{name}' has inconsistent numeric precision metadata"
    );
    anyhow::ensure!(
        !matches!(data_type, "decimal" | "numeric") || numeric_scale.is_some(),
        "MySQL exact numeric column '{name}' omits scale"
    );
    let has_fractional_seconds = matches!(data_type, "datetime" | "timestamp" | "time");
    anyhow::ensure!(
        has_fractional_seconds == datetime_precision.is_some(),
        "MySQL column '{name}' has inconsistent temporal precision metadata"
    );
    anyhow::ensure!(
        datetime_precision.is_none_or(|precision| precision <= 6),
        "MySQL temporal column '{name}' has fractional precision above 6"
    );
    let is_enum_set = matches!(data_type, "enum" | "set");
    anyhow::ensure!(
        is_enum_set == enum_set_values.is_some(),
        "MySQL column '{name}' has inconsistent ENUM/SET declaration metadata"
    );
    let is_spatial = matches!(
        data_type,
        "geometry"
            | "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    );
    anyhow::ensure!(
        srs_id.is_none() || is_spatial,
        "MySQL non-spatial column '{name}' has an SRS id"
    );
    Ok(())
}

pub fn parse_enum_set_values(
    data_type: &str,
    column_type: &str,
) -> anyhow::Result<Option<Vec<String>>> {
    if !matches!(data_type, "enum" | "set") {
        return Ok(None);
    }
    let prefix = format!("{data_type}(");
    anyhow::ensure!(
        column_type
            .get(..prefix.len())
            .is_some_and(|actual| actual.eq_ignore_ascii_case(&prefix))
            && column_type.ends_with(')'),
        "MySQL {data_type} COLUMN_TYPE has invalid framing"
    );
    let input = &column_type.as_bytes()[prefix.len()..column_type.len() - 1];
    let mut offset = 0;
    let mut values = Vec::new();
    while offset < input.len() {
        anyhow::ensure!(
            input[offset] == b'\'',
            "MySQL {data_type} COLUMN_TYPE member is not quoted"
        );
        offset += 1;
        let mut value = Vec::new();
        loop {
            let byte = *input.get(offset).ok_or_else(|| {
                anyhow::anyhow!("MySQL {data_type} COLUMN_TYPE has an unterminated member")
            })?;
            offset += 1;
            match byte {
                b'\'' if input.get(offset) == Some(&b'\'') => {
                    value.push(b'\'');
                    offset += 1;
                }
                b'\'' => break,
                b'\\' => {
                    let escaped = *input.get(offset).ok_or_else(|| {
                        anyhow::anyhow!("MySQL {data_type} COLUMN_TYPE ends in an escape")
                    })?;
                    offset += 1;
                    value.push(match escaped {
                        b'0' => 0,
                        b'b' => 8,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'Z' => 26,
                        escaped => escaped,
                    });
                }
                byte => value.push(byte),
            }
        }
        values.push(String::from_utf8(value).map_err(|error| {
            anyhow::anyhow!("MySQL {data_type} COLUMN_TYPE member is not valid UTF-8: {error}")
        })?);
        if offset == input.len() {
            break;
        }
        anyhow::ensure!(
            input[offset] == b',',
            "MySQL {data_type} COLUMN_TYPE members are not comma-separated"
        );
        offset += 1;
        anyhow::ensure!(
            offset < input.len(),
            "MySQL {data_type} COLUMN_TYPE has a trailing comma"
        );
    }
    anyhow::ensure!(
        !values.is_empty(),
        "MySQL {data_type} COLUMN_TYPE has no members"
    );
    Ok(Some(values))
}

fn required<T>(row: &Row, name: &str) -> anyhow::Result<T>
where
    T: mysql_async::prelude::FromValue,
{
    match row.get_opt(name) {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(anyhow::anyhow!(
            "MySQL metadata returned an invalid value for required column '{name}'"
        )),
        None => Err(anyhow::anyhow!(
            "MySQL metadata omitted required column '{name}'"
        )),
    }
}

async fn observe_mysql_request<T>(
    operation: &'static str,
    request: impl Future<Output = mysql_async::Result<T>>,
) -> anyhow::Result<T> {
    observe_external_request("mysql", operation, request)
        .await
        .map_err(anyhow::Error::from)
}
