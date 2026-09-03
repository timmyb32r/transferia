use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::{DataType, TimeUnit};
use futures_util::future::BoxFuture;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

use super::config::{MySqlSourceConfig, TableConfig};
use super::reader::{MySqlSnapshotMetadata, MySqlSource};
use crate::connectors::mysql::common::{
    connect, connect_with_max_allowed_packet, quote_identifier, validate_identifier,
};
use crate::connectors::mysql::src_batch_and_stream::{
    acquire_execution_lock, begin_locked_snapshot, inspect_mysql8_gtid_source,
    is_replication_safety_violation, replication_safety_violation,
    AuthoritativeColumnIdentity, AuthoritativeTableIdentity, MySqlBinlogBoundary,
    MySqlExecutionLock, MySqlGtidState, MySqlSnapshotSession, MySqlSourceIdentity,
    SnapshotStreamPreparation, SnapshotStreamTracker,
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
    SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_core::failure::DataPlaneFailure;
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
}

pub const MYSQL_SOURCE_METADATA_COLUMNS: &[MySqlSourceMetadataColumn] = &[
    MySqlSourceMetadataColumn {
        name: "_system_source_database",
        role: SYSTEM_ROLE_SOURCE_DATABASE,
        data_type: DataType::Utf8,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_schema",
        role: SYSTEM_ROLE_SOURCE_SCHEMA,
        data_type: DataType::Utf8,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_table",
        role: SYSTEM_ROLE_SOURCE_TABLE,
        data_type: DataType::Utf8,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_transaction_id",
        role: SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
        data_type: DataType::Binary,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_timestamp_ms",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
        data_type: DataType::Int64,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_timestamp_us",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
        data_type: DataType::Int64,
    },
    MySqlSourceMetadataColumn {
        name: "_system_source_timestamp_ns",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        data_type: DataType::Int64,
    },
    MySqlSourceMetadataColumn {
        name: "_system_event_timestamp_ms",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        data_type: DataType::Int64,
    },
    MySqlSourceMetadataColumn {
        name: "_system_event_timestamp_us",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        data_type: DataType::Int64,
    },
    MySqlSourceMetadataColumn {
        name: "_system_event_timestamp_ns",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
        data_type: DataType::Int64,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MySqlColumnKind {
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
    Json,
    Date,
    DateTime,
    TimestampUtc,
}

impl MySqlColumnKind {
    pub(crate) fn arrow_type(self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::UInt8 => DataType::UInt8,
            Self::Int16 => DataType::Int16,
            Self::UInt16 => DataType::UInt16,
            Self::Int32 => DataType::Int32,
            Self::UInt32 => DataType::UInt32,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Binary => DataType::Binary,
            Self::Utf8 | Self::Json => DataType::Utf8,
            Self::Date => DataType::Date32,
            Self::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::TimestampUtc => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ColumnPlan {
    pub(crate) name: String,
    pub(crate) kind: MySqlColumnKind,
    pub(crate) nullable: bool,
    pub(crate) primary_key: bool,
    pub(crate) max_length: Option<usize>,
    pub(crate) expression: String,
    pub(crate) column_type: String,
    pub(crate) character_set: Option<String>,
    pub(crate) collation: Option<String>,
    pub(crate) collation_id: Option<u16>,
    pub(crate) extra: String,
    pub(crate) generation_expression: Option<String>,
    pub(crate) primary_key_ordinal: Option<u64>,
    pub(crate) primary_key_prefix_length: Option<u64>,
    pub(crate) primary_key_direction: Option<String>,
}

#[derive(Clone)]
pub(crate) struct DiscoveredTable {
    pub(crate) config: TableConfig,
    pub(crate) schema: DatasetSchema,
    pub(crate) columns: Vec<ColumnPlan>,
    pub(crate) engine: String,
}

pub struct MySqlSourceConnector {
    config: MySqlSourceConfig,
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
            config,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            snapshot_stream: tokio::sync::OnceCell::new(),
            stream: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn discovered_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async { self.load_discovered_tables().await.map(Arc::new) })
            .await
            .map(Arc::clone)
    }

    async fn load_discovered_tables(&self) -> anyhow::Result<Vec<DiscoveredTable>> {
        let mut connection = observe_external_request(
            "mysql",
            "connect_source_discovery",
            connect(&self.config.connection),
        )
        .await?;
        let mut tables = Vec::with_capacity(self.config.tables.len());
        for table in &self.config.tables {
            tables.push(
                discover_table(
                    &mut connection,
                    &self.config.connection.database,
                    table.clone(),
                    self.config.replication.is_some(),
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
        let replication = self.config.replication.as_ref().ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL replication configuration is missing"
            ))
        })?;
        let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
        let preflight = inspect_mysql8_gtid_source(
            &self.config.connection,
            timeout,
            cancellation,
        )
        .await?;
        if &preflight.source != expected_source {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL source identity changed before replication stream handoff"
            )));
        }
        let current_tables = self.load_discovered_tables().await?;
        let current_authoritative_tables = authoritative_table_identities(
            &self.config.connection.database,
            &current_tables,
        );
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
                let replication = self.config.replication.as_ref().ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "MySQL replication configuration is missing"
                    ))
                })?;
                let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
                let preview_tables = Arc::new(self.load_discovered_tables().await?);
                let preflight = inspect_mysql8_gtid_source(
                    &self.config.connection,
                    timeout,
                    &cancellation,
                )
                .await?;
                let preparation = SnapshotStreamTracker::claim_or_resume(
                    replication.server_id,
                    &self.config.tables,
                    &preflight.source,
                    durable.clone(),
                    &replay_identity,
                )
                .await?;
                let authoritative = authoritative_table_identities(
                    &self.config.connection.database,
                    &preview_tables,
                );
                let (tracker, boundary, sessions, execution_lock, streaming) = match preparation {
                    SnapshotStreamPreparation::Create(mut tracker) => {
                        let bootstrap = begin_locked_snapshot(
                            &self.config.connection,
                            &self.config.tables,
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
                        let _resume_position = inspect_existing_replication_offset(
                            replication,
                            &preflight.source,
                            &authoritative,
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
                let replication = self.config.replication.as_ref().ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "MySQL replication configuration is missing"
                    ))
                })?;
                let timeout = Duration::from_millis(replication.bootstrap_timeout_ms);
                let tables = Arc::new(self.load_discovered_tables().await?);
                let authoritative_tables = authoritative_table_identities(
                    &self.config.connection.database,
                    &tables,
                );
                let preflight = inspect_mysql8_gtid_source(
                    &self.config.connection,
                    timeout,
                    &cancellation,
                )
                .await?;
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
                                &self.config.tables,
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
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::MySql(SourceDescriptor {
            behavior: if self.config.replication.is_some() {
                SourceBehavior::ChangelogRows
            } else {
                SourceBehavior::FiniteAppendOnlyRows
            },
            delivery_modes: if self.config.replication.is_some() {
                SourceDeliveryModes::STREAM_AND_BATCH_AND_STREAM
            } else {
                SourceDeliveryModes::BATCH
            },
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
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("MySQL discovery cancelled"),
                tables = self.discovered_tables() => tables?,
            };
            build_delivery_discovery(
                self.config.replication.is_some(),
                delivery_type,
                request,
                &tables,
            )
        })
    }

    fn prepare_execution(
        &self,
        context: SourceExecutionContext,
    ) -> BoxFuture<'_, anyhow::Result<Option<PreparedSourceExecution>>> {
        Box::pin(async move {
            if self.config.replication.is_none() {
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
                let remaining_phases =
                    self.execution_phases(DeliveryType::Stream, &discovery)?;
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

    fn complete_execution_phase(
        &self,
        phase: SourcePhase,
        durable: transferia_registry::durable::DurableContext,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if phase != SourcePhase::Snapshot || self.config.replication.is_none() {
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
            let replication = self.config.replication.as_ref().ok_or_else(|| {
                classify_replication_error(replication_safety_violation(anyhow::anyhow!(
                    "MySQL replication configuration is missing"
                )))
            })?;
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
                let replication = self.config.replication.as_ref().ok_or_else(|| {
                    classify_replication_error(replication_safety_violation(anyhow::anyhow!(
                        "MySQL replication configuration is missing"
                    )))
                })?;
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
                    source_identity.clone(),
                    tables.as_ref().clone(),
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
                    self.config.replication.is_none(),
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
                    let (
                        session_table,
                        _session_connection_id,
                        session_max_row_bytes,
                        connection,
                    ) = session.into_parts();
                    anyhow::ensure!(
                        session_table.name == table.config.name,
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
                            database: self.config.connection.database.clone(),
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
                        self.config.connection.database.clone(),
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
                        self.config.connection.database.clone(),
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
        if self.config.replication.is_some() {
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

fn build_delivery_discovery(
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
                incoming.columns.extend(table.schema.columns.iter().enumerate().map(
                    |(index, column)| {
                        SchemaColumn::new(
                            old_value_column_name(index),
                            column.data_type.clone(),
                            true,
                        )
                        .with_old_value_of(column.name.clone())
                    },
                ));
                incoming
                    .columns
                    .extend(MYSQL_SOURCE_METADATA_COLUMNS.iter().map(|column| {
                        SchemaColumn::new(column.name.to_owned(), column.data_type.clone(), false)
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

fn authoritative_table_identities(
    database: &str,
    tables: &[DiscoveredTable],
) -> Vec<AuthoritativeTableIdentity> {
    tables
        .iter()
        .map(|table| AuthoritativeTableIdentity {
            database: database.to_owned(),
            table: table.config.name.clone(),
            engine: table.engine.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| AuthoritativeColumnIdentity {
                    name: column.name.clone(),
                    column_type: column.column_type.clone(),
                    nullable: column.nullable,
                    character_set: column.character_set.clone(),
                    collation: column.collation.clone(),
                    collation_id: column.collation_id,
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

async fn discover_table(
    connection: &mut Conn,
    database: &str,
    table: TableConfig,
    replication: bool,
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
    let rows: Vec<Row> = observe_mysql_request(
        "discover_column_identities",
        connection.exec(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.IS_NULLABLE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, col.ID AS COLLATION_ID, c.EXTRA, c.GENERATION_EXPRESSION, c.CHARACTER_MAXIMUM_LENGTH, s.SEQ_IN_INDEX, s.SUB_PART, s.COLLATION FROM information_schema.COLUMNS AS c LEFT JOIN information_schema.COLLATIONS AS col ON col.COLLATION_NAME = c.COLLATION_NAME LEFT JOIN information_schema.STATISTICS AS s ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.INDEX_NAME = 'PRIMARY' AND s.COLUMN_NAME = c.COLUMN_NAME WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? ORDER BY c.ORDINAL_POSITION",
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
        .map(column_plan)
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|error| classify_discovery_contract_error(replication, error))?;
    let schema = DatasetSchema::new(
        columns
            .iter()
            .map(|column| {
                let mut schema = SchemaColumn::new(
                    column.name.clone(),
                    column.kind.arrow_type(),
                    column.nullable,
                )
                .with_constraints(column.primary_key, false, column.max_length);
                if column.kind == MySqlColumnKind::Json {
                    schema = schema.with_arrow_extension(ARROW_JSON_EXTENSION_NAME);
                }
                schema
            })
            .collect(),
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
pub(crate) fn old_value_column_name(index: usize) -> String {
    format!("_system_old_value_{index}")
}

fn column_plan(row: &Row) -> anyhow::Result<ColumnPlan> {
    let name = required::<String>(row, "COLUMN_NAME")?;
    validate_identifier("column", &name)?;
    let data_type = required::<String>(row, "DATA_TYPE")?.to_ascii_lowercase();
    let column_type = required::<String>(row, "COLUMN_TYPE")?;
    let column_type_lowercase = column_type.to_ascii_lowercase();
    let nullable = required::<String>(row, "IS_NULLABLE")? == "YES";
    let primary_key_ordinal = required::<Option<u64>>(row, "SEQ_IN_INDEX")?;
    let primary_key = primary_key_ordinal.is_some();
    let max_length = required::<Option<u64>>(row, "CHARACTER_MAXIMUM_LENGTH")?
        .map(usize::try_from)
        .transpose()?;
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
    anyhow::ensure!(
        character_set.is_some() == collation.is_some()
            && (collation_id.is_none() || collation.is_some()),
        "MySQL metadata returned inconsistent character-set identity for column '{name}'"
    );
    let extra = required::<String>(row, "EXTRA")?;
    let generation_expression = required::<Option<String>>(row, "GENERATION_EXPRESSION")?;
    let primary_key_prefix_length = required::<Option<u64>>(row, "SUB_PART")?;
    let primary_key_direction = required::<Option<String>>(row, "COLLATION")?;
    let unsigned = column_type_lowercase
        .split_ascii_whitespace()
        .any(|token| token == "unsigned");
    let kind = match data_type.as_str() {
        "tinyint" => if unsigned { MySqlColumnKind::UInt8 } else { MySqlColumnKind::Int8 },
        "smallint" => if unsigned { MySqlColumnKind::UInt16 } else { MySqlColumnKind::Int16 },
        "mediumint" | "int" | "integer" => if unsigned { MySqlColumnKind::UInt32 } else { MySqlColumnKind::Int32 },
        "bigint" => if unsigned { MySqlColumnKind::UInt64 } else { MySqlColumnKind::Int64 },
        "float" => MySqlColumnKind::Float32,
        "double" | "real" => MySqlColumnKind::Float64,
        "bit" | "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" | "geometry" | "point" | "linestring" | "polygon"
        | "multipoint" | "multilinestring" | "multipolygon" | "geometrycollection"
        | "vector" => MySqlColumnKind::Binary,
        "json" => MySqlColumnKind::Json,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
        | "enum" | "set" | "inet4" | "inet6" | "uuid" => MySqlColumnKind::Utf8,
        "date" => MySqlColumnKind::Date,
        "datetime" => MySqlColumnKind::DateTime,
        "timestamp" => MySqlColumnKind::TimestampUtc,
        "decimal" | "numeric" | "time" | "year" => MySqlColumnKind::Utf8,
        _ => anyhow::bail!(
            "unsupported MySQL/MariaDB column type '{data_type}' ({column_type}) for column '{name}'"
        ),
    };
    let quoted = quote_identifier(&name);
    let canonical_text = matches!(data_type.as_str(), "decimal" | "numeric" | "time" | "year");
    let expression = if canonical_text {
        format!("CAST({quoted} AS CHAR) AS {quoted}")
    } else {
        quoted
    };
    Ok(ColumnPlan {
        name,
        kind,
        nullable,
        primary_key,
        max_length,
        expression,
        column_type,
        character_set,
        collation,
        collation_id,
        extra,
        generation_expression,
        primary_key_ordinal,
        primary_key_prefix_length,
        primary_key_direction,
    })
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
