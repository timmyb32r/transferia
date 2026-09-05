use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

use super::config::{PostgresSourceConfig, TableConfig};
use crate::connectors::postgres::common::{
    connect, postgres_to_arrow, quote_identifier, validate_identifier,
};
use crate::connectors::postgres::src_batch::{ExportedSnapshot, PostgresSource};
use crate::connectors::postgres::src_batch_and_stream::{
    AmbiguousReplicationSlotCreation, ReplicationSlotBootstrap, SnapshotStreamPreparation,
    SnapshotStreamTracker,
};
use crate::connectors::postgres::src_stream::{
    is_replication_contract_violation, is_replication_safety_violation,
    replication_safety_violation, validate_pgoutput_publication, LogicalDecoder,
    PostgresReplicationSource, PostgresSourceIdentity,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA,
    SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::durable::{
    CompareExchangeResult, DurableContext, DurableLease, DurableStorage,
};
use transferia_registry::{
    PreparedSourceExecution, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
    SourceExecutionContext, SourceExecutionPhase, SourcePhase,
};

pub const POSTGRES_REPLICATION_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
    SystemColumnKind::ChangeOperation,
    SystemColumnKind::ChangedColumns,
];

pub struct PostgresSourceMetadataColumn {
    pub(crate) name: &'static str,

    pub(crate) role: &'static str,

    pub(crate) data_type: arrow::datatypes::DataType,
}

pub const POSTGRES_SOURCE_METADATA_COLUMNS: &[PostgresSourceMetadataColumn] = &[
    PostgresSourceMetadataColumn {
        name: "_system_source_database",
        role: SYSTEM_ROLE_SOURCE_DATABASE,
        data_type: arrow::datatypes::DataType::Utf8,
    },
    PostgresSourceMetadataColumn {
        name: "_system_source_schema",
        role: SYSTEM_ROLE_SOURCE_SCHEMA,
        data_type: arrow::datatypes::DataType::Utf8,
    },
    PostgresSourceMetadataColumn {
        name: "_system_source_table",
        role: SYSTEM_ROLE_SOURCE_TABLE,
        data_type: arrow::datatypes::DataType::Utf8,
    },
    PostgresSourceMetadataColumn {
        name: "_system_source_transaction_id",
        role: SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
        data_type: arrow::datatypes::DataType::UInt64,
    },
    PostgresSourceMetadataColumn {
        name: "_system_source_timestamp_ms",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresSourceMetadataColumn {
        name: "_system_source_timestamp_us",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresSourceMetadataColumn {
        name: "_system_source_timestamp_ns",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresSourceMetadataColumn {
        name: "_system_event_timestamp_ms",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresSourceMetadataColumn {
        name: "_system_event_timestamp_us",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresSourceMetadataColumn {
        name: "_system_event_timestamp_ns",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
        data_type: arrow::datatypes::DataType::Int64,
    },
];

const POSTGRES_SNAPSHOT_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

#[derive(Clone)]
pub struct DiscoveredTable {
    pub(crate) config: TableConfig,
    pub(crate) schema: DatasetSchema,
    pub(crate) type_oids: Vec<u32>,
    pub(crate) replica_identity_full: bool,
    pub(crate) replica_identity: String,
    pub(crate) relation_oid: u32,
}

pub struct PostgresSourceConnector {
    config: PostgresSourceConfig,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    exported_snapshot: tokio::sync::OnceCell<Arc<ExportedSnapshot>>,
    snapshot_stream: tokio::sync::OnceCell<Arc<SnapshotStreamExecution>>,
    stream_start_lsn: tokio::sync::OnceCell<Option<u64>>,
    replication_ownership: tokio::sync::OnceCell<HeldReplicationOwnership>,

    delivery_type: OnceLock<DeliveryType>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

struct HeldReplicationOwnership {
    delivery_id: Arc<str>,

    delivery_storage: Arc<dyn DurableStorage>,

    resource_storage: Arc<dyn DurableStorage>,

    source_identity: PostgresSourceIdentity,

    postgres_lease: Arc<tokio::sync::Mutex<tokio_postgres::Client>>,

    decoder: LogicalDecoder,

    _lease: DurableLease,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedReplicationResourceOwner {
    version: u8,

    delivery_id: String,

    source: PostgresSourceIdentity,

    slot: String,
}

const REPLICATION_RESOURCE_OWNER_VERSION: u8 = 1;

struct SnapshotStreamExecution {
    tracker: tokio::sync::Mutex<SnapshotStreamTracker>,

    replay_identity: Arc<str>,

    snapshot: Mutex<Option<Arc<ExportedSnapshot>>>,

    tables: Arc<Vec<DiscoveredTable>>,

    start_lsn: Mutex<Option<u64>>,

    source_identity: PostgresSourceIdentity,
}

impl PostgresSourceConnector {
    pub fn from_config(
        config: PostgresSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            exported_snapshot: tokio::sync::OnceCell::new(),
            snapshot_stream: tokio::sync::OnceCell::new(),
            stream_start_lsn: tokio::sync::OnceCell::new(),
            replication_ownership: tokio::sync::OnceCell::new(),
            delivery_type: OnceLock::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn ensure_replication_resource_ownership(
        &self,
        durable: &DurableContext,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<&HeldReplicationOwnership> {
        let replication = &self.config.replication;
        let slot = super::super::src_stream::replication_slot(&durable.delivery_id)?;
        let held = self
            .replication_ownership
            .get_or_try_init(|| async {
                let mut identity_client = connect(&self.config.connection).await?;
                let preflight_tables =
                    discover_replication_tables(&identity_client, &self.config.tables).await?;
                if let super::super::src_stream::ReplicationPlugin::Pgoutput { publication } = &replication.plugin {
                    validate_pgoutput_publication(&identity_client, publication, &preflight_tables, false).await?;
                }
                let source_identity = identify_source(
                    &self.config.connection,
                    &identity_client,
                    cancellation,
                    Duration::from_millis(replication.bootstrap_timeout_ms),
                )
                .await?;
                let resource_key = replication_resource_key(&source_identity, slot);
                acquire_postgres_replication_lease(&identity_client, &resource_key).await?;
                let lease = durable
                    .resource_storage
                    .acquire_execution_lease(&resource_key)
                    .await?;
                persist_replication_resource_owner(
                    durable,
                    &resource_key,
                    &source_identity,
                    slot,
                )
                .await?;
                let decoder = tokio::select! {
                    () = cancellation.cancelled() => anyhow::bail!("PostgreSQL plugin preparation cancelled"),
                    result = tokio::time::timeout(
                        Duration::from_millis(replication.bootstrap_timeout_ms),
                        super::super::src_stream::resolve_plugin(
                            &mut identity_client, &replication.plugin, slot, &resource_key, &preflight_tables,
                        ),
                    ) => result.map_err(|_| anyhow::anyhow!("PostgreSQL plugin/publication preparation timed out"))??,
                };
                Ok::<_, anyhow::Error>(HeldReplicationOwnership {
                    decoder,
                    delivery_id: Arc::clone(&durable.delivery_id),
                    delivery_storage: Arc::clone(&durable.storage),
                    resource_storage: Arc::clone(&durable.resource_storage),
                    source_identity,
                    postgres_lease: Arc::new(tokio::sync::Mutex::new(identity_client)),
                    _lease: lease,
                })
            })
            .await?;
        anyhow::ensure!(
            held.delivery_id == durable.delivery_id
                && Arc::ptr_eq(&held.delivery_storage, &durable.storage)
                && Arc::ptr_eq(&held.resource_storage, &durable.resource_storage),
            "PostgreSQL source connector cannot be reused with a different durable execution context"
        );
        Ok(held)
    }

    fn bind_delivery_type(&self, delivery_type: DeliveryType) -> anyhow::Result<()> {
        anyhow::ensure!(
            *self.delivery_type.get_or_init(|| delivery_type) == delivery_type,
            "PostgreSQL source connector cannot be reused for a different delivery type"
        );
        Ok(())
    }

    async fn discovered_tables(
        &self,
        delivery_type: DeliveryType,
    ) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.bind_delivery_type(delivery_type)?;
        self.discovered
            .get_or_try_init(|| async {
                if delivery_type == DeliveryType::Batch {
                    let snapshot = self.exported_snapshot().await?;
                    let snapshot_client = snapshot.client().await?;
                    discover_tables(&snapshot_client, &self.config.tables)
                        .await
                        .map(Arc::new)
                } else {
                    let client = connect(&self.config.connection).await?;
                    let tables = discover_replication_tables(&client, &self.config.tables).await?;
                    if let super::super::src_stream::ReplicationPlugin::Pgoutput { publication } =
                        &self.config.replication.plugin
                    {
                        validate_pgoutput_publication(&client, publication, &tables, false).await?;
                    }
                    Ok(Arc::new(tables))
                }
            })
            .await
            .map(Arc::clone)
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

    async fn exported_snapshot(&self) -> anyhow::Result<Arc<ExportedSnapshot>> {
        self.exported_snapshot
            .get_or_try_init(|| ExportedSnapshot::create(&self.config.connection))
            .await
            .map(Arc::clone)
    }

    async fn snapshot_stream_execution(
        &self,
        durable: transferia_registry::durable::DurableContext,
        cancellation: tokio_util::sync::CancellationToken,
        source_identity: PostgresSourceIdentity,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<Arc<SnapshotStreamExecution>> {
        let replication = &self.config.replication;
        let ownership = self
            .ensure_replication_resource_ownership(&durable, &cancellation)
            .await?;
        let decoder = &ownership.decoder;
        let slot = super::super::src_stream::replication_slot(&durable.delivery_id)?.to_owned();
        let slot = slot.as_str();
        let initialized_replay_identity = Arc::clone(&replay_identity);
        self.snapshot_stream
            .get_or_try_init(|| async move {
                let identity_client = connect(&self.config.connection).await?;
                let preflight_tables =
                    discover_replication_tables(&identity_client, &self.config.tables).await?;
                if let LogicalDecoder::Pgoutput { publication } = decoder {
                    validate_pgoutput_publication(
                        &identity_client,
                        publication,
                        &preflight_tables,
                        matches!(replication.plugin, super::super::src_stream::ReplicationPlugin::Auto),
                    )
                    .await?;
                }
                let slot_exists = replication_slot_exists(&identity_client, slot).await?;
                let preparation = SnapshotStreamTracker::claim_or_resume(
                    decoder,
                    &self.config.tables,
                    &source_identity,
                    durable,
                    slot_exists,
                    &initialized_replay_identity,
                )
                .await?;
                match preparation {
                    SnapshotStreamPreparation::Create(mut tracker) => {
                        let bootstrap = ReplicationSlotBootstrap::create(
                            &self.config.connection,
                            slot,
                            decoder.plugin(),
                            &source_identity.system(),
                            &cancellation,
                            Duration::from_millis(replication.bootstrap_timeout_ms),
                        )
                        .await
                        .map_err(|error| {
                            if error
                                .downcast_ref::<AmbiguousReplicationSlotCreation>()
                                .is_some()
                            {
                                anyhow::anyhow!(
                                    "PostgreSQL exact replication-slot snapshot bootstrap has an ambiguous CREATE result; inspect and deliberately remove the exact configured slot before retrying if it exists: {error}"
                                )
                            } else {
                                anyhow::anyhow!(
                                    "PostgreSQL exact replication-slot snapshot bootstrap failed without an ambiguous CREATE result; retry is allowed only after the connector proves the configured slot is absent: {error}"
                                )
                            }
                        })?;
                        let snapshot = ExportedSnapshot::from_replication_slot(
                            &self.config.connection,
                            bootstrap,
                        )
                        .await?;
                        let snapshot_client = snapshot.client().await?;
                        let tables = Arc::new(
                            discover_replication_tables(&snapshot_client, &self.config.tables)
                                .await?,
                        );
                        if let LogicalDecoder::Pgoutput { publication } = decoder {
                            validate_pgoutput_publication(
                                &*snapshot_client,
                                publication,
                                &tables,
                                matches!(replication.plugin, super::super::src_stream::ReplicationPlugin::Auto),
                            )
                            .await?;
                        }
                        let snapshot_database_oid = database_oid(&snapshot_client).await?;
                        anyhow::ensure!(
                            snapshot_database_oid == source_identity.database_oid,
                            "PostgreSQL database identity changed while establishing the exact snapshot boundary"
                        );
                        drop(snapshot_client);
                        tracker
                            .mark_snapshot_ready(u64::try_from(snapshot.lsn)?, &tables)
                            .await?;
                        Ok(Arc::new(SnapshotStreamExecution {
                            tracker: tokio::sync::Mutex::new(tracker),
                            replay_identity: Arc::clone(&initialized_replay_identity),
                            snapshot: Mutex::new(Some(snapshot)),
                            tables,
                            start_lsn: Mutex::new(None),
                            source_identity,
                        }))
                    }
                    SnapshotStreamPreparation::Streaming { tracker, start_lsn } => {
                        let tables = Arc::new(preflight_tables);
                        tracker.validate_authoritative_tables(&tables)?;
                        Ok(Arc::new(SnapshotStreamExecution {
                            tracker: tokio::sync::Mutex::new(tracker),
                            replay_identity: Arc::clone(&initialized_replay_identity),
                            snapshot: Mutex::new(None),
                            tables,
                            start_lsn: Mutex::new(Some(start_lsn)),
                            source_identity,
                        }))
                    }
                }
            })
            .await
            .map(Arc::clone)
            .and_then(|execution| {
                if execution.replay_identity.as_ref() == replay_identity.as_ref() {
                    Ok(execution)
                } else {
                    Err(replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL batch_and_stream connector was prepared under a different replay-affecting delivery configuration"
                    )))
                }
            })
    }

    async fn prepare_stream_start(
        &self,
        ownership: &HeldReplicationOwnership,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let replication = &self.config.replication;
        let slot = super::super::src_stream::replication_slot(&ownership.delivery_id)?;
        self.stream_start_lsn
            .get_or_try_init(|| async {
                let connection = ownership.postgres_lease.lock().await;
                let slot_exists =
                    replication_slot_exists(&connection, slot).await?;
                drop(connection);
                if slot_exists {
                    return Ok::<Option<u64>, anyhow::Error>(None);
                }
                let bootstrap = ReplicationSlotBootstrap::create(
                    &self.config.connection,
                    slot,
                    ownership.decoder.plugin(),
                    &ownership.source_identity.system(),
                    cancellation,
                    Duration::from_millis(replication.bootstrap_timeout_ms),
                )
                .await
                .map_err(|error| {
                    if error
                        .downcast_ref::<AmbiguousReplicationSlotCreation>()
                        .is_some()
                    {
                        anyhow::anyhow!(
                            "PostgreSQL stream slot creation has an ambiguous result; inspect the exact configured slot and deliberately reset it before retrying: {error}"
                        )
                    } else {
                        error
                    }
                })?;
                let start_lsn = bootstrap.consistent_lsn;
                drop(bootstrap);
                Ok(Some(start_lsn))
            })
            .await?;
        Ok(())
    }
}

fn replication_resource_key(source: &PostgresSourceIdentity, slot: &str) -> String {
    format!(
        "postgres-replication-{}-{}-{slot}",
        source.system_identifier, source.database_oid
    )
}

async fn acquire_postgres_replication_lease(
    client: &tokio_postgres::Client,
    resource_key: &str,
) -> anyhow::Result<()> {
    // A collision can only reject two unrelated executions; it can never let
    // two owners through. Use the project-standard non-cryptographic digest
    // and both 64-bit halves to keep that false-contention risk negligible.
    let digest = murmur3::murmur3_x64_128(&mut Cursor::new(resource_key.as_bytes()), 0)?;
    let bytes = digest.to_le_bytes();
    let first = i64::from_le_bytes(bytes[..8].try_into()?);
    let second = i64::from_le_bytes(bytes[8..].try_into()?);
    let row = observe_external_request(
        "postgres",
        "acquire_replication_execution_lease",
        client.query_one(
            "SELECT pg_catalog.pg_try_advisory_lock($1) AND pg_catalog.pg_try_advisory_lock($2)",
            &[&first, &second],
        ),
    )
    .await?;
    let acquired: bool = row.try_get(0)?;
    anyhow::ensure!(
        acquired,
        "PostgreSQL replication slot execution is already active on the exact source"
    );
    Ok(())
}

async fn persist_replication_resource_owner(
    durable: &DurableContext,
    resource_key: &str,
    source: &PostgresSourceIdentity,
    slot: &str,
) -> anyhow::Result<()> {
    let expected = PersistedReplicationResourceOwner {
        version: REPLICATION_RESOURCE_OWNER_VERSION,
        delivery_id: durable.delivery_id.to_string(),
        source: source.clone(),
        slot: slot.to_owned(),
    };
    if let Some(current) = durable.resource_storage.read(resource_key).await? {
        return validate_replication_resource_owner(&current.payload, &expected);
    }

    let payload = serde_json::to_vec(&expected)?;
    match durable
        .resource_storage
        .compare_exchange(resource_key, None, &payload)
        .await?
    {
        CompareExchangeResult::Applied(_) => Ok(()),
        CompareExchangeResult::Conflict(Some(current)) => {
            validate_replication_resource_owner(&current.payload, &expected)
        }
        CompareExchangeResult::Conflict(None) => anyhow::bail!(
            "PostgreSQL replication resource ownership changed while it was being claimed"
        ),
    }
}

fn validate_replication_resource_owner(
    payload: &[u8],
    expected: &PersistedReplicationResourceOwner,
) -> anyhow::Result<()> {
    let actual: PersistedReplicationResourceOwner = serde_json::from_slice(payload)?;
    anyhow::ensure!(
        actual.version == REPLICATION_RESOURCE_OWNER_VERSION,
        "unsupported PostgreSQL replication resource owner version {}",
        actual.version
    );
    anyhow::ensure!(
        actual.source == expected.source && actual.slot == expected.slot,
        "PostgreSQL replication resource ownership does not match its exact source identity and slot"
    );
    anyhow::ensure!(
        actual.delivery_id == expected.delivery_id,
        "PostgreSQL replication slot '{}' on the exact source is already owned by delivery '{}' and cannot be claimed by delivery '{}'",
        expected.slot,
        actual.delivery_id,
        expected.delivery_id
    );
    Ok(())
}

async fn database_oid(client: &tokio_postgres::Client) -> anyhow::Result<u32> {
    let row = observe_external_request(
        "postgres",
        "identify_database",
        client.query_one(
            "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()",
            &[],
        ),
    )
    .await?;
    let oid: u32 = row.get(0);
    anyhow::ensure!(oid != 0, "PostgreSQL returned an invalid database OID");
    Ok(oid)
}

async fn replication_slot_exists(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<bool> {
    let row = observe_external_request(
        "postgres",
        "inspect_replication_slot",
        client.query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        ),
    )
    .await?;
    Ok(row.get(0))
}

async fn identify_source(
    connection: &crate::connectors::postgres::common::PostgresConnectionConfig,
    client: &tokio_postgres::Client,
    cancellation: &tokio_util::sync::CancellationToken,
    timeout: Duration,
) -> anyhow::Result<PostgresSourceIdentity> {
    let database_oid = database_oid(client).await?;
    let system =
        ReplicationSlotBootstrap::identify_system(connection, cancellation, timeout).await?;
    anyhow::ensure!(
        system.database == connection.database,
        "PostgreSQL IDENTIFY_SYSTEM returned a different database"
    );
    Ok(PostgresSourceIdentity {
        system_identifier: system.system_identifier,
        database: system.database,
        database_oid,
    })
}

pub(super) fn classify_replication_connector_error(error: anyhow::Error) -> anyhow::Error {
    if is_replication_contract_violation(&error) || is_replication_safety_violation(&error) {
        anyhow::Error::new(transferia_core::failure::DataPlaneFailure::fatal(error))
    } else {
        error
    }
}

pub(super) fn require_replication_replay_identity(
    replay_identity: Option<Arc<str>>,
) -> anyhow::Result<Arc<str>> {
    let replay_identity = replay_identity.ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "PostgreSQL replication requires a non-secret replay identity bound to the complete replay-affecting delivery configuration"
        ))
    })?;
    if replay_identity.is_empty() {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "PostgreSQL replication replay identity must not be empty"
        )));
    }
    Ok(replay_identity)
}

async fn discover_tables(
    client: &tokio_postgres::Client,
    configured: &[TableConfig],
) -> anyhow::Result<Vec<DiscoveredTable>> {
    let mut tables = Vec::with_capacity(configured.len());
    for table in configured {
        tables.push(discover_table(client, table.clone()).await?);
    }
    Ok(tables)
}

async fn discover_replication_tables(
    client: &tokio_postgres::Client,
    configured: &[TableConfig],
) -> anyhow::Result<Vec<DiscoveredTable>> {
    let tables = discover_tables(client, configured).await?;
    validate_replication_table_identities(&tables)?;
    Ok(tables)
}

pub(super) fn validate_replication_table_identities(
    tables: &[DiscoveredTable],
) -> anyhow::Result<()> {
    for table in tables {
        let name = format!("{}.{}", table.config.schema, table.config.name);
        match table.replica_identity.as_str() {
            "f" => {}
            "d" => anyhow::ensure!(
                table.schema.columns.iter().any(|column| column.primary_key),
                "PostgreSQL replication table '{name}' uses DEFAULT replica identity but has no primary key; configure REPLICA IDENTITY FULL so updates and deletes retain an exact old row identity"
            ),
            "i" => anyhow::bail!(
                "PostgreSQL replication table '{name}' uses REPLICA IDENTITY USING INDEX, which can differ from the declared primary key and cannot preserve primary-key row identity; use the primary-key DEFAULT identity or REPLICA IDENTITY FULL"
            ),
            "n" => anyhow::bail!(
                "PostgreSQL replication table '{name}' uses REPLICA IDENTITY NOTHING and cannot preserve old row identity; use the primary-key DEFAULT identity or REPLICA IDENTITY FULL"
            ),
            other => anyhow::bail!(
                "PostgreSQL replication table '{name}' returned unsupported replica identity '{other}'"
            ),
        }
    }
    Ok(())
}

fn build_delivery_discovery(
    replication: bool,
    delivery_type: DeliveryType,
    request: transferia_core::delivery::DeliveryDiscoveryRequest,
    tables: &[DiscoveredTable],
) -> anyhow::Result<DeliveryDiscovery> {
    let system_columns = if replication {
        POSTGRES_REPLICATION_SYSTEM_COLUMNS
    } else {
        POSTGRES_SNAPSHOT_SYSTEM_COLUMNS
    };
    let discovered_system_columns = system_columns
        .iter()
        .copied()
        .map(Into::into)
        .collect::<Vec<_>>();
    let datasets =
        tables
            .iter()
            .map(|table| {
                let mut incoming = incoming_user_schema(&table.schema);
                if replication {
                    if table.replica_identity_full {
                        incoming
                            .columns
                            .extend(table.schema.columns.iter().enumerate().map(
                                |(index, column)| {
                                    SchemaColumn::new(
                                        old_value_column_name(index),
                                        column.data_type.clone(),
                                        true,
                                    )
                                    .with_old_value_of(column.name.clone())
                                },
                            ));
                    } else {
                        incoming.columns.extend(
                            table
                                .schema
                                .columns
                                .iter()
                                .enumerate()
                                .filter(|(_, column)| column.primary_key)
                                .map(|(index, column)| {
                                    SchemaColumn::new(
                                        old_key_column_name(index),
                                        column.data_type.clone(),
                                        true,
                                    )
                                    .with_old_key_of(column.name.clone())
                                }),
                        );
                    }
                }
                incoming
                    .columns
                    .extend(POSTGRES_SOURCE_METADATA_COLUMNS.iter().map(|column| {
                        SchemaColumn::new(column.name.to_owned(), column.data_type.clone(), false)
                            .with_system_role(column.role)
                    }));
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
    let table_partitions = || {
        (0..tables.len())
            .map(i64::try_from)
            .collect::<Result<Vec<_>, _>>()
    };
    let source_topology = match (replication, delivery_type) {
        (false, DeliveryType::Batch) | (true, DeliveryType::BatchAndStream) => {
            SourceTopology::CoLocatedStaticPartitions(table_partitions()?)
        }
        (true, DeliveryType::Stream) => SourceTopology::StaticPartitions(vec![0]),
        _ => anyhow::bail!(
            "PostgreSQL source configuration does not support delivery type '{}'",
            delivery_type.label()
        ),
    };
    Ok(DeliveryDiscovery {
        source_name: Arc::from("postgres"),
        source_topology,
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: request.keep_system_columns,
        datasets,
        performance_advice: Vec::new(),
    })
}

impl SourceConnector for PostgresSourceConnector {
    fn compatibility(
        &self,
        delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::Postgres(SourceDescriptor {
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
            let tables = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("PostgreSQL discovery cancelled"), tables = self.discovered_tables(delivery_type) => tables? };
            build_delivery_discovery(
                delivery_type != DeliveryType::Batch,
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
            self.bind_delivery_type(context.delivery_type)?;
            let replay_identity = if context.delivery_type == DeliveryType::Batch {
                None
            } else {
                Some(
                    require_replication_replay_identity(context.replay_identity.clone())
                        .map_err(classify_replication_connector_error)?,
                )
            };
            let replication_ownership = if context.delivery_type == DeliveryType::Batch {
                None
            } else {
                Some(
                    self.ensure_replication_resource_ownership(
                        &context.durable,
                        &context.cancellation,
                    )
                    .await?,
                )
            };
            let source_identity =
                replication_ownership.map(|ownership| ownership.source_identity.clone());
            if context.delivery_type == DeliveryType::Stream {
                let ownership = replication_ownership.ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL replication resource ownership is missing"
                    ))
                })?;
                self.prepare_stream_start(ownership, &context.cancellation)
                    .await
                    .map_err(classify_replication_connector_error)?;
                return Ok(None);
            }
            if context.delivery_type != DeliveryType::BatchAndStream {
                return Ok(None);
            }
            let source_identity = source_identity.ok_or_else(|| {
                anyhow::anyhow!("PostgreSQL replication configuration is missing")
            })?;
            let replay_identity = replay_identity.ok_or_else(|| {
                classify_replication_connector_error(replication_safety_violation(anyhow::anyhow!(
                    "PostgreSQL batch_and_stream replay identity is missing after validation"
                )))
            })?;
            let execution = self
                .snapshot_stream_execution(
                    context.durable,
                    context.cancellation,
                    source_identity,
                    replay_identity,
                )
                .await
                .map_err(classify_replication_connector_error)?;
            let discovery = build_delivery_discovery(
                true,
                DeliveryType::BatchAndStream,
                context.request,
                &execution.tables,
            )?;
            let mut remaining_phases =
                self.execution_phases(DeliveryType::BatchAndStream, &discovery)?;
            if execution
                .start_lsn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
            {
                remaining_phases.remove(0);
            }
            Ok(Some(PreparedSourceExecution {
                discovery,
                remaining_phases,
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
        _durable: transferia_registry::durable::DurableContext,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if phase != SourcePhase::Snapshot
                || self.delivery_type.get() != Some(&DeliveryType::BatchAndStream)
            {
                return Ok(());
            }
            let replication = &self.config.replication;
            let execution = self
                .snapshot_stream
                .get()
                .ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL batch_and_stream execution was not prepared"
                    ))
                })
                .map_err(classify_replication_connector_error)?;
            let start_lsn = {
                let mut tracker = execution.tracker.lock().await;
                match tracker.streaming_lsn() {
                    Some(lsn) => lsn,
                    None => tracker
                        .mark_streaming()
                        .await
                        .map_err(classify_replication_connector_error)?,
                }
            };
            *execution
                .start_lsn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(start_lsn);
            let snapshot = execution
                .snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(snapshot) = snapshot {
                snapshot
                    .close_replication_owner(Duration::from_millis(
                        replication.bootstrap_timeout_ms,
                    ))
                    .await?;
                execution
                    .snapshot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
            }
            Ok(())
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            self.bind_delivery_type(context.delivery_type)?;
            let partition_id = context.partition_id;
            let replay_identity = if context.delivery_type == DeliveryType::Batch {
                None
            } else {
                Some(
                    require_replication_replay_identity(context.replay_identity.clone())
                        .map_err(classify_replication_connector_error)?,
                )
            };
            let replication_ownership = if context.delivery_type == DeliveryType::Batch {
                None
            } else {
                Some(
                    self.ensure_replication_resource_ownership(
                        &context.durable,
                        &context.cancellation,
                    )
                    .await?,
                )
            };
            let snapshot_stream = if context.delivery_type == DeliveryType::BatchAndStream {
                Some(
                    self.snapshot_stream
                        .get()
                        .ok_or_else(|| {
                            replication_safety_violation(anyhow::anyhow!(
                                "PostgreSQL batch_and_stream execution was not prepared"
                            ))
                        })
                        .map_err(classify_replication_connector_error)?,
                )
            } else {
                None
            };
            if let Some(execution) = snapshot_stream {
                let expected_replay_identity = replay_identity.as_deref().ok_or_else(|| {
                    classify_replication_connector_error(replication_safety_violation(
                        anyhow::anyhow!(
                            "PostgreSQL batch_and_stream replay identity is missing after validation"
                        ),
                    ))
                })?;
                if execution.replay_identity.as_ref() != expected_replay_identity {
                    return Err(classify_replication_connector_error(
                        replication_safety_violation(anyhow::anyhow!(
                            "PostgreSQL batch_and_stream source was built under a different replay-affecting delivery configuration"
                        )),
                    ));
                }
            }
            let tables = match snapshot_stream {
                Some(execution) => Arc::clone(&execution.tables),
                None => self.discovered_tables(context.delivery_type).await?,
            };
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            if context.delivery_type != DeliveryType::Batch {
                let replication = &self.config.replication;
                let replay_identity = replay_identity.ok_or_else(|| {
                    classify_replication_connector_error(replication_safety_violation(
                        anyhow::anyhow!(
                            "PostgreSQL replication replay identity is missing after validation"
                        ),
                    ))
                })?;
                if context.phase == SourcePhase::Stream {
                    anyhow::ensure!(
                        partition_id == 0,
                        "PostgreSQL replication has exactly one stream partition"
                    );
                    let start_lsn = snapshot_stream
                        .map(|execution| {
                            execution
                                .start_lsn
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .ok_or_else(|| {
                                    replication_safety_violation(anyhow::anyhow!(
                                        "PostgreSQL snapshot phase has not committed its WAL handoff"
                                    ))
                                })
                        })
                        .transpose()
                        .map_err(classify_replication_connector_error)?
                        .or_else(|| self.stream_start_lsn.get().copied().flatten());
                    let source_identity = match snapshot_stream {
                        Some(execution) => execution.source_identity.clone(),
                        None => replication_ownership
                            .map(|ownership| ownership.source_identity.clone())
                            .ok_or_else(|| {
                                replication_safety_violation(anyhow::anyhow!(
                                    "PostgreSQL replication resource ownership is missing"
                                ))
                            })
                            .map_err(classify_replication_connector_error)?,
                    };
                    let client = replication_ownership
                        .map(|ownership| Arc::clone(&ownership.postgres_lease))
                        .ok_or_else(|| {
                            replication_safety_violation(anyhow::anyhow!(
                                "PostgreSQL replication execution lease is missing"
                            ))
                        })
                        .map_err(classify_replication_connector_error)?;
                    let source = PostgresReplicationSource::new(
                        client,
                        replication.clone(),
                        replication_ownership
                            .ok_or_else(|| {
                                anyhow::anyhow!("PostgreSQL replication ownership is missing")
                            })?
                            .decoder
                            .clone(),
                        source_identity,
                        Arc::from(self.config.connection.database.as_str()),
                        tables.as_ref().clone(),
                        counters,
                        context.cancellation,
                        context.durable,
                        start_lsn,
                        replay_identity,
                    )
                    .await
                    .map_err(classify_replication_connector_error)?;
                    return Ok(Box::new(source) as Box<dyn Source>);
                }
                anyhow::ensure!(
                    context.delivery_type == DeliveryType::BatchAndStream
                        && context.phase == SourcePhase::Snapshot,
                    "PostgreSQL replication configuration supports snapshot reads only in batch_and_stream mode"
                );
            }
            let client = connect(&self.config.connection).await?;
            let index = usize::try_from(partition_id)?;
            let table = tables
                .get(index)
                .ok_or_else(|| {
                    anyhow::anyhow!("PostgreSQL source partition {partition_id} does not exist")
                })?
                .clone();
            let (snapshot, changelog_snapshot) = match snapshot_stream {
                Some(execution) => (
                    execution
                        .snapshot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone()
                        .ok_or_else(|| {
                            anyhow::anyhow!("PostgreSQL exact slot snapshot owner is unavailable")
                        })?,
                    true,
                ),
                None => (self.exported_snapshot().await?, false),
            };
            Ok(Box::new(
                PostgresSource::new(
                    client,
                    snapshot,
                    partition_id,
                    table,
                    self.config.connection.database.clone(),
                    self.config.batch_rows,
                    self.config.copy_to_format,
                    counters,
                    changelog_snapshot,
                )
                .await?,
            ) as Box<dyn Source>)
        })
    }

    fn build_speedtest_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        if context.delivery_type != DeliveryType::Batch {
            return Box::pin(async {
                anyhow::bail!(
                    "PostgreSQL replication speedtest requires an isolated replication slot"
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

pub fn incoming_user_schema(stored: &DatasetSchema) -> DatasetSchema {
    // Snapshot and CDC expose one stable Arrow user schema. CDC needs nullable
    // incoming fields for unchanged TOAST values; snapshots use the same
    // representation so consumers cannot distinguish the modes by data fields.
    let mut incoming = stored.clone();
    for column in &mut incoming.columns {
        column.nullable = true;
    }
    incoming
}

pub async fn discover_table(
    client: &tokio_postgres::Client,
    table: TableConfig,
) -> anyhow::Result<DiscoveredTable> {
    let query = format!(
        "SELECT * FROM {}.{} LIMIT 0",
        quote_identifier(&table.schema),
        quote_identifier(&table.name)
    );
    let statement = client.prepare(&query).await.map_err(|error| {
        anyhow::anyhow!(
            "cannot inspect PostgreSQL table '{}.{}': {error}",
            table.schema,
            table.name
        )
    })?;
    anyhow::ensure!(
        !statement.columns().is_empty(),
        "PostgreSQL table '{}.{}' has no columns",
        table.schema,
        table.name
    );
    let nullability = client.query(
        "SELECT column_name, is_nullable = 'YES' FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
        &[&table.schema, &table.name],
    ).await?.into_iter().map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1))).collect::<HashMap<_, _>>();
    let physical_types = client
        .query(
            "SELECT a.attname, a.atttypid, EXISTS (\
                 SELECT 1 FROM pg_index AS i \
                 WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)\
             ) AS primary_key \
             FROM pg_attribute AS a \
             JOIN pg_class AS c ON c.oid = a.attrelid \
             JOIN pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&table.schema, &table.name],
        )
        .await?;
    let relation = client
        .query_one(
            "SELECT c.oid, c.relreplident::text FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2",
            &[&table.schema, &table.name],
        )
        .await?;
    let relation_oid = relation.get::<_, u32>(0);
    let replica_identity = relation.get::<_, String>(1);
    anyhow::ensure!(
        matches!(replica_identity.as_str(), "d" | "n" | "f" | "i"),
        "PostgreSQL table '{}.{}' returned unsupported replica identity '{}'",
        table.schema,
        table.name,
        replica_identity,
    );
    anyhow::ensure!(
        physical_types.len() == statement.columns().len(),
        "PostgreSQL physical schema for '{}.{}' has {} columns, query declared {}",
        table.schema,
        table.name,
        physical_types.len(),
        statement.columns().len()
    );
    let columns = statement
        .columns()
        .iter()
        .zip(&physical_types)
        .map(|(column, physical)| {
            validate_identifier("column", column.name())?;
            let nullable = *nullability.get(column.name()).ok_or_else(|| {
                anyhow::anyhow!(
                    "missing nullability metadata for column '{}'",
                    column.name()
                )
            })?;
            Ok(SchemaColumn::new(
                column.name().to_owned(),
                postgres_to_arrow(column.type_())?,
                nullable,
            )
            .with_constraints(physical.get::<_, bool>(2), false, None))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let type_oids = physical_types
        .iter()
        .zip(statement.columns())
        .map(|(row, column)| {
            let name: String = row.get(0);
            anyhow::ensure!(
                name == column.name(),
                "PostgreSQL physical/query schema order differs at '{}' versus '{}'",
                name,
                column.name()
            );
            Ok(row.get::<_, u32>(1))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for index in 0..columns.len() {
        for reserved in [old_value_column_name(index), old_key_column_name(index)] {
            anyhow::ensure!(
                columns.iter().all(|column| column.name != reserved),
                "PostgreSQL table '{}.{}' column '{}' conflicts with a reserved CDC control column name",
                table.schema,
                table.name,
                reserved,
            );
        }
    }
    Ok(DiscoveredTable {
        config: table,
        schema: DatasetSchema::new(columns),
        type_oids,
        replica_identity_full: replica_identity == "f",
        replica_identity,
        relation_oid,
    })
}

pub fn old_value_column_name(index: usize) -> String {
    format!("_system_old_value_{index}")
}

pub fn old_key_column_name(index: usize) -> String {
    format!("_system_old_key_{index}")
}
