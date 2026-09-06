use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, BinaryArray, Date32Array, StringArray};
use futures_util::future::BoxFuture;

use super::client::{probe_network, ReconnectingClient};
use super::config::ClickHouseInsertFormat;
use super::http::HttpInsertTransport;
use super::table::{
    prepare_tables, speedtest_create_table_ddl, validate_speedtest_table, validate_table_schema,
};
use super::transport::{InsertTransport, NativeTransport};
use super::{ClickHouseSink, ClickHouseSinkConfig};
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::delivery::{
    validate_batch_against_discovery, validate_stored_projection, ArrowTypeFamily,
    DeliveryDiscovery, NameSyntax, SinkLimits, SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_core::SystemColumnKind;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
    SinkSpeedtestIsolationSafety, SpeedtestPhysicalTarget,
};

const SHARD_GROUPS_QUERY: &str =
    "SELECT DISTINCT toString(cluster) AS cluster FROM system.clusters ORDER BY cluster";
const CLICKHOUSE_DATE32_MIN_DAYS: i32 = -25_567;
const CLICKHOUSE_DATE32_MAX_DAYS: i32 = 120_529;

pub struct ClickHouseSinkConnector {
    config: ClickHouseSinkConfig,
    client: Arc<ReconnectingClient>,
    speedtest_scope: Option<Arc<ClickHouseSpeedtestScope>>,
}

pub(super) struct ClickHouseSpeedtestScope {
    pub(super) database: Arc<str>,

    pub(super) owner_marker: Arc<str>,

    pub(super) tables: BTreeSet<Arc<str>>,

    pub(super) physical_targets: BTreeSet<(Arc<str>, Arc<str>)>,

    pub(super) shard_group: Option<Arc<str>>,

    pub(super) replica_hosts: BTreeSet<Arc<str>>,

    pub(super) attempted_tables: Mutex<BTreeSet<Arc<str>>>,

    pub(super) claimed_tables: Mutex<BTreeSet<Arc<str>>>,
}

impl ClickHouseSpeedtestScope {
    pub(super) fn record_attempt(&self, table: Arc<str>) {
        self.attempted_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table);
    }

    pub(super) fn attempted_tables(&self) -> BTreeSet<Arc<str>> {
        self.attempted_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn claim(&self, table: Arc<str>) {
        self.claimed_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table);
    }

    pub(super) fn claimed_tables(&self) -> BTreeSet<Arc<str>> {
        self.claimed_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn unclaim(&self, table: &str) {
        self.claimed_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(table);
        self.attempted_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(table);
    }
}

pub enum ClickHouseConnectionCheck {
    Verified { shard_groups: Vec<String> },
    NetworkReachable,
}

impl ClickHouseSinkConnector {
    pub fn from_config(config: ClickHouseSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let client = Arc::new(ReconnectingClient::new(&config));
        Ok(Self {
            config,
            client,
            speedtest_scope: None,
        })
    }

    async fn shared_client(&self) -> anyhow::Result<Arc<ReconnectingClient>> {
        let client = Arc::clone(&self.client);
        client
            .ensure_connected()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
        Ok(client)
    }

    pub async fn check_connection(
        config: ClickHouseSinkConfig,
    ) -> anyhow::Result<ClickHouseConnectionCheck> {
        config.validate_connection()?;
        if config.database.is_empty() || config.username.is_empty() {
            probe_network(&config.hosts, config.port, config.connect_timeout()).await?;
            return Ok(ClickHouseConnectionCheck::NetworkReachable);
        }
        config.validate()?;
        let client = ReconnectingClient::new(&config);
        let groups = query_shard_groups(&client).await?;
        validate_selected_shard_group(
            (!config.shard_group.is_empty()).then_some(config.shard_group.as_str()),
            &groups,
        )?;
        Ok(ClickHouseConnectionCheck::Verified {
            shard_groups: groups,
        })
    }
}

async fn query_shard_groups(client: &ReconnectingClient) -> anyhow::Result<Vec<String>> {
    let started = std::time::Instant::now();
    let batches = client.query_all(SHARD_GROUPS_QUERY).await;
    tracing::info!(
        stage = "shard_groups_query",
        elapsed_ms = started.elapsed().as_millis(),
        success = batches.is_ok(),
        "ClickHouse connection check stage completed"
    );
    let batches = batches.map_err(|error| connection_check_error(&error))?;
    let mut groups = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("cluster")
            .ok_or_else(|| anyhow::anyhow!("ClickHouse system.clusters omitted 'cluster'"))?;
        append_shard_groups(column.as_ref(), &mut groups)?;
    }
    groups.sort();
    groups.dedup();
    Ok(groups)
}

pub fn connection_check_error(error: &clickhouse_arrow::Error) -> anyhow::Error {
    let rendered = error.to_string();
    if rendered.contains("AUTHENTICATION_FAILED") || rendered.contains("Authentication failed") {
        anyhow::anyhow!(
            "ClickHouse is reachable, but authentication failed. Check the username and password. If the password field is empty, enter the password for this user and try again."
        )
    } else {
        anyhow::anyhow!("ClickHouse connection check failed: {rendered}")
    }
}

fn append_shard_groups(column: &dyn Array, groups: &mut Vec<String>) -> anyhow::Result<()> {
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        groups.extend(values.iter().flatten().map(str::to_owned));
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        for value in values.iter().flatten() {
            groups.push(
                std::str::from_utf8(value)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "ClickHouse system.clusters returned a non-UTF-8 shard group: {error}"
                        )
                    })?
                    .to_owned(),
            );
        }
        return Ok(());
    }
    anyhow::bail!(
        "ClickHouse system.clusters returned unsupported Arrow type {:?} for 'cluster'",
        column.data_type(),
    )
}

fn validate_selected_shard_group(
    selected: Option<&str>,
    available: &[String],
) -> anyhow::Result<()> {
    if let Some(selected) = selected {
        anyhow::ensure!(
            available.iter().any(|candidate| candidate == selected),
            "ClickHouse shard group '{selected}' is not available to this user"
        );
    }
    Ok(())
}

fn effective_shard_group<'a>(
    config: &ClickHouseSinkConfig,
    available: &'a [String],
) -> anyhow::Result<Option<&'a str>> {
    if !config.shard_group.is_empty() {
        validate_selected_shard_group(Some(&config.shard_group), available)?;
        return Ok(available
            .iter()
            .find(|candidate| candidate.as_str() == config.shard_group)
            .map(String::as_str));
    }
    if config.effective_data_host_count() <= 1 {
        return Ok(None);
    }
    anyhow::ensure!(
        available.len() == 1,
        "ClickHouse has {} data hosts and {} available shard groups; select a shard group explicitly",
        config.effective_data_host_count(),
        available.len(),
    );
    Ok(available.first().map(String::as_str))
}

impl SinkLimits for ClickHouseSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let identifier = TextLimit {
            syntax: NameSyntax::AsciiIdentifier,
            max_utf8_bytes: None,
        };
        SinkLimitsDescription {
            sink: "clickhouse",
            dataset_name: Some(identifier.clone()),
            column_name: Some(identifier),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Decimal,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "ClickHouse requires at least one dataset"
        );
        let mut names = std::collections::HashSet::with_capacity(discovery.datasets.len());
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "ClickHouse datasets repeat table '{}'",
                dataset.name
            );
            validate_stored_projection(discovery, dataset)?;
            validate_table_schema(&dataset.name, &dataset.stored_schema).map_err(|error| {
                error.context(format!(
                    "discovered {:?} dataset '{}' is incompatible with ClickHouse",
                    dataset.role, dataset.name,
                ))
            })?;
        }
        Ok(())
    }

    fn validate_batch(
        &self,
        discovery: &DeliveryDiscovery,
        batch: &transferia_core::sink::SinkBatch,
    ) -> anyhow::Result<()> {
        validate_batch_against_discovery(discovery, batch)?;
        for (field, array) in batch
            .batch
            .schema()
            .fields()
            .iter()
            .zip(batch.batch.columns())
        {
            if field.data_type() != &arrow::datatypes::DataType::Date32 {
                continue;
            }
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse Date32 Arrow type mismatch"))?;
            for (row, value) in values.iter().enumerate() {
                let Some(value) = value else {
                    continue;
                };
                anyhow::ensure!(
                    (CLICKHOUSE_DATE32_MIN_DAYS..=CLICKHOUSE_DATE32_MAX_DAYS).contains(&value),
                    "ClickHouse dataset '{}' column '{}' row {} contains Date32 day {}, outside the lossless ClickHouse Date32 range {}..={} (1900-01-01 through 2299-12-31)",
                    batch.table,
                    field.name(),
                    row,
                    value,
                    CLICKHOUSE_DATE32_MIN_DAYS,
                    CLICKHOUSE_DATE32_MAX_DAYS,
                );
            }
        }
        Ok(())
    }
}

impl SinkConnector for ClickHouseSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::ClickHouse
    }

    fn limits(&self) -> &dyn SinkLimits {
        &self.config
    }

    fn destination_type(
        &self,
        column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        super::table::destination_type(column)
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            if let Some(scope) = &self.speedtest_scope {
                return prepare_speedtest_tables(client.as_ref(), &self.config, &request, scope)
                    .await;
            }
            let groups = if !self.config.shard_group.is_empty()
                || self.config.effective_data_host_count() > 1
            {
                query_shard_groups(client.as_ref()).await?
            } else {
                Vec::new()
            };
            let shard_group = effective_shard_group(&self.config, &groups)?;
            if let Some(shard_group) = shard_group {
                tracing::info!(
                    shard_group,
                    data_host_count = self.config.effective_data_host_count(),
                    "preparing ClickHouse tables on cluster"
                );
            }
            prepare_tables(client.as_ref(), &self.config, &request, shard_group).await
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            if let Some(scope) = &self.speedtest_scope {
                verify_all_clickhouse_tables(
                    client.as_ref(),
                    &self.config,
                    scope,
                    context.discovery.as_ref(),
                )
                .await?;
            }
            let transport: Arc<dyn InsertTransport> = match self.config.insert_format {
                ClickHouseInsertFormat::Native => Arc::new(NativeTransport::new(client)),
                ClickHouseInsertFormat::Parquet | ClickHouseInsertFormat::ArrowStream => {
                    Arc::new(HttpInsertTransport::new(&self.config, client)?)
                }
            };
            tracing::info!(
                partition = context.partition_id,
                insert_format = ?self.config.insert_format,
                "building ClickHouse sink on shared client"
            );
            Ok(
                Box::new(ClickHouseSink::with_transport_for_partition_and_visibility(
                    self.config.clone(),
                    context.counters,
                    transport,
                    context.partition_id,
                    context.keep_system_columns,
                    context.discovery,
                )) as Box<dyn Sink>,
            )
        })
    }

    fn isolate_speedtest(
        self: Arc<Self>,
        discovery: Arc<DeliveryDiscovery>,
        isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            let groups = if !self.config.shard_group.is_empty()
                || self.config.effective_data_host_count() > 1
            {
                query_shard_groups(client.as_ref()).await?
            } else {
                Vec::new()
            };
            let shard_group = effective_shard_group(&self.config, &groups)?.map(Arc::from);
            let replica_hosts =
                query_replica_hosts(client.as_ref(), shard_group.as_deref()).await?;
            let (isolated_discovery, table_names, tables) =
                isolate_discovery(discovery.as_ref(), &isolation_id)?;
            let physical_targets = discovery
                .datasets
                .iter()
                .map(|dataset| {
                    let scratch = table_names.get(&dataset.name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "ClickHouse speedtest omitted dataset '{}' from its scratch mapping",
                            dataset.name
                        )
                    })?;
                    Ok(SpeedtestPhysicalTarget {
                        production: clickhouse_physical_target(
                            &self.config.database,
                            &dataset.name,
                        ),
                        scratch: clickhouse_physical_target(&self.config.database, scratch),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let scope = Arc::new(ClickHouseSpeedtestScope {
                database: Arc::from(self.config.database.as_str()),
                owner_marker: random_owner_marker()?,
                tables,
                physical_targets: physical_target_set(&physical_targets),
                shard_group,
                replica_hosts,
                attempted_tables: Mutex::new(BTreeSet::new()),
                claimed_tables: Mutex::new(BTreeSet::new()),
            });
            let connector: Arc<dyn SinkConnector> = Arc::new(Self {
                config: self.config.clone(),
                client: Arc::clone(&self.client),
                speedtest_scope: Some(scope),
            });
            SinkSpeedtestIsolation::scratch(
                connector,
                discovery.as_ref(),
                isolated_discovery,
                table_names,
                physical_targets,
            )
        })
    }

    fn cleanup_speedtest<'a>(
        &'a self,
        isolation: &'a SinkSpeedtestIsolation,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let scope = self.speedtest_scope.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "refusing to clean speedtest tables with a production ClickHouse connector"
                )
            })?;
            validate_cleanup_scope(isolation, scope)?;

            let client = self.shared_client().await?;
            let mut failures = Vec::new();
            for table in scope.attempted_tables() {
                if drop_owned_clickhouse_table(client.as_ref(), scope, &table)
                    .await
                    .is_err()
                {
                    failures.push(format!("'{table}'"));
                }
            }
            anyhow::ensure!(
                failures.is_empty(),
                "Failed to remove ClickHouse speedtest tables: {}",
                failures.join("; ")
            );
            Ok(())
        })
    }
}

const SPEEDTEST_TABLE_PREFIX: &str = "_transferia_st_";

type IsolatedDiscovery = (
    DeliveryDiscovery,
    BTreeMap<Arc<str>, Arc<str>>,
    BTreeSet<Arc<str>>,
);

fn isolate_discovery(
    original: &DeliveryDiscovery,
    isolation_id: &str,
) -> anyhow::Result<IsolatedDiscovery> {
    validate_isolation_id(isolation_id)?;
    let mut discovery = original.clone();
    let mut table_names = BTreeMap::new();
    let mut tables = BTreeSet::new();
    for (index, dataset) in discovery.datasets.iter_mut().enumerate() {
        let original_name = Arc::clone(&dataset.name);
        let scratch: Arc<str> =
            Arc::from(format!("{SPEEDTEST_TABLE_PREFIX}{isolation_id}_{index:x}"));
        super::identifier::validate_identifier(&scratch)?;
        anyhow::ensure!(
            table_names
                .insert(original_name, Arc::clone(&scratch))
                .is_none(),
            "ClickHouse speedtest source discovery repeats a dataset name"
        );
        anyhow::ensure!(
            tables.insert(Arc::clone(&scratch)),
            "ClickHouse speedtest generated a duplicate scratch table"
        );
        dataset.name = scratch;
    }
    Ok((discovery, table_names, tables))
}

fn validate_isolation_id(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "ClickHouse speedtest isolation ID must contain exactly 32 lowercase hexadecimal characters"
    );
    Ok(())
}

fn is_speedtest_table(value: &str) -> bool {
    let Some((id, index)) = value
        .strip_prefix(SPEEDTEST_TABLE_PREFIX)
        .and_then(|suffix| suffix.split_once('_'))
    else {
        return false;
    };
    validate_isolation_id(id).is_ok()
        && !index.is_empty()
        && index.len() <= 16
        && index
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn clickhouse_physical_target(database: &str, table: &str) -> Arc<str> {
    Arc::from(format!(
        "{}.{}",
        super::client::quote_identifier(database),
        super::client::quote_identifier(table)
    ))
}

fn physical_target_set(targets: &[SpeedtestPhysicalTarget]) -> BTreeSet<(Arc<str>, Arc<str>)> {
    targets
        .iter()
        .map(|target| (Arc::clone(&target.production), Arc::clone(&target.scratch)))
        .collect()
}

fn validate_cleanup_scope(
    isolation: &SinkSpeedtestIsolation,
    scope: &ClickHouseSpeedtestScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        isolation.safety() == SinkSpeedtestIsolationSafety::Scratch,
        "refusing to clean a ClickHouse speedtest isolation without scratch safety"
    );
    let actual_tables = isolation
        .discovery
        .datasets
        .iter()
        .map(|dataset| Arc::clone(&dataset.name))
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual_tables == scope.tables,
        "refusing to clean ClickHouse speedtest tables: isolated discovery does not match the connector-owned scratch set"
    );
    anyhow::ensure!(
        scope.tables.iter().all(|table| is_speedtest_table(table)),
        "refusing to clean a ClickHouse table outside the speedtest namespace"
    );
    anyhow::ensure!(
        physical_target_set(isolation.physical_targets()) == scope.physical_targets,
        "refusing to clean ClickHouse speedtest tables: physical target proof does not match the connector-owned scratch set"
    );
    Ok(())
}

fn clickhouse_cleanup_ddl(
    database: &str,
    table: &str,
    shard_group: Option<&str>,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        is_speedtest_table(table),
        "refusing to remove ClickHouse table outside the speedtest namespace"
    );
    let on_cluster = shard_group
        .map(|cluster| format!(" ON CLUSTER {}", super::client::quote_identifier(cluster)))
        .unwrap_or_default();
    Ok(format!(
        "DROP TABLE IF EXISTS {}.{}{on_cluster} SYNC",
        super::client::quote_identifier(database),
        super::client::quote_identifier(table)
    ))
}

fn random_owner_marker() -> anyhow::Result<Arc<str>> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)?;
    let mut marker = String::from("transferia-speedtest-owner:");
    for byte in random {
        write!(&mut marker, "{byte:02x}")?;
    }
    Ok(Arc::from(marker))
}

async fn query_replica_hosts(
    client: &ReconnectingClient,
    shard_group: Option<&str>,
) -> anyhow::Result<BTreeSet<Arc<str>>> {
    let query = shard_group.map_or_else(
        || "SELECT hostName() AS host".to_owned(),
        |cluster| {
            format!(
                "SELECT DISTINCT hostName() AS host FROM clusterAllReplicas({}, system.one) ORDER BY host",
                super::table::quote_string_literal(cluster)
            )
        },
    );
    let batches = observe_external_request(
        "clickhouse",
        "speedtest_resolve_replicas",
        client.query_all(&query),
    )
    .await
    .map_err(|_| anyhow::anyhow!("ClickHouse speedtest could not resolve its replica scope"))?;
    let mut hosts = BTreeSet::new();
    for batch in batches {
        let column = batch
            .column_by_name("host")
            .ok_or_else(|| anyhow::anyhow!("ClickHouse replica query omitted 'host'"))?;
        append_strings(column.as_ref(), "host", |value| {
            hosts.insert(Arc::from(value));
        })?;
    }
    anyhow::ensure!(
        !hosts.is_empty(),
        "ClickHouse speedtest resolved an empty replica scope"
    );
    Ok(hosts)
}

fn append_strings(
    column: &dyn Array,
    label: &str,
    mut append: impl FnMut(&str),
) -> anyhow::Result<()> {
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        for value in values.iter().flatten() {
            append(value);
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        for value in values.iter().flatten() {
            append(std::str::from_utf8(value).map_err(|_| {
                anyhow::anyhow!("ClickHouse speedtest returned non-UTF-8 '{label}' metadata")
            })?);
        }
        return Ok(());
    }
    anyhow::bail!(
        "ClickHouse speedtest returned unsupported Arrow type {:?} for '{label}'",
        column.data_type()
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReplicaOwnershipEvidence {
    Owned,
    Missing,
    Unsafe,
}

pub(super) fn classify_replica_owners(
    expected_hosts: &BTreeSet<Arc<str>>,
    actual: &BTreeMap<Arc<str>, Option<Arc<str>>>,
    expected_marker: &str,
) -> ReplicaOwnershipEvidence {
    if actual.is_empty() {
        return ReplicaOwnershipEvidence::Missing;
    }
    if actual.keys().ne(expected_hosts.iter()) {
        return ReplicaOwnershipEvidence::Unsafe;
    }
    if actual
        .values()
        .all(|marker| marker.as_deref() == Some(expected_marker))
    {
        ReplicaOwnershipEvidence::Owned
    } else {
        ReplicaOwnershipEvidence::Unsafe
    }
}

pub(super) const fn replica_owner_allows_side_effect(evidence: ReplicaOwnershipEvidence) -> bool {
    matches!(evidence, ReplicaOwnershipEvidence::Owned)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DropCompletion {
    Complete,
    StillOwnedAfterSuccess,
    StillOwnedAfterFailure,
    Unsafe,
}

pub(super) const fn classify_drop_completion(
    command_succeeded: bool,
    evidence: ReplicaOwnershipEvidence,
) -> DropCompletion {
    match (command_succeeded, evidence) {
        (_, ReplicaOwnershipEvidence::Missing) => DropCompletion::Complete,
        (true, ReplicaOwnershipEvidence::Owned) => DropCompletion::StillOwnedAfterSuccess,
        (false, ReplicaOwnershipEvidence::Owned) => DropCompletion::StillOwnedAfterFailure,
        (_, ReplicaOwnershipEvidence::Unsafe) => DropCompletion::Unsafe,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CleanupOwnershipAction {
    AlreadyAbsent,
    VerifySchemaAndDrop,
    Preserve,
}

pub(super) const fn cleanup_ownership_action(
    evidence: ReplicaOwnershipEvidence,
) -> CleanupOwnershipAction {
    match evidence {
        ReplicaOwnershipEvidence::Missing => CleanupOwnershipAction::AlreadyAbsent,
        ReplicaOwnershipEvidence::Owned => CleanupOwnershipAction::VerifySchemaAndDrop,
        ReplicaOwnershipEvidence::Unsafe => CleanupOwnershipAction::Preserve,
    }
}

async fn clickhouse_owner_evidence(
    client: &ReconnectingClient,
    scope: &ClickHouseSpeedtestScope,
    table: &str,
) -> anyhow::Result<ReplicaOwnershipEvidence> {
    anyhow::ensure!(
        scope.tables.contains(table) && is_speedtest_table(table),
        "ClickHouse speedtest ownership verification rejected an unknown table"
    );
    let database = super::table::quote_string_literal(&scope.database);
    let table_literal = super::table::quote_string_literal(table);
    let query = scope.shard_group.as_deref().map_or_else(
        || {
            format!(
                "SELECT hostName() AS host, comment FROM system.tables WHERE database = {database} AND name = {table_literal}"
            )
        },
        |cluster| {
            format!(
                "SELECT hostName() AS host, comment FROM clusterAllReplicas({}, system.tables) WHERE database = {database} AND name = {table_literal} ORDER BY host",
                super::table::quote_string_literal(cluster)
            )
        },
    );
    let batches = observe_external_request(
        "clickhouse",
        "speedtest_verify_owner",
        client.query_all(&query),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("ClickHouse scratch table '{table}' owner marker is unreadable")
    })?;
    let mut owners = BTreeMap::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() == 2,
            "ClickHouse owner query for '{table}' returned {} columns instead of 2",
            batch.num_columns()
        );
        let hosts = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8)?;
        let hosts = hosts
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse owner-query hosts are not strings"))?;
        let comments = arrow::compute::cast(batch.column(1), &arrow::datatypes::DataType::Utf8)?;
        let comments = comments
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse owner-query comments are not strings"))?;
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !hosts.is_null(row),
                "ClickHouse owner query for '{table}' returned a NULL host"
            );
            let host: Arc<str> = Arc::from(hosts.value(row));
            let marker = (!comments.is_null(row)).then(|| Arc::from(comments.value(row)));
            anyhow::ensure!(
                owners.insert(host, marker).is_none(),
                "ClickHouse owner query for '{table}' returned a duplicate replica"
            );
        }
    }
    Ok(classify_replica_owners(
        &scope.replica_hosts,
        &owners,
        &scope.owner_marker,
    ))
}

async fn verify_owned_clickhouse_table(
    client: &ReconnectingClient,
    config: &ClickHouseSinkConfig,
    scope: &ClickHouseSpeedtestScope,
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        replica_owner_allows_side_effect(
            clickhouse_owner_evidence(client, scope, &dataset.table).await?
        ),
        "ClickHouse scratch table '{}' has a missing, unreadable, or foreign owner marker on at least one pinned replica",
        dataset.table
    );
    validate_speedtest_table(
        client,
        config,
        &dataset.table,
        &dataset.schema,
        dataset.role,
        dataset.changelog,
    )
    .await
}

async fn prepare_speedtest_tables(
    client: &ReconnectingClient,
    config: &ClickHouseSinkConfig,
    request: &SinkPrepare,
    scope: &ClickHouseSpeedtestScope,
) -> anyhow::Result<()> {
    let requested = request
        .datasets
        .iter()
        .map(|dataset| Arc::clone(&dataset.table))
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        requested == scope.tables,
        "ClickHouse speedtest preparation does not match its connector-owned scratch set"
    );
    for dataset in &request.datasets {
        scope.record_attempt(Arc::clone(&dataset.table));
        let ddl = speedtest_create_table_ddl(
            config,
            dataset,
            scope.shard_group.as_deref(),
            &scope.owner_marker,
        )?;
        let create_result = observe_external_request(
            "clickhouse",
            "speedtest_create_owned_table",
            client.execute(&ddl),
        )
        .await;
        let mut verified = verify_owned_clickhouse_table(client, config, scope, dataset).await;
        if create_result.is_err() || verified.is_err() {
            verified = verify_owned_clickhouse_table(client, config, scope, dataset).await;
        }
        if verified.is_err() {
            anyhow::bail!(
                "ClickHouse speedtest could not prove exclusive ownership of scratch table '{}' after CREATE",
                dataset.table
            );
        }
        scope.claim(Arc::clone(&dataset.table));
    }
    Ok(())
}

async fn verify_all_clickhouse_tables(
    client: &ReconnectingClient,
    config: &ClickHouseSinkConfig,
    scope: &ClickHouseSpeedtestScope,
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        scope.claimed_tables() == scope.tables,
        "ClickHouse speedtest cannot write before every scratch table is proven owned"
    );
    for dataset in &discovery.datasets {
        let prepared = transferia_registry::DatasetPrepare {
            namespace: None,
            role: dataset.role,
            table: Arc::clone(&dataset.name),
            schema: dataset.stored_schema.clone(),
            changelog: dataset
                .system_columns
                .iter()
                .any(|column| column.kind == SystemColumnKind::ChangeOperation),
        };
        verify_owned_clickhouse_table(client, config, scope, &prepared).await?;
    }
    Ok(())
}

async fn drop_owned_clickhouse_table(
    client: &ReconnectingClient,
    scope: &ClickHouseSpeedtestScope,
    table: &str,
) -> anyhow::Result<()> {
    match cleanup_ownership_action(clickhouse_owner_evidence(client, scope, table).await?) {
        CleanupOwnershipAction::AlreadyAbsent => {
            scope.unclaim(table);
            return Ok(());
        }
        CleanupOwnershipAction::VerifySchemaAndDrop => {}
        CleanupOwnershipAction::Preserve => anyhow::bail!(
            "ClickHouse scratch table '{table}' is not proven owned on every pinned replica immediately before cleanup"
        ),
    }
    let ddl = clickhouse_cleanup_ddl(&scope.database, table, scope.shard_group.as_deref())?;
    let drop_result =
        observe_external_request("clickhouse", "speedtest_drop_table", client.execute(&ddl)).await;
    let evidence = clickhouse_owner_evidence(client, scope, table).await?;
    match classify_drop_completion(drop_result.is_ok(), evidence) {
        DropCompletion::Complete => {
            scope.unclaim(table);
            Ok(())
        }
        DropCompletion::StillOwnedAfterSuccess => anyhow::bail!(
            "ClickHouse reported successful scratch DROP for '{table}', but at least one pinned replica still owns the table"
        ),
        DropCompletion::StillOwnedAfterFailure => anyhow::bail!(
            "ClickHouse scratch DROP for '{table}' failed and the table still exists on pinned replicas"
        ),
        DropCompletion::Unsafe => anyhow::bail!(
            "ClickHouse scratch DROP for '{table}' left an incomplete or unsafe replica state"
        ),
    }
}

#[cfg(test)]
#[path = "tests/connector.rs"]
mod tests;
