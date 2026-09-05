use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, PerformanceAdvice, SinkLimits,
    SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::Sink;
use transferia_core::source::Source;
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_delivery_contracts::parser::ParserFactory;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_delivery_contracts::DeliveryType;

use crate::durable::DurableContext;

#[derive(Clone, Copy, Debug, Default, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionCheckStatus {
    #[default]
    Verified,

    NetworkReachable,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCheckResult {
    pub status: ConnectionCheckStatus,

    pub message: Option<String>,

    pub options: BTreeMap<String, Vec<String>>,
}

impl Default for ConnectionCheckResult {
    fn default() -> Self {
        Self {
            status: ConnectionCheckStatus::Verified,
            message: Some(
                "Connection verified, including access to the configured entities.".to_owned(),
            ),
            options: BTreeMap::new(),
        }
    }
}

impl ConnectionCheckResult {
    #[must_use]
    pub fn network_reachable() -> Self {
        Self {
            status: ConnectionCheckStatus::NetworkReachable,
            message: Some(
                "Network connection is available, but authentication was not checked.".to_owned(),
            ),
            options: BTreeMap::new(),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum EndpointRole {
    Source,
    Sink,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicOption {
    pub value: String,

    pub label: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicOptions {
    pub options: Vec<DynamicOption>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub warning: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptionsRequest {
    #[serde(default)]
    pub query: Option<String>,

    #[serde(default)]
    pub refresh: bool,

    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SpeedtestUnsupported {
    #[error("this source cannot create a non-disruptive isolated speedtest reader")]
    SourceIsolation,

    #[error("this destination cannot create and clean up an isolated speedtest target")]
    DestinationIsolation,

    #[error("this destination does not implement speedtest scratch cleanup")]
    DestinationCleanup,
}

pub trait SourceConnector: Send + Sync {
    /// Describe the record semantics of the requested mode, not a UI toggle.
    fn compatibility(&self, delivery_type: DeliveryType) -> EndpointDescriptor;

    /// Reject source configurations whose proven peak source-side working set
    /// cannot fit the delivery's explicit per-partition memory budget.
    fn validate_pipeline_memory_limit(&self, _limit_bytes: usize) -> anyhow::Result<()> {
        Ok(())
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>>;

    /// Called only by an assigned worker after preview planning and before
    /// destination preparation or any destination side effect. A connector
    /// may create source-side execution state and return the authoritative raw
    /// discovery plus the phases which still need to run. Remaining phases must
    /// be an exact, non-empty suffix of [`Self::execution_phases`], allowing a
    /// durable connector to resume after a completed phase without replaying it.
    /// The default does no additional I/O.
    fn prepare_execution(
        &self,
        _context: SourceExecutionContext,
    ) -> BoxFuture<'_, anyhow::Result<Option<PreparedSourceExecution>>> {
        Box::pin(async { Ok(None) })
    }

    /// Return the ordered execution phases for this source and delivery mode.
    /// Combined delivery requires a connector-owned plan because a generic
    /// runner cannot infer the snapshot/stream transition safely. Until the
    /// runner has a distributed phase barrier, every multi-phase topology must
    /// use `CoLocatedStaticPartitions` so one worker owns the whole transition.
    fn execution_phases(
        &self,
        delivery_type: DeliveryType,
        discovery: &DeliveryDiscovery,
    ) -> anyhow::Result<Vec<SourceExecutionPhase>> {
        match delivery_type {
            DeliveryType::Batch => Ok(vec![SourceExecutionPhase {
                phase: SourcePhase::Snapshot,
                topology: discovery.source_topology.clone(),
                finite: true,
            }]),
            DeliveryType::Stream => Ok(vec![SourceExecutionPhase {
                phase: SourcePhase::Stream,
                topology: discovery.source_topology.clone(),
                finite: false,
            }]),
            DeliveryType::BatchAndStream => anyhow::bail!(
                "batch_and_stream delivery requires an explicit connector execution phase plan"
            ),
        }
    }

    /// Finalize connector-owned state after every phase partition task in this
    /// process has completed successfully. Multi-phase plans are currently
    /// restricted to co-located partitions owned by one worker, so this is not
    /// a distributed barrier.
    fn complete_execution_phase(
        &self,
        _phase: SourcePhase,
        _durable: DurableContext,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>>;

    /// Build a source reader which cannot advance, contend with, or rebalance
    /// the configured production cursor/consumer/replication slot.
    ///
    /// Finite snapshot connectors may delegate to [`Self::build_source`] when
    /// reading is side-effect free. Streaming connectors must instead create a
    /// connector-owned isolated identity. The default fails closed so adding a
    /// source cannot silently make a destructive speed test available.
    fn build_speedtest_source(
        &self,
        _context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async { Err(SpeedtestUnsupported::SourceIsolation.into()) })
    }

    fn parser(&self) -> Arc<dyn ParserFactory>;

    fn parses_rows(&self) -> bool;
}

pub struct SourceDiscoveryContext {
    pub request: DeliveryDiscoveryRequest,

    pub cancellation: CancellationToken,

    pub delivery_type: DeliveryType,
}

pub struct SourceExecutionContext {
    pub request: DeliveryDiscoveryRequest,

    pub cancellation: CancellationToken,

    pub delivery_type: DeliveryType,

    /// Exact, non-secret identity of the replay-affecting delivery
    /// configuration. Durable sources must bind resumable state to this value
    /// and reject a mismatch rather than replaying it under new semantics.
    pub replay_identity: Option<Arc<str>>,

    pub durable: DurableContext,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourcePhase {
    Snapshot,

    Stream,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceExecutionPhase {
    pub phase: SourcePhase,

    pub topology: SourceTopology,

    pub finite: bool,
}

#[derive(Clone, Debug)]
pub struct PreparedSourceExecution {
    /// Discovery produced from the source-side execution state.
    pub discovery: DeliveryDiscovery,

    /// Exact non-empty suffix of the preview phase plan which still must run.
    pub remaining_phases: Vec<SourceExecutionPhase>,
}

pub struct SourceBuildContext {
    pub partition_id: i64,

    pub delivery_type: DeliveryType,

    pub phase: SourcePhase,

    /// See [`SourceExecutionContext::replay_identity`].
    pub replay_identity: Option<Arc<str>>,

    pub cancellation: CancellationToken,

    pub memory: PipelineMemory,

    pub durable: DurableContext,
}

pub trait SinkConnector: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;

    /// Delivery modes accepted by this configured destination, independently of record semantics.
    fn delivery_modes(&self) -> &'static [DeliveryType] {
        &[DeliveryType::Batch, DeliveryType::Stream, DeliveryType::BatchAndStream]
    }

    fn validate_delivery_type(&self, delivery_type: DeliveryType) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.delivery_modes().contains(&delivery_type),
            "configured destination does not support '{}' delivery; allowed modes: {}",
            delivery_type.label(),
            self.delivery_modes().iter().map(|mode| format!("'{}'", mode.label())).collect::<Vec<_>>().join(", ")
        );
        Ok(())
    }

    fn limits(&self) -> &dyn SinkLimits;

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String>;

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>>;

    fn validate_pipeline_memory_limit(&self, _limit_bytes: usize) -> anyhow::Result<()> {
        Ok(())
    }

    fn performance_advice(&self) -> Vec<PerformanceAdvice> {
        Vec::new()
    }

    /// Describe a constant-time, exact row-count check for a completed finite
    /// snapshot. The generic delivery runner never falls back to a table scan
    /// or an approximate catalog statistic.
    ///
    /// `AdditiveBaseline` is appropriate when the snapshot appends to the
    /// destination: the runner persists the initial count before preparation
    /// and expects `final == baseline + output`. `ReplacedTotal` is appropriate
    /// only when completion atomically replaces the complete destination
    /// dataset and therefore expects `final == output`.
    fn snapshot_row_count_strategy(&self) -> Option<SnapshotRowCountStrategy> {
        None
    }

    /// Read exact destination row totals through an O(1) metadata operation.
    /// Implementations must return one entry for every discovered dataset,
    /// including an explicit `exists=false` entry for an absent destination.
    /// This method is called only when [`Self::snapshot_row_count_strategy`]
    /// returned `Some`.
    fn snapshot_row_counts<'a>(
        &'a self,
        _discovery: &'a DeliveryDiscovery,
    ) -> BoxFuture<'a, anyhow::Result<Vec<SnapshotDatasetRowCount>>> {
        Box::pin(async {
            anyhow::bail!(
                "destination declared snapshot row-count verification without implementing its metadata probe"
            )
        })
    }

    fn build_sink(&self, context: SinkBuildContext)
        -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>>;

    /// Build a connector-owned, externally isolated scratch namespace for a
    /// destructive throughput probe.
    ///
    /// The default deliberately rejects the probe: a generic control plane
    /// cannot safely infer how to create and later remove scratch entities for
    /// an arbitrary destination. Implementations must guarantee that every
    /// rewritten dataset is disjoint from configured production entities and
    /// that [`Self::cleanup_speedtest`] removes only those rewritten entities.
    /// This method itself must be side-effect free: external scratch creation
    /// begins only in [`Self::prepare`], after the runtime has installed its
    /// mandatory cleanup guard.
    fn isolate_speedtest(
        self: Arc<Self>,
        _discovery: Arc<DeliveryDiscovery>,
        _isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async { Err(SpeedtestUnsupported::DestinationIsolation.into()) })
    }

    /// Remove entities created by [`Self::isolate_speedtest`]. Cleanup must be
    /// idempotent so it is safe after partial preparation and failed probes.
    fn cleanup_speedtest<'a>(
        &'a self,
        _isolation: &'a SinkSpeedtestIsolation,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Err(SpeedtestUnsupported::DestinationCleanup.into()) })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotRowCountStrategy {
    /// The destination retains pre-existing rows and the snapshot adds rows.
    AdditiveBaseline,

    /// The completed snapshot replaces the destination's full contents.
    ReplacedTotal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotDatasetRowCount {
    pub role: DatasetRole,

    pub table: Arc<str>,

    /// Credential-free physical identity used to reject a persisted baseline
    /// after the destination configuration changes.
    pub target: Arc<str>,

    pub exists: bool,

    pub rows: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkSpeedtestIsolationSafety {
    /// The sink is observational/in-memory and performs no external writes.
    NoExternalWrites,

    /// Every output is rewritten into a connector-owned scratch namespace.
    Scratch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeedtestPhysicalTarget {
    pub production: Arc<str>,

    pub scratch: Arc<str>,
}

#[derive(Clone)]
pub struct SinkSpeedtestIsolation {
    connector: Arc<dyn SinkConnector>,

    pub discovery: Arc<DeliveryDiscovery>,

    safety: SinkSpeedtestIsolationSafety,

    table_names: BTreeMap<Arc<str>, Arc<str>>,

    physical_targets: Vec<SpeedtestPhysicalTarget>,
}

impl SinkSpeedtestIsolation {
    /// Construct an identity mapping only for a sink which performs no
    /// external writes (for example, the benchmark discard sink).
    #[must_use]
    pub fn no_external_writes(
        connector: Arc<dyn SinkConnector>,
        discovery: Arc<DeliveryDiscovery>,
    ) -> Self {
        let table_names = discovery
            .datasets
            .iter()
            .map(|dataset| (Arc::clone(&dataset.name), Arc::clone(&dataset.name)))
            .collect();
        let connector: Arc<dyn SinkConnector> = Arc::new(CheckedSpeedtestSinkConnector {
            connector,
            discovery: Arc::clone(&discovery),
        });
        Self {
            connector,
            discovery,
            safety: SinkSpeedtestIsolationSafety::NoExternalWrites,
            table_names,
            physical_targets: Vec::new(),
        }
    }

    /// Construct a scratch mapping for an external sink.
    ///
    /// Logical dataset names may remain unchanged when the connector maps them
    /// to explicit physical paths. Physical scratch targets are therefore the
    /// authoritative safety boundary and must be non-empty, unique, and
    /// disjoint from every configured production target.
    pub fn scratch(
        connector: Arc<dyn SinkConnector>,
        original: &DeliveryDiscovery,
        discovery: DeliveryDiscovery,
        table_names: BTreeMap<Arc<str>, Arc<str>>,
        physical_targets: Vec<SpeedtestPhysicalTarget>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            original.datasets.len() == discovery.datasets.len(),
            "speedtest isolation must preserve every discovered dataset"
        );
        anyhow::ensure!(
            original.source_name == discovery.source_name
                && original.source_topology == discovery.source_topology
                && original.schema_origin == discovery.schema_origin
                && original.keep_system_columns == discovery.keep_system_columns,
            "speedtest isolation must preserve source discovery semantics"
        );
        let original_names = original
            .datasets
            .iter()
            .map(|dataset| dataset.name.as_ref())
            .collect::<std::collections::BTreeSet<_>>();
        let isolated_names = discovery
            .datasets
            .iter()
            .map(|dataset| dataset.name.as_ref())
            .collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(
            original_names.len() == original.datasets.len(),
            "speedtest isolation requires unique original dataset names"
        );
        anyhow::ensure!(
            isolated_names.len() == discovery.datasets.len(),
            "speedtest isolation requires unique isolated dataset names"
        );
        anyhow::ensure!(
            table_names.len() == original.datasets.len(),
            "speedtest isolation must map every discovered dataset exactly once"
        );
        let mapped_original_names = table_names
            .keys()
            .map(AsRef::as_ref)
            .collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(
            mapped_original_names == original_names,
            "speedtest isolation mapping must contain exactly the original datasets"
        );
        let unique_names = table_names
            .values()
            .map(AsRef::as_ref)
            .collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(
            unique_names.len() == table_names.len(),
            "speedtest scratch dataset names must be unique"
        );
        anyhow::ensure!(
            unique_names == isolated_names,
            "speedtest isolation mapping must contain exactly the isolated datasets"
        );
        for original_dataset in &original.datasets {
            let isolated_name = table_names.get(&original_dataset.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "speedtest isolation has no mapping for dataset '{}'",
                    original_dataset.name
                )
            })?;
            let isolated_dataset = discovery
                .datasets
                .iter()
                .find(|dataset| dataset.name == *isolated_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "speedtest isolation maps dataset '{}' to missing dataset '{}'",
                        original_dataset.name,
                        isolated_name
                    )
                })?;
            anyhow::ensure!(
                datasets_are_equivalent(original_dataset, isolated_dataset),
                "speedtest isolation changed the schema or role of dataset '{}'",
                original_dataset.name
            );
        }
        anyhow::ensure!(
            !physical_targets.is_empty(),
            "speedtest scratch isolation must declare its physical targets"
        );
        anyhow::ensure!(
            physical_targets.len() == original.datasets.len(),
            "speedtest scratch isolation must declare exactly one physical target per dataset"
        );
        let production_targets = physical_targets
            .iter()
            .map(|target| target.production.as_ref())
            .collect::<std::collections::BTreeSet<_>>();
        let scratch_targets = physical_targets
            .iter()
            .map(|target| target.scratch.as_ref())
            .collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(
            production_targets.len() == physical_targets.len(),
            "speedtest production physical targets must be unique"
        );
        anyhow::ensure!(
            scratch_targets.len() == physical_targets.len(),
            "speedtest scratch physical targets must be unique"
        );
        for target in &physical_targets {
            anyhow::ensure!(
                !target.production.is_empty() && !target.scratch.is_empty(),
                "speedtest physical target identifiers must not be empty"
            );
            anyhow::ensure!(
                !production_targets.contains(target.scratch.as_ref()),
                "speedtest scratch physical target '{}' aliases a production target",
                target.scratch
            );
        }
        let discovery = Arc::new(discovery);
        let connector: Arc<dyn SinkConnector> = Arc::new(CheckedSpeedtestSinkConnector {
            connector,
            discovery: Arc::clone(&discovery),
        });
        Ok(Self {
            connector,
            discovery,
            safety: SinkSpeedtestIsolationSafety::Scratch,
            table_names,
            physical_targets,
        })
    }

    #[must_use]
    pub const fn safety(&self) -> SinkSpeedtestIsolationSafety {
        self.safety
    }

    #[must_use]
    pub fn connector(&self) -> &Arc<dyn SinkConnector> {
        &self.connector
    }

    #[must_use]
    pub fn physical_targets(&self) -> &[SpeedtestPhysicalTarget] {
        &self.physical_targets
    }

    pub fn table_name(&self, original: &str) -> anyhow::Result<Arc<str>> {
        self.table_names.get(original).cloned().ok_or_else(|| {
            anyhow::anyhow!("speedtest isolation has no mapping for dataset '{original}'")
        })
    }
}

struct CheckedSpeedtestSinkConnector {
    connector: Arc<dyn SinkConnector>,

    discovery: Arc<DeliveryDiscovery>,
}

impl SinkConnector for CheckedSpeedtestSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        self.connector.compatibility()
    }

    fn delivery_modes(&self) -> &'static [DeliveryType] {
        self.connector.delivery_modes()
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.connector.limits()
    }

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String> {
        self.connector.destination_type(column)
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        let validation = validate_speedtest_prepare(&self.discovery, &request);
        let connector = Arc::clone(&self.connector);
        Box::pin(async move {
            validation?;
            connector.prepare(request).await
        })
    }

    fn validate_pipeline_memory_limit(&self, limit_bytes: usize) -> anyhow::Result<()> {
        self.connector.validate_pipeline_memory_limit(limit_bytes)
    }

    fn performance_advice(&self) -> Vec<PerformanceAdvice> {
        self.connector.performance_advice()
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        let validation = validate_speedtest_build_context(&self.discovery, &context);
        let connector = Arc::clone(&self.connector);
        Box::pin(async move {
            validation?;
            connector.build_sink(context).await
        })
    }

    fn cleanup_speedtest<'a>(
        &'a self,
        isolation: &'a SinkSpeedtestIsolation,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        self.connector.cleanup_speedtest(isolation)
    }
}

/// Verify that a speedtest connector received the exact isolated discovery
/// approved by [`SinkSpeedtestIsolation::scratch`].
pub fn validate_speedtest_discovery(
    expected: &DeliveryDiscovery,
    actual: &DeliveryDiscovery,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected.source_name == actual.source_name
            && expected.source_topology == actual.source_topology
            && expected.schema_origin == actual.schema_origin
            && expected.keep_system_columns == actual.keep_system_columns,
        "speedtest sink context changed source discovery semantics"
    );
    anyhow::ensure!(
        expected.datasets.len() == actual.datasets.len(),
        "speedtest sink context changed the isolated dataset count"
    );
    for (expected, actual) in expected.datasets.iter().zip(&actual.datasets) {
        anyhow::ensure!(
            expected.name == actual.name && datasets_are_equivalent(expected, actual),
            "speedtest sink context changed isolated dataset '{}'",
            expected.name
        );
    }
    Ok(())
}

/// Verify the destructive preparation boundary before an isolated connector
/// performs any external I/O.
pub fn validate_speedtest_prepare(
    expected: &DeliveryDiscovery,
    request: &SinkPrepare,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected.datasets.len() == request.datasets.len(),
        "speedtest prepare changed the isolated dataset count"
    );
    for (expected, actual) in expected.datasets.iter().zip(&request.datasets) {
        let same_table = expected.name == actual.table;
        let changelog = expected.system_columns.iter().any(|column| {
            column.kind == transferia_core::data::system_columns::SystemColumnKind::ChangeOperation
        });
        anyhow::ensure!(
            same_table
                && expected.role == actual.role
                && schemas_are_equivalent(&expected.stored_schema, &actual.schema)
                && changelog == actual.changelog,
            "speedtest prepare changed isolated dataset '{}'",
            expected.name
        );
    }
    Ok(())
}

/// Verify the runtime sink boundary before constructing a destructive writer.
pub fn validate_speedtest_build_context(
    expected: &DeliveryDiscovery,
    context: &SinkBuildContext,
) -> anyhow::Result<()> {
    validate_speedtest_discovery(expected, &context.discovery)?;
    anyhow::ensure!(
        context.keep_system_columns == expected.keep_system_columns,
        "speedtest sink context changed system-column retention"
    );
    Ok(())
}

fn datasets_are_equivalent(
    original: &transferia_core::delivery::DiscoveredDataset,
    isolated: &transferia_core::delivery::DiscoveredDataset,
) -> bool {
    original.role == isolated.role
        && schemas_are_equivalent(&original.incoming_schema, &isolated.incoming_schema)
        && schemas_are_equivalent(&original.stored_schema, &isolated.stored_schema)
        && original.system_columns == isolated.system_columns
}

fn schemas_are_equivalent(original: &DatasetSchema, isolated: &DatasetSchema) -> bool {
    original.columns.len() == isolated.columns.len()
        && original
            .columns
            .iter()
            .zip(&isolated.columns)
            .all(|(original, isolated)| {
                original.name == isolated.name
                    && original.data_type == isolated.data_type
                    && original.nullable == isolated.nullable
                    && original.primary_key == isolated.primary_key
                    && original.low_cardinality == isolated.low_cardinality
                    && original.max_length == isolated.max_length
                    && original.arrow_extension_name == isolated.arrow_extension_name
                    && original.system_role == isolated.system_role
                    && original.old_value_of == isolated.old_value_of
                    && original.old_key_of == isolated.old_key_of
            })
}

pub struct SinkBuildContext {
    pub partition_id: i64,

    /// User-visible delivery name used by destination formats that identify the source.
    pub delivery_name: Arc<str>,

    /// Stable identity of the prepared source execution, when the source defines one.
    pub replay_identity: Option<Arc<str>>,

    /// Whether the source replays the same finite snapshot after a partition restart.
    pub finite_source: bool,

    pub counters: Arc<SinkCounters>,

    pub keep_system_columns: bool,

    pub discovery: Arc<DeliveryDiscovery>,

    pub durable: DurableContext,
}

pub struct SinkPrepare {
    pub datasets: Vec<DatasetPrepare>,

    pub finite_source: bool,

    pub transfer_id: Arc<str>,

    /// Stable identity of the prepared source execution, when the source defines one.
    pub replay_identity: Option<Arc<str>>,
}

pub struct DatasetPrepare {
    pub role: DatasetRole,

    pub table: Arc<str>,

    pub schema: DatasetSchema,

    pub changelog: bool,
}

impl SinkPrepare {
    pub fn from_discovery(
        discovery: &DeliveryDiscovery,
        finite_source: bool,
        transfer_id: impl Into<Arc<str>>,
        replay_identity: Option<Arc<str>>,
    ) -> anyhow::Result<Option<Self>> {
        if discovery.datasets.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            datasets: discovery
                .datasets
                .iter()
                .map(|dataset| DatasetPrepare {
                    role: dataset.role,
                    table: Arc::clone(&dataset.name),
                    schema: dataset.stored_schema.clone(),
                    changelog: dataset
                        .system_columns
                        .iter()
                        .any(|column| {
                            column.kind
                                == transferia_core::data::system_columns::SystemColumnKind::ChangeOperation
                        }),
                })
                .collect(),
            finite_source,
            transfer_id: transfer_id.into(),
            replay_identity,
        }))
    }
}
