use alloc::collections::BTreeMap;
use alloc::sync::Arc;

use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Default, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionCheckStatus {
    #[default]
    Verified,

    NetworkReachable,
}

#[derive(Clone, Debug, Default, serde::Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCheckResult {
    pub status: ConnectionCheckStatus,

    pub message: Option<String>,

    pub options: BTreeMap<String, Vec<String>>,
}

impl ConnectionCheckResult {
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

use crate::core::data::schema::{DatasetSchema, SchemaColumn};
use crate::core::delivery::{DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, SinkLimits};
use crate::core::memory::PipelineMemory;
use crate::core::sink::Sink;
use crate::core::source::Source;
use crate::delivery::semantics::EndpointDescriptor;
use crate::durable::DurableContext;
use crate::metrics::SinkCounters;
use crate::parsers::ParserPlan;

// ---------------------------------------------------------------------------
// SourceProvider
// ---------------------------------------------------------------------------

pub trait SourceProvider: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;
    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>>;

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>>;

    fn parser_plan(&self) -> &ParserPlan;
}

pub struct SourceDiscoveryContext {
    pub request: DeliveryDiscoveryRequest,
    pub cancellation: CancellationToken,
}

pub struct SourceBuildContext {
    pub partition_id: i64,
    pub cancellation: CancellationToken,
    pub memory: PipelineMemory,
    pub durable: DurableContext,
}

// ---------------------------------------------------------------------------
// SinkProvider
// ---------------------------------------------------------------------------

pub trait SinkProvider: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;
    fn limits(&self) -> &dyn SinkLimits;
    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String>;
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Validate constraints that span the global pipeline and sink-specific
    /// buffering configuration.
    fn validate_pipeline_memory_limit(&self, _limit_bytes: usize) -> anyhow::Result<()> {
        Ok(())
    }

    fn build_sink(&self, context: SinkBuildContext)
        -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>>;
}

pub struct SinkBuildContext {
    pub partition_id: i64,
    pub counters: Arc<SinkCounters>,
    pub keep_system_columns: bool,
    pub discovery: Arc<DeliveryDiscovery>,
    pub durable: DurableContext,
}

pub struct SinkPrepare {
    pub datasets: Vec<DatasetPrepare>,
}

pub struct DatasetPrepare {
    pub role: DatasetRole,
    pub table: Arc<str>,
    pub schema: DatasetSchema,
}

impl SinkPrepare {
    pub fn from_discovery(discovery: &DeliveryDiscovery) -> anyhow::Result<Option<Self>> {
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
                })
                .collect(),
        }))
    }
}
