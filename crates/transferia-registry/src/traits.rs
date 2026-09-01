use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, SinkLimits,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::Sink;
use transferia_core::source::Source;
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_delivery_contracts::parser::ParserFactory;
use transferia_delivery_contracts::semantics::EndpointDescriptor;

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

pub trait SourceConnector: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>>;

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>>;

    fn parser(&self) -> Arc<dyn ParserFactory>;

    fn parses_rows(&self) -> bool;
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

pub trait SinkConnector: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;

    fn limits(&self) -> &dyn SinkLimits;

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String>;

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>>;

    fn validate_pipeline_memory_limit(&self, _limit_bytes: usize) -> anyhow::Result<()> {
        Ok(())
    }

    fn build_sink(&self, context: SinkBuildContext)
        -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>>;
}

pub struct SinkBuildContext {
    pub partition_id: i64,

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
        }))
    }
}
