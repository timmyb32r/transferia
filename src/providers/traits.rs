use alloc::sync::Arc;
use std::collections::HashMap;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::compatibility::EndpointDescriptor;
use crate::delivery::{DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, SinkLimits};
use crate::durable::DurableContext;
use crate::metrics::SinkCounters;
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::sink::Sink;
use crate::pipeline::source::Source;
use crate::types::schema::DatasetSchema;

// ---------------------------------------------------------------------------
// SourceProvider
// ---------------------------------------------------------------------------

pub trait SourceProvider: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;
    fn delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>>;

    fn build_source(
        &self,
        partition_id: i64,
        cancel_token: CancellationToken,
        memory: PipelineMemory,
        durable: DurableContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>>;

    fn partitions_for_worker(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>>;

    fn parser_plan(&self) -> &ParserPlan;
}

// ---------------------------------------------------------------------------
// SinkProvider
// ---------------------------------------------------------------------------

pub trait SinkProvider: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor;
    fn limits(&self) -> &dyn SinkLimits;
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Validate constraints that span the global pipeline and sink-specific
    /// buffering configuration.
    fn validate_pipeline_memory_limit(&self, _limit_bytes: usize) -> anyhow::Result<()> {
        Ok(())
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>>;
}

pub struct SinkContext {
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

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

type SourceFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync>;

pub struct ProviderRegistry {
    sources: HashMap<&'static str, SourceFactory>,
    sinks: HashMap<&'static str, SinkFactory>,
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
        }
    }

    pub fn register_source<
        F: Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static,
    >(
        &mut self,
        name: &'static str,
        factory: F,
    ) {
        self.sources.insert(name, Box::new(factory));
    }

    pub fn register_sink<
        F: Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static,
    >(
        &mut self,
        name: &'static str,
        factory: F,
    ) {
        self.sinks.insert(name, Box::new(factory));
    }

    pub fn build_source(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SourceProvider>> {
        match self.sources.get(kind) {
            Some(f) => f(raw),
            None => anyhow::bail!(
                "Unknown source provider '{}'; registered: {:?}",
                kind,
                self.sources.keys().collect::<Vec<_>>(),
            ),
        }
    }

    pub fn build_sink(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SinkProvider>> {
        match self.sinks.get(kind) {
            Some(f) => f(raw),
            None => anyhow::bail!(
                "Unknown sink provider '{}'; registered: {:?}",
                kind,
                self.sinks.keys().collect::<Vec<_>>(),
            ),
        }
    }
}
