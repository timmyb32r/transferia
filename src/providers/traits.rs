use alloc::sync::Arc;
use std::collections::HashMap;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::compatibility::EndpointDescriptor;
use crate::metrics::SinkCounters;
use crate::parsers::ParserConfig;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::sink::Sink;
use crate::pipeline::source::Source;
use crate::types::schema::DatasetSchema;

// ---------------------------------------------------------------------------
// SourceProvider
// ---------------------------------------------------------------------------

pub trait SourceProvider: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Other
    }
    fn build_source(
        &self,
        partition_id: i64,
        cancel_token: CancellationToken,
        memory: PipelineMemory,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>>;

    fn discover_partitions(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>>;

    fn resolve_table_name(&self) -> anyhow::Result<String>;
    fn parser_config(&self) -> Option<&ParserConfig>;

    /// Runtime dataset schema produced by this source/parser pipeline.
    fn schema(&self) -> Option<&DatasetSchema> {
        None
    }
}

// ---------------------------------------------------------------------------
// SinkProvider
// ---------------------------------------------------------------------------

pub trait SinkProvider: Send + Sync {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Other
    }
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
}

pub struct SinkPrepare {
    pub table: Arc<str>,
    pub schema: DatasetSchema,
    pub dlq_table: Arc<str>,
    pub dlq_schema: DatasetSchema,
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
