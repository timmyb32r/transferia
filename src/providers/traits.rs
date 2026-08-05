use std::sync::Arc;
use std::collections::HashMap;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{ParserConfig, MiddlewareConfig, SchemaConfig};
use crate::pipeline::source::Source;
use crate::pipeline::sink::Sink;

// ---------------------------------------------------------------------------
// SourceProvider
// ---------------------------------------------------------------------------

pub trait SourceProvider: Send + Sync {
    fn build_source<'a>(
        &'a self,
        partition_id: i64,
        cancel_token: CancellationToken,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn Source>>>;

    fn discover_partitions<'a>(
        &'a self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'a, anyhow::Result<Vec<i64>>>;

    fn resolve_table_name(&self) -> anyhow::Result<String>;
    fn parser_config(&self) -> &ParserConfig;
}

// ---------------------------------------------------------------------------
// SinkProvider
// ---------------------------------------------------------------------------

pub trait SinkProvider: Send + Sync {
    fn build_sink<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<Arc<dyn Sink>>>;

    fn create_tables<'a>(
        &'a self,
        table: &str,
        dlq_table: &str,
        schema: &'a SchemaConfig,
        recreate: bool,
    ) -> BoxFuture<'a, anyhow::Result<()>>;

    fn verify_tables<'a>(
        &'a self,
        table: &str,
        dlq_table: &str,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub struct ProviderRegistry {
    sources: HashMap<&'static str, Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync>>,
    sinks: HashMap<&'static str, Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self { sources: HashMap::new(), sinks: HashMap::new() }
    }

    pub fn register_source(
        &mut self,
        name: &'static str,
        factory: impl Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static,
    ) {
        self.sources.insert(name, Box::new(factory));
    }

    pub fn register_sink(
        &mut self,
        name: &'static str,
        factory: impl Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static,
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
