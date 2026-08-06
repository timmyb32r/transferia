use std::sync::Arc;
use std::collections::HashMap;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{ParserConfig, SchemaConfig};
use crate::pipeline::source::Source;
use crate::pipeline::sink::Sink;

// ---------------------------------------------------------------------------
// SourceProvider
// ---------------------------------------------------------------------------

pub trait SourceProvider: Send + Sync {
    fn build_source(
        &self,
        partition_id: i64,
        cancel_token: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>>;

    fn discover_partitions(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>>;

    fn resolve_table_name(&self) -> anyhow::Result<String>;
    fn parser_config(&self) -> Option<&ParserConfig>;

    /// Column schema for DDL. Defaults to `None` — the schema comes from
    /// `parser_config().settings`. Override for sources that derive the schema
    /// independently (e.g. `ClickHouse` source from DESCRIBE TABLE).
    fn schema_config(&self) -> Option<&SchemaConfig> {
        None
    }
}

// ---------------------------------------------------------------------------
// SinkProvider
// ---------------------------------------------------------------------------

pub trait SinkProvider: Send + Sync {
    fn build_sink(&self) -> BoxFuture<'_, anyhow::Result<Arc<dyn Sink>>>;
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
        Self { sources: HashMap::new(), sinks: HashMap::new() }
    }

    pub fn register_source<F: Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
        factory: F,
    ) {
        self.sources.insert(name, Box::new(factory));
    }

    pub fn register_sink<F: Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static>(
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
