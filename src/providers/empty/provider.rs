use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;

use crate::config::yaml::SchemaConfig;
use crate::pipeline::sink::Sink;
use crate::providers::empty::sink::EmptySink;
use crate::providers::traits::SinkProvider;

/// Empty sink config — only accepts a `batch_size` hint for consistency with
/// other sink configs, but does nothing with it (rows are discarded).
#[derive(Debug, Deserialize)]
pub struct EmptySinkConfig {
    #[serde(default = "default_batch")]
    pub batch_size: usize,
}

fn default_batch() -> usize {
    10000
}

pub struct EmptySinkProvider {
    sink: Arc<EmptySink>,
}

impl EmptySinkProvider {
    pub fn from_config(_value: Value) -> anyhow::Result<Self> {
        // Config is minimal — just validate it parses.
        let _cfg: EmptySinkConfig = serde_yaml::from_value(_value)
            .map_err(|e| anyhow::anyhow!("Failed to parse empty sink config: {}", e))?;
        Ok(Self { sink: Arc::new(EmptySink::new()) })
    }
}

impl SinkProvider for EmptySinkProvider {
    fn build_sink<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<Arc<dyn Sink>>> {
        let sink = self.sink.clone();
        Box::pin(async move { Ok(sink as Arc<dyn Sink>) })
    }

    fn create_tables<'a>(
        &'a self,
        table: &str,
        dlq_table: &str,
        _schema: &'a SchemaConfig,
        _recreate: bool,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        tracing::info!(
            "empty-sink: skipping table creation for '{}' and '{}'",
            table,
            dlq_table
        );
        Box::pin(async move { Ok(()) })
    }

    fn verify_tables<'a>(
        &'a self,
        table: &str,
        dlq_table: &str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        tracing::info!(
            "empty-sink: skipping table verification for '{}' and '{}'",
            table,
            dlq_table
        );
        Box::pin(async move { Ok(()) })
    }
}
