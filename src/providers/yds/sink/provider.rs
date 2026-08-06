use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;

use crate::config::yaml::SchemaConfig;
use crate::pipeline::sink::Sink;
use crate::providers::traits::SinkProvider;
use crate::providers::yds::sink::writer::YdsSink;
use crate::serializer::Serializer;
use crate::serializer::json_serializer::JsonSerializer;

#[derive(Debug, Deserialize)]
pub struct YdsSinkConfig {
    /// YDB connection string (e.g. "grpc://localhost:2135/local").
    pub connection_string: String,
    /// YDS topic path.
    pub topic_path: String,
    /// YDB database (default: "/Root").
    #[serde(default = "default_database")]
    pub database: String,
    /// Serializer type (currently only "json").
    #[serde(default = "default_serializer")]
    pub serializer_type: String,
}

fn default_database() -> String { "/Root".into() }
fn default_serializer() -> String { "json".into() }

pub struct YdsSinkProvider {
    _topic_path: String,
    serializer: Arc<dyn Serializer>,
}

impl YdsSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: YdsSinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse YDS sink config: {}", e))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("yds sink: connection_string must not be empty");
        }
        if cfg.topic_path.is_empty() {
            anyhow::bail!("yds sink: topic_path must not be empty");
        }

        let serializer: Arc<dyn Serializer> = match cfg.serializer_type.as_str() {
            "json" => Arc::new(JsonSerializer),
            other => anyhow::bail!("Unknown YDS sink serializer: {}", other),
        };

        tracing::info!(
            "YDS sink: topic={} serializer={}",
            cfg.topic_path, cfg.serializer_type,
        );

        Ok(Self {
            _topic_path: cfg.topic_path,
            serializer,
        })
    }
}

impl SinkProvider for YdsSinkProvider {
    fn build_sink<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<Arc<dyn Sink>>> {
        let sink = Arc::new(YdsSink::new(self.serializer.clone()));
        Box::pin(async move { Ok(sink as Arc<dyn Sink>) })
    }

    fn create_tables<'a>(
        &'a self,
        table: &str,
        _dlq_table: &str,
        _schema: &'a SchemaConfig,
        _recreate: bool,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        tracing::info!("YDS sink: skipping table creation for '{}'", table);
        Box::pin(async move { Ok(()) })
    }

}
