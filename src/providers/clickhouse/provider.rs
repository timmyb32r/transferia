use alloc::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::sink::ClickHouseSink;
use crate::providers::traits::SinkProvider;

/// `ClickHouse` sink config.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct SinkConfig {
    pub connection_string: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_batch")]
    pub batch_size: usize,
    #[serde(default = "default_linger")]
    pub max_linger_ms: u64,
    #[serde(default = "default_connections")]
    pub max_connections: usize,
    #[serde(default = "default_tls")]
    pub use_tls: bool,
    #[serde(default)]
    pub tls_domain: Option<String>,
}

fn default_database() -> String { "default".into() }
fn default_username() -> String { "default".into() }
const fn default_batch() -> usize { 10000 }
const fn default_linger() -> u64 { 500 }
const fn default_connections() -> usize { 4 }
const fn default_tls() -> bool { true }

pub struct ClickHouseSinkProvider {
    cfg: SinkConfig,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: SinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse ClickHouse sink config: {e}"))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("clickhouse.connection_string must not be empty");
        }
        if cfg.database.is_empty() {
            anyhow::bail!("clickhouse.database must not be empty");
        }
        Ok(Self { cfg })
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn build_sink(&self) -> BoxFuture<'_, anyhow::Result<Arc<dyn Sink>>> {
        Box::pin(async move {
            let sink = ClickHouseSink::new(&self.cfg, 10_000).await?;
            Ok(Arc::new(sink) as Arc<dyn Sink>)
        })
    }
}
