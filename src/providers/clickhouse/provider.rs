use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;

use crate::config::yaml::SchemaConfig;
use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::sink::ClickHouseSink;
use crate::providers::traits::SinkProvider;

/// ClickHouse sink config (extracted from common `SinkConfig` for provider isolation).
#[derive(Debug, Deserialize)]
pub struct ClickHouseSinkConfig {
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
fn default_batch() -> usize { 10000 }
fn default_linger() -> u64 { 500 }
fn default_connections() -> usize { 4 }
fn default_tls() -> bool { true }

pub struct ClickHouseSinkProvider {
    cfg: ClickHouseSinkConfig,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: ClickHouseSinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse ClickHouse sink config: {}", e))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("clickhouse.connection_string must not be empty");
        }
        if cfg.database.is_empty() {
            anyhow::bail!("clickhouse.database must not be empty");
        }
        Ok(Self { cfg })
    }

    /// Reconstruct a `SinkConfig` (the common type) from provider config.
    fn to_sink_config(&self) -> crate::config::yaml::SinkConfig {
        crate::config::yaml::SinkConfig {
            connection_string: self.cfg.connection_string.clone(),
            database: self.cfg.database.clone(),
            username: self.cfg.username.clone(),
            password: self.cfg.password.clone(),
            batch_size: self.cfg.batch_size,
            max_linger_ms: self.cfg.max_linger_ms,
            max_connections: self.cfg.max_connections,
            use_tls: self.cfg.use_tls,
            tls_domain: self.cfg.tls_domain.clone(),
            recreate_tables: false, // not used by sink directly
        }
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn build_sink<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<Arc<dyn Sink>>> {
        Box::pin(async move {
            let sink = ClickHouseSink::new(&self.to_sink_config()).await?;
            Ok(Arc::new(sink) as Arc<dyn Sink>)
        })
    }

    fn create_tables<'a>(
        &'a self,
        table: &str,
        dlq_table: &str,
        schema: &'a SchemaConfig,
        recreate: bool,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        // Build a temporary sink for DDL (one short-lived connection).
        let cfg = self.to_sink_config();
        let table = table.to_string();
        let dlq_table = dlq_table.to_string();
        let schema = schema.clone();
        Box::pin(async move {
            let sink = ClickHouseSink::new(&cfg).await?;
            let cols = ClickHouseSink::schema_columns(&schema)?;
            sink.create_table(&table, &cols, &schema.order_by, recreate).await?;
            let dlq_cols: Vec<(String, String)> = crate::parser::json_parser::DLQ_CH_COLUMNS
                .iter().map(|(n, t)| ((*n).to_string(), (*t).to_string())).collect();
            sink.create_table(&dlq_table, &dlq_cols, &[], recreate).await?;
            Ok(())
        })
    }

    fn verify_tables<'a>(
        &'a self,
        table: &str,
        dlq_table: &str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        let cfg = self.to_sink_config();
        let table = table.to_string();
        let dlq_table = dlq_table.to_string();
        Box::pin(async move {
            let sink = ClickHouseSink::new(&cfg).await?;
            sink.verify_table(&table).await?;
            sink.verify_table(&dlq_table).await?;
            Ok(())
        })
    }
}
