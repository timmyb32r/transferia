use alloc::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::sink::{ClickHouseSink, ClickhouseSinkConfig};
use crate::providers::traits::SinkProvider;

pub struct ClickHouseSinkProvider {
    cfg: ClickhouseSinkConfig,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: ClickhouseSinkConfig = serde_yaml::from_value(value)
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
        let cfg = self.cfg.clone();
        Box::pin(async move {
            let sink = ClickHouseSink::new(cfg, 10_000).await?;
            Ok(Arc::new(sink) as Arc<dyn Sink>)
        })
    }
}