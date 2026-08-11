use futures_util::future::BoxFuture;
use serde_yaml::Value;

use super::table::prepare_tables;
use super::{ClickHouseSink, ClickHouseSinkConfig};
use crate::pipeline::sink::Sink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct ClickHouseSinkProvider {
    config: ClickHouseSinkConfig,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        Ok(Self {
            config: ClickHouseSinkConfig::from_value(value)?,
        })
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        let config = self.config.clone();
        Box::pin(async move { prepare_tables(&config, &request).await })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        let config = self.config.clone();
        Box::pin(async move {
            tracing::info!(
                partition = context.partition_id,
                "building independent ClickHouse sink"
            );
            Ok(Box::new(ClickHouseSink::new(config, context.counters).await?) as Box<dyn Sink>)
        })
    }
}
