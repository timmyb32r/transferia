use futures_util::future::BoxFuture;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::sink::{ClickHouseAdmin, ClickHouseSink, ClickhouseSinkConfig};
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct ClickHouseSinkProvider {
    cfg: ClickhouseSinkConfig,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: ClickhouseSinkConfig = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse ClickHouse sink config: {error}"))?;
        anyhow::ensure!(
            !cfg.connection_string.is_empty(),
            "clickhouse.connection_string must not be empty"
        );
        anyhow::ensure!(
            !cfg.database.is_empty(),
            "clickhouse.database must not be empty"
        );
        anyhow::ensure!(
            cfg.max_insert_rows > 0,
            "clickhouse.max_insert_rows must be positive"
        );
        anyhow::ensure!(
            cfg.max_insert_bytes > 0,
            "clickhouse.max_insert_bytes must be positive"
        );
        anyhow::ensure!(
            cfg.flush_interval_ms > 0,
            "clickhouse.flush_interval_ms must be positive"
        );
        Ok(Self { cfg })
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        let config = self.cfg.clone();
        Box::pin(async move {
            let admin = ClickHouseAdmin::connect(&config).await?;
            admin
                .create_table(
                    &request.table,
                    &request.columns,
                    &request.order_by,
                    request.recreate,
                )
                .await?;
            admin
                .create_table(
                    &request.dlq_table,
                    &request.dlq_columns,
                    &[],
                    request.recreate,
                )
                .await
        })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        let config = self.cfg.clone();
        Box::pin(async move {
            tracing::info!(
                partition = context.partition_id,
                "building independent ClickHouse sink"
            );
            Ok(Box::new(ClickHouseSink::new(config, context.counters).await?) as Box<dyn Sink>)
        })
    }
}
