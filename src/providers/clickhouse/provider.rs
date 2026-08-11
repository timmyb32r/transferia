use futures_util::future::BoxFuture;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::admin::ClickHouseAdmin;
use crate::providers::clickhouse::schema::schema_columns;
use crate::providers::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct ClickHouseSinkProvider {
    cfg: ClickHouseSinkConfig,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: ClickHouseSinkConfig = serde_yaml::from_value(value)
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
            for key in &config.sorting_key {
                anyhow::ensure!(
                    request
                        .schema
                        .columns
                        .iter()
                        .any(|column| &column.name == key),
                    "clickhouse.sorting_key column '{key}' is absent from the dataset schema"
                );
            }
            let columns = schema_columns(&request.schema.columns)?;
            let dlq_columns = schema_columns(&request.dlq_schema.columns)?;
            let admin = ClickHouseAdmin::connect(&config).await?;
            admin
                .create_table(
                    &request.table,
                    &columns,
                    &config.sorting_key,
                    config.recreate_tables,
                )
                .await?;
            admin
                .create_table(
                    &request.dlq_table,
                    &dlq_columns,
                    &[],
                    config.recreate_tables,
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
