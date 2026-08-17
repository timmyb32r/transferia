use std::sync::Arc;

use arrow::array::StringArray;
use futures_util::future::BoxFuture;

use super::client::ReconnectingClient;
use super::table::{prepare_tables, validate_table_schema};
use super::transport::NativeTransport;
use super::{ClickHouseSink, ClickHouseSinkConfig};
use crate::core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use crate::core::sink::Sink;
use crate::delivery::semantics::EndpointDescriptor;
use crate::providers::traits::{SinkBuildContext, SinkPrepare, SinkProvider};

pub struct ClickHouseSinkProvider {
    config: ClickHouseSinkConfig,
    client: Arc<ReconnectingClient>,
}

impl ClickHouseSinkProvider {
    pub fn from_config(config: ClickHouseSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let client = Arc::new(ReconnectingClient::new(&config));
        Ok(Self { config, client })
    }

    async fn shared_client(&self) -> anyhow::Result<Arc<ReconnectingClient>> {
        let client = Arc::clone(&self.client);
        client
            .ensure_connected()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
        Ok(client)
    }

    pub async fn check_connection(config: ClickHouseSinkConfig) -> anyhow::Result<Vec<String>> {
        config.validate()?;
        let client = ReconnectingClient::new(&config);
        let batches = client
            .query_all("SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster")
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection check failed: {error}"))?;
        let mut groups = Vec::new();
        for batch in batches {
            let column = batch
                .column_by_name("cluster")
                .ok_or_else(|| anyhow::anyhow!("ClickHouse system.clusters omitted 'cluster'"))?;
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("ClickHouse system.clusters returned a non-string 'cluster'")
                })?;
            groups.extend(values.iter().flatten().map(str::to_owned));
        }
        groups.sort();
        groups.dedup();
        Ok(groups)
    }
}

impl SinkLimits for ClickHouseSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let identifier = TextLimit {
            syntax: NameSyntax::AsciiIdentifier,
            max_utf8_bytes: None,
        };
        SinkLimitsDescription {
            sink: "clickhouse",
            dataset_name: Some(identifier.clone()),
            column_name: Some(identifier),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "ClickHouse requires at least one dataset"
        );
        let mut names = std::collections::HashSet::with_capacity(discovery.datasets.len());
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "ClickHouse datasets repeat table '{}'",
                dataset.name
            );
            validate_stored_projection(discovery, dataset)?;
            validate_table_schema(&dataset.name, &dataset.stored_schema).map_err(|error| {
                error.context(format!(
                    "discovered {:?} dataset '{}' is incompatible with ClickHouse",
                    dataset.role, dataset.name,
                ))
            })?;
        }
        Ok(())
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::ClickHouse
    }

    fn limits(&self) -> &dyn SinkLimits {
        &self.config
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            prepare_tables(client.as_ref(), &self.config, &request).await
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            tracing::info!(
                partition = context.partition_id,
                "building ClickHouse sink on shared client"
            );
            Ok(
                Box::new(ClickHouseSink::with_transport_for_partition_and_visibility(
                    self.config.clone(),
                    context.counters,
                    Arc::new(NativeTransport::new(client)),
                    context.partition_id,
                    context.keep_system_columns,
                    context.discovery,
                )) as Box<dyn Sink>,
            )
        })
    }
}

#[cfg(test)]
#[path = "tests/provider.rs"]
mod tests;
