use std::sync::Arc;

use arrow::array::{Array, BinaryArray, StringArray};
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

const SHARD_GROUPS_QUERY: &str =
    "SELECT DISTINCT toString(cluster) AS cluster FROM system.clusters ORDER BY cluster";

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
        let groups = query_shard_groups(&client).await?;
        validate_selected_shard_group(
            (!config.shard_group.is_empty()).then_some(config.shard_group.as_str()),
            &groups,
        )?;
        Ok(groups)
    }
}

pub async fn query_shard_groups(client: &ReconnectingClient) -> anyhow::Result<Vec<String>> {
    let started = std::time::Instant::now();
    let batches = client.query_all(SHARD_GROUPS_QUERY).await;
    tracing::info!(
        stage = "shard_groups_query",
        elapsed_ms = started.elapsed().as_millis(),
        success = batches.is_ok(),
        "ClickHouse connection check stage completed"
    );
    let batches =
        batches.map_err(|error| anyhow::anyhow!("ClickHouse connection check failed: {error}"))?;
    let mut groups = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("cluster")
            .ok_or_else(|| anyhow::anyhow!("ClickHouse system.clusters omitted 'cluster'"))?;
        append_shard_groups(column.as_ref(), &mut groups)?;
    }
    groups.sort();
    groups.dedup();
    Ok(groups)
}

fn append_shard_groups(column: &dyn Array, groups: &mut Vec<String>) -> anyhow::Result<()> {
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        groups.extend(values.iter().flatten().map(str::to_owned));
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        for value in values.iter().flatten() {
            groups.push(
                std::str::from_utf8(value)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "ClickHouse system.clusters returned a non-UTF-8 shard group: {error}"
                        )
                    })?
                    .to_owned(),
            );
        }
        return Ok(());
    }
    anyhow::bail!(
        "ClickHouse system.clusters returned unsupported Arrow type {:?} for 'cluster'",
        column.data_type(),
    )
}

pub fn validate_selected_shard_group(
    selected: Option<&str>,
    available: &[String],
) -> anyhow::Result<()> {
    if let Some(selected) = selected {
        anyhow::ensure!(
            available.iter().any(|candidate| candidate == selected),
            "ClickHouse shard group '{selected}' is not available to this user"
        );
    }
    Ok(())
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

    fn destination_type(
        &self,
        column: &crate::core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        super::table::destination_type(column)
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            if !self.config.shard_group.is_empty() {
                let groups = query_shard_groups(client.as_ref()).await?;
                validate_selected_shard_group(Some(&self.config.shard_group), &groups)?;
            }
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
