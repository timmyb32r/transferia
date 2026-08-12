use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;

use super::client::ReconnectingClient;
use super::table::{prepare_tables, validate_table_schema};
use super::transport::NativeTransport;
use super::{ClickHouseSink, ClickHouseSinkConfig};
use crate::compatibility::EndpointDescriptor;
use crate::delivery::{
    validate_stored_projection, ArrowTypeFamily, DatasetRole, DeliveryDiscovery, NameSyntax,
    SinkLimits, SinkLimitsDescription, TextLimit,
};
use crate::pipeline::sink::Sink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct ClickHouseSinkProvider {
    config: ClickHouseSinkConfig,
    client: Arc<ReconnectingClient>,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let config = ClickHouseSinkConfig::from_value(value)?;
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
                ArrowTypeFamily::Date64,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            discovery.datasets.len() == 2,
            "ClickHouse requires exactly main and dead-letter datasets, discovered {}",
            discovery.datasets.len(),
        );
        let main = discovery.dataset(DatasetRole::Main)?;
        let dlq = discovery.dataset(DatasetRole::DeadLetterQueue)?;
        anyhow::ensure!(
            main.name != dlq.name,
            "ClickHouse main and dead-letter datasets resolve to the same table '{}'",
            main.name,
        );

        for dataset in [main, dlq] {
            validate_stored_projection(discovery, dataset)?;
            validate_table_schema(&dataset.name, &dataset.stored_schema).map_err(|error| {
                error.context(format!(
                    "discovered {:?} dataset '{}' is incompatible with ClickHouse",
                    dataset.role, dataset.name,
                ))
            })?;
        }

        for key in &self.sorting_key {
            anyhow::ensure!(
                main.stored_schema
                    .columns
                    .iter()
                    .any(|column| &column.name == key),
                "clickhouse.sorting_key column '{key}' is absent from discovered main dataset '{}'",
                main.name,
            );
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

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
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
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::delivery::{DiscoveredDataset, SchemaOrigin};
    use crate::types::schema::{DatasetSchema, SchemaColumn};

    fn discovery(table: &str, data_type: DataType) -> DeliveryDiscovery {
        let schema = DatasetSchema::new(vec![SchemaColumn::new("value".into(), data_type, false)]);
        DeliveryDiscovery {
            source_name: Arc::from("source-topic"),
            source_partitions: vec![0],
            schema_origin: SchemaOrigin::ParserProjection,
            keep_system_columns: false,
            datasets: vec![
                DiscoveredDataset {
                    role: DatasetRole::Main,
                    name: Arc::from(table),
                    incoming_schema: schema.clone(),
                    stored_schema: schema.clone(),
                    system_columns: Vec::new(),
                },
                DiscoveredDataset {
                    role: DatasetRole::DeadLetterQueue,
                    name: Arc::from(format!("{table}_dlq")),
                    incoming_schema: schema.clone(),
                    stored_schema: schema,
                    system_columns: Vec::new(),
                },
            ],
        }
    }

    #[tokio::test]
    async fn provider_constructs_shared_client_without_connecting() -> anyhow::Result<()> {
        let provider = ClickHouseSinkProvider::from_config(serde_yaml::from_str(
            "endpoint: 127.0.0.1:1\ntrusted_plaintext: true\nconnect_timeout_ms: 1\n",
        )?)?;

        let first = Arc::clone(&provider.client);
        let second = Arc::clone(&provider.client);

        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[test]
    fn limits_are_declarative_and_validate_discovered_schema() -> anyhow::Result<()> {
        let provider = ClickHouseSinkProvider::from_config(serde_yaml::from_str(
            "endpoint: 127.0.0.1:1\ntrusted_plaintext: true\nsorting_key: [value]\n",
        )?)?;

        let description = provider.limits().description();
        assert_eq!(description.sink, "clickhouse");
        assert_eq!(
            description.dataset_name.expect("table limit").syntax,
            NameSyntax::AsciiIdentifier,
        );
        assert_eq!(
            description.column_name.expect("column limit").syntax,
            NameSyntax::AsciiIdentifier,
        );
        assert!(description.object_key.is_none());
        provider
            .limits()
            .validate_discovery(&discovery("events", DataType::Int64))?;

        let invalid_name = provider
            .limits()
            .validate_discovery(&discovery("default.events", DataType::Int64))
            .unwrap_err();
        assert!(format!("{invalid_name:#}").contains("invalid ClickHouse table name"));

        let unsupported_type = provider
            .limits()
            .validate_discovery(&discovery("events", DataType::Date32))
            .unwrap_err();
        assert!(format!("{unsupported_type:#}").contains("Arrow Date32 is unavailable"));
        Ok(())
    }
}
