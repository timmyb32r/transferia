use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use transferia::connectors::clickhouse::{ClickHouseSinkConfig, ClickHouseSinkConnector};
use transferia::connectors::generator::{
    DataGeneratorConfig, DataGeneratorPreset, DataGeneratorSourceConnector, GenerationAmount,
};
use transferia::core::delivery::DeliveryDiscoveryRequest;
use transferia::delivery::preparation::validate_discovered_pipeline;
use transferia::metrics::MetricsRegistry;
use transferia::registry::{SinkConnector, SourceConnector, SourceDiscoveryContext};

#[tokio::test]
async fn clickbench_schema_passes_clickhouse_limits_without_network() -> anyhow::Result<()> {
    let source = DataGeneratorSourceConnector::from_config(
        DataGeneratorConfig {
            table_name: "hits".to_owned(),
            preset: DataGeneratorPreset::ClickBench,
            amount: GenerationAmount::Rows { row_count: 1 },
            start_row: 0,
        },
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = source
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: tokio_util::sync::CancellationToken::new(),
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Batch,
        })
        .await?;
    let columns = &discovery.datasets[0].stored_schema.columns;
    assert_eq!(columns.len(), 105);
    assert_eq!(
        columns
            .iter()
            .find(|column| column.name == "EventDate")
            .map(|column| &column.data_type),
        Some(&DataType::Date32),
    );
    assert_eq!(
        columns
            .iter()
            .find(|column| column.name == "EventTime")
            .map(|column| &column.data_type),
        Some(&DataType::Timestamp(TimeUnit::Second, None)),
    );
    let sink_config: ClickHouseSinkConfig = serde_yaml::from_str(
        "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: default\nusername: default\n",
    )?;
    let sink = ClickHouseSinkConnector::from_config(sink_config)?;

    validate_discovered_pipeline(
        &source.compatibility(transferia::delivery::config::yaml::DeliveryType::Batch),
        &sink.compatibility(),
        sink.limits(),
        &discovery,
        false,
    )?;
    Ok(())
}
