use std::sync::Arc;

use transferia_core::data::message::SourceBatch;
use transferia_core::delivery::DeliveryDiscoveryRequest;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::{MetricsRegistry, SourceCounters};
use transferia_registry::{SourceConnector as _, SourceDiscoveryContext};

use super::source::DataGeneratorSource;
use super::{DataGeneratorConfig, DataGeneratorSourceConnector};

fn config() -> DataGeneratorConfig {
    DataGeneratorConfig {
        table_name: "my_table".to_owned(),
        column_count: 10,
        data_size_bytes: 1_600,
        batch_rows: 7,
    }
}

#[tokio::test]
async fn generator_produces_the_exact_configured_logical_size() -> anyhow::Result<()> {
    let mut source = DataGeneratorSource::new(
        config(),
        PipelineMemory::new(1_024),
        Arc::new(SourceCounters::new()),
    )?;
    let mut rows = 0_u64;
    loop {
        match source.read_batch().await? {
            SourceBatch::Typed {
                tables,
                source_rows,
                ..
            } => {
                assert_eq!(tables[0].table.as_ref(), "my_table");
                assert_eq!(tables[0].batch.num_columns(), 10);
                rows += source_rows;
            }
            SourceBatch::Finished => break,
            SourceBatch::Raw { .. } => panic!("generator returned raw data"),
        }
    }
    assert_eq!(rows * 10 * 8, 1_600);
    Ok(())
}

#[tokio::test]
async fn discovery_matches_generated_table_and_columns() -> anyhow::Result<()> {
    let connector =
        DataGeneratorSourceConnector::from_config(config(), Arc::new(MetricsRegistry::new()))?;
    let discovery = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
        .await?;
    assert_eq!(discovery.datasets[0].name.as_ref(), "my_table");
    assert_eq!(discovery.datasets[0].stored_schema.columns.len(), 10);
    Ok(())
}

#[test]
fn generator_rejects_unrepresentable_requested_sizes() {
    let mut config = config();
    config.data_size_bytes = 101;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("must be divisible"));
}
