use std::sync::Arc;

use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::META_PRIMARY_KEY;
use transferia_core::delivery::DeliveryDiscoveryRequest;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::{MetricsRegistry, SourceCounters};
use transferia_registry::{SourceConnector as _, SourceDiscoveryContext};

use super::source::DataGeneratorSource;
use super::{
    DataGeneratorConfig, DataGeneratorPreset, DataGeneratorSourceConnector, GenerationAmount,
};

fn config() -> DataGeneratorConfig {
    DataGeneratorConfig {
        table_name: "my_table".to_owned(),
        preset: DataGeneratorPreset::Numeric { column_count: 10 },
        amount: GenerationAmount::DataSize {
            data_size_bytes: 1_600,
        },
        start_row: 0,
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
                assert_eq!(
                    tables[0]
                        .batch
                        .schema()
                        .field(0)
                        .metadata()
                        .get(META_PRIMARY_KEY)
                        .map(String::as_str),
                    Some("true")
                );
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
async fn generator_caps_batches_at_sixteen_mebibytes() -> anyhow::Result<()> {
    const TARGET_BYTES: u64 = 16 * 1024 * 1024;
    const ROW_BYTES: u64 = 10 * 8;
    let aligned_target_bytes = TARGET_BYTES / ROW_BYTES * ROW_BYTES;
    let mut config = config();
    config.amount = GenerationAmount::DataSize {
        data_size_bytes: aligned_target_bytes * 2,
    };
    let mut source = DataGeneratorSource::new(
        config,
        PipelineMemory::new((TARGET_BYTES * 4) as usize),
        Arc::new(SourceCounters::new()),
    )?;

    let SourceBatch::Typed { source_rows, .. } = source.read_batch().await? else {
        panic!("generator did not return a typed batch");
    };

    assert_eq!(source_rows * ROW_BYTES, aligned_target_bytes);
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
    assert!(discovery.datasets[0].stored_schema.columns[0].primary_key);
    assert!(discovery.datasets[0]
        .stored_schema
        .columns
        .iter()
        .skip(1)
        .all(|column| !column.primary_key));
    Ok(())
}

#[tokio::test]
async fn transfer_logs_preset_matches_the_declared_schema_and_primary_key() -> anyhow::Result<()> {
    let config = DataGeneratorConfig {
        table_name: "logs".to_owned(),
        preset: DataGeneratorPreset::TransferLogs,
        amount: GenerationAmount::Rows { row_count: 1 },
        start_row: 0,
    };
    let connector = DataGeneratorSourceConnector::from_config(
        config.clone(),
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
        .await?;
    let primary_keys = discovery.datasets[0]
        .stored_schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        primary_keys,
        vec![
            "_system_topic",
            "_system_partition",
            "_system_offset",
            "_system_message_index"
        ]
    );

    let mut source = DataGeneratorSource::new(
        config,
        PipelineMemory::new(16 * 1024 * 1024),
        Arc::new(SourceCounters::new()),
    )?;
    let SourceBatch::Typed { tables, .. } = source.read_batch().await? else {
        panic!("generator did not return a typed batch");
    };
    assert_eq!(tables[0].batch.num_columns(), 26);
    assert_eq!(tables[0].batch.schema().field(0).name(), "caller");
    assert_eq!(tables[0].batch.schema().field(22).name(), "_system_topic");
    assert_eq!(
        tables[0]
            .batch
            .schema()
            .field(22)
            .metadata()
            .get(META_PRIMARY_KEY)
            .map(String::as_str),
        Some("true")
    );
    Ok(())
}

#[test]
fn generator_rejects_unrepresentable_requested_sizes() {
    let mut config = config();
    config.amount = GenerationAmount::DataSize {
        data_size_bytes: 101,
    };
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("must be divisible"));
}

#[test]
fn generator_accepts_an_exact_row_count() -> anyhow::Result<()> {
    let mut config = config();
    config.amount = GenerationAmount::Rows {
        row_count: 50_000_000,
    };
    config.validate()?;
    assert_eq!(config.total_rows()?, 50_000_000);
    Ok(())
}

#[tokio::test]
async fn generator_can_produce_disjoint_row_identifier_ranges() -> anyhow::Result<()> {
    let mut config = DataGeneratorConfig {
        table_name: "logs".to_owned(),
        preset: DataGeneratorPreset::TransferLogs,
        amount: GenerationAmount::Rows { row_count: 2 },
        start_row: 50_000_000,
    };
    config.validate()?;
    let mut source = DataGeneratorSource::new(
        config.clone(),
        PipelineMemory::new(16 * 1024 * 1024),
        Arc::new(SourceCounters::new()),
    )?;

    let SourceBatch::Typed { tables, .. } = source.read_batch().await? else {
        panic!("generator did not return a typed batch");
    };
    let offsets = tables[0]
        .batch
        .column(24)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("system offset must be Int64");
    assert_eq!(offsets.values(), &[50_000_000, 50_000_001]);
    assert!(matches!(source.read_batch().await?, SourceBatch::Finished));

    config.start_row = u64::MAX;
    assert!(config.validate().is_err());
    Ok(())
}
