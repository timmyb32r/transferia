use std::sync::Arc;

use arrow::array::{Array as _, BinaryArray, Date32Array, Int64Array, TimestampSecondArray};
use arrow::datatypes::{DataType, TimeUnit};
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
async fn infinite_generator_keeps_producing_bounded_batches() -> anyhow::Result<()> {
    let mut config = config();
    config.amount = GenerationAmount::Infinite;
    config.validate()?;
    assert_eq!(config.total_rows()?, None);
    let mut source = DataGeneratorSource::new(
        config,
        PipelineMemory::new(1_024),
        Arc::new(SourceCounters::new()),
    )?;
    for _ in 0..100 {
        match source.read_batch().await? {
            SourceBatch::Typed { source_rows, .. } => assert!(source_rows > 0 && source_rows <= 12),
            _ => panic!("infinite generator must not finish"),
        }
    }
    Ok(())
}

#[test]
fn generator_delivery_modes_follow_generation_amount() -> anyhow::Result<()> {
    use transferia_delivery_contracts::DeliveryType;
    for infinite in [false, true] {
        let mut config = config();
        if infinite {
            config.amount = GenerationAmount::Infinite;
        }
        let connector =
            DataGeneratorSourceConnector::from_config(config, Arc::new(MetricsRegistry::new()))?;
        for (mode, expected) in [
            (DeliveryType::Batch, !infinite),
            (DeliveryType::Stream, true),
            (DeliveryType::BatchAndStream, false),
        ] {
            assert_eq!(
                connector.compatibility(mode).supports_delivery_type(mode),
                expected
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn infinite_generator_fails_before_wrapping_identifiers() -> anyhow::Result<()> {
    let mut config = config();
    config.amount = GenerationAmount::Infinite;
    config.start_row = u64::MAX;
    let mut source = DataGeneratorSource::new(
        config,
        PipelineMemory::new(1_024),
        Arc::new(SourceCounters::new()),
    )?;
    assert!(source.read_batch().await.is_err());
    let mut config = super::tests::config();
    config.preset = DataGeneratorPreset::TransferLogs;
    config.amount = GenerationAmount::Infinite;
    config.start_row = i64::MAX as u64;
    assert!(config.validate().is_err());
    let mut source = DataGeneratorSource::new(
        config,
        PipelineMemory::new(1_024),
        Arc::new(SourceCounters::new()),
    )?;
    assert!(source.read_batch().await.is_err());
    Ok(())
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
            SourceBatch::Dataset { .. } | SourceBatch::Raw { .. } => panic!("generator returned raw data"),
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
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
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
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
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

#[tokio::test]
async fn clickbench_preset_preserves_the_official_schema_and_primary_key() -> anyhow::Result<()> {
    let config = DataGeneratorConfig {
        table_name: "hits".to_owned(),
        preset: DataGeneratorPreset::ClickBench,
        amount: GenerationAmount::Rows { row_count: 32 },
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
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
        })
        .await?;
    let schema = &discovery.datasets[0].stored_schema;
    assert_eq!(schema.columns.len(), 105);
    assert_eq!(schema.columns[0].name, "WatchID");
    assert_eq!(schema.columns[0].data_type, DataType::Int64);
    assert_eq!(schema.columns[2].name, "Title");
    assert_eq!(schema.columns[2].data_type, DataType::Binary);
    assert_eq!(schema.columns[4].name, "EventTime");
    assert_eq!(
        schema.columns[4].data_type,
        DataType::Timestamp(TimeUnit::Second, None)
    );
    assert_eq!(schema.columns[5].name, "EventDate");
    assert_eq!(schema.columns[5].data_type, DataType::Date32);
    assert_eq!(schema.columns[104].name, "CLID");
    assert_eq!(
        schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["WatchID", "EventTime", "EventDate", "CounterID", "UserID"]
    );
    assert!(schema.columns.iter().all(|column| !column.nullable));

    let mut source = DataGeneratorSource::new(
        config,
        PipelineMemory::new(16 * 1024 * 1024),
        Arc::new(SourceCounters::new()),
    )?;
    let SourceBatch::Typed { tables, .. } = source.read_batch().await? else {
        panic!("generator did not return a typed batch");
    };
    assert_eq!(tables[0].batch.schema().fields().len(), 105);
    assert_eq!(tables[0].batch.num_rows(), 32);
    Ok(())
}

#[test]
fn clickbench_generation_is_deterministic_across_batch_boundaries() -> anyhow::Result<()> {
    let whole = DataGeneratorPreset::ClickBench.batch(1_000, 128)?;
    let prefix = DataGeneratorPreset::ClickBench.batch(1_000, 37)?;
    let suffix = DataGeneratorPreset::ClickBench.batch(1_037, 91)?;

    for column in 0..whole.num_columns() {
        assert_eq!(
            whole.column(column).slice(0, 37).to_data(),
            prefix.column(column).to_data()
        );
        assert_eq!(
            whole.column(column).slice(37, 91).to_data(),
            suffix.column(column).to_data()
        );
    }
    Ok(())
}

#[test]
fn clickbench_generation_matches_representative_reference_distributions() -> anyhow::Result<()> {
    let rows = 20_000_usize;
    let batch = DataGeneratorPreset::ClickBench.batch(0, rows as u64)?;

    let watch_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("WatchID must be Int64");
    let unique_watch_ids = watch_ids
        .values()
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(unique_watch_ids.len(), rows);
    assert!(watch_ids
        .values()
        .iter()
        .all(|value| { (4_611_686_018_427_387_904..=i64::MAX).contains(value) }));

    let title = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Title must preserve arbitrary bytes");
    let empty_titles = (0..rows)
        .filter(|row| title.value_length(*row) == 0)
        .count();
    assert!(empty_titles.abs_diff(3_501) < 250);
    let title_lengths = (0..rows)
        .map(|row| usize::try_from(title.value_length(row)))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(title_lengths.iter().copied().max().unwrap_or(0) <= 921);
    assert!(title_lengths.iter().copied().max().unwrap_or(0) >= 800);

    let params = batch
        .column(35)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Params must be Binary");
    let empty_params = (0..rows)
        .filter(|row| params.value_length(*row) == 0)
        .count();
    assert!(empty_params.abs_diff(19_774) < 100);

    let event_time = batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .expect("EventTime must remain a second-resolution timestamp");
    assert!(event_time
        .values()
        .iter()
        .all(|value| (1_372_708_958..=1_375_299_776).contains(value)));
    let event_date = batch
        .column(5)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("EventDate must remain a date");
    assert!(event_date
        .values()
        .iter()
        .all(|value| (15_888..=15_917).contains(value)));
    Ok(())
}

#[test]
fn clickbench_batch_accounting_is_bounded_and_range_is_lossless() -> anyhow::Result<()> {
    let preset = DataGeneratorPreset::ClickBench;
    let rows = 4_096_u64;
    let accounted = preset.batch_bytes(50_000, rows)?;
    let batch = preset.batch(50_000, rows)?;
    let buffers = batch
        .columns()
        .iter()
        .map(|array| u64::try_from(array.get_buffer_memory_size()))
        .sum::<Result<u64, _>>()?;
    assert!(accounted >= buffers);
    assert!(accounted <= buffers + 105 * 128);

    let invalid = DataGeneratorConfig {
        table_name: "hits".to_owned(),
        preset,
        amount: GenerationAmount::Rows { row_count: 2 },
        start_row: (1_u64 << 62) - 1,
    };
    assert!(invalid
        .validate()
        .unwrap_err()
        .to_string()
        .contains("WatchID remains unique"));
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
    assert_eq!(config.total_rows()?, Some(50_000_000));
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
