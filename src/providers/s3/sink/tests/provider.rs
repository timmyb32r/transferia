use super::*;
use crate::core::data::schema::SchemaColumn;
use crate::core::delivery::{DiscoveredDataset, SchemaOrigin};

fn discovered_dataset(
    role: DatasetRole,
    name: &'static str,
    value_type: DataType,
    value_nullable: bool,
) -> DiscoveredDataset {
    let system_columns = REQUIRED_ROUTING_COLUMNS.to_vec();
    let incoming_schema = DatasetSchema::new(vec![
        SchemaColumn::new("value".into(), value_type, value_nullable),
        SchemaColumn::new(
            SystemColumnKind::Topic.default_name().into(),
            DataType::Utf8,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::Partition.default_name().into(),
            DataType::Int64,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::Offset.default_name().into(),
            DataType::Int64,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::MessageIndex.default_name().into(),
            DataType::UInt64,
            false,
        ),
    ]);
    DiscoveredDataset {
        role,
        name: Arc::from(name),
        stored_schema: DatasetSchema::new(vec![SchemaColumn::new(
            "value".into(),
            incoming_schema.columns[0].data_type.clone(),
            value_nullable,
        )]),
        incoming_schema,
        system_columns: system_columns.iter().copied().map(Into::into).collect(),
    }
}

fn discovery(value_type: DataType, value_nullable: bool) -> DeliveryDiscovery {
    DeliveryDiscovery {
        source_name: Arc::from("topic-a"),
        source_topology: crate::core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: vec![
            discovered_dataset(
                DatasetRole::Main,
                "events",
                value_type.clone(),
                value_nullable,
            ),
            discovered_dataset(
                DatasetRole::DeadLetterQueue,
                "events_dlq",
                value_type,
                value_nullable,
            ),
        ],
    }
}

#[test]
fn epoch_must_fit_pipeline_memory_to_guarantee_progress() -> anyhow::Result<()> {
    let provider = S3SinkProvider::from_config(serde_yaml::from_str(
        "bucket: test\nbuffering: { max_buffered_bytes: 64, max_epoch_bytes: 48 }\n",
    )?)?;

    assert!(provider.validate_pipeline_memory_limit(47).is_err());
    provider.validate_pipeline_memory_limit(48)?;
    Ok(())
}

#[test]
fn publishes_and_enforces_the_s3_destination_contract() -> anyhow::Result<()> {
    let provider = S3SinkProvider::from_config(serde_yaml::from_str("bucket: test\n")?)?;
    let description = provider.limits().description();
    assert_eq!(description.sink, "s3");
    assert_eq!(
        description
            .object_key
            .expect("S3 object-key limit")
            .max_utf8_bytes,
        MAX_OBJECT_KEY_BYTES,
    );
    provider
        .limits()
        .validate_discovery(&discovery(DataType::Utf8, false))?;

    let mut invalid_name = discovery(DataType::Utf8, false);
    invalid_name.datasets[0].name = Arc::from("nested/events");
    assert!(provider
        .limits()
        .validate_discovery(&invalid_name)
        .expect_err("dataset names are single key segments")
        .to_string()
        .contains("path segment"));

    assert!(provider
        .limits()
        .validate_discovery(&discovery(DataType::Binary, false))
        .expect_err("binary values have no S3 JSON encoding contract")
        .to_string()
        .contains("does not support"));
    Ok(())
}

#[test]
fn field_partitioning_is_checked_against_discovered_columns() -> anyhow::Result<()> {
    let provider = S3SinkProvider::from_config(serde_yaml::from_str(
        "bucket: test\npartitioning: { type: fields, columns: [value] }\n",
    )?)?;
    provider
        .limits()
        .validate_discovery(&discovery(DataType::Int64, false))?;
    assert!(provider
        .limits()
        .validate_discovery(&discovery(DataType::Int64, true))
        .expect_err("nullable key columns can create poison rows")
        .to_string()
        .contains("non-nullable"));
    assert!(provider
        .limits()
        .validate_discovery(&discovery(DataType::Float64, false))
        .expect_err("floating-point path values are unsupported")
        .to_string()
        .contains("unsupported Arrow type"));
    Ok(())
}

#[test]
fn discovery_rejects_shared_main_and_dlq_namespace() -> anyhow::Result<()> {
    let provider = S3SinkProvider::from_config(serde_yaml::from_str("bucket: test\n")?)?;
    let mut discovered = discovery(DataType::Utf8, false);
    discovered.datasets[1].name = Arc::clone(&discovered.datasets[0].name);
    let error = provider
        .limits()
        .validate_discovery(&discovered)
        .expect_err("main and DLQ objects must not share keys");
    assert!(
        format!("{error:#}").contains("repeat object namespace"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn discovery_rejects_static_object_key_overhead() -> anyhow::Result<()> {
    let mut provider = S3SinkProvider::from_config(serde_yaml::from_str("bucket: test\n")?)?;
    provider.cfg.prefix = "x".repeat(MAX_OBJECT_KEY_BYTES);
    assert!(provider
        .limits()
        .validate_discovery(&discovery(DataType::Utf8, false))
        .expect_err("overlong static namespace must fail before workers start")
        .to_string()
        .contains("1024-byte limit"));
    Ok(())
}

#[test]
fn record_time_discovery_validates_the_rendered_namespace_without_rewriting() -> anyhow::Result<()>
{
    let mut provider = S3SinkProvider::from_config(serde_yaml::from_str(
        "bucket: test\npartitioning: { type: record_time, window: 1h, path: 'year=%Y', timezone: UTC }\n",
    )?)?;
    let probe = provider.cfg.main_partition_path_probe()?;
    assert_eq!(probe, "year=9999");
    let mut discovered = discovery(DataType::Utf8, false);
    for dataset in &mut discovered.datasets {
        dataset
            .system_columns
            .push(SystemColumnKind::WriteTimestampMs.into());
        dataset.incoming_schema.columns.push(SchemaColumn::new(
            SystemColumnKind::WriteTimestampMs.default_name().into(),
            DataType::Int64,
            false,
        ));
    }
    provider.limits().validate_discovery(&discovered)?;

    provider.cfg.prefix = "x".repeat(MAX_OBJECT_KEY_BYTES - probe.len());
    assert!(provider.limits().validate_discovery(&discovered).is_err());
    Ok(())
}

#[tokio::test]
async fn partition_sinks_share_one_uploader() -> anyhow::Result<()> {
    let provider = S3SinkProvider::from_config(serde_yaml::from_str("bucket: test\n")?)?;
    let discovery = Arc::new(crate::core::delivery::DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: crate::core::delivery::SourceTopology::StaticPartitions(vec![1, 2]),
        schema_origin: crate::core::delivery::SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: Vec::new(),
    });
    assert_eq!(Arc::strong_count(&provider.uploader), 1);

    let first = provider
        .build_sink(SinkBuildContext {
            durable: crate::durable::test_support::context(),
            partition_id: 1,
            counters: Arc::new(crate::metrics::SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
        })
        .await?;
    let second = provider
        .build_sink(SinkBuildContext {
            durable: crate::durable::test_support::context(),
            partition_id: 2,
            counters: Arc::new(crate::metrics::SinkCounters::new()),
            keep_system_columns: false,
            discovery,
        })
        .await?;

    assert_eq!(Arc::strong_count(&provider.uploader), 3);
    drop(first);
    drop(second);
    assert_eq!(Arc::strong_count(&provider.uploader), 1);
    Ok(())
}
