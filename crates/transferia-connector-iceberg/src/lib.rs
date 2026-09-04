extern crate alloc;

pub mod iceberg;

pub use iceberg::{
    check_sink_connection, check_source_connection, IcebergSinkConfig, IcebergSinkConnector,
    IcebergSourceConfig, IcebergSourceConnector,
};

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::tuning::{NumericScale, TuningParameter};
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("iceberg", "Apache Iceberg")
            .source::<IcebergSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || serde_json::json!({
                    "catalog": { "uri": "", "request_timeout_ms": 30000, "warehouse": null, "auth": { "type": "none" } },
                    "storage": { "type": "s3", "bucket": "", "request_timeout_ms": 30000, "region": null, "endpoint": null, "credentials": null, "session_token": null, "path_style_access": false, "allow_anonymous": false },
                    "namespace": "default", "table_names": [""],
                    "read_batch_rows": 65_536,
                    "read_data_file_concurrency": 32,
                    "read_manifest_concurrency": 32,
                    "parquet_metadata_size_hint_bytes": 524_288,
                    "parquet_range_coalesce_bytes": 1_048_576_u64,
                    "parquet_range_fetch_concurrency": 10
                }),
                {
                    let metrics = Arc::clone(metrics);
                    move |config| Ok(Box::new(IcebergSourceConnector::from_config(config, Arc::clone(&metrics))?))
                },
            )?
            .source_tuning_parameters(iceberg_source_tuning_parameters())?
            .source_checker::<IcebergSourceConfig, _, _>(|config| async move {
                check_source_connection(&config).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            })
            .sink::<IcebergSinkConfig, _, _>(
                || serde_json::json!({
                    "catalog": { "uri": "", "request_timeout_ms": 30000, "warehouse": null, "auth": { "type": "none" } },
                    "storage": { "type": "s3", "bucket": "", "request_timeout_ms": 30000, "region": null, "endpoint": null, "credentials": null, "session_token": null, "path_style_access": false, "allow_anonymous": false },
                    "namespace": "default", "create_if_missing": false,
                    "target_file_size_bytes": 134_217_728,
                    "commit_target_size_bytes": 536_870_912,
                    "parquet_compression": "zstd", "parquet_row_group_rows": 250_000,
                    "write_concurrency": 8
                }),
                |config| Ok(Box::new(IcebergSinkConnector::from_config(config)?)),
            )?
            .sink_tuning_parameters(iceberg_sink_tuning_parameters())?
            .sink_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .sink_checker::<IcebergSinkConfig, _, _>(|config| async move {
                check_sink_connection(&config).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            }),
    )?;
    Ok(())
}

fn iceberg_source_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/read_batch_rows".to_owned(),
            label: "Rows per Arrow batch".to_owned(),
            baseline: 65_536,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![16_384, 65_536, 262_144, 1_048_576],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/read_data_file_concurrency".to_owned(),
            label: "Concurrent data files".to_owned(),
            baseline: 32,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![4, 8, 16, 32],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/read_manifest_concurrency".to_owned(),
            label: "Concurrent manifests".to_owned(),
            baseline: 32,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![4, 8, 16, 32],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/parquet_metadata_size_hint_bytes".to_owned(),
            label: "Parquet metadata size hint".to_owned(),
            baseline: 512 << 10,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![128 << 10, 512 << 10, 2 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/parquet_range_coalesce_bytes".to_owned(),
            label: "Parquet range coalescing".to_owned(),
            baseline: 1 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![256 << 10, 1 << 20, 4 << 20, 16 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/parquet_range_fetch_concurrency".to_owned(),
            label: "Concurrent Parquet ranges".to_owned(),
            baseline: 10,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![2, 4, 8, 10, 16],
            scale: NumericScale::Logarithmic,
        },
    ]
}

fn iceberg_sink_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/target_file_size_bytes".to_owned(),
            label: "Target file size".to_owned(),
            baseline: 128 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![32 << 20, 64 << 20, 128 << 20, 256 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/commit_target_size_bytes".to_owned(),
            label: "Commit target size".to_owned(),
            baseline: 512 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![128 << 20, 256 << 20, 512 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::Choice {
            pointer: "/parquet_compression".to_owned(),
            label: "Parquet compression".to_owned(),
            baseline: serde_json::Value::from("zstd"),
            values: vec!["zstd", "lz4", "none"]
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        },
        TuningParameter::UnsignedInteger {
            pointer: "/parquet_row_group_rows".to_owned(),
            label: "Rows per Parquet row group".to_owned(),
            baseline: 250_000,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![65_536, 250_000, 1_000_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/write_concurrency".to_owned(),
            label: "Concurrent file writers".to_owned(),
            baseline: 8,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![1, 2, 4, 8, 16],
            scale: NumericScale::Logarithmic,
        },
    ]
}
