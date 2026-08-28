extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod s3;
}

pub use connectors::s3;

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("s3", "S3")
            .source_draft::<s3::src_batch::S3SourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || serde_json::json!({
                    "bucket": "", "path_prefix": "", "table_name": "",
                    "region": "us-east-1", "endpoint": null,
                    "credentials": { "access_key": "", "secret_key": "" },
                    "parser": {
                        "type": "parquet", "batch_rows": 65536
                    },
                    "timeout_ms": 30000
                }),
                {
                    let metrics = Arc::clone(metrics);
                    move |config| Ok(Box::new(s3::S3SourceConnector::from_config(config, Arc::clone(&metrics))?))
                },
            )?
            .source_checker::<s3::src_batch::S3SourceConfig, _, _>(|config| async move {
                config.check_connection().await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            })
            .sink_draft::<s3::sink::S3SinkConfig, _, _>(
                || serde_json::json!({
                    "bucket": "", "object_layout_version": 5, "path_prefix": "",
                    "region": "us-east-1", "endpoint": null,
                    "credentials": { "access_key": "", "secret_key": "" },
                    "format": {
                        "type": "parquet", "compression": "zstd",
                        "row_group": { "max_rows": 1_000_000, "max_bytes": 134_217_728 }
                    },
                    "partitioning": { "type": "source" },
                    "rotation": { "max_rows": 10000, "max_bytes": "", "on_partition_path_change": "keep_epoch" },
                    "buffering": { "max_epoch_buffers": 32, "max_pending_upload_objects": 64, "max_buffered_bytes": "", "max_epoch_bytes": "" },
                    "upload": { "multipart_threshold": "", "part_size": "", "parallel_parts": 2, "max_in_flight_objects": 2, "operation_timeout": "" },
                    "retry": { "initial_backoff": "", "max_backoff": "", "max_attempts": 10 }
                }),
                |config| Ok(Box::new(s3::sink::S3SinkConnector::from_config(config)?)),
            )?
            .sink_checker::<s3::sink::S3SinkConfig, _, _>(|config| async move {
                config.check_connection().await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            }),
    )?;
    Ok(())
}
