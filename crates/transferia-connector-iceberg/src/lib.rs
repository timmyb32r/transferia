extern crate alloc;

pub mod iceberg;

pub use iceberg::{
    check_sink_connection, check_source_connection, IcebergSinkConfig, IcebergSinkConnector,
    IcebergSourceConfig, IcebergSourceConnector,
};

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
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
                    "storage": { "type": "s3", "bucket": "", "request_timeout_ms": 30000, "region": null, "endpoint": null, "access_key_id": null, "secret_access_key": null, "session_token": null, "path_style_access": false, "allow_anonymous": false },
                    "table": { "namespace": ["default"], "name": "" }, "output_name": ""
                }),
                {
                    let metrics = Arc::clone(metrics);
                    move |config| Ok(Box::new(IcebergSourceConnector::from_config(config, Arc::clone(&metrics))?))
                },
            )?
            .source_checker::<IcebergSourceConfig, _, _>(|config| async move {
                check_source_connection(&config).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            })
            .sink::<IcebergSinkConfig, _, _>(
                || serde_json::json!({
                    "catalog": { "uri": "", "request_timeout_ms": 30000, "warehouse": null, "auth": { "type": "none" } },
                    "storage": { "type": "s3", "bucket": "", "request_timeout_ms": 30000, "region": null, "endpoint": null, "access_key_id": null, "secret_access_key": null, "session_token": null, "path_style_access": false, "allow_anonymous": false },
                    "tables": [{ "dataset": "", "namespace": ["default"], "name": "", "create_if_missing": false, "location": null }],
                    "target_file_size_bytes": 134_217_728
                }),
                |config| Ok(Box::new(IcebergSinkConnector::from_config(config)?)),
            )?
            .sink_checker::<IcebergSinkConfig, _, _>(|config| async move {
                check_sink_connection(&config).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            }),
    )?;
    Ok(())
}
