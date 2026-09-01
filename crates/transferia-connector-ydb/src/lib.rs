extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod ydb;

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("ydb", "YDB")
            .source::<ydb::YdbSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "endpoint": "", "database": "", "trusted_plaintext": false,
                        "auth": { "type": "token", "token": "" },
                        "tables": [{ "name": "", "path": "" }], "batch_rows": 65536,
                        "request_timeout_ms": 30000
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(ydb::YdbSourceConnector::from_config(
                            config,
                            Arc::clone(&metrics),
                        )?))
                    }
                },
            )?
            .sink::<ydb::YdbSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "endpoint": "", "database": "", "trusted_plaintext": false,
                        "auth": { "type": "token", "token": "" },
                        "tables": [{ "name": "", "path": "" }], "create_tables": true,
                        "retry_max_ms": 30000,
                        "request_timeout_ms": 30000
                    })
                },
                |config| Ok(Box::new(ydb::YdbSinkConnector::from_config(config)?)),
            )?
            .source_checker::<ydb::YdbConnectionCheckConfig, _, _>(|config| async move {
                if config.credentials_complete() {
                    ydb::check_connection(&config.connection()).await?;
                    Ok(transferia_registry::ConnectionCheckResult::default())
                } else {
                    ydb::check_network_connection(&config).await?;
                    Ok(transferia_registry::ConnectionCheckResult {
                        message: Some(
                            "YDB is network-reachable. Authentication and database access were not checked because database or credentials are incomplete."
                                .to_owned(),
                        ),
                        ..transferia_registry::ConnectionCheckResult::network_reachable()
                    })
                }
            })
            .sink_checker::<ydb::YdbConnectionCheckConfig, _, _>(|config| async move {
                if config.credentials_complete() {
                    ydb::check_connection(&config.connection()).await?;
                    Ok(transferia_registry::ConnectionCheckResult::default())
                } else {
                    ydb::check_network_connection(&config).await?;
                    Ok(transferia_registry::ConnectionCheckResult {
                        message: Some(
                            "YDB is network-reachable. Authentication and database access were not checked because database or credentials are incomplete."
                                .to_owned(),
                        ),
                        ..transferia_registry::ConnectionCheckResult::network_reachable()
                    })
                }
            }),
    )?;
    Ok(())
}
