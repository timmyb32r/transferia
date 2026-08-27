extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod ytsaurus;
}

pub use connectors::ytsaurus;

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("ytsaurus", "YTsaurus")
            .source::<ytsaurus::YTsaurusSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "auth": { "type": "token", "token": "" },
                        "host": "", "port": 8000, "trusted_plaintext": true,
                        "trusted_native_rpc_plaintext": false,
                        "timeout_ms": 30000,
                        "tables": [{ "path": "" }],
                        "read_ordering": { "type": "ordered" },
                        "native_rpc_service_ticket_file": null,
                        "table_reader": {},
                        "batch_rows": 65536,
                        "stream_retry_max_attempts": 12,
                        "stream_retry_initial_ms": 100,
                        "stream_retry_max_ms": 5000,
                        "stream_open_timeout_ms": 30000,
                        "stream_idle_timeout_ms": 30000
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(ytsaurus::YTsaurusSourceConnector::from_config(
                            config,
                            Arc::clone(&metrics),
                        )?))
                    }
                },
            )?
            .source_checker::<ytsaurus::YTsaurusSourceConfig, _, _>(|config| async move {
                ytsaurus::check_connection(&config.connection).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            })
            .sink_draft::<ytsaurus::YTsaurusSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "auth": { "type": "token", "token": "" },
                        "host": "", "port": 8000, "trusted_plaintext": true,
                        "trusted_native_rpc_plaintext": false,
                        "timeout_ms": 30000
                    })
                },
                |config| {
                    Ok(Box::new(ytsaurus::YTsaurusSinkConnector::from_config(
                        config,
                    )?))
                },
            )?
            .sink_checker::<ytsaurus::YTsaurusSinkConfig, _, _>(|config| async move {
                ytsaurus::check_connection(&config.connection).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            }),
    )?;
    Ok(())
}
