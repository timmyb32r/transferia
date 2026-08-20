extern crate alloc;

pub use transferia_provider_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod providers {
    pub use transferia_provider_support::address;

    pub mod ytsaurus;
}

pub use providers::ytsaurus;

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
                        "host": "", "port": 8000, "trusted_plaintext": true,
                        "timeout_ms": 30000,
                        "tables": [{ "path": "", "output_name": "" }],
                        "batch_rows": 65536
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(ytsaurus::YTsaurusSourceProvider::from_config(
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
            .sink::<ytsaurus::YTsaurusSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "", "port": 8000, "trusted_plaintext": true,
                        "timeout_ms": 30000, "path": "",
                        "replace_tables": true, "format": "arrow"
                    })
                },
                |config| {
                    Ok(Box::new(ytsaurus::YTsaurusSinkProvider::from_config(
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
