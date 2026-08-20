extern crate alloc;

pub use transferia_provider_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod providers {
    pub use transferia_provider_support::address;

    pub mod postgres;
}

pub use providers::postgres;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("postgres", "PostgreSQL")
            .source::<postgres::src_batch::PostgresSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "host": "", "port": 5432, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true,
                        "tables": [{ "schema": "", "name": "" }], "batch_rows": 65536
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(postgres::PostgresSourceProvider::from_config(
                            config,
                            Arc::clone(&metrics),
                        )?))
                    }
                },
            )?
            .source_checker::<postgres::src_batch::PostgresSourceConfig, _, _>(
                |config| async move {
                    postgres::check_connection(&config.connection).await?;
                    Ok(transferia_registry::ConnectionCheckResult::default())
                },
            )
            .sink::<postgres::sink::PostgresSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "", "port": 5432, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true, "create_tables": true
                    })
                },
                |config| {
                    Ok(Box::new(postgres::PostgresSinkProvider::from_config(
                        config,
                    )?))
                },
            )?
            .sink_checker::<postgres::sink::PostgresSinkConfig, _, _>(|config| async move {
                postgres::check_connection(&config.connection).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            }),
    )?;
    Ok(())
}
