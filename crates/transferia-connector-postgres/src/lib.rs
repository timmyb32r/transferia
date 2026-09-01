extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod postgres;
}

pub use connectors::postgres;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("postgres", "PostgreSQL")
            .source::<postgres::src_batch::PostgresSourceConfig, _, _>(
                vec![DeliveryMode::Batch, DeliveryMode::Stream],
                false,
                || {
                    serde_json::json!({
                        "host": "", "port": 5432, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true,
                        "tables": [{ "schema": "", "name": "" }], "batch_rows": 65536,
                        "replication": null
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(postgres::PostgresSourceConnector::from_config(
                            config,
                            Arc::clone(&metrics),
                        )?))
                    }
                },
            )?
            .source_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .source_checker::<postgres::PostgresConnectionCheckConfig, _, _>(|config| async move {
                check_postgres_connection(config).await
            })
            .sink::<postgres::sink::PostgresSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "", "port": 5432, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true, "create_tables": true
                    })
                },
                |config| {
                    Ok(Box::new(postgres::PostgresSinkConnector::from_config(
                        config,
                    )?))
                },
            )?
            .sink_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .sink_checker::<postgres::PostgresConnectionCheckConfig, _, _>(|config| async move {
                check_postgres_connection(config).await
            }),
    )?;
    Ok(())
}

async fn check_postgres_connection(
    config: postgres::PostgresConnectionCheckConfig,
) -> anyhow::Result<transferia_registry::ConnectionCheckResult> {
    if config.credentials_complete() {
        postgres::check_connection(&config.connection()).await?;
        Ok(transferia_registry::ConnectionCheckResult::default())
    } else {
        postgres::check_network_connection(&config).await?;
        Ok(transferia_registry::ConnectionCheckResult {
            message: Some(
                "PostgreSQL is network-reachable. Authentication was not checked because database or username is incomplete."
                    .to_owned(),
            ),
            ..transferia_registry::ConnectionCheckResult::network_reachable()
        })
    }
}

#[cfg(test)]
mod tests;
