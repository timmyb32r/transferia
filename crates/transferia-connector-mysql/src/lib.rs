extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod mysql;
}

pub use connectors::mysql;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("mysql", "MySQL")
            .source::<mysql::src_batch::MySqlSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "host": "", "port": 3306, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true,
                        "tables": [{ "name": "" }], "batch_rows": 65536
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(mysql::MySqlSourceConnector::from_config(
                            config,
                            Arc::clone(&metrics),
                        )?))
                    }
                },
            )?
            .source_checker::<mysql::MySqlConnectionCheckConfig, _, _>(|config| async move {
                check_mysql_connection(config).await
            }),
    )?;
    Ok(())
}

async fn check_mysql_connection(
    config: mysql::MySqlConnectionCheckConfig,
) -> anyhow::Result<transferia_registry::ConnectionCheckResult> {
    if config.credentials_complete() {
        mysql::check_connection(&config.connection()).await?;
        Ok(transferia_registry::ConnectionCheckResult::default())
    } else {
        mysql::check_network_connection(&config).await?;
        Ok(transferia_registry::ConnectionCheckResult {
            message: Some(
                "MySQL is network-reachable. Authentication was not checked because database or username is incomplete."
                    .to_owned(),
            ),
            ..transferia_registry::ConnectionCheckResult::network_reachable()
        })
    }
}

#[cfg(test)]
mod tests;
