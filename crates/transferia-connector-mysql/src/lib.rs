extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod mysql;
}

pub use connectors::mysql;

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
        ComponentRegistration::new("mysql", "MySQL")
            .source::<mysql::src_batch::MySqlSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "host": "", "port": 3306, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true,
                        "tables": [{ "name": "" }], "batch_rows": 16384,
                        "read_protocol": "binary"
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
            .source_tuning_parameters(mysql_source_tuning_parameters())?
            .source_checker::<mysql::MySqlConnectionCheckConfig, _, _>(|config| async move {
                check_mysql_connection(config).await
            })
            .sink::<mysql::sink::MySqlSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "", "port": 3306, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true,
                        "create_tables": true, "insert_rows": 1000
                    })
                },
                |config| Ok(Box::new(mysql::MySqlSinkConnector::from_config(config)?)),
            )?
            .sink_tuning_parameters(mysql_sink_tuning_parameters())?
            .sink_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .sink_checker::<mysql::MySqlConnectionCheckConfig, _, _>(|config| async move {
                check_mysql_connection(config).await
            }),
    )?;
    Ok(())
}

fn mysql_source_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/batch_rows".to_owned(),
            label: "Rows per snapshot batch".to_owned(),
            baseline: 16_384,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![16_384, 65_536, 262_144, 1_048_576],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::Choice {
            pointer: "/read_protocol".to_owned(),
            label: "Read protocol".to_owned(),
            baseline: serde_json::Value::from("binary"),
            values: ["text", "binary"]
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        },
    ]
}

fn mysql_sink_tuning_parameters() -> Vec<TuningParameter> {
    vec![TuningParameter::UnsignedInteger {
        pointer: "/insert_rows".to_owned(),
        label: "Rows per INSERT".to_owned(),
        baseline: 1_000,
        minimum: 1,
        maximum: u64::MAX,
        candidates: vec![100, 250, 1_000, 4_000],
        scale: NumericScale::Logarithmic,
    }]
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
