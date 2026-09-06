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
use transferia_registry::tuning::{NumericScale, TuningParameter};
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("postgres", "PostgreSQL")
            .source::<postgres::source::PostgresSourceConfig, _, _>(
                vec![
                    DeliveryMode::Batch,
                    DeliveryMode::Stream,
                    DeliveryMode::BatchAndStream,
                ],
                false,
                || {
                    serde_json::json!({
                        "host": "", "port": 5432, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true,
                        "hide_system_tables": true,
                        "tables": { "type": "selected", "rules": [] }, "batch_rows": 16384,
                        "copy_to_format": "binary"
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
            .source_tuning_parameters(postgres_source_tuning_parameters())?
            .source_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .source_checker::<postgres::PostgresConnectionCheckConfig, _, _>(|config| async move {
                let mut result = check_postgres_connection(config.clone()).await?;
                if config.credentials_complete() {
                    result.tables = Some(postgres::list_tables(&config.connection()).await?);
                }
                Ok(result)
            })
            .sink::<postgres::sink::PostgresSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "", "port": 5432, "database": "", "username": "",
                        "password": "", "trusted_plaintext": true, "create_tables": true,
                        "copy_from_format": "binary"
                    })
                },
                |config| {
                    Ok(Box::new(postgres::PostgresSinkConnector::from_config(
                        config,
                    )?))
                },
            )?
            .sink_tuning_parameters(postgres_sink_tuning_parameters())?
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

fn postgres_source_tuning_parameters() -> Vec<TuningParameter> {
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
            pointer: "/copy_to_format".to_owned(),
            label: "COPY TO format".to_owned(),
            baseline: serde_json::Value::from("binary"),
            values: ["binary", "text"]
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        },
    ]
}

fn postgres_sink_tuning_parameters() -> Vec<TuningParameter> {
    vec![TuningParameter::Choice {
        pointer: "/copy_from_format".to_owned(),
        label: "COPY FROM format".to_owned(),
        baseline: serde_json::Value::from("binary"),
        values: ["binary", "text"]
            .into_iter()
            .map(serde_json::Value::from)
            .collect(),
    }]
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
