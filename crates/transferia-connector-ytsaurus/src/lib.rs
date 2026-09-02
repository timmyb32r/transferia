extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod ytsaurus;
}

pub use connectors::ytsaurus;

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::{
    ComponentRegistration, ConnectionCheckResult, DeliveryMode, RegistryBuilder,
};

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
            .source_checker::<ytsaurus::YTsaurusSourceConfig, _, _>(check_source_connection)
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
            .sink_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .sink_checker::<ytsaurus::YTsaurusSinkConfig, _, _>(check_sink_connection),
    )?;
    Ok(())
}

async fn check_source_connection(
    config: ytsaurus::YTsaurusSourceConfig,
) -> anyhow::Result<ConnectionCheckResult> {
    let paths = configured_source_paths(&config);
    let result = if let Some(paths) = paths {
        ytsaurus::check_source_tables(&config.connection, &paths).await?;
        ConnectionCheckResult::default()
    } else {
        ytsaurus::check_connection(&config.connection).await?;
        incomplete_entities_result(
            "YTsaurus connection and authentication are verified, but source table access was not checked because at least one table path is empty.",
        )
    };
    with_rpc_proxy_roles(&config.connection, result, "#/proxy_role").await
}

async fn check_sink_connection(
    config: ytsaurus::YTsaurusSinkConfig,
) -> anyhow::Result<ConnectionCheckResult> {
    let path = config.path().trim();
    let result = if path.is_empty() {
        ytsaurus::check_connection(&config.connection).await?;
        incomplete_entities_result(
            "YTsaurus connection and authentication are verified, but destination entity access was not checked because Path is empty.",
        )
    } else {
        ytsaurus::check_sink_directory(&config.connection, path).await?;
        ConnectionCheckResult::default()
    };
    with_rpc_proxy_roles(&config.connection, result, "#/tables/proxy_role").await
}

async fn with_rpc_proxy_roles(
    connection: &ytsaurus::YTsaurusConnectionConfig,
    mut result: ConnectionCheckResult,
    config_path: &str,
) -> anyhow::Result<ConnectionCheckResult> {
    result.options.insert(
        config_path.to_owned(),
        ytsaurus::list_rpc_proxy_roles(connection).await?,
    );
    Ok(result)
}

fn configured_source_paths(config: &ytsaurus::YTsaurusSourceConfig) -> Option<Vec<String>> {
    (!config.tables.is_empty()
        && config
            .tables
            .iter()
            .all(|table| !table.path.trim().is_empty()))
    .then(|| {
        config
            .tables
            .iter()
            .map(|table| table.path.clone())
            .collect()
    })
}

fn incomplete_entities_result(message: &str) -> ConnectionCheckResult {
    ConnectionCheckResult {
        message: Some(message.to_owned()),
        ..ConnectionCheckResult::network_reachable()
    }
}

#[cfg(test)]
mod tests;
