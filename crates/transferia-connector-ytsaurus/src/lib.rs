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
use transferia_registry::tuning::{NumericScale, TuningParameter};
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
            .source_tuning_parameters(ytsaurus_source_tuning_parameters())?
            .source_checker::<ytsaurus::YTsaurusSourceConfig, _, _>(check_source_connection)
            .sink_draft::<ytsaurus::YTsaurusSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "auth": { "type": "token", "token": "" },
                        "host": "", "port": 8000, "trusted_plaintext": true,
                        "trusted_native_rpc_plaintext": false,
                        "timeout_ms": 30000,
                        "write_target_bytes": 536_870_912_u64,
                        "write_concurrency": 4,
                        "write_flush_interval_ms": 1000,
                        "write_row_buffer_bytes": 1_048_576_u64,
                        "table_writer": {
                            "block_size": 16_777_216_u64,
                            "max_buffer_size": 16_777_216_u64,
                            "writer_window_size": 67_108_864_u64,
                            "writer_group_size": 16_777_216_u64,
                            "desired_chunk_size": 2_147_483_648_u64
                        },
                        "primary_key_sort_timeout_ms": 86_400_000_u64
                    })
                },
                |config| {
                    Ok(Box::new(ytsaurus::YTsaurusSinkConnector::from_config(
                        config,
                    )?))
                },
            )?
            .sink_tuning_parameters(ytsaurus_sink_tuning_parameters())?
            .sink_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .sink_checker::<ytsaurus::YTsaurusSinkConfig, _, _>(check_sink_connection),
    )?;
    Ok(())
}

fn ytsaurus_source_tuning_parameters() -> Vec<TuningParameter> {
    vec![TuningParameter::UnsignedInteger {
        pointer: "/batch_rows".to_owned(),
        label: "Rows per Arrow batch".to_owned(),
        baseline: 65_536,
        minimum: 1,
        maximum: u64::MAX,
        candidates: vec![16_384, 65_536, 262_144, 1_048_576],
        scale: NumericScale::Logarithmic,
    }]
}

fn ytsaurus_sink_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/write_target_bytes".to_owned(),
            label: "Write target bytes".to_owned(),
            baseline: 512 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![64 << 20, 256 << 20, 512 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/write_concurrency".to_owned(),
            label: "Concurrent writes".to_owned(),
            baseline: 4,
            minimum: 1,
            maximum: 32,
            candidates: vec![1, 2, 4, 8],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/write_flush_interval_ms".to_owned(),
            label: "Write flush interval".to_owned(),
            baseline: 1_000,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![50, 100, 250, 1_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/write_row_buffer_bytes".to_owned(),
            label: "Writer row buffer".to_owned(),
            baseline: 1 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![256 << 10, 512 << 10, 1 << 20],
            scale: NumericScale::Logarithmic,
        },
    ]
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
