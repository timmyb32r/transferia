extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod clickhouse;
}

pub use connectors::clickhouse;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("clickhouse", "ClickHouse")
            .source::<clickhouse::src_batch::ClickHouseSourceConfig, _, _>(
                vec![DeliveryMode::Batch], false,
                || serde_json::json!({
                    "hosts": [""], "port": clickhouse::DEFAULT_NATIVE_PORT,
                    "trusted_plaintext": true, "username": "", "password": "",
                    "tables": [{ "database": "", "name": "" }],
                    "batch_rows": 65536, "connect_timeout_ms": 30000, "request_timeout_ms": 30000
                }),
                { let metrics = Arc::clone(metrics); move |config| Ok(Box::new(clickhouse::ClickHouseSourceConnector::from_config(config, Arc::clone(&metrics))?)) },
            )?
            .source_checker::<clickhouse::src_batch::ClickHouseSourceConfig, _, _>({
                let metrics = Arc::clone(metrics);
                move |config| { let metrics = Arc::clone(&metrics); async move {
                    clickhouse::ClickHouseSourceConnector::check_connection(config, metrics).await
                }}
            })
            .sink::<clickhouse::ClickHouseSinkConfig, _, _>(
                || serde_json::json!({
                    "hosts": [""], "port": clickhouse::DEFAULT_NATIVE_PORT,
                    "trusted_plaintext": true, "database": "", "username": "",
                    "password": "", "shard_group": "", "insert_target_rows": 100_000,
                    "insert_target_bytes": 67_108_864, "flush_interval_ms": 100,
                    "retry_initial_ms": 50, "retry_max_ms": 30000,
                    "connect_timeout_ms": 30000, "request_timeout_ms": 30000
                }),
                |config| Ok(Box::new(clickhouse::ClickHouseSinkConnector::from_config(config)?)),
            )?
            .sink_checker::<clickhouse::ClickHouseSinkConfig, _, _>(|config| async move {
                let checked = clickhouse::ClickHouseSinkConnector::check_connection(config).await?;
                Ok(sink_connection_check_result(checked))
            }),
    )?;
    Ok(())
}

fn sink_connection_check_result(
    checked: clickhouse::sink::ClickHouseConnectionCheck,
) -> transferia_registry::ConnectionCheckResult {
    match checked {
        clickhouse::sink::ClickHouseConnectionCheck::Verified { shard_groups } => {
            transferia_registry::ConnectionCheckResult {
                options: std::collections::BTreeMap::from([(
                    "#/shard_group".to_owned(),
                    shard_groups,
                )]),
                ..Default::default()
            }
        }
        clickhouse::sink::ClickHouseConnectionCheck::NetworkReachable => {
            transferia_registry::ConnectionCheckResult::network_reachable()
        }
    }
}
