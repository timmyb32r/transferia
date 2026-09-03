extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod clickhouse;
}

pub use connectors::clickhouse;

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
        ComponentRegistration::new("clickhouse", "ClickHouse")
            .source::<clickhouse::src_batch::ClickHouseSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "hosts": [""], "port": clickhouse::DEFAULT_NATIVE_PORT,
                        "http_port": 8123,
                        "trusted_plaintext": true, "username": "", "password": "",
                        "tables": [{ "database": "", "name": "" }],
                        "batch_rows": 65409,
                        "snapshot_reader": {
                            "type": "parquet", "compression": "zstd", "max_threads": 32,
                            "row_group_rows": 250_000, "decode_threads": 16,
                            "max_response_bytes": 2_147_483_648_u64
                        },
                        "connect_timeout_ms": 30000, "request_timeout_ms": 30000
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(
                            clickhouse::ClickHouseSourceConnector::from_config(
                                config,
                                Arc::clone(&metrics),
                            )?,
                        ))
                    }
                },
            )?
            .source_tuning_parameters(clickhouse_source_tuning_parameters())?
            .source_checker::<clickhouse::src_batch::ClickHouseSourceConfig, _, _>({
                let metrics = Arc::clone(metrics);
                move |config| {
                    let metrics = Arc::clone(&metrics);
                    async move {
                        clickhouse::ClickHouseSourceConnector::check_connection(config, metrics)
                            .await
                    }
                }
            })
            .sink::<clickhouse::ClickHouseSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "hosts": [""], "port": clickhouse::DEFAULT_NATIVE_PORT,
                        "http_port": 8123,
                        "trusted_plaintext": true, "database": "", "username": "",
                        "password": "", "shard_group": "", "insert_target_rows": 1_000_000,
                        "insert_target_bytes": 671_088_640, "insert_concurrency": 32,
                        "insert_format": "native", "compression": "zstd",
                        "format_threads": 8, "parquet_row_group_rows": 1_000_000,
                        "flush_interval_ms": 100,
                        "retry_initial_ms": 50, "retry_max_ms": 30000,
                        "connect_timeout_ms": 30000, "request_timeout_ms": 30000
                    })
                },
                |config| {
                    Ok(Box::new(clickhouse::ClickHouseSinkConnector::from_config(
                        config,
                    )?))
                },
            )?
            .sink_tuning_parameters(clickhouse_sink_tuning_parameters())?
            .sink_record_semantics(vec![
                RecordSemantics::AppendOnly,
                RecordSemantics::Changelog,
            ])?
            .sink_checker::<clickhouse::ClickHouseSinkConfig, _, _>(|config| async move {
                let checked = clickhouse::ClickHouseSinkConnector::check_connection(config).await?;
                Ok(sink_connection_check_result(checked))
            }),
    )?;
    Ok(())
}

fn clickhouse_source_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/batch_rows".to_owned(),
            label: "Maximum block rows".to_owned(),
            baseline: 65_409,
            minimum: 1,
            maximum: i64::MAX as u64,
            candidates: vec![16_384, 65_409, 262_144, 1_000_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::Choice {
            pointer: "/snapshot_reader/compression".to_owned(),
            label: "Snapshot compression".to_owned(),
            baseline: serde_json::Value::from("zstd"),
            values: vec!["zstd", "lz4"]
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        },
        TuningParameter::UnsignedInteger {
            pointer: "/snapshot_reader/row_group_rows".to_owned(),
            label: "Rows per Parquet row group".to_owned(),
            baseline: 250_000,
            minimum: 1,
            maximum: i64::MAX as u64,
            candidates: vec![65_536, 250_000, 1_000_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/snapshot_reader/decode_threads".to_owned(),
            label: "Parquet decode threads".to_owned(),
            baseline: 16,
            minimum: 1,
            maximum: 32,
            candidates: vec![1, 4, 8, 16, 32],
            scale: NumericScale::Logarithmic,
        },
    ]
}

fn clickhouse_sink_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/insert_target_rows".to_owned(),
            label: "Rows per insert".to_owned(),
            baseline: 1_000_000,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![100_000, 250_000, 500_000, 1_000_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/insert_target_bytes".to_owned(),
            label: "Bytes per insert".to_owned(),
            baseline: 640 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![64 << 20, 256 << 20, 640 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/insert_concurrency".to_owned(),
            label: "Concurrent inserts".to_owned(),
            baseline: 32,
            minimum: 1,
            maximum: 32,
            candidates: vec![1, 2, 4, 8, 16, 32],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::Choice {
            pointer: "/compression".to_owned(),
            label: "Native compression".to_owned(),
            baseline: serde_json::Value::from("zstd"),
            values: vec!["zstd", "lz4", "none"]
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        },
    ]
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
