extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod kafka;
}

pub use connectors::kafka;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    register_with_parsers(
        registry,
        metrics,
        parsers::ParserPluginRegistry::default(),
    )
}

pub fn register_with_parsers(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
    parser_plugins: parsers::ParserPluginRegistry,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("kafka", "Kafka")
            .source_draft::<kafka::KafkaSourceConfig, _, _>(
                vec![DeliveryMode::Stream],
                true,
                || {
                    serde_json::json!({
                        "brokers": [""], "topics": [""], "consumer_group": "",
                        "security": { "type": "plaintext" }, "offset_reset": "earliest",
                        "parser": {}, "batch_max_messages": 1_000,
                        "batch_max_bytes": 16_777_216, "request_timeout_ms": 30_000
                    })
                },
                {
                    let metrics = Arc::clone(metrics);
                    let parser_plugins = parser_plugins.clone();
                    move |config| {
                        Ok(Box::new(kafka::KafkaSourceConnector::from_config_with_parsers(
                            config,
                            Arc::clone(&metrics),
                            &parser_plugins,
                        )?))
                    }
                },
            )?
            .source_checker::<kafka::KafkaSourceConfig, _, _>(|config| async move {
                kafka::check_source_connection(&config).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            })
            .sink::<kafka::KafkaSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "brokers": [""],
                        "topic": { "type": "topic", "topic": "" },
                        "security": { "type": "plaintext" },
                        "serializer": { "type": "json" }, "partition": null,
                        "request_timeout_ms": 30_000, "max_in_flight": 16
                    })
                },
                |config| Ok(Box::new(kafka::KafkaSinkConnector::from_config(config)?)),
            )?
            .sink_checker::<kafka::KafkaSinkConfig, _, _>(|config| async move {
                kafka::check_sink_connection(&config).await?;
                Ok(transferia_registry::ConnectionCheckResult::default())
            }),
    )?;
    Ok(())
}
