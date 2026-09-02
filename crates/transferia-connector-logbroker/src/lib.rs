extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod connectors {
    pub use transferia_connector_support::address;

    pub mod logbroker;
}

pub use connectors::logbroker;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    register_with_parsers(registry, metrics, parsers::ParserPluginRegistry::default())
}

pub fn register_with_parsers(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
    parser_plugins: parsers::ParserPluginRegistry,
) -> anyhow::Result<()> {
    let schema_preview_plugins = parser_plugins.clone();
    registry.register(
        ComponentRegistration::new("logbroker", "Logbroker")
            .source_draft::<logbroker::src_stream::LogbrokerSourceConfig, _, _>(
                vec![DeliveryMode::Stream], true,
                || serde_json::json!({
                    "host": "", "port": 2135,
                    "topics": [{ "path": "", "partitions": [] }], "consumer_name": "",
                    "auth": { "type": "token", "token": "" }, "driver": "ydb",
                    "trusted_plaintext": true, "allow_ttl_rewind": false,
                    "parser": {}, "read_buffer_bytes": 1_048_576
                }),
                {
                    let metrics = Arc::clone(metrics);
                    move |config| logbroker::build_source_connector_with_parsers(
                        config,
                        Arc::clone(&metrics),
                        &parser_plugins,
                    )
                },
            )?
            .source_record_semantics(vec![RecordSemantics::AppendOnly, RecordSemantics::Changelog])?
            .source_schema_previewer(move |raw, request, _cancellation| {
                let parser_plugins = schema_preview_plugins.clone();
                async move {
                    let parser = raw
                        .get("parser")
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("select a parser first"))?;
                    let parser: parsers::ParserConfig = serde_yaml::from_value(parser)?;
                    let source_name = raw
                        .get("topics")
                        .and_then(serde_yaml::Value::as_sequence)
                        .and_then(|topics| topics.first())
                        .and_then(|topic| topic.get("path"))
                        .and_then(serde_yaml::Value::as_str)
                        .unwrap_or_default();
                    let plan = parsers::ParserPlan::from_config_with_plugins(
                        &parser,
                        source_name,
                        &parser_plugins,
                    )?;
                    let discovery_source = if source_name.is_empty() {
                        plan.table()
                    } else {
                        Arc::from(source_name)
                    };
                    plan.delivery_discovery(
                        discovery_source,
                        transferia_core::delivery::SourceTopology::DynamicWorkerLanes,
                        request,
                    )
                }
            })
            .source_checker::<logbroker::src_stream::LogbrokerSourceCheckConfig, _, _>(|config| async move {
                let cancellation = tokio_util::sync::CancellationToken::new();
                let complete = !config.consumer_name.is_empty()
                    && config.topics.first().is_some_and(|topic| !topic.path.is_empty());
                if complete && config.auth.is_configured() {
                    let connection = logbroker::src_stream::LogbrokerSourceConnectionConfig {
                        host: config.host, port: config.port, topics: config.topics,
                        consumer_name: config.consumer_name, auth: config.auth,
                        driver: config.driver.unwrap_or(logbroker::LogbrokerDriver::Ydb),
                        trusted_plaintext: config.trusted_plaintext,
                        read_buffer_bytes: config.read_buffer_bytes,
                    };
                    logbroker::check_connection(&connection, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult::default())
                } else {
                    logbroker::check_network_connection(&config.host, config.port, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult {
                        message: Some("Logbroker is network-reachable. Authentication and entity access were not checked because topic and consumer are incomplete.".to_owned()),
                        ..transferia_registry::ConnectionCheckResult::network_reachable()
                    })
                }
            })
            .source_previewer::<logbroker::src_stream::LogbrokerSourceConnectionConfig, _, _>(|config, max_bytes, cancellation| async move {
                let preview = logbroker::preview_message(&config, max_bytes, cancellation).await?;
                Ok(transferia_registry::SourcePreview {
                    payload: preview.payload.to_vec(),
                    detection_payloads: preview.detection_payloads.into_iter().map(|value| value.to_vec()).collect(),
                    metadata: transferia_registry::SourcePreviewMetadata {
                        topic: preview.metadata.topic, partition: preview.metadata.partition,
                        partition_session_id: preview.metadata.partition_session_id,
                        offset: preview.metadata.offset, sequence_number: preview.metadata.sequence_number,
                        created_at_ms: preview.metadata.created_at_ms, written_at_ms: preview.metadata.written_at_ms,
                        producer_id: preview.metadata.producer_id, message_group_id: preview.metadata.message_group_id,
                        codec: preview.metadata.codec, compressed_size: preview.metadata.compressed_size,
                        declared_uncompressed_size: preview.metadata.declared_uncompressed_size,
                        message_metadata: preview.metadata.message_metadata.into_iter().map(|item| transferia_registry::SourcePreviewMetadataItem { key: item.key, value: item.value }).collect(),
                        write_session_metadata: preview.metadata.write_session_metadata,
                    },
                })
            })
            .sink::<logbroker::sink::LogbrokerSinkConfig, _, _>(
                || serde_json::json!({
                    "host": "", "port": 2135,
                    "topic": { "type": "topic", "topic_path": "" }, "partition_id": null,
                    "auth": { "type": "token", "token": "" },
                    "serializer": { "type": "json" }, "driver": "ydb", "trusted_plaintext": true
                }),
                logbroker::build_sink_connector,
            )?
            .sink_checker::<logbroker::sink::LogbrokerSinkCheckConfig, _, _>(|config| async move {
                let cancellation = tokio_util::sync::CancellationToken::new();
                if !config.auth.is_configured() {
                    logbroker::check_network_connection(&config.host, config.port, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult::network_reachable())
                } else if config.topic.as_ref().and_then(|topic| topic.fixed_topic()).is_none() {
                    logbroker::check_network_connection(&config.host, config.port, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult {
                        message: Some("Logbroker is network-reachable. Authentication and entity access were not checked because topic is incomplete.".to_owned()),
                        ..transferia_registry::ConnectionCheckResult::network_reachable()
                    })
                } else {
                    logbroker::src_stream::check_topic_connection(
                        &config.host, config.port,
                        config.topic.as_ref().and_then(|topic| topic.fixed_topic()).ok_or_else(|| anyhow::anyhow!("Logbroker topic is unavailable"))?,
                        &config.auth,
                        config.driver.unwrap_or(logbroker::LogbrokerDriver::Ydb), cancellation,
                    ).await?;
                    Ok(transferia_registry::ConnectionCheckResult::default())
                }
            }),
    )?;
    Ok(())
}
