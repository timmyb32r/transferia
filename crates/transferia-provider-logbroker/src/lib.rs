extern crate alloc;

pub use transferia_provider_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod providers {
    pub use transferia_provider_support::address;

    pub mod logbroker;
}

pub use providers::logbroker;

use std::sync::Arc;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
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
                { let metrics = Arc::clone(metrics); move |config| logbroker::build_source_provider(config, Arc::clone(&metrics)) },
            )?
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
                } else if config.auth.is_configured() {
                    logbroker::src_stream::check_authentication(&config.host, config.port, &config.auth, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult {
                        message: Some("The token is valid for Logbroker. Topic and consumer access were not checked because they are not configured.".to_owned()),
                        ..Default::default()
                    })
                } else {
                    logbroker::check_network_connection(&config.host, config.port, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult::network_reachable())
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
                    "host": "", "port": 2135, "topic_path": "", "partition_id": null,
                    "auth": { "type": "token", "token": "" },
                    "serializer": { "type": "json" }, "driver": "ydb", "trusted_plaintext": true
                }),
                logbroker::build_sink_provider,
            )?
            .sink_checker::<logbroker::sink::LogbrokerSinkCheckConfig, _, _>(|config| async move {
                let cancellation = tokio_util::sync::CancellationToken::new();
                if !config.auth.is_configured() {
                    logbroker::check_network_connection(&config.host, config.port, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult::network_reachable())
                } else if config.topic_path.is_empty() {
                    logbroker::src_stream::check_authentication(&config.host, config.port, &config.auth, cancellation).await?;
                    Ok(transferia_registry::ConnectionCheckResult {
                        message: Some("The token is valid for Logbroker. Topic access was not checked because it is not configured.".to_owned()),
                        ..Default::default()
                    })
                } else {
                    logbroker::src_stream::check_topic_connection(
                        &config.host, config.port, &config.topic_path, &config.auth,
                        config.driver.unwrap_or(logbroker::LogbrokerDriver::Ydb), cancellation,
                    ).await?;
                    Ok(transferia_registry::ConnectionCheckResult::default())
                }
            }),
    )?;
    Ok(())
}
