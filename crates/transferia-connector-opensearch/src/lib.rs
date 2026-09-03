extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod opensearch;

use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::{ComponentRegistration, DeliveryMode, RegistryBuilder};

pub fn register(
    registry: &mut RegistryBuilder,
    metrics: &Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    registry.register(
        ComponentRegistration::new("opensearch", "OpenSearch")
            .source::<opensearch::src_batch::OpenSearchSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                opensearch::src_batch::initial_config,
                {
                    let metrics = Arc::clone(metrics);
                    move |config| {
                        Ok(Box::new(
                            opensearch::src_batch::OpenSearchSourceConnector::from_config(
                                config,
                                Arc::clone(&metrics),
                            )?,
                        ))
                    }
                },
            )?
            .source_checker::<opensearch::src_batch::OpenSearchSourceConfig, _, _>({
                let metrics = Arc::clone(metrics);
                move |config| {
                    let metrics = Arc::clone(&metrics);
                    async move {
                        opensearch::src_batch::OpenSearchSourceConnector::check_connection(
                            config, metrics,
                        )
                        .await
                    }
                }
            })
            .sink::<opensearch::sink::OpenSearchSinkConfig, _, _>(
                opensearch::sink::initial_config,
                |config| {
                    Ok(Box::new(
                        opensearch::sink::OpenSearchSinkConnector::from_config(config)?,
                    ))
                },
            )?
            .sink_record_semantics(vec![RecordSemantics::AppendOnly])?
            .sink_checker::<opensearch::sink::OpenSearchSinkConfig, _, _>(|config| async move {
                opensearch::sink::OpenSearchSinkConnector::check_connection(config).await
            }),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
