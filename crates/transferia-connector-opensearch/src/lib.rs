extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod opensearch;

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
            .source_tuning_parameters(opensearch_source_tuning_parameters())?
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
            .sink_tuning_parameters(opensearch_sink_tuning_parameters())?
            .sink_record_semantics(vec![RecordSemantics::AppendOnly])?
            .sink_checker::<opensearch::sink::OpenSearchSinkConfig, _, _>(|config| async move {
                opensearch::sink::OpenSearchSinkConnector::check_connection(config).await
            }),
    )?;
    Ok(())
}

fn opensearch_source_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/page_rows".to_owned(),
            label: "Rows per search page".to_owned(),
            baseline: 10_000,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![2_500, 5_000, 10_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/read_concurrency".to_owned(),
            label: "Parallel shard readers".to_owned(),
            baseline: 2,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![1, 2, 4, 8],
            scale: NumericScale::Logarithmic,
        },
    ]
}

fn opensearch_sink_tuning_parameters() -> Vec<TuningParameter> {
    vec![
        TuningParameter::UnsignedInteger {
            pointer: "/bulk_target_rows".to_owned(),
            label: "Rows per bulk request".to_owned(),
            baseline: 20_000,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![2_500, 10_000, 20_000],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/bulk_target_bytes".to_owned(),
            label: "Bytes per bulk request".to_owned(),
            baseline: 16 << 20,
            minimum: 1,
            maximum: u64::MAX,
            candidates: vec![4 << 20, 16 << 20, 32 << 20],
            scale: NumericScale::Logarithmic,
        },
        TuningParameter::UnsignedInteger {
            pointer: "/bulk_concurrency".to_owned(),
            label: "Concurrent bulk requests".to_owned(),
            baseline: 4,
            minimum: 1,
            maximum: 32,
            candidates: vec![1, 2, 4, 8],
            scale: NumericScale::Logarithmic,
        },
    ]
}

#[cfg(test)]
mod tests;
