extern crate alloc;

pub use transferia_connector_support::{durable, metrics, parsers, schema_registry, serializer};

pub mod opensearch;

use std::sync::Arc;

use reqwest::Method;
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
            .source_checker::<opensearch::OpenSearchConnectionCheckConfig, _, _>(
                check_opensearch_connection,
            )
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
            .sink_checker::<opensearch::OpenSearchConnectionCheckConfig, _, _>(
                check_opensearch_connection,
            ),
    )?;
    Ok(())
}

async fn check_opensearch_connection(
    config: opensearch::OpenSearchConnectionCheckConfig,
) -> anyhow::Result<transferia_registry::ConnectionCheckResult> {
    if config.credentials_complete() {
        let connection = config
            .connection()
            .ok_or_else(|| anyhow::anyhow!("OpenSearch authentication is incomplete"))?;
        let client = opensearch::OpenSearchClient::new(&connection)?;
        client
            .request(Method::GET, &[], &[], "application/json", None)
            .await?;
        Ok(transferia_registry::ConnectionCheckResult::default())
    } else {
        check_opensearch_network_connection(&config).await?;
        Ok(transferia_registry::ConnectionCheckResult {
            message: Some(
                "OpenSearch is network-reachable. Authentication was not checked because credentials are incomplete."
                    .to_owned(),
            ),
            ..transferia_registry::ConnectionCheckResult::network_reachable()
        })
    }
}

async fn check_opensearch_network_connection(
    config: &opensearch::OpenSearchConnectionCheckConfig,
) -> anyhow::Result<()> {
    anyhow::ensure!(!config.hosts.is_empty(), "opensearch.hosts must not be empty");
    transferia_connector_support::address::validate_port("opensearch.port", config.port)?;
    let mut failures = Vec::new();
    for host in &config.hosts {
        transferia_connector_support::address::validate_host("opensearch.hosts", host)?;
        match tokio::net::TcpStream::connect((host.as_str(), config.port)).await {
            Ok(_) => return Ok(()),
            Err(error) => failures.push(format!("{host}: {error}")),
        }
    }
    anyhow::bail!(
        "no OpenSearch host is network-reachable on port {}: {}",
        config.port,
        failures.join("; ")
    )
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
