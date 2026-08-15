use std::sync::Arc;

use anyhow::Context as _;
use tokio_util::sync::CancellationToken;

use crate::compatibility::{validate_pipeline, DeliverySemanticsReport, SourceBehavior};
use crate::config::yaml::Config;
use crate::delivery::{DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest};
use crate::durable::DurableContext;
use crate::extension::{EndpointRole, Transferia};
use crate::metrics::MetricsRegistry;
use crate::middleware::build_middleware;
use crate::pipeline::middleware::Middleware;
use crate::providers::catalog::build_provider_catalog_with;
use crate::providers::traits::{SinkProvider, SourceProvider};

pub struct DeliveryPlan {
    pub config: Config,
    pub durable: DurableContext,
    pub metrics_registry: Arc<MetricsRegistry>,
    pub source_kind: String,
    pub sink_kind: String,
    pub source_provider: Arc<dyn SourceProvider>,
    pub sink_provider: Arc<dyn SinkProvider>,
    pub discovery: Arc<DeliveryDiscovery>,
    pub middlewares: Vec<Box<dyn Middleware>>,
    pub semantics: DeliverySemanticsReport,
    pub finite_source: bool,
}

pub async fn build_delivery_plan(
    config: Config,
    cancellation: CancellationToken,
) -> anyhow::Result<DeliveryPlan> {
    build_delivery_plan_with(config, cancellation, &Transferia::public()?).await
}

pub async fn build_delivery_plan_with(
    config: Config,
    cancellation: CancellationToken,
    transferia: &Transferia,
) -> anyhow::Result<DeliveryPlan> {
    let durable = config.durable_storage.build(&config.delivery_id)?;
    anyhow::ensure!(
        config.pipeline_memory_limit_bytes > 0,
        "pipeline_memory_limit_bytes must be positive"
    );

    let metrics_registry = Arc::new(MetricsRegistry::new());
    let catalog = build_provider_catalog_with(transferia, &metrics_registry)?;
    let source_kind = config.source.kind()?.to_owned();
    let sink_kind = config.sink.kind()?.to_owned();
    let source_config = transferia
        .registry()
        .resolve(
            &source_kind,
            EndpointRole::Source,
            config.source.raw()?.clone(),
        )
        .await?;
    let sink_config = transferia
        .registry()
        .resolve(&sink_kind, EndpointRole::Sink, config.sink.raw()?.clone())
        .await?;
    let source_provider: Arc<dyn SourceProvider> =
        Arc::from(catalog.build_source(&source_kind, source_config)?);
    let sink_provider: Arc<dyn SinkProvider> =
        Arc::from(catalog.build_sink(&sink_kind, sink_config)?);
    sink_provider.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;

    let source_descriptor = source_provider.compatibility();
    anyhow::ensure!(
        source_descriptor.supports_delivery_type(config.delivery_type),
        "source '{source_kind}' does not support delivery_type '{}'",
        config.delivery_type.label()
    );
    let finite_source =
        source_descriptor.source_behavior() == Some(SourceBehavior::FiniteSnapshotRows);
    let discovery = source_provider
        .delivery_discovery(
            DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation,
        )
        .await?;
    anyhow::ensure!(
        discovery.keep_system_columns,
        "source delivery discovery returned a system-column projection different from the requested policy"
    );

    let middlewares = config
        .middlewares
        .iter()
        .map(|middleware| build_middleware(middleware.kind()?, middleware.raw()?.clone()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    validate_middlewares(&middlewares, &discovery)?;
    let semantics = validate_discovered_pipeline(
        &source_descriptor,
        &sink_provider.compatibility(),
        sink_provider.limits(),
        &discovery,
        true,
    )?;

    Ok(DeliveryPlan {
        config,
        durable,
        metrics_registry,
        source_kind,
        sink_kind,
        source_provider,
        sink_provider,
        discovery: Arc::new(discovery),
        middlewares,
        semantics,
        finite_source,
    })
}

pub fn validate_discovered_pipeline(
    source: &crate::compatibility::EndpointDescriptor,
    sink: &crate::compatibility::EndpointDescriptor,
    limits: &dyn crate::delivery::SinkLimits,
    discovery: &DeliveryDiscovery,
    keep_system_columns: bool,
) -> anyhow::Result<DeliverySemanticsReport> {
    anyhow::ensure!(
        discovery.keep_system_columns == keep_system_columns,
        "delivery discovery system-column policy differs from pipeline configuration"
    );
    let semantics = validate_pipeline(source, sink, discovery, keep_system_columns);
    semantics.ensure_valid()?;
    limits
        .validate_discovery(discovery)
        .context("delivery violates sink limits")?;
    Ok(semantics)
}

fn validate_middlewares(
    middlewares: &[Box<dyn Middleware>],
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<()> {
    if middlewares.is_empty() {
        return Ok(());
    }
    let main = discovery
        .dataset(DatasetRole::Main)
        .context("middlewares require a discovered main dataset")?;
    for (index, middleware) in middlewares.iter().enumerate() {
        middleware
            .validate_schema(&main.incoming_schema)
            .with_context(|| format!("middleware {index} is incompatible with delivery schema"))?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/delivery_plan.rs"]
mod tests;
