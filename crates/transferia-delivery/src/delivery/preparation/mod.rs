use std::sync::Arc;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::delivery::config::yaml::Config;
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest};
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::middleware::Middleware;
use transferia_delivery_contracts::semantics::{
    validate_pipeline, validate_record_semantics, DeliverySemanticsReport, SourceBehavior,
};
use transferia_registry::durable::DurableContext;
use transferia_registry::{
    Composition, EndpointRole, SinkConnector, SourceConnector, SourceDiscoveryContext,
};

pub struct DeliveryPlan {
    pub pipelines: Vec<PipelinePlan>,

    composition_fingerprint: String,

    replay_identity: Option<Arc<str>>,
}

pub struct PipelinePlan {
    pub config: Config,
    pub replay_identity: Option<Arc<str>>,
    pub durable: DurableContext,
    pub metrics_registry: Arc<MetricsRegistry>,
    pub source_kind: String,
    pub sink_kind: String,
    pub source_connector: Arc<dyn SourceConnector>,
    pub sink_connector: Arc<dyn SinkConnector>,
    pub discovery: Arc<DeliveryDiscovery>,
    pub middlewares: Vec<Box<dyn Middleware>>,
    pub semantics: DeliverySemanticsReport,
    pub finite_source: bool,
}

pub struct ResolvedDeliveryConfig {
    yaml: String,

    composition_fingerprint: String,
}

impl DeliveryPlan {
    pub fn resolved_config(&self) -> anyhow::Result<ResolvedDeliveryConfig> {
        let document = ResolvedConfigDocument {
            replay_identity: self.replay_identity.as_deref().map(str::to_owned),
            pipelines: self
                .pipelines
                .iter()
                .map(|pipeline| pipeline.config.clone())
                .collect(),
        };
        Ok(ResolvedDeliveryConfig {
            yaml: serde_yaml::to_string(&document)?,
            composition_fingerprint: self.composition_fingerprint.clone(),
        })
    }

    pub fn primary(&self) -> anyhow::Result<&PipelinePlan> {
        self.pipelines
            .first()
            .ok_or_else(|| anyhow::anyhow!("delivery plan contains no pipelines"))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedConfigDocument {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_identity: Option<String>,

    pub pipelines: Vec<Config>,
}

impl ResolvedConfigDocument {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("Failed to read resolved config file '{path}': {error}")
        })?;
        let document: Self = serde_yaml::from_str(&contents)
            .map_err(|error| anyhow::anyhow!("Failed to parse resolved YAML config: {error}"))?;
        anyhow::ensure!(
            !document.pipelines.is_empty(),
            "resolved config contains no pipelines"
        );
        Ok(document)
    }
}

impl ResolvedDeliveryConfig {
    #[must_use]
    pub fn yaml(&self) -> &str {
        &self.yaml
    }

    #[must_use]
    pub fn composition_fingerprint(&self) -> &str {
        &self.composition_fingerprint
    }
}

pub async fn build_delivery_plan_with(
    config: Config,
    cancellation: CancellationToken,
    composition: &dyn Composition,
) -> anyhow::Result<DeliveryPlan> {
    build_delivery_plan_internal(config, None, cancellation, composition, true).await
}

pub async fn build_delivery_plan_with_replay_identity(
    config: Config,
    replay_identity: impl Into<Arc<str>>,
    cancellation: CancellationToken,
    composition: &dyn Composition,
) -> anyhow::Result<DeliveryPlan> {
    let replay_identity = replay_identity.into();
    anyhow::ensure!(
        !replay_identity.is_empty(),
        "delivery replay identity must not be empty"
    );
    build_delivery_plan_internal(
        config,
        Some(replay_identity),
        cancellation,
        composition,
        true,
    )
    .await
}

pub async fn build_resolved_delivery_document_with(
    document: ResolvedConfigDocument,
    cancellation: CancellationToken,
    composition: &dyn Composition,
) -> anyhow::Result<DeliveryPlan> {
    let replay_identity = document.replay_identity.map(Arc::<str>::from);
    if let Some(identity) = &replay_identity {
        anyhow::ensure!(
            !identity.is_empty(),
            "delivery replay identity must not be empty"
        );
    }
    let mut pipelines = Vec::new();
    for config in document.pipelines {
        let mut plan = build_delivery_plan_internal(
            config,
            replay_identity.clone(),
            cancellation.child_token(),
            composition,
            false,
        )
        .await?;
        pipelines.append(&mut plan.pipelines);
    }
    anyhow::ensure!(
        !pipelines.is_empty(),
        "resolved config contains no pipelines"
    );
    Ok(DeliveryPlan {
        pipelines,
        composition_fingerprint: composition.fingerprint().to_owned(),
        replay_identity,
    })
}

async fn build_delivery_plan_internal(
    config: Config,
    replay_identity: Option<Arc<str>>,
    cancellation: CancellationToken,
    composition: &dyn Composition,
    resolve_installations: bool,
) -> anyhow::Result<DeliveryPlan> {
    anyhow::ensure!(
        config.pipeline_memory_limit_bytes > 0,
        "pipeline_memory_limit_bytes must be positive"
    );

    let source_kind = config.source.kind()?.to_owned();
    let sink_kind = config.sink.kind()?.to_owned();
    let source_raw = config.source.raw()?.clone();
    let sink_raw = config.sink.raw()?.clone();
    let (source_configs, sink_configs) = if resolve_installations {
        tokio::try_join!(
            composition.resolve_many(
                &source_kind,
                EndpointRole::Source,
                source_raw,
                cancellation.child_token(),
            ),
            composition.resolve_many(
                &sink_kind,
                EndpointRole::Sink,
                sink_raw,
                cancellation.child_token(),
            ),
        )?
    } else {
        (vec![source_raw], vec![sink_raw])
    };
    let pipeline_count = source_configs
        .len()
        .checked_mul(sink_configs.len())
        .ok_or_else(|| anyhow::anyhow!("resolved pipeline count overflow"))?;
    anyhow::ensure!(pipeline_count > 0, "delivery resolved no pipelines");
    let mut pipelines = Vec::with_capacity(pipeline_count);
    for source_config in source_configs {
        for sink_config in &sink_configs {
            let mut pipeline_config = config.clone();
            pipeline_config
                .source
                .replace_raw(source_kind.clone(), source_config.clone());
            pipeline_config
                .sink
                .replace_raw(sink_kind.clone(), sink_config.clone());
            pipelines.push(
                build_pipeline_plan(
                    pipeline_config,
                    replay_identity.clone(),
                    &source_kind,
                    &sink_kind,
                    cancellation.child_token(),
                    composition,
                    pipeline_count,
                    pipelines.len(),
                )
                .await?,
            );
        }
    }
    Ok(DeliveryPlan {
        pipelines,
        composition_fingerprint: composition.fingerprint().to_owned(),
        replay_identity,
    })
}

async fn build_pipeline_plan(
    config: Config,
    replay_identity: Option<Arc<str>>,
    source_kind: &str,
    sink_kind: &str,
    cancellation: CancellationToken,
    composition: &dyn Composition,
    pipeline_count: usize,
    pipeline_index: usize,
) -> anyhow::Result<PipelinePlan> {
    let durable_id = if pipeline_count == 1 {
        config.delivery_id.clone()
    } else {
        format!("{}.pipeline-{pipeline_index}", config.delivery_id)
    };
    let durable = config.durable_storage.build(&durable_id)?;
    let metrics_registry = Arc::new(MetricsRegistry::new());
    let catalog = composition.build_registry(&metrics_registry)?;
    let source_config = config.source.raw()?.clone();
    let sink_config = config.sink.raw()?.clone();
    let source_connector: Arc<dyn SourceConnector> =
        Arc::from(catalog.build_source(source_kind, source_config)?);
    let sink_connector: Arc<dyn SinkConnector> =
        Arc::from(catalog.build_sink(sink_kind, sink_config)?);
    source_connector.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;
    sink_connector.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;

    let source_descriptor = source_connector.compatibility(config.delivery_type);
    anyhow::ensure!(
        source_descriptor.supports_delivery_type(config.delivery_type),
        "source '{source_kind}' does not support delivery_type '{}'",
        config.delivery_type.label()
    );
    let finite_source =
        source_descriptor.source_behavior() == Some(SourceBehavior::FiniteAppendOnlyRows);
    validate_record_semantics(&source_descriptor, &sink_connector.compatibility())?;
    let discovery = source_connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation,
            delivery_type: config.delivery_type,
        })
        .await?;
    anyhow::ensure!(
        discovery.keep_system_columns,
        "source delivery discovery returned a system-column projection different from the requested policy"
    );

    let middlewares = config
        .middlewares
        .iter()
        .map(|middleware| catalog.build_middleware(middleware.kind()?, middleware.raw()?.clone()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut discovery = validate_middlewares(&middlewares, discovery).await?;
    discovery
        .performance_advice
        .extend(sink_connector.performance_advice());
    let semantics = validate_discovered_pipeline(
        &source_descriptor,
        &sink_connector.compatibility(),
        sink_connector.limits(),
        &discovery,
        true,
    )?;

    Ok(PipelinePlan {
        config,
        replay_identity,
        durable,
        metrics_registry,
        source_kind: source_kind.to_owned(),
        sink_kind: sink_kind.to_owned(),
        source_connector,
        sink_connector,
        discovery: Arc::new(discovery),
        middlewares,
        semantics,
        finite_source,
    })
}

pub fn validate_discovered_pipeline(
    source: &transferia_delivery_contracts::semantics::EndpointDescriptor,
    sink: &transferia_delivery_contracts::semantics::EndpointDescriptor,
    limits: &dyn transferia_core::delivery::SinkLimits,
    discovery: &DeliveryDiscovery,
    keep_system_columns: bool,
) -> anyhow::Result<DeliverySemanticsReport> {
    discovery
        .source_topology
        .validate()
        .context("source topology is invalid")?;
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

pub(crate) async fn validate_middlewares(
    middlewares: &[Box<dyn Middleware>],
    mut discovery: DeliveryDiscovery,
) -> anyhow::Result<DeliveryDiscovery> {
    if middlewares.is_empty() {
        return Ok(discovery);
    }
    let main = discovery
        .datasets
        .iter_mut()
        .find(|dataset| dataset.role == DatasetRole::Main)
        .context("middlewares require a discovered main dataset")?;
    for (index, middleware) in middlewares.iter().enumerate() {
        main.stored_schema = middleware
            .output_schema(&main.stored_schema)
            .await
            .with_context(|| format!("middleware {index} is incompatible with delivery schema"))?;
    }
    Ok(discovery)
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
