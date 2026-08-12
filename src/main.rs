extern crate alloc;

use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use transferia::compatibility::validate_pipeline;
use transferia::config::yaml::Config;
use transferia::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest, SinkLimits};
use transferia::metrics::{spawn_stats_reporter, MetricsRegistry, ParseCounters, SinkCounters};
use transferia::middleware::build_middleware;
use transferia::parsers::ParserFactory as DataParserFactory;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::middleware::Middleware;
use transferia::pipeline::retry::{jittered_retry_delay, stable_retry_seed};
use transferia::pipeline::{
    run_partition_pipeline_with_progress, PipelineFailure, PipelineProgress,
};
use transferia::providers::traits::{
    ProviderRegistry, SinkContext, SinkPrepare, SinkProvider, SourceProvider,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "PQv1 data transfer pipeline")]
struct Cli {
    #[arg(long, env = "CONFIG_PATH")]
    config: String,
    #[arg(long, default_value_t = 1)]
    total_workers: u32,
    #[arg(long, default_value_t = 0)]
    worker_index: u32,
}

fn validate_worker_assignment(cli: &Cli) -> anyhow::Result<()> {
    anyhow::ensure!(cli.total_workers > 0, "total_workers must be positive");
    anyhow::ensure!(
        cli.worker_index < cli.total_workers,
        "worker_index must be less than total_workers"
    );
    Ok(())
}

#[derive(Clone)]
struct PipelineDeps {
    parser: Arc<dyn DataParserFactory>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    source_provider: Arc<dyn SourceProvider>,
    sink_provider: Arc<dyn SinkProvider>,
    discovery: Arc<DeliveryDiscovery>,
    memory_limit: usize,
    cancellation: CancellationToken,
    keep_system_columns: bool,
}

async fn run_partition_attempt(
    partition_id: i64,
    deps: &PipelineDeps,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
    attempt_token: CancellationToken,
    progress: Arc<PipelineProgress>,
) -> anyhow::Result<()> {
    let memory = PipelineMemory::new(deps.memory_limit);
    let source = deps
        .source_provider
        .build_source(partition_id, attempt_token.clone(), memory.clone())
        .await
        .context("source creation failed")?;
    let sink = deps
        .sink_provider
        .build_sink(SinkContext {
            partition_id,
            counters: sink_counters,
            keep_system_columns: deps.keep_system_columns,
            discovery: Arc::clone(&deps.discovery),
        })
        .await
        .context("sink creation failed")?;
    run_partition_pipeline_with_progress(
        source,
        Arc::clone(&deps.parser),
        Arc::clone(&deps.middlewares),
        sink,
        memory,
        attempt_token,
        partition_id,
        parse_counters,
        progress,
    )
    .await
}

const INITIAL_PARTITION_RESTART_DELAY: core::time::Duration = core::time::Duration::from_secs(1);
const MAX_PARTITION_RESTART_DELAY: core::time::Duration = core::time::Duration::from_secs(30);

#[derive(Debug)]
struct PartitionRestartPolicy {
    consecutive_failures: u32,
    next_delay: core::time::Duration,
}

impl PartitionRestartPolicy {
    const fn new() -> Self {
        Self {
            consecutive_failures: 0,
            next_delay: INITIAL_PARTITION_RESTART_DELAY,
        }
    }

    fn record_failure(&mut self, made_durable_progress: bool) -> (u32, core::time::Duration) {
        if made_durable_progress {
            self.consecutive_failures = 0;
            self.next_delay = INITIAL_PARTITION_RESTART_DELAY;
        }
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let delay = self.next_delay;
        self.next_delay = self
            .next_delay
            .saturating_mul(2)
            .min(MAX_PARTITION_RESTART_DELAY);
        (self.consecutive_failures, delay)
    }
}

async fn run_partition_task(
    partition_id: i64,
    deps: PipelineDeps,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
) -> anyhow::Result<()> {
    let mut restart_policy = PartitionRestartPolicy::new();
    let retry_seed = stable_retry_seed(&partition_id.to_le_bytes());
    let progress = Arc::new(PipelineProgress::new());

    loop {
        if deps.cancellation.is_cancelled() {
            return Ok(());
        }
        let attempt_token = deps.cancellation.child_token();
        let progress_checkpoint = progress.checkpoint();
        let result = run_partition_attempt(
            partition_id,
            &deps,
            Arc::clone(&parse_counters),
            Arc::clone(&sink_counters),
            attempt_token.clone(),
            Arc::clone(&progress),
        )
        .await;
        // PQ background tasks are children of this token and must never
        // survive into the next attempt.
        attempt_token.cancel();

        let error = match result {
            Ok(()) if deps.cancellation.is_cancelled() => return Ok(()),
            Ok(()) => anyhow::anyhow!("partition pipeline stopped unexpectedly"),
            Err(error) => error,
        };
        let retryable = error
            .downcast_ref::<PipelineFailure>()
            .is_none_or(PipelineFailure::is_retryable);
        if !retryable {
            return Err(error).context("non-retryable partition failure");
        }
        let (consecutive_failure, base_restart_delay) =
            restart_policy.record_failure(progress.advanced_since(progress_checkpoint));
        let restart_delay = jittered_retry_delay(
            base_restart_delay,
            consecutive_failure.saturating_sub(1),
            retry_seed,
        );

        tracing::error!(
            partition = partition_id,
            consecutive_failure,
            delay_ms = restart_delay.as_millis(),
            "pipeline failed, restarting: {error}"
        );
        tokio::select! {
            () = deps.cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(restart_delay) => {}
        }
    }
}

async fn stop_partition_tasks(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    cancellation: &CancellationToken,
) {
    cancellation.cancel();
    let stopped = tokio::time::timeout(core::time::Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if stopped.is_err() {
        tracing::warn!("partition shutdown grace period expired; aborting remaining tasks");
        tasks.shutdown().await;
    }
}

fn build_provider_registry(metrics_registry: &Arc<MetricsRegistry>) -> ProviderRegistry {
    let mut registry = ProviderRegistry::new();
    registry.register_source("pqv1", {
        let registry = Arc::clone(metrics_registry);
        move |value| {
            Ok(Box::new(
                transferia::providers::pqv1::provider::PqV1SourceProvider::from_config(
                    value,
                    Arc::clone(&registry),
                )?,
            ))
        }
    });
    registry.register_sink("clickhouse", |value| {
        Ok(Box::new(
            transferia::providers::clickhouse::ClickHouseSinkProvider::from_config(value)?,
        ))
    });
    registry.register_sink("discard", |value| {
        Ok(Box::new(
            transferia::providers::discard::provider::DiscardSinkProvider::from_config(value)?,
        ))
    });
    registry.register_sink("s3", |value| {
        Ok(Box::new(
            transferia::providers::s3::sink::S3SinkProvider::from_config(value)?,
        ))
    });
    registry
}

fn spawn_shutdown_listener(cancellation: CancellationToken) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            cancellation.cancel();
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            cancellation.cancel();
        }
    });
    Ok(())
}

fn validate_discovered_pipeline(
    source: &transferia::compatibility::EndpointDescriptor,
    sink: &transferia::compatibility::EndpointDescriptor,
    limits: &dyn SinkLimits,
    discovery: &DeliveryDiscovery,
    keep_system_columns: bool,
) -> anyhow::Result<transferia::compatibility::DeliverySemanticsReport> {
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
        .dataset(transferia::delivery::DatasetRole::Main)
        .context("middlewares require a discovered main dataset")?;
    for (index, middleware) in middlewares.iter().enumerate() {
        middleware
            .validate_schema(&main.incoming_schema)
            .with_context(|| format!("middleware {index} is incompatible with delivery schema"))?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    validate_worker_assignment(&cli)?;
    let config = Config::from_file(&cli.config)?;
    anyhow::ensure!(
        config.pipeline_memory_limit_bytes > 0,
        "pipeline_memory_limit_bytes must be positive"
    );

    let metrics_registry = Arc::new(MetricsRegistry::new());
    let registry = build_provider_registry(&metrics_registry);

    let source_kind = config.source.kind()?;
    let sink_kind = config.sink.kind()?;
    let source_provider: Arc<dyn SourceProvider> =
        Arc::from(registry.build_source(source_kind, config.source.raw()?.clone())?);
    let sink_provider: Arc<dyn SinkProvider> =
        Arc::from(registry.build_sink(sink_kind, config.sink.raw()?.clone())?);
    sink_provider.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;

    let cancellation = CancellationToken::new();
    spawn_shutdown_listener(cancellation.clone())?;
    let discovery = source_provider
        .delivery_discovery(
            DeliveryDiscoveryRequest {
                keep_system_columns: config.keep_system_columns_in_sink,
            },
            cancellation.clone(),
        )
        .await?;
    anyhow::ensure!(
        discovery.keep_system_columns == config.keep_system_columns_in_sink,
        "source delivery discovery returned a system-column projection different from the requested policy"
    );
    let middlewares = config
        .middlewares
        .iter()
        .map(|middleware| build_middleware(middleware.kind()?, middleware.raw()?.clone()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    validate_middlewares(&middlewares, &discovery)?;
    let semantics = validate_discovered_pipeline(
        &source_provider.compatibility(),
        &sink_provider.compatibility(),
        sink_provider.limits(),
        &discovery,
        config.keep_system_columns_in_sink,
    )?;
    tracing::info!(report = %serde_json::to_string(&semantics)?, "delivery semantics inferred from configuration");
    tracing::info!(limits = %serde_json::to_string(&sink_provider.limits().description())?, "sink limits validated against delivery discovery");
    let discovery = Arc::new(discovery);

    let parser_plan = source_provider.parser_plan();
    let parses_rows = parser_plan.parses_rows();
    let parser = parser_plan.parser();

    let partitions = source_provider
        .partitions_for_worker(cli.total_workers, cli.worker_index)
        .await?;
    if partitions.is_empty() {
        tracing::warn!("No source partitions assigned");
        return Ok(());
    }

    if let Some(request) = SinkPrepare::from_discovery(&discovery)? {
        sink_provider.prepare(request).await?;
    }

    if let Some(metrics) = &config.metrics {
        if metrics.enabled {
            spawn_stats_reporter(
                Arc::clone(&metrics_registry),
                metrics.interval_ms,
                metrics.per_partition,
            );
        }
    }

    let deps = PipelineDeps {
        parser,
        middlewares: Arc::new(middlewares),
        source_provider,
        sink_provider,
        discovery,
        memory_limit: config.pipeline_memory_limit_bytes,
        cancellation: cancellation.clone(),
        keep_system_columns: config.keep_system_columns_in_sink,
    };
    let mut tasks = JoinSet::new();
    for partition_id in partitions {
        let parse_counters = Arc::new(ParseCounters::new());
        let sink_counters = Arc::new(SinkCounters::new());
        metrics_registry.register_parse(partition_id, parses_rows, Arc::clone(&parse_counters));
        metrics_registry.register_sink(partition_id, Arc::clone(&sink_counters));
        metrics_registry.set_delivery_guarantee(partition_id, semantics.guarantee);
        tasks.spawn(run_partition_task(
            partition_id,
            deps.clone(),
            parse_counters,
            sink_counters,
        ));
    }
    while !tasks.is_empty() {
        let result = tokio::select! {
            () = cancellation.cancelled() => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                return Ok(());
            }
            result = tasks.join_next() => result,
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok(Ok(())) if cancellation.is_cancelled() => {}
            Ok(Ok(())) => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                anyhow::bail!("partition task stopped while the service was still running");
            }
            Ok(Err(error)) => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                return Err(error).context("partition task failed");
            }
            Err(error) => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                return Err(anyhow::Error::new(error)).context("partition task panicked");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;
