extern crate alloc;

use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use tokio::signal;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use transferia::application::delivery_plan::validate_discovered_pipeline;
use transferia::application::delivery_plan::{
    build_delivery_plan_with, build_resolved_delivery_plan_with, DeliveryPlan,
};
use transferia::application::worker_control::WorkerControl;
use transferia::config::yaml::Config;
use transferia::delivery::DeliveryDiscovery;
use transferia::extension::Transferia;
use transferia::metrics::{spawn_stats_reporter, ParseCounters, SinkCounters};
use transferia::parsers::ParserFactory as DataParserFactory;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::middleware::Middleware;
use transferia::pipeline::retry::{jittered_retry_delay, stable_retry_seed};
use transferia::pipeline::{
    run_partition_pipeline_with_progress, PipelineFailure, PipelineProgress,
};
use transferia::providers::traits::{SinkContext, SinkPrepare, SinkProvider, SourceProvider};
#[cfg(test)]
use transferia::{
    delivery::{DeliveryDiscoveryRequest, SinkLimits},
    metrics::MetricsRegistry,
    providers::catalog::build_provider_catalog,
};

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "PQv1 data transfer pipeline")]
struct Cli {
    #[arg(long, env = "CONFIG_PATH")]
    config: Option<String>,
    #[arg(long)]
    server: bool,
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: std::net::SocketAddr,
    #[arg(long, default_value = ".transferia-server")]
    state_dir: std::path::PathBuf,
    #[arg(long, default_value_t = 1)]
    total_workers: u32,
    #[arg(long, default_value_t = 0)]
    worker_index: u32,
    #[arg(long, hide = true)]
    parent_control: Option<std::net::SocketAddr>,
    #[arg(long, env = "TRANSFERIA_PARENT_TOKEN", hide = true)]
    parent_token: Option<String>,
    #[arg(long, hide = true)]
    resolved_config: bool,
    #[arg(long, hide = true)]
    composition_fingerprint: Option<String>,
}

fn validate_worker_assignment(cli: &Cli) -> anyhow::Result<()> {
    anyhow::ensure!(cli.total_workers > 0, "total_workers must be positive");
    anyhow::ensure!(
        cli.worker_index < cli.total_workers,
        "worker_index must be less than total_workers"
    );
    anyhow::ensure!(
        cli.parent_control.is_some() == cli.parent_token.is_some(),
        "--parent-control and TRANSFERIA_PARENT_TOKEN must be provided together"
    );
    anyhow::ensure!(
        cli.resolved_config == cli.composition_fingerprint.is_some(),
        "--resolved-config and --composition-fingerprint must be provided together"
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
    finite_source: bool,
    durable: transferia::durable::DurableContext,
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
        .build_source(
            partition_id,
            attempt_token.clone(),
            memory.clone(),
            deps.durable.clone(),
        )
        .await
        .context("source creation failed")?;
    let sink = deps
        .sink_provider
        .build_sink(SinkContext {
            partition_id,
            counters: sink_counters,
            keep_system_columns: deps.keep_system_columns,
            discovery: Arc::clone(&deps.discovery),
            durable: deps.durable.clone(),
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

        let Some(error) = classify_partition_completion(
            result,
            deps.cancellation.is_cancelled(),
            deps.finite_source,
        ) else {
            return Ok(());
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

fn classify_partition_completion(
    result: anyhow::Result<()>,
    cancelled: bool,
    finite_source: bool,
) -> Option<anyhow::Error> {
    match result {
        Ok(()) if cancelled || finite_source => None,
        Ok(()) => Some(anyhow::Error::msg(
            "partition pipeline stopped unexpectedly",
        )),
        Err(error) => Some(error),
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

pub async fn run(transferia: Transferia) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    if cli.server {
        return transferia::server::run_with(cli.bind, cli.state_dir, transferia).await;
    }
    validate_worker_assignment(&cli)?;
    let config_path = cli
        .config
        .as_deref()
        .context("--config is required unless --server is selected")?;
    let config = Config::from_file(config_path)?;
    let cancellation = CancellationToken::new();
    let parent_control = match (cli.parent_control, cli.parent_token.as_deref()) {
        (Some(address), Some(token)) => {
            Some(WorkerControl::connect(address, token, cancellation.clone()).await?)
        }
        (None, None) => None,
        _ => {
            anyhow::bail!("--parent-control and TRANSFERIA_PARENT_TOKEN must be provided together")
        }
    };
    spawn_shutdown_listener(cancellation.clone())?;
    if let Some(expected) = &cli.composition_fingerprint {
        anyhow::ensure!(
            expected == transferia.composition_fingerprint(),
            "worker composition does not match the composition that resolved its configuration"
        );
    }
    let plan = if cli.resolved_config {
        build_resolved_delivery_plan_with(config, cancellation.clone(), &transferia).await?
    } else {
        build_delivery_plan_with(config, cancellation.clone(), &transferia).await?
    };
    let DeliveryPlan {
        config,
        durable,
        metrics_registry,
        source_provider,
        sink_provider,
        discovery,
        middlewares,
        semantics,
        finite_source,
        ..
    } = plan;
    tracing::info!(report = %serde_json::to_string(&semantics)?, "delivery semantics inferred from configuration");
    tracing::info!(limits = %serde_json::to_string(&sink_provider.limits().description())?, "sink limits validated against delivery discovery");

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
        spawn_stats_reporter(
            Arc::clone(&metrics_registry),
            metrics.interval_ms,
            metrics.per_partition,
        );
    }

    let deps = PipelineDeps {
        parser,
        middlewares: Arc::new(middlewares),
        source_provider,
        sink_provider,
        discovery,
        memory_limit: config.pipeline_memory_limit_bytes,
        cancellation: cancellation.clone(),
        keep_system_columns: true,
        finite_source,
        durable,
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
    if let Some(parent_control) = &parent_control {
        parent_control.ready().await?;
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
            Ok(Ok(())) if finite_source => {}
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
#[path = "tests/main.rs"]
mod tests;
