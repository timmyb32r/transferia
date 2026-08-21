use std::sync::Arc;

use anyhow::Context as _;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{run_partition_pipeline_with_progress, PipelineProgress};
use crate::delivery::preparation::{DeliveryPlan, PipelinePlan};
use transferia_core::delivery::DeliveryDiscovery;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::PipelineMemory;
use transferia_delivery_contracts::metrics::{spawn_stats_reporter, ParseCounters, SinkCounters};
use transferia_delivery_contracts::middleware::Middleware;
use transferia_delivery_contracts::parser::ParserFactory;
use transferia_delivery_contracts::retry::{jittered_retry_delay, stable_retry_seed};
use transferia_registry::durable::DurableContext;
use transferia_registry::{
    SinkBuildContext, SinkPrepare, SinkConnector, SourceBuildContext, SourceConnector,
};

#[derive(Clone)]
struct PipelineDependencies {
    parser: Arc<dyn ParserFactory>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    source_connector: Arc<dyn SourceConnector>,
    sink_connector: Arc<dyn SinkConnector>,
    discovery: Arc<DeliveryDiscovery>,
    memory_limit: usize,
    cancellation: CancellationToken,
    keep_system_columns: bool,
    finite_source: bool,
    durable: DurableContext,
}

struct PipelineExecution {
    tasks: JoinSet<DataPlaneResult<()>>,
    cancellation: CancellationToken,
    finite_source: bool,
}

impl PipelineExecution {
    pub async fn wait(mut self) -> anyhow::Result<()> {
        while !self.tasks.is_empty() {
            let result = tokio::select! {
                () = self.cancellation.cancelled() => {
                    stop_partition_tasks(&mut self.tasks, &self.cancellation).await;
                    return Ok(());
                }
                result = self.tasks.join_next() => result,
            };
            let Some(result) = result else {
                break;
            };
            match result {
                Ok(Ok(())) if self.cancellation.is_cancelled() || self.finite_source => {}
                Ok(Ok(())) => {
                    self.shutdown().await;
                    anyhow::bail!("partition task stopped while the service was still running");
                }
                Ok(Err(error)) => {
                    self.shutdown().await;
                    return Err(anyhow::Error::new(error)).context("partition task failed");
                }
                Err(error) => {
                    self.shutdown().await;
                    return Err(anyhow::Error::new(error)).context("partition task panicked");
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        stop_partition_tasks(&mut self.tasks, &self.cancellation).await;
    }
}

pub struct DeliveryExecution {
    pipelines: JoinSet<anyhow::Result<()>>,

    cancellation: CancellationToken,
}

impl DeliveryExecution {
    pub async fn wait(mut self) -> anyhow::Result<()> {
        while let Some(result) = self.pipelines.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.shutdown().await;
                    return Err(error).context("pipeline failed");
                }
                Err(error) => {
                    self.shutdown().await;
                    return Err(anyhow::Error::new(error)).context("pipeline task panicked");
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        self.cancellation.cancel();
        while self.pipelines.join_next().await.is_some() {}
    }
}

pub async fn start_delivery(
    plan: DeliveryPlan,
    total_workers: u32,
    worker_index: u32,
    cancellation: CancellationToken,
) -> anyhow::Result<Option<DeliveryExecution>> {
    let mut pipelines = JoinSet::new();
    for pipeline in plan.pipelines {
        match start_pipeline(
            pipeline,
            total_workers,
            worker_index,
            cancellation.child_token(),
        )
        .await
        {
            Ok(Some(execution)) => {
                pipelines.spawn(execution.wait());
            }
            Ok(None) => {}
            Err(error) => {
                cancellation.cancel();
                while pipelines.join_next().await.is_some() {}
                return Err(error);
            }
        }
    }
    if pipelines.is_empty() {
        return Ok(None);
    }
    Ok(Some(DeliveryExecution {
        pipelines,
        cancellation,
    }))
}

async fn start_pipeline(
    plan: PipelinePlan,
    total_workers: u32,
    worker_index: u32,
    cancellation: CancellationToken,
) -> anyhow::Result<Option<PipelineExecution>> {
    let PipelinePlan {
        config,
        durable,
        metrics_registry,
        source_connector,
        sink_connector,
        discovery,
        middlewares,
        semantics,
        finite_source,
        ..
    } = plan;
    tracing::info!(report = %serde_json::to_string(&semantics)?, "delivery semantics inferred from configuration");
    tracing::info!(limits = %serde_json::to_string(&sink_connector.limits().description())?, "sink limits validated against delivery discovery");

    let parses_rows = source_connector.parses_rows();
    let parser = source_connector.parser();
    let partitions = discovery
        .source_topology
        .partitions_for_worker(total_workers, worker_index)?;
    if partitions.is_empty() {
        tracing::warn!("No source partitions assigned");
        return Ok(None);
    }

    if let Some(request) = SinkPrepare::from_discovery(&discovery)? {
        sink_connector.prepare(request).await?;
    }
    if let Some(metrics) = &config.metrics {
        spawn_stats_reporter(
            Arc::clone(&metrics_registry),
            metrics.interval_ms,
            metrics.per_partition,
        );
    }

    let dependencies = PipelineDependencies {
        parser,
        middlewares: Arc::new(middlewares),
        source_connector,
        sink_connector,
        discovery,
        memory_limit: config.pipeline_memory_limit_bytes,
        cancellation: cancellation.clone(),
        keep_system_columns: true,
        finite_source,
        durable,
    };
    let mut tasks = JoinSet::new();
    let mut startup_receivers = Vec::new();
    for partition_id in partitions {
        let parse_counters = Arc::new(ParseCounters::new());
        let sink_counters = Arc::new(SinkCounters::new());
        metrics_registry.register_parse(partition_id, parses_rows, Arc::clone(&parse_counters));
        metrics_registry.register_sink(partition_id, Arc::clone(&sink_counters));
        metrics_registry.set_delivery_guarantee(partition_id, semantics.guarantee);
        let (startup, startup_receiver) = oneshot::channel();
        startup_receivers.push((partition_id, startup_receiver));
        tasks.spawn(run_partition_task(
            partition_id,
            dependencies.clone(),
            parse_counters,
            sink_counters,
            startup,
        ));
    }
    if let Err(error) = wait_for_partition_startup(startup_receivers, &cancellation).await {
        stop_partition_tasks(&mut tasks, &cancellation).await;
        return Err(error);
    }
    Ok(Some(PipelineExecution {
        tasks,
        cancellation,
        finite_source,
    }))
}

async fn run_partition_attempt(
    partition_id: i64,
    dependencies: &PipelineDependencies,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
    attempt_token: CancellationToken,
    progress: Arc<PipelineProgress>,
    startup: &mut Option<oneshot::Sender<()>>,
) -> DataPlaneResult<()> {
    let memory = PipelineMemory::new(dependencies.memory_limit);
    let source = dependencies
        .source_connector
        .build_source(SourceBuildContext {
            partition_id,
            cancellation: attempt_token.clone(),
            memory: memory.clone(),
            durable: dependencies.durable.clone(),
        })
        .await
        .map_err(|error| DataPlaneFailure::retryable(error.context("source creation failed")))?;
    let sink = dependencies
        .sink_connector
        .build_sink(SinkBuildContext {
            partition_id,
            counters: sink_counters,
            keep_system_columns: dependencies.keep_system_columns,
            discovery: Arc::clone(&dependencies.discovery),
            durable: dependencies.durable.clone(),
        })
        .await
        .map_err(|error| DataPlaneFailure::retryable(error.context("sink creation failed")))?;
    if let Some(startup) = startup.take() {
        let _ignored = startup.send(());
    }
    run_partition_pipeline_with_progress(
        source,
        Arc::clone(&dependencies.parser),
        Arc::clone(&dependencies.middlewares),
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
    dependencies: PipelineDependencies,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
    startup: oneshot::Sender<()>,
) -> DataPlaneResult<()> {
    let mut restart_policy = PartitionRestartPolicy::new();
    let retry_seed = stable_retry_seed(&partition_id.to_le_bytes());
    let progress = Arc::new(PipelineProgress::new());
    let mut startup = Some(startup);

    loop {
        if dependencies.cancellation.is_cancelled() {
            return Ok(());
        }
        let attempt_token = dependencies.cancellation.child_token();
        let progress_checkpoint = progress.checkpoint();
        let result = run_partition_attempt(
            partition_id,
            &dependencies,
            Arc::clone(&parse_counters),
            Arc::clone(&sink_counters),
            attempt_token.clone(),
            Arc::clone(&progress),
            &mut startup,
        )
        .await;
        attempt_token.cancel();

        let Some(error) = classify_partition_completion(
            result,
            dependencies.cancellation.is_cancelled(),
            dependencies.finite_source,
        ) else {
            return Ok(());
        };
        if !error.is_retryable() {
            return Err(error.context("non-retryable partition failure"));
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
            () = dependencies.cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(restart_delay) => {}
        }
    }
}

async fn wait_for_partition_startup(
    receivers: Vec<(i64, oneshot::Receiver<()>)>,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    for (partition_id, receiver) in receivers {
        tokio::select! {
            () = cancellation.cancelled() => anyhow::bail!("worker startup was cancelled"),
            result = receiver => result.with_context(|| {
                format!("partition {partition_id} stopped before constructing its source and sink")
            })?,
        }
    }
    Ok(())
}

fn classify_partition_completion(
    result: DataPlaneResult<()>,
    cancelled: bool,
    finite_source: bool,
) -> Option<DataPlaneFailure> {
    match result {
        Ok(()) if cancelled || finite_source => None,
        Ok(()) => Some(DataPlaneFailure::retryable(anyhow::anyhow!(
            "partition pipeline stopped unexpectedly"
        ))),
        Err(error) => Some(error),
    }
}

async fn stop_partition_tasks(
    tasks: &mut JoinSet<DataPlaneResult<()>>,
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

#[cfg(test)]
#[path = "tests/runner.rs"]
mod tests;
