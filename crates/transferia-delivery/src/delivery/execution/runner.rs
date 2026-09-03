use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::PipelineProgress;
use crate::delivery::preparation::{DeliveryPlan, PipelinePlan};
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery};
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::PipelineMemory;
use transferia_delivery_contracts::metrics::{spawn_stats_reporter, ParseCounters, SinkCounters};
use transferia_delivery_contracts::middleware::Middleware;
use transferia_delivery_contracts::parser::ParserFactory;
use transferia_delivery_contracts::retry::{jittered_retry_delay, stable_retry_seed};
use transferia_registry::durable::{CompareExchangeResult, DurableContext};
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SnapshotDatasetRowCount,
    SnapshotRowCountStrategy, SourceBuildContext, SourceConnector,
};
use transferia_pipeline::{OutputDatasetRowCount, OutputRowCounts};

const SNAPSHOT_ROW_COUNT_BASELINE_KEY: &str = "snapshot-row-count/baseline";
const SNAPSHOT_ROW_COUNT_BASELINE_VERSION: u8 = 1;

struct SnapshotReconciliation {
    output_rows: Arc<OutputRowCounts>,
    sink: Arc<dyn SinkConnector>,
    discovery: Arc<DeliveryDiscovery>,
    strategy: Option<SnapshotRowCountStrategy>,
    baseline: Vec<SnapshotDatasetRowCount>,
    total_workers: u32,
    worker_index: u32,
}

impl SnapshotReconciliation {
    async fn prepare(
        output_rows: Arc<OutputRowCounts>,
        sink: Arc<dyn SinkConnector>,
        discovery: Arc<DeliveryDiscovery>,
        durable: DurableContext,
        total_workers: u32,
        worker_index: u32,
    ) -> anyhow::Result<Self> {
        let strategy = (total_workers == 1)
            .then(|| sink.snapshot_row_count_strategy())
            .flatten();
        let baseline = if strategy == Some(SnapshotRowCountStrategy::AdditiveBaseline) {
            load_or_capture_snapshot_baseline(sink.as_ref(), &discovery, &durable).await?
        } else {
            Vec::new()
        };
        Ok(Self {
            output_rows,
            sink,
            discovery,
            strategy,
            baseline,
            total_workers,
            worker_index,
        })
    }

    async fn verify(self) -> anyhow::Result<()> {
        let output = self.output_rows.snapshot()?;
        for dataset in &output {
            tracing::info!(
                worker_index = self.worker_index,
                total_workers = self.total_workers,
                dataset = %dataset.table,
                role = if dataset.is_dlq { "dead_letter_queue" } else { "main" },
                output_rows = dataset.rows,
                "finite snapshot output row count completed"
            );
        }

        let Some(strategy) = self.strategy else {
            tracing::info!(
                worker_index = self.worker_index,
                total_workers = self.total_workers,
                "exact destination snapshot row-count verification is unavailable"
            );
            return Ok(());
        };

        let final_counts = normalize_destination_counts(
            &self.discovery,
            self.sink.snapshot_row_counts(&self.discovery).await?,
        )?;
        for count in reconcile_snapshot_row_counts(strategy, output, self.baseline, final_counts)? {
            tracing::info!(
                dataset = %count.table,
                role = role_name(count.role),
                destination = %count.target,
                output_rows = count.output_rows,
                destination_rows = count.destination_rows,
                expectation = match strategy {
                    SnapshotRowCountStrategy::AdditiveBaseline => "baseline_plus_output",
                    SnapshotRowCountStrategy::ReplacedTotal => "replaced_total",
                },
                "finite snapshot destination row count verified"
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
struct VerifiedSnapshotRowCount {
    role: DatasetRole,
    table: Arc<str>,
    target: Arc<str>,
    output_rows: u64,
    destination_rows: u64,
}

fn reconcile_snapshot_row_counts(
    strategy: SnapshotRowCountStrategy,
    output: Vec<OutputDatasetRowCount>,
    baseline: Vec<SnapshotDatasetRowCount>,
    final_counts: Vec<SnapshotDatasetRowCount>,
) -> anyhow::Result<Vec<VerifiedSnapshotRowCount>> {
    let output = output_count_map(output)?;
    let baseline = destination_count_map(baseline)?;
    let mut verified = Vec::with_capacity(final_counts.len());
    for count in final_counts {
        anyhow::ensure!(
            count.exists,
            "snapshot row-count verification failed: destination '{}' for dataset '{}' does not exist after completion",
            count.target,
            count.table
        );
        let key = dataset_key(count.role, &count.table);
        let output_rows = output
            .get(&key)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot row-count verification has no output total for dataset '{}'",
                    count.table
                )
            })?
            .rows;
        let expected_rows = match strategy {
            SnapshotRowCountStrategy::AdditiveBaseline => {
                let baseline = baseline.get(&key).ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot row-count verification has no persisted baseline for dataset '{}'",
                        count.table
                    )
                })?;
                anyhow::ensure!(
                    baseline.target == count.target,
                    "persisted snapshot row-count baseline targets '{}', but the current destination is '{}' for dataset '{}'",
                    baseline.target,
                    count.target,
                    count.table
                );
                baseline.rows.checked_add(output_rows).ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot row-count expectation overflow for dataset '{}'",
                        count.table
                    )
                })?
            }
            SnapshotRowCountStrategy::ReplacedTotal => output_rows,
        };
        anyhow::ensure!(
            count.rows == expected_rows,
            "snapshot row-count mismatch for destination '{}' dataset '{}': expected {expected_rows} rows, found {}",
            count.target,
            count.table,
            count.rows
        );
        verified.push(VerifiedSnapshotRowCount {
            role: count.role,
            table: count.table,
            target: count.target,
            output_rows,
            destination_rows: count.rows,
        });
    }
    Ok(verified)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSnapshotBaseline {
    version: u8,
    datasets: Vec<PersistedDatasetRowCount>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedDatasetRowCount {
    table: String,
    is_dlq: bool,
    target: String,
    exists: bool,
    rows: u64,
}

async fn load_or_capture_snapshot_baseline(
    sink: &dyn SinkConnector,
    discovery: &DeliveryDiscovery,
    durable: &DurableContext,
) -> anyhow::Result<Vec<SnapshotDatasetRowCount>> {
    if let Some(value) = durable.storage.read(SNAPSHOT_ROW_COUNT_BASELINE_KEY).await? {
        let baseline = decode_snapshot_baseline(&value.payload, discovery)?;
        let current = normalize_destination_counts(
            discovery,
            sink.snapshot_row_counts(discovery)
                .await
                .context("failed to validate the persisted destination row-count baseline")?,
        )?;
        ensure_same_destination_targets(&baseline, &current)?;
        return Ok(baseline);
    }
    let captured = normalize_destination_counts(
        discovery,
        sink.snapshot_row_counts(discovery)
            .await
            .context("failed to capture the destination snapshot row-count baseline")?,
    )?;
    let payload = encode_snapshot_baseline(&captured)?;
    match durable
        .storage
        .compare_exchange(SNAPSHOT_ROW_COUNT_BASELINE_KEY, None, &payload)
        .await?
    {
        CompareExchangeResult::Applied(_) => Ok(captured),
        CompareExchangeResult::Conflict(Some(value)) => {
            let persisted = decode_snapshot_baseline(&value.payload, discovery)?;
            ensure_same_destination_targets(&persisted, &captured)?;
            Ok(persisted)
        }
        CompareExchangeResult::Conflict(None) => anyhow::bail!(
            "snapshot row-count baseline disappeared during compare-exchange"
        ),
    }
}

fn ensure_same_destination_targets(
    baseline: &[SnapshotDatasetRowCount],
    current: &[SnapshotDatasetRowCount],
) -> anyhow::Result<()> {
    let baseline = destination_count_map(baseline.to_vec())?;
    let current = destination_count_map(current.to_vec())?;
    anyhow::ensure!(
        baseline.keys().eq(current.keys()),
        "persisted snapshot row-count baseline has a different dataset set"
    );
    for (key, baseline) in baseline {
        let current = current.get(&key).ok_or_else(|| {
            anyhow::anyhow!(
                "persisted snapshot row-count baseline has no current target for dataset '{}'",
                baseline.table
            )
        })?;
        anyhow::ensure!(
            baseline.target == current.target,
            "persisted snapshot row-count baseline targets '{}', but the current destination is '{}' for dataset '{}'",
            baseline.target,
            current.target,
            baseline.table
        );
    }
    Ok(())
}

fn encode_snapshot_baseline(counts: &[SnapshotDatasetRowCount]) -> anyhow::Result<Vec<u8>> {
    let datasets = counts
        .iter()
        .map(|count| PersistedDatasetRowCount {
            table: count.table.to_string(),
            is_dlq: count.role == DatasetRole::DeadLetterQueue,
            target: count.target.to_string(),
            exists: count.exists,
            rows: count.rows,
        })
        .collect();
    Ok(serde_json::to_vec(&PersistedSnapshotBaseline {
        version: SNAPSHOT_ROW_COUNT_BASELINE_VERSION,
        datasets,
    })?)
}

fn decode_snapshot_baseline(
    payload: &[u8],
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<Vec<SnapshotDatasetRowCount>> {
    let baseline: PersistedSnapshotBaseline = serde_json::from_slice(payload)
        .context("snapshot row-count baseline is corrupt")?;
    anyhow::ensure!(
        baseline.version == SNAPSHOT_ROW_COUNT_BASELINE_VERSION,
        "snapshot row-count baseline version {} is unsupported",
        baseline.version
    );
    normalize_destination_counts(
        discovery,
        baseline
            .datasets
            .into_iter()
            .map(|count| SnapshotDatasetRowCount {
                role: DatasetRole::from_is_dlq(count.is_dlq),
                table: Arc::from(count.table),
                target: Arc::from(count.target),
                exists: count.exists,
                rows: count.rows,
            })
            .collect(),
    )
}

fn output_row_counts(discovery: &DeliveryDiscovery) -> anyhow::Result<OutputRowCounts> {
    OutputRowCounts::new(discovery.datasets.iter().map(|dataset| {
        (
            Arc::clone(&dataset.name),
            dataset.role == DatasetRole::DeadLetterQueue,
        )
    }))
}

fn normalize_destination_counts(
    discovery: &DeliveryDiscovery,
    counts: Vec<SnapshotDatasetRowCount>,
) -> anyhow::Result<Vec<SnapshotDatasetRowCount>> {
    let expected = discovery
        .datasets
        .iter()
        .map(|dataset| dataset_key(dataset.role, &dataset.name))
        .collect::<std::collections::BTreeSet<_>>();
    let mut actual = BTreeMap::new();
    for count in counts {
        anyhow::ensure!(
            count.exists || count.rows == 0,
            "absent destination '{}' reported {} rows",
            count.target,
            count.rows
        );
        let key = dataset_key(count.role, &count.table);
        anyhow::ensure!(
            actual.insert(key.clone(), count).is_none(),
            "destination row-count probe repeated dataset '{}'",
            key.1
        );
    }
    anyhow::ensure!(
        actual.keys().cloned().collect::<std::collections::BTreeSet<_>>() == expected,
        "destination row-count probe returned a different dataset set than discovery"
    );
    Ok(actual.into_values().collect())
}

fn output_count_map(
    counts: Vec<OutputDatasetRowCount>,
) -> anyhow::Result<BTreeMap<(bool, String), OutputDatasetRowCount>> {
    let mut mapped = BTreeMap::new();
    for count in counts {
        let key = (count.is_dlq, count.table.to_string());
        anyhow::ensure!(
            mapped.insert(key.clone(), count).is_none(),
            "snapshot output row counter repeated dataset '{}'",
            key.1
        );
    }
    Ok(mapped)
}

fn destination_count_map(
    counts: Vec<SnapshotDatasetRowCount>,
) -> anyhow::Result<BTreeMap<(bool, String), SnapshotDatasetRowCount>> {
    let mut mapped = BTreeMap::new();
    for count in counts {
        let key = dataset_key(count.role, &count.table);
        anyhow::ensure!(
            mapped.insert(key.clone(), count).is_none(),
            "snapshot destination row counter repeated dataset '{}'",
            key.1
        );
    }
    Ok(mapped)
}

fn dataset_key(role: DatasetRole, table: &str) -> (bool, String) {
    (role == DatasetRole::DeadLetterQueue, table.to_owned())
}

const fn role_name(role: DatasetRole) -> &'static str {
    match role {
        DatasetRole::Main => "main",
        DatasetRole::DeadLetterQueue => "dead_letter_queue",
    }
}

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
    completed_snapshot_rows: Option<Arc<OutputRowCounts>>,
}

struct PipelineExecution {
    tasks: JoinSet<DataPlaneResult<()>>,
    cancellation: CancellationToken,
    finite_source: bool,
    snapshot_reconciliation: Option<SnapshotReconciliation>,
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
        if let Some(reconciliation) = self.snapshot_reconciliation {
            reconciliation.verify().await?;
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

    let completed_snapshot_rows = finite_source
        .then(|| output_row_counts(&discovery))
        .transpose()?
        .map(Arc::new);
    let snapshot_reconciliation = if let Some(rows) = completed_snapshot_rows.as_ref() {
        Some(
            SnapshotReconciliation::prepare(
                Arc::clone(rows),
                Arc::clone(&sink_connector),
                Arc::clone(&discovery),
                durable.clone(),
                total_workers,
                worker_index,
            )
            .await?,
        )
    } else {
        None
    };

    if let Some(request) =
        SinkPrepare::from_discovery(&discovery, finite_source, config.delivery_id.clone())?
    {
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
        completed_snapshot_rows,
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
        snapshot_reconciliation,
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
    output_row_counts: Option<Arc<OutputRowCounts>>,
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
        .map_err(source_build_failure)?;
    let sink = dependencies
        .sink_connector
        .build_sink(SinkBuildContext {
            partition_id,
            finite_source: dependencies.finite_source,
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
    transferia_pipeline::run_partition_pipeline_with_progress_and_row_counts(
        source,
        Arc::clone(&dependencies.parser),
        Arc::clone(&dependencies.middlewares),
        sink,
        memory,
        attempt_token,
        partition_id,
        parse_counters,
        progress,
        output_row_counts,
    )
    .await
}

fn source_build_failure(error: anyhow::Error) -> DataPlaneFailure {
    DataPlaneFailure::retryable_or_passthrough(error).context("source creation failed")
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
        let attempt_row_counts = dependencies
            .completed_snapshot_rows
            .as_ref()
            .map(|_| output_row_counts(&dependencies.discovery))
            .transpose()
            .map_err(DataPlaneFailure::fatal)?
            .map(Arc::new);
        let result = run_partition_attempt(
            partition_id,
            &dependencies,
            Arc::clone(&parse_counters),
            Arc::clone(&sink_counters),
            attempt_token.clone(),
            Arc::clone(&progress),
            &mut startup,
            attempt_row_counts.clone(),
        )
        .await;
        attempt_token.cancel();

        if let (Some(completed), Some(attempt)) = (
            &dependencies.completed_snapshot_rows,
            attempt_row_counts.as_ref(),
        ) {
            completed.merge(attempt).map_err(DataPlaneFailure::fatal)?;
        }

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
            error = ?error,
            "pipeline failed, restarting"
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
