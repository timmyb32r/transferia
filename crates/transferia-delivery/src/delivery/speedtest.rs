#![allow(
    clippy::cast_lossless,
    clippy::expect_used,
    clippy::naive_bytecount,
    clippy::significant_drop_tightening,
    clippy::unnecessary_wraps,
    reason = "speedtest replay values are validated before infallible generic Arrow conversions"
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Cursor, Seek as _, SeekFrom};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use arrow::util::display::array_value_to_string;
use futures_util::future::BoxFuture;
use futures_util::FutureExt as _;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::execution::run_partition_pipeline;
use super::preparation::{DeliveryPlan, PipelinePlan};
use transferia_core::compact_record_batch;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::DatasetRole;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::{MemoryReservation, PipelineMemory};
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_core::source::{CommitMarker, Source};
use transferia_delivery_contracts::metrics::{ParseCounters, SinkCounters};
use transferia_registry::durable::{
    CompareExchangeResult, DurableContext, DurableStorage, DurableValue,
};
use transferia_registry::{
    SinkBuildContext, SinkPrepare, SinkSpeedtestIsolation, SinkSpeedtestIsolationSafety,
    SourceBuildContext,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeedtestMeasurement {
    pub rows: u64,

    pub arrow_bytes: u64,

    pub elapsed: Duration,

    pub completed: bool,
}

impl SpeedtestMeasurement {
    #[must_use]
    pub fn rows_per_second(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64()
    }

    #[must_use]
    pub fn bytes_per_second(&self) -> f64 {
        self.arrow_bytes as f64 / self.elapsed.as_secs_f64()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeedtestColumnProfile {
    pub name: String,

    pub arrow_type: String,

    pub null_count: usize,

    pub distinct_count: Option<u64>,

    pub min_value: Option<String>,

    pub max_value: Option<String>,

    pub range_kind: Option<SpeedtestRangeKind>,

    pub min_length: Option<usize>,

    pub max_length: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpeedtestRangeKind {
    Numeric,
    Temporal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeedtestDatasetProfile {
    pub name: String,

    pub is_dlq: bool,

    pub rows: usize,

    pub arrow_bytes: usize,

    pub columns: Vec<SpeedtestColumnProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeedtestProfile {
    pub sampled_deliveries: usize,

    pub sample_limit_bytes: usize,

    pub truncated: bool,

    pub datasets: Vec<SpeedtestDatasetProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeedtestEstimate {
    pub logical_streams: u32,

    pub source: SpeedtestMeasurement,

    pub destination: SpeedtestMeasurement,

    pub profile: SpeedtestProfile,
}

#[derive(Clone)]
pub struct SpeedtestSample {
    deliveries: Arc<[SpooledDelivery]>,
}

pub struct SourceSpeedtestResult {
    pub measurement: SpeedtestMeasurement,

    pub profile: SpeedtestProfile,

    pub sample: SpeedtestSample,
}

#[derive(Debug, thiserror::Error)]
#[error("speedtest was cancelled")]
pub struct SpeedtestCancelled;

#[derive(Debug, thiserror::Error)]
#[error(
    "speedtest scratch cleanup failed after {attempts} attempt(s); manual cleanup required for: {scratch_targets}"
)]
pub struct SpeedtestCleanupFailure {
    attempts: u64,

    scratch_targets: String,
}

#[derive(Debug, thiserror::Error)]
#[error("speedtest source cleanup failed; manual source-session cleanup may be required")]
pub struct SpeedtestSourceCleanupFailure;

/// Measure a validated delivery without committing source progress or writing
/// generated rows into configured production entities.
pub async fn estimate_delivery(
    mut plan: DeliveryPlan,
    cancellation: CancellationToken,
    duration: Duration,
    cleanup_timeout: Duration,
    cleanup_tasks: TaskTracker,
) -> anyhow::Result<SpeedtestEstimate> {
    validate_speedtest_window(duration)?;
    validate_cleanup_timeout(cleanup_timeout)?;
    let (isolation_id, isolation) =
        destination_speedtest_isolation(&plan, cancellation.child_token()).await?;
    let source = benchmark_source(&mut plan, cancellation.child_token(), duration).await?;
    let (pipeline, partition_id) = single_pipeline(&plan)?;
    let destination = measure_destination(
        pipeline,
        source.sample.deliveries.as_ref(),
        ephemeral_durable(&pipeline.config.delivery_id, "destination"),
        partition_id,
        cancellation.child_token(),
        duration,
        cleanup_timeout,
        cleanup_tasks,
        isolation_id,
        isolation,
    )
    .await?;
    anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
    Ok(SpeedtestEstimate {
        logical_streams: 1,
        source: source.measurement,
        destination,
        profile: source.profile,
    })
}

/// Fail before reading the source when the configured destination cannot prove
/// an isolated, cleanable speedtest target. Connector isolation is required to
/// be free of destination mutations.
///
/// Actual scratch creation remains in
/// [`SinkConnector::prepare`](transferia_registry::SinkConnector::prepare).
pub async fn validate_destination_speedtest(
    plan: &DeliveryPlan,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    destination_speedtest_isolation(plan, cancellation)
        .await
        .map(|_| ())
}

async fn destination_speedtest_isolation(
    plan: &DeliveryPlan,
    cancellation: CancellationToken,
) -> anyhow::Result<(String, SinkSpeedtestIsolation)> {
    anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
    let (pipeline, _) = single_pipeline(plan)?;
    let isolation_id = unique_isolation_id()?;
    let isolation = Arc::clone(&pipeline.sink_connector)
        .isolate_speedtest(Arc::clone(&pipeline.discovery), isolation_id.clone())
        .await
        .context("destination speedtest isolation preflight failed")?;
    anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
    Ok((isolation_id, isolation))
}

pub async fn benchmark_source(
    plan: &mut DeliveryPlan,
    cancellation: CancellationToken,
    estimate_window: Duration,
) -> anyhow::Result<SourceSpeedtestResult> {
    validate_speedtest_window(estimate_window)?;
    let mut profiled =
        collect_source_profile(plan, cancellation.child_token(), estimate_window).await?;
    profiled.measurement = benchmark_source_throughput(plan, cancellation, estimate_window).await?;
    Ok(profiled)
}

async fn collect_source_profile(
    plan: &mut DeliveryPlan,
    cancellation: CancellationToken,
    estimate_window: Duration,
) -> anyhow::Result<SourceSpeedtestResult> {
    validate_speedtest_window(estimate_window)?;
    let (pipeline, partition_id) = single_pipeline_mut(plan)?;
    let source_durable = ephemeral_durable(&pipeline.config.delivery_id, "source");

    let source_memory = PipelineMemory::new(pipeline.config.pipeline_memory_limit_bytes);
    let run_token = cancellation.child_token();
    anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
    let source = pipeline
        .source_connector
        .build_speedtest_source(SourceBuildContext {
            partition_id,
            delivery_type: pipeline.config.delivery_type,
            phase: transferia_registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: run_token.clone(),
            memory: source_memory.clone(),
            durable: source_durable,
        })
        .await
        .context("speedtest source creation failed")?;
    anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
    let collector = Arc::new(ProfileCollector::new(
        pipeline.config.pipeline_memory_limit_bytes,
    ));
    // Endpoint speedtests intentionally exclude delivery middlewares: the
    // source ceiling is source + parser into discard, not a transform chain.
    let middlewares = Arc::new(Vec::new());
    let source_started = Instant::now();
    let (source, shutdown_failed) = NoCommitSource::new(source);
    let source_run = run_partition_pipeline(
        Box::new(source),
        pipeline.source_connector.parser(),
        middlewares,
        Box::new(ProfileSink::new(Arc::clone(&collector))),
        source_memory,
        run_token.clone(),
        partition_id,
        Arc::new(ParseCounters::new()),
    );
    tokio::pin!(source_run);
    let (source_completed, externally_cancelled) = tokio::select! {
        () = cancellation.cancelled() => {
            run_token.cancel();
            source_run.await.map_err(anyhow::Error::new)?;
            (false, true)
        }
        result = &mut source_run => {
            result.map_err(anyhow::Error::new)?;
            (true, false)
        }
        () = tokio::time::sleep(estimate_window) => {
            run_token.cancel();
            source_run.await.map_err(anyhow::Error::new)?;
            (false, false)
        }
    };
    ensure_source_cleanup_succeeded(&shutdown_failed)?;
    if externally_cancelled {
        return Err(SpeedtestCancelled.into());
    }
    let collected = collector.snapshot()?;
    // Sampling runs in the same backpressured pipeline as the source probe.
    // Wall-clock elapsed is therefore the only honest denominator; subtracting
    // sampling task time would double-count overlap and inflate throughput.
    let source_elapsed = nonzero_elapsed(source_started.elapsed());
    anyhow::ensure!(collected.rows > 0, "source speedtest produced no rows");
    let samples = collected.samples;
    anyhow::ensure!(
        !samples.is_empty(),
        "source speedtest produced no sample batch"
    );
    ensure_all_main_datasets_sampled(&pipeline.discovery, &samples)?;
    let profiles = aggregate_sample_profiles(&samples)?;
    let profile = SpeedtestProfile {
        sampled_deliveries: samples.len(),
        sample_limit_bytes: collected.sample_limit_bytes,
        truncated: collected.truncated,
        datasets: profiles,
    };
    let source_measurement = SpeedtestMeasurement {
        rows: collected.rows,
        arrow_bytes: collected.arrow_bytes,
        elapsed: source_elapsed,
        completed: source_completed,
    };
    Ok(SourceSpeedtestResult {
        measurement: source_measurement,
        profile,
        sample: SpeedtestSample {
            deliveries: Arc::from(samples),
        },
    })
}

/// Measures the source/parser ceiling through a true no-op discard sink. This
/// deliberately performs no profile IPC or column-statistics work.
///
/// It is used
/// directly for source tuning trials after the baseline sample was captured.
pub async fn benchmark_source_throughput(
    plan: &mut DeliveryPlan,
    cancellation: CancellationToken,
    estimate_window: Duration,
) -> anyhow::Result<SpeedtestMeasurement> {
    validate_speedtest_window(estimate_window)?;
    let (pipeline, partition_id) = single_pipeline_mut(plan)?;
    let memory = PipelineMemory::new(pipeline.config.pipeline_memory_limit_bytes);
    let run_token = cancellation.child_token();
    let source = pipeline
        .source_connector
        .build_speedtest_source(SourceBuildContext {
            partition_id,
            delivery_type: pipeline.config.delivery_type,
            phase: transferia_registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: run_token.clone(),
            memory: memory.clone(),
            durable: ephemeral_durable(&pipeline.config.delivery_id, "source-throughput"),
        })
        .await
        .context("speedtest source creation failed")?;
    let counters = Arc::new(Mutex::new((0_u64, 0_u64)));
    let started = Instant::now();
    let (source, shutdown_failed) = NoCommitSource::new(source);
    let source_run = run_partition_pipeline(
        Box::new(source),
        pipeline.source_connector.parser(),
        Arc::new(Vec::new()),
        Box::new(MeasurementDiscardSink {
            counters: Arc::clone(&counters),
        }),
        memory,
        run_token.clone(),
        partition_id,
        Arc::new(ParseCounters::new()),
    );
    tokio::pin!(source_run);
    let (completed, externally_cancelled) = tokio::select! {
        () = cancellation.cancelled() => {
            run_token.cancel();
            source_run.await.map_err(anyhow::Error::new)?;
            (false, true)
        }
        result = &mut source_run => {
            result.map_err(anyhow::Error::new)?;
            (true, false)
        }
        () = tokio::time::sleep(estimate_window) => {
            run_token.cancel();
            source_run.await.map_err(anyhow::Error::new)?;
            (false, false)
        }
    };
    ensure_source_cleanup_succeeded(&shutdown_failed)?;
    if externally_cancelled {
        return Err(SpeedtestCancelled.into());
    }
    let elapsed = nonzero_elapsed(started.elapsed());
    let (rows, arrow_bytes) = *counters
        .lock()
        .map_err(|_| anyhow::anyhow!("speedtest measurement mutex was poisoned"))?;
    anyhow::ensure!(rows > 0, "source speedtest produced no rows");
    Ok(SpeedtestMeasurement {
        rows,
        arrow_bytes,
        elapsed,
        completed,
    })
}

pub async fn benchmark_destination(
    plan: &DeliveryPlan,
    sample: &SpeedtestSample,
    cancellation: CancellationToken,
    estimate_window: Duration,
    cleanup_timeout: Duration,
    cleanup_tasks: TaskTracker,
) -> anyhow::Result<SpeedtestMeasurement> {
    validate_speedtest_window(estimate_window)?;
    validate_cleanup_timeout(cleanup_timeout)?;
    let (pipeline, partition_id) = single_pipeline(plan)?;
    let (isolation_id, isolation) =
        destination_speedtest_isolation(plan, cancellation.child_token()).await?;
    measure_destination(
        pipeline,
        sample.deliveries.as_ref(),
        ephemeral_durable(&pipeline.config.delivery_id, "destination"),
        partition_id,
        cancellation,
        estimate_window,
        cleanup_timeout,
        cleanup_tasks,
        isolation_id,
        isolation,
    )
    .await
}

fn single_pipeline(plan: &DeliveryPlan) -> anyhow::Result<(&PipelinePlan, i64)> {
    anyhow::ensure!(
        plan.pipelines.len() == 1,
        "speedtest requires exactly one resolved pipeline, got {}",
        plan.pipelines.len()
    );
    let pipeline = plan
        .pipelines
        .first()
        .context("speedtest delivery plan is empty")?;
    let partition_id = single_partition(pipeline)?;
    Ok((pipeline, partition_id))
}

fn single_pipeline_mut(plan: &mut DeliveryPlan) -> anyhow::Result<(&mut PipelinePlan, i64)> {
    anyhow::ensure!(
        plan.pipelines.len() == 1,
        "speedtest requires exactly one resolved pipeline, got {}",
        plan.pipelines.len()
    );
    let pipeline = plan
        .pipelines
        .first_mut()
        .context("speedtest delivery plan is empty")?;
    let partition_id = single_partition(pipeline)?;
    Ok((pipeline, partition_id))
}

fn single_partition(pipeline: &PipelinePlan) -> anyhow::Result<i64> {
    let partitions = pipeline
        .discovery
        .source_topology
        .partitions_for_worker(1, 0)?;
    anyhow::ensure!(
        partitions.len() == 1,
        "speedtest requires one logical source stream, got {} partitions",
        partitions.len()
    );
    Ok(partitions[0])
}

async fn measure_destination(
    pipeline: &PipelinePlan,
    samples: &[SpooledDelivery],
    durable: DurableContext,
    partition_id: i64,
    cancellation: CancellationToken,
    estimate_window: Duration,
    cleanup_timeout: Duration,
    cleanup_tasks: TaskTracker,
    isolation_id: String,
    isolation: SinkSpeedtestIsolation,
) -> anyhow::Result<SpeedtestMeasurement> {
    anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
    let cleanup = CleanupGuard::new(isolation.clone(), cleanup_timeout, cleanup_tasks);
    let result = async {
        anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
        let memory = PipelineMemory::new(pipeline.config.pipeline_memory_limit_bytes);
        let source = ProfileGeneratorSource::new(
            samples,
            &pipeline.discovery,
            &isolation,
            &isolation_id,
            memory.clone(),
            estimate_window,
        )
        .await?;
        anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
        let prepare = SinkPrepare::from_discovery(
            &isolation.discovery,
            pipeline.finite_source,
            format!("speedtest-{isolation_id}"),
            None,
        )?;
        if let Some(request) = prepare {
            isolation.connector().prepare(request).await?;
        }
        anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
        let counters = Arc::new(SinkCounters::new());
        let sink = isolation
            .connector()
            .build_sink(SinkBuildContext {
                partition_id,
                delivery_name: Arc::from(pipeline.config.delivery_name.clone()),
                replay_identity: None,
                finite_source: pipeline.finite_source,
                counters: Arc::clone(&counters),
                keep_system_columns: isolation.discovery.keep_system_columns,
                discovery: Arc::clone(&isolation.discovery),
                durable,
            })
            .await
            .context("speedtest destination creation failed")?;
        anyhow::ensure!(!cancellation.is_cancelled(), SpeedtestCancelled);
        let started = Instant::now();
        run_partition_pipeline(
            Box::new(source),
            pipeline.source_connector.parser(),
            Arc::new(Vec::new()),
            sink,
            memory,
            cancellation.clone(),
            partition_id,
            Arc::new(ParseCounters::new()),
        )
        .await
        .map_err(anyhow::Error::new)?;
        let elapsed = nonzero_elapsed(started.elapsed());
        anyhow::ensure!(
            counters.rows_total() > 0,
            "destination speedtest committed no rows"
        );
        Ok(SpeedtestMeasurement {
            rows: counters.rows_total(),
            arrow_bytes: counters.bytes_total(),
            elapsed,
            completed: false,
        })
    };
    run_with_cleanup(cleanup, result, cancellation.clone()).await
}

async fn run_with_cleanup<F, T>(
    mut cleanup: CleanupGuard,
    future: F,
    cancellation: CancellationToken,
) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    let result = std::panic::AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("destination speedtest task panicked")));
    let cleanup_result = cleanup.cleanup().await;
    match (result, cleanup_result, cancellation.is_cancelled()) {
        (Ok(_), Ok(()), true) => Err(SpeedtestCancelled.into()),
        (Ok(measurement), Ok(()), false) => Ok(measurement),
        (Err(error), Ok(()), _) => Err(error),
        (Ok(_), Err(cleanup_error), _) => Err(cleanup_error.context("speedtest cleanup failed")),
        (Err(error), Err(cleanup_error), _) => {
            Err(error.context(format!("speedtest cleanup also failed: {cleanup_error:#}")))
        }
    }
}

fn validate_speedtest_window(duration: Duration) -> anyhow::Result<()> {
    anyhow::ensure!(
        !duration.is_zero(),
        "speedtest duration must be greater than zero"
    );
    anyhow::ensure!(
        tokio::time::Instant::now().checked_add(duration).is_some(),
        "speedtest duration is too large"
    );
    Ok(())
}

fn validate_cleanup_timeout(duration: Duration) -> anyhow::Result<()> {
    anyhow::ensure!(
        !duration.is_zero(),
        "speedtest cleanup timeout must be greater than zero"
    );
    anyhow::ensure!(
        tokio::time::Instant::now().checked_add(duration).is_some(),
        "speedtest cleanup timeout is too large"
    );
    Ok(())
}

fn nonzero_elapsed(elapsed: Duration) -> Duration {
    elapsed.max(Duration::from_nanos(1))
}

fn unique_isolation_id() -> anyhow::Result<String> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)?;
    let mut result = String::with_capacity(32);
    for byte in random {
        write!(&mut result, "{byte:02x}")?;
    }
    Ok(result)
}

struct CleanupGuard {
    connector: Option<Arc<dyn transferia_registry::SinkConnector>>,

    isolation: SinkSpeedtestIsolation,

    timeout: Duration,

    tasks: TaskTracker,
}

impl CleanupGuard {
    fn new(isolation: SinkSpeedtestIsolation, timeout: Duration, tasks: TaskTracker) -> Self {
        let connector = (isolation.safety() == SinkSpeedtestIsolationSafety::Scratch)
            .then(|| Arc::clone(isolation.connector()));
        Self {
            connector,
            isolation,
            timeout,
            tasks,
        }
    }

    async fn cleanup(&mut self) -> anyhow::Result<()> {
        let Some(connector) = self.connector.take() else {
            return Ok(());
        };
        let isolation = self.isolation.clone();
        let timeout = self.timeout;
        self.tasks
            .spawn(async move { cleanup_until_deadline(connector, isolation, timeout).await })
            .await
            .context("speedtest cleanup task failed")?
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let Some(connector) = self.connector.take() else {
            return;
        };
        let isolation = self.isolation.clone();
        let timeout = self.timeout;
        self.tasks.spawn(async move {
            if cleanup_until_deadline(connector, isolation, timeout)
                .await
                .is_err()
            {
                // cleanup_until_deadline already emitted the credential-safe,
                // target-bearing diagnostic required for manual recovery.
            }
        });
    }
}

async fn cleanup_until_deadline(
    connector: Arc<dyn transferia_registry::SinkConnector>,
    isolation: SinkSpeedtestIsolation,
    timeout: Duration,
) -> anyhow::Result<()> {
    validate_cleanup_timeout(timeout)?;
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .context("speedtest cleanup deadline overflow")?;
    let scratch_targets = isolation
        .physical_targets()
        .iter()
        .map(|target| target.scratch.as_ref())
        .collect::<Vec<_>>()
        .join(", ");
    let mut attempts = 0_u64;

    loop {
        attempts = attempts.saturating_add(1);
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt =
            std::panic::AssertUnwindSafe(connector.cleanup_speedtest(&isolation)).catch_unwind();
        if matches!(
            tokio::time::timeout(remaining, attempt).await,
            Ok(Ok(Ok(())))
        ) {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        tracing::warn!(
            attempts,
            scratch_targets = %scratch_targets,
            "mandatory speedtest scratch cleanup failed; retrying within the configured cleanup timeout"
        );
        tokio::time::sleep(remaining.min(cleanup_retry_delay(attempts))).await;
    }

    tracing::error!(
        attempts,
        scratch_targets = %scratch_targets,
        "mandatory speedtest scratch cleanup exhausted the configured timeout; manual cleanup required"
    );
    Err(SpeedtestCleanupFailure {
        attempts,
        scratch_targets,
    }
    .into())
}

fn cleanup_retry_delay(attempt: u64) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1).min(3)).unwrap_or(3);
    Duration::from_millis(250_u64.saturating_mul(2_u64.saturating_pow(exponent)))
}

struct NoCommitSource {
    inner: Box<dyn Source>,

    shutdown_failed: Arc<AtomicBool>,
}

impl NoCommitSource {
    fn new(inner: Box<dyn Source>) -> (Self, Arc<AtomicBool>) {
        let shutdown_failed = Arc::new(AtomicBool::new(false));
        (
            Self {
                inner,
                shutdown_failed: Arc::clone(&shutdown_failed),
            },
            shutdown_failed,
        )
    }
}

impl Source for NoCommitSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        self.inner.read_batch()
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, DataPlaneResult<()>> {
        let shutdown_failed = Arc::clone(&self.shutdown_failed);
        Box::pin(async move {
            let result = self.inner.shutdown().await;
            if result.is_err() {
                shutdown_failed.store(true, Ordering::Release);
            }
            result
        })
    }
}

fn ensure_source_cleanup_succeeded(shutdown_failed: &AtomicBool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !shutdown_failed.load(Ordering::Acquire),
        SpeedtestSourceCleanupFailure
    );
    Ok(())
}

struct AnonymousSampleFile {
    // File::try_clone shares the Unix open-file-description offset. A mutex
    // therefore serializes seek+decode so concurrent tuning trials cannot
    // corrupt one another's IPC reads.
    file: Mutex<File>,
}

#[derive(Clone)]
struct SpooledOutput {
    table: Arc<str>,

    is_dlq: bool,

    system_columns: transferia_core::data::system_columns::SystemColumns,

    schema: SchemaRef,

    arrow_bytes: usize,

    file: Arc<AnonymousSampleFile>,

    profile: ProfiledDataset,
}

#[derive(Clone)]
struct SpooledDelivery {
    outputs: Arc<[SpooledOutput]>,
}

#[derive(Clone)]
struct LoadedOutput {
    table: Arc<str>,

    is_dlq: bool,

    batch: RecordBatch,

    system_columns: transferia_core::data::system_columns::SystemColumns,

    memory: MemoryReservation,

    key_row_offset: u128,
}

struct LoadedDelivery {
    outputs: Vec<LoadedOutput>,
}

fn spool_record_batch(batch: &RecordBatch) -> anyhow::Result<AnonymousSampleFile> {
    let path = anonymous_sample_path()?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| {
            format!(
                "create anonymous speedtest sample file '{}'",
                path.display()
            )
        })?;
    if let Err(error) = std::fs::remove_file(&path) {
        drop(std::fs::remove_file(&path));
        return Err(error).with_context(|| {
            format!(
                "unlink anonymous speedtest sample file '{}' before writing source data",
                path.display()
            )
        });
    }
    {
        let mut writer = FileWriter::try_new(&mut file, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(AnonymousSampleFile {
        file: Mutex::new(file),
    })
}

fn anonymous_sample_path() -> anyhow::Result<PathBuf> {
    Ok(std::env::temp_dir().join(format!(
        "transferia-speedtest-{}.arrow",
        unique_isolation_id()?
    )))
}

fn read_spooled_batch(file: &AnonymousSampleFile) -> anyhow::Result<RecordBatch> {
    let batch = {
        let mut handle = file
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("speedtest sample file mutex was poisoned"))?;
        handle.seek(SeekFrom::Start(0))?;
        let mut reader = FileReader::try_new(&mut *handle, None)?;
        let batch = reader
            .next()
            .transpose()?
            .context("speedtest sample file contains no RecordBatch")?;
        anyhow::ensure!(
            reader.next().transpose()?.is_none(),
            "speedtest sample file contains more than one RecordBatch"
        );
        batch
    };
    Ok(batch)
}

struct ProfileCollector {
    state: Mutex<CollectedProfile>,
}

#[derive(Clone)]
struct CollectedProfile {
    rows: u64,

    arrow_bytes: u64,

    sample_limit_bytes: usize,

    sampled_arrow_bytes: usize,

    truncated: bool,

    samples: Vec<SpooledDelivery>,
}

impl ProfileCollector {
    const fn new(sample_limit_bytes: usize) -> Self {
        Self {
            state: Mutex::new(CollectedProfile {
                rows: 0,
                arrow_bytes: 0,
                sample_limit_bytes,
                sampled_arrow_bytes: 0,
                truncated: false,
                samples: Vec::new(),
            }),
        }
    }

    async fn add(&self, delivery: &Delivery) -> anyhow::Result<()> {
        let outputs = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for output in &delivery.outputs {
                state.rows = state
                    .rows
                    .checked_add(output.rows() as u64)
                    .context("speedtest source row count overflow")?;
                state.arrow_bytes = state
                    .arrow_bytes
                    .checked_add(output.bytes() as u64)
                    .context("speedtest source byte count overflow")?;
            }
            let outputs = delivery
                .outputs
                .iter()
                .filter(|output| output.rows() > 0)
                .map(|output| {
                    (
                        Arc::clone(&output.table),
                        output.is_dlq,
                        output.batch.clone(),
                        output.system_columns.clone(),
                        output.memory.clone(),
                    )
                })
                .collect::<Vec<_>>();
            let delivery_bytes = outputs.iter().try_fold(0_usize, |total, output| {
                total
                    .checked_add(output.2.get_array_memory_size())
                    .context("speedtest sampled delivery byte count overflow")
            })?;
            if outputs.is_empty() {
                return Ok(());
            }
            let Some(next_sampled_bytes) = state.sampled_arrow_bytes.checked_add(delivery_bytes)
            else {
                anyhow::bail!("speedtest sampled byte count overflow");
            };
            if next_sampled_bytes > state.sample_limit_bytes && state.samples.is_empty() {
                tracing::warn!(
                    required_bytes = next_sampled_bytes,
                    limit_bytes = state.sample_limit_bytes,
                    "progress-critical first speedtest sample exceeds the configured pipeline memory limit"
                );
                state.truncated = true;
            } else if next_sampled_bytes > state.sample_limit_bytes {
                state.truncated = true;
                return Ok(());
            }
            state.sampled_arrow_bytes = next_sampled_bytes;
            outputs
        };
        let samples = tokio::task::spawn_blocking(move || {
            outputs
                .into_iter()
                .map(|(table, is_dlq, batch, system_columns, memory)| {
                    let arrow_bytes = batch.get_array_memory_size();
                    let result = spool_record_batch(&batch).and_then(|file| {
                        Ok(SpooledOutput {
                            profile: profile_batch_state(&table, is_dlq, &batch)?,
                            table,
                            is_dlq,
                            system_columns,
                            schema: batch.schema(),
                            arrow_bytes,
                            file: Arc::new(file),
                        })
                    });
                    drop(memory);
                    result
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .await
        .context("speedtest sample spool task failed")??;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("speedtest profile mutex was poisoned"))?;
        state.samples.push(SpooledDelivery {
            outputs: Arc::from(samples),
        });
        drop(state);
        Ok(())
    }

    fn snapshot(&self) -> anyhow::Result<CollectedProfile> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("speedtest profile mutex was poisoned"))?
            .clone())
    }
}

fn ensure_all_main_datasets_sampled(
    discovery: &transferia_core::delivery::DeliveryDiscovery,
    samples: &[SpooledDelivery],
) -> anyhow::Result<()> {
    let sampled = samples
        .iter()
        .flat_map(|sample| sample.outputs.iter())
        .filter(|output| !output.is_dlq)
        .map(|output| output.table.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = discovery
        .datasets
        .iter()
        .filter(|dataset| dataset.role == DatasetRole::Main)
        .filter(|dataset| !sampled.contains(dataset.name.as_ref()))
        .map(|dataset| dataset.name.as_ref())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        missing.is_empty(),
        "source speedtest did not observe rows for configured dataset(s): {}",
        missing.join(", ")
    );
    Ok(())
}

fn aggregate_sample_profiles(
    samples: &[SpooledDelivery],
) -> anyhow::Result<Vec<SpeedtestDatasetProfile>> {
    let mut profiles = BTreeMap::<(Arc<str>, bool), ProfiledDataset>::new();
    for output in samples.iter().flat_map(|sample| sample.outputs.iter()) {
        let key = (Arc::clone(&output.table), output.is_dlq);
        match profiles.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(output.profile.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge(&output.profile)?;
            }
        }
    }
    profiles
        .into_values()
        .map(ProfiledDataset::finish)
        .collect()
}

struct ProfileSink {
    collector: Arc<ProfileCollector>,
}

struct MeasurementDiscardSink {
    counters: Arc<Mutex<(u64, u64)>>,
}

impl Sink for MeasurementDiscardSink {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(async move {
            while let Some(delivery) = tokio::select! {
                () = io.cancellation.cancelled() => return Ok(()),
                delivery = io.deliveries.recv() => delivery,
            } {
                {
                    let mut counters = self.counters.lock().map_err(|_| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "speedtest measurement mutex was poisoned"
                        ))
                    })?;
                    for output in &delivery.outputs {
                        counters.0 =
                            counters
                                .0
                                .checked_add(output.rows() as u64)
                                .ok_or_else(|| {
                                    DataPlaneFailure::fatal(anyhow::anyhow!(
                                        "speedtest source row count overflow"
                                    ))
                                })?;
                        counters.1 =
                            counters
                                .1
                                .checked_add(output.bytes() as u64)
                                .ok_or_else(|| {
                                    DataPlaneFailure::fatal(anyhow::anyhow!(
                                        "speedtest source byte count overflow"
                                    ))
                                })?;
                    }
                }
                let id = delivery.id;
                drop(delivery);
                io.events
                    .send(SinkEvent::CommittedThrough(id))
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::retryable(anyhow::anyhow!(
                            "speedtest discard sink event channel closed"
                        ))
                    })?;
            }
            Ok(())
        })
    }
}

impl ProfileSink {
    const fn new(collector: Arc<ProfileCollector>) -> Self {
        Self { collector }
    }
}

impl Sink for ProfileSink {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(async move {
            while let Some(delivery) = tokio::select! {
                () = io.cancellation.cancelled() => return Ok(()),
                delivery = io.deliveries.recv() => delivery,
            } {
                self.collector
                    .add(&delivery)
                    .await
                    .map_err(DataPlaneFailure::fatal)?;
                let id = delivery.id;
                drop(delivery);
                io.events
                    .send(SinkEvent::CommittedThrough(id))
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::retryable(anyhow::anyhow!(
                            "speedtest profile sink event channel closed"
                        ))
                    })?;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum UniqueKeyKind {
    Signed {
        span: u128,
        direction: ShiftDirection,
        max_iteration: u128,
    },
    Unsigned {
        span: u128,
        direction: ShiftDirection,
        max_iteration: u128,
    },
    Utf8 {
        suffix_width: usize,
        max_iteration: u128,
    },
    UnboundedUtf8,
    LargeUtf8 {
        suffix_width: usize,
        max_iteration: u128,
    },
    UnboundedLargeUtf8,
    Binary {
        suffix_width: usize,
        max_iteration: u128,
    },
    UnboundedBinary,
    LargeBinary {
        suffix_width: usize,
        max_iteration: u128,
    },
    UnboundedLargeBinary,
    FixedSizeBinary {
        width: i32,
        max_iteration: u128,
    },
}

#[derive(Clone, Copy, Debug)]
enum ShiftDirection {
    Up,
    Down,
}

async fn load_spooled_deliveries(
    deliveries: &[SpooledDelivery],
    memory: &PipelineMemory,
) -> anyhow::Result<Vec<LoadedDelivery>> {
    let reserved_bytes = deliveries
        .iter()
        .flat_map(|delivery| delivery.outputs.iter())
        .try_fold(0_usize, |total, output| {
            total
                .checked_add(output.arrow_bytes)
                .context("speedtest sample memory estimate overflow")
        })?;
    let reservation = memory.reserve_progress_source(reserved_bytes).await;
    let mut loaded = Vec::with_capacity(deliveries.len());
    for delivery in deliveries {
        let mut outputs = Vec::with_capacity(delivery.outputs.len());
        for output in delivery.outputs.iter() {
            let file = Arc::clone(&output.file);
            let schema = Arc::clone(&output.schema);
            let batch = tokio::task::spawn_blocking(move || {
                // Arrow IPC may expose every column as a slice of one shared
                // record-block allocation. Summing each array's retained size
                // then counts that allocation once per column and can throttle
                // replay by orders of magnitude. Materialize only when the
                // shared backing is materially larger than the visible arrays.
                let decoded = compact_record_batch(read_spooled_batch(&file)?)?;
                Ok::<_, anyhow::Error>(RecordBatch::try_new(schema, decoded.columns().to_vec())?)
            })
            .await
            .context("speedtest sample load task failed")??;
            outputs.push(LoadedOutput {
                table: Arc::clone(&output.table),
                is_dlq: output.is_dlq,
                batch,
                system_columns: output.system_columns.clone(),
                memory: reservation.clone(),
                key_row_offset: 0,
            });
        }
        loaded.push(LoadedDelivery { outputs });
    }
    let actual_bytes = loaded
        .iter()
        .flat_map(|delivery| &delivery.outputs)
        .try_fold(0_usize, |total, output| {
            total
                .checked_add(output.batch.get_array_memory_size())
                .context("speedtest loaded sample size overflow")
        })?;
    reservation.grow_progress_source_to(actual_bytes)?;
    let _ = reservation.shrink_to(actual_bytes);
    let mut offsets = HashMap::<Arc<str>, u128>::new();
    for output in loaded
        .iter_mut()
        .flat_map(|delivery| delivery.outputs.iter_mut())
    {
        let offset = offsets.entry(Arc::clone(&output.table)).or_default();
        output.key_row_offset = *offset;
        *offset = offset
            .checked_add(output.batch.num_rows() as u128)
            .context("speedtest sampled dataset row count overflow")?;
    }
    Ok(loaded)
}

#[derive(Clone)]
struct UniqueKey {
    column: usize,

    kind: UniqueKeyKind,

    namespace: u64,

    sample_rows: u128,

    forbidden_iterations: BTreeSet<u128>,
}

struct ProfileGeneratorSource {
    // These spooled batches are the generator's empirical in-flight profile.
    // Replaying them preserves joint column distributions, correlations, null
    // patterns, numeric/temporal ranges, lengths, and per-batch cardinalities
    // exactly. Synthesizing columns independently from marginal summaries
    // would be less representative and could violate domain constraints.
    deliveries: Vec<LoadedDelivery>,

    table_names: HashMap<Arc<str>, Arc<str>>,

    keys: HashMap<Arc<str>, UniqueKey>,

    memory: PipelineMemory,

    duration: Duration,

    deadline: Option<tokio::time::Instant>,

    iteration: u128,

    delivery_index: usize,

    emitted: bool,
}

impl ProfileGeneratorSource {
    async fn new(
        deliveries: &[SpooledDelivery],
        discovery: &transferia_core::delivery::DeliveryDiscovery,
        isolation: &SinkSpeedtestIsolation,
        isolation_id: &str,
        memory: PipelineMemory,
        duration: Duration,
    ) -> anyhow::Result<Self> {
        let deliveries = load_spooled_deliveries(deliveries, &memory).await?;
        let outputs = deliveries
            .iter()
            .flat_map(|delivery| delivery.outputs.iter())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            outputs.iter().all(|output| {
                !output
                    .system_columns
                    .contains(SystemColumnKind::ChangeOperation)
                    && !output
                        .system_columns
                        .contains(SystemColumnKind::ChangedColumns)
            }),
            "destination speedtest cannot safely synthesize and replay changelog rows"
        );
        let table_names = outputs
            .iter()
            .map(|output| {
                Ok((
                    Arc::clone(&output.table),
                    isolation.table_name(&output.table)?,
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        let namespace = u64::from_str_radix(
            isolation_id
                .get(..16)
                .context("speedtest isolation identifier is too short")?,
            16,
        )?;
        let keys = unique_key_strategies(&outputs, discovery, namespace)?;
        Ok(Self {
            deliveries,
            table_names,
            keys,
            memory,
            duration,
            deadline: None,
            iteration: 0,
            delivery_index: 0,
            emitted: false,
        })
    }
}

impl Source for ProfileGeneratorSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let deadline = match self.deadline {
                Some(deadline) => deadline,
                None => {
                    let deadline = tokio::time::Instant::now()
                        .checked_add(self.duration)
                        .ok_or_else(|| {
                            DataPlaneFailure::fatal(anyhow::anyhow!(
                                "speedtest duration is too large"
                            ))
                        })?;
                    self.deadline = Some(deadline);
                    deadline
                }
            };
            if self.emitted && tokio::time::Instant::now() >= deadline {
                return Ok(SourceBatch::Finished);
            }
            let delivery = self.deliveries.get(self.delivery_index).ok_or_else(|| {
                DataPlaneFailure::fatal(anyhow::anyhow!(
                    "speedtest sample sequence unexpectedly became empty"
                ))
            })?;
            let mut tables = Vec::with_capacity(delivery.outputs.len());
            let mut generated_bytes = 0_usize;
            let mut rows = 0_u64;
            let mut reservations = Vec::with_capacity(delivery.outputs.len() + 1);
            for output in &delivery.outputs {
                let (batch, replacement_bytes) = match self.keys.get(&output.table) {
                    Some(key) => rewrite_unique_key(
                        &output.batch,
                        key,
                        self.iteration,
                        output.key_row_offset,
                    )
                    .map_err(DataPlaneFailure::fatal)?,
                    None => (output.batch.clone(), 0),
                };
                generated_bytes =
                    generated_bytes
                        .checked_add(replacement_bytes)
                        .ok_or_else(|| {
                            DataPlaneFailure::fatal(anyhow::anyhow!(
                                "speedtest generated byte count overflow"
                            ))
                        })?;
                rows = rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "speedtest generated row count overflow"
                    ))
                })?;
                reservations.push(output.memory.clone());
                tables.push(TableData::new(
                    Arc::clone(self.table_names.get(&output.table).ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "speedtest table mapping disappeared"
                        ))
                    })?),
                    output.is_dlq,
                    batch,
                    output.system_columns.clone(),
                ));
            }
            if generated_bytes > 0 {
                // Rewritten PK arrays coexist with the immutable sampled arrays
                // until the sink acknowledges this delivery. Treat that delta as
                // progress-critical so a sample equal to the configured limit
                // cannot deadlock before its first repeated batch.
                // The immutable sample owns the progress-source lease for the
                // destination probe. The rewritten PK arrays are a downstream
                // materialization over those sampled arrays, so account their
                // exact delta as transform memory. `reserve_transform` is the
                // pipeline's progress exception and cannot deadlock behind the
                // resident sample when that sample is at or over the limit.
                reservations.push(self.memory.reserve_transform(generated_bytes));
            }
            self.delivery_index += 1;
            if self.delivery_index == self.deliveries.len() {
                self.delivery_index = 0;
                self.iteration = self.iteration.checked_add(1).ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "speedtest generator iteration overflow"
                    ))
                })?;
            }
            self.emitted = true;
            Ok(SourceBatch::Typed {
                tables,
                source_rows: rows,
                commit_marker: None,
                memory: reservations,
            })
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn unique_key_strategies(
    outputs: &[&LoadedOutput],
    discovery: &transferia_core::delivery::DeliveryDiscovery,
    namespace: u64,
) -> anyhow::Result<HashMap<Arc<str>, UniqueKey>> {
    let mut result = HashMap::new();
    let mut grouped = BTreeMap::<Arc<str>, Vec<&LoadedOutput>>::new();
    for &output in outputs {
        grouped
            .entry(Arc::clone(&output.table))
            .or_default()
            .push(output);
    }
    for (table, outputs) in grouped {
        let dataset = discovery
            .datasets
            .iter()
            .find(|dataset| dataset.name == table)
            .with_context(|| format!("sampled unknown dataset '{table}'"))?;
        let primary_keys = dataset
            .incoming_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .collect::<Vec<_>>();
        if primary_keys.is_empty() {
            continue;
        }
        let mut candidates = primary_keys
            .iter()
            .filter_map(|(column_index, column)| {
                unique_key_rank(&column.data_type).map(|rank| (rank, *column_index, *column))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(rank, _, _)| *rank);
        let mut selected = None;
        let mut rejected = Vec::new();
        let batches = outputs
            .iter()
            .map(|output| &output.batch)
            .collect::<Vec<_>>();
        for (_, column_index, column) in candidates {
            match build_unique_key_kind_many(&batches, column_index, column) {
                Ok(kind) => {
                    let capacity = unique_key_max_iteration(kind);
                    if selected.as_ref().is_none_or(|(_, selected_kind)| {
                        capacity > unique_key_max_iteration(*selected_kind)
                    }) {
                        selected = Some((column_index, kind));
                    }
                }
                Err(error) => rejected.push(format!("{}: {error:#}", column.name)),
            }
        }
        let Some((column, kind)) = selected else {
            anyhow::bail!(
                "dataset '{}' has no primary-key column that can be replayed losslessly for a destination speedtest{}",
                table,
                if rejected.is_empty() {
                    String::new()
                } else {
                    format!(": {}", rejected.join("; "))
                }
            );
        };
        let namespace = match kind {
            UniqueKeyKind::FixedSizeBinary { .. } => {
                fixed_binary_namespace_many(&batches, column, namespace)?
            }
            UniqueKeyKind::UnboundedUtf8
            | UniqueKeyKind::UnboundedLargeUtf8
            | UniqueKeyKind::UnboundedBinary
            | UniqueKeyKind::UnboundedLargeBinary => {
                unbounded_namespace_many(&batches, column, kind, namespace)?
            }
            _ => namespace,
        };
        let forbidden_iterations = forbidden_replay_iterations_many(&batches, column, kind)?;
        let sample_rows = batches.iter().try_fold(0_u128, |total, batch| {
            total
                .checked_add(batch.num_rows() as u128)
                .context("speedtest sampled dataset row count overflow")
        })?;
        result.insert(
            table,
            UniqueKey {
                column,
                kind,
                namespace,
                sample_rows,
                forbidden_iterations,
            },
        );
    }
    Ok(result)
}

const fn unique_key_max_iteration(kind: UniqueKeyKind) -> u128 {
    match kind {
        UniqueKeyKind::Signed { max_iteration, .. }
        | UniqueKeyKind::Unsigned { max_iteration, .. }
        | UniqueKeyKind::Utf8 { max_iteration, .. }
        | UniqueKeyKind::LargeUtf8 { max_iteration, .. }
        | UniqueKeyKind::Binary { max_iteration, .. }
        | UniqueKeyKind::LargeBinary { max_iteration, .. }
        | UniqueKeyKind::FixedSizeBinary { max_iteration, .. } => max_iteration,
        UniqueKeyKind::UnboundedUtf8
        | UniqueKeyKind::UnboundedLargeUtf8
        | UniqueKeyKind::UnboundedBinary
        | UniqueKeyKind::UnboundedLargeBinary => u128::MAX,
    }
}

const fn unique_key_rank(data_type: &DataType) -> Option<u8> {
    Some(match data_type {
        DataType::FixedSizeBinary(width) if *width >= 16 => 0,
        DataType::UInt64 | DataType::Int64 => 1,
        DataType::Utf8 | DataType::LargeUtf8 => 2,
        DataType::Binary | DataType::LargeBinary => 3,
        DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8 => 4,
        _ => return None,
    })
}

fn build_unique_key_kind_many(
    batches: &[&RecordBatch],
    column: usize,
    schema: &transferia_core::data::schema::SchemaColumn,
) -> anyhow::Result<UniqueKeyKind> {
    anyhow::ensure!(!batches.is_empty(), "empty primary-key sample");
    for batch in batches {
        anyhow::ensure!(
            batch.schema().field(column).data_type() == &schema.data_type,
            "sample type does not match discovery"
        );
        anyhow::ensure!(
            batch.column(column).null_count() == 0,
            "sampled primary key contains NULL"
        );
    }
    macro_rules! signed {
        ($array:ty, $native:ty) => {{
            let mut min = None;
            let mut max = None;
            for batch in batches {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<$array>()
                    .context("sampled signed primary-key type mismatch")?;
                for value in values.values().iter().copied() {
                    let value = value as i128;
                    min = Some(min.map_or(value, |current: i128| current.min(value)));
                    max = Some(max.map_or(value, |current: i128| current.max(value)));
                }
            }
            let min = min.context("empty primary-key sample")?;
            let max = max.context("empty primary-key sample")?;
            let (span, direction, max_iteration) =
                signed_shift(min, max, <$native>::MIN as i128, <$native>::MAX as i128)?;
            UniqueKeyKind::Signed {
                span,
                direction,
                max_iteration,
            }
        }};
    }
    macro_rules! unsigned {
        ($array:ty, $native:ty) => {{
            let mut min = None;
            let mut max = None;
            for batch in batches {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<$array>()
                    .context("sampled unsigned primary-key type mismatch")?;
                for value in values.values().iter().copied() {
                    let value = value as u128;
                    min = Some(min.map_or(value, |current: u128| current.min(value)));
                    max = Some(max.map_or(value, |current: u128| current.max(value)));
                }
            }
            let min = min.context("empty primary-key sample")?;
            let max = max.context("empty primary-key sample")?;
            let (span, direction, max_iteration) =
                unsigned_shift(min, max, <$native>::MAX as u128)?;
            UniqueKeyKind::Unsigned {
                span,
                direction,
                max_iteration,
            }
        }};
    }
    Ok(match &schema.data_type {
        DataType::Int8 => signed!(Int8Array, i8),
        DataType::Int16 => signed!(Int16Array, i16),
        DataType::Int32 => signed!(Int32Array, i32),
        DataType::Int64 => signed!(Int64Array, i64),
        DataType::UInt8 => unsigned!(UInt8Array, u8),
        DataType::UInt16 => unsigned!(UInt16Array, u16),
        DataType::UInt32 => unsigned!(UInt32Array, u32),
        DataType::UInt64 => unsigned!(UInt64Array, u64),
        DataType::Utf8 => {
            let max = batches.iter().try_fold(0, |max, batch| {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("sampled Utf8 primary-key type mismatch")?;
                Ok::<_, anyhow::Error>(
                    max.max(values.iter().flatten().map(str::len).max().unwrap_or(0)),
                )
            })?;
            match schema.max_length {
                Some(_) => {
                    let (suffix_width, max_iteration) =
                        suffix_capacity(schema.max_length, max, 36)?;
                    UniqueKeyKind::Utf8 {
                        suffix_width,
                        max_iteration,
                    }
                }
                None => UniqueKeyKind::UnboundedUtf8,
            }
        }
        DataType::LargeUtf8 => {
            let max = batches.iter().try_fold(0, |max, batch| {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .context("sampled LargeUtf8 primary-key type mismatch")?;
                Ok::<_, anyhow::Error>(
                    max.max(values.iter().flatten().map(str::len).max().unwrap_or(0)),
                )
            })?;
            match schema.max_length {
                Some(_) => {
                    let (suffix_width, max_iteration) =
                        suffix_capacity(schema.max_length, max, 36)?;
                    UniqueKeyKind::LargeUtf8 {
                        suffix_width,
                        max_iteration,
                    }
                }
                None => UniqueKeyKind::UnboundedLargeUtf8,
            }
        }
        DataType::Binary => {
            let max = batches.iter().try_fold(0, |max, batch| {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .context("sampled Binary primary-key type mismatch")?;
                Ok::<_, anyhow::Error>(
                    max.max(values.iter().flatten().map(<[u8]>::len).max().unwrap_or(0)),
                )
            })?;
            match schema.max_length {
                Some(_) => {
                    let (suffix_width, max_iteration) =
                        suffix_capacity(schema.max_length, max, 256)?;
                    UniqueKeyKind::Binary {
                        suffix_width,
                        max_iteration,
                    }
                }
                None => UniqueKeyKind::UnboundedBinary,
            }
        }
        DataType::LargeBinary => {
            let max = batches.iter().try_fold(0, |max, batch| {
                let values = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .context("sampled LargeBinary primary-key type mismatch")?;
                Ok::<_, anyhow::Error>(
                    max.max(values.iter().flatten().map(<[u8]>::len).max().unwrap_or(0)),
                )
            })?;
            match schema.max_length {
                Some(_) => {
                    let (suffix_width, max_iteration) =
                        suffix_capacity(schema.max_length, max, 256)?;
                    UniqueKeyKind::LargeBinary {
                        suffix_width,
                        max_iteration,
                    }
                }
                None => UniqueKeyKind::UnboundedLargeBinary,
            }
        }
        DataType::FixedSizeBinary(width) if *width >= 16 => {
            let rows = batches.iter().try_fold(0_u128, |total, batch| {
                total
                    .checked_add(batch.num_rows() as u128)
                    .context("sampled primary-key row count overflow")
            })?;
            anyhow::ensure!(rows > 0, "empty primary-key sample");
            let max_iteration = (u128::from(u64::MAX) - (rows - 1)) / rows;
            anyhow::ensure!(
                max_iteration > 0,
                "fixed-binary primary-key space is exhausted"
            );
            UniqueKeyKind::FixedSizeBinary {
                width: *width,
                max_iteration,
            }
        }
        _ => anyhow::bail!("unsupported primary-key type {:?}", schema.data_type),
    })
}

fn signed_shift(
    min: i128,
    max: i128,
    type_min: i128,
    type_max: i128,
) -> anyhow::Result<(u128, ShiftDirection, u128)> {
    let span = u128::try_from(
        max.checked_sub(min)
            .context("signed primary-key range overflow")?,
    )?
    .checked_add(1)
    .context("signed primary-key span overflow")?;
    let up = u128::try_from(type_max - max)? / span;
    let down = u128::try_from(min - type_min)? / span;
    let (direction, max_iteration) = if up >= down {
        (ShiftDirection::Up, up)
    } else {
        (ShiftDirection::Down, down)
    };
    anyhow::ensure!(
        max_iteration > 0,
        "integer primary-key space cannot hold a second sample batch"
    );
    Ok((span, direction, max_iteration))
}

fn unsigned_shift(
    min: u128,
    max: u128,
    type_max: u128,
) -> anyhow::Result<(u128, ShiftDirection, u128)> {
    let span = max
        .checked_sub(min)
        .and_then(|range| range.checked_add(1))
        .context("unsigned primary-key span overflow")?;
    let up = (type_max - max) / span;
    let down = min / span;
    let (direction, max_iteration) = if up >= down {
        (ShiftDirection::Up, up)
    } else {
        (ShiftDirection::Down, down)
    };
    anyhow::ensure!(
        max_iteration > 0,
        "integer primary-key space cannot hold a second sample batch"
    );
    Ok((span, direction, max_iteration))
}

fn suffix_capacity(
    max_length: Option<usize>,
    observed_max: usize,
    radix: u128,
) -> anyhow::Result<(usize, u128)> {
    let max_length = max_length.context("bounded replay suffix requires max_length")?;
    let width = max_length
        .checked_sub(observed_max)
        .with_context(|| format!("sampled primary key exceeds declared max_length {max_length}"))?;
    anyhow::ensure!(
        width > 0,
        "declared max_length leaves no room for a unique replay suffix"
    );
    let max_iteration = (0..width)
        .try_fold(1_u128, |value, _| value.checked_mul(radix))
        .unwrap_or(u128::MAX)
        .checked_sub(1)
        .context("primary-key replay suffix has no usable values")?;
    anyhow::ensure!(
        max_iteration > 0,
        "primary-key replay suffix has no usable values"
    );
    Ok((width, max_iteration))
}

fn forbidden_replay_iterations_many(
    batches: &[&RecordBatch],
    column: usize,
    kind: UniqueKeyKind,
) -> anyhow::Result<BTreeSet<u128>> {
    let (suffix_width, radix, values): (usize, u128, Vec<&[u8]>) = match kind {
        UniqueKeyKind::Utf8 { suffix_width, .. } => {
            let mut values = Vec::new();
            for batch in batches {
                values.extend(
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("sampled Utf8 primary-key type mismatch")?
                        .iter()
                        .map(|value| value.expect("validated non-null primary key").as_bytes()),
                );
            }
            (suffix_width, 36, values)
        }
        UniqueKeyKind::LargeUtf8 { suffix_width, .. } => {
            let mut values = Vec::new();
            for batch in batches {
                values.extend(
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .context("sampled LargeUtf8 primary-key type mismatch")?
                        .iter()
                        .map(|value| value.expect("validated non-null primary key").as_bytes()),
                );
            }
            (suffix_width, 36, values)
        }
        UniqueKeyKind::Binary { suffix_width, .. } => {
            let mut values = Vec::new();
            for batch in batches {
                values.extend(
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .context("sampled Binary primary-key type mismatch")?
                        .iter()
                        .map(|value| value.expect("validated non-null primary key")),
                );
            }
            (suffix_width, 256, values)
        }
        UniqueKeyKind::LargeBinary { suffix_width, .. } => {
            let mut values = Vec::new();
            for batch in batches {
                values.extend(
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .context("sampled LargeBinary primary-key type mismatch")?
                        .iter()
                        .map(|value| value.expect("validated non-null primary key")),
                );
            }
            (suffix_width, 256, values)
        }
        _ => return Ok(BTreeSet::new()),
    };
    let originals = values.iter().copied().collect::<HashSet<_>>();
    let mut forbidden = BTreeSet::new();
    for value in values {
        let Some(prefix_length) = value.len().checked_sub(suffix_width) else {
            continue;
        };
        let (prefix, suffix) = value.split_at(prefix_length);
        if !originals.contains(prefix) {
            continue;
        }
        if let Some(iteration) = decode_replay_suffix(suffix, radix) {
            if iteration > 0 {
                forbidden.insert(iteration);
            }
        }
    }
    Ok(forbidden)
}

fn decode_replay_suffix(suffix: &[u8], radix: u128) -> Option<u128> {
    suffix.iter().try_fold(0_u128, |value, byte| {
        let digit = match radix {
            36 => match byte {
                b'0'..=b'9' => u128::from(byte - b'0'),
                b'a'..=b'z' => u128::from(byte - b'a') + 10,
                _ => return None,
            },
            256 => u128::from(*byte),
            _ => return None,
        };
        value.checked_mul(radix)?.checked_add(digit)
    })
}

fn fixed_binary_namespace_many(
    batches: &[&RecordBatch],
    column: usize,
    seed: u64,
) -> anyhow::Result<u64> {
    let mut occupied = HashSet::new();
    for batch in batches {
        let source = batch
            .column(column)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .context("sampled fixed-size binary primary-key type mismatch")?;
        for value in source {
            let prefix: [u8; 8] = value
                .expect("validated non-null primary key")
                .get(..8)
                .expect("validated fixed-size binary width")
                .try_into()
                .expect("fixed-size binary namespace is eight bytes");
            occupied.insert(u64::from_be_bytes(prefix));
        }
    }
    let mut candidate = seed;
    for _ in 0..=occupied.len() {
        if !occupied.contains(&candidate) {
            return Ok(candidate);
        }
        candidate = candidate.wrapping_add(1);
    }
    anyhow::bail!("fixed-size binary namespace space is exhausted")
}

fn unbounded_namespace_many(
    batches: &[&RecordBatch],
    column: usize,
    kind: UniqueKeyKind,
    seed: u64,
) -> anyhow::Result<u64> {
    let mut candidate = seed;
    for _ in 0..=batches.iter().map(|batch| batch.num_rows()).sum::<usize>() {
        let binary_prefix = unbounded_key_prefix(candidate);
        let text_prefix = unbounded_text_prefix(candidate);
        let occupied = batches.iter().try_fold(false, |occupied, batch| {
            if occupied {
                return Ok::<_, anyhow::Error>(true);
            }
            let found = match kind {
                UniqueKeyKind::UnboundedUtf8 => batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("sampled Utf8 primary-key type mismatch")?
                    .iter()
                    .flatten()
                    .any(|value| value.starts_with(&text_prefix)),
                UniqueKeyKind::UnboundedLargeUtf8 => batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .context("sampled LargeUtf8 primary-key type mismatch")?
                    .iter()
                    .flatten()
                    .any(|value| value.starts_with(&text_prefix)),
                UniqueKeyKind::UnboundedBinary => batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .context("sampled Binary primary-key type mismatch")?
                    .iter()
                    .flatten()
                    .any(|value| value.starts_with(&binary_prefix)),
                UniqueKeyKind::UnboundedLargeBinary => batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .context("sampled LargeBinary primary-key type mismatch")?
                    .iter()
                    .flatten()
                    .any(|value| value.starts_with(&binary_prefix)),
                _ => anyhow::bail!("bounded primary-key kind has no namespace prefix"),
            };
            Ok(found)
        })?;
        if !occupied {
            return Ok(candidate);
        }
        candidate = candidate.wrapping_add(1);
    }
    anyhow::bail!("unbounded primary-key namespace space is exhausted")
}

fn unbounded_key_prefix(namespace: u64) -> [u8; 10] {
    let mut prefix = [0_u8; 10];
    prefix[..2].copy_from_slice(b"\0T");
    prefix[2..].copy_from_slice(&namespace.to_be_bytes());
    prefix
}

fn unbounded_text_prefix(namespace: u64) -> String {
    format!("\0transferia-speedtest-{namespace:016x}:")
}

fn available_replay_iteration(
    requested: u128,
    maximum: u128,
    forbidden: &BTreeSet<u128>,
) -> anyhow::Result<u128> {
    let mut available = requested;
    for value in forbidden {
        if *value > available {
            break;
        }
        available = available
            .checked_add(1)
            .context("primary-key replay iteration overflow")?;
    }
    anyhow::ensure!(
        available <= maximum,
        "primary-key replay space exhausted after excluding sampled keys"
    );
    Ok(available)
}

fn rewrite_unique_key(
    batch: &RecordBatch,
    key: &UniqueKey,
    iteration: u128,
    row_offset: u128,
) -> anyhow::Result<(RecordBatch, usize)> {
    if iteration == 0 {
        return Ok((batch.clone(), 0));
    }
    let iteration = match key.kind {
        UniqueKeyKind::Utf8 { max_iteration, .. }
        | UniqueKeyKind::LargeUtf8 { max_iteration, .. }
        | UniqueKeyKind::Binary { max_iteration, .. }
        | UniqueKeyKind::LargeBinary { max_iteration, .. } => {
            available_replay_iteration(iteration, max_iteration, &key.forbidden_iterations)?
        }
        _ => iteration,
    };
    let column = key.column;
    macro_rules! signed_array {
        ($array:ty, $native:ty, $span:expr, $direction:expr, $max_iteration:expr) => {{
            anyhow::ensure!(
                iteration <= $max_iteration,
                "integer primary-key replay space exhausted"
            );
            let delta = i128::try_from(
                iteration
                    .checked_mul($span)
                    .context("primary-key shift overflow")?,
            )?;
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<$array>()
                .context("speedtest signed primary-key type mismatch")?;
            Arc::new(<$array>::from_iter_values(source.values().iter().map(
                |value| {
                    let value = *value as i128;
                    let shifted = match $direction {
                        ShiftDirection::Up => value + delta,
                        ShiftDirection::Down => value - delta,
                    };
                    <$native>::try_from(shifted).expect("validated primary-key shift")
                },
            ))) as ArrayRef
        }};
    }
    macro_rules! unsigned_array {
        ($array:ty, $native:ty, $span:expr, $direction:expr, $max_iteration:expr) => {{
            anyhow::ensure!(
                iteration <= $max_iteration,
                "integer primary-key replay space exhausted"
            );
            let delta = iteration
                .checked_mul($span)
                .context("primary-key shift overflow")?;
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<$array>()
                .context("speedtest unsigned primary-key type mismatch")?;
            Arc::new(<$array>::from_iter_values(source.values().iter().map(
                |value| {
                    let value = *value as u128;
                    let shifted = match $direction {
                        ShiftDirection::Up => value + delta,
                        ShiftDirection::Down => value - delta,
                    };
                    <$native>::try_from(shifted).expect("validated primary-key shift")
                },
            ))) as ArrayRef
        }};
    }
    let replacement: ArrayRef = match (key.kind, batch.schema().field(column).data_type()) {
        (
            UniqueKeyKind::Signed {
                span,
                direction,
                max_iteration,
            },
            DataType::Int8,
        ) => signed_array!(Int8Array, i8, span, direction, max_iteration),
        (
            UniqueKeyKind::Signed {
                span,
                direction,
                max_iteration,
            },
            DataType::Int16,
        ) => signed_array!(Int16Array, i16, span, direction, max_iteration),
        (
            UniqueKeyKind::Signed {
                span,
                direction,
                max_iteration,
            },
            DataType::Int32,
        ) => signed_array!(Int32Array, i32, span, direction, max_iteration),
        (
            UniqueKeyKind::Signed {
                span,
                direction,
                max_iteration,
            },
            DataType::Int64,
        ) => signed_array!(Int64Array, i64, span, direction, max_iteration),
        (
            UniqueKeyKind::Unsigned {
                span,
                direction,
                max_iteration,
            },
            DataType::UInt8,
        ) => unsigned_array!(UInt8Array, u8, span, direction, max_iteration),
        (
            UniqueKeyKind::Unsigned {
                span,
                direction,
                max_iteration,
            },
            DataType::UInt16,
        ) => unsigned_array!(UInt16Array, u16, span, direction, max_iteration),
        (
            UniqueKeyKind::Unsigned {
                span,
                direction,
                max_iteration,
            },
            DataType::UInt32,
        ) => unsigned_array!(UInt32Array, u32, span, direction, max_iteration),
        (
            UniqueKeyKind::Unsigned {
                span,
                direction,
                max_iteration,
            },
            DataType::UInt64,
        ) => unsigned_array!(UInt64Array, u64, span, direction, max_iteration),
        (
            UniqueKeyKind::Utf8 {
                suffix_width,
                max_iteration,
            },
            DataType::Utf8,
        ) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("speedtest Utf8 primary-key type mismatch")?;
            let suffix = fixed_radix_suffix(iteration, suffix_width, 36, max_iteration)?;
            Arc::new(StringArray::from_iter_values(source.iter().map(|value| {
                let mut value = value.expect("validated non-null primary key").to_owned();
                value.push_str(&suffix);
                value
            })))
        }
        (UniqueKeyKind::UnboundedUtf8, DataType::Utf8) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("speedtest Utf8 primary-key type mismatch")?;
            Arc::new(StringArray::from_iter_values(source.iter().map(|value| {
                unbounded_text_key(
                    key.namespace,
                    value.expect("validated non-null primary key"),
                    iteration,
                )
            })))
        }
        (
            UniqueKeyKind::LargeUtf8 {
                suffix_width,
                max_iteration,
            },
            DataType::LargeUtf8,
        ) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("speedtest LargeUtf8 primary-key type mismatch")?;
            let suffix = fixed_radix_suffix(iteration, suffix_width, 36, max_iteration)?;
            Arc::new(LargeStringArray::from_iter_values(source.iter().map(
                |value| {
                    let mut value = value.expect("validated non-null primary key").to_owned();
                    value.push_str(&suffix);
                    value
                },
            )))
        }
        (UniqueKeyKind::UnboundedLargeUtf8, DataType::LargeUtf8) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("speedtest LargeUtf8 primary-key type mismatch")?;
            Arc::new(LargeStringArray::from_iter_values(source.iter().map(
                |value| {
                    unbounded_text_key(
                        key.namespace,
                        value.expect("validated non-null primary key"),
                        iteration,
                    )
                },
            )))
        }
        (
            UniqueKeyKind::Binary {
                suffix_width,
                max_iteration,
            },
            DataType::Binary,
        ) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("speedtest Binary primary-key type mismatch")?;
            let suffix = fixed_binary_suffix(iteration, suffix_width, max_iteration)?;
            let values = source
                .iter()
                .map(|value| {
                    let mut value = value.expect("validated non-null primary key").to_vec();
                    value.extend_from_slice(&suffix);
                    value
                })
                .collect::<Vec<_>>();
            Arc::new(BinaryArray::from_iter_values(values))
        }
        (UniqueKeyKind::UnboundedBinary, DataType::Binary) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("speedtest Binary primary-key type mismatch")?;
            let values = source
                .iter()
                .map(|value| {
                    unbounded_binary_key(
                        key.namespace,
                        value.expect("validated non-null primary key"),
                        iteration,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Arc::new(BinaryArray::from_iter_values(values))
        }
        (
            UniqueKeyKind::LargeBinary {
                suffix_width,
                max_iteration,
            },
            DataType::LargeBinary,
        ) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .context("speedtest LargeBinary primary-key type mismatch")?;
            let suffix = fixed_binary_suffix(iteration, suffix_width, max_iteration)?;
            let values = source
                .iter()
                .map(|value| {
                    let mut value = value.expect("validated non-null primary key").to_vec();
                    value.extend_from_slice(&suffix);
                    value
                })
                .collect::<Vec<_>>();
            Arc::new(LargeBinaryArray::from_iter_values(values))
        }
        (UniqueKeyKind::UnboundedLargeBinary, DataType::LargeBinary) => {
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .context("speedtest LargeBinary primary-key type mismatch")?;
            let values = source
                .iter()
                .map(|value| {
                    unbounded_binary_key(
                        key.namespace,
                        value.expect("validated non-null primary key"),
                        iteration,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Arc::new(LargeBinaryArray::from_iter_values(values))
        }
        (
            UniqueKeyKind::FixedSizeBinary {
                width,
                max_iteration,
            },
            DataType::FixedSizeBinary(actual_width),
        ) if width == *actual_width => {
            anyhow::ensure!(
                iteration <= max_iteration,
                "fixed-binary primary-key replay space exhausted"
            );
            let source = batch
                .column(column)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .context("speedtest fixed-size binary primary-key type mismatch")?;
            let values = (0..batch.num_rows())
                .map(|row| {
                    let ordinal = iteration
                        .checked_mul(key.sample_rows)
                        .and_then(|value| value.checked_add(row_offset))
                        .and_then(|value| value.checked_add(row as u128))
                        .context("fixed-binary primary-key ordinal overflow")?;
                    let ordinal = u64::try_from(ordinal)
                        .context("fixed-binary primary-key space exhausted")?;
                    let mut value = source.value(row).to_vec();
                    value[..8].copy_from_slice(&key.namespace.to_be_bytes());
                    value[8..16].copy_from_slice(&ordinal.to_be_bytes());
                    Ok(value)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                values.iter().map(Vec::as_slice),
            )?)
        }
        _ => anyhow::bail!("speedtest primary-key Arrow type changed after discovery"),
    };
    let mut columns = batch.columns().to_vec();
    columns[column] = replacement;
    let replacement_bytes = columns[column].get_array_memory_size();
    Ok((
        RecordBatch::try_new(batch.schema(), columns)?,
        replacement_bytes,
    ))
}

fn unbounded_text_key(namespace: u64, value: &str, iteration: u128) -> String {
    format!(
        "{}{:016x}:{value}:{iteration:032x}",
        unbounded_text_prefix(namespace),
        value.len()
    )
}

fn unbounded_binary_key(namespace: u64, value: &[u8], iteration: u128) -> anyhow::Result<Vec<u8>> {
    let length = u64::try_from(value.len()).context("unbounded binary primary key is too long")?;
    let capacity = 10_usize
        .checked_add(8)
        .and_then(|size| size.checked_add(value.len()))
        .and_then(|size| size.checked_add(16))
        .context("unbounded binary primary-key size overflow")?;
    let mut result = Vec::with_capacity(capacity);
    result.extend_from_slice(&unbounded_key_prefix(namespace));
    result.extend_from_slice(&length.to_be_bytes());
    result.extend_from_slice(value);
    result.extend_from_slice(&iteration.to_be_bytes());
    Ok(result)
}

fn fixed_radix_suffix(
    mut value: u128,
    width: usize,
    radix: u128,
    maximum: u128,
) -> anyhow::Result<String> {
    anyhow::ensure!(value <= maximum, "text primary-key replay space exhausted");
    let mut bytes = vec![b'0'; width];
    for byte in bytes.iter_mut().rev() {
        let digit = u8::try_from(value % radix)?;
        *byte = if digit < 10 {
            b'0' + digit
        } else {
            b'a' + digit - 10
        };
        value /= radix;
    }
    anyhow::ensure!(value == 0, "text primary-key replay space exhausted");
    Ok(String::from_utf8(bytes).expect("base36 suffix is ASCII"))
}

fn fixed_binary_suffix(mut value: u128, width: usize, maximum: u128) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        value <= maximum,
        "binary primary-key replay space exhausted"
    );
    let mut bytes = vec![0_u8; width];
    for byte in bytes.iter_mut().rev() {
        *byte = value as u8;
        value >>= 8;
    }
    anyhow::ensure!(value == 0, "binary primary-key replay space exhausted");
    Ok(bytes)
}

#[derive(Clone)]
struct ProfiledDataset {
    name: String,

    is_dlq: bool,

    rows: usize,

    arrow_bytes: usize,

    columns: Vec<ProfiledColumn>,
}

impl ProfiledDataset {
    fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.name == other.name
                && self.is_dlq == other.is_dlq
                && self.columns.len() == other.columns.len(),
            "incompatible speedtest dataset profiles"
        );
        self.rows = self
            .rows
            .checked_add(other.rows)
            .context("speedtest dataset profile row count overflow")?;
        self.arrow_bytes = self
            .arrow_bytes
            .checked_add(other.arrow_bytes)
            .context("speedtest dataset profile byte count overflow")?;
        for (column, other) in self.columns.iter_mut().zip(&other.columns) {
            column.merge(other)?;
        }
        Ok(())
    }

    fn finish(self) -> anyhow::Result<SpeedtestDatasetProfile> {
        Ok(SpeedtestDatasetProfile {
            name: self.name,
            is_dlq: self.is_dlq,
            rows: self.rows,
            arrow_bytes: self.arrow_bytes,
            columns: self
                .columns
                .into_iter()
                .map(ProfiledColumn::finish)
                .collect::<anyhow::Result<_>>()?,
        })
    }
}

#[derive(Clone)]
struct ProfiledColumn {
    name: String,

    arrow_type: String,

    null_count: usize,

    cardinality: Option<CardinalitySketch>,

    bounds: Option<ProfileBounds>,

    range_kind: Option<SpeedtestRangeKind>,

    min_length: Option<usize>,

    max_length: Option<usize>,
}

impl ProfiledColumn {
    fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.name == other.name
                && self.arrow_type == other.arrow_type
                && self.range_kind == other.range_kind,
            "sampled column schema changed during the speedtest"
        );
        self.null_count = self
            .null_count
            .checked_add(other.null_count)
            .context("speedtest column null count overflow")?;
        match (&mut self.cardinality, &other.cardinality) {
            (Some(left), Some(right)) => left.merge(right)?,
            (None, None) => {}
            _ => anyhow::bail!("sampled column cardinality support changed during the speedtest"),
        }
        match (&mut self.bounds, &other.bounds) {
            (Some(left), Some(right)) => left.merge(right),
            (None, None) => {}
            _ => anyhow::bail!("sampled column range support changed during the speedtest"),
        }
        self.min_length = match (self.min_length, other.min_length) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.max_length = match (self.max_length, other.max_length) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        Ok(())
    }

    fn finish(self) -> anyhow::Result<SpeedtestColumnProfile> {
        let (min_value, max_value) = self.bounds.map_or((None, None), |bounds| {
            (Some(bounds.min_value), Some(bounds.max_value))
        });
        Ok(SpeedtestColumnProfile {
            name: self.name,
            arrow_type: self.arrow_type,
            null_count: self.null_count,
            distinct_count: self.cardinality.map(|sketch| sketch.estimate()),
            min_value,
            max_value,
            range_kind: self.range_kind,
            min_length: self.min_length,
            max_length: self.max_length,
        })
    }
}

#[derive(Clone)]
struct ProfileBounds {
    min_key: Vec<u8>,

    min_value: String,

    max_key: Vec<u8>,

    max_value: String,
}

impl ProfileBounds {
    fn merge(&mut self, other: &Self) {
        if other.min_key < self.min_key {
            self.min_key.clone_from(&other.min_key);
            self.min_value.clone_from(&other.min_value);
        }
        if other.max_key > self.max_key {
            self.max_key.clone_from(&other.max_key);
            self.max_value.clone_from(&other.max_value);
        }
    }
}

fn profile_batch_state(
    table: &str,
    is_dlq: bool,
    batch: &RecordBatch,
) -> anyhow::Result<ProfiledDataset> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| profile_column_state(field.name(), array.as_ref()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ProfiledDataset {
        name: table.to_owned(),
        is_dlq,
        rows: batch.num_rows(),
        arrow_bytes: batch.get_array_memory_size(),
        columns,
    })
}

fn profile_column_state(name: &str, array: &dyn Array) -> anyhow::Result<ProfiledColumn> {
    let mut min_length = None;
    let mut max_length = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        if is_length_profiled(array.data_type()) {
            let length = value_length(array, row)?;
            min_length = Some(min_length.map_or(length, |current: usize| current.min(length)));
            max_length = Some(max_length.map_or(length, |current: usize| current.max(length)));
        }
    }
    Ok(ProfiledColumn {
        name: name.to_owned(),
        arrow_type: format!("{:?}", array.data_type()),
        null_count: array.null_count(),
        cardinality: cardinality_sketch(array)?,
        bounds: profile_bounds(array)?,
        range_kind: range_kind(array.data_type()),
        min_length,
        max_length,
    })
}

const CARDINALITY_PRECISION: u32 = 10;
const CARDINALITY_REGISTERS: usize = 1 << CARDINALITY_PRECISION;

#[derive(Clone)]
struct CardinalitySketch {
    registers: [u8; CARDINALITY_REGISTERS],

    observations: u64,
}

impl Default for CardinalitySketch {
    fn default() -> Self {
        Self {
            registers: [0; CARDINALITY_REGISTERS],
            observations: 0,
        }
    }
}

impl CardinalitySketch {
    fn add(&mut self, value: &[u8]) -> anyhow::Result<()> {
        let hash = murmur3::murmur3_x64_128(&mut Cursor::new(value), 0)? as u64;
        let index = usize::try_from(hash >> (64 - CARDINALITY_PRECISION))?;
        let remainder = hash << CARDINALITY_PRECISION;
        let rank = (remainder.leading_zeros() + 1).min(64 - CARDINALITY_PRECISION + 1) as u8;
        self.registers[index] = self.registers[index].max(rank);
        self.observations = self
            .observations
            .checked_add(1)
            .context("speedtest cardinality observation count overflow")?;
        Ok(())
    }

    fn estimate(&self) -> u64 {
        if self.observations == 0 {
            return 0;
        }
        let register_count = CARDINALITY_REGISTERS as f64;
        let harmonic_sum = self
            .registers
            .iter()
            .map(|rank| 2_f64.powi(-i32::from(*rank)))
            .sum::<f64>();
        let raw = 0.7213 / (1.0 + 1.079 / register_count) * register_count.powi(2) / harmonic_sum;
        let zero_registers = self.registers.iter().filter(|rank| **rank == 0).count();
        let corrected = if zero_registers > 0 && raw <= 2.5 * register_count {
            register_count * (register_count / zero_registers as f64).ln()
        } else {
            raw
        };
        corrected.round().clamp(1.0, self.observations as f64) as u64
    }

    fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        for (register, other) in self
            .registers
            .iter_mut()
            .zip(other.registers.iter().copied())
        {
            *register = (*register).max(other);
        }
        self.observations = self
            .observations
            .checked_add(other.observations)
            .context("speedtest cardinality observation count overflow")?;
        Ok(())
    }
}

fn cardinality_sketch(array: &dyn Array) -> anyhow::Result<Option<CardinalitySketch>> {
    if !matches!(
        array.data_type(),
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
    ) {
        return Ok(None);
    }
    if matches!(array.data_type(), DataType::Null) {
        return Ok(Some(CardinalitySketch::default()));
    }
    let mut sketch = CardinalitySketch::default();
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        add_cardinality_value(&mut sketch, array, row)?;
    }
    Ok(Some(sketch))
}

fn add_cardinality_value(
    sketch: &mut CardinalitySketch,
    array: &dyn Array,
    row: usize,
) -> anyhow::Result<()> {
    macro_rules! scalar {
        ($array:ty) => {{
            let value = array
                .as_any()
                .downcast_ref::<$array>()
                .context("speedtest cardinality Arrow type mismatch")?
                .value(row);
            sketch.add(&value.to_le_bytes())?;
        }};
    }
    match array.data_type() {
        DataType::Boolean => sketch.add(&[u8::from(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("speedtest Boolean array type mismatch")?
                .value(row),
        )])?,
        DataType::Int8 => scalar!(Int8Array),
        DataType::Int16 => scalar!(Int16Array),
        DataType::Int32 => scalar!(Int32Array),
        DataType::Int64 => scalar!(Int64Array),
        DataType::UInt8 => scalar!(UInt8Array),
        DataType::UInt16 => scalar!(UInt16Array),
        DataType::UInt32 => scalar!(UInt32Array),
        DataType::UInt64 => scalar!(UInt64Array),
        DataType::Float16 => scalar!(Float16Array),
        DataType::Float32 => scalar!(Float32Array),
        DataType::Float64 => scalar!(Float64Array),
        DataType::Decimal128(_, _) => scalar!(Decimal128Array),
        DataType::Decimal256(_, _) => scalar!(Decimal256Array),
        DataType::Utf8 => sketch.add(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("speedtest Utf8 array type mismatch")?
                .value(row)
                .as_bytes(),
        )?,
        DataType::LargeUtf8 => sketch.add(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("speedtest LargeUtf8 array type mismatch")?
                .value(row)
                .as_bytes(),
        )?,
        DataType::Utf8View => sketch.add(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .context("speedtest Utf8View array type mismatch")?
                .value(row)
                .as_bytes(),
        )?,
        DataType::Binary => sketch.add(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("speedtest Binary array type mismatch")?
                .value(row),
        )?,
        DataType::LargeBinary => sketch.add(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .context("speedtest LargeBinary array type mismatch")?
                .value(row),
        )?,
        DataType::BinaryView => sketch.add(
            array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .context("speedtest BinaryView array type mismatch")?
                .value(row),
        )?,
        DataType::FixedSizeBinary(_) => sketch.add(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .context("speedtest FixedSizeBinary array type mismatch")?
                .value(row),
        )?,
        DataType::Date32 => scalar!(Date32Array),
        DataType::Date64 => scalar!(Date64Array),
        DataType::Time32(TimeUnit::Second) => scalar!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => scalar!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => scalar!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => scalar!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => scalar!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => scalar!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => scalar!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => scalar!(TimestampNanosecondArray),
        DataType::Duration(TimeUnit::Second) => scalar!(DurationSecondArray),
        DataType::Duration(TimeUnit::Millisecond) => scalar!(DurationMillisecondArray),
        DataType::Duration(TimeUnit::Microsecond) => scalar!(DurationMicrosecondArray),
        DataType::Duration(TimeUnit::Nanosecond) => scalar!(DurationNanosecondArray),
        DataType::Null => {}
        unsupported => anyhow::bail!(
            "internal speedtest cardinality dispatch error for Arrow type {unsupported:?}"
        ),
    }
    Ok(())
}

const fn range_kind(data_type: &DataType) -> Option<SpeedtestRangeKind> {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Some(SpeedtestRangeKind::Numeric),
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Some(SpeedtestRangeKind::Temporal),
        _ => None,
    }
}

const fn is_length_profiled(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn value_length(array: &dyn Array, row: usize) -> anyhow::Result<usize> {
    Ok(match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .context("speedtest Utf8 array type mismatch")?
            .value(row)
            .len(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .context("speedtest LargeUtf8 array type mismatch")?
            .value(row)
            .len(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .context("speedtest Utf8View array type mismatch")?
            .value(row)
            .len(),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("speedtest Binary array type mismatch")?
            .value(row)
            .len(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .context("speedtest LargeBinary array type mismatch")?
            .value(row)
            .len(),
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .context("speedtest BinaryView array type mismatch")?
            .value(row)
            .len(),
        DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .context("speedtest FixedSizeBinary array type mismatch")?
            .value(row)
            .len(),
        data_type => anyhow::bail!("Arrow type {data_type:?} has no profiled value length"),
    })
}

fn profile_bounds(array: &dyn Array) -> anyhow::Result<Option<ProfileBounds>> {
    if range_kind(array.data_type()).is_none() || array.len() == array.null_count() {
        return Ok(None);
    }
    let converter = RowConverter::new(vec![SortField::new(array.data_type().clone())])?;
    let values = converter.convert_columns(&[array.slice(0, array.len())])?;
    let mut minimum = None;
    let mut maximum = None;
    for index in 0..array.len() {
        if array.is_null(index) {
            continue;
        }
        let row = values.row(index);
        let key = row.as_ref();
        if minimum.is_none_or(|current: usize| key < values.row(current).as_ref()) {
            minimum = Some(index);
        }
        if maximum.is_none_or(|current: usize| key > values.row(current).as_ref()) {
            maximum = Some(index);
        }
    }
    let minimum = minimum.context("non-null profiled array has no minimum")?;
    let maximum = maximum.context("non-null profiled array has no maximum")?;
    Ok(Some(ProfileBounds {
        min_key: values.row(minimum).as_ref().to_vec(),
        min_value: array_value_to_string(array, minimum)?,
        max_key: values.row(maximum).as_ref().to_vec(),
        max_value: array_value_to_string(array, maximum)?,
    }))
}

#[derive(Default)]
struct EphemeralDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
}

impl DurableStorage for EphemeralDurableStorage {
    fn read<'a>(&'a self, key: &'a str) -> BoxFuture<'a, anyhow::Result<Option<DurableValue>>> {
        Box::pin(async move {
            Ok(self
                .values
                .lock()
                .map_err(|_| anyhow::anyhow!("ephemeral durable mutex was poisoned"))?
                .get(key)
                .cloned())
        })
    }

    fn compare_exchange<'a>(
        &'a self,
        key: &'a str,
        expected_revision: Option<u64>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<CompareExchangeResult>> {
        Box::pin(async move {
            let mut values = self
                .values
                .lock()
                .map_err(|_| anyhow::anyhow!("ephemeral durable mutex was poisoned"))?;
            let current = values.get(key).cloned();
            if current.as_ref().map(|value| value.revision) != expected_revision {
                return Ok(CompareExchangeResult::Conflict(current));
            }
            let revision = match expected_revision {
                Some(revision) => revision
                    .checked_add(1)
                    .context("ephemeral durable revision overflow")?,
                None => 0,
            };
            let value = DurableValue {
                revision,
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            drop(values);
            Ok(CompareExchangeResult::Applied(value))
        })
    }
}

fn ephemeral_durable(delivery_id: &str, stage: &str) -> DurableContext {
    DurableContext {
        delivery_id: Arc::from(format!("speedtest-{stage}-{delivery_id}")),
        storage: Arc::new(EphemeralDurableStorage::default()),
        resource_storage: Arc::new(EphemeralDurableStorage::default()),
    }
}

#[cfg(test)]
#[path = "tests/speedtest.rs"]
mod tests;
