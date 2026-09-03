use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use futures_util::future::BoxFuture;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DiscoveredDataset, SchemaOrigin, SinkLimits, SourceTopology, NO_LIMITS,
};
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::EndpointDescriptor;

struct RowCountSink {
    strategy: SnapshotRowCountStrategy,
    responses: Mutex<VecDeque<Vec<SnapshotDatasetRowCount>>>,
    probes: AtomicU64,
}

impl RowCountSink {
    fn new(
        strategy: SnapshotRowCountStrategy,
        responses: impl IntoIterator<Item = Vec<SnapshotDatasetRowCount>>,
    ) -> Self {
        Self {
            strategy,
            responses: Mutex::new(responses.into_iter().collect()),
            probes: AtomicU64::new(0),
        }
    }
}

impl SinkConnector for RowCountSink {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn SinkLimits {
        &NO_LIMITS
    }

    fn destination_type(&self, _column: &SchemaColumn) -> anyhow::Result<String> {
        Ok("test".to_owned())
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn snapshot_row_count_strategy(&self) -> Option<SnapshotRowCountStrategy> {
        Some(self.strategy)
    }

    fn snapshot_row_counts<'a>(
        &'a self,
        _discovery: &'a DeliveryDiscovery,
    ) -> BoxFuture<'a, anyhow::Result<Vec<SnapshotDatasetRowCount>>> {
        Box::pin(async move {
            self.probes.fetch_add(1, Ordering::Relaxed);
            self.responses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("unexpected row-count probe"))
        })
    }

    fn build_sink(
        &self,
        _context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async { anyhow::bail!("test sink is metadata-only") })
    }
}

fn reconciliation_discovery() -> DeliveryDiscovery {
    let schema = DatasetSchema::new(Vec::new());
    DeliveryDiscovery {
        source_name: Arc::from("source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                role: DatasetRole::Main,
                name: Arc::from("events"),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from("events_dlq"),
                incoming_schema: schema.clone(),
                stored_schema: schema,
                system_columns: Vec::new(),
            },
        ],
        performance_advice: Vec::new(),
    }
}

fn destination_count(
    role: DatasetRole,
    table: &str,
    target: &str,
    exists: bool,
    rows: u64,
) -> SnapshotDatasetRowCount {
    SnapshotDatasetRowCount {
        role,
        table: Arc::from(table),
        target: Arc::from(target),
        exists,
        rows,
    }
}

fn output_count(role: DatasetRole, table: &str, rows: u64) -> OutputDatasetRowCount {
    OutputDatasetRowCount {
        table: Arc::from(table),
        is_dlq: role == DatasetRole::DeadLetterQueue,
        rows,
    }
}

#[tokio::test]
async fn startup_barrier_requires_every_partition_and_reports_early_exit() {
    let cancellation = CancellationToken::new();
    let (first, first_rx) = oneshot::channel();
    let (second, second_rx) = oneshot::channel::<()>();
    first.send(()).expect("startup receiver must be alive");
    drop(second);

    let error = wait_for_partition_startup(vec![(7, first_rx), (11, second_rx)], &cancellation)
        .await
        .expect_err("all assigned partitions must cross the construction barrier");
    assert!(error.to_string().contains("partition 11"));
    assert!(error.to_string().contains("source and sink"));
}

#[tokio::test]
async fn startup_barrier_is_cancellable() {
    let cancellation = CancellationToken::new();
    let (_sender, receiver) = oneshot::channel();
    cancellation.cancel();

    let error = wait_for_partition_startup(vec![(3, receiver)], &cancellation)
        .await
        .expect_err("cancelled startup must stop waiting");
    assert!(error.to_string().contains("cancelled"));
}

#[test]
fn retryable_partition_failures_use_capped_backoff_without_exhaustion() {
    let mut policy = PartitionRestartPolicy::new();

    for expected_failure in 1..=100 {
        let (failure, delay) = policy.record_failure(false);
        assert_eq!(failure, expected_failure);
        assert!(delay <= MAX_PARTITION_RESTART_DELAY);
    }
    assert_eq!(policy.next_delay, MAX_PARTITION_RESTART_DELAY);
}

#[test]
fn durable_progress_resets_failure_streak_and_backoff() {
    let mut policy = PartitionRestartPolicy::new();
    for _ in 0..10 {
        policy.record_failure(false);
    }

    let (failure, delay) = policy.record_failure(true);

    assert_eq!(failure, 1);
    assert_eq!(delay, INITIAL_PARTITION_RESTART_DELAY);
    for expected_failure in 2..5 {
        let (failure, _) = policy.record_failure(false);
        assert_eq!(failure, expected_failure);
    }
}

#[test]
fn finite_source_completion_is_not_restarted() {
    assert!(classify_partition_completion(Ok(()), false, true).is_none());
    assert!(classify_partition_completion(Ok(()), false, false).is_some());
}

#[test]
fn replaced_snapshot_row_counts_verify_main_and_dlq_exactly() {
    let verified = reconcile_snapshot_row_counts(
        SnapshotRowCountStrategy::ReplacedTotal,
        vec![
            output_count(DatasetRole::Main, "events", 17),
            output_count(DatasetRole::DeadLetterQueue, "events_dlq", 2),
        ],
        Vec::new(),
        vec![
            destination_count(DatasetRole::Main, "events", "//tmp/events", true, 17),
            destination_count(
                DatasetRole::DeadLetterQueue,
                "events_dlq",
                "//tmp/events_dlq",
                true,
                2,
            ),
        ],
    )
    .expect("exact replacement counts");

    assert_eq!(verified.len(), 2);
    assert_eq!(verified[0].output_rows, 17);
    assert_eq!(verified[0].destination_rows, 17);
}

#[test]
fn additive_snapshot_row_counts_include_the_persisted_baseline() {
    let verified = reconcile_snapshot_row_counts(
        SnapshotRowCountStrategy::AdditiveBaseline,
        vec![output_count(DatasetRole::Main, "events", 11)],
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "analytics.events",
            true,
            29,
        )],
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "analytics.events",
            true,
            40,
        )],
    )
    .expect("baseline plus output must match");

    assert_eq!(verified[0].destination_rows, 40);
}

#[test]
fn snapshot_row_count_mismatch_missing_target_and_overflow_fail_closed() {
    let mismatch = reconcile_snapshot_row_counts(
        SnapshotRowCountStrategy::ReplacedTotal,
        vec![output_count(DatasetRole::Main, "events", 9)],
        Vec::new(),
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "//tmp/events",
            true,
            8,
        )],
    )
    .expect_err("row loss must fail");
    assert!(mismatch.to_string().contains("expected 9 rows, found 8"));

    let missing = reconcile_snapshot_row_counts(
        SnapshotRowCountStrategy::ReplacedTotal,
        vec![output_count(DatasetRole::Main, "events", 0)],
        Vec::new(),
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "//tmp/events",
            false,
            0,
        )],
    )
    .expect_err("missing destination must fail even for an empty source");
    assert!(missing.to_string().contains("does not exist"));

    let overflow = reconcile_snapshot_row_counts(
        SnapshotRowCountStrategy::AdditiveBaseline,
        vec![output_count(DatasetRole::Main, "events", 1)],
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "analytics.events",
            true,
            u64::MAX,
        )],
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "analytics.events",
            true,
            u64::MAX,
        )],
    )
    .expect_err("expectation overflow must fail");
    assert!(overflow.to_string().contains("expectation overflow"));
}

#[test]
fn snapshot_destination_probe_must_match_discovery_exactly() {
    let discovery = reconciliation_discovery();
    let duplicate = normalize_destination_counts(
        &discovery,
        vec![
            destination_count(DatasetRole::Main, "events", "//tmp/events", true, 1),
            destination_count(DatasetRole::Main, "events", "//tmp/events", true, 1),
        ],
    )
    .expect_err("duplicate dataset must fail");
    assert!(duplicate.to_string().contains("repeated dataset"));

    let incomplete = normalize_destination_counts(
        &discovery,
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "//tmp/events",
            true,
            1,
        )],
    )
    .expect_err("missing DLQ count must fail");
    assert!(incomplete.to_string().contains("different dataset set"));

    let absent_with_rows = normalize_destination_counts(
        &DeliveryDiscovery {
            datasets: vec![discovery.datasets[0].clone()],
            ..discovery
        },
        vec![destination_count(
            DatasetRole::Main,
            "events",
            "//tmp/events",
            false,
            1,
        )],
    )
    .expect_err("absent target cannot contain rows");
    assert!(absent_with_rows.to_string().contains("absent destination"));
}

#[tokio::test]
async fn additive_baseline_is_persisted_reused_and_target_bound() {
    let discovery = reconciliation_discovery();
    let baseline = vec![
        destination_count(DatasetRole::Main, "events", "analytics.events", true, 7),
        destination_count(
            DatasetRole::DeadLetterQueue,
            "events_dlq",
            "analytics.events_dlq",
            false,
            0,
        ),
    ];
    let current = vec![
        destination_count(DatasetRole::Main, "events", "analytics.events", true, 13),
        destination_count(
            DatasetRole::DeadLetterQueue,
            "events_dlq",
            "analytics.events_dlq",
            true,
            1,
        ),
    ];
    let changed = vec![
        destination_count(DatasetRole::Main, "events", "other.events", true, 13),
        destination_count(
            DatasetRole::DeadLetterQueue,
            "events_dlq",
            "other.events_dlq",
            true,
            1,
        ),
    ];
    let sink = RowCountSink::new(
        SnapshotRowCountStrategy::AdditiveBaseline,
        [baseline.clone(), current, changed],
    );
    let durable = transferia_test_support::durable_context();

    assert_eq!(
        load_or_capture_snapshot_baseline(&sink, &discovery, &durable)
            .await
            .expect("first baseline capture"),
        baseline
    );
    assert_eq!(
        load_or_capture_snapshot_baseline(&sink, &discovery, &durable)
            .await
            .expect("persisted baseline reuse"),
        baseline,
        "current destination rows must not replace the original baseline"
    );
    let error = load_or_capture_snapshot_baseline(&sink, &discovery, &durable)
        .await
        .expect_err("changed physical destination must fail before prepare/write");
    assert!(error.to_string().contains("other.events"));
    assert_eq!(sink.probes.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn multi_worker_snapshot_does_not_claim_an_uncoordinated_global_count() {
    let discovery = Arc::new(reconciliation_discovery());
    let sink = Arc::new(RowCountSink::new(
        SnapshotRowCountStrategy::ReplacedTotal,
        Vec::new(),
    ));
    let rows = Arc::new(output_row_counts(&discovery).expect("output rows"));
    let sink_trait: Arc<dyn SinkConnector> = sink.clone();

    let reconciliation = SnapshotReconciliation::prepare(
        rows,
        sink_trait,
        discovery,
        transferia_test_support::durable_context(),
        2,
        0,
    )
    .await
    .expect("multi-worker output accounting remains available");

    assert_eq!(reconciliation.strategy, None);
    assert_eq!(sink.probes.load(Ordering::Relaxed), 0);
}

#[test]
fn corrupt_or_wrong_version_snapshot_baseline_is_rejected() {
    let discovery = reconciliation_discovery();
    assert!(decode_snapshot_baseline(b"not-json", &discovery)
        .expect_err("corrupt baseline")
        .to_string()
        .contains("corrupt"));

    let payload = serde_json::json!({
        "version": SNAPSHOT_ROW_COUNT_BASELINE_VERSION + 1,
        "datasets": []
    });
    let error = decode_snapshot_baseline(payload.to_string().as_bytes(), &discovery)
        .expect_err("unknown version must fail");
    assert!(error.to_string().contains("unsupported"));
}

#[test]
fn source_build_failure_preserves_an_explicit_fatal_disposition() {
    let fatal = DataPlaneFailure::fatal(anyhow::anyhow!("snapshot owner is gone"));
    let classified = source_build_failure(fatal.into());

    assert!(!classified.is_retryable());
    assert!(classified.to_string().contains("source creation failed"));

    let unclassified = source_build_failure(anyhow::anyhow!("connection reset"));
    assert!(unclassified.is_retryable());
}
