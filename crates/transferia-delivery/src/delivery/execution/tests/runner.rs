use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DiscoveredDataset, SchemaOrigin, SinkLimits, SourceTopology, NO_LIMITS,
};
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::sink::{Sink, SinkEvent, SinkIo};
use transferia_core::source::{CommitMarker, Source};
use transferia_delivery_contracts::parser::{ParserFactory, ParserSession};
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::SourceDiscoveryContext;

struct RowCountSink {
    strategy: SnapshotRowCountStrategy,
    responses: Mutex<VecDeque<Vec<SnapshotDatasetRowCount>>>,
    probes: AtomicU64,
}

#[derive(Default)]
struct PhaseEvents {
    entries: Mutex<Vec<String>>,
    changed: Notify,
    completion_gate: Mutex<Option<Arc<Notify>>>,
}

impl PhaseEvents {
    fn push(&self, event: impl Into<String>) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.into());
        self.changed.notify_waiters();
    }

    fn snapshot(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn wait_for(&self, expected: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                if self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .any(|entry| entry == expected)
                {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for phase event '{expected}'"));
    }

    fn hold_phase_completion(&self, gate: Arc<Notify>) {
        *self
            .completion_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
    }

    fn phase_completion_gate(&self) -> Option<Arc<Notify>> {
        self.completion_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[derive(Clone)]
enum SnapshotRead {
    Finish,
    Wait(Arc<BTreeMap<i64, Arc<Notify>>>),
    Fatal,
}

struct PhaseSourceConnector {
    phases: Vec<SourceExecutionPhase>,
    prepared: Option<PreparedSourceExecution>,
    snapshot_read: SnapshotRead,
    events: Arc<PhaseEvents>,
}

impl SourceConnector for PhaseSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::DataGenerator(SourceDescriptor {
            behavior: SourceBehavior::ChangelogRows,
            delivery_modes: SourceDeliveryModes::BATCH_AND_STREAM,
        })
    }

    fn delivery_discovery(
        &self,
        _context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async { anyhow::bail!("unused discovery") })
    }

    fn prepare_execution(
        &self,
        context: SourceExecutionContext,
    ) -> BoxFuture<'_, anyhow::Result<Option<PreparedSourceExecution>>> {
        let prepared = self.prepared.clone();
        self.events.push("prepare_execution");
        Box::pin(async move {
            anyhow::ensure!(
                context.replay_identity.as_deref() == Some("phase-runner-test-revision-1"),
                "runner did not pass the plan replay identity to source preparation"
            );
            Ok(prepared)
        })
    }

    fn execution_phases(
        &self,
        _delivery_type: DeliveryType,
        _discovery: &DeliveryDiscovery,
    ) -> anyhow::Result<Vec<SourceExecutionPhase>> {
        Ok(self.phases.clone())
    }

    fn complete_execution_phase(
        &self,
        phase: SourcePhase,
        _durable: DurableContext,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let gate = self.events.phase_completion_gate();
        self.events.push(format!("complete:{phase:?}"));
        Box::pin(async move {
            anyhow::ensure!(
                !cancellation.is_cancelled(),
                "phase completion barrier received a cancelled token"
            );
            if let Some(gate) = gate {
                gate.notified().await;
            }
            Ok(())
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        let replay_identity = context.replay_identity.clone();
        self.events.push(format!(
            "build:{:?}:{}",
            context.phase, context.partition_id
        ));
        let read = match context.phase {
            SourcePhase::Snapshot => match &self.snapshot_read {
                SnapshotRead::Finish => PhaseSourceRead::Finish,
                SnapshotRead::Wait(gates) => PhaseSourceRead::Wait(Arc::clone(
                    gates
                        .get(&context.partition_id)
                        .expect("test snapshot partition must have a gate"),
                )),
                SnapshotRead::Fatal => PhaseSourceRead::Fatal,
            },
            SourcePhase::Stream => PhaseSourceRead::Stream,
        };
        let source = PhaseSource {
            phase: context.phase,
            partition_id: context.partition_id,
            read,
            events: Arc::clone(&self.events),
        };
        Box::pin(async move {
            anyhow::ensure!(
                replay_identity.as_deref() == Some("phase-runner-test-revision-1"),
                "runner did not pass the plan replay identity to source construction"
            );
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }

    fn parser(&self) -> Arc<dyn ParserFactory> {
        Arc::new(UnusedParserFactory)
    }

    fn parses_rows(&self) -> bool {
        true
    }
}

enum PhaseSourceRead {
    Finish,
    Wait(Arc<Notify>),
    Fatal,
    Stream,
}

struct PhaseSource {
    phase: SourcePhase,
    partition_id: i64,
    read: PhaseSourceRead,
    events: Arc<PhaseEvents>,
}

impl Source for PhaseSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            match &self.read {
                PhaseSourceRead::Finish => {}
                PhaseSourceRead::Wait(gate) => gate.notified().await,
                PhaseSourceRead::Fatal => {
                    return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                        "injected snapshot failure"
                    )));
                }
                PhaseSourceRead::Stream => {
                    std::future::pending::<()>().await;
                }
            }
            self.events
                .push(format!("finish:{:?}:{}", self.phase, self.partition_id));
            Ok(SourceBatch::Finished)
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct UnusedParserFactory;

impl ParserFactory for UnusedParserFactory {
    fn create_session(self: Arc<Self>, _memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        panic!("phase lifecycle tests emit no parser input")
    }
}

struct PhaseSinkConnector {
    prepare_calls: AtomicU64,
    events: Arc<PhaseEvents>,
}

impl PhaseSinkConnector {
    fn new(events: Arc<PhaseEvents>) -> Self {
        Self {
            prepare_calls: AtomicU64::new(0),
            events,
        }
    }
}

impl SinkConnector for PhaseSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn SinkLimits {
        &NO_LIMITS
    }

    fn destination_type(&self, _column: &SchemaColumn) -> anyhow::Result<String> {
        Ok("discard".to_owned())
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        self.prepare_calls.fetch_add(1, Ordering::Relaxed);
        self.events.push("sink_prepare");
        Box::pin(async { Ok(()) })
    }

    fn build_sink(
        &self,
        _context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async { Ok(Box::new(PhaseSink) as Box<dyn Sink>) })
    }
}

struct PhaseSink;

impl Sink for PhaseSink {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(async move {
            loop {
                let delivery = tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv() => delivery,
                };
                let Some(delivery) = delivery else {
                    return Ok(());
                };
                let id = delivery.id;
                drop(delivery);
                io.events
                    .send(SinkEvent::CommittedThrough(id))
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::retryable(anyhow::anyhow!(
                            "phase test sink event receiver closed"
                        ))
                    })?;
            }
        })
    }
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

fn phase_discovery(topology: SourceTopology) -> DeliveryDiscovery {
    let schema = DatasetSchema::new(Vec::new());
    DeliveryDiscovery {
        source_name: Arc::from("phase-source"),
        source_topology: topology,
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: true,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    }
}

fn phase_plan(phase: SourcePhase, topology: SourceTopology, finite: bool) -> SourceExecutionPhase {
    SourceExecutionPhase {
        phase,
        topology,
        finite,
    }
}

fn phase_pipeline_plan(
    source: Arc<PhaseSourceConnector>,
    sink: Arc<PhaseSinkConnector>,
    discovery: DeliveryDiscovery,
) -> PipelinePlan {
    let config = serde_yaml::from_str(
        r"
delivery_id: phase-runner-test
delivery_name: Test delivery
durable_storage:
  type: local_file
  path: /tmp/phase-runner-test-unused
delivery_type: batch_and_stream
source:
  test: {}
sink:
  test: {}
pipeline_memory_limit_bytes: 1048576
",
    )
    .expect("phase runner test config");
    let semantics = transferia_delivery_contracts::semantics::validate_pipeline(
        &source.compatibility(),
        &sink.compatibility(),
        &discovery,
        true,
    );
    PipelinePlan {
        config,
        replay_identity: Some(Arc::from("phase-runner-test-revision-1")),
        durable: transferia_test_support::durable_context(),
        metrics_registry: Arc::new(MetricsRegistry::new()),
        source_kind: "test".to_owned(),
        sink_kind: "test".to_owned(),
        source_connector: source,
        sink_connector: sink,
        discovery: Arc::new(discovery),
        middlewares: Vec::new(),
        semantics,
        finite_source: false,
    }
}

#[tokio::test]
async fn authoritative_discovery_is_revalidated_before_sink_prepare() {
    let events = Arc::new(PhaseEvents::default());
    let topology = SourceTopology::StaticPartitions(vec![0]);
    let mut invalid_authoritative = phase_discovery(topology.clone());
    invalid_authoritative.keep_system_columns = false;
    let snapshot = phase_plan(SourcePhase::Snapshot, topology.clone(), true);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![snapshot.clone()],
        prepared: Some(PreparedSourceExecution {
            discovery: invalid_authoritative,
            remaining_phases: vec![snapshot],
        }),
        snapshot_read: SnapshotRead::Finish,
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let error = start_pipeline(
        phase_pipeline_plan(source, Arc::clone(&sink), phase_discovery(topology)),
        1,
        0,
        CancellationToken::new(),
    )
    .await
    .err()
    .expect("authoritative discovery must be rejected before sink preparation");

    assert!(error.to_string().contains("system-column policy"));
    assert_eq!(sink.prepare_calls.load(Ordering::Relaxed), 0);
    assert_eq!(events.snapshot(), vec!["prepare_execution".to_owned()]);
}

#[tokio::test]
async fn prepared_execution_may_resume_from_an_exact_phase_suffix() {
    let events = Arc::new(PhaseEvents::default());
    let topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
    let discovery = phase_discovery(topology.clone());
    let snapshot = phase_plan(SourcePhase::Snapshot, topology.clone(), true);
    let stream = phase_plan(SourcePhase::Stream, topology.clone(), false);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![snapshot, stream.clone()],
        prepared: Some(PreparedSourceExecution {
            discovery: discovery.clone(),
            remaining_phases: vec![stream],
        }),
        snapshot_read: SnapshotRead::Finish,
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let cancellation = CancellationToken::new();
    let execution = start_pipeline(
        phase_pipeline_plan(source, sink, discovery),
        2,
        0,
        cancellation.clone(),
    )
    .await
    .expect("exact stream-only suffix must resume")
    .expect("co-located worker owns the remaining phase");

    let started = events.snapshot();
    assert!(started.iter().any(|event| event == "build:Stream:0"));
    assert!(!started
        .iter()
        .any(|event| event.starts_with("build:Snapshot")));

    cancellation.cancel();
    execution
        .wait()
        .await
        .expect("resumed stream cancellation must stop cleanly");
}

#[tokio::test]
async fn prepared_execution_rejects_a_non_suffix_before_sink_prepare() {
    let events = Arc::new(PhaseEvents::default());
    let topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
    let discovery = phase_discovery(topology.clone());
    let snapshot = phase_plan(SourcePhase::Snapshot, topology.clone(), true);
    let stream = phase_plan(SourcePhase::Stream, topology, false);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![snapshot.clone(), stream],
        prepared: Some(PreparedSourceExecution {
            discovery: discovery.clone(),
            remaining_phases: vec![snapshot],
        }),
        snapshot_read: SnapshotRead::Finish,
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let error = start_pipeline(
        phase_pipeline_plan(source, Arc::clone(&sink), discovery),
        1,
        0,
        CancellationToken::new(),
    )
    .await
    .err()
    .expect("a phase prefix must not be accepted as resumable state");

    assert!(error.to_string().contains("not an exact suffix"));
    assert_eq!(sink.prepare_calls.load(Ordering::Relaxed), 0);
    assert_eq!(events.snapshot(), vec!["prepare_execution".to_owned()]);
}

#[tokio::test]
async fn every_snapshot_partition_finishes_before_phase_completion_and_stream_build() {
    let events = Arc::new(PhaseEvents::default());
    let first_gate = Arc::new(Notify::new());
    let second_gate = Arc::new(Notify::new());
    let gates = Arc::new(BTreeMap::from([
        (0, Arc::clone(&first_gate)),
        (1, Arc::clone(&second_gate)),
    ]));
    let topology = SourceTopology::CoLocatedStaticPartitions(vec![0, 1]);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![
            phase_plan(SourcePhase::Snapshot, topology.clone(), true),
            phase_plan(SourcePhase::Stream, topology.clone(), false),
        ],
        prepared: None,
        snapshot_read: SnapshotRead::Wait(gates),
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let cancellation = CancellationToken::new();
    let execution = start_pipeline(
        phase_pipeline_plan(source, sink, phase_discovery(topology)),
        1,
        0,
        cancellation.clone(),
    )
    .await
    .expect("snapshot phase startup")
    .expect("worker owns both phases");
    let wait = tokio::spawn(execution.wait());

    first_gate.notify_one();
    events.wait_for("finish:Snapshot:0").await;
    let halfway = events.snapshot();
    assert!(!halfway.iter().any(|event| event == "complete:Snapshot"));
    assert!(!halfway.iter().any(|event| event == "build:Stream:0"));

    second_gate.notify_one();
    events.wait_for("build:Stream:0").await;
    let completed = events.snapshot();
    let first_finished = completed
        .iter()
        .position(|event| event == "finish:Snapshot:0")
        .expect("first snapshot partition completion");
    let second_finished = completed
        .iter()
        .position(|event| event == "finish:Snapshot:1")
        .expect("second snapshot partition completion");
    let phase_completed = completed
        .iter()
        .position(|event| event == "complete:Snapshot")
        .expect("snapshot phase completion");
    let stream_built = completed
        .iter()
        .position(|event| event == "build:Stream:0")
        .expect("stream construction");
    assert!(first_finished < phase_completed);
    assert!(second_finished < phase_completed);
    assert!(phase_completed < stream_built);

    cancellation.cancel();
    wait.await
        .expect("phase wait task")
        .expect("phase execution cancellation");
}

#[tokio::test]
async fn snapshot_failure_never_completes_the_phase_or_starts_stream() {
    let events = Arc::new(PhaseEvents::default());
    let topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![
            phase_plan(SourcePhase::Snapshot, topology.clone(), true),
            phase_plan(SourcePhase::Stream, topology.clone(), false),
        ],
        prepared: None,
        snapshot_read: SnapshotRead::Fatal,
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let execution = start_pipeline(
        phase_pipeline_plan(source, sink, phase_discovery(topology)),
        1,
        0,
        CancellationToken::new(),
    )
    .await
    .expect("snapshot phase startup")
    .expect("worker owns both phases");
    let error = execution
        .wait()
        .await
        .expect_err("fatal snapshot read must fail the phase");

    assert!(format!("{error:#}").contains("injected snapshot failure"));
    let events = events.snapshot();
    assert!(!events.iter().any(|event| event == "complete:Snapshot"));
    assert!(!events.iter().any(|event| event.starts_with("build:Stream")));
}

#[tokio::test]
async fn snapshot_cancellation_never_completes_the_phase_or_starts_stream() {
    let events = Arc::new(PhaseEvents::default());
    let gate = Arc::new(Notify::new());
    let topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![
            phase_plan(SourcePhase::Snapshot, topology.clone(), true),
            phase_plan(SourcePhase::Stream, topology.clone(), false),
        ],
        prepared: None,
        snapshot_read: SnapshotRead::Wait(Arc::new(BTreeMap::from([(0, gate)]))),
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let cancellation = CancellationToken::new();
    let execution = start_pipeline(
        phase_pipeline_plan(source, sink, phase_discovery(topology)),
        1,
        0,
        cancellation.clone(),
    )
    .await
    .expect("snapshot phase startup")
    .expect("worker owns both phases");

    cancellation.cancel();
    execution
        .wait()
        .await
        .expect("cancellation must stop the current phase cleanly");
    let events = events.snapshot();
    assert!(!events.iter().any(|event| event == "complete:Snapshot"));
    assert!(!events.iter().any(|event| event.starts_with("build:Stream")));
}

#[tokio::test]
async fn cancellation_after_snapshot_drain_waits_for_phase_barrier_and_skips_stream() {
    let events = Arc::new(PhaseEvents::default());
    let completion_gate = Arc::new(Notify::new());
    events.hold_phase_completion(Arc::clone(&completion_gate));
    let topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
    let source = Arc::new(PhaseSourceConnector {
        phases: vec![
            phase_plan(SourcePhase::Snapshot, topology.clone(), true),
            phase_plan(SourcePhase::Stream, topology.clone(), false),
        ],
        prepared: None,
        snapshot_read: SnapshotRead::Finish,
        events: Arc::clone(&events),
    });
    let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
    let cancellation = CancellationToken::new();
    let execution = start_pipeline(
        phase_pipeline_plan(source, sink, phase_discovery(topology)),
        1,
        0,
        cancellation.clone(),
    )
    .await
    .expect("snapshot phase startup")
    .expect("worker owns both phases");
    let wait = tokio::spawn(execution.wait());

    events.wait_for("complete:Snapshot").await;
    cancellation.cancel();
    completion_gate.notify_one();
    wait.await
        .expect("phase wait task")
        .expect("completed snapshot barrier must survive cancellation");

    let events = events.snapshot();
    assert!(events.iter().any(|event| event == "complete:Snapshot"));
    assert!(!events.iter().any(|event| event.starts_with("build:Stream")));
}

#[tokio::test]
async fn multi_phase_static_partitions_fail_before_external_side_effects() {
    for worker_index in 0..2 {
        let events = Arc::new(PhaseEvents::default());
        let source = Arc::new(PhaseSourceConnector {
            phases: vec![
                phase_plan(
                    SourcePhase::Snapshot,
                    SourceTopology::StaticPartitions(vec![0, 1]),
                    true,
                ),
                phase_plan(
                    SourcePhase::Stream,
                    SourceTopology::StaticPartitions(vec![0, 1]),
                    false,
                ),
            ],
            prepared: None,
            snapshot_read: SnapshotRead::Finish,
            events: Arc::clone(&events),
        });
        let sink = Arc::new(PhaseSinkConnector::new(Arc::clone(&events)));
        let error = start_pipeline(
            phase_pipeline_plan(
                source,
                Arc::clone(&sink),
                phase_discovery(SourceTopology::StaticPartitions(vec![0])),
            ),
            2,
            worker_index,
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("multi-worker phases require a real distributed barrier");

        assert!(error
            .to_string()
            .contains("requires CoLocatedStaticPartitions"));
        assert_eq!(sink.prepare_calls.load(Ordering::Relaxed), 0);
        assert!(events.snapshot().is_empty());
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
