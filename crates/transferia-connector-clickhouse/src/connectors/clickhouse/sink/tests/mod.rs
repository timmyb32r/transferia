use core::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use super::{
    ClickHouseCompression, ClickHouseInsertFormat, ClickHouseSink, ClickHouseSinkConfig,
    InsertError, InsertTransport,
};
use super::actor::clickhouse_changelog_batches;
use crate::metrics::SinkCounters;
use transferia_core::data::changelog::{project_sink_batch, ProjectedSinkBatch};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{
    Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo,
};

#[derive(Clone, Copy)]
enum Plan {
    Success,
    Transient,
    Permanent,
}

struct FakeState {
    calls: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    inserted_rows: Mutex<Vec<usize>>,
    plans: Mutex<VecDeque<Plan>>,
    gate: Semaphore,
    block: bool,
    started: Notify,
}

struct FakeTransport {
    state: Arc<FakeState>,
}

impl FakeTransport {
    fn new(block: bool, plans: impl IntoIterator<Item = Plan>) -> (Arc<Self>, Arc<FakeState>) {
        let state = Arc::new(FakeState {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            inserted_rows: Mutex::new(Vec::new()),
            plans: Mutex::new(plans.into_iter().collect()),
            gate: Semaphore::new(0),
            block,
            started: Notify::new(),
        });
        (
            Arc::new(Self {
                state: Arc::clone(&state),
            }),
            state,
        )
    }
}

struct ActiveGuard(Arc<FakeState>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl InsertTransport for FakeTransport {
    fn insert(
        &self,
        _table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state
                .inserted_rows
                .lock()
                .expect("inserted rows poisoned")
                .push(batches.iter().map(RecordBatch::num_rows).sum());
            state.calls.fetch_add(1, Ordering::AcqRel);
            let active = state.active.fetch_add(1, Ordering::AcqRel) + 1;
            state.max_active.fetch_max(active, Ordering::AcqRel);
            let _guard = ActiveGuard(Arc::clone(&state));
            state.started.notify_waiters();
            if state.block {
                state
                    .gate
                    .acquire()
                    .await
                    .expect("test gate closed")
                    .forget();
            }
            let plan = state
                .plans
                .lock()
                .expect("plans poisoned")
                .pop_front()
                .unwrap_or(Plan::Success);
            match plan {
                Plan::Success => Ok(()),
                Plan::Transient => Err(InsertError::Transient(anyhow::anyhow!("temporary"))),
                Plan::Permanent => Err(InsertError::Permanent(anyhow::anyhow!("permanent"))),
            }
        })
    }
}

fn config() -> ClickHouseSinkConfig {
    ClickHouseSinkConfig {
        hosts: vec!["unused".into()],
        port: 9000,
        http_port: 8123,
        trusted_plaintext: true,
        tls_ca_file: None,
        data_host_count: None,
        database: "default".into(),
        username: "default".into(),
        password: String::new(),
        shard_group: String::new(),
        insert_target_rows: 1,
        insert_target_bytes: usize::MAX,
        insert_concurrency: 1,
        insert_format: ClickHouseInsertFormat::Native,
        compression: ClickHouseCompression::default(),
        format_threads: 8,
        parquet_row_group_rows: 1_000_000,
        async_insert: false,
        flush_interval_ms: 100,
        retry_initial_ms: 10,
        retry_max_ms: 100,
        retry_max_attempts: None,
        connect_timeout_ms: 30_000,
        request_timeout_ms: 30_000,
    }
}

fn delivery(memory: &PipelineMemory, id: u64, tables: &[&str]) -> Delivery {
    let mut outputs = Vec::new();
    for table in tables {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![id.cast_signed()]))],
        )
        .unwrap();
        let bytes = batch.get_array_memory_size();
        outputs.push(SinkBatch {
            table: Arc::from(*table),
            is_dlq: table.ends_with("_dlq"),
            batch,
            byte_size: bytes,
            memory: memory.reserve_transform(bytes),
            system_columns: transferia_core::data::system_columns::SystemColumns::default(),
        });
    }
    Delivery {
        id: DeliveryId::new(id),
        outputs,
        meta: DeliveryMeta { source_messages: 1 },
    }
}

fn discovery() -> Arc<DeliveryDiscovery> {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "value".into(),
        DataType::Int64,
        false,
    )]);
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("source-topic"),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserProjection,
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
    })
}

fn spawn_sink(
    transport: Arc<dyn InsertTransport>,
    memory: PipelineMemory,
    counters: Arc<SinkCounters>,
) -> (
    mpsc::Sender<Delivery>,
    mpsc::Receiver<SinkEvent>,
    CancellationToken,
    JoinHandle<transferia_core::failure::DataPlaneResult<()>>,
) {
    spawn_sink_with_config(config(), transport, memory, counters)
}

fn spawn_sink_with_config(
    config: ClickHouseSinkConfig,
    transport: Arc<dyn InsertTransport>,
    memory: PipelineMemory,
    counters: Arc<SinkCounters>,
) -> (
    mpsc::Sender<Delivery>,
    mpsc::Receiver<SinkEvent>,
    CancellationToken,
    JoinHandle<transferia_core::failure::DataPlaneResult<()>>,
) {
    let sink = ClickHouseSink::with_transport(config, counters, transport, discovery());
    let (delivery_tx, delivery_rx) = mpsc::channel(8);
    let (event_tx, event_rx) = mpsc::channel(8);
    let cancellation = CancellationToken::new();
    let io = SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory,
        cancellation: cancellation.clone(),
    };
    let task = tokio::spawn(Box::new(sink).run(io));
    (delivery_tx, event_rx, cancellation, task)
}

async fn wait_calls(state: &FakeState, calls: usize) {
    while state.calls.load(Ordering::Acquire) < calls {
        tokio::task::yield_now().await;
    }
}

fn changelog_discovery() -> DeliveryDiscovery {
    let id = SchemaColumn::new("id".into(), DataType::Int64, false)
        .with_constraints(true, false, None);
    let value = SchemaColumn::new("value".into(), DataType::Int64, true);
    let operation = SchemaColumn::new(
        SystemColumnKind::ChangeOperation.default_name().into(),
        SystemColumnKind::ChangeOperation.data_type(),
        false,
    );
    let offset = SchemaColumn::new(
        SystemColumnKind::Offset.default_name().into(),
        SystemColumnKind::Offset.data_type(),
        false,
    );
    DeliveryDiscovery {
        source_name: Arc::from("postgres"),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: true,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(vec![
                id.clone(),
                value.clone(),
                operation,
                offset,
            ]),
            stored_schema: DatasetSchema::new(vec![id, value]),
            system_columns: vec![
                SystemColumnKind::ChangeOperation.into(),
                SystemColumnKind::Offset.into(),
            ],
        }],
        performance_advice: Vec::new(),
    }
}

async fn changelog_sink_batch(
    operations: &[&str],
    ids: &[i64],
    values: &[i64],
    versions: &[i64],
) -> anyhow::Result<SinkBatch> {
    let discovery = changelog_discovery();
    let fields = discovery.datasets[0]
        .incoming_schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(StringArray::from(operations.to_vec())),
            Arc::new(Int64Array::from(versions.to_vec())),
        ],
    )?;
    Ok(SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        byte_size: batch.get_array_memory_size(),
        batch,
        memory: PipelineMemory::new(1_000_000).reserve(1).await,
        system_columns: SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
                index: 2,
            },
            SystemColumn {
                kind: SystemColumnKind::Offset,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
                index: 3,
            },
        ]),
    })
}

#[tokio::test]
async fn changelog_collapses_same_lsn_changes_and_writes_pk_tombstones() -> anyhow::Result<()> {
    let input = changelog_sink_batch(
        &["c", "u", "c", "d"],
        &[1, 1, 2, 2],
        &[10, 11, 20, 20],
        &[42, 42, 42, 42],
    )
    .await?;
    let ProjectedSinkBatch::Changelog(changelog) =
        project_sink_batch(&changelog_discovery(), &input)?
    else {
        panic!("CDC operation metadata must produce a changelog batch")
    };
    let batches = clickhouse_changelog_batches(&changelog)?;

    assert_eq!(batches.len(), 2);
    let upsert = &batches[0];
    assert_eq!(upsert.num_rows(), 1);
    assert_eq!(upsert.num_columns(), 4);
    assert_eq!(
        upsert.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        1
    );
    assert_eq!(
        upsert.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        11
    );
    assert_eq!(
        upsert.column(2).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
        43
    );
    assert_eq!(
        upsert.column(3).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
        0
    );

    let tombstone = &batches[1];
    assert_eq!(tombstone.num_rows(), 1);
    assert_eq!(tombstone.num_columns(), 3);
    assert_eq!(
        tombstone.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        2
    );
    assert_eq!(
        tombstone.column(1).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
        43
    );
    assert_eq!(
        tombstone.column(2).as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
        43
    );
    assert!(tombstone.schema().field_with_name("value").is_err());
    assert!(tombstone
        .schema()
        .field_with_name(SystemColumnKind::ChangeOperation.default_name())
        .is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn full_buffer_starts_immediately_after_the_active_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(true, []);
    let (tx, mut events, cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::clone(&counters));

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    tx.send(delivery(&memory, 2, &["events"])).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(state.calls.load(Ordering::Acquire), 1);
    assert_eq!(state.max_active.load(Ordering::Acquire), 1);

    state.gate.add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    wait_calls(&state, 2).await;
    state.gate.add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
    );
    assert_eq!(state.max_active.load(Ordering::Acquire), 1);
    assert_eq!(counters.flushes_total(), 2);
    assert_eq!(counters.source_messages_total(), 2);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn explicit_insert_concurrency_uses_parallel_connections_and_commits_a_prefix() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(true, []);
    let mut sink_config = config();
    sink_config.insert_concurrency = 2;
    let (tx, mut events, cancellation, task) = spawn_sink_with_config(
        sink_config,
        transport,
        memory.clone(),
        Arc::clone(&counters),
    );

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    tx.send(delivery(&memory, 2, &["events"])).await.unwrap();
    wait_calls(&state, 2).await;
    assert_eq!(state.max_active.load(Ordering::Acquire), 2);
    assert!(events.try_recv().is_err());

    state.gate.add_permits(2);
    let mut committed_through = DeliveryId::new(0);
    while committed_through != DeliveryId::new(2) {
        let Some(SinkEvent::CommittedThrough(delivery_id)) = events.recv().await else {
            panic!("ClickHouse sink stopped before committing both concurrent INSERTs");
        };
        committed_through = delivery_id;
    }
    assert_eq!(counters.flushes_total(), 2);
    assert_eq!(counters.source_messages_total(), 2);

    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn backlog_is_split_at_the_configured_insert_target() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(true, []);
    let (tx, mut events, cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::clone(&counters));

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    tx.send(delivery(&memory, 2, &["events"])).await.unwrap();
    tx.send(delivery(&memory, 3, &["events"])).await.unwrap();
    tokio::task::yield_now().await;

    for expected_call in 2..=3 {
        state.gate.add_permits(1);
        events.recv().await.unwrap();
        wait_calls(&state, expected_call).await;
    }
    state.gate.add_permits(1);
    events.recv().await.unwrap();

    assert_eq!(
        *state.inserted_rows.lock().expect("inserted rows poisoned"),
        vec![1, 1, 1]
    );
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn low_volume_delivery_flushes_at_interval() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(false, []);
    let mut low_volume = config();
    low_volume.insert_target_rows = 1_000;
    let (tx, mut events, cancellation, task) =
        spawn_sink_with_config(low_volume, transport, memory.clone(), Arc::clone(&counters));

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    tokio::time::advance(Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    wait_calls(&state, 1).await;
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    assert_eq!(counters.flushes_total(), 1);
    assert_eq!(counters.retries_total(), 0);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancellation_aborts_and_joins_an_active_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) = FakeTransport::new(true, []);
    let (tx, _events, cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::new(SinkCounters::new()));
    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    assert_eq!(state.active.load(Ordering::Acquire), 1);

    cancellation.cancel();
    task.await.unwrap().unwrap();
    assert_eq!(state.active.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn delivery_error_does_not_detach_an_active_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) = FakeTransport::new(true, []);
    let (tx, _events, _cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::new(SinkCounters::new()));
    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;

    tx.send(delivery(&memory, 3, &["events"])).await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("delivery order"), "{error:#}");
    assert_eq!(state.active.load(Ordering::Acquire), 0);
}

#[tokio::test(start_paused = true)]
async fn low_volume_tables_share_the_same_flush_deadline() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(false, []);
    let mut low_volume = config();
    low_volume.insert_target_rows = 1_000;
    let (tx, mut events, cancellation, task) =
        spawn_sink_with_config(low_volume, transport, memory.clone(), counters);

    tx.send(delivery(&memory, 1, &["events", "events_dlq"]))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(100)).await;
    wait_calls(&state, 2).await;
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn full_pipeline_budget_requests_an_immediate_insert() {
    let memory = PipelineMemory::new(1);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(false, []);
    let mut high_targets = config();
    high_targets.insert_target_rows = 1_000;
    let (tx, mut events, cancellation, task) =
        spawn_sink_with_config(high_targets, transport, memory.clone(), counters);

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn source_read_credit_does_not_force_an_immediate_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let source_credit = memory.reserve_progress_source(1_000_000).await;
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(false, []);
    let mut high_targets = config();
    high_targets.insert_target_rows = 1_000;
    let (tx, mut events, cancellation, task) =
        spawn_sink_with_config(high_targets, transport, memory.clone(), counters);

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    tokio::time::advance(Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    wait_calls(&state, 1).await;
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    drop(source_credit);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn multi_table_delivery_commits_only_after_both_inserts() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(true, []);
    let (tx, mut events, cancellation, task) = spawn_sink(transport, memory.clone(), counters);
    tx.send(delivery(&memory, 1, &["events", "events_dlq"]))
        .await
        .unwrap();
    wait_calls(&state, 1).await;
    assert!(events.try_recv().is_err());
    state.gate.add_permits(1);
    wait_calls(&state, 2).await;
    assert!(events.try_recv().is_err());
    state.gate.add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn transient_error_retries_frozen_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let counters = Arc::new(SinkCounters::new());
    let (transport, state) = FakeTransport::new(false, [Plan::Transient, Plan::Success]);
    let (tx, mut events, cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::clone(&counters));
    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    assert!(events.try_recv().is_err());
    tokio::time::advance(Duration::from_millis(12)).await;
    wait_calls(&state, 2).await;
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    assert_eq!(counters.flushes_total(), 1);
    assert_eq!(counters.retries_total(), 1);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn transient_error_stops_at_retry_limit() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) =
        FakeTransport::new(false, [Plan::Transient, Plan::Transient, Plan::Success]);
    let mut limited = config();
    limited.retry_max_attempts = Some(2);
    let (tx, mut events, _cancellation, task) = spawn_sink_with_config(
        limited,
        transport,
        memory.clone(),
        Arc::new(SinkCounters::new()),
    );

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    tokio::time::advance(Duration::from_millis(12)).await;
    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(failure.is_retryable());
    assert_eq!(state.calls.load(Ordering::Acquire), 2);
    assert!(events.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn hanging_insert_stops_at_attempt_deadline() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) = FakeTransport::new(true, []);
    let mut limited = config();
    limited.request_timeout_ms = 10;
    limited.retry_max_attempts = Some(1);
    let (tx, mut events, _cancellation, task) = spawn_sink_with_config(
        limited,
        transport,
        memory.clone(),
        Arc::new(SinkCounters::new()),
    );

    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    wait_calls(&state, 1).await;
    tokio::time::advance(Duration::from_millis(10)).await;

    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(failure.is_retryable());
    assert!(format!("{error:#}").contains("result is ambiguous"));
    assert_eq!(state.active.load(Ordering::Acquire), 0);
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn permanent_error_is_fatal_and_never_commits() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, _state) = FakeTransport::new(false, [Plan::Permanent]);
    let (tx, mut events, _cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::new(SinkCounters::new()));
    tx.send(delivery(&memory, 1, &["events"])).await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn delivery_mismatch_is_fatal_before_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) = FakeTransport::new(false, []);
    let (tx, mut events, _cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::new(SinkCounters::new()));

    tx.send(delivery(&memory, 1, &["unexpected_table"]))
        .await
        .unwrap();
    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(format!("{error:#}").contains("has no Main dataset named 'unexpected_table'"));
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn schema_mismatch_is_fatal_before_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) = FakeTransport::new(false, []);
    let (tx, mut events, _cancellation, task) =
        spawn_sink(transport, memory.clone(), Arc::new(SinkCounters::new()));
    let mut invalid = delivery(&memory, 1, &["events"]);
    let values = Arc::clone(invalid.outputs[0].batch.column(0));
    invalid.outputs[0].batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "renamed_value",
            DataType::Int64,
            false,
        )])),
        vec![values],
    )
    .unwrap();

    tx.send(invalid).await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(format!("{error:#}").contains("column 0"));
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn empty_delivery_commits_without_insert() {
    let memory = PipelineMemory::new(1_000_000);
    let (transport, state) = FakeTransport::new(false, []);
    let (tx, mut events, cancellation, task) =
        spawn_sink(transport, memory, Arc::new(SinkCounters::new()));
    tx.send(Delivery {
        id: DeliveryId::new(1),
        outputs: Vec::new(),
        meta: DeliveryMeta { source_messages: 1 },
    })
    .await
    .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    assert_eq!(state.calls.load(Ordering::Acquire), 0);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}
