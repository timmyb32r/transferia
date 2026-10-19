use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use transferia_core::delivery::UpdatePolicy;
use transferia_core::{DatasetRole, DatasetSchema, DeliveryDiscovery};

fn dataset() -> DiscoveredDataset {
    DiscoveredDataset {
        namespace: Some(Arc::from("db")),
        name: Arc::from("new_table"),
        role: DatasetRole::Main,
        update_policy: UpdatePolicy::Strict,
        incoming_schema: DatasetSchema::default(),
        stored_schema: DatasetSchema::default(),
        system_columns: vec![],
    }
}

struct Actor(Arc<AtomicBool>);
impl Sink for Actor {
    fn update_discovery(&mut self, discovery: std::sync::Arc<transferia_core::DeliveryDiscovery>) -> transferia_core::failure::DataPlaneResult<()> {
        drop(discovery);
        Ok(())
    }

    fn run_session(&mut self, mut io: SinkIo) -> BoxFuture<'_, DataPlaneResult<()>> {
        Box::pin(async move {
            while let Some(delivery) = io.deliveries.recv().await {
                io.events
                    .send(SinkEvent::CommittedThrough(delivery.id))
                    .await
                    .unwrap();
            }
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct Admission {
    old_drained: Arc<AtomicBool>,
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}
impl DatasetAdmission for Option<Admission> {
    fn pending_datasets(&self, _: &Delivery) -> DataPlaneResult<Vec<DiscoveredDataset>> { Ok(vec![]) }
    fn prepare(&mut self, _: DiscoveredDataset, _: DatasetStage) -> BoxFuture<'_, DataPlaneResult<Arc<DeliveryDiscovery>>> {
        let admission = self.take().unwrap();
        Box::pin(async move {
            assert!(admission.old_drained.load(Ordering::SeqCst));
            admission.entered.send(()).unwrap();
            admission.release.await.unwrap();
            Ok(Arc::new(DeliveryDiscovery {
                source_name: Arc::from("test"),
                source_topology: transferia_core::SourceTopology::StaticPartitions(vec![0]),
                schema_origin: transferia_core::SchemaOrigin::SourceNative,
                keep_system_columns: false,
                datasets: vec![dataset()],
                performance_advice: vec![],
            }))
        })
    }
}

#[tokio::test]
async fn admission_drains_session_without_recreating_sink_or_acknowledging_before_preparation() {
    let drained = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (input_tx, input_rx) = mpsc::channel(2);
    let (events_tx, mut events_rx) = mpsc::channel(2);
    let task = tokio::spawn(run(
        Box::new(Actor(drained.clone())),
        Box::new(Some(Admission {
            old_drained: drained,
            entered: entered_tx,
            release: release_rx,
        })),
        input_rx,
        events_tx,
        PipelineMemory::new(1024 * 1024),
        CancellationToken::new(),
    ));
    input_tx
        .send(SinkInput::Dataset {
            id: DeliveryId::new(1),
            dataset: dataset(),
        })
        .await
        .unwrap();
    entered_rx.await.unwrap();
    assert!(events_rx.try_recv().is_err());
    release_tx.send(()).unwrap();
    assert!(
        matches!(events_rx.recv().await, Some(SinkEvent::CommittedThrough(id)) if id == DeliveryId::new(1))
    );
    drop(input_tx);
    task.await.unwrap().unwrap();
}

struct BarrierSource {
    emitted: bool,
    committed: Arc<AtomicBool>,
    reread: Arc<AtomicBool>,
}
impl transferia_core::Source for BarrierSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<transferia_core::SourceBatch>> {
        Box::pin(async move {
            if self.emitted {
                self.reread.store(true, Ordering::SeqCst);
                assert!(self.committed.load(Ordering::SeqCst));
                return Ok(transferia_core::SourceBatch::Finished);
            }
            self.emitted = true;
            Ok(transferia_core::SourceBatch::Dataset {
                dataset: Box::new(dataset()),
                commit_marker: transferia_core::CommitMarker::new(1_u8),
                memory: vec![],
            })
        })
    }
    fn commit_offsets<'a>(
        &'a mut self,
        markers: &'a [transferia_core::CommitMarker],
    ) -> BoxFuture<'a, DataPlaneResult<()>> {
        Box::pin(async move {
            assert_eq!(*markers[0].value::<u8>().unwrap(), 1);
            self.committed.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[tokio::test]
async fn reader_does_not_fetch_first_rows_before_durable_schema_commit() {
    let committed = Arc::new(AtomicBool::new(false));
    let reread = Arc::new(AtomicBool::new(false));
    let (output_tx, mut output_rx) = mpsc::channel(2);
    let (events_tx, events_rx) = mpsc::channel(2);
    let task = tokio::spawn(crate::reader_loop(
        Box::new(BarrierSource {
            emitted: false,
            committed: committed.clone(),
            reread: reread.clone(),
        }),
        output_tx,
        events_rx,
        PipelineMemory::new(1024 * 1024),
        CancellationToken::new(),
        Arc::new(crate::PipelineProgress::new()),
        None,
    ));
    let barrier = output_rx.recv().await.unwrap();
    assert!(matches!(barrier.payload, crate::ReadPayload::Dataset(_)));
    tokio::task::yield_now().await;
    assert!(!reread.load(Ordering::SeqCst));
    assert!(!committed.load(Ordering::SeqCst));
    events_tx
        .send(SinkEvent::CommittedThrough(barrier.id))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    assert!(reread.load(Ordering::SeqCst));
    assert!(committed.load(Ordering::SeqCst));
}

struct FailedAdmission;
impl DatasetAdmission for FailedAdmission {
    fn pending_datasets(&self, _: &Delivery) -> DataPlaneResult<Vec<DiscoveredDataset>> { Ok(vec![]) }
    fn prepare(&mut self, _: DiscoveredDataset, _: DatasetStage) -> BoxFuture<'_, DataPlaneResult<Arc<DeliveryDiscovery>>> {
        Box::pin(async { Err(DataPlaneFailure::fatal(anyhow::anyhow!("schema rejected"))) })
    }
}

#[tokio::test]
async fn rejected_schema_never_acknowledges_create() {
    let (input_tx, input_rx) = mpsc::channel(2);
    let (events_tx, mut events_rx) = mpsc::channel(2);
    input_tx
        .send(SinkInput::Dataset {
            id: DeliveryId::new(1),
            dataset: dataset(),
        })
        .await
        .unwrap();
    drop(input_tx);
    let result = run(
        Box::new(Actor(Arc::new(AtomicBool::new(false)))),
        Box::new(FailedAdmission),
        input_rx,
        events_tx,
        PipelineMemory::new(1024),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert!(events_rx.recv().await.is_none());
}

#[tokio::test]
async fn cancellation_during_preparation_never_acknowledges_create() {
    let drained = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (input_tx, input_rx) = mpsc::channel(2);
    let (events_tx, mut events_rx) = mpsc::channel(2);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(run(
        Box::new(Actor(drained.clone())),
        Box::new(Some(Admission {
            old_drained: drained,
            entered: entered_tx,
            release: release_rx,
        })),
        input_rx,
        events_tx,
        PipelineMemory::new(1024),
        cancellation.clone(),
    ));
    input_tx
        .send(SinkInput::Dataset {
            id: DeliveryId::new(1),
            dataset: dataset(),
        })
        .await
        .unwrap();
    entered_rx.await.unwrap();
    cancellation.cancel();
    release_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert!(events_rx.recv().await.is_none());
}

struct PersistentActor {
    sessions: usize,
    updates: usize,
    observations: Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
}

impl Sink for PersistentActor {
    fn update_discovery(&mut self, _: Arc<DeliveryDiscovery>) -> DataPlaneResult<()> {
        self.updates += 1;
        Ok(())
    }

    fn run_session(&mut self, mut io: SinkIo) -> BoxFuture<'_, DataPlaneResult<()>> {
        self.sessions += 1;
        Box::pin(async move {
            while let Some(delivery) = io.deliveries.recv().await {
                self.observations.lock().unwrap().push((self.sessions, self.updates));
                io.events.send(SinkEvent::CommittedThrough(delivery.id)).await.unwrap();
            }
            Ok(())
        })
    }
}

struct CatalogAdmission;
impl DatasetAdmission for CatalogAdmission {
    fn pending_datasets(&self, _: &Delivery) -> DataPlaneResult<Vec<DiscoveredDataset>> { Ok(vec![]) }

    fn prepare(&mut self, dataset: DiscoveredDataset, _: DatasetStage) -> BoxFuture<'_, DataPlaneResult<Arc<DeliveryDiscovery>>> {
        Box::pin(async move {
            Ok(Arc::new(DeliveryDiscovery {
                source_name: Arc::from("test"),
                source_topology: transferia_core::SourceTopology::StaticPartitions(vec![0]),
                schema_origin: transferia_core::SchemaOrigin::SourceNative,
                keep_system_columns: false,
                datasets: vec![dataset],
                performance_advice: vec![],
            }))
        })
    }
}

#[tokio::test]
async fn schema_admission_preserves_the_same_sink_instance_and_state() {
    let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (input_tx, input_rx) = mpsc::channel(4);
    let (events_tx, mut events_rx) = mpsc::channel(4);
    let task = tokio::spawn(run(
        Box::new(PersistentActor { sessions: 0, updates: 0, observations: Arc::clone(&observations) }),
        Box::new(CatalogAdmission), input_rx, events_tx,
        PipelineMemory::new(1024), CancellationToken::new(),
    ));
    for id in [0, 2] {
        input_tx.send(SinkInput::Data(Delivery {
            id: DeliveryId::new(id), outputs: vec![], meta: Default::default(),
        })).await.unwrap();
        if id == 0 {
            input_tx.send(SinkInput::Dataset { id: DeliveryId::new(1), dataset: dataset() }).await.unwrap();
        }
    }
    drop(input_tx);
    let mut committed = Vec::new();
    while let Some(SinkEvent::CommittedThrough(id)) = events_rx.recv().await { committed.push(id.get()); }
    task.await.unwrap().unwrap();
    assert_eq!(committed, [0, 1, 2]);
    assert_eq!(*observations.lock().unwrap(), [(1, 0), (2, 1)]);
}

struct BackpressuredActor;
impl Sink for BackpressuredActor {
    fn update_discovery(&mut self, _: Arc<DeliveryDiscovery>) -> DataPlaneResult<()> { Ok(()) }
    fn run_session(&mut self, mut io: SinkIo) -> BoxFuture<'_, DataPlaneResult<()>> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            while let Some(delivery) = io.deliveries.recv().await {
                io.events.send(SinkEvent::CommittedThrough(delivery.id)).await.unwrap();
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn admission_keeps_polling_the_sink_while_its_input_channel_is_full() {
    let count = crate::CHANNEL_CAPACITY * 3;
    let (input_tx, input_rx) = mpsc::channel(count);
    let (events_tx, mut events_rx) = mpsc::channel(count);
    let task = tokio::spawn(run(Box::new(BackpressuredActor), Box::new(CatalogAdmission), input_rx, events_tx,
        PipelineMemory::new(1024), CancellationToken::new()));
    for id in 0..count {
        input_tx.send(SinkInput::Data(Delivery { id: DeliveryId::new(u64::try_from(id).unwrap()), outputs: vec![], meta: Default::default() })).await.unwrap();
    }
    drop(input_tx);
    tokio::time::timeout(std::time::Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
    let mut committed = 0;
    while events_rx.recv().await.is_some() { committed += 1; }
    assert_eq!(committed, count);
}

struct RuntimeAdmission {
    prepared: bool,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl DatasetAdmission for RuntimeAdmission {
    fn pending_datasets(&self, delivery: &Delivery) -> DataPlaneResult<Vec<DiscoveredDataset>> {
        if self.prepared { return Ok(vec![]); }
        delivery.outputs.iter().map(|batch| batch.dataset(false).map_err(DataPlaneFailure::fatal)).collect()
    }
    fn prepare(&mut self, dataset: DiscoveredDataset, stage: DatasetStage) -> BoxFuture<'_, DataPlaneResult<Arc<DeliveryDiscovery>>> {
        Box::pin(async move {
            assert_eq!(stage, DatasetStage::Sink);
            self.entered.take().unwrap().send(()).unwrap();
            self.release.take().unwrap().await.unwrap();
            self.prepared = true;
            CatalogAdmission.prepare(dataset, stage).await
        })
    }
}

#[tokio::test]
async fn runtime_batch_waits_for_preparation_and_cancellation_never_delivers_or_acknowledges() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use transferia_core::SinkBatch;
    for cancel in [false, true] {
    let memory = PipelineMemory::new(1024 * 1024);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let output = SinkBatch {
        schema: Arc::new(DatasetSchema::from_arrow(batch.schema().as_ref()).unwrap()),
        namespace: None, table: Arc::from("runtime_table"), is_dlq: false,
        byte_size: batch.get_array_memory_size(), batch,
        memory: memory.reserve_transform(64), system_columns: Default::default(),
    };
    let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (input_tx, input_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(run(
        Box::new(PersistentActor { sessions: 0, updates: 0, observations: Arc::clone(&observations) }),
        Box::new(RuntimeAdmission { prepared: false, entered: Some(entered_tx), release: Some(release_rx) }),
        input_rx, events_tx, memory, cancellation.clone(),
    ));
    input_tx.send(SinkInput::Data(Delivery { id: DeliveryId::new(7), outputs: vec![output], meta: Default::default() })).await.unwrap();
    entered_rx.await.unwrap();
    assert!(events_rx.try_recv().is_err());
    assert!(observations.lock().unwrap().is_empty());
    if cancel { cancellation.cancel(); }
    release_tx.send(()).unwrap();
    drop(input_tx);
    let committed = events_rx.recv().await;
    task.await.unwrap().unwrap();
    if cancel {
        assert_eq!(committed, None);
        assert!(observations.lock().unwrap().is_empty());
    } else {
        assert_eq!(committed, Some(SinkEvent::CommittedThrough(DeliveryId::new(7))));
        assert_eq!(*observations.lock().unwrap(), [(2, 1)]);
    }
    }
}
