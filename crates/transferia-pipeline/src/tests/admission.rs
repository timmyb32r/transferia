use super::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use transferia_core::{DatasetSchema, DatasetRole};
use transferia_core::delivery::UpdatePolicy;

fn dataset() -> DiscoveredDataset {
    DiscoveredDataset { namespace: Some(Arc::from("db")), name: Arc::from("new_table"),
        role: DatasetRole::Main, update_policy: UpdatePolicy::Strict,
        incoming_schema: DatasetSchema::default(), stored_schema: DatasetSchema::default(), system_columns: vec![] }
}

struct Actor(Arc<AtomicBool>);
impl Sink for Actor {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(async move {
            while let Some(delivery) = io.deliveries.recv().await {
                io.events.send(SinkEvent::CommittedThrough(delivery.id)).await.unwrap();
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
    fn prepare(&mut self, _: DiscoveredDataset) -> BoxFuture<'_, DataPlaneResult<Box<dyn Sink>>> {
        let admission = self.take().unwrap();
        Box::pin(async move {
            assert!(admission.old_drained.load(Ordering::SeqCst));
            admission.entered.send(()).unwrap();
            admission.release.await.unwrap();
            Ok(Box::new(Actor(Arc::new(AtomicBool::new(false)))) as Box<dyn Sink>)
        })
    }
}

#[tokio::test]
async fn admission_drains_old_sink_and_never_acknowledges_before_preparation() {
    let drained = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (input_tx, input_rx) = mpsc::channel(2);
    let (events_tx, mut events_rx) = mpsc::channel(2);
    let task = tokio::spawn(run(Box::new(Actor(drained.clone())), Box::new(Some(Admission {
        old_drained: drained, entered: entered_tx, release: release_rx,
    })), input_rx, events_tx, PipelineMemory::new(1024 * 1024), CancellationToken::new()));
    input_tx.send(SinkInput::Dataset { id: DeliveryId::new(1), dataset: dataset() }).await.unwrap();
    entered_rx.await.unwrap();
    assert!(events_rx.try_recv().is_err());
    release_tx.send(()).unwrap();
    assert!(matches!(events_rx.recv().await, Some(SinkEvent::CommittedThrough(id)) if id == DeliveryId::new(1)));
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
                dataset: Box::new(dataset()), commit_marker: transferia_core::CommitMarker::new(1_u8), memory: vec![],
            })
        })
    }
    fn commit_offsets<'a>(&'a mut self, markers: &'a [transferia_core::CommitMarker]) -> BoxFuture<'a, DataPlaneResult<()>> {
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
    let task = tokio::spawn(crate::reader_loop(Box::new(BarrierSource {
        emitted: false, committed: committed.clone(), reread: reread.clone(),
    }), output_tx, events_rx, PipelineMemory::new(1024 * 1024), CancellationToken::new(),
        Arc::new(crate::PipelineProgress::new()), None));
    let barrier = output_rx.recv().await.unwrap();
    assert!(matches!(barrier.payload, crate::ReadPayload::Dataset(_)));
    tokio::task::yield_now().await;
    assert!(!reread.load(Ordering::SeqCst));
    assert!(!committed.load(Ordering::SeqCst));
    events_tx.send(SinkEvent::CommittedThrough(barrier.id)).await.unwrap();
    task.await.unwrap().unwrap();
    assert!(reread.load(Ordering::SeqCst));
    assert!(committed.load(Ordering::SeqCst));
}

struct FailedAdmission;
impl DatasetAdmission for FailedAdmission {
    fn prepare(&mut self, _: DiscoveredDataset) -> BoxFuture<'_, DataPlaneResult<Box<dyn Sink>>> {
        Box::pin(async { Err(DataPlaneFailure::fatal(anyhow::anyhow!("schema rejected"))) })
    }
}

#[tokio::test]
async fn rejected_schema_never_acknowledges_create() {
    let (input_tx, input_rx) = mpsc::channel(2);
    let (events_tx, mut events_rx) = mpsc::channel(2);
    input_tx.send(SinkInput::Dataset { id: DeliveryId::new(1), dataset: dataset() }).await.unwrap();
    drop(input_tx);
    let result = run(Box::new(Actor(Arc::new(AtomicBool::new(false)))),
        Box::new(FailedAdmission), input_rx, events_tx, PipelineMemory::new(1024),
        CancellationToken::new()).await;
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
    let task = tokio::spawn(run(Box::new(Actor(drained.clone())), Box::new(Some(Admission {
        old_drained: drained, entered: entered_tx, release: release_rx,
    })), input_rx, events_tx, PipelineMemory::new(1024), cancellation.clone()));
    input_tx.send(SinkInput::Dataset { id: DeliveryId::new(1), dataset: dataset() }).await.unwrap();
    entered_rx.await.unwrap();
    cancellation.cancel();
    release_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert!(events_rx.recv().await.is_none());
}
