use super::*;
use crate::core::memory::PipelineMemory;
use crate::core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkIo};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn acknowledges_marker_only_delivery() -> anyhow::Result<()> {
    let counters = Arc::new(SinkCounters::new());
    let sink = Box::new(DiscardSink::new(Arc::clone(&counters)));
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: PipelineMemory::new(16),
        cancellation: cancellation.clone(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(7),
            outputs: Vec::new(),
            meta: DeliveryMeta { source_messages: 3 },
        })
        .await?;
    assert_eq!(
        event_rx.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(7)))
    );
    assert_eq!(counters.source_messages_total(), 3);
    cancellation.cancel();
    task.await??;
    Ok(())
}
