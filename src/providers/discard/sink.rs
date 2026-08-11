use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::metrics::SinkCounters;
use crate::pipeline::sink::{Sink, SinkEvent, SinkIo};

/// Benchmark-only sink which acknowledges every delivery after counting and
/// dropping it. It deliberately provides no durability.
pub struct DiscardSink {
    counters: Arc<SinkCounters>,
}

impl DiscardSink {
    #[must_use]
    pub const fn new(counters: Arc<SinkCounters>) -> Self {
        Self { counters }
    }
}

impl Sink for DiscardSink {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            loop {
                let delivery = tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv() => delivery,
                };
                let Some(delivery) = delivery else {
                    return Ok(());
                };
                let rows = delivery
                    .outputs
                    .iter()
                    .map(|batch| batch.rows() as u64)
                    .sum();
                let bytes = delivery
                    .outputs
                    .iter()
                    .map(|batch| batch.bytes() as u64)
                    .sum();
                self.counters.add_rows(rows);
                self.counters.add_bytes(bytes);
                self.counters
                    .add_source_messages(delivery.meta.source_messages);
                self.counters.add_flush();
                let id = delivery.id;
                drop(delivery);
                tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    result = io.events.send(SinkEvent::CommittedThrough(id)) => {
                        result.map_err(|_| anyhow::anyhow!("discard sink event channel closed"))?;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::PipelineMemory;
    use crate::pipeline::sink::{Delivery, DeliveryId, DeliveryMeta, SinkIo};
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
                meta: DeliveryMeta {
                    source_messages: 3,
                    ..DeliveryMeta::default()
                },
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
}
