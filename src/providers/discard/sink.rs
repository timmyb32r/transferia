use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::delivery::execution::sink::{Sink, SinkEvent, SinkIo};
use crate::metrics::SinkCounters;

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
#[path = "tests/sink.rs"]
mod tests;
