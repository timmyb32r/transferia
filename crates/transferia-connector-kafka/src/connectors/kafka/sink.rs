use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::KafkaSinkConfig;
use crate::metrics::SinkCounters;
use crate::serializer::DeliverySerializer;
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_registry::SinkBuildContext;

pub(super) struct KafkaSink {
    config: Arc<KafkaSinkConfig>,
    producer: FutureProducer,
    serializer: DeliverySerializer,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
}

impl KafkaSink {
    pub(super) fn new(
        config: Arc<KafkaSinkConfig>,
        producer: FutureProducer,
        serializer: DeliverySerializer,
        context: SinkBuildContext,
    ) -> Self {
        Self {
            config,
            producer,
            serializer,
            counters: context.counters,
            discovery: context.discovery,
        }
    }

    async fn write(&mut self, delivery: &Delivery) -> anyhow::Result<()> {
        let (payloads, rows) = self
            .serializer
            .serialize(
                delivery,
                &self.discovery,
                self.config.as_ref() as &dyn SinkLimits,
                4 * 1024 * 1024,
            )
            .await?;
        let payload_bytes = payloads.iter().map(Vec::len).sum::<usize>();
        let timeout = super::config::timeout(self.config.request_timeout_ms);
        let mut pending = FuturesUnordered::new();
        let mut payloads = payloads.into_iter();
        for batch in &delivery.outputs {
            let topic: Arc<str> = self.config.topic.topic_for_table(&batch.table).into();
            for _ in 0..batch.rows() {
                let payload = payloads.next().ok_or_else(|| {
                    anyhow::anyhow!("Kafka serializer returned fewer payloads than input rows")
                })?;
                let producer = self.producer.clone();
                let topic = Arc::clone(&topic);
                let partition = self.config.partition;
                pending.push(async move {
                    let mut record =
                        FutureRecord::<(), [u8]>::to(topic.as_ref()).payload(payload.as_ref());
                    if let Some(partition) = partition {
                        record = record.partition(partition);
                    }
                    producer
                        .send(record, timeout)
                        .await
                        .map_err(|(error, _)| anyhow::anyhow!("Kafka delivery failed: {error}"))?;
                    Ok::<(), anyhow::Error>(())
                });
                if pending.len() >= self.config.max_in_flight {
                    pending.next().await.transpose()?;
                }
            }
        }
        anyhow::ensure!(
            payloads.next().is_none(),
            "Kafka serializer returned more payloads than input rows"
        );
        while let Some(result) = pending.next().await {
            result?;
        }
        self.counters.add_rows(rows);
        self.counters
            .add_bytes(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
        self.counters.add_flush();
        Ok(())
    }
}

impl Sink for KafkaSink {
    fn run(
        mut self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            loop {
                let delivery = tokio::select! {
                    biased;
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv() => delivery,
                };
                let Some(delivery) = delivery else {
                    return Ok(());
                };
                self.write(&delivery)
                    .await
                    .map_err(DataPlaneFailure::retryable)?;
                io.events
                    .send(SinkEvent::CommittedThrough(delivery.id))
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::fatal(anyhow::anyhow!("Kafka sink event receiver closed"))
                    })?;
            }
        })
    }
}
