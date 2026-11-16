use std::collections::HashMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use super::config::LogbrokerSinkConfig;
use crate::connectors::logbroker::pqv1::sink::writer::serialize_delivery;
use crate::connectors::logbroker::topic_writer::{TopicCodec, TopicWriter, TopicWriterConfig};
use crate::metrics::SinkCounters;
use crate::serializer::{DeliverySerializer, QueueMessageMode};
use transferia_core::delivery::SinkLimits;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Sink, SinkEvent, SinkIo};
use transferia_registry::SinkBuildContext;

const NETWORK_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(10);
/// Logbroker serves its topics from this database.
const YDB_DATABASE: &str = "/Root";

pub(super) struct YdbTopicSink {
    config: Arc<LogbrokerSinkConfig>,
    token: Arc<str>,
    counters: Arc<SinkCounters>,
    delivery_name: Arc<str>,
    discovery: Arc<transferia_core::delivery::DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
}

impl YdbTopicSink {
    pub(super) fn new(
        config: Arc<LogbrokerSinkConfig>,
        token: Arc<str>,
        context: SinkBuildContext,
    ) -> anyhow::Result<Self> {
        let limits: Arc<dyn SinkLimits> = Arc::clone(&config) as Arc<dyn SinkLimits>;
        Ok(Self {
            config,
            token,
            counters: context.counters,
            delivery_name: context.delivery_name,
            discovery: context.discovery,
            limits,
        })
    }

    async fn run_actor(&self, mut io: SinkIo) -> anyhow::Result<()> {
        let mut sessions = HashMap::<(String, String), TopicWriter>::new();
        let mut serializer = DeliverySerializer::new(
            &self.config.serializer,
            QueueMessageMode::ValuesOnly,
            &self.delivery_name,
        )?;

        while let Some(delivery) = io.deliveries.recv().await {
            let started = std::time::Instant::now();
            let serialized = serialize_delivery(
                &mut serializer,
                &delivery,
                &self.discovery,
                self.limits.as_ref(),
            )
            .await
            .map_err(|error| anyhow::Error::from(DataPlaneFailure::fatal(error)))?;
            let payload_bytes = serialized.payload_bytes()?;
            for batch in serialized.batches {
                let topic = self.config.topic.topic_for_table(&batch.table);
                let identity = (topic.clone(), batch.table.to_string());
                if !sessions.contains_key(&identity) {
                    let session = self.connect_session(&topic, &batch.table, &io.cancellation).await?;
                    sessions.insert(identity.clone(), session);
                }
                let session = sessions
                    .get_mut(&identity)
                    .ok_or_else(|| anyhow::anyhow!("YDB Topic writer session disappeared"))?;
                let batch_payloads = batch
                    .messages
                    .into_iter()
                    .map(|message| {
                        anyhow::ensure!(
                            message.key.is_none(),
                            "Logbroker value-only serializer unexpectedly produced a key"
                        );
                        message.value.ok_or_else(|| {
                            anyhow::anyhow!("Logbroker value-only serializer produced a tombstone")
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                session.write(batch_payloads).await?;
            }
            self.counters.add_rows(serialized.source_rows);
            self.counters.add_bytes(payload_bytes);
            self.counters.add_flush();
            self.counters
                .add_source_messages(delivery.meta.source_messages);
            self.counters.add_busy(started.elapsed());
            io.events
                .send(SinkEvent::CommittedThrough(delivery.id))
                .await
                .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
        }
        Ok(())
    }

    async fn connect_session(
        &self,
        topic: &str,
        producer_id: &str,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<TopicWriter> {
        TopicWriter::connect(
            &TopicWriterConfig {
                host: self.config.host.clone(),
                port: self.config.port,
                database: YDB_DATABASE.to_owned(),
                topic: topic.to_owned(),
                producer_id: producer_id.to_owned(),
                partition_id: self.config.partition_id,
                codec: TopicCodec::Raw,
                network_timeout: NETWORK_TIMEOUT,
            },
            &self.token,
            cancellation,
        )
        .await
    }
}

impl Sink for YdbTopicSink {
    fn update_discovery(&mut self, discovery: std::sync::Arc<transferia_core::DeliveryDiscovery>) -> transferia_core::failure::DataPlaneResult<()> {
        self.discovery = discovery;
        Ok(())
    }

    fn run_session(
        &mut self,
        io: SinkIo,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            self.run_actor(io)
                .await
                .map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}
