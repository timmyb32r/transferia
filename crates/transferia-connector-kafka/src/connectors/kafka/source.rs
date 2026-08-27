use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Message as _, OwnedMessage, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use tokio_util::sync::CancellationToken;

use super::config::KafkaSourceConfig;
use crate::metrics::SourceCounters;
use transferia_core::data::message::{Message, MessageMeta, SourceBatch};
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};

#[derive(Debug)]
struct KafkaCommitMarker {
    offsets: BTreeMap<(String, i32), i64>,
}

pub(super) struct KafkaSource {
    consumer: StreamConsumer,
    config: Arc<KafkaSourceConfig>,
    cancellation: CancellationToken,
    memory: PipelineMemory,
    counters: Arc<SourceCounters>,
}

impl KafkaSource {
    pub(super) const fn new(
        consumer: StreamConsumer,
        config: Arc<KafkaSourceConfig>,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
    ) -> Self {
        Self {
            consumer,
            config,
            cancellation,
            memory,
            counters,
        }
    }

    async fn receive(&self) -> DataPlaneResult<OwnedMessage> {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                return Err(DataPlaneFailure::retryable(anyhow::anyhow!("Kafka source cancelled")));
            }
            result = self.consumer.recv() => result,
        };
        self.counters.add_response_wait(started.elapsed());
        result.map(|message| message.detach()).map_err(|error| {
            DataPlaneFailure::retryable(anyhow::anyhow!("Kafka read failed: {error}"))
        })
    }

    async fn read(&self) -> DataPlaneResult<SourceBatch> {
        let first = self.receive().await?;
        let mut records = vec![first];
        let mut payload_bytes = records[0].payload().map_or(0, <[u8]>::len);
        if payload_bytes > self.config.batch_max_bytes {
            return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "Kafka record size {payload_bytes} exceeds explicit kafka.batch_max_bytes {}",
                self.config.batch_max_bytes
            )));
        }
        while records.len() < self.config.batch_max_messages
            && payload_bytes < self.config.batch_max_bytes
        {
            let result =
                tokio::time::timeout(core::time::Duration::from_millis(1), self.receive()).await;
            let Ok(message) = result else { break };
            let message = message?;
            let bytes = message.payload().map_or(0, <[u8]>::len);
            if payload_bytes.saturating_add(bytes) > self.config.batch_max_bytes {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "Kafka buffered record would exceed explicit kafka.batch_max_bytes {}; lower broker fetch size or raise the configured batch limit",
                    self.config.batch_max_bytes
                )));
            }
            payload_bytes = payload_bytes.checked_add(bytes).ok_or_else(|| {
                DataPlaneFailure::fatal(anyhow::anyhow!("Kafka batch byte count overflow"))
            })?;
            records.push(message);
        }
        let reservation = self.memory.reserve_progress_source(payload_bytes).await;
        let mut offsets = BTreeMap::<(String, i32), i64>::new();
        let mut messages = Vec::with_capacity(records.len());
        for record in records {
            let payload = record.payload().ok_or_else(|| {
                DataPlaneFailure::fatal(anyhow::anyhow!(
                    "Kafka record at {}:{}:{} has no value",
                    record.topic(),
                    record.partition(),
                    record.offset()
                ))
            })?;
            let next_offset = record
                .offset()
                .checked_add(1)
                .ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("Kafka offset overflow")))?;
            offsets
                .entry((record.topic().to_owned(), record.partition()))
                .and_modify(|current| *current = (*current).max(next_offset))
                .or_insert(next_offset);
            let timestamp = match record.timestamp() {
                Timestamp::NotAvailable => None,
                Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => Some(value),
            };
            messages.push(Message {
                value: Bytes::copy_from_slice(payload),
                meta: MessageMeta {
                    topic: Some(Arc::from(record.topic())),
                    partition: Some(i64::from(record.partition())),
                    offset: Some(record.offset()),
                    write_timestamp_ms: timestamp,
                },
            });
        }
        self.counters
            .add_records(u64::try_from(messages.len()).unwrap_or(u64::MAX));
        // librdkafka exposes payloads after Kafka batch decompression. Do not
        // misreport those decoded payload bytes as raw network throughput.
        self.counters
            .add_network_decoded_bytes(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
        Ok(SourceBatch::Raw {
            messages,
            commit_marker: Some(CommitMarker::new(KafkaCommitMarker { offsets })),
            memory: vec![reservation],
        })
    }

    fn commit(&self, markers: &[CommitMarker]) -> DataPlaneResult<()> {
        let mut offsets = BTreeMap::<(String, i32), i64>::new();
        for marker in markers {
            let marker = marker
                .value::<KafkaCommitMarker>()
                .map_err(|error| DataPlaneFailure::fatal(anyhow::Error::new(error)))?;
            for ((topic, partition), offset) in &marker.offsets {
                offsets
                    .entry((topic.clone(), *partition))
                    .and_modify(|current| *current = (*current).max(*offset))
                    .or_insert(*offset);
            }
        }
        let mut list = TopicPartitionList::new();
        for ((topic, partition), offset) in offsets {
            list.add_partition_offset(&topic, partition, Offset::Offset(offset))
                .map_err(|error| DataPlaneFailure::fatal(anyhow::Error::new(error)))?;
        }
        self.consumer
            .commit(&list, CommitMode::Sync)
            .map_err(|error| {
                DataPlaneFailure::retryable(anyhow::anyhow!("Kafka offset commit failed: {error}"))
            })?;
        Ok(())
    }
}

impl Source for KafkaSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move { self.read().await })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move { self.commit(markers) })
    }
}
