use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Headers as _, Message as _, OwnedMessage, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use tokio_util::sync::CancellationToken;

use super::config::KafkaSourceConfig;
use crate::metrics::SourceCounters;
use transferia_core::data::message::{Message, MessageHeader, MessageMeta, SourceBatch};
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};
use transferia_registry::{SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem};

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
        let mut retained_bytes = record_retained_bytes(&records[0])?;
        if retained_bytes > self.config.batch_max_bytes {
            return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "Kafka record retained size {retained_bytes} exceeds explicit kafka.batch_max_bytes {}",
                self.config.batch_max_bytes
            )));
        }
        while records.len() < self.config.batch_max_messages
            && retained_bytes < self.config.batch_max_bytes
        {
            let result =
                tokio::time::timeout(core::time::Duration::from_millis(1), self.receive()).await;
            let Ok(message) = result else { break };
            let message = message?;
            let bytes = record_retained_bytes(&message)?;
            if retained_bytes.saturating_add(bytes) > self.config.batch_max_bytes {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "Kafka buffered record would exceed explicit kafka.batch_max_bytes {}; lower broker fetch size or raise the configured batch limit",
                    self.config.batch_max_bytes
                )));
            }
            retained_bytes = retained_bytes.checked_add(bytes).ok_or_else(|| {
                DataPlaneFailure::fatal(anyhow::anyhow!("Kafka batch byte count overflow"))
            })?;
            records.push(message);
        }
        let reservation = self.memory.reserve_progress_source(retained_bytes).await;
        let mut offsets = BTreeMap::<(String, i32), i64>::new();
        let mut messages = Vec::with_capacity(records.len());
        for record in records {
            let next_offset = record
                .offset()
                .checked_add(1)
                .ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("Kafka offset overflow")))?;
            offsets
                .entry((record.topic().to_owned(), record.partition()))
                .and_modify(|current| *current = (*current).max(next_offset))
                .or_insert(next_offset);
            messages.push(source_message(&record));
        }
        self.counters
            .add_records(u64::try_from(messages.len()).unwrap_or(u64::MAX));
        // librdkafka exposes payloads after Kafka batch decompression. Do not
        // misreport those decoded payload bytes as raw network throughput.
        self.counters
            .add_network_decoded_bytes(u64::try_from(retained_bytes).unwrap_or(u64::MAX));
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

fn source_message(record: &OwnedMessage) -> Message {
    let payload = record.payload();
    let timestamp = match record.timestamp() {
        Timestamp::NotAvailable => None,
        Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => Some(value),
    };
    Message {
        value: payload.map_or_else(Bytes::new, Bytes::copy_from_slice),
        tombstone: payload.is_none(),
        key: record.key().map(Bytes::copy_from_slice),
        headers: record
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|header| MessageHeader {
                        key: Arc::from(header.key),
                        value: header.value.map(Bytes::copy_from_slice),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
            .into(),
        meta: MessageMeta {
            topic: Some(Arc::from(record.topic())),
            partition: Some(i64::from(record.partition())),
            offset: Some(record.offset()),
            write_timestamp_ms: timestamp,
        },
    }
}

pub(super) fn preview_message(
    record: &OwnedMessage,
    max_bytes: usize,
) -> anyhow::Result<SourcePreview> {
    let payload = record.payload().unwrap_or_default();
    anyhow::ensure!(
        payload.len() <= max_bytes,
        "Kafka message payload is {} bytes, exceeding max_bytes={max_bytes}",
        payload.len()
    );
    let timestamp = record.timestamp();
    let mut message_metadata = Vec::new();
    if let Some(key) = record.key() {
        message_metadata.push(SourcePreviewMetadataItem {
            key: "kafka.key".to_owned(),
            value: key.to_vec(),
        });
    }
    if let Some(headers) = record.headers() {
        message_metadata.extend(headers.iter().map(|header| {
            SourcePreviewMetadataItem {
                key: header.key.to_owned(),
                value: header.value.unwrap_or_default().to_vec(),
            }
        }));
    }
    Ok(SourcePreview {
        payload: payload.to_vec(),
        detection_payloads: vec![payload.to_vec()],
        metadata: SourcePreviewMetadata {
            topic: record.topic().to_owned(),
            partition: i64::from(record.partition()),
            partition_session_id: 0,
            offset: record.offset(),
            sequence_number: record.offset(),
            created_at_ms: match timestamp {
                Timestamp::CreateTime(value) => Some(value),
                Timestamp::NotAvailable | Timestamp::LogAppendTime(_) => None,
            },
            written_at_ms: match timestamp {
                Timestamp::LogAppendTime(value) => Some(value),
                Timestamp::NotAvailable | Timestamp::CreateTime(_) => None,
            },
            producer_id: String::new(),
            message_group_id: None,
            codec: "librdkafka-decoded".to_owned(),
            compressed_size: 0,
            declared_uncompressed_size: Some(payload.len()),
            message_metadata,
            write_session_metadata: BTreeMap::new(),
        },
    })
}

fn record_retained_bytes(record: &OwnedMessage) -> DataPlaneResult<usize> {
    let mut bytes = record.payload().map_or(0, <[u8]>::len);
    bytes = bytes
        .checked_add(record.key().map_or(0, <[u8]>::len))
        .ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("Kafka record size overflow")))?;
    if let Some(headers) = record.headers() {
        for header in headers.iter() {
            bytes = bytes
                .checked_add(header.key.len())
                .and_then(|size| size.checked_add(header.value.map_or(0, <[u8]>::len)))
                .ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!("Kafka record size overflow"))
                })?;
        }
    }
    Ok(bytes)
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

#[cfg(test)]
#[path = "tests/source.rs"]
mod tests;
