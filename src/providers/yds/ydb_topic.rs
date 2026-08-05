use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use futures_util::future::BoxFuture;

use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::{Message, MessageBatch};

/// Compute a deterministic dedup token from a message batch.
///
/// Uses a fast hash of (partition_id, msg_count, first_msg_prefix, last_msg_prefix)
/// to produce a token that is:
/// - **Deterministic**: same batch of messages → same token (survives replays)
/// - **Unique enough**: different batches → different token with very high probability
///
/// The token is formatted as a hex string for use as ClickHouse
/// `insert_deduplication_token`.
pub(crate) fn compute_dedup_token(partition_id: i64, messages: &[Message]) -> Option<String> {
    if messages.is_empty() {
        return None;
    }
    let mut hasher = DefaultHasher::new();
    partition_id.hash(&mut hasher);
    messages.len().hash(&mut hasher);
    // Hash first 64 bytes of first message
    let first = &messages.first().unwrap().value;
    let first_prefix = if first.len() > 64 { &first[..64] } else { &first[..] };
    first_prefix.hash(&mut hasher);
    // Hash last 64 bytes of last message
    let last = &messages.last().unwrap().value;
    let last_prefix = if last.len() > 64 { &last[..64] } else { &last[..] };
    last_prefix.hash(&mut hasher);
    Some(format!("{:016x}", hasher.finish()))
}

pub struct YdbTopicSource {
    reader: ydb::TopicReader,
    partition_id: i64,
}

impl YdbTopicSource {
    pub async fn new(
        connection_string: &str,
        topic_path: &str,
        consumer_name: &str,
        partition_id: i64,
        credentials: crate::providers::yds::credentials::YdbCredentials,
        discovery_endpoint: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut builder = ydb::ClientBuilder::new_from_connection_string(connection_string)?
            .with_credentials(credentials);

        if let Some(endpoint) = discovery_endpoint {
            let discovery = ydb::StaticDiscovery::new_from_str(endpoint)
                .map_err(|e| anyhow::anyhow!("Failed to create StaticDiscovery from '{}': {}", endpoint, e))?;
            builder = builder.with_discovery(discovery);
        }

        let client = builder.client()?;
        let mut topic_client = client.topic_client();
        let selector = ydb::TopicSelector {
            path: topic_path.to_string(),
            partition_ids: Some(vec![partition_id]),
            read_from: None,
        };
        let selectors = ydb::TopicSelectors(vec![selector]);
        let reader = topic_client.create_reader(consumer_name.to_string(), selectors).await?;
        Ok(Self { reader, partition_id })
    }
}

impl Source for YdbTopicSource {
    fn read_batch<'a>(&'a mut self) -> BoxFuture<'a, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            let mut batch = self.reader.read_batch().await?;
            let commit_marker = if !batch.messages.is_empty() {
                Some(CommitMarker::new(batch.get_commit_marker()))
            } else {
                None
            };
            let estimated = batch.messages.len();
            let mut messages = Vec::with_capacity(estimated);
            for msg in &mut batch.messages {
                if let Some(bytes) = msg.read_and_take().await? {
                    messages.push(Message { value: Bytes::from(bytes) });
                }
            }
            let dedup_token = compute_dedup_token(self.partition_id, &messages);
            Ok(ReadResult::Batch(MessageBatch { messages, partition_id: self.partition_id, commit_marker, dedup_token }))
        })
    }

    fn commit_offsets<'a>(&'a mut self, marker: &'a CommitMarker) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if let Some(ydb_marker) = marker.downcast_ref::<ydb::TopicReaderCommitMarker>() {
                self.reader.commit(ydb_marker.clone())
                    .map_err(|e| anyhow::anyhow!("Commit failed: {}", e))?;
            } else {
                anyhow::bail!("Invalid commit marker type");
            }
            Ok(())
        })
    }
}
