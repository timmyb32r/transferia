use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::pipeline::source::{CommitMarker, Source};
use crate::types::{Message, MessageBatch};

/// YDB Topic source — reads messages from a single YDB topic partition.
///
/// Implements the `Source` trait using the ydb-rs-sdk TopicReader API.
/// Each instance connects to one specific partition.
pub struct YdbTopicSource {
    reader: ydb::TopicReader,
    partition_id: i64,
}

impl YdbTopicSource {
    /// Create a new YDB topic reader for a specific partition.
    ///
    /// Uses `force-exhaustive-all` feature to construct TopicSelector via struct literal.
    /// `create_reader` takes &mut TopicClient.
    pub async fn new(
        connection_string: &str,
        topic_path: &str,
        consumer_name: &str,
        partition_id: i64,
        credentials: ydb::AnonymousCredentials,
    ) -> anyhow::Result<Self> {
        let client = ydb::ClientBuilder::new_from_connection_string(connection_string)?
            .with_credentials(credentials)
            .client()?;

        // create_reader takes &mut self on TopicClient
        let mut topic_client = client.topic_client();

        // With force-exhaustive-all feature, struct literal works
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

#[async_trait]
impl Source for YdbTopicSource {
    /// Read the next batch of messages from this partition.
    ///
    /// - `read_batch()` returns `TopicReaderBatch` directly (never `Option`)
    /// - Store `get_commit_marker()` BEFORE consuming messages via `read_and_take()`
    /// - `&mut batch.messages` for the `read_and_take()` loop
    /// - `read_and_take()` returns `Option<Vec<u8>>` — `if let Some(bytes) = ...`
    ///
    /// ydb-0.13.5 `TopicReaderMessage` exposes only `offset: i64`,
    /// `created_at: Option<SystemTime>` and `written_at: SystemTime` (no key or
    /// headers), so `key`/`headers` are filled with empty defaults.
    async fn read_batch(&mut self) -> anyhow::Result<MessageBatch> {
        let mut batch = self.reader.read_batch().await?;

        // Store commit marker BEFORE consuming messages
        let commit_marker = if !batch.messages.is_empty() {
            Some(CommitMarker::new(batch.get_commit_marker()))
        } else {
            None
        };

        // Estimate capacity from SDK batch size (default ~1000)
        let estimated = batch.messages.len();
        let mut messages = Vec::with_capacity(estimated);

        for msg in &mut batch.messages {
            if let Some(bytes) = msg.read_and_take().await? {
                messages.push(Message {
                    offset: msg.offset,
                    key: Vec::new(),
                    value: Bytes::from(bytes),
                    create_time: msg.created_at.map(DateTime::<Utc>::from),
                    write_time: Some(DateTime::<Utc>::from(msg.written_at)),
                    headers: HashMap::new(),
                });
            }
        }

        Ok(MessageBatch {
            messages,
            partition_id: self.partition_id,
            commit_marker,
        })
    }

    /// Commit offsets using the marker obtained during read_batch().
    ///
    /// - `commit(&mut self, marker)` — synchronous, returns YdbResult<()>
    /// - Downcast `CommitMarker` to `TopicReaderCommitMarker`
    async fn commit_offsets(&mut self, marker: &CommitMarker) -> anyhow::Result<()> {
        if let Some(ydb_marker) = marker.downcast_ref::<ydb::TopicReaderCommitMarker>() {
            self.reader
                .commit(ydb_marker.clone())
                .map_err(|e| anyhow::anyhow!("Commit failed: {}", e))?;
        } else {
            anyhow::bail!("Invalid commit marker type");
        }
        Ok(())
    }
}
