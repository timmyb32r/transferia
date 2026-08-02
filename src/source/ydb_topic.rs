use async_trait::async_trait;
use bytes::Bytes;

use crate::pipeline::source::{CommitMarker, Source};
use crate::types::{Message, MessageBatch};

/// YDB Topic source — reads messages from a single YDB topic partition.
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
        credentials: ydb::AnonymousCredentials,
    ) -> anyhow::Result<Self> {
        let client = ydb::ClientBuilder::new_from_connection_string(connection_string)?
            .with_credentials(credentials)
            .client()?;

        let mut topic_client = client.topic_client();
        let selector = ydb::TopicSelector {
            path: topic_path.to_string(),
            partition_ids: Some(vec![partition_id]),
            read_from: None,
        };
        let selectors = ydb::TopicSelectors(vec![selector]);

        let reader = topic_client
            .create_reader(consumer_name.to_string(), selectors)
            .await?;

        Ok(Self { reader, partition_id })
    }
}

#[async_trait]
impl Source for YdbTopicSource {
    async fn read_batch(&mut self) -> anyhow::Result<MessageBatch> {
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
                messages.push(Message {
                    value: Bytes::from(bytes),
                });
            }
        }

        Ok(MessageBatch {
            messages,
            partition_id: self.partition_id,
            commit_marker,
        })
    }

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
