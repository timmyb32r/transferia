use bytes::Bytes;
use futures_util::future::BoxFuture;

use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::providers::yds::credentials::YdbCredentials;
use crate::providers::yds::config::YdsSourceConfig;
use crate::types::message::SourcePartition;
use crate::types::message::{Message, MessageBatch};

pub struct YdbTopicSource {
    reader: ydb::TopicReader,
    partition_id: i64,
    _config: YdsSourceConfig,
}

impl YdbTopicSource {
    pub async fn new(
        config: YdsSourceConfig,
        partition_id: i64,
        credentials: YdbCredentials,
    ) -> anyhow::Result<Self> {
        let mut builder = ydb::ClientBuilder::new_from_connection_string(&config.connection_string)?
            .with_credentials(credentials);

        if let Some(ref endpoint) = config.discovery_endpoint {
            let discovery = ydb::StaticDiscovery::new_from_str(endpoint.as_str())
                .map_err(|e| anyhow::anyhow!("Failed to create StaticDiscovery from '{endpoint}': {e}"))?;
            builder = builder.with_discovery(discovery);
        }

        let client = builder.client()?;
        let mut topic_client = client.topic_client();
        let selector = ydb::TopicSelector {
            path: config.topic_path.clone(),
            partition_ids: Some(vec![partition_id]),
            read_from: None,
        };
        let selectors = ydb::TopicSelectors(vec![selector]);
        let reader = topic_client.create_reader(config.consumer_name.clone(), selectors).await?;
        Ok(Self { reader, partition_id, _config: config })
    }
}

impl Source for YdbTopicSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            let mut batch = self.reader.read_batch().await?;
            let commit_marker = (!batch.messages.is_empty()).then(|| CommitMarker::new(batch.get_commit_marker()));
            let estimated = batch.messages.len();
            let mut messages = Vec::with_capacity(estimated);
            for msg in &mut batch.messages {
                if let Some(bytes) = msg.read_and_take().await? {
                    messages.push(Message {
                        value: Bytes::from(bytes),
                        offset: Some(msg.offset),
                        partition: Some(SourcePartition::Int(msg.get_partition_id())),
                    });
                }
            }
            Ok(ReadResult::Batch(MessageBatch { messages, partition_id: self.partition_id, commit_marker }))
        })
    }

    fn commit_offsets<'ctx>(&'ctx mut self, marker: &'ctx CommitMarker) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move {
            if let Some(ydb_marker) = marker.downcast_ref::<ydb::TopicReaderCommitMarker>() {
                self.reader.commit(ydb_marker.clone())
                    .map_err(|e| anyhow::anyhow!("Commit failed: {e}"))?;
            } else {
                anyhow::bail!("Invalid commit marker type");
            }
            Ok(())
        })
    }
}
