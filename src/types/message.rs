use bytes::Bytes;

use crate::pipeline::source::CommitMarker;
use crate::types::exactly_once::PartitionKey;

/// A single message from the source.
#[derive(Debug, Clone)]
pub struct Message {
    pub value: Bytes,
    /// Exactly-once offset (None = source does not support exactly-once).
    pub offset: Option<i64>,
    /// Exactly-once partition (None = source does not support exactly-once).
    pub partition: Option<PartitionKey>,
}

/// A batch of messages read from a source partition.
#[derive(Debug)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub partition_id: i64,
    pub commit_marker: Option<CommitMarker>,
}
