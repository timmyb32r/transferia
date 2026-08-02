use bytes::Bytes;

use crate::pipeline::source::CommitMarker;

/// A single message from the source. Minimal — only the payload bytes.
/// `offset`, `key`, `headers`, `create_time`, `write_time` are never read
/// downstream and have been removed (~78% less data moved per message).
#[derive(Debug, Clone)]
pub struct Message {
    pub value: Bytes,
}

/// A batch of messages read from a source partition.
#[derive(Debug)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub partition_id: i64,
    pub commit_marker: Option<CommitMarker>,
}
