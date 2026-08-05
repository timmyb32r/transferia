use bytes::Bytes;

use crate::pipeline::source::CommitMarker;

/// A single message from the source. Minimal — only the payload bytes.
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
    /// Deduplication token for exactly-once sinks.
    /// Derived deterministically from the batch content so replays
    /// produce the same token. `None` for sources without offsets (S3).
    pub dedup_token: Option<String>,
}
