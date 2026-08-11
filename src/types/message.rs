use bytes::Bytes;
use std::sync::Arc;

use crate::pipeline::source::CommitMarker;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourcePartition {
    Int(i64),
    String(Arc<str>),
}

/// A single message from the source.
#[derive(Debug, Clone)]
pub struct Message {
    pub value: Bytes,
    pub meta: MessageMeta,
}

/// Provider-neutral source metadata that can be materialized as system columns.
#[derive(Debug, Clone, Default)]
pub struct MessageMeta {
    pub topic_name: Option<Arc<str>>,
    pub partition: Option<SourcePartition>,
    pub offset: Option<i64>,
    pub write_timestamp_ms: Option<i64>,
}

impl Message {
    #[must_use]
    pub fn new(value: Bytes) -> Self {
        Self {
            value,
            meta: MessageMeta::default(),
        }
    }
}

/// A batch of messages read from a source partition.
#[derive(Debug)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub partition_id: i64,
    pub commit_marker: Option<CommitMarker>,
    /// Reservations for source-owned buffers backing `messages`.
    pub memory: Vec<crate::pipeline::memory::MemoryReservation>,
}
