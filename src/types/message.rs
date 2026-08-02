use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::pipeline::source::CommitMarker;

/// A single message read from a source (e.g., YDB topic partition).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Message {
    pub offset: i64,
    pub key: Vec<u8>,
    pub value: Bytes,
    pub create_time: Option<DateTime<Utc>>,
    pub write_time: Option<DateTime<Utc>>,
    pub headers: HashMap<String, String>,
}

/// A batch of messages read from a source partition.
#[derive(Debug, Clone)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub partition_id: i64,
    pub commit_marker: Option<CommitMarker>,
}
