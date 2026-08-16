use bytes::Bytes;
use std::sync::Arc;

use crate::data::table_data::TableData;
use crate::source::CommitMarker;

/// A single message from the source.
#[derive(Debug, Clone)]
pub struct Message {
    pub value: Bytes,
    pub meta: MessageMeta,
}

/// Provider-neutral source metadata that can be materialized as system columns.
#[derive(Debug, Clone, Default)]
pub struct MessageMeta {
    pub topic: Option<Arc<str>>,
    pub partition: Option<i64>,
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

/// One source read result. Raw sources feed messages to a configured parser;
/// typed sources bypass parsing and preserve native Arrow columns.
#[derive(Debug)]
pub enum SourceBatch {
    Raw {
        messages: Vec<Message>,
        commit_marker: Option<CommitMarker>,
        memory: Vec<crate::memory::MemoryReservation>,
    },
    Typed {
        tables: Vec<TableData>,
        source_rows: u64,
        commit_marker: Option<CommitMarker>,
        memory: Vec<crate::memory::MemoryReservation>,
    },
    Finished,
}
