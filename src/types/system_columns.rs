use std::sync::Arc;

use arrow::datatypes::DataType;

/// Semantic role of a parser-generated source metadata column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemColumnKind {
    TopicName,
    PartitionNum,
    Offset,
    MessageIndex,
    WriteTimestampMs,
}

impl SystemColumnKind {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::TopicName => "_system_topic_name",
            Self::PartitionNum => "_system_partition_num",
            Self::Offset => "_system_offset",
            Self::MessageIndex => "_system_message_index",
            Self::WriteTimestampMs => "_system_write_timestamp_ms",
        }
    }

    #[must_use]
    pub const fn data_type(self) -> DataType {
        match self {
            Self::TopicName => DataType::Utf8,
            Self::PartitionNum | Self::Offset | Self::WriteTimestampMs => DataType::Int64,
            Self::MessageIndex => DataType::UInt64,
        }
    }
}

/// Physical Arrow location of one semantic system column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemColumn {
    pub kind: SystemColumnKind,
    pub index: usize,
}

/// Immutable metadata carried next to a [`RecordBatch`](arrow::record_batch::RecordBatch).
#[derive(Debug, Clone, Default)]
pub struct SystemColumns(Arc<[SystemColumn]>);

impl SystemColumns {
    #[must_use]
    pub fn new(columns: impl Into<Arc<[SystemColumn]>>) -> Self {
        Self(columns.into())
    }

    #[must_use]
    pub fn get(&self, kind: SystemColumnKind) -> Option<SystemColumn> {
        self.0.iter().copied().find(|column| column.kind == kind)
    }

    #[must_use]
    pub fn contains(&self, kind: SystemColumnKind) -> bool {
        self.get(kind).is_some()
    }

    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = SystemColumn> + '_ {
        self.0.iter().copied()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
