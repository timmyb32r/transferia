use std::sync::Arc;

use arrow::datatypes::DataType;

/// Semantic role of a parser-generated source metadata column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemColumnKind {
    Topic,
    Partition,
    Offset,
    MessageIndex,
    WriteTimestampMs,
    /// Debezium-compatible row operation (`c`, `r`, `u`, or `d`).
    ChangeOperation,
    /// Bit mask identifying user columns physically present in a changelog row.
    ///
    /// `PostgreSQL` logical replication uses this to distinguish an unchanged
    /// TOAST value from SQL `NULL`. Bit `n` corresponds to user column `n`.
    ChangedColumns,
}

impl SystemColumnKind {
    #[must_use]
    pub const fn default_name(self) -> &'static str {
        match self {
            Self::Topic => "_system_topic",
            Self::Partition => "_system_partition",
            Self::Offset => "_system_offset",
            Self::MessageIndex => "_system_message_index",
            Self::WriteTimestampMs => "_system_write_timestamp_ms",
            Self::ChangeOperation => "_system_change_operation",
            Self::ChangedColumns => "_system_changed_columns",
        }
    }

    #[must_use]
    pub const fn data_type(self) -> DataType {
        match self {
            Self::Topic | Self::ChangeOperation => DataType::Utf8,
            Self::Partition | Self::Offset | Self::WriteTimestampMs => DataType::Int64,
            Self::MessageIndex => DataType::UInt64,
            Self::ChangedColumns => DataType::Binary,
        }
    }
}

/// Physical Arrow location of one semantic system column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemColumn {
    pub kind: SystemColumnKind,
    pub index: usize,
    pub name: Arc<str>,
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
        self.0.iter().find(|column| column.kind == kind).cloned()
    }

    #[must_use]
    pub fn contains(&self, kind: SystemColumnKind) -> bool {
        self.get(kind).is_some()
    }

    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SystemColumn> + '_ {
        self.0.iter()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
