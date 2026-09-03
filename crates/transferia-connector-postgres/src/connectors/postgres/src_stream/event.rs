use std::sync::Arc;

use bytes::Bytes;
use transferia_core::ChangeOperation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LogicalValue {
    Null,
    /// `PostgreSQL` omitted an unchanged TOAST value from an update tuple.
    UnchangedToast,
    Text(Bytes),
    Binary(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OldValuesKind {
    Key,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChangeEvent {
    pub schema: Arc<str>,
    pub table: Arc<str>,
    pub operation: ChangeOperation,
    pub values: Vec<LogicalValue>,
    pub old_values: Option<Vec<LogicalValue>>,
    pub old_values_kind: Option<OldValuesKind>,
    pub lsn: u64,
    pub transaction_id: u32,
    pub commit_timestamp_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationColumn {
    pub name: Arc<str>,
    pub type_oid: u32,
    pub key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Relation {
    pub oid: u32,
    pub schema: Arc<str>,
    pub table: Arc<str>,
    pub replica_identity: u8,
    pub columns: Arc<[RelationColumn]>,
}
