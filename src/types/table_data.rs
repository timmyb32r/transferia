use alloc::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::types::exactly_once::ExactlyOnceKey;

/// Pipeline unit: one Arrow batch destined for one pre-resolved table.
///
/// Flows: **parser → middlewares → accumulator**.
/// `table` is already resolved to the concrete target name
/// (`"my_table"` or `"my_table_dlq"`) — there is no `dlq_flag` indirection.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableData {
    /// Resolved target table: `"my_table"` or `"my_table_dlq"`.
    pub table: Arc<str>,
    /// Informational flag for tracing / short-circuit decisions.
    pub is_dlq: bool,
    /// Arrow columnar data.
    pub batch: RecordBatch,
    /// Monotonic batch id for tracing.
    pub batch_id: u64,
    /// Exactly-once key descriptor. `None` → at-least-once (no dedup).
    /// The actual key values are columns inside `batch`.
    pub exactly_once_key: Option<ExactlyOnceKey>,
}

/// Accumulated write for a single table. Built by [`BatchAccumulator`] from one or
/// more [`TableData`]s sharing the same `table`. Consumed by [`Sink::write`].
///
/// **Invariant:** all batches in `batches` share the schema of `table`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableWrite {
    /// Resolved table name (pre-resolved by the parser — no DLQ indirection).
    pub table: Arc<str>,
    /// Arrow batches to insert (one or more).
    pub batches: Vec<RecordBatch>,
    /// Exactly-once key descriptor. `None` → at-least-once (plain INSERT).
    pub exactly_once_key: Option<ExactlyOnceKey>,
}

impl TableData {
    #[must_use]
    pub const fn new(
        table: Arc<str>,
        is_dlq: bool,
        batch: RecordBatch,
        batch_id: u64,
        exactly_once_key: Option<ExactlyOnceKey>,
    ) -> Self {
        Self { table, is_dlq, batch, batch_id, exactly_once_key }
    }
}

impl TableWrite {
    #[must_use]
    pub const fn new(
        table: Arc<str>,
        batches: Vec<RecordBatch>,
        exactly_once_key: Option<ExactlyOnceKey>,
    ) -> Self {
        Self { table, batches, exactly_once_key }
    }
}

/// Canonical `<table>_dlq` naming convention. The only place that formats this suffix.
#[must_use]
pub fn dlq_name(table: &str) -> String {
    format!("{table}_dlq")
}
