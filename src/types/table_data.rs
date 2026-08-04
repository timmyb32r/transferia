use std::sync::Arc;

use arrow::record_batch::RecordBatch;

/// Single transport object for the pipeline: parser → middlewares → accumulator → sink.
/// `table` is already resolved to the concrete target name ("my_table" or "my_table.dlq")
/// — there is no `dlq_flag` indirection for the sink.
#[derive(Debug, Clone)]
pub struct TableData {
    /// Resolved target table: `"my_table"` or `"my_table.dlq"`.
    pub table: Arc<str>,
    /// Informational flag for tracing / short-circuit decisions (may be removed later).
    pub is_dlq: bool,
    /// Arrow columnar data.
    pub batch: RecordBatch,
    /// Monotonic batch id for tracing.
    pub batch_id: u64,
}

/// Canonical `<table>.dlq` naming convention. The only place that formats this suffix.
pub fn dlq_name(table: &str) -> String {
    format!("{table}.dlq")
}
