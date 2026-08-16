use alloc::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::data::system_columns::SystemColumns;

/// Pipeline unit: one Arrow batch destined for one pre-resolved table.
///
/// Flows: **parser → middlewares → sink delivery**.
/// `table` is already resolved to the concrete target name
/// (`"my_table"` or `"my_table_dlq"`) — there is no `dlq_flag` indirection.
#[derive(Debug, Clone)]
pub struct TableData {
    /// Resolved target table: `"my_table"` or `"my_table_dlq"`.
    pub table: Arc<str>,
    /// Informational flag for tracing / short-circuit decisions.
    pub is_dlq: bool,
    /// Arrow columnar data.
    pub batch: RecordBatch,
    /// Semantic roles of parser-generated Arrow columns.
    pub system_columns: SystemColumns,
}

impl TableData {
    #[must_use]
    pub const fn new(
        table: Arc<str>,
        is_dlq: bool,
        batch: RecordBatch,
        system_columns: SystemColumns,
    ) -> Self {
        Self {
            table,
            is_dlq,
            batch,
            system_columns,
        }
    }
}

/// Canonical `<table>_dlq` naming convention. The only place that formats this suffix.
#[must_use]
pub fn dlq_name(table: &str) -> String {
    format!("{table}_dlq")
}
