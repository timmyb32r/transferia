use std::future::Future;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

/// Sink trait: writes Arrow batches to a destination.
///
/// The table name is **pre-resolved** by the caller — the sink does not
/// transform or suffix it. For DLQ data the caller passes `"my_table.dlq"`;
/// for main data it passes `"my_table"`. The sink is entirely unaware of
/// DLQ semantics.
pub trait Sink: Send + Sync {
    /// Write many batches into a **pre-resolved** table name.
    /// One call = one `INSERT` operation (typically `insert_many`).
    fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        table: Arc<str>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
