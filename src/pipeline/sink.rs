use std::future::Future;

use crate::types::table_data::TableWrite;

/// Sink trait: writes accumulated Arrow batches to a destination.
///
/// The table name arrives **pre-resolved** inside [`TableWrite`] — the
/// sink does not transform or suffix it. For DLQ data the caller passes
/// `"my_table.dlq"`; for main data it passes `"my_table"`. The sink is
/// entirely unaware of DLQ semantics.
pub trait Sink: Send + Sync {
    /// Write all batches of a [`TableWrite`] into the target table.
    /// One call = one `INSERT` operation (typically `insert_many`).
    fn write(
        &self,
        write: TableWrite,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
