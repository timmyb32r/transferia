use alloc::sync::Arc;

use crate::core::data::schema::DatasetSchema;
use crate::core::data::table_data::TableData;

/// The Middleware trait transforms a `TableData` into another `TableData`.
///
/// Implementations filter, enrich, or otherwise transform the batch. The
/// result carries the same `table` / `is_dlq` unless the
/// implementation intentionally changes them.
///
/// **Contract:** middleware implementations are only called for **non-DLQ**
/// batches (`is_dlq == false`). DLQ short-circuits in `pipeline::apply_middlewares`
/// before reaching any implementation — implementations do not need to handle
/// DLQ schema mismatches.
///
/// This trait is **synchronous** — implementations are CPU-bound (filtering,
/// column manipulation). No heap-allocated future per call.
pub trait Middleware: Send + Sync {
    /// Validate this transform against the discovered main-dataset schema.
    /// Current middleware preserves schema; a future schema-changing transform
    /// must extend this contract to return its projected schema.
    fn validate_schema(&self, schema: &DatasetSchema) -> anyhow::Result<()>;

    /// Transform a batch into a (possibly filtered/transformed) batch.
    fn process(&self, data: TableData) -> anyhow::Result<TableData>;
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

impl<T: Middleware + ?Sized> Middleware for &T {
    fn validate_schema(&self, schema: &DatasetSchema) -> anyhow::Result<()> {
        (**self).validate_schema(schema)
    }

    fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data)
    }
}

impl<T: Middleware + Send + Sync + ?Sized> Middleware for Box<T> {
    fn validate_schema(&self, schema: &DatasetSchema) -> anyhow::Result<()> {
        (**self).validate_schema(schema)
    }

    fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data)
    }
}

impl<T: Middleware + ?Sized> Middleware for Arc<T> {
    fn validate_schema(&self, schema: &DatasetSchema) -> anyhow::Result<()> {
        (**self).validate_schema(schema)
    }

    fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data)
    }
}
