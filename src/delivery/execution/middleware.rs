use alloc::sync::Arc;

use async_trait::async_trait;

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
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Validate this transform against the discovered main-dataset schema.
    /// Current middleware preserves schema; a future schema-changing transform
    /// must extend this contract to return its projected schema.
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema>;

    /// Transform a batch into a (possibly filtered/transformed) batch.
    async fn process(&self, data: TableData) -> anyhow::Result<TableData>;
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for &T {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        (**self).output_schema(schema).await
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data).await
    }
}

#[async_trait]
impl<T: Middleware + Send + Sync + ?Sized> Middleware for Box<T> {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        (**self).output_schema(schema).await
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data).await
    }
}

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for Arc<T> {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        (**self).output_schema(schema).await
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data).await
    }
}
