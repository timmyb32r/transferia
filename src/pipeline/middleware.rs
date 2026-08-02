use std::sync::Arc;

use crate::types::arrow_batch::ArrowBatch;

/// The Middleware trait transforms an Arrow batch into another Arrow batch.
///
/// Implementations filter, enrich, or otherwise transform the batch. The
/// result carries the same `meta` (dlq_flag, table_name, batch_id) unless
/// the implementation intentionally changes it.
///
/// This trait is **synchronous** — implementations are CPU-bound (filtering,
/// column manipulation). No heap-allocated future per call.
pub trait Middleware: Send + Sync {
    /// Transform a batch into a (possibly filtered/transformed) batch.
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch>;
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

impl<T: Middleware + ?Sized> Middleware for &T {
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        (**self).process(batch)
    }
}

impl<T: Middleware + Send + Sync + ?Sized> Middleware for Box<T> {
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        (**self).process(batch)
    }
}

impl<T: Middleware + ?Sized> Middleware for Arc<T> {
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        (**self).process(batch)
    }
}
