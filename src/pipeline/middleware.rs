use async_trait::async_trait;
use std::sync::Arc;

use crate::types::arrow_batch::ArrowBatch;

/// The Middleware trait transforms an Arrow batch into another Arrow batch.
///
/// Implementations filter, enrich, or otherwise transform the batch. The
/// result carries the same `meta` (dlq_flag, table_name, batch_id) unless
/// the implementation intentionally changes it.
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Transform a batch into a (possibly filtered/transformed) batch.
    async fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch>;
}

// ---------------------------------------------------------------------------
// Blanket impl for &T
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for &T {
    async fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        (**self).process(batch).await
    }
}

#[async_trait]
impl<T: Middleware + Send + Sync + ?Sized> Middleware for Box<T> {
    async fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        (**self).process(batch).await
    }
}

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for Arc<T> {
    async fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        (**self).process(batch).await
    }
}
