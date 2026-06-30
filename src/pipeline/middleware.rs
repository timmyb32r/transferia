use async_trait::async_trait;
use std::sync::Arc;

use crate::pipeline::source::RawBatch;
use crate::types::arrow_batch::ArrowBatch;

/// The Middleware trait transforms a raw batch of bytes into Arrow record batches.
///
/// Implementations parse the raw bytes (e.g., JSON), extract fields using
/// JSONPath, and produce typed Arrow arrays. Invalid rows are routed to a
/// dead-letter queue batch.
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Transform a raw batch into Arrow batches.
    ///
    /// Returns `(valid_batch, optional_dlq_batch)`:
    /// - `valid_batch` contains the successfully parsed rows.
    /// - `dlq_batch` contains rows that failed to parse, if any.
    ///   `None` means all rows were valid.
    async fn process(&self, raw: RawBatch) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)>;
}

// ---------------------------------------------------------------------------
// Blanket impl for &T
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for &T {
    async fn process(&self, raw: RawBatch) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        (**self).process(raw).await
    }
}

#[async_trait]
impl<T: Middleware + Send + Sync + ?Sized> Middleware for Box<T> {
    async fn process(&self, raw: RawBatch) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        (**self).process(raw).await
    }
}

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for Arc<T> {
    async fn process(&self, raw: RawBatch) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        (**self).process(raw).await
    }
}
