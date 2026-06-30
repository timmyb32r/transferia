use async_trait::async_trait;
use std::sync::Arc;

use crate::types::arrow_batch::ArrowBatch;

/// The Sink trait writes Arrow record batches to a destination.
///
/// Implementations handle the actual data persistence (e.g., ClickHouse
/// Arrow Flight insert).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write an Arrow batch to the destination.
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Blanket impls for &T, Box<T>, Arc<T>
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Sink + ?Sized> Sink for &T {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        (**self).write_batch(batch).await
    }
}

#[async_trait]
impl<T: Sink + Send + Sync + ?Sized> Sink for Box<T> {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        (**self).write_batch(batch).await
    }
}

#[async_trait]
impl<T: Sink + ?Sized> Sink for Arc<T> {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        (**self).write_batch(batch).await
    }
}
