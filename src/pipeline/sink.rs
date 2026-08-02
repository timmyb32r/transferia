use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::types::arrow_batch::ArrowBatch;

/// The Sink trait writes Arrow data to a destination (ClickHouse).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a single Arrow batch (used for DLQ, individual batches).
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()>;

    /// Write multiple RecordBatches in a single INSERT (no concat copy).
    /// `dlq_flag` selects the target table (false = main, true = DLQ).
    async fn write_batches(&self, batches: &[RecordBatch], dlq_flag: bool) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Sink + ?Sized> Sink for &T {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        (**self).write_batch(batch).await
    }
    async fn write_batches(&self, batches: &[RecordBatch], dlq_flag: bool) -> anyhow::Result<()> {
        (**self).write_batches(batches, dlq_flag).await
    }
}

#[async_trait]
impl<T: Sink + Send + Sync + ?Sized> Sink for Box<T> {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        (**self).write_batch(batch).await
    }
    async fn write_batches(&self, batches: &[RecordBatch], dlq_flag: bool) -> anyhow::Result<()> {
        (**self).write_batches(batches, dlq_flag).await
    }
}

#[async_trait]
impl<T: Sink + ?Sized> Sink for Arc<T> {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        (**self).write_batch(batch).await
    }
    async fn write_batches(&self, batches: &[RecordBatch], dlq_flag: bool) -> anyhow::Result<()> {
        (**self).write_batches(batches, dlq_flag).await
    }
}
