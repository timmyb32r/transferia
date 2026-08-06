use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::types::message::MessageBatch;

// ---------------------------------------------------------------------------
// CommitMarker
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct CommitMarker(Arc<dyn core::any::Any + Send + Sync>);

impl CommitMarker {
    pub fn new<T: core::any::Any + Send + Sync>(marker: T) -> Self {
        Self(Arc::new(marker))
    }

    #[must_use]
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}

impl core::fmt::Debug for CommitMarker {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CommitMarker").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// ReadResult
// ---------------------------------------------------------------------------

/// Result of a source read. First-class terminal state — no sentinel conventions.
#[non_exhaustive]
pub enum ReadResult {
    /// Raw messages for parsing (YDS, S3, `PQv1`).
    Batch(MessageBatch),
    /// Pre-parsed Arrow batches — bypass parser, zero-copy into the pipeline
    /// (`ClickHouse` source).
    Arrow(Vec<arrow::record_batch::RecordBatch>),
    /// No more data (S3 snapshot complete, CH table exhausted).
    Exhausted,
    /// Non-retryable source failure → exit 1.
    Failed(anyhow::Error),
}

// ---------------------------------------------------------------------------
// Source trait (object-safe via BoxFuture)
// ---------------------------------------------------------------------------

pub trait Source: Send {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>>;
    fn commit_offsets<'ctx>(&'ctx mut self, marker: &'ctx CommitMarker) -> BoxFuture<'ctx, anyhow::Result<()>>;
}

/// Delegating impl: `Box<dyn Source>` is itself a `Source`.
impl Source for Box<dyn Source> {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        (**self).read_batch()
    }
    fn commit_offsets<'ctx>(&'ctx mut self, marker: &'ctx CommitMarker) -> BoxFuture<'ctx, anyhow::Result<()>> {
        (**self).commit_offsets(marker)
    }
}
