use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::types::MessageBatch;

// ---------------------------------------------------------------------------
// CommitMarker
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct CommitMarker(Arc<dyn std::any::Any + Send + Sync>);

impl CommitMarker {
    pub fn new<T: std::any::Any + Send + Sync>(marker: T) -> Self {
        Self(Arc::new(marker))
    }

    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}

impl std::fmt::Debug for CommitMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitMarker").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// ReadResult
// ---------------------------------------------------------------------------

/// Result of a source read. First-class terminal state — no sentinel conventions.
pub enum ReadResult {
    /// Raw messages for parsing (YDS, S3, PQv1).
    Batch(MessageBatch),
    /// Pre-parsed Arrow batches — bypass parser, zero-copy into the pipeline
    /// (ClickHouse source).
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
    fn read_batch<'a>(&'a mut self) -> BoxFuture<'a, anyhow::Result<ReadResult>>;
    fn commit_offsets<'a>(&'a mut self, marker: &'a CommitMarker) -> BoxFuture<'a, anyhow::Result<()>>;
}

/// Delegating impl: `Box<dyn Source>` is itself a `Source`.
impl Source for Box<dyn Source> {
    fn read_batch<'a>(&'a mut self) -> BoxFuture<'a, anyhow::Result<ReadResult>> {
        (**self).read_batch()
    }
    fn commit_offsets<'a>(&'a mut self, marker: &'a CommitMarker) -> BoxFuture<'a, anyhow::Result<()>> {
        (**self).commit_offsets(marker)
    }
}
