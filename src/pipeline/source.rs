use async_trait::async_trait;
use std::sync::Arc;

use crate::types::MessageBatch;

// ---------------------------------------------------------------------------
// CommitMarker
// ---------------------------------------------------------------------------

/// Opaque commit marker that wraps the SDK-specific marker type.
/// Implementations store per-batch markers and pass them to commit.
#[derive(Clone)]
pub struct CommitMarker(Arc<dyn std::any::Any + Send + Sync>);

impl CommitMarker {
    /// Wrap an SDK-specific commit marker (e.g., `TopicReaderCommitMarker`).
    pub fn new<T: std::any::Any + Send + Sync>(marker: T) -> Self {
        Self(Arc::new(marker))
    }

    /// Try to downcast to the concrete SDK marker type.
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
// Source trait
// ---------------------------------------------------------------------------

/// The Source trait represents a data source that produces batches of messages.
///
/// Implementations are per-partition (one Source instance per partition).
/// The trait is async and fallible, with the caller responsible for retries.
#[async_trait]
pub trait Source: Send {
    /// Read the next batch of messages from the source.
    ///
    /// Returns a `MessageBatch`. An empty `MessageBatch` (`messages.is_empty()`)
    /// indicates that no messages were available (the underlying SDK returned an
    /// empty batch). This is **not** an error -- the caller should retry after a
    /// short sleep.
    async fn read_batch(&mut self) -> anyhow::Result<MessageBatch>;

    /// Commit offsets using the marker obtained during `read_batch()`.
    ///
    /// Called after a successful sink write to implement at-least-once semantics.
    async fn commit_offsets(&mut self, marker: &CommitMarker) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Blanket impl for &mut T
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Source + ?Sized> Source for &mut T {
    async fn read_batch(&mut self) -> anyhow::Result<MessageBatch> {
        (**self).read_batch().await
    }

    async fn commit_offsets(&mut self, marker: &CommitMarker) -> anyhow::Result<()> {
        (**self).commit_offsets(marker).await
    }
}

#[async_trait]
impl<T: Source + Send + Sync + ?Sized> Source for Box<T> {
    async fn read_batch(&mut self) -> anyhow::Result<MessageBatch> {
        (**self).read_batch().await
    }

    async fn commit_offsets(&mut self, marker: &CommitMarker) -> anyhow::Result<()> {
        (**self).commit_offsets(marker).await
    }
}
