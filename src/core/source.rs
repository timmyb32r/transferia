use alloc::sync::Arc;

use futures_util::future::BoxFuture;

use crate::core::data::message::SourceBatch;

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
// Source trait (object-safe via BoxFuture)
// ---------------------------------------------------------------------------

pub trait Source: Send {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>>;
    /// Commit one durability group. Implementations must submit every marker
    /// in the slice as one source-side commit operation.
    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, anyhow::Result<()>>;
}

/// Delegating impl: `Box<dyn Source>` is itself a `Source`.
impl Source for Box<dyn Source> {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        (**self).read_batch()
    }
    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        (**self).commit_offsets(markers)
    }
}
