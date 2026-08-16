use alloc::sync::Arc;

use futures_util::future::BoxFuture;

use crate::data::message::SourceBatch;
use crate::failure::DataPlaneResult;

// ---------------------------------------------------------------------------
// CommitMarker
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct CommitMarker {
    value: Arc<dyn core::any::Any + Send + Sync>,
    type_name: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitMarkerTypeMismatch {
    expected: &'static str,
    actual: &'static str,
}

impl core::fmt::Display for CommitMarkerTypeMismatch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "commit marker type mismatch: expected '{}', received '{}'",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for CommitMarkerTypeMismatch {}

impl CommitMarker {
    pub fn new<T: core::any::Any + Send + Sync>(marker: T) -> Self {
        Self {
            value: Arc::new(marker),
            type_name: core::any::type_name::<T>(),
        }
    }

    pub fn value<T: 'static>(&self) -> Result<&T, CommitMarkerTypeMismatch> {
        self.value
            .downcast_ref::<T>()
            .ok_or_else(|| CommitMarkerTypeMismatch {
                expected: core::any::type_name::<T>(),
                actual: self.type_name,
            })
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
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>>;
    /// Commit one durability group. Implementations must submit every marker
    /// in the slice as one source-side commit operation.
    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, DataPlaneResult<()>>;
}

/// Delegating impl: `Box<dyn Source>` is itself a `Source`.
impl Source for Box<dyn Source> {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        (**self).read_batch()
    }
    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, DataPlaneResult<()>> {
        (**self).commit_offsets(markers)
    }
}

#[cfg(test)]
#[path = "tests/source.rs"]
mod tests;
