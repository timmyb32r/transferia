use std::future::Future;
use std::sync::Arc;

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
// Source trait
// ---------------------------------------------------------------------------

pub trait Source: Send {
    fn read_batch(
        &mut self,
    ) -> impl Future<Output = anyhow::Result<MessageBatch>> + Send;

    fn commit_offsets(
        &mut self,
        marker: &CommitMarker,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
