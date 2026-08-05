use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::types::table_data::TableWrite;

/// Sink trait: writes accumulated Arrow batches to a destination.
///
/// The table name arrives **pre-resolved** inside [`TableWrite`] — the
/// sink does not transform or suffix it. The sink is entirely unaware of
/// provider-specific semantics.
pub trait Sink: Send + Sync {
    /// Write all batches of a [`TableWrite`] into the target table.
    fn write<'a>(&'a self, write: TableWrite) -> BoxFuture<'a, anyhow::Result<()>>;
}

/// Delegating impl: `Arc<dyn Sink>` is itself a `Sink`.
impl Sink for Arc<dyn Sink> {
    fn write<'a>(&'a self, write: TableWrite) -> BoxFuture<'a, anyhow::Result<()>> {
        (**self).write(write)
    }
}
