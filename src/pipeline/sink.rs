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
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Downcast to concrete type for startup checks. Override in concrete sinks.
    /// Default panics — only `ClickHouseSink` and `PoisoningSink` override this.
    fn as_any(&self) -> &dyn core::any::Any;
}

/// Delegating impl: `Arc<dyn Sink>` is itself a `Sink`.
impl Sink for Arc<dyn Sink> {
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        (**self).write(write)
    }
    fn as_any(&self) -> &dyn core::any::Any {
        (**self).as_any()
    }
}
