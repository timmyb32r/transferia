use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::pipeline::sink::Sink;
use crate::types::table_data::TableWrite;

/// Sink wrapper with a poison flag and concurrent `write()` guard.
///
/// After the first INSERT error the flag is set — all subsequent `write()` calls
/// immediately return an error. The in-flight guard (`AtomicBool`) panics on
/// detection of a concurrent `write()` call — this is a violation of the
/// "at most one `write()` at a time" invariant (spec §4.1).
pub struct PoisoningSink {
    inner: Arc<dyn Sink>,
    poisoned: AtomicBool,
    write_in_flight: AtomicBool,
}

impl PoisoningSink {
    pub fn new(inner: Arc<dyn Sink>) -> Self {
        Self {
            inner,
            poisoned: AtomicBool::new(false),
            write_in_flight: AtomicBool::new(false),
        }
    }
}

impl Sink for PoisoningSink {
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            // Enforcement: only one write() at a time
            assert!(!self.write_in_flight.swap(true, Ordering::AcqRel), "PoisoningSink: concurrent write() detected \u{2014} waterline corruption risk");
            let result = async {
                if self.poisoned.load(Ordering::Acquire) {
                    anyhow::bail!("sink poisoned by a prior insert failure");
                }
                return self.inner.write(write).await
            }
            .await;
            self.write_in_flight.store(false, Ordering::Release);
            if result.is_err() {
                self.poisoned.store(true, Ordering::Release);
            }
            result
        })
    }

    fn as_any(&self) -> &dyn core::any::Any { self }
}
