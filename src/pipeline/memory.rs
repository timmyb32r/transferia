use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// Per-partition admission budget. Reservations follow owned buffers through
/// the pipeline; retaining data in the sink therefore throttles `PQv1` upstream.
#[derive(Clone)]
pub struct PipelineMemory {
    inner: Arc<MemoryInner>,
}

struct MemoryInner {
    limit: usize,
    used: AtomicUsize,
    changed: Notify,
}

/// A cloneable lease: the bytes are released when the last owner disappears.
#[derive(Clone)]
pub struct MemoryReservation {
    lease: Arc<MemoryLease>,
}

struct MemoryLease {
    bytes: usize,
    memory: Arc<MemoryInner>,
}

impl PipelineMemory {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0, "pipeline memory limit must be positive");
        Self {
            inner: Arc::new(MemoryInner {
                limit,
                used: AtomicUsize::new(0),
                changed: Notify::new(),
            }),
        }
    }

    #[must_use]
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    #[must_use]
    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Wait for byte capacity. A single oversized allocation is admitted only
    /// when the budget is otherwise empty, then blocks every subsequent one.
    pub async fn reserve(&self, bytes: usize) -> MemoryReservation {
        let bytes = bytes.max(1);
        let oversized = bytes > self.inner.limit;
        let mut warned = false;
        loop {
            // Register before observing `used`: otherwise a release between
            // the load and `notified().await` could be missed.
            let changed = self.inner.changed.notified();
            let used = self.inner.used.load(Ordering::Acquire);
            let fits = if oversized {
                used == 0
            } else {
                used.checked_add(bytes)
                    .is_some_and(|next| next <= self.inner.limit)
            };
            if fits {
                let next = used.saturating_add(bytes);
                if self
                    .inner
                    .used
                    .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    if oversized && !warned {
                        tracing::warn!(
                            required_bytes = bytes,
                            limit_bytes = self.inner.limit,
                            "single pipeline allocation exceeds memory budget; admitting it alone",
                        );
                    }
                    return MemoryReservation {
                        lease: Arc::new(MemoryLease {
                            bytes,
                            memory: Arc::clone(&self.inner),
                        }),
                    };
                }
                continue;
            }
            warned |= oversized;
            changed.await;
        }
    }

    /// Reserve the output of an already-running transform. It may cross the
    /// limit once: otherwise queued input reservations can deadlock the worker
    /// that must consume them. The worker calls [`Self::wait_below_limit`]
    /// before starting the next transform, so the overage cannot cascade.
    #[must_use]
    pub fn reserve_transform(&self, bytes: usize) -> MemoryReservation {
        let bytes = bytes.max(1);
        let before = self
            .inner
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
            })
            .expect("pipeline memory accounting overflow");
        let after = before + bytes;
        if after > self.inner.limit {
            tracing::warn!(
                required_bytes = bytes,
                used_before = before,
                used_after = after,
                limit_bytes = self.inner.limit,
                "pipeline transform temporarily exceeded memory budget",
            );
        }
        MemoryReservation {
            lease: Arc::new(MemoryLease {
                bytes,
                memory: Arc::clone(&self.inner),
            }),
        }
    }

    pub async fn wait_below_limit(&self) {
        loop {
            let changed = self.inner.changed.notified();
            if self.used() < self.inner.limit {
                return;
            }
            changed.await;
        }
    }
}

impl MemoryReservation {
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.lease.bytes
    }
}

impl core::fmt::Debug for MemoryReservation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("bytes", &self.lease.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        self.memory.used.fetch_sub(self.bytes, Ordering::AcqRel);
        self.memory.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reservation_is_released_by_last_clone() {
        let memory = PipelineMemory::new(100);
        let lease = memory.reserve(60).await;
        let clone = lease.clone();
        assert_eq!(memory.used(), 60);
        drop(lease);
        assert_eq!(memory.used(), 60);
        drop(clone);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn oversized_allocation_is_admitted_alone() {
        let memory = PipelineMemory::new(10);
        let lease = memory.reserve(20).await;
        assert_eq!(memory.used(), 20);
        drop(lease);
        assert_eq!(memory.used(), 0);
    }
}
