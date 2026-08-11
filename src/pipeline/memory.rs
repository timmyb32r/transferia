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
    source_used: AtomicUsize,
    transform_used: AtomicUsize,
    changed: Notify,
}

/// A cloneable lease: the bytes are released when the last owner disappears.
#[derive(Clone)]
pub struct MemoryReservation {
    lease: Arc<MemoryLease>,
}

struct MemoryLease {
    bytes: AtomicUsize,
    kind: ReservationKind,
    memory: Arc<MemoryInner>,
}

#[derive(Clone, Copy, Debug)]
enum ReservationKind {
    Source,
    Transform,
}

impl PipelineMemory {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0, "pipeline memory limit must be positive");
        Self {
            inner: Arc::new(MemoryInner {
                limit,
                used: AtomicUsize::new(0),
                source_used: AtomicUsize::new(0),
                transform_used: AtomicUsize::new(0),
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

    /// Bytes retained by source buffers which the parser can consume to make
    /// progress. They must not prevent the parser from taking its next input.
    #[must_use]
    pub fn source_used(&self) -> usize {
        self.inner.source_used.load(Ordering::Acquire)
    }

    /// Bytes retained at or after the transform stage. Unlike queued source
    /// bytes, these can only be released by downstream progress.
    #[must_use]
    pub fn transform_used(&self) -> usize {
        self.inner.transform_used.load(Ordering::Acquire)
    }

    /// Whether retained pipeline data has reached the shared admission limit.
    #[must_use]
    pub fn is_pressured(&self) -> bool {
        self.used() >= self.limit()
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
                    self.inner.source_used.fetch_add(bytes, Ordering::AcqRel);
                    return MemoryReservation {
                        lease: Arc::new(MemoryLease {
                            bytes: AtomicUsize::new(bytes),
                            kind: ReservationKind::Source,
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
    /// that must consume them. The worker calls
    /// [`Self::wait_transform_below_limit`] before starting the next transform,
    /// so the overage cannot cascade.
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
        self.inner.transform_used.fetch_add(bytes, Ordering::AcqRel);
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
                bytes: AtomicUsize::new(bytes),
                kind: ReservationKind::Transform,
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

    /// Wait only for downstream capacity. Queued source reservations are
    /// deliberately excluded: consuming those buffers is how the parser frees
    /// them, so waiting on total usage here can deadlock the pipeline.
    pub async fn wait_transform_below_limit(&self) {
        loop {
            let changed = self.inner.changed.notified();
            if self.transform_used() < self.inner.limit {
                return;
            }
            changed.await;
        }
    }
}

impl MemoryReservation {
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.lease.bytes.load(Ordering::Acquire)
    }

    /// Reduce accounting after a peak allocation has been released. This is
    /// useful for decompression, where compressed and decoded buffers overlap
    /// briefly but only the decoded bytes continue through the pipeline.
    ///
    /// Returns whether the reservation was reduced. Reservations never grow.
    #[must_use]
    pub fn shrink_to(&self, bytes: usize) -> bool {
        let bytes = bytes.max(1);
        let reduced =
            self.lease
                .bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (bytes < current).then_some(bytes)
                });
        let Ok(previous) = reduced else {
            return false;
        };
        let released = previous - bytes;
        self.lease.memory.used.fetch_sub(released, Ordering::AcqRel);
        match self.lease.kind {
            ReservationKind::Source => {
                self.lease
                    .memory
                    .source_used
                    .fetch_sub(released, Ordering::AcqRel);
            }
            ReservationKind::Transform => {
                self.lease
                    .memory
                    .transform_used
                    .fetch_sub(released, Ordering::AcqRel);
            }
        }
        self.lease.memory.changed.notify_waiters();
        true
    }
}

impl core::fmt::Debug for MemoryReservation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        let bytes = self.bytes.load(Ordering::Acquire);
        self.memory.used.fetch_sub(bytes, Ordering::AcqRel);
        match self.kind {
            ReservationKind::Source => {
                self.memory.source_used.fetch_sub(bytes, Ordering::AcqRel);
            }
            ReservationKind::Transform => {
                self.memory
                    .transform_used
                    .fetch_sub(bytes, Ordering::AcqRel);
            }
        }
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
        assert_eq!(memory.source_used(), 60);
        assert_eq!(memory.transform_used(), 0);
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

    #[tokio::test]
    async fn pressure_starts_at_the_limit_and_clears_on_release() {
        let memory = PipelineMemory::new(10);
        let lease = memory.reserve(10).await;
        assert!(memory.is_pressured());
        drop(lease);
        assert!(!memory.is_pressured());
    }

    #[tokio::test]
    async fn transform_admission_ignores_queued_source_bytes() {
        let memory = PipelineMemory::new(10);
        let source = memory.reserve(10).await;
        tokio::time::timeout(
            core::time::Duration::from_millis(50),
            memory.wait_transform_below_limit(),
        )
        .await
        .expect("source bytes must not block the parser");
        assert_eq!(memory.source_used(), 10);
        assert_eq!(memory.transform_used(), 0);
        drop(source);
    }

    #[tokio::test]
    async fn shrinking_peak_reservation_updates_stage_and_total_usage() {
        let memory = PipelineMemory::new(100);
        let lease = memory.reserve(80).await;
        let clone = lease.clone();
        assert!(clone.shrink_to(30));
        assert_eq!(lease.bytes(), 30);
        assert_eq!(memory.used(), 30);
        assert_eq!(memory.source_used(), 30);
        assert!(!lease.shrink_to(40));
        drop(lease);
        assert_eq!(memory.used(), 30);
        drop(clone);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn transform_usage_is_accounted_separately() {
        let memory = PipelineMemory::new(10);
        let lease = memory.reserve_transform(12);
        assert_eq!(memory.used(), 12);
        assert_eq!(memory.source_used(), 0);
        assert_eq!(memory.transform_used(), 12);
        assert!(tokio::time::timeout(
            core::time::Duration::from_millis(20),
            memory.wait_transform_below_limit(),
        )
        .await
        .is_err());
        drop(lease);
        memory.wait_transform_below_limit().await;
    }
}
