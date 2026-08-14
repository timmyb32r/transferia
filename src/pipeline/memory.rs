use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
    progress_source_active: AtomicUsize,
    active_transform: AtomicUsize,
    changed: Notify,
}

/// A cloneable lease: the bytes are released when the last owner disappears.
#[derive(Clone)]
pub struct MemoryReservation {
    lease: Arc<MemoryLease>,
}

/// Exclusive reservation for a transform currently materializing its output.
///
/// It is converted into a normal retained-transform lease without a temporary
/// drop/re-add gap once the exact output size is known.
pub struct ActiveTransformReservation {
    memory: Arc<MemoryInner>,
    bytes: usize,
    finished: bool,
}

struct MemoryLease {
    bytes: AtomicUsize,
    resize: Mutex<()>,
    kind: ReservationKind,
    memory: Arc<MemoryInner>,
}

#[derive(Clone, Copy, Debug)]
enum ReservationKind {
    Source,
    ProgressSource,
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
                progress_source_active: AtomicUsize::new(0),
                active_transform: AtomicUsize::new(0),
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

    /// Whether retained downstream/transform data has reached the admission limit.
    /// Source read-ahead is deliberately excluded: it should throttle new source
    /// reads, not force sink flushes or stop the parser from consuming that input.
    #[must_use]
    pub fn is_transform_pressured(&self) -> bool {
        self.transform_used() >= self.limit()
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
                            resize: Mutex::new(()),
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

    /// Reserve one source read that must be admitted to let the pipeline make progress.
    ///
    /// The returned lease is globally serialized within this per-partition budget: another call
    /// waits until every clone is dropped. It may cross the limit because downstream transform
    /// bytes can otherwise prevent the source from producing the rows that close and release
    /// them. The full read remains accounted and continues to pressure ordinary reservations.
    #[expect(
        clippy::expect_used,
        reason = "a usize-sized process cannot reserve more than usize::MAX bytes"
    )]
    pub(crate) async fn reserve_progress_source(&self, bytes: usize) -> MemoryReservation {
        let bytes = bytes.max(1);
        loop {
            let changed = self.inner.changed.notified();
            if self
                .inner
                .progress_source_active
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            changed.await;
        }
        let before = self
            .inner
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
            })
            .expect("pipeline memory accounting overflow");
        let after = before + bytes;
        self.inner.source_used.fetch_add(bytes, Ordering::AcqRel);
        if after > self.inner.limit {
            tracing::warn!(
                required_bytes = bytes,
                used_before = before,
                used_after = after,
                limit_bytes = self.inner.limit,
                "progress-critical pipeline source read temporarily exceeded memory budget",
            );
        }
        MemoryReservation {
            lease: Arc::new(MemoryLease {
                bytes: AtomicUsize::new(bytes),
                resize: Mutex::new(()),
                kind: ReservationKind::ProgressSource,
                memory: Arc::clone(&self.inner),
            }),
        }
    }

    /// Reserve the output of an already-running transform. It may cross the
    /// limit once: otherwise queued input reservations can deadlock the worker
    /// that must consume them. The worker calls
    /// [`Self::wait_transform_below_limit`] before starting the next transform,
    /// so the overage cannot cascade.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "a usize-sized process cannot reserve more than usize::MAX bytes"
    )]
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
                resize: Mutex::new(()),
                kind: ReservationKind::Transform,
                memory: Arc::clone(&self.inner),
            }),
        }
    }

    /// Admit one transform while respecting retained downstream pressure.
    ///
    /// Source bytes are deliberately excluded: consuming them is what releases
    /// that memory. If retained transform bytes are below the limit but this one
    /// estimate does not fit, exactly one active transform is admitted as a
    /// progress exception; it may be the delivery that closes an S3 epoch.
    #[expect(
        clippy::expect_used,
        reason = "a usize-sized process cannot reserve more than usize::MAX bytes"
    )]
    pub async fn admit_active_transform(&self, bytes: usize) -> ActiveTransformReservation {
        let bytes = bytes.max(1);
        loop {
            let changed = self.inner.changed.notified();
            let retained = self.inner.transform_used.load(Ordering::Acquire);
            if retained < self.inner.limit
                && self
                    .inner
                    .active_transform
                    .compare_exchange(0, bytes, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.inner
                    .used
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        used.checked_add(bytes)
                    })
                    .expect("pipeline memory accounting overflow");
                if retained.saturating_add(bytes) > self.inner.limit {
                    tracing::warn!(
                        required_bytes = bytes,
                        retained_transform_bytes = retained,
                        limit_bytes = self.inner.limit,
                        "progress-critical parser transform temporarily exceeded memory budget",
                    );
                }
                return ActiveTransformReservation {
                    memory: Arc::clone(&self.inner),
                    bytes,
                    finished: false,
                };
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
            if !self.is_transform_pressured() {
                return;
            }
            changed.await;
        }
    }
}

impl ActiveTransformReservation {
    /// Replace the conservative active estimate with the exact retained output.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "a usize-sized process cannot reserve more than usize::MAX bytes"
    )]
    pub fn finish(mut self, bytes: usize) -> MemoryReservation {
        let bytes = bytes.max(1);
        if bytes >= self.bytes {
            self.memory
                .used
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_add(bytes - self.bytes)
                })
                .expect("pipeline memory accounting overflow");
        } else {
            self.memory
                .used
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        }
        self.memory
            .transform_used
            .fetch_add(bytes, Ordering::AcqRel);
        self.memory.active_transform.store(0, Ordering::Release);
        self.memory.changed.notify_waiters();
        self.finished = true;
        MemoryReservation {
            lease: Arc::new(MemoryLease {
                bytes: AtomicUsize::new(bytes),
                resize: Mutex::new(()),
                kind: ReservationKind::Transform,
                memory: Arc::clone(&self.memory),
            }),
        }
    }
}

impl Drop for ActiveTransformReservation {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.memory.used.fetch_sub(self.bytes, Ordering::AcqRel);
        self.memory.active_transform.store(0, Ordering::Release);
        self.memory.changed.notify_waiters();
    }
}

impl MemoryReservation {
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.lease.bytes.load(Ordering::Acquire)
    }

    /// Grow the single progress-source lease to cover overlapping raw and decoded buffers.
    /// This is synchronous because acquiring the lease already proved it is the only bypass
    /// allocation in this per-partition budget.
    pub(crate) fn grow_progress_source_to(&self, bytes: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(self.lease.kind, ReservationKind::ProgressSource),
            "only a progress-source reservation can grow"
        );
        let bytes = bytes.max(1);
        let _resize = self
            .lease
            .resize
            .lock()
            .map_err(|_| anyhow::anyhow!("pipeline memory lease resize state is poisoned"))?;
        let previous = self.lease.bytes.load(Ordering::Acquire);
        if bytes <= previous {
            return Ok(());
        }
        let added = bytes - previous;
        self.lease
            .memory
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(added)
            })
            .map_err(|_| anyhow::anyhow!("pipeline memory accounting overflow"))?;
        self.lease
            .memory
            .source_used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(added)
            })
            .map_err(|_| anyhow::anyhow!("pipeline source memory accounting overflow"))?;
        self.lease.bytes.store(bytes, Ordering::Release);
        if self.lease.memory.used.load(Ordering::Acquire) > self.lease.memory.limit {
            tracing::warn!(
                required_bytes = bytes,
                limit_bytes = self.lease.memory.limit,
                "progress-critical source transform temporarily exceeded memory budget",
            );
        }
        Ok(())
    }

    /// Reduce accounting after a peak allocation has been released. This is
    /// useful for decompression, where compressed and decoded buffers overlap
    /// briefly but only the decoded bytes continue through the pipeline.
    ///
    /// Returns whether the reservation was reduced. This operation never grows it.
    #[must_use]
    pub fn shrink_to(&self, bytes: usize) -> bool {
        let bytes = bytes.max(1);
        let Ok(_resize) = self.lease.resize.lock() else {
            return false;
        };
        let previous = self.lease.bytes.load(Ordering::Acquire);
        if bytes >= previous {
            return false;
        }
        self.lease.bytes.store(bytes, Ordering::Release);
        let released = previous - bytes;
        self.lease.memory.used.fetch_sub(released, Ordering::AcqRel);
        match self.lease.kind {
            ReservationKind::Source | ReservationKind::ProgressSource => {
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
            ReservationKind::Source | ReservationKind::ProgressSource => {
                self.memory.source_used.fetch_sub(bytes, Ordering::AcqRel);
            }
            ReservationKind::Transform => {
                self.memory
                    .transform_used
                    .fetch_sub(bytes, Ordering::AcqRel);
            }
        }
        if matches!(self.kind, ReservationKind::ProgressSource) {
            self.memory
                .progress_source_active
                .store(0, Ordering::Release);
        }
        self.memory.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "tests/memory.rs"]
mod tests;
