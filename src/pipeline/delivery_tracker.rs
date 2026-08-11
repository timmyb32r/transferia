use std::collections::BTreeMap;

use super::sink::DeliveryId;

struct DeliveryState<H> {
    remaining: usize,
    source_messages: u64,
    hold: Option<H>,
}

pub struct Committed {
    pub through: DeliveryId,
    pub source_messages: u64,
}

/// Ordered completion accounting shared by durable sink actors.
pub struct DeliveryTracker<H> {
    entries: BTreeMap<DeliveryId, DeliveryState<H>>,
    next_received: DeliveryId,
    next_commit: DeliveryId,
}

impl<H> DeliveryTracker<H> {
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            next_received: DeliveryId::new(1),
            next_commit: DeliveryId::new(1),
        }
    }

    pub fn accept(
        &mut self,
        id: DeliveryId,
        remaining: usize,
        source_messages: u64,
        hold: Option<H>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            id == self.next_received,
            "sink delivery order violation: expected {}, got {}",
            self.next_received.get(),
            id.get()
        );
        self.next_received = self.next_received.next();
        let hold = if remaining == 0 { None } else { hold };
        self.entries.insert(
            id,
            DeliveryState {
                remaining,
                source_messages,
                hold,
            },
        );
        Ok(())
    }

    pub fn complete(&mut self, id: DeliveryId, units: usize) -> anyhow::Result<()> {
        let state = self
            .entries
            .get_mut(&id)
            .ok_or_else(|| anyhow::anyhow!("missing delivery progress {}", id.get()))?;
        state.remaining = state
            .remaining
            .checked_sub(units)
            .ok_or_else(|| anyhow::anyhow!("delivery progress underflow"))?;
        if state.remaining == 0 {
            state.hold = None;
        }
        Ok(())
    }

    pub fn take_committed(&mut self) -> Option<Committed> {
        let mut through = None;
        let mut source_messages = 0_u64;
        while self
            .entries
            .get(&self.next_commit)
            .is_some_and(|state| state.remaining == 0)
        {
            let state = self
                .entries
                .remove(&self.next_commit)
                .expect("completed delivery disappeared");
            source_messages = source_messages.saturating_add(state.source_messages);
            through = Some(self.next_commit);
            self.next_commit = self.next_commit.next();
        }
        through.map(|through| Committed {
            through,
            source_messages,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct Hold(Arc<AtomicUsize>);

    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn commits_only_the_contiguous_completed_prefix() -> anyhow::Result<()> {
        let mut tracker = DeliveryTracker::<()>::new();
        tracker.accept(DeliveryId::new(1), 2, 3, None)?;
        tracker.accept(DeliveryId::new(2), 1, 5, None)?;
        tracker.complete(DeliveryId::new(2), 1)?;
        assert!(tracker.take_committed().is_none());

        tracker.complete(DeliveryId::new(1), 2)?;
        let committed = tracker.take_committed().expect("completed prefix");
        assert_eq!(committed.through, DeliveryId::new(2));
        assert_eq!(committed.source_messages, 8);
        assert!(tracker.is_empty());
        Ok(())
    }

    #[test]
    fn releases_hold_as_soon_as_delivery_work_completes() -> anyhow::Result<()> {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut tracker = DeliveryTracker::new();
        tracker.accept(DeliveryId::new(1), 2, 1, Some(Hold(Arc::clone(&drops))))?;

        tracker.complete(DeliveryId::new(1), 2)?;

        assert_eq!(drops.load(Ordering::Relaxed), 1);
        Ok(())
    }
}
