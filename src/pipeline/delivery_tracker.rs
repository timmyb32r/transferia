use std::collections::BTreeMap;

use super::sink::DeliveryId;

struct DeliveryState {
    remaining: usize,
    source_messages: u64,
}

pub struct Committed {
    pub through: DeliveryId,
    pub source_messages: u64,
}

/// Ordered completion accounting shared by durable sink actors.
pub struct DeliveryTracker {
    entries: BTreeMap<DeliveryId, DeliveryState>,
    next_received: DeliveryId,
    next_commit: DeliveryId,
}

impl DeliveryTracker {
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
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            id == self.next_received,
            "sink delivery order violation: expected {}, got {}",
            self.next_received.get(),
            id.get()
        );
        self.next_received = self.next_received.next();
        self.entries.insert(
            id,
            DeliveryState {
                remaining,
                source_messages,
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
            let state = self.entries.remove(&self.next_commit)?;
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
mod tests;
