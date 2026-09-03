use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use transferia_core::sink::{DeliveryId, SinkBatch};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputDatasetRowCount {
    pub table: Arc<str>,

    pub is_dlq: bool,

    pub rows: u64,
}

/// Exact post-parser row totals for one finite pipeline attempt.
///
/// Parsed deliveries remain pending until both the sink acknowledgement and
/// source commit succeed. The runner merges committed prefixes from failed
/// attempts before restarting, so retries neither lose nor double-count rows.
pub struct OutputRowCounts {
    state: Mutex<OutputRowCountState>,
}

struct OutputRowCountState {
    committed: BTreeMap<(bool, Arc<str>), u64>,
    pending: BTreeMap<DeliveryId, BTreeMap<(bool, Arc<str>), u64>>,
}

impl OutputRowCounts {
    pub fn new(
        datasets: impl IntoIterator<Item = (Arc<str>, bool)>,
    ) -> anyhow::Result<Self> {
        let mut rows = BTreeMap::new();
        for (table, is_dlq) in datasets {
            anyhow::ensure!(
                rows.insert((is_dlq, Arc::clone(&table)), 0).is_none(),
                "snapshot row counter repeats dataset '{}' (dlq={is_dlq})",
                table
            );
        }
        Ok(Self {
            state: Mutex::new(OutputRowCountState {
                committed: rows,
                pending: BTreeMap::new(),
            }),
        })
    }

    pub(crate) fn observe_delivery(
        &self,
        id: DeliveryId,
        outputs: &[SinkBatch],
    ) -> anyhow::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot row counter lock is poisoned"))?;
        anyhow::ensure!(
            !state.pending.contains_key(&id),
            "snapshot row counter observed delivery {} more than once",
            id.get()
        );
        let mut delivery = BTreeMap::new();
        for output in outputs {
            let key = (output.is_dlq, Arc::clone(&output.table));
            anyhow::ensure!(
                state.committed.contains_key(&key),
                "pipeline produced undiscovered dataset '{}' (dlq={})",
                output.table,
                output.is_dlq
            );
            let rows = u64::try_from(output.rows())
                .map_err(|_| anyhow::anyhow!("snapshot output row count does not fit u64"))?;
            let current = delivery.get(&key).copied().unwrap_or(0_u64);
            delivery.insert(
                key,
                current.checked_add(rows).ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot row count overflow for dataset '{}' (dlq={})",
                        output.table,
                        output.is_dlq
                    )
                })?,
            );
        }
        state.pending.insert(id, delivery);
        Ok(())
    }

    pub(crate) fn commit_through(&self, committed: DeliveryId) -> anyhow::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot row counter lock is poisoned"))?;
        let next = validated_committed_counts(&state, committed)?;
        state.committed = next;
        state.pending.retain(|id, _| *id > committed);
        Ok(())
    }

    pub(crate) fn validate_commit_through(&self, committed: DeliveryId) -> anyhow::Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot row counter lock is poisoned"))?;
        validated_committed_counts(&state, committed).map(|_| ())
    }

    pub fn merge(&self, other: &Self) -> anyhow::Result<()> {
        let snapshot = other.snapshot()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot row counter lock is poisoned"))?;
        let mut next = state.committed.clone();
        for count in snapshot {
            let key = (count.is_dlq, Arc::clone(&count.table));
            let current = next.get(&key).copied().ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot row counter cannot merge unknown dataset '{}' (dlq={})",
                    count.table,
                    count.is_dlq
                )
            })?;
            next.insert(
                key,
                current.checked_add(count.rows).ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot row count overflow for dataset '{}' (dlq={})",
                        count.table,
                        count.is_dlq
                    )
                })?,
            );
        }
        state.committed = next;
        Ok(())
    }

    pub fn snapshot(&self) -> anyhow::Result<Vec<OutputDatasetRowCount>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot row counter lock is poisoned"))?;
        Ok(state
            .committed
            .iter()
            .map(|((is_dlq, table), rows)| OutputDatasetRowCount {
                table: Arc::clone(table),
                is_dlq: *is_dlq,
                rows: *rows,
            })
            .collect())
    }
}

fn validated_committed_counts(
    state: &OutputRowCountState,
    committed: DeliveryId,
) -> anyhow::Result<BTreeMap<(bool, Arc<str>), u64>> {
    anyhow::ensure!(
        state.pending.contains_key(&committed),
        "snapshot row counter committed unknown delivery {}",
        committed.get()
    );
    let mut next = state.committed.clone();
    for delivery in state.pending.range(..=committed).map(|(_, rows)| rows) {
        for (key, rows) in delivery {
            let current = next.get(key).copied().ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot row counter lost dataset '{}' (dlq={})",
                    key.1,
                    key.0
                )
            })?;
            next.insert(
                key.clone(),
                current.checked_add(*rows).ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot row count overflow for dataset '{}' (dlq={})",
                        key.1,
                        key.0
                    )
                })?,
            );
        }
    }
    Ok(next)
}
