use std::sync::Arc;

use serde::{Deserialize, Serialize};
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableStorage};

use crate::connectors::postgres::source::{DiscoveredTable, TableConfig};
use crate::connectors::postgres::src_stream::{
    authoritative_table_identities, replication_safety_violation, AuthoritativeTableIdentity,
    LogicalDecoder, PostgresSourceIdentity,
};

const STATE_VERSION: u8 = 3;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "phase", deny_unknown_fields)]
enum PersistedPhase {
    Claimed,
    Snapshot { consistent_lsn: u64 },
    Streaming { start_lsn: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedState {
    version: u8,

    replay_identity: String,

    slot: String,

    plugin: String,

    publication: Option<String>,

    tables: Vec<PersistedTable>,

    source: PostgresSourceIdentity,

    authoritative_tables: Option<Vec<AuthoritativeTableIdentity>>,

    state: PersistedPhase,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedTable {
    schema: String,
    name: String,
}

pub enum SnapshotStreamPreparation {
    Create(SnapshotStreamTracker),
    Streaming {
        tracker: SnapshotStreamTracker,
        start_lsn: u64,
    },
}

pub struct SnapshotStreamTracker {
    storage: Arc<dyn DurableStorage>,
    key: String,
    revision: u64,
    identity: PersistedState,
}

impl SnapshotStreamTracker {
    pub(crate) async fn claim_or_resume(
        decoder: &LogicalDecoder,
        tables: &[TableConfig],
        source: &PostgresSourceIdentity,
        durable: DurableContext,
        slot_exists: bool,
        replay_identity: &str,
    ) -> anyhow::Result<SnapshotStreamPreparation> {
        if replay_identity.is_empty() {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL batch_and_stream replay identity must not be empty"
            )));
        }
        let slot = super::super::src_stream::replication_slot(&durable.delivery_id)?.to_owned();
        let identity = persisted_state(
            decoder,
            tables,
            source,
            replay_identity,
            PersistedPhase::Claimed,
            &slot,
        );
        let key = format!("postgres-snapshot-stream-{slot}");
        if let Some(current) = durable.storage.read(&key).await? {
            let persisted = decode_and_validate(&current.payload, &identity)
                .map_err(replication_safety_violation)?;
            match persisted.state.clone() {
                PersistedPhase::Streaming { start_lsn } => {
                    return Ok(SnapshotStreamPreparation::Streaming {
                        tracker: Self {
                            storage: durable.storage,
                            key,
                            revision: current.revision,
                            identity: persisted,
                        },
                        start_lsn,
                    });
                }
                PersistedPhase::Claimed => {
                    if slot_exists {
                        return Err(replication_safety_violation(anyhow::anyhow!(
                            "PostgreSQL batch_and_stream snapshot bootstrap was interrupted before its exact WAL boundary was persisted and replication slot '{slot}' still exists; remove that exact slot deliberately before retrying"
                        )));
                    }
                    let payload = serde_json::to_vec(&identity)?;
                    let applied = durable
                        .storage
                        .compare_exchange(&key, Some(current.revision), &payload)
                        .await?;
                    let CompareExchangeResult::Applied(value) = applied else {
                        return Err(replication_safety_violation(anyhow::anyhow!(
                            "PostgreSQL batch_and_stream durable state changed while recycling an interrupted slot-free bootstrap"
                        )));
                    };
                    return Ok(SnapshotStreamPreparation::Create(Self {
                        storage: durable.storage,
                        key,
                        revision: value.revision,
                        identity,
                    }));
                }
                PersistedPhase::Snapshot { .. } => {
                    let message = if slot_exists {
                        format!(
                            "PostgreSQL batch_and_stream snapshot was interrupted before its exact WAL handoff and replication slot '{slot}' still exists; the exported snapshot cannot survive process loss, so reset the destination snapshot attempt and remove that exact slot deliberately before retrying"
                        )
                    } else {
                        format!(
                            "PostgreSQL batch_and_stream snapshot was interrupted before its exact WAL handoff and replication slot '{slot}' is absent; refusing to bootstrap a new snapshot because the destination may contain rows committed from the previous snapshot attempt, so reset that destination attempt deliberately before retrying"
                        )
                    };
                    return Err(replication_safety_violation(anyhow::Error::msg(message)));
                }
            }
        }

        if slot_exists {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL replication slot '{slot}' already exists without matching batch_and_stream durable ownership; refusing to replace or use it"
            )));
        }

        let payload = serde_json::to_vec(&identity)?;
        let applied = durable
            .storage
            .compare_exchange(&key, None, &payload)
            .await?;
        let CompareExchangeResult::Applied(value) = applied else {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL batch_and_stream durable state was claimed by another execution"
            )));
        };
        Ok(SnapshotStreamPreparation::Create(Self {
            storage: durable.storage,
            key,
            revision: value.revision,
            identity,
        }))
    }

    pub(crate) async fn mark_snapshot_ready(
        &mut self,
        consistent_lsn: u64,
        tables: &[DiscoveredTable],
    ) -> anyhow::Result<()> {
        if !matches!(&self.identity.state, PersistedPhase::Claimed) {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL batch_and_stream state is not claimed"
            )));
        }
        let authoritative_tables =
            authoritative_table_identities(tables).map_err(replication_safety_violation)?;
        self.store(
            PersistedPhase::Snapshot { consistent_lsn },
            Some(authoritative_tables),
        )
        .await
    }

    pub(crate) async fn mark_streaming(&mut self) -> anyhow::Result<u64> {
        let consistent_lsn = match &self.identity.state {
            PersistedPhase::Snapshot { consistent_lsn } => *consistent_lsn,
            PersistedPhase::Claimed | PersistedPhase::Streaming { .. } => {
                return Err(replication_safety_violation(anyhow::anyhow!(
                    "PostgreSQL batch_and_stream snapshot phase is not ready"
                )));
            }
        };
        if self.identity.authoritative_tables.is_none() {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL batch_and_stream authoritative schema is missing"
            )));
        }
        self.store(
            PersistedPhase::Streaming {
                start_lsn: consistent_lsn,
            },
            self.identity.authoritative_tables.clone(),
        )
        .await?;
        Ok(consistent_lsn)
    }

    pub(crate) fn validate_authoritative_tables(
        &self,
        tables: &[DiscoveredTable],
    ) -> anyhow::Result<()> {
        let expected = self.identity.authoritative_tables.as_ref().ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL batch_and_stream authoritative schema is missing"
            ))
        })?;
        let actual =
            authoritative_table_identities(tables).map_err(replication_safety_violation)?;
        if actual != *expected {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL batch_and_stream authoritative table schema changed after the exact snapshot boundary"
            )));
        }
        Ok(())
    }

    pub(crate) const fn streaming_lsn(&self) -> Option<u64> {
        match &self.identity.state {
            PersistedPhase::Streaming { start_lsn } => Some(*start_lsn),
            PersistedPhase::Claimed | PersistedPhase::Snapshot { .. } => None,
        }
    }

    async fn store(
        &mut self,
        state: PersistedPhase,
        authoritative_tables: Option<Vec<AuthoritativeTableIdentity>>,
    ) -> anyhow::Result<()> {
        let next = PersistedState {
            authoritative_tables,
            state,
            ..self.identity.clone()
        };
        let payload = serde_json::to_vec(&next)?;
        match self
            .storage
            .compare_exchange(&self.key, Some(self.revision), &payload)
            .await?
        {
            CompareExchangeResult::Applied(value) => {
                self.revision = value.revision;
                self.identity = next;
                Ok(())
            }
            CompareExchangeResult::Conflict(_) => {
                Err(replication_safety_violation(anyhow::anyhow!(
                    "PostgreSQL batch_and_stream durable phase was modified by another execution"
                )))
            }
        }
    }
}

fn persisted_state(
    decoder: &LogicalDecoder,
    tables: &[TableConfig],
    source: &PostgresSourceIdentity,
    replay_identity: &str,
    state: PersistedPhase,
    slot: &str,
) -> PersistedState {
    PersistedState {
        version: STATE_VERSION,
        replay_identity: replay_identity.to_owned(),
        slot: slot.to_owned(),
        plugin: decoder.plugin().to_owned(),
        publication: match decoder {
            LogicalDecoder::Pgoutput { publication } => Some(publication.clone()),
            LogicalDecoder::Wal2Json => None,
        },
        tables: tables
            .iter()
            .map(|table| PersistedTable {
                schema: table.schema.clone(),
                name: table.name.clone(),
            })
            .collect(),
        source: source.clone(),
        authoritative_tables: None,
        state,
    }
}

fn decode_and_validate(
    payload: &[u8],
    expected: &PersistedState,
) -> anyhow::Result<PersistedState> {
    let actual: PersistedState = serde_json::from_slice(payload)?;
    anyhow::ensure!(
        actual.version == STATE_VERSION,
        "unsupported PostgreSQL batch_and_stream state version {}",
        actual.version
    );
    anyhow::ensure!(
        matches!(&actual.state, PersistedPhase::Claimed) == actual.authoritative_tables.is_none(),
        "PostgreSQL batch_and_stream durable state has inconsistent phase and authoritative schema"
    );
    anyhow::ensure!(
        actual.slot == expected.slot
            && actual.replay_identity == expected.replay_identity
            && actual.plugin == expected.plugin
            && actual.publication == expected.publication
            && actual.tables == expected.tables
            && actual.source == expected.source,
        "PostgreSQL batch_and_stream durable state belongs to different replay-affecting delivery or source settings"
    );
    Ok(actual)
}

#[cfg(test)]
#[path = "tests/phase.rs"]
mod tests;
