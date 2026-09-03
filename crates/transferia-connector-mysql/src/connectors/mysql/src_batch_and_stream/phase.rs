use std::collections::HashSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableStorage};

use super::{
    replication_safety_violation, AuthoritativeColumnIdentity, AuthoritativeTableIdentity,
    MySqlBinlogBoundary, MySqlSourceIdentity, validate_server_uuid,
};
use crate::connectors::mysql::common::validate_identifier;
use crate::connectors::mysql::src_batch::TableConfig;
use crate::connectors::mysql::src_stream::GtidSet;

pub const SNAPSHOT_STREAM_STATE_KEY: &str = "mysql-snapshot-stream";
const STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "phase", deny_unknown_fields)]
enum PersistedPhase {
    Claimed,

    Snapshot { boundary: MySqlBinlogBoundary },

    Streaming { start_boundary: MySqlBinlogBoundary },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedState {
    version: u8,

    replay_identity: String,

    server_id: u32,

    source: MySqlSourceIdentity,

    configured_tables: Vec<String>,

    authoritative_tables: Option<Vec<AuthoritativeTableIdentity>>,

    state: PersistedPhase,
}

pub enum SnapshotStreamPreparation {
    Create(SnapshotStreamTracker),

    Streaming {
        tracker: SnapshotStreamTracker,
        start_boundary: MySqlBinlogBoundary,
    },
}

pub struct SnapshotStreamTracker {
    storage: Arc<dyn DurableStorage>,
    revision: u64,
    identity: PersistedState,
}

impl SnapshotStreamTracker {
    /// Claim or resume one delivery-local snapshot/stream handoff.
    ///
    /// Calling this method is valid only during source preparation, before any
    /// destination side effect. A `Claimed` state can therefore be recycled
    /// after a process loss. A `Snapshot` state cannot: MySQL consistent
    /// snapshots are connection-owned and do not survive process termination.
    pub async fn claim_or_resume(
        server_id: u32,
        tables: &[TableConfig],
        source: &MySqlSourceIdentity,
        durable: DurableContext,
        replay_identity: &str,
    ) -> anyhow::Result<SnapshotStreamPreparation> {
        validate_claim(server_id, tables, source, replay_identity)
            .map_err(replication_safety_violation)?;
        let expected = persisted_state(server_id, tables, source, replay_identity);
        if let Some(current) = durable.storage.read(SNAPSHOT_STREAM_STATE_KEY).await? {
            let persisted = decode_and_validate(&current.payload, &expected)
                .map_err(replication_safety_violation)?;
            match persisted.state.clone() {
                PersistedPhase::Claimed => {
                    let payload = serde_json::to_vec(&expected)?;
                    let applied = durable
                        .storage
                        .compare_exchange(
                            SNAPSHOT_STREAM_STATE_KEY,
                            Some(current.revision),
                            &payload,
                        )
                        .await?;
                    let CompareExchangeResult::Applied(value) = applied else {
                        return Err(replication_safety_violation(anyhow::anyhow!(
                            "MySQL batch_and_stream durable claim changed while recycling a pre-side-effect bootstrap"
                        )));
                    };
                    return Ok(SnapshotStreamPreparation::Create(Self {
                        storage: durable.storage,
                        revision: value.revision,
                        identity: expected,
                    }));
                }
                PersistedPhase::Snapshot { .. } => {
                    return Err(replication_safety_violation(anyhow::anyhow!(
                        "MySQL batch_and_stream snapshot was interrupted after its exact GTID boundary was persisted; connection-owned snapshot sessions cannot survive process loss, so reset the destination snapshot attempt deliberately before retrying"
                    )));
                }
                PersistedPhase::Streaming { start_boundary } => {
                    return Ok(SnapshotStreamPreparation::Streaming {
                        tracker: Self {
                            storage: durable.storage,
                            revision: current.revision,
                            identity: persisted,
                        },
                        start_boundary,
                    });
                }
            }
        }

        let payload = serde_json::to_vec(&expected)?;
        let applied = durable
            .storage
            .compare_exchange(SNAPSHOT_STREAM_STATE_KEY, None, &payload)
            .await?;
        let CompareExchangeResult::Applied(value) = applied else {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL batch_and_stream durable state was claimed by another execution"
            )));
        };
        Ok(SnapshotStreamPreparation::Create(Self {
            storage: durable.storage,
            revision: value.revision,
            identity: expected,
        }))
    }

    pub async fn mark_snapshot_ready(
        &mut self,
        boundary: &MySqlBinlogBoundary,
        authoritative_tables: &[AuthoritativeTableIdentity],
    ) -> anyhow::Result<()> {
        if !matches!(&self.identity.state, PersistedPhase::Claimed) {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL batch_and_stream state is not claimed"
            )));
        }
        validate_boundary(boundary).map_err(replication_safety_violation)?;
        validate_authoritative_identity(&self.identity, authoritative_tables)
            .map_err(replication_safety_violation)?;
        self.store(
            PersistedPhase::Snapshot {
                boundary: boundary.clone(),
            },
            Some(authoritative_tables.to_vec()),
        )
        .await
    }

    pub async fn mark_streaming(&mut self) -> anyhow::Result<MySqlBinlogBoundary> {
        let boundary = match &self.identity.state {
            PersistedPhase::Snapshot { boundary } => boundary.clone(),
            PersistedPhase::Claimed | PersistedPhase::Streaming { .. } => {
                return Err(replication_safety_violation(anyhow::anyhow!(
                    "MySQL batch_and_stream snapshot phase is not ready"
                )));
            }
        };
        let authoritative_tables = self
            .identity
            .authoritative_tables
            .clone()
            .ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL batch_and_stream authoritative schema is missing"
                ))
            })?;
        self.store(
            PersistedPhase::Streaming {
                start_boundary: boundary.clone(),
            },
            Some(authoritative_tables),
        )
        .await?;
        Ok(boundary)
    }

    pub fn streaming_boundary(&self) -> Option<&MySqlBinlogBoundary> {
        match &self.identity.state {
            PersistedPhase::Streaming { start_boundary } => Some(start_boundary),
            PersistedPhase::Claimed | PersistedPhase::Snapshot { .. } => None,
        }
    }

    pub fn validate_authoritative_tables(
        &self,
        authoritative_tables: &[AuthoritativeTableIdentity],
    ) -> anyhow::Result<()> {
        let expected = self
            .identity
            .authoritative_tables
            .as_deref()
            .ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL batch_and_stream authoritative schema is missing"
                ))
            })?;
        if expected != authoritative_tables {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL batch_and_stream authoritative table schema changed after the exact GTID snapshot boundary"
            )));
        }
        Ok(())
    }

    async fn store(
        &mut self,
        state: PersistedPhase,
        authoritative_tables: Option<Vec<AuthoritativeTableIdentity>>,
    ) -> anyhow::Result<()> {
        let next = PersistedState {
            state,
            authoritative_tables,
            ..self.identity.clone()
        };
        let payload = serde_json::to_vec(&next)?;
        match self
            .storage
            .compare_exchange(
                SNAPSHOT_STREAM_STATE_KEY,
                Some(self.revision),
                &payload,
            )
            .await?
        {
            CompareExchangeResult::Applied(value) => {
                self.revision = value.revision;
                self.identity = next;
                Ok(())
            }
            CompareExchangeResult::Conflict(_) => Err(replication_safety_violation(
                anyhow::anyhow!(
                    "MySQL batch_and_stream durable phase was modified by another execution"
                ),
            )),
        }
    }
}

fn persisted_state(
    server_id: u32,
    tables: &[TableConfig],
    source: &MySqlSourceIdentity,
    replay_identity: &str,
) -> PersistedState {
    PersistedState {
        version: STATE_VERSION,
        replay_identity: replay_identity.to_owned(),
        server_id,
        source: source.clone(),
        configured_tables: tables.iter().map(|table| table.name.clone()).collect(),
        authoritative_tables: None,
        state: PersistedPhase::Claimed,
    }
}

fn validate_claim(
    server_id: u32,
    tables: &[TableConfig],
    source: &MySqlSourceIdentity,
    replay_identity: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(server_id != 0, "MySQL replication server_id must be non-zero");
    anyhow::ensure!(
        !replay_identity.is_empty(),
        "MySQL batch_and_stream replay identity must not be empty"
    );
    anyhow::ensure!(
        !source.server_uuid.is_empty() && !source.database.is_empty(),
        "MySQL source identity is incomplete"
    );
    validate_server_uuid(&source.server_uuid)?;
    validate_identifier("database", &source.database)?;
    anyhow::ensure!(
        !tables.is_empty(),
        "MySQL batch_and_stream requires at least one table"
    );
    let mut names = HashSet::with_capacity(tables.len());
    for table in tables {
        validate_identifier("table", &table.name)?;
        anyhow::ensure!(
            names.insert(table.name.as_str()),
            "MySQL batch_and_stream repeats table '{}'",
            table.name
        );
    }
    Ok(())
}

pub(super) fn validate_boundary(boundary: &MySqlBinlogBoundary) -> anyhow::Result<()> {
    anyhow::ensure!(
        !boundary.filename.is_empty() && !boundary.filename.contains('\0'),
        "MySQL binlog boundary filename is invalid"
    );
    anyhow::ensure!(
        (4..=u64::from(u32::MAX)).contains(&boundary.position),
        "MySQL binlog boundary position must fit the protocol range 4..={}",
        u32::MAX
    );
    anyhow::ensure!(
        boundary.source_timestamp_micros >= 0,
        "MySQL binlog boundary source timestamp precedes the Unix epoch"
    );
    GtidSet::parse_mysql(&boundary.gtid_executed).map_err(|error| {
        anyhow::anyhow!("MySQL binlog boundary contains an invalid GTID set: {error}")
    })?;
    Ok(())
}

fn validate_authoritative_identity(
    state: &PersistedState,
    authoritative_tables: &[AuthoritativeTableIdentity],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        authoritative_tables.len() == state.configured_tables.len(),
        "MySQL authoritative schema has {} tables, expected {}",
        authoritative_tables.len(),
        state.configured_tables.len()
    );
    for (actual, expected_table) in authoritative_tables
        .iter()
        .zip(&state.configured_tables)
    {
        anyhow::ensure!(
            actual.database == state.source.database
                && actual.table == *expected_table,
            "MySQL authoritative schema identity does not match configured table '{}.{}'",
            state.source.database,
            expected_table
        );
        validate_authoritative_table(actual)?;
    }
    Ok(())
}

fn validate_authoritative_table(table: &AuthoritativeTableIdentity) -> anyhow::Result<()> {
    validate_identifier("database", &table.database)?;
    validate_identifier("table", &table.table)?;
    anyhow::ensure!(
        table.engine.eq_ignore_ascii_case("InnoDB"),
        "MySQL authoritative identity for '{}.{}' must use InnoDB, received '{}'",
        table.database,
        table.table,
        table.engine
    );
    anyhow::ensure!(
        !table.columns.is_empty(),
        "MySQL authoritative identity for '{}.{}' has no columns",
        table.database,
        table.table
    );
    let mut names = HashSet::with_capacity(table.columns.len());
    let mut primary_key_ordinals = HashSet::new();
    for column in &table.columns {
        validate_authoritative_column(column)?;
        anyhow::ensure!(
            names.insert(column.name.as_str()),
            "MySQL authoritative identity for '{}.{}' repeats column '{}'",
            table.database,
            table.table,
            column.name
        );
        if let Some(ordinal) = column.primary_key_ordinal {
            anyhow::ensure!(
                primary_key_ordinals.insert(ordinal),
                "MySQL authoritative identity for '{}.{}' repeats primary-key ordinal {}",
                table.database,
                table.table,
                ordinal
            );
        }
    }
    anyhow::ensure!(
        !primary_key_ordinals.is_empty(),
        "MySQL authoritative identity for '{}.{}' has no primary key",
        table.database,
        table.table
    );
    anyhow::ensure!(
        (1..=u64::try_from(primary_key_ordinals.len())?)
            .all(|ordinal| primary_key_ordinals.contains(&ordinal)),
        "MySQL authoritative identity for '{}.{}' has non-contiguous primary-key ordinals",
        table.database,
        table.table
    );
    Ok(())
}

fn validate_authoritative_column(column: &AuthoritativeColumnIdentity) -> anyhow::Result<()> {
    validate_identifier("column", &column.name)?;
    anyhow::ensure!(
        !column.column_type.is_empty(),
        "MySQL authoritative identity for column '{}' has an empty type",
        column.name
    );
    anyhow::ensure!(
        column.character_set.is_some() == column.collation.is_some()
            && column.collation.is_some() == column.collation_id.is_some(),
        "MySQL authoritative identity for column '{}' has inconsistent character-set, collation, and numeric collation-id metadata",
        column.name
    );
    anyhow::ensure!(
        column.primary_key_ordinal.is_none() || !column.nullable,
        "MySQL authoritative identity marks nullable column '{}' as part of the primary key",
        column.name
    );
    if let Some(prefix_length) = column.primary_key_prefix_length {
        anyhow::ensure!(
            prefix_length > 0 && column.primary_key_ordinal.is_some(),
            "MySQL authoritative identity for column '{}' has an invalid primary-key prefix length",
            column.name
        );
    }
    if let Some(direction) = &column.primary_key_direction {
        anyhow::ensure!(
            column.primary_key_ordinal.is_some() && matches!(direction.as_str(), "A" | "D"),
            "MySQL authoritative identity for column '{}' has invalid primary-key direction '{}'",
            column.name,
            direction
        );
    }
    anyhow::ensure!(
        column.primary_key_ordinal.is_some()
            || (column.primary_key_prefix_length.is_none()
                && column.primary_key_direction.is_none()),
        "MySQL authoritative identity for column '{}' has primary-key modifiers without membership",
        column.name
    );
    Ok(())
}

fn decode_and_validate(payload: &[u8], expected: &PersistedState) -> anyhow::Result<PersistedState> {
    let actual: PersistedState = serde_json::from_slice(payload)?;
    anyhow::ensure!(
        actual.version == STATE_VERSION,
        "unsupported MySQL batch_and_stream state version {}",
        actual.version
    );
    let claimed = matches!(&actual.state, PersistedPhase::Claimed);
    anyhow::ensure!(
        claimed == actual.authoritative_tables.is_none(),
        "MySQL batch_and_stream durable state has inconsistent phase and authoritative schema"
    );
    anyhow::ensure!(
        actual.replay_identity == expected.replay_identity
            && actual.server_id == expected.server_id
            && actual.source == expected.source
            && actual.configured_tables == expected.configured_tables,
        "MySQL batch_and_stream durable state belongs to different replay-affecting delivery or source settings"
    );
    if let PersistedPhase::Snapshot { boundary }
    | PersistedPhase::Streaming {
        start_boundary: boundary,
    } = &actual.state
    {
        validate_boundary(boundary)?;
        validate_authoritative_identity(
            &actual,
            actual
                .authoritative_tables
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("authoritative schema is missing"))?,
        )?;
    }
    Ok(actual)
}

#[cfg(test)]
#[path = "tests/phase.rs"]
mod tests;
