use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableStorage};

use super::config::MySqlReplicationConfig;
use super::position::{GtidSet, MySqlBinlogPosition};
use crate::connectors::mysql::src_batch_and_stream::{
    replication_safety_violation, AuthoritativeTableIdentity, MySqlBinlogBoundary,
    MySqlSourceIdentity,
};

pub const REPLICATION_OFFSET_STATE_KEY: &str = "mysql-replication-offset";
const STATE_VERSION: u8 = 2;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationOffsetState {
    version: u8,
    replay_identity: String,
    server_id: u32,
    source: MySqlSourceIdentity,
    authoritative_tables: Vec<AuthoritativeTableIdentity>,
    initial_boundary: MySqlBinlogBoundary,
    committed_position: MySqlBinlogPosition,
    committed_gtids: GtidSet,
}

pub struct MySqlReplicationOffsetTracker {
    storage: Arc<dyn DurableStorage>,
    revision: u64,
    identity: ReplicationOffsetState,
}

/// Reads and validates an already-owned exact resume position without creating
/// or updating durable state.
///
/// `None` means that the caller must capture a fresh exact boundary while it
/// still holds the `MySQL` consistency lock. `Some` is the only position an
/// existing delivery may resume from.
pub async fn inspect_existing_replication_offset(
    config: &MySqlReplicationConfig,
    source: &MySqlSourceIdentity,
    authoritative_tables: &[AuthoritativeTableIdentity],
    durable: &DurableContext,
    expected_initial_boundary: Option<&MySqlBinlogBoundary>,
    current_executed_gtids: &GtidSet,
    current_purged_gtids: &GtidSet,
    replay_identity: &str,
) -> anyhow::Result<Option<MySqlBinlogPosition>> {
    validate_identity(
        config,
        source,
        authoritative_tables,
        expected_initial_boundary,
        replay_identity,
    )
    .map_err(replication_safety_violation)?;
    let Some(current) = durable.storage.read(REPLICATION_OFFSET_STATE_KEY).await? else {
        return Ok(None);
    };
    let state = decode_and_validate(
        &current.payload,
        config,
        source,
        authoritative_tables,
        expected_initial_boundary,
        replay_identity,
    )?;
    validate_gtid_continuity(
        &state.committed_gtids,
        current_executed_gtids,
        current_purged_gtids,
    )?;
    Ok(Some(state.committed_position))
}

impl MySqlReplicationOffsetTracker {
    /// Opens delivery-owned replication state without ever adopting an
    /// arbitrary server-side position.
    ///
    /// A new state requires an exact boundary captured by the caller. Existing
    /// state is accepted only when every replay-affecting identity field still
    /// matches byte-for-byte. The stored committed position is then the sole
    /// resume point.
    pub async fn prepare(
        config: &MySqlReplicationConfig,
        source: &MySqlSourceIdentity,
        authoritative_tables: &[AuthoritativeTableIdentity],
        durable: DurableContext,
        exact_start_boundary: Option<&MySqlBinlogBoundary>,
        current_executed_gtids: &GtidSet,
        current_purged_gtids: &GtidSet,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<(Self, MySqlBinlogPosition, GtidSet)> {
        validate_identity(
            config,
            source,
            authoritative_tables,
            exact_start_boundary,
            &replay_identity,
        )
        .map_err(replication_safety_violation)?;

        if let Some(current) = durable.storage.read(REPLICATION_OFFSET_STATE_KEY).await? {
            let state = decode_and_validate(
                &current.payload,
                config,
                source,
                authoritative_tables,
                exact_start_boundary,
                &replay_identity,
            )?;
            validate_gtid_continuity(
                &state.committed_gtids,
                current_executed_gtids,
                current_purged_gtids,
            )?;
            let committed_position = state.committed_position.clone();
            let committed_gtids = state.committed_gtids.clone();
            return Ok((
                Self {
                    storage: durable.storage,
                    revision: current.revision,
                    identity: state,
                },
                committed_position,
                committed_gtids,
            ));
        }

        let boundary = exact_start_boundary.ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL replication has no durable offset and no freshly captured exact start boundary; refusing to adopt a server position"
            ))
        })?;
        let committed_position =
            boundary_position(boundary).map_err(replication_safety_violation)?;
        let committed_gtids = GtidSet::parse_mysql(&boundary.gtid_executed)
            .map_err(|error| replication_safety_violation(error.into()))?;
        validate_gtid_continuity(
            &committed_gtids,
            current_executed_gtids,
            current_purged_gtids,
        )?;
        let state = ReplicationOffsetState {
            version: STATE_VERSION,
            replay_identity: replay_identity.to_string(),
            server_id: config.server_id,
            source: source.clone(),
            authoritative_tables: authoritative_tables.to_vec(),
            initial_boundary: boundary.clone(),
            committed_position: committed_position.clone(),
            committed_gtids: committed_gtids.clone(),
        };
        let payload = serde_json::to_vec(&state)?;
        let result = durable
            .storage
            .compare_exchange(REPLICATION_OFFSET_STATE_KEY, None, &payload)
            .await?;
        let CompareExchangeResult::Applied(value) = result else {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL durable replication offset was claimed by another writer"
            )));
        };
        Ok((
            Self {
                storage: durable.storage,
                revision: value.revision,
                identity: state,
            },
            committed_position,
            committed_gtids,
        ))
    }

    pub async fn store(
        &mut self,
        committed_position: &MySqlBinlogPosition,
        committed_gtids: &GtidSet,
    ) -> anyhow::Result<()> {
        committed_position
            .validate()
            .map_err(|error| replication_safety_violation(error.into()))?;
        committed_gtids
            .validate()
            .map_err(|error| replication_safety_violation(error.into()))?;
        if !self.identity.committed_gtids.is_subset_of(committed_gtids) {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL committed GTID state moved backwards"
            )));
        }
        if committed_position == &self.identity.committed_position
            && committed_gtids == &self.identity.committed_gtids
        {
            return Ok(());
        }
        let next = ReplicationOffsetState {
            committed_position: committed_position.clone(),
            committed_gtids: committed_gtids.clone(),
            ..self.identity.clone()
        };
        let payload = serde_json::to_vec(&next)?;
        match self
            .storage
            .compare_exchange(REPLICATION_OFFSET_STATE_KEY, Some(self.revision), &payload)
            .await?
        {
            CompareExchangeResult::Applied(value) => {
                self.revision = value.revision;
                self.identity = next;
                Ok(())
            }
            CompareExchangeResult::Conflict(_) => Err(replication_safety_violation(
                anyhow::anyhow!("MySQL durable replication offset was modified by another writer"),
            )),
        }
    }
}

fn decode_and_validate(
    payload: &[u8],
    config: &MySqlReplicationConfig,
    source: &MySqlSourceIdentity,
    authoritative_tables: &[AuthoritativeTableIdentity],
    exact_start_boundary: Option<&MySqlBinlogBoundary>,
    replay_identity: &str,
) -> anyhow::Result<ReplicationOffsetState> {
    let validate = || -> anyhow::Result<ReplicationOffsetState> {
        let state: ReplicationOffsetState = serde_json::from_slice(payload)?;
        anyhow::ensure!(
            state.version == STATE_VERSION,
            "unsupported MySQL replication offset state version {}",
            state.version
        );
        anyhow::ensure!(
            state.replay_identity == replay_identity
                && state.server_id == config.server_id
                && state.source == *source
                && state.authoritative_tables == authoritative_tables,
            "MySQL durable replication offset belongs to a different replay identity, source, server_id, or authoritative table schema"
        );
        if let Some(boundary) = exact_start_boundary {
            anyhow::ensure!(
                state.initial_boundary == *boundary,
                "MySQL durable replication offset belongs to a different exact stream start boundary"
            );
        }
        boundary_position(&state.initial_boundary)?;
        state.committed_position.validate()?;
        state.committed_gtids.validate()?;
        let initial_gtids = GtidSet::parse_mysql(&state.initial_boundary.gtid_executed)?;
        anyhow::ensure!(
            initial_gtids.is_subset_of(&state.committed_gtids),
            "MySQL durable committed GTID set does not contain its exact initial boundary"
        );
        Ok(state)
    };
    validate().map_err(replication_safety_violation)
}

fn validate_identity(
    config: &MySqlReplicationConfig,
    source: &MySqlSourceIdentity,
    authoritative_tables: &[AuthoritativeTableIdentity],
    exact_start_boundary: Option<&MySqlBinlogBoundary>,
    replay_identity: &str,
) -> anyhow::Result<()> {
    config.validate()?;
    anyhow::ensure!(
        !replay_identity.is_empty(),
        "MySQL replication replay identity must not be empty"
    );
    anyhow::ensure!(
        !source.server_uuid.is_empty() && !source.database.is_empty(),
        "MySQL replication source identity is incomplete"
    );
    anyhow::ensure!(
        !authoritative_tables.is_empty(),
        "MySQL replication requires at least one authoritative table"
    );
    let mut table_names = BTreeSet::new();
    for table in authoritative_tables {
        anyhow::ensure!(
            table.database == source.database
                && !table.table.is_empty()
                && table.engine.eq_ignore_ascii_case("InnoDB")
                && !table.columns.is_empty(),
            "MySQL authoritative table identity is incomplete or belongs to another database"
        );
        anyhow::ensure!(
            table_names.insert(table.table.as_str()),
            "MySQL authoritative table identity repeats table '{}'",
            table.table
        );
        let mut column_names = BTreeSet::new();
        let mut primary_key_ordinals = BTreeSet::new();
        for column in &table.columns {
            anyhow::ensure!(
                !column.name.is_empty()
                    && !column.column_type.is_empty()
                    && column_names.insert(column.name.as_str()),
                "MySQL authoritative identity for table '{}' has an incomplete or duplicate column",
                table.table
            );
            if let Some(ordinal) = column.primary_key_ordinal {
                anyhow::ensure!(
                    ordinal > 0 && primary_key_ordinals.insert(ordinal),
                    "MySQL authoritative identity for table '{}' has an invalid primary-key ordinal",
                    table.table
                );
            }
        }
        anyhow::ensure!(
            !primary_key_ordinals.is_empty(),
            "MySQL authoritative identity for table '{}' has no primary key",
            table.table
        );
    }
    if let Some(boundary) = exact_start_boundary {
        boundary_position(boundary)?;
    }
    Ok(())
}

fn boundary_position(boundary: &MySqlBinlogBoundary) -> anyhow::Result<MySqlBinlogPosition> {
    GtidSet::parse_mysql(&boundary.gtid_executed)?;
    anyhow::ensure!(
        boundary.source_timestamp_micros >= 0,
        "MySQL exact replication boundary timestamp precedes the Unix epoch"
    );
    MySqlBinlogPosition::new(boundary.filename.as_bytes().to_vec(), boundary.position)
        .map_err(Into::into)
}

fn validate_gtid_continuity(
    committed_gtids: &GtidSet,
    current_executed_gtids: &GtidSet,
    current_purged_gtids: &GtidSet,
) -> anyhow::Result<()> {
    committed_gtids
        .validate()
        .map_err(|error| replication_safety_violation(error.into()))?;
    current_executed_gtids
        .validate()
        .map_err(|error| replication_safety_violation(error.into()))?;
    current_purged_gtids
        .validate()
        .map_err(|error| replication_safety_violation(error.into()))?;
    if !committed_gtids.is_subset_of(current_executed_gtids) {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "the current MySQL executed GTID set does not contain every durably committed transaction; refusing a reset or reused binlog history"
        )));
    }
    if !current_purged_gtids.is_subset_of(committed_gtids) {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL purged one or more transactions that this delivery has not durably committed"
        )));
    }
    Ok(())
}
