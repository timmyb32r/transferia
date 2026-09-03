use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableStorage};

use super::config::{LogicalDecoder, PostgresReplicationConfig};
use super::identity::{
    authoritative_table_identities, AuthoritativeTableIdentity, PostgresSourceIdentity,
};
use super::reader::{format_lsn, parse_lsn};
use crate::connectors::postgres::common::quote_identifier;
use crate::connectors::postgres::source::DiscoveredTable;

const STATE_VERSION: u8 = 3;
const EXISTING_SLOT_QUERY: &str = "SELECT plugin, confirmed_flush_lsn::text, database::text, datoid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1";

#[derive(Debug)]
pub(crate) struct ReplicationSafetyViolation {
    source: anyhow::Error,
}

impl std::fmt::Display for ReplicationSafetyViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl std::error::Error for ReplicationSafetyViolation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub(crate) fn replication_safety_violation(source: anyhow::Error) -> anyhow::Error {
    if source
        .downcast_ref::<ReplicationSafetyViolation>()
        .is_some()
    {
        source
    } else {
        anyhow::Error::new(ReplicationSafetyViolation { source })
    }
}

pub(crate) fn is_replication_safety_violation(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ReplicationSafetyViolation>().is_some()
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationOffsetState {
    version: u8,

    replay_identity: String,

    slot: String,

    plugin: String,

    publication: Option<String>,

    source: PostgresSourceIdentity,

    authoritative_tables: Vec<AuthoritativeTableIdentity>,

    committed_lsn: u64,
}

struct ExistingSlot {
    plugin: String,
    committed_lsn: u64,
}

#[derive(Debug, Eq, PartialEq)]
enum RecreatedSlotRecoveryPlan {
    VerifyExact,
    CatchUpThenVerifyExact,
}

pub(super) struct ReplicationSlotTracker {
    storage: Arc<dyn DurableStorage>,
    key: String,
    revision: Option<u64>,

    replay_identity: Arc<str>,

    slot: String,

    plugin: String,

    publication: Option<String>,

    source: PostgresSourceIdentity,

    authoritative_tables: Vec<AuthoritativeTableIdentity>,
}

impl ReplicationSlotTracker {
    pub(super) async fn prepare(
        client: &Client,
        config: &PostgresReplicationConfig,
        source: &PostgresSourceIdentity,
        tables: &[DiscoveredTable],
        durable: DurableContext,
        exact_start_lsn: Option<u64>,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<(Self, u64)> {
        if replay_identity.is_empty() {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL replication replay identity must not be empty"
            )));
        }
        let plugin = config.decoder.plugin().to_owned();
        let publication = match &config.decoder {
            LogicalDecoder::Pgoutput { publication } => Some(publication.clone()),
            LogicalDecoder::Wal2Json => None,
        };
        let authoritative_tables =
            authoritative_table_identities(tables).map_err(replication_safety_violation)?;
        let key = format!("postgres-replication-{}", config.slot);
        let persisted = durable.storage.read(&key).await?;
        let persisted_lsn = persisted
            .as_ref()
            .map(|value| {
                decode_state(
                    &value.payload,
                    &config.slot,
                    &plugin,
                    publication.as_deref(),
                    source,
                    &authoritative_tables,
                    &replay_identity,
                )
            })
            .transpose()?;
        let mut tracker = Self {
            storage: durable.storage,
            key,
            revision: persisted.as_ref().map(|value| value.revision),
            replay_identity,
            slot: config.slot.clone(),
            plugin: plugin.clone(),
            publication,
            source: source.clone(),
            authoritative_tables,
        };

        if let (Some(persisted), Some(start)) = (persisted_lsn, exact_start_lsn) {
            if persisted < start {
                return Err(replication_safety_violation(anyhow::anyhow!(
                    "PostgreSQL durable replication offset {} precedes exact snapshot boundary {}",
                    super::reader::format_lsn(persisted),
                    super::reader::format_lsn(start)
                )));
            }
        }
        let requested_lsn = persisted_lsn.or(exact_start_lsn);
        let committed_lsn = match existing_slot(client, &config.slot, source).await? {
            Some(existing) => {
                if existing.plugin != plugin {
                    return Err(replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL slot '{}' uses plugin '{}', configuration requires '{}'",
                        config.slot,
                        existing.plugin,
                        plugin,
                    )));
                }
                let committed = requested_lsn.ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL replication slot '{}' already exists, but this delivery has no durable offset or freshly created exact start position; refusing to adopt an unowned slot",
                        config.slot,
                    ))
                })?;
                if existing.committed_lsn > committed {
                    return Err(replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL slot '{}' confirmed LSN {} is ahead of durable LSN {}; refusing to skip uncommitted changes",
                        config.slot,
                        super::reader::format_lsn(existing.committed_lsn),
                        super::reader::format_lsn(committed),
                    )));
                }
                if committed > existing.committed_lsn {
                    advance_slot(client, &config.slot, committed).await?;
                }
                committed
            }
            None => {
                let committed = requested_lsn.ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL replication slot '{}' does not exist and no durable committed LSN is available",
                        config.slot,
                    ))
                })?;
                let schema = pg_tm_aux_schema(client).await?.ok_or_else(|| {
                    replication_safety_violation(anyhow::anyhow!(
                        "PostgreSQL replication slot '{}' disappeared and pg_tm_aux is not installed",
                        config.slot,
                    ))
                })?;
                let created_lsn =
                    recreate_slot(client, &schema, &config.slot, &plugin, committed).await?;
                match recreated_slot_recovery_plan(created_lsn, committed)? {
                    RecreatedSlotRecoveryPlan::VerifyExact => {}
                    RecreatedSlotRecoveryPlan::CatchUpThenVerifyExact => {
                        advance_slot(client, &config.slot, committed).await?;
                    }
                }
                verify_slot_exact(client, &config.slot, &plugin, source, committed).await?;
                committed
            }
        };
        if persisted_lsn != Some(committed_lsn) {
            tracker.store(committed_lsn).await?;
        }
        Ok((tracker, committed_lsn))
    }

    pub(super) async fn store(&mut self, committed_lsn: u64) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&ReplicationOffsetState {
            version: STATE_VERSION,
            replay_identity: self.replay_identity.to_string(),
            slot: self.slot.clone(),
            plugin: self.plugin.clone(),
            publication: self.publication.clone(),
            source: self.source.clone(),
            authoritative_tables: self.authoritative_tables.clone(),
            committed_lsn,
        })?;
        match self
            .storage
            .compare_exchange(&self.key, self.revision, &payload)
            .await?
        {
            CompareExchangeResult::Applied(value) => {
                self.revision = Some(value.revision);
                Ok(())
            }
            CompareExchangeResult::Conflict(_) => {
                Err(replication_safety_violation(anyhow::anyhow!(
                    "PostgreSQL durable replication offset was modified by another writer"
                )))
            }
        }
    }
}

fn decode_state(
    payload: &[u8],
    slot: &str,
    plugin: &str,
    publication: Option<&str>,
    source: &PostgresSourceIdentity,
    authoritative_tables: &[AuthoritativeTableIdentity],
    replay_identity: &str,
) -> anyhow::Result<u64> {
    let decode = || -> anyhow::Result<u64> {
        let state: ReplicationOffsetState = serde_json::from_slice(payload)?;
        anyhow::ensure!(
            state.version == STATE_VERSION,
            "unsupported PostgreSQL replication offset state version {}",
            state.version,
        );
        anyhow::ensure!(
            state.replay_identity == replay_identity
                && state.slot == slot
                && state.plugin == plugin
                && state.publication.as_deref() == publication
                && state.source == *source
                && state.authoritative_tables == authoritative_tables,
            "PostgreSQL durable replication offset belongs to a different replay-affecting delivery, source, slot, plugin, or authoritative schema (stored slot '{stored_slot}' with plugin '{stored_plugin}', expected slot '{slot}' with plugin '{plugin}')",
            stored_slot = state.slot,
            stored_plugin = state.plugin,
        );
        Ok(state.committed_lsn)
    };
    decode().map_err(replication_safety_violation)
}

async fn existing_slot(
    client: &Client,
    slot: &str,
    expected_source: &PostgresSourceIdentity,
) -> anyhow::Result<Option<ExistingSlot>> {
    client
        .query_opt(EXISTING_SLOT_QUERY, &[&slot])
        .await?
        .map(|row| {
            let database = row
                .try_get::<_, Option<String>>(2)
                .map_err(|error| replication_safety_violation(error.into()))?;
            let database_oid = row
                .try_get::<_, Option<u32>>(3)
                .map_err(|error| replication_safety_violation(error.into()))?;
            validate_slot_database(
                slot,
                database.as_deref(),
                database_oid,
                &expected_source.database,
                expected_source.database_oid,
            )?;
            let plugin = row
                .try_get(0)
                .map_err(|error| replication_safety_violation(error.into()))?;
            let lsn = row
                .try_get::<_, Option<String>>(1)
                .map_err(|error| replication_safety_violation(error.into()))?
                .as_deref()
                .map(parse_lsn)
                .transpose()
                .map_err(replication_safety_violation)?
                .unwrap_or(0);
            Ok(ExistingSlot {
                plugin,
                committed_lsn: lsn,
            })
        })
        .transpose()
}

fn validate_slot_database(
    slot: &str,
    actual_database: Option<&str>,
    actual_database_oid: Option<u32>,
    expected_database: &str,
    expected_database_oid: u32,
) -> anyhow::Result<()> {
    if actual_database != Some(expected_database)
        || actual_database_oid != Some(expected_database_oid)
    {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "PostgreSQL replication slot '{}' belongs to database {:?} with OID {:?}, expected exact database '{}' with OID {}",
            slot,
            actual_database,
            actual_database_oid,
            expected_database,
            expected_database_oid,
        )));
    }
    Ok(())
}

async fn pg_tm_aux_schema(client: &Client) -> anyhow::Result<Option<String>> {
    let row = client
        .query_one(
            "SELECT min(n.nspname) \
             FROM pg_catalog.pg_extension AS e \
             JOIN pg_catalog.pg_depend AS d \
               ON d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass \
              AND d.refobjid = e.oid AND d.deptype = 'e' \
             JOIN pg_catalog.pg_proc AS p \
               ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid = p.oid \
             JOIN pg_catalog.pg_namespace AS n ON p.pronamespace = n.oid \
             WHERE e.extname = 'pg_tm_aux' \
               AND p.proname = 'pg_create_logical_replication_slot_lsn'",
            &[],
        )
        .await?;
    Ok(row.get(0))
}

async fn recreate_slot(
    client: &Client,
    schema: &str,
    slot: &str,
    plugin: &str,
    committed_lsn: u64,
) -> anyhow::Result<u64> {
    let query = recreate_slot_query(schema);
    let requested_lsn = format_lsn(committed_lsn);
    let row = client
        .query_one(&query, &[&slot, &plugin, &requested_lsn])
        .await?;
    let created_slot: String = row
        .try_get(0)
        .map_err(|error| replication_safety_violation(error.into()))?;
    let created_lsn = parse_lsn(
        row.try_get::<_, String>(1)
            .map_err(|error| replication_safety_violation(error.into()))?
            .as_str(),
    )
    .map_err(replication_safety_violation)?;
    if created_slot != slot {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "pg_tm_aux recreated unexpected slot '{}' instead of '{}'",
            created_slot,
            slot,
        )));
    }
    tracing::info!(slot, created_lsn = %format_lsn(created_lsn), requested_lsn = %requested_lsn, "recreated PostgreSQL replication slot through pg_tm_aux");
    Ok(created_lsn)
}

fn recreated_slot_recovery_plan(
    created_lsn: u64,
    requested_lsn: u64,
) -> anyhow::Result<RecreatedSlotRecoveryPlan> {
    if created_lsn > requested_lsn {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "pg_tm_aux recreated PostgreSQL replication slot at LSN {} ahead of durable LSN {}",
            format_lsn(created_lsn),
            format_lsn(requested_lsn),
        )));
    }
    Ok(if created_lsn < requested_lsn {
        RecreatedSlotRecoveryPlan::CatchUpThenVerifyExact
    } else {
        RecreatedSlotRecoveryPlan::VerifyExact
    })
}

async fn verify_slot_exact(
    client: &Client,
    slot: &str,
    plugin: &str,
    expected_source: &PostgresSourceIdentity,
    requested_lsn: u64,
) -> anyhow::Result<()> {
    let actual = existing_slot(client, slot, expected_source)
        .await?
        .ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL replication slot '{}' disappeared before recovery verification",
                slot,
            ))
        })?;
    if actual.plugin != plugin || actual.committed_lsn != requested_lsn {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "PostgreSQL replication slot '{}' recovery verification failed: plugin '{}' and confirmed LSN {}, expected plugin '{}' and exact durable LSN {}",
            slot,
            actual.plugin,
            format_lsn(actual.committed_lsn),
            plugin,
            format_lsn(requested_lsn),
        )));
    }
    Ok(())
}

fn recreate_slot_query(schema: &str) -> String {
    format!(
        "SELECT slot_name, lsn::text FROM {}.{}($1, $2, false, $3::pg_lsn)",
        quote_identifier(schema),
        quote_identifier("pg_create_logical_replication_slot_lsn"),
    )
}

pub(super) async fn advance_slot(
    client: &Client,
    slot: &str,
    committed_lsn: u64,
) -> anyhow::Result<()> {
    let requested_lsn = format_lsn(committed_lsn);
    let row = client
        .query_one(
            "SELECT slot_name::text, end_lsn::text FROM pg_catalog.pg_replication_slot_advance($1, $2::text::pg_lsn)",
            &[&slot, &requested_lsn],
        )
        .await?;
    let advanced_slot: String = row
        .try_get(0)
        .map_err(|error| replication_safety_violation(error.into()))?;
    let advanced_lsn = parse_lsn(
        row.try_get::<_, String>(1)
            .map_err(|error| replication_safety_violation(error.into()))?
            .as_str(),
    )
    .map_err(replication_safety_violation)?;
    if advanced_slot != slot || advanced_lsn != committed_lsn {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "PostgreSQL replication slot advance returned slot '{}' at LSN {}, expected '{}' at exact LSN {}",
            advanced_slot,
            format_lsn(advanced_lsn),
            slot,
            requested_lsn,
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/slot_recovery.rs"]
mod tests;
