use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableStorage};

use super::config::PostgresReplicationConfig;
use super::reader::{format_lsn, parse_lsn};
use crate::connectors::postgres::common::quote_identifier;

const STATE_VERSION: u8 = 1;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationOffsetState {
    version: u8,
    slot: String,
    plugin: String,
    committed_lsn: u64,
}

struct ExistingSlot {
    plugin: String,
    committed_lsn: u64,
}

pub(super) struct ReplicationSlotTracker {
    storage: Arc<dyn DurableStorage>,
    key: String,
    revision: Option<u64>,
    slot: String,
    plugin: String,
}

impl ReplicationSlotTracker {
    pub(super) async fn prepare(
        client: &Client,
        config: &PostgresReplicationConfig,
        durable: DurableContext,
    ) -> anyhow::Result<(Self, u64)> {
        let plugin = config.decoder.plugin().to_owned();
        let key = format!("postgres-replication-{}", config.slot);
        let persisted = durable.storage.read(&key).await?;
        let persisted_lsn = persisted
            .as_ref()
            .map(|value| decode_state(&value.payload, &config.slot, &plugin))
            .transpose()?;
        let mut tracker = Self {
            storage: durable.storage,
            key,
            revision: persisted.as_ref().map(|value| value.revision),
            slot: config.slot.clone(),
            plugin: plugin.clone(),
        };

        let committed_lsn = match existing_slot(client, &config.slot).await? {
            Some(existing) => {
                anyhow::ensure!(
                    existing.plugin == plugin,
                    "PostgreSQL slot '{}' uses plugin '{}', configuration requires '{}'",
                    config.slot,
                    existing.plugin,
                    plugin,
                );
                let committed = persisted_lsn.map_or(existing.committed_lsn, |persisted| {
                    persisted.max(existing.committed_lsn)
                });
                if committed > existing.committed_lsn {
                    advance_slot(client, &config.slot, committed).await?;
                }
                committed
            }
            None => {
                let committed = persisted_lsn.ok_or_else(|| {
                    anyhow::anyhow!(
                        "PostgreSQL replication slot '{}' does not exist and no durable committed LSN is available",
                        config.slot,
                    )
                })?;
                let schema = pg_tm_aux_schema(client).await?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "PostgreSQL replication slot '{}' disappeared and pg_tm_aux is not installed",
                        config.slot,
                    )
                })?;
                recreate_slot(client, &schema, &config.slot, &plugin, committed).await?;
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
            slot: self.slot.clone(),
            plugin: self.plugin.clone(),
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
            CompareExchangeResult::Conflict(_) => anyhow::bail!(
                "PostgreSQL durable replication offset was modified by another writer"
            ),
        }
    }
}

fn decode_state(payload: &[u8], slot: &str, plugin: &str) -> anyhow::Result<u64> {
    let state: ReplicationOffsetState = serde_json::from_slice(payload)?;
    anyhow::ensure!(
        state.version == STATE_VERSION,
        "unsupported PostgreSQL replication offset state version {}",
        state.version,
    );
    anyhow::ensure!(
        state.slot == slot && state.plugin == plugin,
        "PostgreSQL durable replication offset belongs to slot '{}' with plugin '{}', expected slot '{}' with plugin '{}'",
        state.slot,
        state.plugin,
        slot,
        plugin,
    );
    Ok(state.committed_lsn)
}

async fn existing_slot(client: &Client, slot: &str) -> anyhow::Result<Option<ExistingSlot>> {
    client
        .query_opt(
            "SELECT plugin, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database()",
            &[&slot],
        )
        .await?
        .map(|row| {
            let lsn = row
                .get::<_, Option<String>>(1)
                .as_deref()
                .map(parse_lsn)
                .transpose()?
                .unwrap_or(0);
            Ok(ExistingSlot {
                plugin: row.get(0),
                committed_lsn: lsn,
            })
        })
        .transpose()
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
) -> anyhow::Result<()> {
    let query = recreate_slot_query(schema);
    let requested_lsn = format_lsn(committed_lsn);
    let row = client
        .query_one(&query, &[&slot, &plugin, &requested_lsn])
        .await?;
    let created_slot: String = row.get(0);
    let created_lsn = parse_lsn(row.get::<_, String>(1).as_str())?;
    anyhow::ensure!(
        created_slot == slot && created_lsn <= committed_lsn,
        "pg_tm_aux recreated unexpected slot '{}' at LSN {} instead of '{}' at or before {}",
        created_slot,
        format_lsn(created_lsn),
        slot,
        requested_lsn,
    );
    tracing::info!(slot, lsn = %requested_lsn, "recreated PostgreSQL replication slot through pg_tm_aux");
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
    client
        .query_one(
            "SELECT pg_replication_slot_advance($1, $2::text::pg_lsn)::text",
            &[&slot, &format_lsn(committed_lsn)],
        )
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/slot_recovery.rs"]
mod tests;
