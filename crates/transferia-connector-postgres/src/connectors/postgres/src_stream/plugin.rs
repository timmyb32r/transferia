use tokio_postgres::{error::SqlState, Client};
use transferia_connector_support::external_request::observe_external_request;

use super::config::{LogicalDecoder, ReplicationPlugin};
use super::publication::validate_pgoutput_publication;
use crate::connectors::postgres::common::quote_identifier;
use crate::connectors::postgres::source::DiscoveredTable;

/// Resolve once under the transfer's replication lease, before creating its slot.
pub(crate) async fn resolve_plugin(
    client: &mut Client,
    configured: &ReplicationPlugin,
    transfer_id: &str,
    resource_key: &str,
    tables: &[DiscoveredTable],
) -> anyhow::Result<LogicalDecoder> {
    let existing = observe_external_request(
        "postgres",
        "inspect_replication_plugin",
        client.query_opt(
            "SELECT plugin, database = pg_catalog.current_database() FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&transfer_id],
        ),
    )
    .await?;
    if let Some(row) = &existing {
        anyhow::ensure!(
            row.try_get::<_, Option<bool>>(1)? == Some(true),
            "The transfer's replication slot belongs to a different database or is not logical"
        );
    }
    let existing = existing
        .map(|row| row.try_get::<_, Option<String>>(0))
        .transpose()?;
    anyhow::ensure!(
        !matches!(existing, Some(None)),
        "The transfer's replication slot is not a logical slot"
    );
    let existing = existing.flatten();
    let decoder = match configured {
        ReplicationPlugin::Auto => {
            let pgoutput = probe_plugin(client, "pgoutput").await?;
            let wal2json = probe_plugin(client, "wal2json").await?;
            match select_auto_plugin(pgoutput, wal2json, existing.as_deref())? {
                "pgoutput" => LogicalDecoder::Pgoutput {
                    publication: transfer_id.to_owned(),
                },
                "wal2json" => LogicalDecoder::Wal2Json,
                _ => anyhow::bail!("Unsupported automatically selected replication plugin"),
            }
        }
        ReplicationPlugin::Pgoutput { publication } => LogicalDecoder::Pgoutput {
            publication: publication.clone(),
        },
        ReplicationPlugin::Wal2Json => LogicalDecoder::Wal2Json,
    };
    anyhow::ensure!(
        existing.as_deref().is_none_or(|plugin| plugin == decoder.plugin()),
        "The existing replication slot uses a different plugin; it will not be recreated"
    );
    if let LogicalDecoder::Pgoutput { publication } = &decoder {
        if matches!(configured, ReplicationPlugin::Auto) {
            ensure_auto_publication(client, publication, resource_key, tables, existing.is_some()).await?;
        } else {
            validate_pgoutput_publication(client, publication, tables, false).await?;
        }
    }
    Ok(decoder)
}

fn select_auto_plugin(
    pgoutput: bool,
    wal2json: bool,
    existing: Option<&str>,
) -> anyhow::Result<&'static str> {
    match (existing, pgoutput, wal2json) {
        (Some("pgoutput"), true, _) | (None, true, _) => Ok("pgoutput"),
        (Some("wal2json"), _, true) | (None, false, true) => Ok("wal2json"),
        (Some(_), _, _) => anyhow::bail!(
            "The existing slot's plugin is unavailable or unsupported; refusing to switch plugins"
        ),
        (None, false, false) => anyhow::bail!(
            "Neither pgoutput nor wal2json is available on the PostgreSQL server"
        ),
    }
}

async fn probe_plugin(client: &Client, plugin: &str) -> anyhow::Result<bool> {
    // Loading an output plugin through a temporary slot checks the actual server
    // library, unlike pg_available_extensions (output plugins need not be extensions).
    // The backend PID makes concurrent probes independent; temporary slots also
    // disappear if this connection is cancelled or lost.
    let row = observe_external_request(
        "postgres",
        "replication_probe_identity",
        client.query_one("SELECT pg_catalog.pg_backend_pid()", &[]),
    )
    .await?;
    let name = format!("transferia_probe_{}_{}", row.try_get::<_, i32>(0)?, plugin);
    let result = observe_external_request(
        "postgres",
        "probe_replication_plugin",
        client.query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, $2, true)",
            &[&name, &plugin],
        ),
    )
    .await;
    match result {
        Ok(_) => {
            observe_external_request(
                "postgres",
                "drop_replication_probe",
                client.query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&name]),
            )
            .await?;
            Ok(true)
        }
        Err(error) if error.as_db_error().is_some_and(|error| {
            plugin_is_missing(error.code(), error.message(), plugin)
        }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn plugin_is_missing(code: &SqlState, message: &str, plugin: &str) -> bool {
    // UNDEFINED_FILE can also mean missing/corrupt WAL. Only a missing plugin
    // library is availability information; all other errors must fail closed.
    *code == SqlState::UNDEFINED_FILE
        && message.starts_with(&format!("could not access file \"{plugin}\":"))
        && message.ends_with("No such file or directory")
}

async fn ensure_auto_publication(
    client: &mut Client,
    publication: &str,
    resource_key: &str,
    tables: &[DiscoveredTable],
    slot_exists: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(!tables.is_empty(), "An automatic publication requires selected tables");
    let marker = format!("transferia:{resource_key}");
    let transaction = observe_external_request(
        "postgres", "begin_auto_publication", client.transaction(),
    ).await?;
    let existing = observe_external_request(
        "postgres",
        "inspect_auto_publication",
        transaction.query_opt(
            "SELECT pg_catalog.obj_description(oid, 'pg_publication') FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication],
        ),
    ).await?;
    match existing {
        Some(row) => anyhow::ensure!(
            row.try_get::<_, Option<String>>(0)?.as_deref() == Some(marker.as_str()),
            "Publication '{publication}' already exists and is not owned by this transfer"
        ),
        None => {
            anyhow::ensure!(
                !slot_exists,
                "Automatic publication '{publication}' is missing for an existing replication slot; refusing to recreate it"
            );
            // Publish all actions. Unsupported TRUNCATE must reach the decoder
            // and fail closed, never disappear from an automatically created feed.
            let names = tables.iter().map(|table| format!(
                "{}.{}", quote_identifier(&table.config.schema), quote_identifier(&table.config.name),
            )).collect::<Vec<_>>().join(", ");
            let statement = format!("CREATE PUBLICATION {} FOR TABLE {names}", quote_identifier(publication));
            observe_external_request("postgres", "create_auto_publication", transaction.batch_execute(&statement)).await?;
            let statement = format!(
                "COMMENT ON PUBLICATION {} IS '{}'",
                quote_identifier(publication), marker.replace('\'', "''"),
            );
            observe_external_request("postgres", "mark_auto_publication_owner", transaction.batch_execute(&statement)).await?;
        }
    }
    let published = observe_external_request(
        "postgres",
        "inspect_auto_publication_tables",
        transaction.query("SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = $1", &[&publication]),
    ).await?;
    let actual = published.iter().map(|row| Ok((row.try_get::<_, String>(0)?, row.try_get::<_, String>(1)?)))
        .collect::<Result<std::collections::BTreeSet<_>, tokio_postgres::Error>>()?;
    let expected = tables.iter().map(|table| (table.config.schema.clone(), table.config.name.clone()))
        .collect::<std::collections::BTreeSet<_>>();
    anyhow::ensure!(actual == expected, "Automatic publication '{publication}' has a different table set; refusing to change it");
    validate_pgoutput_publication(&transaction, publication, tables, true).await?;
    observe_external_request("postgres", "commit_auto_publication", transaction.commit()).await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/plugin.rs"]
mod tests;
