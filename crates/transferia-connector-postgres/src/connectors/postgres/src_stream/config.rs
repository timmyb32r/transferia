use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::connectors::postgres::common::validate_identifier;

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresReplicationConfig {
    #[serde(default)]
    #[schemars(title = "Plugin")]
    pub plugin: ReplicationPlugin,

    #[serde(default = "default_max_changes")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub max_changes: usize,

    #[serde(default = "default_poll_interval_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub poll_interval_ms: u64,

    #[serde(default = "default_bootstrap_timeout_ms")]
    #[schemars(
        title = "Replication bootstrap timeout",
        description = "Maximum time in milliseconds for opening the replication session and exporting the exact slot snapshot",
        extend("x-ui" = { "widget": "hidden" })
    )]
    pub bootstrap_timeout_ms: u64,
}

impl Default for PostgresReplicationConfig {
    fn default() -> Self {
        Self {
            plugin: ReplicationPlugin::Auto,
            max_changes: default_max_changes(),
            poll_interval_ms: default_poll_interval_ms(),
            bootstrap_timeout_ms: default_bootstrap_timeout_ms(),
        }
    }
}

impl PostgresReplicationConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if let ReplicationPlugin::Pgoutput { publication } = &self.plugin {
            validate_identifier("publication", publication)?;
        }
        anyhow::ensure!(
            self.max_changes > 0 && i32::try_from(self.max_changes).is_ok(),
            "replication.max_changes must be in 1..={}",
            i32::MAX
        );
        anyhow::ensure!(
            self.poll_interval_ms > 0,
            "replication.poll_interval_ms must be positive"
        );
        anyhow::ensure!(
            self.bootstrap_timeout_ms > 0,
            "replication.bootstrap_timeout_ms must be positive"
        );
        Ok(())
    }
}

pub(crate) fn replication_slot(delivery_id: &str) -> anyhow::Result<&str> {
    anyhow::ensure!(
        !delivery_id.is_empty()
            && delivery_id.len() <= crate::connectors::postgres::common::MAX_IDENTIFIER_BYTES
            && delivery_id.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "PostgreSQL replication requires a transfer ID containing only lowercase ASCII letters, digits, and underscores, at most 63 bytes; the slot name must equal the transfer ID"
    );
    Ok(delivery_id)
}

#[derive(Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicationPlugin {
    #[default]
    #[schemars(title = "auto")]
    Auto,
    #[schemars(title = "pgoutput")]
    Pgoutput { publication: String },
    #[serde(rename = "wal2json")]
    #[schemars(title = "wal2json")]
    Wal2Json,
}

#[derive(Clone)]
pub enum LogicalDecoder {
    Pgoutput { publication: String },
    Wal2Json,
}

impl LogicalDecoder {
    pub(crate) const fn plugin(&self) -> &'static str {
        match self {
            Self::Pgoutput { .. } => "pgoutput",
            Self::Wal2Json => "wal2json",
        }
    }
}

const fn default_max_changes() -> usize {
    4_096
}

const fn default_poll_interval_ms() -> u64 {
    100
}

const fn default_bootstrap_timeout_ms() -> u64 {
    30_000
}
