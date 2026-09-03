use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::postgres::common::validate_identifier;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "replication", "delivery_modes": ["stream", "batch_and_stream"], "record_semantics": ["changelog"] } }))]
pub struct PostgresReplicationConfig {
    pub slot: String,

    pub decoder: LogicalDecoder,

    #[serde(default = "default_max_changes")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub max_changes: usize,

    #[serde(default = "default_poll_interval_ms")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub poll_interval_ms: u64,

    #[serde(default = "default_bootstrap_timeout_ms")]
    #[schemars(
        title = "Replication bootstrap timeout",
        description = "Maximum time in milliseconds for opening the replication session and exporting the exact slot snapshot",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub bootstrap_timeout_ms: u64,
}

impl PostgresReplicationConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("replication slot", &self.slot)?;
        self.decoder.validate()?;
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

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalDecoder {
    Pgoutput { publication: String },
    Wal2Json,
}

impl LogicalDecoder {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Pgoutput { publication } => validate_identifier("publication", publication),
            Self::Wal2Json => Ok(()),
        }
    }

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
