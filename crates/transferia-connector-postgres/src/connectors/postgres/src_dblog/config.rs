use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::postgres::common::validate_identifier;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresReplicationConfig {
    pub slot: String,

    pub decoder: LogicalDecoder,

    #[serde(default = "default_max_changes")]
    #[schemars(extend("x-ui" = { "advanced": true }))]
    pub max_changes: usize,

    #[serde(default = "default_poll_interval_ms")]
    #[schemars(extend("x-ui" = { "advanced": true }))]
    pub poll_interval_ms: u64,
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

    pub(super) const fn plugin(&self) -> &'static str {
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
