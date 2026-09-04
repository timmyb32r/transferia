use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "replication", "delivery_modes": ["stream", "batch_and_stream"], "record_semantics": ["changelog"] } }))]
pub struct MySqlReplicationConfig {
    #[schemars(
        title = "Replica server ID",
        description = "Unique non-zero MySQL replication client ID used to fence this binlog reader on the source server",
        range(min = 1)
    )]
    pub server_id: u32,

    #[serde(default = "default_max_events")]
    #[schemars(
        title = "Maximum transaction events",
        description = "Maximum binlog events and decoded rows accepted in one transaction before failing closed; lower values bound memory more tightly",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub max_events: usize,

    #[serde(default = "default_max_transaction_bytes")]
    #[schemars(
        title = "Maximum transaction bytes",
        description = "Maximum encoded binlog bytes buffered for one transaction before failing closed",
        range(min = 19, max = 1_073_741_824),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub max_transaction_bytes: usize,

    #[serde(default = "default_poll_interval_ms")]
    #[schemars(
        title = "Stream poll interval",
        description = "Milliseconds between MySQL replication heartbeats and the maximum time a local read waits before yielding an empty batch; it must be shorter than the replication request timeout so an idle closed reader releases its connection-owned execution lock before reacquisition times out",
        range(min = 1_u64, max = 18_446_744_073_709_u64),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub poll_interval_ms: u64,

    #[serde(default = "default_bootstrap_timeout_ms")]
    #[schemars(
        title = "Replication request timeout",
        description = "Maximum milliseconds for replication preflight, exact-boundary capture, stream startup, and stream shutdown requests",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub bootstrap_timeout_ms: u64,
}

impl MySqlReplicationConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.server_id > 0,
            "mysql.replication.server_id must be positive"
        );
        anyhow::ensure!(
            self.max_events > 0,
            "mysql.replication.max_events must be positive"
        );
        anyhow::ensure!(
            self.max_transaction_bytes
                >= mysql_async::binlog::events::BinlogEventHeader::LEN,
            "mysql.replication.max_transaction_bytes must be at least one 19-byte binlog event header"
        );
        anyhow::ensure!(
            self.max_transaction_bytes <= super::super::MYSQL_CLIENT_PACKET_MAX_BYTES,
            "mysql.replication.max_transaction_bytes must not exceed the MySQL protocol maximum of {} bytes",
            super::super::MYSQL_CLIENT_PACKET_MAX_BYTES
        );
        anyhow::ensure!(
            self.poll_interval_ms > 0,
            "mysql.replication.poll_interval_ms must be positive"
        );
        heartbeat_period_nanoseconds(self.poll_interval_ms)?;
        anyhow::ensure!(
            self.bootstrap_timeout_ms > 0,
            "mysql.replication.bootstrap_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.poll_interval_ms < self.bootstrap_timeout_ms,
            "mysql.replication.poll_interval_ms must be shorter than bootstrap_timeout_ms so an idle binlog connection releases its execution lock before lock reacquisition times out"
        );
        Ok(())
    }
}

pub fn heartbeat_period_nanoseconds(poll_interval_ms: u64) -> anyhow::Result<u64> {
    poll_interval_ms.checked_mul(1_000_000).ok_or_else(|| {
        anyhow::anyhow!(
            "mysql.replication.poll_interval_ms does not fit MySQL's unsigned 64-bit heartbeat period in nanoseconds"
        )
    })
}

const fn default_max_events() -> usize {
    4_096
}

const fn default_max_transaction_bytes() -> usize {
    64 * 1024 * 1024
}

const fn default_poll_interval_ms() -> u64 {
    100
}

const fn default_bootstrap_timeout_ms() -> u64 {
    30_000
}
