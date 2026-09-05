use schemars::JsonSchema;
use serde::Deserialize;
use transferia_registry::table_selection::TableSelection;

use crate::connectors::mysql::common::{
    MySqlConnectionConfig, MYSQL_CLIENT_PACKET_MAX_BYTES,
    MYSQL_CLIENT_PACKET_MIN_BYTES,
};
use crate::connectors::mysql::src_stream::MySqlReplicationConfig;

pub const DEFAULT_MYSQL_BATCH_TARGET_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MYSQL_MAX_ROW_BYTES: usize = MYSQL_CLIENT_PACKET_MAX_BYTES;
pub const MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MySqlReadProtocol {
    Text,

    #[default]
    Binary,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, serde::Serialize, Eq, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NewTables {
    #[default]
    #[schemars(title = "Include automatically")]
    IncludeAutomatically,
    #[schemars(title = "Ignore new tables")]
    Ignore,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "mysql", "delivery_modes": ["batch", "stream", "batch_and_stream"], "record_semantics": ["append_only", "changelog"], "batch_stream_handoff": "exact_switchover" } }))]
pub struct MySqlSourceConfig {
    #[serde(flatten)]
    pub connection: MySqlConnectionConfig,

    #[schemars(extend("x-ui" = { "widget": "table_selection" }))]
    pub tables: TableSelection,

    #[serde(default)]
    #[schemars(title = "New tables", description = "Include automatically follows CREATE TABLE events from the binlog and applies the same Include/Exclude rules. The destination is prepared before the first row is read. Existing tables renamed into a rule are rejected: their earlier rows require a new snapshot. Watermark-based DBLog support is future work. Ignore new tables keeps the startup membership fixed. Restart restores the committed membership, not current wildcard matches.",
        extend("x-ui" = { "delivery_types": ["stream", "batch_and_stream"] }))]
    pub new_tables: NewTables,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub batch_rows: usize,

    #[serde(default = "default_batch_target_bytes")]
    #[schemars(
        range(min = 1, max = 1_073_741_824),
        title = "Snapshot batch target bytes",
        description = "Target retained decoded MySQL row heap per snapshot batch. The reader may include one final indivisible row after crossing the target; max_row_bytes separately bounds its wire packet, while decoded Row/Value overhead is measured and accounted from actual allocations.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub batch_target_bytes: usize,

    #[serde(default = "default_max_row_bytes")]
    #[schemars(
        range(min = 1024, max = 1_073_741_824),
        title = "Maximum snapshot row packet bytes",
        description = "Exact mysql_async client max_allowed_packet for one MySQL wire packet. Decoded Row/Value overhead is measured and accounted separately. Valid range: 1024..=1073741824 bytes.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub max_row_bytes: usize,

    #[serde(default)]
    #[schemars(
        title = "Read protocol",
        description = "MySQL wire protocol used for snapshot rows. Binary is the lossless measured high-throughput default. Text remains available for supported schemas, but discovery rejects FLOAT columns because MySQL text formatting cannot preserve every f32 value exactly.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub read_protocol: MySqlReadProtocol,

    /// Configures row-based binary-log replication for stream and `batch_and_stream` deliveries.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub replication: MySqlReplicationConfig,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub database: String,
    pub name: String,
}

impl MySqlSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.rules.is_empty(), "mysql.tables must contain at least one rule");
        self.tables.compile()?;
        anyhow::ensure!(self.batch_rows > 0, "mysql.batch_rows must be positive");
        anyhow::ensure!(
            (1..=MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES).contains(&self.batch_target_bytes),
            "mysql.batch_target_bytes must be in 1..={MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES}"
        );
        anyhow::ensure!(
            (MYSQL_CLIENT_PACKET_MIN_BYTES..=MYSQL_CLIENT_PACKET_MAX_BYTES)
                .contains(&self.max_row_bytes),
            "mysql.max_row_bytes must be in {MYSQL_CLIENT_PACKET_MIN_BYTES}..={MYSQL_CLIENT_PACKET_MAX_BYTES}"
        );
        self.batch_target_bytes
            .checked_add(self.max_row_bytes)
            .ok_or_else(|| anyhow::anyhow!(
                "mysql.batch_target_bytes + mysql.max_row_bytes exceeds this platform's addressable memory"
            ))?;
        self.replication.validate()?;
        Ok(())
    }
}

const fn default_batch_rows() -> usize {
    16_384
}

const fn default_batch_target_bytes() -> usize {
    DEFAULT_MYSQL_BATCH_TARGET_BYTES
}

const fn default_max_row_bytes() -> usize {
    DEFAULT_MYSQL_MAX_ROW_BYTES
}
