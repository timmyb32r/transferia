use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::postgres::common::{PostgresConnectionConfig, PostgresCopyFormat};
use transferia_registry::table_selection::TableSelection;
use crate::connectors::postgres::src_stream::PostgresReplicationConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "postgres", "delivery_modes": ["batch", "stream", "batch_and_stream"], "record_semantics": ["append_only", "changelog"], "batch_stream_handoff": "exact_switchover" } }))]
pub struct PostgresSourceConfig {
    #[serde(flatten)]
    pub connection: PostgresConnectionConfig,

    #[schemars(extend("x-ui" = { "widget": "table_selection", "table_membership": "fixed" }))]
    pub tables: TableSelection,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub batch_rows: usize,

    #[serde(default)]
    #[schemars(
        title = "COPY TO format",
        description = "PostgreSQL wire format used for snapshot COPY TO STDOUT",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub copy_to_format: PostgresCopyFormat,

    /// Configures logical replication for stream and `batch_and_stream` deliveries.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "inline_object", "section": "advanced", "delivery_types": ["stream", "batch_and_stream"] }))]
    pub replication: PostgresReplicationConfig,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    #[serde(default = "default_schema")]
    pub schema: String,

    pub name: String,
}

impl PostgresSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.rules.is_empty(), "postgres.tables must contain at least one rule");
        self.tables.compile()?;
        anyhow::ensure!(self.batch_rows > 0, "postgres.batch_rows must be positive");
        self.replication.validate()?;
        Ok(())
    }
}

fn default_schema() -> String {
    "public".into()
}
const fn default_batch_rows() -> usize {
    16_384
}
