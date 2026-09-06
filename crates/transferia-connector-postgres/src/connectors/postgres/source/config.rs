use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::postgres::common::{PostgresConnectionConfig, PostgresCopyFormat};
use crate::connectors::postgres::src_stream::PostgresReplicationConfig;
use transferia_registry::table_selection::TableSelection;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "postgres", "delivery_modes": ["batch", "stream", "batch_and_stream"], "record_semantics": ["append_only", "changelog"], "batch_stream_handoff": "exact_switchover" } }))]
pub struct PostgresSourceConfig {
    #[serde(flatten)]
    pub connection: PostgresConnectionConfig,

    #[serde(default = "default_hide_system_tables")]
    #[schemars(
        title = "Hide system tables",
        description = "Exclude tables in information_schema and schemas whose names start with pg_ from table selection and suggestions. Disable to include them. Changing this filter uses the last successful connection check without reconnecting. The same filter applies at startup; PostgreSQL table membership stays fixed during replication.",
        extend("x-ui" = { "order": 1 })
    )]
    pub hide_system_tables: bool,

    #[schemars(extend("x-ui" = { "widget": "table_selection", "table_membership": "fixed", "order": 2 }))]
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
    pub(crate) fn resolve_tables(
        &self,
        mut catalog: Vec<transferia_registry::TableIdentity>,
    ) -> anyhow::Result<Vec<transferia_registry::TableIdentity>> {
        if self.hide_system_tables {
            catalog.retain(|table| {
                table.namespace != "information_schema" && !table.namespace.starts_with("pg_")
            });
        }
        self.tables.compile()?.resolve(&catalog)?.selected_tables()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(
            !self.tables.is_empty(),
            "postgres.tables must contain at least one rule"
        );
        self.tables.compile()?;
        anyhow::ensure!(self.batch_rows > 0, "postgres.batch_rows must be positive");
        self.replication.validate()?;
        Ok(())
    }
}

fn default_schema() -> String {
    "public".into()
}
const fn default_hide_system_tables() -> bool {
    true
}
const fn default_batch_rows() -> usize {
    16_384
}
