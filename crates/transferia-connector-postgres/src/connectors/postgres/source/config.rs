use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::postgres::common::{
    validate_identifier, PostgresConnectionConfig, PostgresCopyFormat,
};
use crate::connectors::postgres::src_stream::PostgresReplicationConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "snapshot", "delivery_modes": ["batch"], "record_semantics": ["append_only"] } }))]
pub struct PostgresSourceConfig {
    #[serde(flatten)]
    pub connection: PostgresConnectionConfig,

    pub tables: Vec<TableConfig>,

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
    pub replication: Option<PostgresReplicationConfig>,
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
        anyhow::ensure!(!self.tables.is_empty(), "postgres.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "postgres.batch_rows must be positive");
        if let Some(replication) = &self.replication {
            replication.validate()?;
        }
        let mut names = std::collections::HashSet::new();
        for table in &self.tables {
            validate_identifier("schema", &table.schema)?;
            validate_identifier("table", &table.name)?;
            anyhow::ensure!(
                names.insert(table.name.as_str()),
                "postgres.tables repeats destination name '{}'",
                table.name
            );
        }
        Ok(())
    }
}

fn default_schema() -> String {
    "public".into()
}
const fn default_batch_rows() -> usize {
    16_384
}
