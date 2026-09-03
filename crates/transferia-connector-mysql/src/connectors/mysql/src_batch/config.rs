use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::mysql::common::{validate_identifier, MySqlConnectionConfig};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MySqlReadProtocol {
    Text,

    #[default]
    Binary,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MySqlSourceConfig {
    #[serde(flatten)]
    pub connection: MySqlConnectionConfig,

    pub tables: Vec<TableConfig>,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub batch_rows: usize,

    #[serde(default)]
    #[schemars(
        title = "Read protocol",
        description = "MySQL wire protocol used for snapshot rows. Binary is the measured high-throughput default; text remains available for comparison and compatibility diagnostics.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub read_protocol: MySqlReadProtocol,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub name: String,
}

impl MySqlSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "mysql.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "mysql.batch_rows must be positive");
        let mut names = std::collections::HashSet::new();
        for table in &self.tables {
            validate_identifier("table", &table.name)?;
            anyhow::ensure!(
                names.insert(table.name.as_str()),
                "mysql.tables repeats table name '{}'",
                table.name
            );
        }
        Ok(())
    }
}

const fn default_batch_rows() -> usize {
    16_384
}
