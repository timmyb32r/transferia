use schemars::JsonSchema;
use serde::Deserialize;

use crate::providers::postgres::common::validate_identifier;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    pub connection: String,
    pub trusted_plaintext: bool,
    pub tables: Vec<TableConfig>,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
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
        anyhow::ensure!(
            !self.connection.trim().is_empty(),
            "postgres.connection must not be empty"
        );
        anyhow::ensure!(self.trusted_plaintext, "postgres.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network");
        anyhow::ensure!(!self.tables.is_empty(), "postgres.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "postgres.batch_rows must be positive");
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
    65_536
}
