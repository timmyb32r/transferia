use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::mysql::common::MySqlConnectionConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MySqlSinkConfig {
    #[serde(flatten)]
    pub connection: MySqlConnectionConfig,

    pub create_tables: bool,

    #[serde(default = "default_insert_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub insert_rows: usize,
}

impl MySqlSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(self.insert_rows > 0, "mysql.insert_rows must be positive");
        Ok(())
    }
}

const fn default_insert_rows() -> usize {
    250
}
