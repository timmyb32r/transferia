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
    pub(crate) fn target_database<'a>(
        &'a self,
        namespace: Option<&'a str>,
    ) -> anyhow::Result<&'a str> {
        let database = if self.connection.database.is_empty() {
            namespace.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL destination needs a Database: the source dataset has no database/schema"
                )
            })?
        } else {
            &self.connection.database
        };
        crate::connectors::mysql::common::validate_identifier("database", database)?;
        Ok(database)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(self.insert_rows > 0, "mysql.insert_rows must be positive");
        Ok(())
    }
}

const fn default_insert_rows() -> usize {
    250
}
