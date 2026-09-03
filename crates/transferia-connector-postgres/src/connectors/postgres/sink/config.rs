use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::postgres::common::{PostgresConnectionConfig, PostgresCopyFormat};

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkConfig {
    #[serde(flatten)]
    pub connection: PostgresConnectionConfig,

    pub create_tables: bool,

    #[serde(default)]
    #[schemars(
        title = "COPY FROM format",
        description = "PostgreSQL wire format used for COPY FROM STDIN",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub copy_from_format: PostgresCopyFormat,
}

impl PostgresSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()
    }
}
