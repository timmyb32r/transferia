use schemars::JsonSchema;
use serde::Deserialize;

use crate::providers::postgres::common::PostgresConnectionConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkConfig {
    #[serde(flatten)]
    pub connection: PostgresConnectionConfig,

    pub create_tables: bool,
}

impl PostgresSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()
    }
}
