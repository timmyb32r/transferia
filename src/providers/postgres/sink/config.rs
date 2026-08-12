use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkConfig {
    pub connection: String,
    pub trusted_plaintext: bool,
    pub create_tables: bool,
}

impl PostgresSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.connection.trim().is_empty(),
            "postgres.connection must not be empty"
        );
        anyhow::ensure!(self.trusted_plaintext, "postgres.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network");
        Ok(())
    }
}
