use core::fmt;

use serde::Deserialize;

use crate::parsers::ParserConfig;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PqV1AuthConfig {
    #[serde(rename = "type")]
    pub auth_type: String,
    pub token: Option<String>,
    pub token_file: Option<String>,
}

impl PqV1AuthConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.auth_type == "access_token",
            "pqv1.auth.type must be 'access_token'"
        );
        anyhow::ensure!(
            self.token.is_some() ^ self.token_file.is_some(),
            "pqv1.auth requires exactly one of 'token' or 'token_file'"
        );
        if let Some(token) = &self.token {
            anyhow::ensure!(
                !token.trim().is_empty(),
                "pqv1.auth.token must not be empty"
            );
        }
        if let Some(path) = &self.token_file {
            anyhow::ensure!(
                !path.trim().is_empty(),
                "pqv1.auth.token_file must not be empty"
            );
        }
        Ok(())
    }
}

impl fmt::Debug for PqV1AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PqV1AuthConfig")
            .field("auth_type", &self.auth_type)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("token_file", &self.token_file)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PqV1SourceConfig {
    pub connection_string: String,
    pub topic_path: String,
    pub consumer_name: String,
    pub auth: PqV1AuthConfig,
    pub parser: ParserConfig,
    pub partition_ids: Vec<i64>,
    #[serde(default = "default_network_timeout_ms")]
    pub network_timeout_ms: u64,
    #[serde(default = "default_decompression_concurrency")]
    pub decompression_concurrency: usize,
    #[serde(default)]
    pub benchmark_discard_before_decompression: bool,
}

const fn default_network_timeout_ms() -> u64 {
    30_000
}

const fn default_decompression_concurrency() -> usize {
    4
}
