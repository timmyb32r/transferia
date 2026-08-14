use core::fmt;

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PqV1AuthConfig {
    #[serde(rename = "type")]
    pub auth_type: String,

    #[schemars(extend("x-ui" = { "widget": "password" }))]
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

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PqV1SinkConfig {
    pub host: String,

    pub port: u16,

    pub topic_path: String,

    pub message_group_id: String,

    pub partition_group_id: i64,

    pub auth: PqV1AuthConfig,

    pub trusted_plaintext: bool,

    #[serde(default = "default_network_timeout_ms")]
    pub network_timeout_ms: u64,
}

impl PqV1SinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        crate::providers::address::validate_host("pqv1.host", &self.host)?;
        crate::providers::address::validate_port("pqv1.port", self.port)?;
        anyhow::ensure!(self.trusted_plaintext, "pqv1.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network");
        anyhow::ensure!(
            !self.topic_path.is_empty(),
            "pqv1.topic_path must not be empty"
        );
        anyhow::ensure!(
            !self.message_group_id.is_empty() && self.message_group_id.len() <= 2048,
            "pqv1.message_group_id must contain 1..=2048 UTF-8 bytes"
        );
        anyhow::ensure!(
            self.partition_group_id >= 0,
            "pqv1.partition_group_id must be nonnegative"
        );
        anyhow::ensure!(
            self.network_timeout_ms >= 100,
            "pqv1.network_timeout_ms must be at least 100ms"
        );
        self.auth.validate()
    }

    #[must_use]
    pub const fn network_timeout(&self) -> core::time::Duration {
        core::time::Duration::from_millis(self.network_timeout_ms)
    }

    pub(crate) fn endpoint(&self) -> String {
        crate::providers::address::url("grpc", &self.host, self.port)
    }
}

pub(crate) const fn default_network_timeout_ms() -> u64 {
    30_000
}
