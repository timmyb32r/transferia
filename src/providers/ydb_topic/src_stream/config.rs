use core::fmt;

use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::ParserConfig;

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TopologyDiscovery {
    TopicApi,
    Configured,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbTopicAuthConfig {
    #[serde(rename = "type")]
    pub auth_type: String,

    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub token: Option<String>,

    pub token_file: Option<String>,
}

impl YdbTopicAuthConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.auth_type == "access_token",
            "ydb_topic.auth.type must be 'access_token'"
        );
        anyhow::ensure!(
            self.token.is_some() ^ self.token_file.is_some(),
            "ydb_topic.auth requires exactly one of 'token' or 'token_file'"
        );
        if let Some(token) = &self.token {
            anyhow::ensure!(
                !token.trim().is_empty(),
                "ydb_topic.auth.token must not be empty"
            );
        }
        if let Some(path) = &self.token_file {
            anyhow::ensure!(
                !path.trim().is_empty(),
                "ydb_topic.auth.token_file must not be empty"
            );
        }
        Ok(())
    }

    pub(super) fn load_token(&self) -> anyhow::Result<String> {
        self.validate()?;
        let token = if let Some(path) = self.token_file.as_deref() {
            let expanded = shellexpand::full(path).map_err(|error| {
                anyhow::anyhow!("Failed to expand ydb_topic.auth.token_file '{path}': {error}")
            })?;
            std::fs::read_to_string(expanded.as_ref()).map_err(|error| {
                anyhow::anyhow!("Failed to read YDB access token from '{expanded}': {error}")
            })?
        } else if let Some(token) = self.token.as_deref() {
            token.to_owned()
        } else {
            anyhow::bail!("ydb_topic.auth has no configured token source");
        };
        let token = token.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "YDB access token is empty");
        Ok(token)
    }
}

impl fmt::Debug for YdbTopicAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YdbTopicAuthConfig")
            .field("auth_type", &self.auth_type)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("token_file", &self.token_file)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbTopicSourceConfig {
    #[schemars(description = "YDB or Logbroker entry points tried in order")]
    pub hosts: Vec<String>,

    pub port: u16,

    #[schemars(description = "YDB database path, for Logbroker usually /Root")]
    pub database: String,

    pub topic_path: String,

    pub consumer_name: String,

    #[schemars(
        description = "API used only to discover fixed partitions before opening YDB Topic read sessions"
    )]
    pub topology_discovery: TopologyDiscovery,

    pub auth: YdbTopicAuthConfig,

    pub trusted_plaintext: bool,

    #[schemars(with = "crate::parsers::config::ParserSchema")]
    pub parser: ParserConfig,

    #[serde(default)]
    #[schemars(
        description = "Partitions to read; an empty list means every active partition discovered at startup"
    )]
    pub partition_ids: Vec<i64>,

    #[serde(default = "default_network_timeout_ms")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub network_timeout_ms: u64,

    #[serde(default = "default_read_buffer_bytes")]
    #[schemars(extend("x-ui" = { "section": "advanced", "widget": "byte_size" }))]
    pub read_buffer_bytes: usize,
}

const fn default_network_timeout_ms() -> u64 {
    10_000
}

const fn default_read_buffer_bytes() -> usize {
    1_048_576
}
