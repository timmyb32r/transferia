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
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YdbTopicAuthConfig {
    #[schemars(title = "Token")]
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },

    #[schemars(title = "Token file")]
    TokenFile { token_file: String },
}

impl YdbTopicAuthConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Token { token } => anyhow::ensure!(
                !token.trim().is_empty(),
                "ydb_topic.auth.token must not be empty"
            ),
            Self::TokenFile { token_file } => anyhow::ensure!(
                !token_file.trim().is_empty(),
                "ydb_topic.auth.token_file must not be empty"
            ),
        }
        Ok(())
    }

    pub(super) fn load_token(&self) -> anyhow::Result<String> {
        self.validate()?;
        let token = match self {
            Self::Token { token } => token.clone(),
            Self::TokenFile { token_file } => {
                let expanded = shellexpand::full(token_file).map_err(|error| {
                    anyhow::anyhow!(
                        "Failed to expand ydb_topic.auth.token_file '{token_file}': {error}"
                    )
                })?;
                std::fs::read_to_string(expanded.as_ref()).map_err(|error| {
                    anyhow::anyhow!("Failed to read YDB access token from '{expanded}': {error}")
                })?
            }
        };
        let token = token.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "YDB access token is empty");
        Ok(token)
    }
}

impl fmt::Debug for YdbTopicAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token { .. } => formatter
                .debug_struct("Token")
                .field("token", &"[REDACTED]")
                .finish(),
            Self::TokenFile { token_file } => formatter
                .debug_struct("TokenFile")
                .field("token_file", token_file)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbTopicSourceConfig {
    pub host: String,

    pub port: u16,

    pub topic_path: String,

    pub consumer_name: String,

    #[schemars(
        description = "API used only to discover fixed partitions before opening YDB Topic read sessions"
    )]
    pub topology_discovery: TopologyDiscovery,

    pub auth: YdbTopicAuthConfig,

    pub trusted_plaintext: bool,

    #[schemars(
        with = "crate::parsers::config::ParserSchema",
        extend("x-ui" = { "widget": "parser" })
    )]
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
