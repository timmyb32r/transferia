use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::ParserConfig;
use crate::providers::logbroker::{LogbrokerAuthConfig, LogbrokerDriver};

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogbrokerTopicConfig {
    #[schemars(title = "Topic path")]
    pub path: String,

    #[serde(default)]
    #[schemars(
        title = "Partition IDs",
        description = "Partitions to read; leave empty to let YDB Topic assign every partition dynamically",
        extend("x-ui" = { "widget": "partition_ranges" })
    )]
    pub partitions: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogbrokerSourceConfig {
    pub host: String,

    pub port: u16,

    #[schemars(extend("x-ui" = { "control_width": "auth" }))]
    pub auth: LogbrokerAuthConfig,

    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "topic" }))]
    pub topics: Vec<LogbrokerTopicConfig>,

    pub consumer_name: String,

    #[schemars(title = "Driver", extend("x-ui" = { "section": "advanced" }))]
    pub driver: LogbrokerDriver,

    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub trusted_plaintext: bool,

    #[serde(default)]
    #[schemars(
        title = "Allow TTL rewind",
        description = "Continue from the oldest retained message when committed offsets have expired",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub allow_ttl_rewind: bool,

    #[schemars(
        with = "crate::parsers::config::ParserSchema",
        extend("x-ui" = { "widget": "parser" })
    )]
    pub parser: ParserConfig,

    #[serde(default = "default_read_buffer_bytes")]
    #[schemars(extend("x-ui" = { "section": "advanced", "widget": "byte_size" }))]
    pub read_buffer_bytes: usize,

    #[serde(default = "default_pqv1_decompression_concurrency")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub pqv1_decompression_concurrency: usize,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub pqv1_discard_before_decompression: bool,
}

#[derive(Debug, Deserialize)]
pub struct LogbrokerSourceConnectionConfig {
    pub host: String,

    pub port: u16,

    pub topics: Vec<LogbrokerTopicConfig>,

    pub consumer_name: String,

    pub auth: LogbrokerAuthConfig,

    pub driver: LogbrokerDriver,

    pub trusted_plaintext: bool,

    #[serde(default = "default_read_buffer_bytes")]
    pub read_buffer_bytes: usize,
}

#[derive(Deserialize)]
pub struct LogbrokerSourceCheckConfig {
    pub host: String,

    pub port: u16,

    pub auth: LogbrokerAuthConfig,

    #[serde(default)]
    pub topics: Vec<LogbrokerTopicConfig>,

    #[serde(default)]
    pub consumer_name: String,

    #[serde(default)]
    pub driver: Option<LogbrokerDriver>,

    #[serde(default)]
    pub trusted_plaintext: bool,

    #[serde(default = "default_read_buffer_bytes")]
    pub read_buffer_bytes: usize,
}

pub(super) const fn default_read_buffer_bytes() -> usize {
    1_048_576
}

pub(super) const fn default_pqv1_decompression_concurrency() -> usize {
    4
}
