use core::time::Duration;

use rdkafka::ClientConfig;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::ParserConfig;
use crate::serializer::SerializerConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KafkaSecurityConfig {
    Plaintext,
}

impl KafkaSecurityConfig {
    pub(super) fn apply(&self, client: &mut ClientConfig) {
        client.set("security.protocol", "plaintext");
    }
}

#[derive(Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaOffsetReset {
    Earliest,
    Latest,
}

impl KafkaOffsetReset {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceConfig {
    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "broker" }))]
    pub brokers: Vec<String>,

    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "topic" }))]
    pub topics: Vec<String>,

    pub consumer_group: String,

    pub security: KafkaSecurityConfig,

    pub offset_reset: KafkaOffsetReset,

    #[schemars(
        with = "crate::parsers::config::ParserSchema",
        extend("x-ui" = { "widget": "parser" })
    )]
    pub parser: ParserConfig,

    #[serde(default = "default_batch_max_messages")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub batch_max_messages: usize,

    #[serde(default = "default_batch_max_bytes")]
    #[schemars(extend("x-ui" = { "section": "advanced", "widget": "byte_size" }))]
    pub batch_max_bytes: usize,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub request_timeout_ms: u64,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkConfig {
    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "broker" }))]
    pub brokers: Vec<String>,

    pub topic: String,

    pub security: KafkaSecurityConfig,

    pub serializer: SerializerConfig,

    #[serde(default)]
    pub partition: Option<i32>,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub request_timeout_ms: u64,

    #[serde(default = "default_max_in_flight")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub max_in_flight: usize,
}

pub(super) const fn default_batch_max_messages() -> usize {
    1_000
}

pub(super) const fn default_batch_max_bytes() -> usize {
    16 * 1024 * 1024
}

pub(super) const fn default_request_timeout_ms() -> u64 {
    30_000
}

const fn default_max_in_flight() -> usize {
    16
}

pub(super) fn validate_brokers(brokers: &[String]) -> anyhow::Result<()> {
    anyhow::ensure!(!brokers.is_empty(), "kafka.brokers must not be empty");
    for broker in brokers {
        anyhow::ensure!(
            !broker.trim().is_empty(),
            "kafka.brokers must not contain empty values"
        );
        anyhow::ensure!(
            broker == broker.trim(),
            "kafka broker addresses must not have leading or trailing whitespace"
        );
    }
    Ok(())
}

pub(super) fn base_client_config(
    brokers: &[String],
    security: &KafkaSecurityConfig,
    request_timeout_ms: u64,
) -> anyhow::Result<ClientConfig> {
    validate_brokers(brokers)?;
    anyhow::ensure!(
        request_timeout_ms > 0,
        "kafka.request_timeout_ms must be positive"
    );
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", brokers.join(","))
        .set("request.timeout.ms", request_timeout_ms.to_string())
        .set("socket.timeout.ms", request_timeout_ms.to_string());
    security.apply(&mut client);
    Ok(client)
}

pub(super) const fn timeout(milliseconds: u64) -> Duration {
    Duration::from_millis(milliseconds)
}
