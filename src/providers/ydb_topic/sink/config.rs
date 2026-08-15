use schemars::JsonSchema;
use serde::Deserialize;

use crate::providers::ydb_topic::src_stream::{YdbTopicAuthConfig, YdbTopicDriver};

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbTopicSinkConfig {
    pub host: String,

    pub port: u16,

    #[schemars(title = "Topic path")]
    pub topic_path: String,

    #[schemars(
        title = "Producer ID",
        description = "Stable producer identity used for ordering and deduplication"
    )]
    pub producer_id: String,

    #[serde(default)]
    #[schemars(
        title = "Partition ID",
        description = "Write to one explicit partition; leave empty for automatic assignment"
    )]
    pub partition_id: Option<i64>,

    pub auth: YdbTopicAuthConfig,

    #[schemars(title = "Driver", extend("x-ui" = { "section": "advanced" }))]
    pub driver: YdbTopicDriver,

    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub trusted_plaintext: bool,
}

impl YdbTopicSinkConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        crate::providers::address::validate_host("ydb_topic.host", &self.host)?;
        crate::providers::address::validate_port("ydb_topic.port", self.port)?;
        anyhow::ensure!(
            !self.topic_path.trim().is_empty(),
            "ydb_topic.topic_path must not be empty"
        );
        anyhow::ensure!(
            !self.producer_id.is_empty() && self.producer_id.len() <= 2048,
            "ydb_topic.producer_id must contain 1..=2048 UTF-8 bytes"
        );
        anyhow::ensure!(
            self.partition_id
                .is_none_or(|partition_id| partition_id >= 0),
            "ydb_topic.partition_id must be nonnegative"
        );
        anyhow::ensure!(
            self.trusted_plaintext,
            "ydb_topic.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network"
        );
        self.auth.validate()
    }
}
