use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::logbroker::{LogbrokerAuthConfig, LogbrokerDriver};
use crate::serializer::SerializerConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogbrokerSinkConfig {
    pub host: String,

    pub port: u16,

    #[schemars(extend("x-ui" = { "control_width": "auth" }))]
    pub auth: LogbrokerAuthConfig,

    #[schemars(extend("x-ui" = { "control_width": "routing" }))]
    pub topic: LogbrokerTopicConfig,

    #[serde(default)]
    #[schemars(
        title = "Partition ID",
        description = "Write to one explicit partition; leave empty for automatic assignment",
        extend("x-ui" = { "widget": "hidden" })
    )]
    pub partition_id: Option<i64>,

    #[schemars(extend("x-ui" = { "widget": "serializer" }))]
    pub serializer: SerializerConfig,

    #[schemars(title = "Driver", extend("x-ui" = { "section": "advanced" }))]
    pub driver: LogbrokerDriver,

    #[schemars(default = "default_trusted_plaintext", extend("x-ui" = { "widget": "hidden" }))]
    pub trusted_plaintext: bool,
}

const fn default_trusted_plaintext() -> bool {
    true
}

#[derive(Deserialize)]
pub struct LogbrokerSinkCheckConfig {
    pub host: String,

    pub port: u16,

    pub auth: LogbrokerAuthConfig,

    #[serde(default)]
    pub topic: Option<LogbrokerTopicConfig>,

    #[serde(default)]
    pub driver: Option<LogbrokerDriver>,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogbrokerTopicConfig {
    #[schemars(title = "Topic")]
    Topic {
        #[schemars(title = "Topic path")]
        topic_path: String,
    },

    #[schemars(title = "Topic prefix")]
    TopicPrefix {
        #[schemars(
            title = "Topic prefix",
            description = "Each dataset is written to <topic_prefix>.<dataset_name>"
        )]
        topic_prefix: String,
    },
}

impl LogbrokerTopicConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        let (value, label) = match self {
            Self::Topic { topic_path } => (topic_path, "logbroker.topic_path"),
            Self::TopicPrefix { topic_prefix } => {
                (topic_prefix, "logbroker.topic_prefix")
            }
        };
        anyhow::ensure!(!value.is_empty(), "{label} must not be empty");
        anyhow::ensure!(
            value == value.trim(),
            "{label} must not have leading or trailing whitespace"
        );
        Ok(())
    }

    pub(crate) fn fixed_topic(&self) -> Option<&str> {
        match self {
            Self::Topic { topic_path } => Some(topic_path.as_str()),
            Self::TopicPrefix { .. } => None,
        }
        .filter(|topic_path| !topic_path.trim().is_empty())
    }

    pub(super) fn topic_for_table(&self, table: &str) -> String {
        match self {
            Self::Topic { topic_path } => topic_path.clone(),
            Self::TopicPrefix { topic_prefix } => format!("{topic_prefix}.{table}"),
        }
    }
}

impl LogbrokerSinkConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        crate::connectors::address::validate_host("logbroker.host", &self.host)?;
        crate::connectors::address::validate_port("logbroker.port", self.port)?;
        self.topic.validate()?;
        anyhow::ensure!(
            self.partition_id
                .is_none_or(|partition_id| partition_id >= 0),
            "logbroker.partition_id must be nonnegative"
        );
        anyhow::ensure!(
            self.trusted_plaintext,
            "logbroker.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network"
        );
        self.auth.validate()?;
        self.serializer.validate()
    }
}
