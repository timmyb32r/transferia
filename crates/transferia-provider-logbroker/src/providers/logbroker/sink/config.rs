use schemars::JsonSchema;
use serde::Deserialize;

use crate::providers::logbroker::{LogbrokerAuthConfig, LogbrokerDriver};
use crate::serializer::SerializerConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogbrokerSinkConfig {
    pub host: String,

    pub port: u16,

    #[schemars(extend("x-ui" = { "control_width": "auth" }))]
    pub auth: LogbrokerAuthConfig,

    #[schemars(title = "Topic path")]
    pub topic_path: String,

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

    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub trusted_plaintext: bool,
}

#[derive(Deserialize)]
pub struct LogbrokerSinkCheckConfig {
    pub host: String,

    pub port: u16,

    pub auth: LogbrokerAuthConfig,

    #[serde(default)]
    pub topic_path: String,

    #[serde(default)]
    pub driver: Option<LogbrokerDriver>,
}

impl LogbrokerSinkConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        crate::providers::address::validate_host("logbroker.host", &self.host)?;
        crate::providers::address::validate_port("logbroker.port", self.port)?;
        anyhow::ensure!(
            !self.topic_path.trim().is_empty(),
            "logbroker.topic_path must not be empty"
        );
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
