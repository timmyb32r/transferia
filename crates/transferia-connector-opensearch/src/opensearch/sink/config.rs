use std::fmt;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;

use super::super::OpenSearchConnectionConfig;

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutedIdentity {
    /// Reject custom-routed source documents because OpenSearch does not make
    /// (`_id`, `_routing`) globally unique across shards.
    #[default]
    #[schemars(title = "Fail on custom routing")]
    Fail,

    /// Explicitly replace destination `_id` with a lossless encoding of the
    /// complete (`_id`, effective routing key) source identity.
    #[schemars(title = "Encode the complete routed identity")]
    EncodeIdentity,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenSearchSinkConfig {
    #[serde(flatten)]
    pub connection: OpenSearchConnectionConfig,

    pub create_indices: bool,

    /// Controls custom-routed source documents. The default fails before any
    /// request. `encode_identity` is an explicit, injective destination `_id`
    /// transformation that preserves (`_id`, effective routing key) identity.
    #[serde(default)]
    #[schemars(
        title = "Custom routing identity",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub routed_identity: RoutedIdentity,

    #[serde(default = "default_bulk_target_rows")]
    #[schemars(range(min = 1), extend("x-ui" = { "section": "advanced" }))]
    pub bulk_target_rows: usize,

    #[serde(default = "default_bulk_target_bytes")]
    #[schemars(
        range(min = 1),
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub bulk_target_bytes: usize,

    #[serde(default = "default_bulk_concurrency")]
    #[schemars(range(min = 1, max = 32), extend("x-ui" = { "section": "advanced" }))]
    pub bulk_concurrency: usize,

    #[serde(default = "default_flush_interval_ms")]
    #[schemars(range(min = 1), extend("x-ui" = { "section": "advanced" }))]
    pub flush_interval_ms: u64,

    #[serde(default = "default_retry_initial_ms")]
    #[schemars(range(min = 1), extend("x-ui" = { "section": "advanced" }))]
    pub retry_initial_ms: u64,

    #[serde(default = "default_retry_max_ms")]
    #[schemars(range(min = 1), extend("x-ui" = { "section": "advanced" }))]
    pub retry_max_ms: u64,

    #[serde(default = "default_retry_max_attempts")]
    #[schemars(range(min = 1), extend("x-ui" = { "section": "advanced" }))]
    pub retry_max_attempts: u32,
}

impl OpenSearchSinkConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(
            self.bulk_target_rows > 0,
            "opensearch.bulk_target_rows must be positive"
        );
        anyhow::ensure!(
            self.bulk_target_bytes > 0,
            "opensearch.bulk_target_bytes must be positive"
        );
        anyhow::ensure!(
            (1..=32).contains(&self.bulk_concurrency),
            "opensearch.bulk_concurrency must be between 1 and 32"
        );
        anyhow::ensure!(
            self.flush_interval_ms > 0,
            "opensearch.flush_interval_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_initial_ms > 0,
            "opensearch.retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_max_ms >= self.retry_initial_ms,
            "opensearch.retry_max_ms must be greater than or equal to retry_initial_ms"
        );
        anyhow::ensure!(
            self.retry_max_attempts > 0,
            "opensearch.retry_max_attempts must be positive"
        );
        Ok(())
    }

    pub(super) const fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms)
    }
}

impl fmt::Debug for OpenSearchSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSearchSinkConfig")
            .field("connection", &self.connection)
            .field("create_indices", &self.create_indices)
            .field("routed_identity", &self.routed_identity)
            .field("bulk_target_rows", &self.bulk_target_rows)
            .field("bulk_target_bytes", &self.bulk_target_bytes)
            .field("bulk_concurrency", &self.bulk_concurrency)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("retry_initial_ms", &self.retry_initial_ms)
            .field("retry_max_ms", &self.retry_max_ms)
            .field("retry_max_attempts", &self.retry_max_attempts)
            .finish()
    }
}

const fn default_bulk_target_rows() -> usize {
    20_000
}

const fn default_bulk_target_bytes() -> usize {
    16 * 1024 * 1024
}

const fn default_bulk_concurrency() -> usize {
    4
}

const fn default_flush_interval_ms() -> u64 {
    250
}

const fn default_retry_initial_ms() -> u64 {
    100
}

const fn default_retry_max_ms() -> u64 {
    10_000
}

const fn default_retry_max_attempts() -> u32 {
    10
}
