use std::collections::HashSet;

use schemars::JsonSchema;
use serde::Deserialize;

use super::super::{validate_index_name, OpenSearchConnectionConfig};

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexConfig {
    pub name: String,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenSearchSourceConfig {
    #[serde(flatten)]
    pub connection: OpenSearchConnectionConfig,

    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "index" }))]
    pub indices: Vec<IndexConfig>,

    #[serde(default = "default_page_rows")]
    #[schemars(
        title = "Rows per search page",
        description = "Maximum documents returned by one sliced point-in-time request",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub page_rows: usize,

    #[serde(default = "default_read_concurrency")]
    #[schemars(
        title = "Parallel shard readers",
        description = "Maximum simultaneous slice page requests per index",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub read_concurrency: usize,

    #[serde(default = "default_pit_keep_alive_ms")]
    #[schemars(
        title = "Point-in-time keep alive, ms",
        description = "OpenSearch renews the coherent index PIT lifetime on every snapshot page request",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub pit_keep_alive_ms: u64,

    #[serde(default = "default_retry_initial_ms")]
    #[schemars(
        title = "Retry initial delay, ms",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub retry_initial_ms: u64,

    #[serde(default = "default_retry_max_ms")]
    #[schemars(
        title = "Retry maximum delay, ms",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub retry_max_ms: u64,

    #[serde(default = "default_retry_max_attempts")]
    #[schemars(
        title = "Retry maximum attempts",
        description = "Total attempts for one unchanged PIT operation before failing",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub retry_max_attempts: usize,
}

impl OpenSearchSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(
            !self.indices.is_empty(),
            "opensearch.indices must not be empty"
        );
        let mut indices = HashSet::with_capacity(self.indices.len());
        for index in &self.indices {
            validate_index_name(&index.name)
                .map_err(|error| error.context("invalid opensearch.indices.name"))?;
            anyhow::ensure!(
                indices.insert(index.name.as_str()),
                "opensearch.indices repeats exact index '{}'",
                index.name
            );
        }
        anyhow::ensure!(self.page_rows > 0, "opensearch.page_rows must be positive");
        anyhow::ensure!(
            self.read_concurrency > 0,
            "opensearch.read_concurrency must be positive"
        );
        anyhow::ensure!(
            self.pit_keep_alive_ms > 0,
            "opensearch.pit_keep_alive_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_initial_ms > 0,
            "opensearch.retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_max_ms >= self.retry_initial_ms,
            "opensearch.retry_max_ms must be at least retry_initial_ms"
        );
        anyhow::ensure!(
            self.retry_max_attempts > 0,
            "opensearch.retry_max_attempts must be positive"
        );
        Ok(())
    }

    pub(super) fn pit_keep_alive(&self) -> String {
        format!("{}ms", self.pit_keep_alive_ms)
    }
}

const fn default_page_rows() -> usize {
    10_000
}

const fn default_read_concurrency() -> usize {
    2
}

const fn default_pit_keep_alive_ms() -> u64 {
    300_000
}

const fn default_retry_initial_ms() -> u64 {
    100
}

const fn default_retry_max_ms() -> u64 {
    10_000
}

const fn default_retry_max_attempts() -> usize {
    10
}
