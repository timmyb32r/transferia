use std::collections::HashSet;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_BATCH_ROWS: usize = 65_536;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusConnectionConfig {
    pub host: String,

    pub port: u16,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub token: Option<String>,

    pub trusted_plaintext: bool,

    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

impl YTsaurusConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        crate::providers::address::validate_host("ytsaurus.host", &self.host)?;
        crate::providers::address::validate_port("ytsaurus.port", self.port)?;
        anyhow::ensure!(self.timeout_ms > 0, "ytsaurus.timeout_ms must be positive");
        if let Some(token) = &self.token {
            anyhow::ensure!(!token.is_empty(), "ytsaurus.token must not be empty");
        }
        Ok(())
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub(crate) fn endpoint(&self) -> String {
        crate::providers::address::url(
            if self.trusted_plaintext {
                "http"
            } else {
                "https"
            },
            &self.host,
            self.port,
        )
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusSourceConfig {
    #[serde(flatten)]
    pub connection: YTsaurusConnectionConfig,

    pub tables: Vec<SourceTableConfig>,

    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceTableConfig {
    pub path: String,

    pub output_name: String,
}

impl YTsaurusSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "ytsaurus.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "ytsaurus.batch_rows must be positive");
        let mut paths = HashSet::new();
        let mut outputs = HashSet::new();
        for table in &self.tables {
            validate_path(&table.path)?;
            anyhow::ensure!(
                !table.output_name.is_empty(),
                "ytsaurus.tables.output_name must not be empty"
            );
            anyhow::ensure!(
                paths.insert(table.path.as_str()),
                "ytsaurus.tables repeats path '{}'",
                table.path
            );
            anyhow::ensure!(
                outputs.insert(table.output_name.as_str()),
                "ytsaurus.tables repeats output_name '{}'",
                table.output_name
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusWriteFormat {
    #[default]
    Arrow,
    Yson,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusSinkConfig {
    #[serde(flatten)]
    pub connection: YTsaurusConnectionConfig,

    pub tables: Vec<SinkTableConfig>,

    pub replace_tables: bool,

    #[serde(default)]
    pub format: YTsaurusWriteFormat,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SinkTableConfig {
    pub dataset: String,

    pub path: String,
}

impl YTsaurusSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "ytsaurus.tables must not be empty");
        let mut paths = HashSet::new();
        let mut datasets = HashSet::new();
        for table in &self.tables {
            validate_path(&table.path)?;
            anyhow::ensure!(
                !table.dataset.is_empty(),
                "ytsaurus.tables.dataset must not be empty"
            );
            anyhow::ensure!(
                paths.insert(table.path.as_str()),
                "ytsaurus.tables repeats path '{}'",
                table.path
            );
            anyhow::ensure!(
                datasets.insert(table.dataset.as_str()),
                "ytsaurus.tables repeats dataset '{}'",
                table.dataset
            );
        }
        Ok(())
    }

    pub fn path_for_dataset(&self, dataset: &str) -> anyhow::Result<&str> {
        self.tables
            .iter()
            .find(|table| table.dataset == dataset)
            .map(|table| table.path.as_str())
            .ok_or_else(|| anyhow::anyhow!("no YTsaurus path configured for dataset '{dataset}'"))
    }
}

pub fn validate_path(path: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.starts_with("//"),
        "YTsaurus table path must start with '//'"
    );
    anyhow::ensure!(path.len() > 2, "YTsaurus table path must not be the root");
    anyhow::ensure!(
        !path.contains('<') && !path.contains('>') && !path.contains('\0'),
        "YTsaurus table path must not contain rich-path attributes or NUL"
    );
    Ok(())
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

const fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}
