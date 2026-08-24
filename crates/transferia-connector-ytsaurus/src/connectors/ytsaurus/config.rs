use std::time::Duration;
use std::{collections::HashSet, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_BATCH_ROWS: usize = 65_536;
const DEFAULT_TABLE_READER_WINDOW_SIZE: u64 = 20 * 1024 * 1024;
const DEFAULT_TABLE_READER_GROUP_SIZE: u64 = 15 * 1024 * 1024;
const DEFAULT_TABLE_READER_MAX_BUFFER_SIZE: u64 = 100 * 1024 * 1024;
const DEFAULT_STREAM_RETRY_MAX_ATTEMPTS: usize = 12;
const DEFAULT_STREAM_RETRY_INITIAL_MS: u64 = 100;
const DEFAULT_STREAM_RETRY_MAX_MS: u64 = 5_000;
const DEFAULT_STREAM_OPEN_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS: u64 = 30_000;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusConnectionConfig {
    #[schemars(extend("x-ui" = { "control_width": "auth" }))]
    pub auth: YTsaurusAuthConfig,

    pub host: String,

    pub port: u16,

    pub trusted_plaintext: bool,

    #[serde(default = "default_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub timeout_ms: u64,
}

impl YTsaurusConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        crate::connectors::address::validate_host("ytsaurus.host", &self.host)?;
        crate::connectors::address::validate_port("ytsaurus.port", self.port)?;
        anyhow::ensure!(self.timeout_ms > 0, "ytsaurus.timeout_ms must be positive");
        self.auth.validate()?;
        Ok(())
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub(crate) fn endpoint(&self) -> String {
        crate::connectors::address::url(
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
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YTsaurusAuthConfig {
    #[schemars(title = "Token")]
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },

    #[schemars(title = "Token file")]
    TokenFile { token_file: String },
}

impl YTsaurusAuthConfig {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Token { token } => {
                anyhow::ensure!(
                    !token.trim().is_empty(),
                    "ytsaurus.auth.token must not be empty"
                );
            }
            Self::TokenFile { token_file } => {
                anyhow::ensure!(
                    !token_file.trim().is_empty(),
                    "ytsaurus.auth.token_file must not be empty"
                );
            }
        }
        Ok(())
    }

    pub(crate) fn load_token(&self) -> anyhow::Result<String> {
        self.validate()?;
        let token = match self {
            Self::Token { token } => token.clone(),
            Self::TokenFile { token_file } => {
                let expanded = shellexpand::full(token_file)?;
                std::fs::read_to_string(expanded.as_ref()).map_err(|error| {
                    anyhow::anyhow!("failed to read YTsaurus token file '{expanded}': {error}")
                })?
            }
        };
        let token = token.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "YTsaurus token is empty");
        Ok(token)
    }
}

impl fmt::Debug for YTsaurusAuthConfig {
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

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusSourceConfig {
    #[serde(flatten)]
    pub connection: YTsaurusConnectionConfig,

    pub tables: Vec<SourceTableConfig>,

    /// Explicit benchmark-only mode that counts wire rows without materializing
    /// their values as Arrow arrays. Delivery semantics only permit this mode
    /// with the discard destination.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub benchmark_discard: Option<YTsaurusBenchmarkDiscardConfig>,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub batch_rows: usize,

    #[serde(default = "default_stream_retry_max_attempts")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_retry_max_attempts: usize,

    #[serde(default = "default_stream_retry_initial_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_retry_initial_ms: u64,

    #[serde(default = "default_stream_retry_max_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_retry_max_ms: u64,

    #[serde(default = "default_stream_open_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_open_timeout_ms: u64,

    #[serde(default = "default_stream_idle_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_idle_timeout_ms: u64,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusBenchmarkDiscardConfig {
    pub format: YTsaurusReadFormat,

    #[serde(default)]
    pub unordered: bool,

    #[serde(default)]
    pub table_reader: YTsaurusTableReaderConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusReadFormat {
    Arrow,
    Skiff,
    SchemafulDsv,
    YsonBinary,
    YsonText,
    Json,
}

impl YTsaurusReadFormat {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Arrow => "arrow",
            Self::Skiff => "skiff",
            Self::SchemafulDsv => "schemaful_dsv",
            Self::YsonBinary => "yson_binary",
            Self::YsonText => "yson_text",
            Self::Json => "json",
        }
    }
}

#[derive(Clone, Default, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusTableReaderConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_buffer_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_parallel_readers: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_uncompressed_block_cache: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_out_of_order_blocks: Option<bool>,
}

impl YTsaurusTableReaderConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        let window_size = self.window_size.unwrap_or(DEFAULT_TABLE_READER_WINDOW_SIZE);
        let group_size = self.group_size.unwrap_or(DEFAULT_TABLE_READER_GROUP_SIZE);
        let max_buffer_size = self
            .max_buffer_size
            .unwrap_or(DEFAULT_TABLE_READER_MAX_BUFFER_SIZE);
        anyhow::ensure!(window_size > 0, "ytsaurus.table_reader.window_size must be positive");
        anyhow::ensure!(group_size > 0, "ytsaurus.table_reader.group_size must be positive");
        anyhow::ensure!(
            group_size <= window_size,
            "ytsaurus.table_reader.group_size must not exceed window_size"
        );
        anyhow::ensure!(
            max_buffer_size > 0,
            "ytsaurus.table_reader.max_buffer_size must be positive"
        );
        anyhow::ensure!(
            max_buffer_size <= 10 * 1024 * 1024 * 1024,
            "ytsaurus.table_reader.max_buffer_size must not exceed 10 GiB"
        );
        anyhow::ensure!(
            max_buffer_size >= window_size.saturating_mul(2),
            "ytsaurus.table_reader.max_buffer_size must be at least twice window_size"
        );
        if let Some(max_parallel_readers) = self.max_parallel_readers {
            anyhow::ensure!(
                (1..=1000).contains(&max_parallel_readers),
                "ytsaurus.table_reader.max_parallel_readers must be between 1 and 1000"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceTableConfig {
    #[schemars(
        title = "Table name",
        description = "Logical dataset name emitted to the destination"
    )]
    pub name: String,

    pub path: String,
}

impl YTsaurusSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "ytsaurus.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "ytsaurus.batch_rows must be positive");
        anyhow::ensure!(
            self.stream_retry_max_attempts > 0,
            "ytsaurus.stream_retry_max_attempts must be positive"
        );
        anyhow::ensure!(
            self.stream_retry_initial_ms > 0,
            "ytsaurus.stream_retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.stream_retry_max_ms >= self.stream_retry_initial_ms,
            "ytsaurus.stream_retry_max_ms must be at least stream_retry_initial_ms"
        );
        anyhow::ensure!(
            self.stream_open_timeout_ms > 0,
            "ytsaurus.stream_open_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.stream_idle_timeout_ms > 0,
            "ytsaurus.stream_idle_timeout_ms must be positive"
        );
        if let Some(benchmark_discard) = &self.benchmark_discard {
            benchmark_discard.table_reader.validate()?;
        }
        let mut paths = HashSet::new();
        let mut names = HashSet::new();
        for table in &self.tables {
            validate_path(&table.path)?;
            anyhow::ensure!(
                !table.name.trim().is_empty(),
                "ytsaurus.tables.name must not be empty"
            );
            anyhow::ensure!(
                table.name == table.name.trim(),
                "ytsaurus.tables.name must not have leading or trailing whitespace"
            );
            anyhow::ensure!(
                paths.insert(table.path.as_str()),
                "ytsaurus.tables repeats path '{}'",
                table.path
            );
            anyhow::ensure!(
                names.insert(table.name.as_str()),
                "ytsaurus.tables repeats logical table name '{}'",
                table.name
            );
        }
        Ok(())
    }
}

const fn default_stream_retry_max_attempts() -> usize {
    DEFAULT_STREAM_RETRY_MAX_ATTEMPTS
}

const fn default_stream_retry_initial_ms() -> u64 {
    DEFAULT_STREAM_RETRY_INITIAL_MS
}

const fn default_stream_retry_max_ms() -> u64 {
    DEFAULT_STREAM_RETRY_MAX_MS
}

const fn default_stream_open_timeout_ms() -> u64 {
    DEFAULT_STREAM_OPEN_TIMEOUT_MS
}

const fn default_stream_idle_timeout_ms() -> u64 {
    DEFAULT_STREAM_IDLE_TIMEOUT_MS
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
    #[schemars(
        title = "Table type",
        extend("x-ui" = {
            "control_width": "full",
            "defer_variant_details": true,
            "order": -100,
            "reveal_rest_on_selection": true
        })
    )]
    pub tables: YTsaurusTableMode,

    #[serde(flatten)]
    pub connection: YTsaurusConnectionConfig,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YTsaurusTableMode {
    #[schemars(title = "Static tables")]
    StaticTables {
        #[schemars(
            title = "Path",
            description = "Directory where dataset tables are stored"
        )]
        path: String,

        replace_tables: bool,

        #[serde(default)]
        #[schemars(
            title = "Driver exchange format",
            extend("x-ui" = { "section": "advanced" })
        )]
        format: YTsaurusWriteFormat,
    },

    #[schemars(title = "Dynamic tables")]
    DynamicTables {
        #[schemars(
            title = "Path",
            description = "Directory where dataset tables are stored"
        )]
        path: String,

        replace_tables: bool,

        #[serde(default)]
        #[schemars(
            title = "Driver exchange format",
            extend("x-ui" = { "section": "advanced" })
        )]
        format: YTsaurusWriteFormat,
    },
}

impl YTsaurusSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        validate_path(self.path())?;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &str {
        match &self.tables {
            YTsaurusTableMode::StaticTables { path, .. }
            | YTsaurusTableMode::DynamicTables { path, .. } => path,
        }
    }

    #[must_use]
    pub const fn replace_tables(&self) -> bool {
        match &self.tables {
            YTsaurusTableMode::StaticTables { replace_tables, .. }
            | YTsaurusTableMode::DynamicTables { replace_tables, .. } => *replace_tables,
        }
    }

    #[must_use]
    pub const fn format(&self) -> YTsaurusWriteFormat {
        match &self.tables {
            YTsaurusTableMode::StaticTables { format, .. }
            | YTsaurusTableMode::DynamicTables { format, .. } => *format,
        }
    }

    pub fn path_for_dataset(&self, dataset: &str) -> anyhow::Result<String> {
        anyhow::ensure!(
            !dataset.is_empty(),
            "YTsaurus dataset name must not be empty"
        );
        anyhow::ensure!(
            !dataset.contains('/')
                && !dataset.contains('<')
                && !dataset.contains('>')
                && !dataset.contains('\0'),
            "YTsaurus dataset name '{dataset}' cannot be used as one table path segment"
        );
        Ok(format!("{}/{dataset}", self.path().trim_end_matches('/')))
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
