use std::time::Duration;
use std::{collections::HashSet, fmt};

use schemars::JsonSchema;
use serde::Deserialize;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_BATCH_ROWS: usize = 65_536;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusConnectionConfig {
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

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub batch_rows: usize,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceTableConfig {
    pub path: String,
}

impl YTsaurusSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "ytsaurus.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "ytsaurus.batch_rows must be positive");
        let mut paths = HashSet::new();
        for table in &self.tables {
            validate_path(&table.path)?;
            anyhow::ensure!(
                paths.insert(table.path.as_str()),
                "ytsaurus.tables repeats path '{}'",
                table.path
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
    #[schemars(title = "Table type", extend("x-ui" = { "order": -100 }))]
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
