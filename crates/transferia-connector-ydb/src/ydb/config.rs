use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YdbAuth {
    Anonymous,
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },
    TokenFile {
        token_file: String,
    },
}

impl Default for YdbAuth {
    fn default() -> Self {
        Self::Anonymous
    }
}

impl YdbAuth {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Anonymous => Ok(()),
            Self::Token { token } => {
                anyhow::ensure!(!token.is_empty(), "ydb.auth.token must not be empty");
                Ok(())
            }
            Self::TokenFile { token_file } => {
                anyhow::ensure!(
                    !token_file.trim().is_empty(),
                    "ydb.auth.token_file must not be empty"
                );
                Ok(())
            }
        }
    }

    pub async fn resolve(&self) -> anyhow::Result<Option<String>> {
        match self {
            Self::Anonymous => Ok(None),
            Self::Token { token } => Ok(Some(token.clone())),
            Self::TokenFile { token_file } => {
                let path = shellexpand::tilde(token_file).into_owned();
                let token = tokio::fs::read_to_string(&path).await?;
                let token = token.trim().to_owned();
                anyhow::ensure!(!token.is_empty(), "YDB token file '{path}' is empty");
                Ok(Some(token))
            }
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbConnectionConfig {
    pub endpoint: String,

    pub database: String,

    pub trusted_plaintext: bool,

    pub auth: YdbAuth,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub request_timeout_ms: u64,
}

impl YdbConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let endpoint = self.tonic_endpoint()?;
        anyhow::ensure!(
            !self.database.trim().is_empty() && self.database.starts_with('/'),
            "ydb.database must be an absolute non-empty path"
        );
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "ydb.request_timeout_ms must be positive"
        );
        if endpoint.starts_with("http://") {
            anyhow::ensure!(
                self.trusted_plaintext,
                "ydb.trusted_plaintext must be true for a plaintext grpc:// endpoint"
            );
        }
        self.auth.validate()
    }

    pub fn tonic_endpoint(&self) -> anyhow::Result<String> {
        let endpoint = self.endpoint.trim_end_matches('/');
        if let Some(authority) = endpoint.strip_prefix("grpc://") {
            anyhow::ensure!(!authority.is_empty(), "ydb.endpoint has no authority");
            return Ok(format!("http://{authority}"));
        }
        if let Some(authority) = endpoint.strip_prefix("grpcs://") {
            anyhow::ensure!(!authority.is_empty(), "ydb.endpoint has no authority");
            return Ok(format!("https://{authority}"));
        }
        anyhow::bail!("ydb.endpoint must start with grpc:// or grpcs://")
    }

    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

const fn default_request_timeout_ms() -> u64 {
    30_000
}

#[derive(Clone, Deserialize)]
pub struct YdbConnectionCheckConfig {
    #[serde(default)]
    pub endpoint: String,

    #[serde(default)]
    pub database: String,

    #[serde(default)]
    pub trusted_plaintext: bool,

    #[serde(default)]
    pub auth: YdbAuth,

    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl YdbConnectionCheckConfig {
    #[must_use]
    pub fn credentials_complete(&self) -> bool {
        !self.database.trim().is_empty() && self.auth.validate().is_ok()
    }

    #[must_use]
    pub fn connection(&self) -> YdbConnectionConfig {
        YdbConnectionConfig {
            endpoint: self.endpoint.clone(),
            database: self.database.clone(),
            trusted_plaintext: self.trusted_plaintext,
            auth: self.auth.clone(),
            request_timeout_ms: self.request_timeout_ms,
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbTableConfig {
    pub name: String,

    pub path: String,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbSourceConfig {
    #[serde(flatten)]
    pub connection: YdbConnectionConfig,

    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "table" }))]
    pub tables: Vec<YdbTableConfig>,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub batch_rows: usize,
}

impl YdbSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "ydb.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "ydb.batch_rows must be positive");
        let mut paths = std::collections::HashSet::new();
        let mut names = std::collections::HashSet::new();
        for table in &self.tables {
            anyhow::ensure!(
                !table.name.trim().is_empty(),
                "ydb.tables[].name must not be empty"
            );
            anyhow::ensure!(
                names.insert(table.name.as_str()),
                "ydb.tables repeats logical name '{}'",
                table.name
            );
            anyhow::ensure!(
                table.path.starts_with('/') && !table.path.ends_with('/'),
                "YDB table path '{}' must be absolute and must not end with '/'",
                table.path
            );
            anyhow::ensure!(
                paths.insert(table.path.as_str()),
                "ydb.tables repeats path '{}'",
                table.path
            );
        }
        Ok(())
    }
}

const fn default_batch_rows() -> usize {
    65_536
}
