use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;

use super::src_stream::YdbReplicationConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YdbAuth {
    #[schemars(title = "Token")]
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },

    #[schemars(title = "Token file")]
    TokenFile { token_file: String },

    #[schemars(title = "Anonymous")]
    Anonymous,
}

impl Default for YdbAuth {
    fn default() -> Self {
        Self::Token {
            token: String::new(),
        }
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

    #[schemars(extend("x-ui" = { "control_width": "auth" }))]
    pub auth: YdbAuth,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub request_timeout_ms: u64,

    #[serde(default = "default_max_rpc_message_bytes")]
    #[schemars(
        range(min = 1, max = 4_294_967_295_u64),
        title = "Maximum RPC message bytes",
        description = "Maximum encoded YDB gRPC request or response accepted by setup, discovery, snapshot, and destination operations",
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub max_rpc_message_bytes: usize,
}

impl YdbConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let endpoint = self.tonic_endpoint()?;
        validate_absolute_ydb_path("ydb.database", &self.database)?;
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "ydb.request_timeout_ms must be positive"
        );
        anyhow::ensure!(
            (1..=u32::MAX as usize).contains(&self.max_rpc_message_bytes),
            "ydb.max_rpc_message_bytes must be in 1..={}",
            u32::MAX
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
        let (scheme, authority) = if let Some(authority) = self.endpoint.strip_prefix("grpc://") {
            ("http", authority)
        } else if let Some(authority) = self.endpoint.strip_prefix("grpcs://") {
            ("https", authority)
        } else {
            anyhow::bail!("ydb.endpoint must start with grpc:// or grpcs://");
        };
        anyhow::ensure!(
            !authority.is_empty()
                && !authority
                    .bytes()
                    .any(|byte| matches!(byte, b'@' | b'/' | b'?' | b'#')),
            "ydb.endpoint must contain only a bare host[:port] authority without userinfo, path, query, or fragment"
        );
        let authority = authority
            .parse::<http::uri::Authority>()
            .map_err(|_| anyhow::anyhow!("ydb.endpoint contains an invalid authority"))?;
        anyhow::ensure!(
            !authority.host().is_empty(),
            "ydb.endpoint has no host authority"
        );
        Ok(format!("{scheme}://{authority}"))
    }

    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

const fn default_request_timeout_ms() -> u64 {
    30_000
}

const fn default_max_rpc_message_bytes() -> usize {
    256 * 1024 * 1024
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

    #[serde(default = "default_max_rpc_message_bytes")]
    pub max_rpc_message_bytes: usize,
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
            max_rpc_message_bytes: self.max_rpc_message_bytes,
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbTableConfig {
    pub path: String,
}

impl YdbTableConfig {
    #[must_use]
    pub fn name(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or_default()
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "snapshot", "delivery_modes": ["batch"], "record_semantics": ["append_only"] } }))]
pub struct YdbSourceConfig {
    #[serde(flatten)]
    pub connection: YdbConnectionConfig,

    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "table" }))]
    pub tables: Vec<YdbTableConfig>,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub batch_rows: usize,

    #[serde(default = "default_session_shutdown_timeout_ms")]
    #[schemars(
        title = "Session shutdown timeout, ms",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub session_shutdown_timeout_ms: u64,

    #[serde(default = "default_session_shutdown_retry_initial_ms")]
    #[schemars(
        title = "Session shutdown retry backoff, ms",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub session_shutdown_retry_initial_ms: u64,

    /// Configures ordinary YDB table replication from pre-created changefeeds.
    #[serde(default)]
    pub replication: Option<YdbReplicationConfig>,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YdbSinkConfig {
    #[serde(flatten)]
    pub connection: YdbConnectionConfig,

    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "table" }))]
    pub tables: Vec<YdbTableConfig>,

    #[serde(default = "default_create_tables")]
    pub create_tables: bool,

    #[serde(default = "default_retry_max_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_max_ms: u64,
}

impl YdbSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(self.retry_max_ms > 0, "ydb.retry_max_ms must be positive");
        validate_tables(&self.tables)
    }

    pub fn table_path(&self, name: &str) -> anyhow::Result<&str> {
        self.tables
            .iter()
            .find(|table| table.name() == name)
            .map(|table| table.path.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("YDB sink has no physical table mapping for dataset '{name}'")
            })
    }
}

impl YdbSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        anyhow::ensure!(self.batch_rows > 0, "ydb.batch_rows must be positive");
        anyhow::ensure!(
            self.session_shutdown_timeout_ms > 0,
            "ydb.session_shutdown_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.session_shutdown_retry_initial_ms > 0,
            "ydb.session_shutdown_retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.session_shutdown_retry_initial_ms <= self.session_shutdown_timeout_ms,
            "ydb.session_shutdown_retry_initial_ms must not exceed session_shutdown_timeout_ms"
        );
        anyhow::ensure!(
            std::time::Instant::now()
                .checked_add(self.session_shutdown_timeout())
                .is_some(),
            "ydb.session_shutdown_timeout_ms exceeds the platform clock range"
        );
        if let Some(replication) = &self.replication {
            replication.validate()?;
        }
        validate_tables(&self.tables)
    }

    #[must_use]
    pub const fn session_shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.session_shutdown_timeout_ms)
    }

    #[must_use]
    pub const fn session_shutdown_retry_initial(&self) -> Duration {
        Duration::from_millis(self.session_shutdown_retry_initial_ms)
    }
}

fn validate_tables(tables: &[YdbTableConfig]) -> anyhow::Result<()> {
    anyhow::ensure!(!tables.is_empty(), "ydb.tables must not be empty");
    let mut paths = std::collections::HashSet::new();
    let mut names = std::collections::HashSet::new();
    for table in tables {
        validate_absolute_ydb_path("ydb.tables[].path", &table.path)?;
        let name = table.name();
        anyhow::ensure!(
            !name.is_empty(),
            "YDB table path '{}' has no table name",
            table.path
        );
        anyhow::ensure!(
            names.insert(name),
            "ydb.tables repeats logical name '{name}'"
        );
        anyhow::ensure!(
            paths.insert(table.path.as_str()),
            "ydb.tables repeats path '{}'",
            table.path
        );
    }
    Ok(())
}

pub(super) fn validate_absolute_ydb_path(field: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.starts_with('/')
            && value != "/"
            && value.trim() == value
            && !value.ends_with('/')
            && value
                .split('/')
                .skip(1)
                .all(|segment| !segment.is_empty() && segment != "." && segment != ".."),
        "{field} must be a canonical absolute non-root YDB path without surrounding whitespace, empty, '.', or '..' segments"
    );
    Ok(())
}

const fn default_create_tables() -> bool {
    true
}

const fn default_retry_max_ms() -> u64 {
    30_000
}

const fn default_batch_rows() -> usize {
    65_536
}

const fn default_session_shutdown_timeout_ms() -> u64 {
    60_000
}

const fn default_session_shutdown_retry_initial_ms() -> u64 {
    50
}
