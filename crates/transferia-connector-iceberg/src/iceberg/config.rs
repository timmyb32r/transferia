use std::collections::HashSet;
use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RestCatalogConfig {
    pub uri: String,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub request_timeout_ms: u64,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub warehouse: Option<String>,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub auth: RestCatalogAuth,
}

#[derive(Clone, Default, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RestCatalogAuth {
    #[default]
    None,
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },
    OAuth2 {
        client_id: String,

        #[schemars(extend("x-ui" = { "widget": "password" }))]
        client_secret: String,

        #[serde(default)]
        scope: Option<String>,

        #[serde(default)]
        token_url: Option<String>,
    },
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OpenDalStorageConfig {
    S3(S3StorageConfig),
    Hdfs(HdfsStorageConfig),
}

impl Default for OpenDalStorageConfig {
    fn default() -> Self {
        Self::S3(S3StorageConfig::default())
    }
}

impl fmt::Debug for OpenDalStorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::S3(config) => formatter
                .debug_struct("S3")
                .field("bucket", &config.bucket)
                .field("region", &config.region)
                .field("endpoint", &config.endpoint)
                .field(
                    "credentials",
                    &config.credentials.as_ref().map(|_| "[REDACTED]"),
                )
                .field(
                    "session_token",
                    &config.session_token.as_ref().map(|_| "[REDACTED]"),
                )
                .field("path_style_access", &config.path_style_access)
                .field("allow_anonymous", &config.allow_anonymous)
                .finish(),
            Self::Hdfs(config) => formatter
                .debug_struct("Hdfs")
                .field("endpoint", &config.endpoint)
                .field("authority", &config.authority)
                .field("root", &config.root)
                .field("user", &config.user)
                .finish(),
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3StorageConfig {
    pub bucket: String,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub request_timeout_ms: u64,

    #[serde(default = "default_retry_max_times")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_max_times: usize,

    #[serde(default = "default_retry_initial_delay_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_initial_delay_ms: u64,

    #[serde(default = "default_retry_max_delay_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_max_delay_ms: u64,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub region: Option<String>,

    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default)]
    #[schemars(title = "Authentication")]
    pub credentials: Option<S3StorageCredentials>,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub session_token: Option<String>,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub path_style_access: bool,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub allow_anonymous: bool,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3StorageCredentials {
    #[schemars(title = "Access key ID")]
    pub access_key: String,

    #[schemars(title = "Secret access key", extend("x-ui" = { "widget": "password" }))]
    pub secret_key: String,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            request_timeout_ms: default_request_timeout_ms(),
            retry_max_times: default_retry_max_times(),
            retry_initial_delay_ms: default_retry_initial_delay_ms(),
            retry_max_delay_ms: default_retry_max_delay_ms(),
            region: None,
            endpoint: None,
            credentials: None,
            session_token: None,
            path_style_access: false,
            allow_anonymous: false,
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HdfsStorageConfig {
    /// `WebHDFS` endpoint, for example `https://namenode:9871`.
    pub endpoint: String,

    /// Authority expected in Iceberg `hdfs://authority/path` locations.
    pub authority: String,

    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    #[serde(default = "default_retry_max_times")]
    pub retry_max_times: usize,

    #[serde(default = "default_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u64,

    #[serde(default = "default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,

    #[serde(default = "default_root")]
    pub root: String,

    #[serde(default)]
    pub user: Option<String>,
}

fn default_root() -> String {
    "/".to_owned()
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergTableRef {
    #[schemars(extend("x-ui" = { "item_label": "Namespace level" }))]
    pub namespace: Vec<String>,

    pub name: String,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSourceConfig {
    pub catalog: RestCatalogConfig,

    #[serde(default)]
    pub storage: OpenDalStorageConfig,

    pub namespace: String,

    #[schemars(title = "Table names", extend("x-ui" = { "item_label": "Table name" }))]
    pub table_names: Vec<String>,

    #[serde(default = "default_read_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub read_batch_rows: usize,

    #[serde(default = "default_read_data_file_concurrency")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub read_data_file_concurrency: usize,

    #[serde(default = "default_read_manifest_concurrency")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub read_manifest_concurrency: usize,

    #[serde(default = "default_parquet_metadata_size_hint_bytes")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub parquet_metadata_size_hint_bytes: usize,

    #[serde(default = "default_parquet_range_coalesce_bytes")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub parquet_range_coalesce_bytes: u64,

    #[serde(default = "default_parquet_range_fetch_concurrency")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub parquet_range_fetch_concurrency: usize,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSinkConfig {
    pub catalog: RestCatalogConfig,

    #[serde(default)]
    pub storage: OpenDalStorageConfig,

    #[serde(default = "default_namespace")]
    pub namespace: String,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub create_if_missing: bool,

    #[serde(default = "default_target_file_size")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub target_file_size_bytes: usize,
}

fn default_namespace() -> String {
    "default".to_owned()
}

const fn default_target_file_size() -> usize {
    128 * 1024 * 1024
}

const fn default_read_batch_rows() -> usize {
    64 * 1024
}

const fn default_read_data_file_concurrency() -> usize {
    32
}

const fn default_read_manifest_concurrency() -> usize {
    32
}

const fn default_parquet_metadata_size_hint_bytes() -> usize {
    512 * 1024
}

const fn default_parquet_range_coalesce_bytes() -> u64 {
    1024 * 1024
}

const fn default_parquet_range_fetch_concurrency() -> usize {
    10
}

impl RestCatalogConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_required("catalog.uri", &self.uri)?;
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "catalog.request_timeout_ms must be positive"
        );
        let uri = url::Url::parse(&self.uri)?;
        anyhow::ensure!(
            matches!(uri.scheme(), "http" | "https"),
            "catalog.uri must use http or https"
        );
        anyhow::ensure!(
            uri.username().is_empty() && uri.password().is_none(),
            "catalog.uri must not embed credentials"
        );
        if let Some(warehouse) = &self.warehouse {
            validate_required("catalog.warehouse", warehouse)?;
        }
        match &self.auth {
            RestCatalogAuth::None => {}
            RestCatalogAuth::Token { token } => validate_required("catalog.auth.token", token)?,
            RestCatalogAuth::OAuth2 {
                client_id,
                client_secret,
                scope,
                token_url,
            } => {
                validate_required("catalog.auth.client_id", client_id)?;
                anyhow::ensure!(
                    !client_id.contains(':'),
                    "catalog.auth.client_id must not contain ':'"
                );
                validate_required("catalog.auth.client_secret", client_secret)?;
                if let Some(scope) = scope {
                    validate_required("catalog.auth.scope", scope)?;
                }
                if let Some(token_url) = token_url {
                    validate_required("catalog.auth.token_url", token_url)?;
                    let url = url::Url::parse(token_url)?;
                    anyhow::ensure!(
                        matches!(url.scheme(), "http" | "https"),
                        "catalog.auth.token_url must use http or https"
                    );
                }
            }
        }
        Ok(())
    }
}

const fn default_request_timeout_ms() -> u64 {
    30_000
}

const fn default_retry_max_times() -> usize {
    12
}

const fn default_retry_initial_delay_ms() -> u64 {
    100
}

const fn default_retry_max_delay_ms() -> u64 {
    5_000
}

impl OpenDalStorageConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::S3(config) => {
                validate_required("storage.bucket", &config.bucket)?;
                anyhow::ensure!(
                    config.request_timeout_ms > 0,
                    "storage.request_timeout_ms must be positive"
                );
                validate_retry_policy(
                    config.retry_max_times,
                    config.retry_initial_delay_ms,
                    config.retry_max_delay_ms,
                )?;
                if let Some(endpoint) = &config.endpoint {
                    validate_required("storage.endpoint", endpoint)?;
                    let url = url::Url::parse(endpoint)?;
                    anyhow::ensure!(
                        matches!(url.scheme(), "http" | "https"),
                        "storage.endpoint must use http or https"
                    );
                }
                if let Some(credentials) = &config.credentials {
                    validate_required("storage.credentials.access_key", &credentials.access_key)?;
                    validate_required("storage.credentials.secret_key", &credentials.secret_key)?;
                }
            }
            Self::Hdfs(config) => {
                validate_required("storage.endpoint", &config.endpoint)?;
                validate_required("storage.authority", &config.authority)?;
                validate_required("storage.root", &config.root)?;
                anyhow::ensure!(
                    config.request_timeout_ms > 0,
                    "storage.request_timeout_ms must be positive"
                );
                validate_retry_policy(
                    config.retry_max_times,
                    config.retry_initial_delay_ms,
                    config.retry_max_delay_ms,
                )?;
                let url = url::Url::parse(&config.endpoint)?;
                anyhow::ensure!(
                    matches!(url.scheme(), "http" | "https"),
                    "HDFS endpoint must use http or https"
                );
                anyhow::ensure!(config.root.starts_with('/'), "HDFS root must be absolute");
            }
        }
        Ok(())
    }
}

fn validate_retry_policy(
    max_times: usize,
    initial_delay_ms: u64,
    max_delay_ms: u64,
) -> anyhow::Result<()> {
    anyhow::ensure!(max_times > 0, "storage.retry_max_times must be positive");
    anyhow::ensure!(
        initial_delay_ms > 0,
        "storage.retry_initial_delay_ms must be positive"
    );
    anyhow::ensure!(
        max_delay_ms >= initial_delay_ms,
        "storage.retry_max_delay_ms must be at least storage.retry_initial_delay_ms"
    );
    Ok(())
}

impl IcebergTableRef {
    pub fn validate(&self, path: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.namespace.is_empty(),
            "{path}.namespace must not be empty"
        );
        for (index, level) in self.namespace.iter().enumerate() {
            validate_required(&format!("{path}.namespace[{index}]"), level)?;
        }
        validate_required(&format!("{path}.name"), &self.name)
    }
}

impl IcebergSourceConfig {
    pub(crate) fn table_ref(&self, table_name: &str) -> IcebergTableRef {
        IcebergTableRef {
            namespace: vec![self.namespace.clone()],
            name: table_name.to_owned(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        validate_required("namespace", &self.namespace)?;
        anyhow::ensure!(
            !self.table_names.is_empty(),
            "table_names must not be empty"
        );
        anyhow::ensure!(self.read_batch_rows > 0, "read_batch_rows must be positive");
        anyhow::ensure!(
            self.read_data_file_concurrency > 0,
            "read_data_file_concurrency must be positive"
        );
        anyhow::ensure!(
            self.read_manifest_concurrency > 0,
            "read_manifest_concurrency must be positive"
        );
        anyhow::ensure!(
            self.parquet_metadata_size_hint_bytes > 0,
            "parquet_metadata_size_hint_bytes must be positive"
        );
        anyhow::ensure!(
            self.parquet_range_coalesce_bytes > 0,
            "parquet_range_coalesce_bytes must be positive"
        );
        anyhow::ensure!(
            self.parquet_range_fetch_concurrency > 0,
            "parquet_range_fetch_concurrency must be positive"
        );
        let mut unique = HashSet::with_capacity(self.table_names.len());
        for (index, table_name) in self.table_names.iter().enumerate() {
            validate_required(&format!("table_names[{index}]"), table_name)?;
            anyhow::ensure!(
                unique.insert(table_name.as_str()),
                "duplicate Iceberg table name '{table_name}'"
            );
        }
        Ok(())
    }
}

impl IcebergSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        anyhow::ensure!(
            self.target_file_size_bytes > 0,
            "target_file_size_bytes must be positive"
        );
        validate_required("namespace", &self.namespace)?;
        Ok(())
    }
}

fn validate_required(path: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.is_empty(), "{path} must not be empty");
    anyhow::ensure!(
        value.trim() == value,
        "{path} must not contain leading or trailing whitespace"
    );
    Ok(())
}
