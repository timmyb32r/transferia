use std::collections::HashSet;
use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RestCatalogConfig {
    pub uri: String,

    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    #[serde(default)]
    pub warehouse: Option<String>,

    #[serde(default)]
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
                    "access_key_id",
                    &config.access_key_id.as_ref().map(|_| "[REDACTED]"),
                )
                .field(
                    "secret_access_key",
                    &config.secret_access_key.as_ref().map(|_| "[REDACTED]"),
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
    pub request_timeout_ms: u64,

    #[serde(default)]
    pub region: Option<String>,

    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default)]
    pub access_key_id: Option<String>,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub secret_access_key: Option<String>,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub session_token: Option<String>,

    #[serde(default)]
    pub path_style_access: bool,

    #[serde(default)]
    pub allow_anonymous: bool,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            request_timeout_ms: default_request_timeout_ms(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
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

    pub table: IcebergTableRef,

    pub output_name: String,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSinkTable {
    pub dataset: String,

    #[serde(flatten)]
    pub table: IcebergTableRef,

    #[serde(default)]
    pub create_if_missing: bool,

    #[serde(default)]
    pub location: Option<String>,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSinkConfig {
    pub catalog: RestCatalogConfig,

    #[serde(default)]
    pub storage: OpenDalStorageConfig,

    #[schemars(extend("x-ui" = { "item_label": "Table" }))]
    pub tables: Vec<IcebergSinkTable>,

    #[serde(default = "default_target_file_size")]
    pub target_file_size_bytes: usize,
}

const fn default_target_file_size() -> usize {
    128 * 1024 * 1024
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

impl OpenDalStorageConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::S3(config) => {
                validate_required("storage.bucket", &config.bucket)?;
                anyhow::ensure!(
                    config.request_timeout_ms > 0,
                    "storage.request_timeout_ms must be positive"
                );
                if let Some(endpoint) = &config.endpoint {
                    validate_required("storage.endpoint", endpoint)?;
                    let url = url::Url::parse(endpoint)?;
                    anyhow::ensure!(
                        matches!(url.scheme(), "http" | "https"),
                        "storage.endpoint must use http or https"
                    );
                }
                anyhow::ensure!(
                    config.access_key_id.is_some() == config.secret_access_key.is_some(),
                    "S3 access_key_id and secret_access_key must be configured together"
                );
            }
            Self::Hdfs(config) => {
                validate_required("storage.endpoint", &config.endpoint)?;
                validate_required("storage.authority", &config.authority)?;
                validate_required("storage.root", &config.root)?;
                anyhow::ensure!(
                    config.request_timeout_ms > 0,
                    "storage.request_timeout_ms must be positive"
                );
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
    pub fn validate(&self) -> anyhow::Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.table.validate("table")?;
        validate_required("output_name", &self.output_name)
    }
}

impl IcebergSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        anyhow::ensure!(!self.tables.is_empty(), "tables must not be empty");
        anyhow::ensure!(
            self.target_file_size_bytes > 0,
            "target_file_size_bytes must be positive"
        );
        let mut datasets = HashSet::with_capacity(self.tables.len());
        let mut tables = HashSet::with_capacity(self.tables.len());
        for (index, mapping) in self.tables.iter().enumerate() {
            validate_required(&format!("tables[{index}].dataset"), &mapping.dataset)?;
            mapping.table.validate(&format!("tables[{index}]"))?;
            anyhow::ensure!(
                datasets.insert(mapping.dataset.as_str()),
                "duplicate Iceberg dataset mapping '{}'",
                mapping.dataset
            );
            anyhow::ensure!(
                tables.insert((&mapping.table.namespace, mapping.table.name.as_str())),
                "duplicate Iceberg destination table '{}.{}'",
                mapping.table.namespace.join("."),
                mapping.table.name
            );
            if let Some(location) = &mapping.location {
                validate_required(&format!("tables[{index}].location"), location)?;
                anyhow::ensure!(
                    mapping.create_if_missing,
                    "tables[{index}].location is valid only when create_if_missing is true"
                );
            }
        }
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
