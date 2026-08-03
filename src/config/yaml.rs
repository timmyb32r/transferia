use arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;

/// YDB cluster database used for discovery/routing metadata (`x-ydb-database`).
/// Always `/Root` in our deployment — hardcoded rather than configured.
pub const YDB_DATABASE: &str = "/Root";

/// Top-level configuration for the replicator.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub middlewares: Vec<MiddlewareConfig>,
}

impl Config {
    /// Load configuration from a YAML file, expanding ${VAR} and $VAR patterns.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))?;
        // Expand ${VAR} and $VAR environment variables in the YAML text
        let expanded = shellexpand::env(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to expand env vars in config: {}", e))?;
        let config: Self = serde_yaml::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("Failed to parse YAML config: {}", e))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        let parser = &self.source.parser;
        if parser.parser_type != "json_parser" {
            anyhow::bail!("source.parser.parser_type '{}' unsupported (only 'json_parser')", parser.parser_type);
        }
        if parser.settings.columns.is_empty() {
            anyhow::bail!("source.parser.settings.columns must not be empty");
        }
        // Validate table naming (also surfaces a missing name for from_config early).
        parser.resolve_table_name(&self.source.topic_path)?;
        if self.source.connection_string.is_empty() {
            anyhow::bail!("source.connection_string must not be empty");
        }
        if self.source.topic_path.is_empty() {
            anyhow::bail!("source.topic_path must not be empty");
        }
        if self.source.consumer_name.is_empty() {
            anyhow::bail!("source.consumer_name must not be empty");
        }
        if self.sink.connection_string.is_empty() {
            anyhow::bail!("sink.connection_string must not be empty");
        }
        if self.sink.database.is_empty() {
            anyhow::bail!("sink.database must not be empty");
        }
        // Validate arrow types for all column mappings
        for col in &parser.settings.columns {
            parse_arrow_type(&col.arrow_type)
                .map_err(|e| anyhow::anyhow!("Column '{}' has invalid arrow_type: {}", col.column_name, e))?;
        }
        // Validate middleware configuration
        for (i, mw) in self.middlewares.iter().enumerate() {
            match mw.mw_type.as_str() {
                "filter" => {
                    if mw.field.as_ref().is_none_or(|f| f.is_empty()) {
                        anyhow::bail!("middlewares[{}]: filter requires non-empty 'field'", i);
                    }
                    if mw.value.as_ref().is_none_or(|v| v.is_empty()) {
                        anyhow::bail!("middlewares[{}]: filter requires non-empty 'value'", i);
                    }
                    let col = parser.settings.columns.iter()
                        .find(|c| c.column_name == mw.field.as_deref().unwrap_or(""))
                        .ok_or_else(|| anyhow::anyhow!(
                            "middlewares[{}]: filter field '{}' not found in parser columns",
                            i, mw.field.as_deref().unwrap_or("")
                        ))?;
                    let dt = parse_arrow_type(&col.arrow_type)?;
                    if dt != DataType::Utf8 && dt != DataType::LargeUtf8 {
                        anyhow::bail!(
                            "middlewares[{}]: filter field '{}' is {:?}, only Utf8/LargeUtf8 supported",
                            i, col.column_name, dt
                        );
                    }
                }
                other => anyhow::bail!("middlewares[{}]: unknown middleware type '{}'", i, other),
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Source configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    /// Source type: "topic" (default, YDB Topic API via ydb crate) or
    /// "pqv1" (Logbroker PQv1 — MigrationStreamingRead gRPC).
    #[serde(default = "default_source_type")]
    pub source_type: String,
    /// YDB connection string, e.g. "grpc://localhost:2136/local"
    pub connection_string: String,
    /// YDB topic path, e.g. "/local/test-topic"
    pub topic_path: String,
    /// YDB consumer name
    pub consumer_name: String,
    #[serde(default)]
    pub auth: AuthConfig,
    /// Parser configuration: table naming + concrete parser settings.
    pub parser: ParserConfig,
    /// Optional: static discovery endpoint URI (e.g. "grpcs://sas.logbroker.yandex.net:2135").
    /// When set, bypasses YDB's normal endpoint discovery and routes all gRPC services
    /// through this single endpoint — needed for Logbroker-backed YDB topics.
    #[serde(default)]
    pub discovery_endpoint: Option<String>,
    /// Optional: static partition IDs for PQv1 (fallback when DescribeTopic unavailable).
    /// When set AND source_type == "pqv1", partition discovery is skipped entirely.
    #[serde(default)]
    pub partition_ids: Option<Vec<i64>>,
}

fn default_source_type() -> String {
    "topic".to_string()
}

// ---------------------------------------------------------------------------
// Parser configuration (table naming + concrete parser settings)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ParserConfig {
    /// How the destination table name is chosen.
    pub table_naming: TableNaming,
    /// Concrete parser kind (currently only "json_parser").
    pub parser_type: String,
    /// Parser-specific settings (for json_parser: column mappings).
    pub settings: SchemaConfig,
}

#[derive(Debug, Deserialize)]
pub struct TableNaming {
    /// "from_config" — use `name`; "from_topic" — use the topic path verbatim.
    #[serde(rename = "type")]
    pub kind: String,
    /// Explicit table name; required when `kind == "from_config"`.
    #[serde(default)]
    pub name: Option<String>,
}

impl ParserConfig {
    /// Resolve the destination table name from the naming strategy.
    /// `from_config` → configured `name`; `from_topic` → `topic_path` as-is.
    pub fn resolve_table_name(&self, topic_path: &str) -> anyhow::Result<String> {
        match self.table_naming.kind.as_str() {
            "from_config" => self.table_naming.name.clone().filter(|n| !n.is_empty())
                .ok_or_else(|| anyhow::anyhow!("table_naming.name is required for type 'from_config'")),
            "from_topic" => Ok(topic_path.to_string()),
            other => anyhow::bail!("unknown table_naming.type '{}' (use from_config | from_topic)", other),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[allow(dead_code)]
pub struct AuthConfig {
    /// Auth type: "anonymous" (default), "access_token", "service_account"
    #[serde(rename = "type")]
    pub auth_type: String,
    /// Token string for access_token auth (plain text)
    pub token: Option<String>,
    /// Path to file containing the token for access_token auth.
    /// The file is read at startup and its trimmed contents used as the token.
    /// Supports ~ for home directory expansion.
    pub token_file: Option<String>,
    /// Path to service account JSON key file (for service_account auth)
    pub sa_file: Option<String>,
}

// ---------------------------------------------------------------------------
// Schema / column mapping configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SchemaConfig {
    pub columns: Vec<ColumnMapping>,
    /// Optional: custom field name for the raw JSON payload (for DLQ)
    #[serde(default)]
    pub raw_payload_field: Option<String>,
    /// Optional: column names for the ClickHouse `ORDER BY` clause.
    /// When empty/omitted, defaults to `ORDER BY tuple()`.
    #[serde(default)]
    pub order_by: Vec<String>,
    /// How to split incoming message bytes into individual JSON objects.
    /// - `no-split` (default): each message is one JSON
    /// - `new-line`:  split by `\n`, each non-empty line is one JSON
    #[serde(default)]
    pub chunk_splitter: ChunkSplitter,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChunkSplitter {
    #[default]
    NoSplit,
    NewLine,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct ColumnMapping {
    /// JSONPath expression, e.g. "$.payload.user.id"
    pub jsonpath: String,
    /// Arrow/ClickHouse column name
    pub column_name: String,
    /// Arrow type string: "Utf8", "Int64", "Float64", "Timestamp(Millisecond, None)", etc.
    pub arrow_type: String,
    /// Whether the column is nullable
    #[serde(default)]
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// Middleware configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct MiddlewareConfig {
    #[serde(rename = "type")]
    pub mw_type: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
}

// ---------------------------------------------------------------------------
// Sink configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SinkConfig {
    /// ClickHouse connection string (Native Protocol port), e.g. "localhost:9000"
    /// For Yandex Cloud Managed ClickHouse, use port 9440 (native TLS), NOT 8443 (HTTPS).
    pub connection_string: String,
    /// ClickHouse database name
    pub database: String,
    /// Rows per batch insert (default: 10000)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Max milliseconds to wait before flushing a partial batch (default: 500)
    #[serde(default = "default_max_linger_ms")]
    pub max_linger_ms: u64,
    /// Max ClickHouse connections in the pool (default: 4)
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// ClickHouse username (default: "default")
    #[serde(default = "default_username")]
    pub username: String,
    /// ClickHouse password (default: empty)
    #[serde(default)]
    pub password: String,
    /// Enable TLS for ClickHouse native protocol connections (default: true).
    /// Set to false if connecting without encryption (e.g. local dev or same-VPC).
    #[serde(default = "default_use_tls")]
    pub use_tls: bool,
    /// Override the SNI/TLS domain name for certificate validation.
    /// If unset, the host from `connection_string` is used.
    /// Yandex Cloud users: this should match your cluster's FQDN.
    #[serde(default)]
    pub tls_domain: Option<String>,
    /// Opt-in for dev/bench: DROP + recreate tables on startup so schema changes
    /// (e.g. a column becoming Nullable, or a new ORDER BY) take effect.
    /// NEVER enable in production — existing data IS LOST.
    #[serde(default)]
    pub recreate_tables: bool,
}

fn default_batch_size() -> usize {
    10000
}

fn default_max_linger_ms() -> u64 {
    500
}

fn default_max_connections() -> usize {
    4
}

fn default_username() -> String {
    "default".to_string()
}

fn default_use_tls() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Arrow type parsing
// ---------------------------------------------------------------------------

/// Parse a human-readable Arrow type string into a `DataType`.
///
/// Supported formats:
/// - `"Utf8"`, `"String"`
/// - `"Int64"`, `"int64"`, `"Int32"`, `"int32"`
/// - `"Float64"`, `"float64"`, `"Float32"`, `"float32"`
/// - `"Boolean"`, `"bool"`
/// - `"Date32"`, `"Date64"`
/// - `"Timestamp(unit, tz)"` where unit is `Second|Millisecond|Microsecond|Nanosecond`
///   and tz is a timezone string or `None`.
pub fn parse_arrow_type(s: &str) -> anyhow::Result<DataType> {
    match s {
        "Utf8" | "String" => Ok(DataType::Utf8),
        "LargeUtf8" | "LargeString" => Ok(DataType::LargeUtf8),
        "Int64" | "int64" => Ok(DataType::Int64),
        "Int32" | "int32" => Ok(DataType::Int32),
        "Int16" | "int16" => Ok(DataType::Int16),
        "Int8" | "int8" => Ok(DataType::Int8),
        "UInt64" | "uint64" => Ok(DataType::UInt64),
        "UInt32" | "uint32" => Ok(DataType::UInt32),
        "UInt16" | "uint16" => Ok(DataType::UInt16),
        "UInt8" | "uint8" => Ok(DataType::UInt8),
        "Float64" | "float64" => Ok(DataType::Float64),
        "Float32" | "float32" => Ok(DataType::Float32),
        "Boolean" | "bool" | "Bool" => Ok(DataType::Boolean),
        "Date32" => Ok(DataType::Date32),
        "Date64" => Ok(DataType::Date64),
        _ if s.starts_with("Timestamp(") => {
            let inner = s
                .trim_start_matches("Timestamp(")
                .trim_end_matches(')');
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            let unit = match parts.first().copied().unwrap_or("Microsecond") {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                other => anyhow::bail!(
                    "Unsupported Timestamp unit '{}'. Use Second, Millisecond, Microsecond, or Nanosecond.",
                    other
                ),
            };
            let tz: Option<String> = if parts.len() > 1 && parts[1] != "None" {
                Some(parts[1].to_string())
            } else {
                None
            };
            Ok(DataType::Timestamp(unit, tz.map(Into::into)))
        }
        other => anyhow::bail!(
            "Unsupported arrow_type: '{}'. Supported: Utf8, String, LargeUtf8, Int64, Int32, Int16, Int8, UInt64, UInt32, UInt16, UInt8, Float64, Float32, Boolean, Date32, Date64, Timestamp(unit, tz)",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// YDB credentials builder
// ---------------------------------------------------------------------------

/// Unified YDB credentials enum — wraps all supported credential types.
pub enum YdbCredentials {
    Anonymous(ydb::AnonymousCredentials),
    AccessToken(ydb::AccessTokenCredentials),
    ServiceAccount(ydb::ServiceAccountCredentials),
}

impl ydb::Credentials for YdbCredentials {
    fn create_token(&self) -> ydb::YdbResult<ydb::TokenInfo> {
        match self {
            YdbCredentials::Anonymous(c) => c.create_token(),
            YdbCredentials::AccessToken(c) => c.create_token(),
            YdbCredentials::ServiceAccount(c) => c.create_token(),
        }
    }
}

/// Build YDB credentials from the auth config section.
///
/// Supported auth types:
/// - `anonymous` (default) — no credentials
/// - `access_token` — static IAM token via `token` field
/// - `service_account` — service account JSON key via `sa_file` field
///
/// Returns `YdbCredentials` which implements `ydb::Credentials`.
/// Build credentials AND extract raw token string for PQv1 auth.
pub fn build_credentials_with_token(auth: &AuthConfig) -> anyhow::Result<(YdbCredentials, Option<String>)> {
    match auth.auth_type.as_str() {
        "" | "anonymous" => Ok((YdbCredentials::Anonymous(ydb::AnonymousCredentials::new()), None)),
        "access_token" => {
            let token = if let Some(path) = auth.token_file.as_deref() {
                let expanded = shellexpand::full(path)
                    .map_err(|e| anyhow::anyhow!("Failed to expand token_file path '{}': {}", path, e))?;
                std::fs::read_to_string(expanded.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to read token from '{}': {}", expanded, e))?
                    .trim()
                    .to_string()
            } else if let Some(tok) = auth.token.as_deref() {
                tok.to_string()
            } else {
                anyhow::bail!("access_token auth requires either 'token' or 'token_file' field");
            };
            Ok((YdbCredentials::AccessToken(ydb::AccessTokenCredentials::from(token.clone())), Some(token)))
        }
        "service_account" => {
            let path = auth.sa_file.as_deref()
                .ok_or_else(|| anyhow::anyhow!("service_account auth requires 'sa_file' field"))?;
            let creds = ydb::ServiceAccountCredentials::from_file(path)
                .map_err(|e| anyhow::anyhow!("Failed to load service account key from '{}': {}", path, e))?;
            Ok((YdbCredentials::ServiceAccount(creds), None))
        }
        other => anyhow::bail!("Unsupported auth type '{}'. Supported: anonymous, access_token, service_account", other),
    }
}

pub fn build_credentials(auth: &AuthConfig) -> anyhow::Result<YdbCredentials> {
    match auth.auth_type.as_str() {
        "" | "anonymous" => Ok(YdbCredentials::Anonymous(ydb::AnonymousCredentials::new())),
        "access_token" => {
            let token = if let Some(path) = auth.token_file.as_deref() {
                let expanded = shellexpand::full(path)
                    .map_err(|e| anyhow::anyhow!("Failed to expand token_file path '{}': {}", path, e))?;
                std::fs::read_to_string(expanded.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to read token from '{}': {}", expanded, e))?
                    .trim()
                    .to_string()
            } else if let Some(tok) = auth.token.as_deref() {
                tok.to_string()
            } else {
                anyhow::bail!("access_token auth requires either 'token' or 'token_file' field");
            };
            Ok(YdbCredentials::AccessToken(ydb::AccessTokenCredentials::from(token)))
        }
        "service_account" => {
            let path = auth.sa_file.as_deref()
                .ok_or_else(|| anyhow::anyhow!("service_account auth requires 'sa_file' field"))?;
            let creds = ydb::ServiceAccountCredentials::from_file(path)
                .map_err(|e| anyhow::anyhow!("Failed to load service account key from '{}': {}", path, e))?;
            Ok(YdbCredentials::ServiceAccount(creds))
        }
        other => anyhow::bail!(
            "Unsupported auth type '{}'. Supported: anonymous, access_token, service_account",
            other
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_arrow_type_utf8() {
        assert_eq!(parse_arrow_type("Utf8").unwrap(), DataType::Utf8);
        assert_eq!(parse_arrow_type("String").unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_parse_arrow_type_int64() {
        assert_eq!(parse_arrow_type("Int64").unwrap(), DataType::Int64);
        assert_eq!(parse_arrow_type("int64").unwrap(), DataType::Int64);
    }

    #[test]
    fn test_parse_arrow_type_float64() {
        assert_eq!(parse_arrow_type("Float64").unwrap(), DataType::Float64);
    }

    #[test]
    fn test_parse_arrow_type_boolean() {
        assert_eq!(parse_arrow_type("Boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_arrow_type("bool").unwrap(), DataType::Boolean);
    }

    #[test]
    fn test_parse_arrow_type_timestamp_millisecond() {
        let parsed = parse_arrow_type("Timestamp(Millisecond, None)").unwrap();
        assert_eq!(parsed, DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn test_parse_arrow_type_timestamp_tz() {
        let parsed = parse_arrow_type("Timestamp(Microsecond, UTC)").unwrap();
        assert_eq!(
            parsed,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_parse_arrow_type_unsupported() {
        assert!(parse_arrow_type("Blob").is_err());
    }

    #[test]
    fn test_config_validate_empty_columns_fails() {
        let cfg = Config {
            source: SourceConfig {
                source_type: "topic".into(),
                connection_string: "grpc://localhost:2136".into(),
                topic_path: "/test".into(),
                consumer_name: "c".into(),
                auth: AuthConfig::default(),
                parser: ParserConfig {
                    table_naming: TableNaming { kind: "from_config".into(), name: Some("events".into()) },
                    parser_type: "json_parser".into(),
                    settings: SchemaConfig { columns: vec![], raw_payload_field: None, order_by: vec![], chunk_splitter: ChunkSplitter::NoSplit },
                },
                discovery_endpoint: None,
                partition_ids: None,
            },
            sink: SinkConfig {
                connection_string: "localhost:9000".into(),
                database: "default".into(),
                batch_size: 1000,
                max_linger_ms: 500,
                max_connections: 4,
                username: "default".into(),
                password: "".into(),
                use_tls: true,
                tls_domain: None,
                recreate_tables: false,
            },
            middlewares: vec![],
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_from_file_nonexistent() {
        let result = Config::from_file("/nonexistent/path.yaml");
        assert!(result.is_err());
    }
}
