use arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;

/// Top-level configuration for the replicator.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub schema: SchemaConfig,
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
        if self.schema.columns.is_empty() {
            anyhow::bail!("schema.columns must not be empty");
        }
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
        if self.sink.table_name.is_empty() {
            anyhow::bail!("sink.table_name must not be empty");
        }
        if self.sink.dlq_table_name.is_empty() {
            anyhow::bail!("sink.dlq_table_name must not be empty");
        }
        // Validate arrow types for all column mappings
        for col in &self.schema.columns {
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
                    let col = self.schema.columns.iter()
                        .find(|c| c.column_name == mw.field.as_deref().unwrap_or(""))
                        .ok_or_else(|| anyhow::anyhow!(
                            "middlewares[{}]: filter field '{}' not found in schema.columns",
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
    /// YDB connection string, e.g. "grpc://localhost:2136/local"
    pub connection_string: String,
    /// YDB topic path, e.g. "/local/test-topic"
    pub topic_path: String,
    /// YDB consumer name
    pub consumer_name: String,
    #[serde(default)]
    pub auth: AuthConfig,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[allow(dead_code)]
pub struct AuthConfig {
    /// Auth type: "anonymous" (default), "access_token", "service_account"
    #[serde(rename = "type")]
    pub auth_type: String,
    /// Token for access_token auth
    pub token: Option<String>,
    /// Path to service account JSON key file
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
    pub connection_string: String,
    /// ClickHouse database name
    pub database: String,
    /// Main target table
    pub table_name: String,
    /// Dead letter queue table
    pub dlq_table_name: String,
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

/// Build YDB credentials from the auth config section.
/// Build YDB credentials from the auth config section.
/// Returns a concrete credential type that implements `ydb::Credentials`.
pub fn build_credentials(auth: &AuthConfig) -> anyhow::Result<ydb::AnonymousCredentials> {
    match auth.auth_type.as_str() {
        "" | "anonymous" => Ok(ydb::AnonymousCredentials::new()),
        // TODO: expand for access_token, service_account, etc.
        other => anyhow::bail!("Unsupported auth type '{}' (PoC supports only anonymous)", other),
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
                connection_string: "grpc://localhost:2136".into(),
                topic_path: "/test".into(),
                consumer_name: "c".into(),
                auth: AuthConfig::default(),
            },
            schema: SchemaConfig {
                columns: vec![],
                raw_payload_field: None,
            },
            sink: SinkConfig {
                connection_string: "localhost:9000".into(),
                database: "default".into(),
                table_name: "events".into(),
                dlq_table_name: "events_dlq".into(),
                batch_size: 1000,
                max_linger_ms: 500,
                max_connections: 4,
                username: "default".into(),
                password: "".into(),
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
