use arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;

/// Top-level configuration for the replicator.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: SourceEntry,
    pub sink: SinkEntry,
    #[serde(default)]
    pub middlewares: Vec<MiddlewareConfig>,
    /// Drop+recreate tables on start (dev/bench only, off by default).
    #[serde(default)]
    pub recreate_tables: bool,
    /// Pipeline: flush batch size (rows).
    #[serde(default = "default_batch_size")]
    pub sink_batch_size: usize,
}

impl Config {
    /// Load configuration from a YAML file, expanding ${VAR} and $VAR patterns.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file '{path}': {e}"))?;
        let expanded = shellexpand::env(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to expand env vars in config: {e}"))?;
        let config: Self = serde_yaml::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("Failed to parse YAML config: {e}"))?;
        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// Provider-agnostic source/sink entries (opaque to common code)
// ---------------------------------------------------------------------------

/// Source config entry: `source: { <kind>: { ... } }` — exactly one key.
#[derive(Debug, Deserialize)]
pub struct SourceEntry {
    #[serde(flatten)]
    pub inner: std::collections::HashMap<String, serde_yaml::Value>,
}

impl SourceEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!("source: no provider key found (expected 'topic', 'pqv1', or 's3')"),
            _ => anyhow::bail!("source: expected exactly one provider key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&serde_yaml::Value> {
        let kind = self.kind()?;
        self.inner.get(kind)
            .ok_or_else(|| anyhow::anyhow!("source: provider key '{kind}' missing from config"))
    }
}

/// Sink config entry: `sink: { <kind>: { ... } }` — exactly one key.
#[derive(Debug, Deserialize)]
pub struct SinkEntry {
    #[serde(flatten)]
    pub inner: std::collections::HashMap<String, serde_yaml::Value>,
}

impl SinkEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!("sink: no provider key found"),
            _ => anyhow::bail!("sink: expected exactly one provider key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&serde_yaml::Value> {
        let kind = self.kind()?;
        self.inner.get(kind)
            .ok_or_else(|| anyhow::anyhow!("sink: provider key '{kind}' missing from config"))
    }
}

// ---------------------------------------------------------------------------
// Common parser validation (called by every source provider)
// ---------------------------------------------------------------------------

/// Validate middleware↔column compatibility against the JSON parser config.
/// The parser itself validates its own config (`columns`, `arrow_type`, etc.)
/// during construction.
pub fn validate_parser_middlewares(
    columns: &[ColumnMapping],
    middlewares: &[MiddlewareConfig],
) -> anyhow::Result<()> {
    for (i, mw) in middlewares.iter().enumerate() {
        if mw.mw_type != "filter" { continue; }
        if mw.field.as_ref().is_none_or(String::is_empty) {
            anyhow::bail!("middlewares[{i}]: filter requires non-empty 'field'");
        }
        if mw.value.as_ref().is_none_or(String::is_empty) {
            anyhow::bail!("middlewares[{i}]: filter requires non-empty 'value'");
        }
        let col = columns.iter()
            .find(|c| c.column_name == mw.field.as_deref().unwrap_or(""))
            .ok_or_else(|| anyhow::anyhow!(
                "middlewares[{}]: filter field '{}' not found in columns",
                i, mw.field.as_deref().unwrap_or("")
            ))?;
        let dt = parse_arrow_type(&col.arrow_type)?;
        if dt != DataType::Utf8 && dt != DataType::LargeUtf8 {
            anyhow::bail!(
                "middlewares[{}]: filter field '{}' is {:?}, only Utf8/LargeUtf8",
                i, col.column_name, dt
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Source configuration — tagged enum
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Parser configuration (table naming + concrete parser settings)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ParserConfig {
    /// How the destination table name is chosen.
    pub table_naming: TableNaming,
    /// Parser entry — `{ <kind>: { <config> } }` — exactly one key.
    #[serde(flatten)]
    pub parser: crate::parsers::ParserEntry,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableNaming {
    /// "`from_config`" — use `name`; "`from_topic`" — use the topic path verbatim.
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
            other => anyhow::bail!("unknown table_naming.type '{other}' (use from_config | from_topic)"),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct AuthConfig {
    /// Auth type: "anonymous" (default), "`access_token`", "`service_account`".
    #[serde(rename = "type")]
    pub auth_type: String,
    /// Token string for `access_token` auth (plain text).
    pub token: Option<String>,
    /// Path to file containing the token for `access_token` auth.
    /// The file is read at startup and its trimmed contents used as the token.
    /// Supports ~ for home directory expansion.
    pub token_file: Option<String>,
    /// Path to service account JSON key file (for `service_account` auth).
    pub sa_file: Option<String>,
}

// ---------------------------------------------------------------------------
// Schema / column mapping configuration
// ---------------------------------------------------------------------------

/// Column names, Arrow types, nullability, and `ORDER BY` clause — the common
/// denominator for DDL generation across all source types.
///
/// Parser-specific concerns live in the parser's own config
/// (e.g. [`crate::parsers::json_parser::JsonParserConfig`]).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SchemaConfig {
    pub columns: Vec<ColumnMapping>,
    /// Optional: column names for the `ClickHouse` `ORDER BY` clause.
    /// When empty/omitted, defaults to `ORDER BY tuple()`.
    pub order_by: Vec<String>,
}

impl SchemaConfig {
    /// Creates a schema from the given column mappings.
    #[must_use]
    pub const fn new(columns: Vec<ColumnMapping>) -> Self {
        Self { columns, order_by: Vec::new() }
    }

    /// Column definitions for DDL — drops `JSONPath`, keeps only name + type.
    #[must_use]
    pub fn column_defs(&self) -> Vec<ColumnDef> {
        self.columns.iter().map(ColumnMapping::to_column_def).collect()
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ChunkSplitter {
    #[default]
    NoSplit,
    NewLine,
}

impl ChunkSplitter {
    /// Last safe split point in `buf` — never cuts a record in half.
    /// `NoSplit`: entire buffer is one record → `buf.len()`.
    /// `NewLine`: position after last `\n`; `0` when no delimiter found
    /// (caller should accumulate more data or treat as EOF remainder).
    #[must_use]
    pub fn safe_split_at(&self, buf: &[u8]) -> usize {
        match self {
            Self::NoSplit => buf.len(),
            Self::NewLine => buf
                .iter()
                .rposition(|&b| b == b'\n')
                .map_or(0, |i| i + 1),
        }
    }

    /// Split a completed chunk into non-empty records.
    /// Trailing empty records (from a final `\n`) are discarded.
    ///
    /// Fast-path for `NewLine` without `\n`: returns `vec![buf]` immediately
    /// (no iteration, no allocation beyond the single-element Vec).
    #[must_use]
    pub fn split_into_records<'buf>(&self, buf: &'buf [u8]) -> Vec<&'buf [u8]> {
        match self {
            Self::NoSplit => vec![buf],
            Self::NewLine => {
                // Fast-path: no delimiter → one record, no split iteration
                if !buf.contains(&b'\n') {
                    return if buf.is_empty() { Vec::new() } else { vec![buf] };
                }
                buf.split(|&b| b == b'\n')
                    .filter(|line| !line.is_empty())
                    .collect()
            }
        }
    }

    /// Count records without allocating a `Vec`. Semantics match
    /// `split_into_records` — non-empty lines, trailing delimiter discarded.
    #[must_use]
    pub fn count_records(&self, buf: &[u8]) -> usize {
        match self {
            Self::NoSplit => usize::from(!buf.is_empty()),
            Self::NewLine => {
                if buf.is_empty() { return 0; }
                // Fast-path: no delimiter → one record
                if !buf.contains(&b'\n') { return 1; }
                buf.split(|&b| b == b'\n')
                    .filter(|line| !line.is_empty())
                    .count()
            }
        }
    }
}

/// Column definition for DDL — name and type only, no parser-specific fields.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub column_name: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Column mapping for JSON parser config — includes a `jsonpath` expression
/// for extracting values from incoming JSON objects.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMapping {
    /// `JSONPath` expression, e.g. "$.payload.user.id".
    pub jsonpath: String,
    /// Arrow/ClickHouse column name.
    pub column_name: String,
    /// Arrow type string: "Utf8", "Int64", "Float64", "Timestamp(Millisecond, None)", etc.
    pub arrow_type: String,
    /// Whether the column is nullable.
    #[serde(default)]
    pub nullable: bool,
}

impl ColumnMapping {
    /// Creates a column mapping with explicit `JSONPath`, name, type and nullability.
    #[must_use]
    pub const fn new(jsonpath: String, column_name: String, arrow_type: String, nullable: bool) -> Self {
        Self { jsonpath, column_name, arrow_type, nullable }
    }

    /// Drop the `JSONPath` — only the DDL-relevant fields remain.
    #[must_use]
    pub fn to_column_def(&self) -> ColumnDef {
        ColumnDef {
            column_name: self.column_name.clone(),
            arrow_type: self.arrow_type.clone(),
            nullable: self.nullable,
        }
    }
}

impl From<ColumnDef> for ColumnMapping {
    fn from(d: ColumnDef) -> Self {
        Self {
            jsonpath: String::new(),
            column_name: d.column_name,
            arrow_type: d.arrow_type,
            nullable: d.nullable,
        }
    }
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

const fn default_batch_size() -> usize {
    10000
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
            let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
            let unit = match parts.first().copied().unwrap_or("Microsecond") {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                other => anyhow::bail!(
                    "Unsupported Timestamp unit '{other}'. Use Second, Millisecond, Microsecond, or Nanosecond."
                ),
            };
            let tz: Option<String> = (parts.len() > 1 && parts[1] != "None").then(|| parts[1].to_string());
            Ok(DataType::Timestamp(unit, tz.map(Into::into)))
        }
        other => anyhow::bail!(
            "Unsupported arrow_type: '{other}'. Supported: Utf8, String, LargeUtf8, Int64, Int32, Int16, Int8, UInt64, UInt32, UInt16, UInt8, Float64, Float32, Boolean, Date32, Date64, Timestamp(unit, tz)"
        ),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_arrow_type_utf8() -> anyhow::Result<()> {
        anyhow::ensure!(parse_arrow_type("Utf8")? == DataType::Utf8);
        anyhow::ensure!(parse_arrow_type("String")? == DataType::Utf8);
        Ok(())
    }

    #[test]
    fn parse_arrow_type_int64() -> anyhow::Result<()> {
        anyhow::ensure!(parse_arrow_type("Int64")? == DataType::Int64);
        anyhow::ensure!(parse_arrow_type("int64")? == DataType::Int64);
        Ok(())
    }

    #[test]
    fn parse_arrow_type_float64() -> anyhow::Result<()> {
        anyhow::ensure!(parse_arrow_type("Float64")? == DataType::Float64);
        Ok(())
    }

    #[test]
    fn parse_arrow_type_boolean() -> anyhow::Result<()> {
        anyhow::ensure!(parse_arrow_type("Boolean")? == DataType::Boolean);
        anyhow::ensure!(parse_arrow_type("bool")? == DataType::Boolean);
        Ok(())
    }

    #[test]
    fn parse_arrow_type_timestamp_millisecond() -> anyhow::Result<()> {
        let parsed = parse_arrow_type("Timestamp(Millisecond, None)")?;
        anyhow::ensure!(parsed == DataType::Timestamp(TimeUnit::Millisecond, None));
        Ok(())
    }

    #[test]
    fn parse_arrow_type_timestamp_tz() -> anyhow::Result<()> {
        let parsed = parse_arrow_type("Timestamp(Microsecond, UTC)")?;
        anyhow::ensure!(parsed == DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        Ok(())
    }

    #[test]
    fn parse_arrow_type_unsupported() -> anyhow::Result<()> {
        let err = parse_arrow_type("Blob")
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected 'Blob' to be rejected"))?;
        anyhow::ensure!(err.to_string().contains("Unsupported arrow_type"), "got: {err}");
        Ok(())
    }

    #[test]
    fn json_parser_empty_columns_fails() -> anyhow::Result<()> {
        let cfg = crate::parsers::json_parser::JsonParserConfig {
            columns: vec![],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NoSplit,
            skip_null_columns: false,
        };
        let err = crate::parsers::json_parser::JsonParser::new(&cfg, "test".into(), None)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected empty columns to fail"))?;
        anyhow::ensure!(err.to_string().contains("columns must not be empty"), "got: {err}");
        Ok(())
    }

    #[test]
    fn config_from_file_nonexistent() -> anyhow::Result<()> {
        let err = Config::from_file("/nonexistent/path.yaml")
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected missing config file to fail"))?;
        anyhow::ensure!(err.to_string().contains("Failed to read config file"), "got: {err}");
        Ok(())
    }
}