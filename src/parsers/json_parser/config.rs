use serde::Deserialize;

use crate::config::yaml::{ChunkSplitter, ColumnMapping, SchemaConfig};

/// JSON parser configuration — deserialized directly from the YAML `json_parser:` block.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonParserConfig {
    pub columns: Vec<ColumnMapping>,
    /// Optional: custom field name for the raw JSON payload (for DLQ).
    #[serde(default)]
    pub raw_payload_field: Option<String>,
    /// Optional: column names for the `ClickHouse` `ORDER BY` clause.
    /// When empty/omitted, defaults to `ORDER BY tuple()`.
    #[serde(default)]
    pub order_by: Vec<String>,
    /// How to split incoming message bytes into individual JSON objects.
    /// - `no-split` (default): each message is one JSON
    /// - `new-line`:  split by `\n`, each non-empty line is one JSON
    #[serde(default)]
    pub chunk_splitter: ChunkSplitter,
    /// When `true`, null-valued columns are elided (absent keys) in serialized
    /// JSON output. When `false` (default), nulls are emitted as `"col": null`.
    #[serde(default)]
    pub skip_null_columns: bool,
    /// When `true`, the parser appends two system columns to every batch:
    /// `__system_partition` (Int64, YDS) / `__system_filename` (Utf8, S3) and
    /// `__system_offset` (Int64). The columns flow through to the sink; a
    /// `clickhouse` sink activates waterline dedup (EXACTLY_ONCE). Other sinks
    /// receive the columns as informational only (AT_LEAST_ONCE).
    #[serde(default)]
    pub add_exactly_once_keys: bool,
}

impl JsonParserConfig {
    /// Convert parser config into a generic [`SchemaConfig`] for DDL generation.
    /// `ColumnMapping` → `ColumnDef` (drops `jsonpath`).
    #[must_use]
    pub fn to_schema_config(&self) -> SchemaConfig {
        SchemaConfig {
            columns: self.columns.clone(),
            order_by: self.order_by.clone(),
        }
    }
}
