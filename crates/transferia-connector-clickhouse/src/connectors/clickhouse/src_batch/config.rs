use std::collections::HashSet;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use transferia_registry::table_selection::TableSelection;

use crate::connectors::clickhouse::sink::ClickHouseCompression;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSourceConfig {
    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "host" }))]
    pub hosts: Vec<String>,

    #[schemars(description = "native port")]
    pub port: u16,

    #[serde(default = "default_http_port")]
    #[schemars(
        title = "HTTP port",
        description = "ClickHouse HTTPS/HTTP port used by the Parquet snapshot reader",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub http_port: u16,

    pub trusted_plaintext: bool,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub tls_ca_file: Option<String>,

    pub username: String,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub password: String,

    #[serde(default = "default_hide_system_tables")]
    #[schemars(title = "Hide system tables", description = "Exclude tables in system, _system, information_schema* and INFORMATION_SCHEMA databases from discovery and table suggestions. Disable to include them.", extend("x-ui" = { "order": 1 }))]
    pub hide_system_tables: bool,

    #[schemars(extend("x-ui" = { "widget": "table_selection", "table_membership": "fixed", "order": 2 }))]
    pub tables: TableSelection,

    #[serde(default = "default_batch_rows")]
    #[schemars(
        title = "Maximum block rows",
        description = "Maximum number of rows requested in one ClickHouse result block",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub batch_rows: usize,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub snapshot_reader: ClickHouseSnapshotReader,

    #[serde(default)]
    #[schemars(
        title = "Unsupported source types",
        description = "When a ClickHouse column cannot be represented by the source Arrow reader: Fail delivery (default) rejects it during discovery. to_string explicitly applies ClickHouse toString to the entire column, including nested values, and outputs UTF-8 text with the original type recorded in schema metadata. This changes the type and may not be reversible. NULL remains NULL. If ClickHouse cannot perform toString or returns invalid UTF-8, the delivery fails; values are never replaced or skipped.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub unsupported_types: UnsupportedTypePolicy,

    #[serde(default = "default_connect_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub connect_timeout_ms: u64,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub request_timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedTypePolicy {
    #[default]
    #[schemars(title = "Fail delivery")]
    Fail,
    #[schemars(title = "to_string")]
    ToString,
}

const fn default_hide_system_tables() -> bool {
    true
}

pub(super) fn is_system_database(database: &str) -> bool {
    matches!(database, "system" | "_system" | "INFORMATION_SCHEMA")
        || database.starts_with("information_schema")
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClickHouseSnapshotReader {
    #[schemars(title = "Parquet (high throughput)")]
    Parquet {
        #[serde(default)]
        compression: ClickHouseParquetCompression,

        #[serde(default = "default_parquet_max_threads")]
        #[schemars(title = "ClickHouse encoding threads", range(min = 1))]
        max_threads: usize,

        #[serde(default = "default_parquet_row_group_rows")]
        #[schemars(title = "Rows per Parquet row group", range(min = 1))]
        row_group_rows: usize,

        #[serde(default = "default_parquet_decode_threads")]
        #[schemars(
            title = "Parquet decode threads",
            description = "Maximum number of Parquet row groups decoded concurrently",
            range(min = 1)
        )]
        decode_threads: usize,

        #[serde(default = "default_parquet_max_response_bytes")]
        #[schemars(
            title = "Maximum Parquet response bytes",
            description = "Explicit memory safety limit for one compressed ClickHouse snapshot response",
            range(min = 1),
            extend("x-ui" = { "widget": "byte_size" })
        )]
        max_response_bytes: u64,
    },

    #[schemars(title = "Native TCP")]
    Native {
        #[serde(default = "default_native_max_threads")]
        #[schemars(title = "ClickHouse read threads", range(min = 1))]
        max_threads: usize,

        #[serde(default = "default_native_compression")]
        compression: ClickHouseCompression,
    },
}

impl Default for ClickHouseSnapshotReader {
    fn default() -> Self {
        Self::Parquet {
            compression: ClickHouseParquetCompression::Zstd,
            max_threads: default_parquet_max_threads(),
            row_group_rows: default_parquet_row_group_rows(),
            decode_threads: default_parquet_decode_threads(),
            max_response_bytes: default_parquet_max_response_bytes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClickHouseParquetCompression {
    Lz4,

    #[default]
    Zstd,
}

impl ClickHouseParquetCompression {
    #[must_use]
    pub const fn clickhouse_name(self) -> &'static str {
        match self {
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

/// Discovery uses the table's PRIMARY KEY, or ORDER BY when both keys match,
/// as the delivery row identity. This is a delivery contract, not a claim that
/// `ClickHouse` enforces uniqueness. Key expressions must be plain columns.
#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub database: String,

    pub name: String,
}

impl ClickHouseSourceConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        self.validate_connection()?;
        anyhow::ensure!(
            !self.username.is_empty(),
            "clickhouse.username must not be empty"
        );
        anyhow::ensure!(
            !self.tables.is_empty(),
            "clickhouse.tables must not be empty"
        );
        anyhow::ensure!(
            self.batch_rows > 0,
            "clickhouse.batch_rows must be positive"
        );
        anyhow::ensure!(
            i64::try_from(self.batch_rows).is_ok(),
            "clickhouse.batch_rows must fit a signed 64-bit ClickHouse setting"
        );
        self.snapshot_reader.validate()?;
        self.tables.compile()?;
        Ok(())
    }

    pub(super) fn validate_connection(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.hosts.is_empty(), "clickhouse.hosts must not be empty");
        let mut hosts = HashSet::with_capacity(self.hosts.len());
        for host in &self.hosts {
            crate::connectors::address::validate_host("clickhouse.hosts", host)?;
            anyhow::ensure!(hosts.insert(host), "clickhouse.hosts repeats host '{host}'");
        }
        crate::connectors::clickhouse::sink::config::validate_native_port(self.port)?;
        anyhow::ensure!(self.http_port > 0, "clickhouse.http_port must be positive");
        if let Some(path) = &self.tls_ca_file {
            anyhow::ensure!(
                !path.trim().is_empty(),
                "clickhouse.tls_ca_file must not be empty"
            );
        }
        anyhow::ensure!(
            self.connect_timeout_ms > 0,
            "clickhouse.connect_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "clickhouse.request_timeout_ms must be positive"
        );
        Ok(())
    }

    pub(super) const fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    pub(super) const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

impl ClickHouseSnapshotReader {
    fn validate(&self) -> anyhow::Result<()> {
        let (max_threads, row_group_rows, decode_threads, max_response_bytes) = match self {
            Self::Parquet {
                max_threads,
                row_group_rows,
                decode_threads,
                max_response_bytes,
                ..
            } => (
                *max_threads,
                Some(*row_group_rows),
                Some(*decode_threads),
                Some(*max_response_bytes),
            ),
            Self::Native { max_threads, .. } => (*max_threads, None, None, None),
        };
        anyhow::ensure!(
            max_threads > 0,
            "clickhouse snapshot read threads must be positive"
        );
        anyhow::ensure!(
            i64::try_from(max_threads).is_ok(),
            "clickhouse snapshot read threads must fit a signed 64-bit ClickHouse setting"
        );
        if let Some(value) = row_group_rows {
            anyhow::ensure!(
                value > 0,
                "clickhouse Parquet row_group_rows must be positive"
            );
            anyhow::ensure!(
                i64::try_from(value).is_ok(),
                "clickhouse Parquet row_group_rows must fit a signed 64-bit ClickHouse setting"
            );
        }
        if let Some(value) = decode_threads {
            anyhow::ensure!(
                value > 0,
                "clickhouse Parquet decode_threads must be positive"
            );
        }
        if let Some(value) = max_response_bytes {
            anyhow::ensure!(
                value > 0,
                "clickhouse Parquet max_response_bytes must be positive"
            );
            anyhow::ensure!(
                usize::try_from(value).is_ok(),
                "clickhouse Parquet max_response_bytes exceeds this platform's address space"
            );
        }
        Ok(())
    }
}

const fn default_batch_rows() -> usize {
    65_409
}
const fn default_http_port() -> u16 {
    8123
}
const fn default_native_max_threads() -> usize {
    16
}
const fn default_native_compression() -> ClickHouseCompression {
    ClickHouseCompression::Zstd
}
const fn default_parquet_max_threads() -> usize {
    32
}
const fn default_parquet_row_group_rows() -> usize {
    250_000
}
const fn default_parquet_decode_threads() -> usize {
    16
}
const fn default_parquet_max_response_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}
const fn default_connect_timeout_ms() -> u64 {
    30_000
}
const fn default_request_timeout_ms() -> u64 {
    30_000
}
