use std::collections::HashSet;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::connectors::clickhouse::sink::identifier::validate_identifier;
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

    pub tables: Vec<TableConfig>,

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

    #[serde(default = "default_connect_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub connect_timeout_ms: u64,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub request_timeout_ms: u64,
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

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub database: String,

    pub name: String,

    /// Columns that the user guarantees identify one logical row uniquely.
    /// `ClickHouse` sorting and primary keys do not enforce uniqueness, so the
    /// connector never infers this contract from table metadata.
    #[serde(default)]
    #[schemars(title = "Unique row key", extend("x-ui" = { "widget": "hidden" }))]
    pub primary_key: Vec<String>,
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
        let mut identities = HashSet::with_capacity(self.tables.len());
        for table in &self.tables {
            validate_identifier(&table.database)
                .map_err(|error| error.context("invalid clickhouse.tables.database"))?;
            validate_identifier(&table.name)
                .map_err(|error| error.context("invalid clickhouse.tables.name"))?;
            let mut primary_keys = HashSet::with_capacity(table.primary_key.len());
            for column in &table.primary_key {
                validate_identifier(column)
                    .map_err(|error| error.context("invalid clickhouse.tables.primary_key"))?;
                anyhow::ensure!(
                    primary_keys.insert(column.as_str()),
                    "clickhouse.tables primary_key repeats column '{}' for '{}.{}'",
                    column,
                    table.database,
                    table.name,
                );
            }
            anyhow::ensure!(
                identities.insert((table.database.as_str(), table.name.as_str())),
                "clickhouse.tables repeats source table '{}.{}'",
                table.database,
                table.name
            );
        }
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
