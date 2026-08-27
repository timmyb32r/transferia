use core::fmt;

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClickHouseCompression {
    None,

    #[default]
    Lz4,

    Zstd,
}

impl From<ClickHouseCompression> for clickhouse_arrow::CompressionMethod {
    fn from(value: ClickHouseCompression) -> Self {
        match value {
            ClickHouseCompression::None => Self::None,
            ClickHouseCompression::Lz4 => Self::LZ4,
            ClickHouseCompression::Zstd => Self::ZSTD,
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSinkConfig {
    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "host" }))]
    pub hosts: Vec<String>,

    #[schemars(description = "native port")]
    pub port: u16,

    /// Explicit acknowledgement that the native hop is plaintext and must be
    /// protected by a trusted local network boundary or verified TLS tunnel.
    pub trusted_plaintext: bool,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub tls_ca_file: Option<String>,

    /// Data-bearing hosts in the resolved cluster topology. Connectivity may
    /// intentionally use a smaller set of hosts, so it cannot carry this fact.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub data_host_count: Option<usize>,

    pub database: String,

    pub username: String,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub password: String,

    #[serde(default)]
    #[schemars(title = "Shard group", extend("x-ui" = { "section": "shard_group" }))]
    pub shard_group: String,

    #[serde(default = "default_insert_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub insert_target_rows: usize,

    #[serde(default = "default_insert_bytes")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub insert_target_bytes: usize,

    /// Maximum number of concurrently active INSERTs. Values above one are an
    /// explicit throughput choice; ordered delivery progress is still committed
    /// only after the contiguous INSERT prefix completes.
    #[serde(default = "default_insert_concurrency")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub insert_concurrency: usize,

    /// Native-protocol compression. LZ4 is the default low-CPU mode; ZSTD can
    /// improve a network-bound delivery and `none` avoids compression work on
    /// a sufficiently fast trusted network.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub compression: ClickHouseCompression,

    /// Let ClickHouse coalesce concurrent native INSERTs server-side. Waiting
    /// remains mandatory, so a successful response still means the buffered
    /// data has reached the table rather than merely entering an async queue.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub async_insert: bool,

    #[serde(default = "default_flush_interval")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub flush_interval_ms: u64,

    #[serde(default = "default_retry_initial")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_initial_ms: u64,

    #[serde(default = "default_retry_max")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_max_ms: u64,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub retry_max_attempts: Option<u32>,

    #[serde(default = "default_connect_timeout")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub connect_timeout_ms: u64,

    #[serde(default = "default_request_timeout")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub request_timeout_ms: u64,
}

impl ClickHouseSinkConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        self.validate_connection()?;
        anyhow::ensure!(
            !self.database.is_empty(),
            "clickhouse.database must not be empty"
        );
        anyhow::ensure!(
            !self.username.is_empty(),
            "clickhouse.username must not be empty"
        );
        anyhow::ensure!(
            self.insert_target_rows > 0,
            "clickhouse.insert_target_rows must be positive"
        );
        anyhow::ensure!(
            self.insert_target_bytes > 0,
            "clickhouse.insert_target_bytes must be positive"
        );
        anyhow::ensure!(
            (1..=32).contains(&self.insert_concurrency),
            "clickhouse.insert_concurrency must be between 1 and 32"
        );
        anyhow::ensure!(
            self.flush_interval_ms > 0,
            "clickhouse.flush_interval_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_initial_ms > 0,
            "clickhouse.retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_max_ms >= self.retry_initial_ms,
            "clickhouse.retry_max_ms must be greater than or equal to retry_initial_ms"
        );
        anyhow::ensure!(
            self.retry_max_attempts != Some(0),
            "clickhouse.retry_max_attempts must be positive"
        );
        Ok(())
    }

    pub(super) fn validate_connection(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.hosts.is_empty(), "clickhouse.hosts must not be empty");
        let mut hosts = std::collections::HashSet::with_capacity(self.hosts.len());
        for host in &self.hosts {
            crate::connectors::address::validate_host("clickhouse.hosts", host)?;
            anyhow::ensure!(hosts.insert(host), "clickhouse.hosts repeats host '{host}'");
        }
        validate_native_port(self.port)?;
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
        anyhow::ensure!(
            self.data_host_count != Some(0),
            "clickhouse.data_host_count must be positive"
        );
        Ok(())
    }

    pub(super) fn effective_data_host_count(&self) -> usize {
        self.data_host_count.unwrap_or(self.hosts.len())
    }

    pub(super) fn effective_retry_max_attempts(&self) -> u32 {
        self.retry_max_attempts
            .unwrap_or(DEFAULT_RETRY_MAX_ATTEMPTS)
    }

    pub(super) const fn connect_timeout(&self) -> core::time::Duration {
        core::time::Duration::from_millis(self.connect_timeout_ms)
    }

    pub(super) const fn request_timeout(&self) -> core::time::Duration {
        core::time::Duration::from_millis(self.request_timeout_ms)
    }
}

impl fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseSinkConfig")
            .field("hosts", &self.hosts)
            .field("port", &self.port)
            .field("trusted_plaintext", &self.trusted_plaintext)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("data_host_count", &self.data_host_count)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("shard_group", &self.shard_group)
            .field("insert_target_rows", &self.insert_target_rows)
            .field("insert_target_bytes", &self.insert_target_bytes)
            .field("insert_concurrency", &self.insert_concurrency)
            .field("compression", &self.compression)
            .field("async_insert", &self.async_insert)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("retry_initial_ms", &self.retry_initial_ms)
            .field("retry_max_ms", &self.retry_max_ms)
            .field("retry_max_attempts", &self.retry_max_attempts)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .finish()
    }
}

const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 20;

const fn default_insert_rows() -> usize {
    100_000
}

pub fn validate_native_port(port: u16) -> anyhow::Result<()> {
    crate::connectors::address::validate_port("clickhouse.port", port)?;
    anyhow::ensure!(
        !matches!(port, 8123 | 8443),
        "clickhouse.port {port} is a ClickHouse HTTP port, but this connector uses the native protocol; configure the native port (default: {})",
        crate::connectors::clickhouse::DEFAULT_NATIVE_PORT
    );
    Ok(())
}

const fn default_insert_bytes() -> usize {
    64 * 1024 * 1024
}

const fn default_insert_concurrency() -> usize {
    1
}

const fn default_flush_interval() -> u64 {
    100
}

const fn default_retry_initial() -> u64 {
    50
}

const fn default_retry_max() -> u64 {
    30_000
}

const fn default_connect_timeout() -> u64 {
    30_000
}

const fn default_request_timeout() -> u64 {
    30_000
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
