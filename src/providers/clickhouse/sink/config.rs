use core::fmt;

use serde::Deserialize;
use serde_yaml::Value;

use super::identifier::validate_identifier;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSinkConfig {
    pub endpoint: String,
    /// Explicit acknowledgement that the native hop is plaintext and must be
    /// protected by a trusted local network boundary or verified TLS tunnel.
    pub trusted_plaintext: bool,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_insert_rows")]
    pub insert_target_rows: usize,
    #[serde(default = "default_insert_bytes")]
    pub insert_target_bytes: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_retry_initial")]
    pub retry_initial_ms: u64,
    #[serde(default = "default_retry_max")]
    pub retry_max_ms: u64,
    #[serde(default)]
    pub retry_max_attempts: Option<u32>,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub sorting_key: Vec<String>,
}

impl ClickHouseSinkConfig {
    pub(super) fn from_value(value: Value) -> anyhow::Result<Self> {
        let config: Self = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse ClickHouse sink config: {error}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.endpoint.is_empty(),
            "clickhouse.endpoint must not be empty"
        );
        anyhow::ensure!(
            self.trusted_plaintext,
            "clickhouse.trusted_plaintext must be true; use a verified TLS tunnel when the destination is not on a trusted local network"
        );
        anyhow::ensure!(
            !self.database.is_empty(),
            "clickhouse.database must not be empty"
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
        anyhow::ensure!(
            self.connect_timeout_ms > 0,
            "clickhouse.connect_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "clickhouse.request_timeout_ms must be positive"
        );
        let mut sorting_columns = std::collections::HashSet::with_capacity(self.sorting_key.len());
        for column in &self.sorting_key {
            validate_identifier(column).map_err(|error| {
                error.context(format!("invalid clickhouse.sorting_key column {column:?}"))
            })?;
            anyhow::ensure!(
                sorting_columns.insert(column),
                "clickhouse.sorting_key repeats column '{column}'"
            );
        }
        Ok(())
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
            .field("endpoint", &self.endpoint)
            .field("trusted_plaintext", &self.trusted_plaintext)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("insert_target_rows", &self.insert_target_rows)
            .field("insert_target_bytes", &self.insert_target_bytes)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("retry_initial_ms", &self.retry_initial_ms)
            .field("retry_max_ms", &self.retry_max_ms)
            .field("retry_max_attempts", &self.retry_max_attempts)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("sorting_key", &self.sorting_key)
            .finish()
    }
}

const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 20;

fn default_database() -> String {
    "default".into()
}

fn default_username() -> String {
    "default".into()
}

const fn default_insert_rows() -> usize {
    100_000
}

const fn default_insert_bytes() -> usize {
    64 * 1024 * 1024
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
