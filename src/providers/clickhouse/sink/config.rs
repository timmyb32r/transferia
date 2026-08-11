use core::fmt;

use serde::Deserialize;
use serde_yaml::Value;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSinkConfig {
    pub connection_string: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_insert_rows")]
    pub max_insert_rows: usize,
    #[serde(default = "default_insert_bytes")]
    pub max_insert_bytes: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_retry_initial")]
    pub retry_initial_ms: u64,
    #[serde(default = "default_retry_max")]
    pub retry_max_ms: u64,
    #[serde(default)]
    pub retry_max_attempts: Option<u32>,
    pub use_tls: bool,
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
            !self.connection_string.is_empty(),
            "clickhouse.connection_string must not be empty"
        );
        anyhow::ensure!(
            !self.database.is_empty(),
            "clickhouse.database must not be empty"
        );
        anyhow::ensure!(
            self.max_insert_rows > 0,
            "clickhouse.max_insert_rows must be positive"
        );
        anyhow::ensure!(
            self.max_insert_bytes > 0,
            "clickhouse.max_insert_bytes must be positive"
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
        anyhow::ensure!(
            !self.use_tls,
            "clickhouse.use_tls=true is unavailable: clickhouse-arrow 0.2 does not verify \
             server certificates; use a verified local TLS tunnel and explicitly set \
             clickhouse.use_tls=false for the trusted plaintext hop"
        );
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
            .field("connection_string", &self.connection_string)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("max_insert_rows", &self.max_insert_rows)
            .field("max_insert_bytes", &self.max_insert_bytes)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("retry_initial_ms", &self.retry_initial_ms)
            .field("retry_max_ms", &self.retry_max_ms)
            .field("retry_max_attempts", &self.retry_max_attempts)
            .field("use_tls", &self.use_tls)
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
mod tests {
    use super::*;

    #[test]
    fn parses_explicit_plaintext_configuration() -> anyhow::Result<()> {
        let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\nsorting_key: [id]\n",
        )?)?;
        anyhow::ensure!(config.sorting_key == ["id"]);
        anyhow::ensure!(!config.use_tls);
        Ok(())
    }

    #[test]
    fn requires_an_explicit_transport_security_choice() -> anyhow::Result<()> {
        let raw: Value = serde_yaml::from_str("connection_string: localhost:9000\n")?;
        assert!(ClickHouseSinkConfig::from_value(raw).is_err());
        Ok(())
    }

    #[test]
    fn rejects_unverified_tls_and_removed_hypothesis_options() -> anyhow::Result<()> {
        let tls: Value =
            serde_yaml::from_str("connection_string: localhost:9000\nuse_tls: true\n")?;
        let stale: Value = serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\nrecreate_tables: true\n",
        )?;

        let tls_error = ClickHouseSinkConfig::from_value(tls).unwrap_err();
        assert!(tls_error
            .to_string()
            .contains("does not verify server certificates"));
        let stale_error = ClickHouseSinkConfig::from_value(stale).unwrap_err();
        assert!(stale_error.to_string().contains("unknown field"));
        Ok(())
    }

    #[test]
    fn rejects_old_order_by_name() {
        let result = serde_yaml::from_str::<ClickHouseSinkConfig>(
            "connection_string: localhost:9000\nuse_tls: false\norder_by: [id]\n",
        );
        assert!(result.is_err());
    }

    #[test]
    fn defaults_to_finite_retries() -> anyhow::Result<()> {
        let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\n",
        )?)?;
        assert_eq!(config.effective_retry_max_attempts(), 20);
        Ok(())
    }

    #[test]
    fn validates_retry_policy() -> anyhow::Result<()> {
        let zero_attempts: Value = serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\nretry_max_attempts: 0\n",
        )?;
        let inverted_backoff: Value = serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\nretry_initial_ms: 20\nretry_max_ms: 10\n",
        )?;
        let zero_connect_timeout: Value = serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\nconnect_timeout_ms: 0\n",
        )?;
        let zero_request_timeout: Value = serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\nrequest_timeout_ms: 0\n",
        )?;

        assert!(ClickHouseSinkConfig::from_value(zero_attempts).is_err());
        assert!(ClickHouseSinkConfig::from_value(inverted_backoff).is_err());
        assert!(ClickHouseSinkConfig::from_value(zero_connect_timeout).is_err());
        assert!(ClickHouseSinkConfig::from_value(zero_request_timeout).is_err());
        Ok(())
    }

    #[test]
    fn debug_redacts_password() -> anyhow::Result<()> {
        let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
            "connection_string: localhost:9000\nuse_tls: false\npassword: super-secret\n",
        )?)?;
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret"));
        Ok(())
    }
}
