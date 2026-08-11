use serde::Deserialize;
use serde_yaml::Value;

#[derive(Debug, Clone, Deserialize)]
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
    #[serde(default = "default_tls")]
    pub use_tls: bool,
    #[serde(default)]
    pub tls_domain: Option<String>,
    #[serde(default)]
    pub sorting_key: Vec<String>,
    #[serde(default)]
    pub recreate_tables: bool,
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
        Ok(())
    }

    pub(super) fn effective_retry_max_attempts(&self) -> u32 {
        self.retry_max_attempts
            .unwrap_or(DEFAULT_RETRY_MAX_ATTEMPTS)
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

const fn default_tls() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owns_table_policy() -> anyhow::Result<()> {
        let config: ClickHouseSinkConfig = serde_yaml::from_str(
            "connection_string: localhost:9000\nsorting_key: [id]\nrecreate_tables: true\n",
        )?;
        anyhow::ensure!(config.sorting_key == ["id"]);
        anyhow::ensure!(config.recreate_tables);
        Ok(())
    }

    #[test]
    fn rejects_old_order_by_name() {
        let result = serde_yaml::from_str::<ClickHouseSinkConfig>(
            "connection_string: localhost:9000\norder_by: [id]\n",
        );
        assert!(result.is_err());
    }

    #[test]
    fn defaults_to_finite_retries() -> anyhow::Result<()> {
        let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
            "connection_string: localhost:9000\n",
        )?)?;
        assert_eq!(config.effective_retry_max_attempts(), 20);
        Ok(())
    }

    #[test]
    fn validates_retry_policy() -> anyhow::Result<()> {
        let zero_attempts: Value =
            serde_yaml::from_str("connection_string: localhost:9000\nretry_max_attempts: 0\n")?;
        let inverted_backoff: Value = serde_yaml::from_str(
            "connection_string: localhost:9000\nretry_initial_ms: 20\nretry_max_ms: 10\n",
        )?;

        assert!(ClickHouseSinkConfig::from_value(zero_attempts).is_err());
        assert!(ClickHouseSinkConfig::from_value(inverted_backoff).is_err());
        Ok(())
    }
}
