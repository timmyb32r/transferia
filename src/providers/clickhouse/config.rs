use serde::Deserialize;

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
}
