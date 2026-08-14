use std::collections::HashSet;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;

use crate::providers::clickhouse::sink::identifier::validate_identifier;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSourceConfig {
    pub hosts: Vec<String>,
    #[schemars(description = "native port")]
    pub port: u16,
    pub trusted_plaintext: bool,
    pub username: String,
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub password: String,
    pub tables: Vec<TableConfig>,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub database: String,
    pub name: String,
    pub output_name: String,
    pub order_by: Vec<String>,
}

impl ClickHouseSourceConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.hosts.is_empty(), "clickhouse.hosts must not be empty");
        let mut hosts = HashSet::with_capacity(self.hosts.len());
        for host in &self.hosts {
            crate::providers::address::validate_host("clickhouse.hosts", host)?;
            anyhow::ensure!(hosts.insert(host), "clickhouse.hosts repeats host '{host}'");
        }
        crate::providers::clickhouse::sink::config::validate_native_port(self.port)?;
        anyhow::ensure!(self.trusted_plaintext, "clickhouse.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network");
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
            self.connect_timeout_ms > 0,
            "clickhouse.connect_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "clickhouse.request_timeout_ms must be positive"
        );
        let mut outputs = HashSet::with_capacity(self.tables.len());
        let mut identities = HashSet::with_capacity(self.tables.len());
        for table in &self.tables {
            validate_identifier(&table.database)
                .map_err(|error| error.context("invalid clickhouse.tables.database"))?;
            validate_identifier(&table.name)
                .map_err(|error| error.context("invalid clickhouse.tables.name"))?;
            validate_identifier(&table.output_name)
                .map_err(|error| error.context("invalid clickhouse.tables.output_name"))?;
            anyhow::ensure!(!table.order_by.is_empty(), "clickhouse.tables entry '{}.{}' must declare order_by for a deterministic snapshot", table.database, table.name);
            let mut ordering = HashSet::with_capacity(table.order_by.len());
            for column in &table.order_by {
                validate_identifier(column)
                    .map_err(|error| error.context("invalid clickhouse.tables.order_by"))?;
                anyhow::ensure!(
                    ordering.insert(column.as_str()),
                    "clickhouse.tables entry '{}.{}' repeats order_by column '{column}'",
                    table.database,
                    table.name
                );
            }
            anyhow::ensure!(
                identities.insert((table.database.as_str(), table.name.as_str())),
                "clickhouse.tables repeats source table '{}.{}'",
                table.database,
                table.name
            );
            anyhow::ensure!(
                outputs.insert(table.output_name.as_str()),
                "clickhouse.tables repeats output_name '{}'",
                table.output_name
            );
        }
        Ok(())
    }

    pub(super) const fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    pub(super) const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

const fn default_batch_rows() -> usize {
    65_536
}
const fn default_connect_timeout_ms() -> u64 {
    30_000
}
const fn default_request_timeout_ms() -> u64 {
    30_000
}
