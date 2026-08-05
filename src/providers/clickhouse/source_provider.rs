use futures_util::future::BoxFuture;
use regex::Regex;
use serde::Deserialize;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::ParserConfig;
use crate::pipeline::source::Source;
use crate::providers::clickhouse::source::{ClickHouseSource, TableRef, TableSelection};
use crate::providers::traits::SourceProvider;

#[derive(Debug, Deserialize)]
pub struct ClickHouseSourceConfig {
    pub connection_string: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_tls")]
    pub use_tls: bool,
    #[serde(default)]
    pub tls_domain: Option<String>,
    #[serde(default = "default_rows_per_page")]
    pub rows_per_page: usize,
    /// Table selection: oneof
    #[serde(default)]
    pub tables: Option<Vec<TableRefConfig>>,
    #[serde(default)]
    pub include_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_patterns: Option<Vec<String>>,
    pub parser: ParserConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableRefConfig {
    pub schema: String,
    pub table: String,
}

fn default_database() -> String { "default".into() }
fn default_username() -> String { "default".into() }
fn default_tls() -> bool { true }
fn default_rows_per_page() -> usize { 10000 }

pub struct ClickHouseSourceProvider {
    cfg: ClickHouseSourceConfig,
}

impl ClickHouseSourceProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: ClickHouseSourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse ClickHouse source config: {}", e))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("ch source: connection_string must not be empty");
        }
        // Validate: must have tables or include_patterns, not both
        match (&cfg.tables, &cfg.include_patterns) {
            (Some(_), Some(_)) => anyhow::bail!(
                "ch source: specify either 'tables' or 'include_patterns', not both"
            ),
            (None, None) => anyhow::bail!(
                "ch source: specify either 'tables' or 'include_patterns'"
            ),
            _ => {}
        }
        Ok(Self { cfg })
    }

    fn build_selection(&self) -> anyhow::Result<TableSelection> {
        if let Some(ref tables) = self.cfg.tables {
            let refs: Vec<TableRef> = tables.iter().map(|t| TableRef {
                schema_name: t.schema.clone(),
                table_name: t.table.clone(),
            }).collect();
            return Ok(TableSelection::Explicit(refs));
        }

        let includes: Vec<Regex> = self.cfg.include_patterns.as_ref()
            .map(|ps| ps.iter().map(|p| Regex::new(p).map_err(|e| anyhow::anyhow!("include regex '{}': {}", p, e))).collect())
            .unwrap_or_else(|| Ok(vec![]))?;

        let excludes: Vec<Regex> = self.cfg.exclude_patterns.as_ref()
            .map(|ps| ps.iter().map(|p| Regex::new(p).map_err(|e| anyhow::anyhow!("exclude regex '{}': {}", p, e))).collect())
            .unwrap_or_else(|| Ok(vec![]))?;

        Ok(TableSelection::Patterns { include_patterns: includes, exclude_patterns: excludes })
    }
}

impl SourceProvider for ClickHouseSourceProvider {
    fn build_source<'a>(
        &'a self,
        partition_id: i64,
        _cancel_token: CancellationToken,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn Source>>> {
        let conn = self.cfg.connection_string.clone();
        let db = self.cfg.database.clone();
        let user = self.cfg.username.clone();
        let pass = self.cfg.password.clone();
        let tls = self.cfg.use_tls;
        let tls_domain = self.cfg.tls_domain.clone();
        let rows = self.cfg.rows_per_page;
        let selection = match self.build_selection() {
            Ok(s) => s,
            Err(e) => return Box::pin(async { Err(e) }),
        };

        Box::pin(async move {
            let src = ClickHouseSource::new(
                &conn, &db, &user, &pass, tls, tls_domain.as_deref(),
                selection, partition_id, rows,
            ).await?;
            Ok(Box::new(src) as Box<dyn Source>)
        })
    }

    fn discover_partitions<'a>(
        &'a self,
        _total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'a, anyhow::Result<Vec<i64>>> {
        let parts = if worker_index == 0 { vec![0] } else { vec![] };
        Box::pin(async move { Ok(parts) })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.parser.resolve_table_name("clickhouse_source")
    }

    fn parser_config(&self) -> &ParserConfig {
        &self.cfg.parser
    }
}
