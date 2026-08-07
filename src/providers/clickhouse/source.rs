use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use regex::Regex;
use serde::Deserialize;

use crate::pipeline::source::{CommitMarker, ReadResult, Source};

/// `ClickHouse` source configuration — deserialised from YAML.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
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
    /// Table selection: oneof.
    #[serde(default)]
    pub tables: Option<Vec<TableRefConfig>>,
    #[serde(default)]
    pub include_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_patterns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct TableRefConfig {
    pub schema: String,
    pub table: String,
}

fn default_database() -> String { "default".into() }
fn default_username() -> String { "default".into() }
const fn default_tls() -> bool { true }
const fn default_rows_per_page() -> usize { 10000 }

impl ClickHouseSourceConfig {
    /// Validate and build a [`TableSelection`] from this config's table spec.
    pub fn build_selection(&self) -> anyhow::Result<TableSelection> {
        match (&self.tables, &self.include_patterns) {
            (Some(_), Some(_)) => anyhow::bail!(
                "ch source: specify either 'tables' or 'include_patterns', not both"
            ),
            (None, None) => anyhow::bail!(
                "ch source: specify either 'tables' or 'include_patterns'"
            ),
            _ => {}
        }
        if let Some(ref tables) = self.tables {
            let refs: Vec<TableRef> = tables.iter().map(|t| TableRef {
                schema_name: t.schema.clone(),
                table_name: t.table.clone(),
            }).collect();
            return Ok(TableSelection::Explicit(refs));
        }
        let includes: Vec<Regex> = self.include_patterns.as_ref()
            .map_or_else(|| Ok(vec![]), |ps| {
                ps.iter().map(|p| Regex::new(p).map_err(|e| anyhow::anyhow!("include regex '{p}': {e}"))).collect()
            })?;
        let excludes: Vec<Regex> = self.exclude_patterns.as_ref()
            .map_or_else(|| Ok(vec![]), |ps| {
                ps.iter().map(|p| Regex::new(p).map_err(|e| anyhow::anyhow!("exclude regex '{p}': {e}"))).collect()
            })?;
        Ok(TableSelection::Patterns { include_patterns: includes, exclude_patterns: excludes })
    }

    /// The first explicitly-configured table, if any; used for schema discovery.
    #[must_use]
    pub fn first_table_ref(&self) -> Option<TableRef> {
        self.tables.as_ref().and_then(|ts| ts.first()).map(|t| TableRef {
            schema_name: t.schema.clone(),
            table_name: t.table.clone(),
        })
    }
}

/// `ClickHouse` source: reads tables into Arrow batches and feeds them directly
/// into the pipeline (Arrow passthrough — no JSON serialization roundtrip).
///
/// Table selection via one of two variants:
/// - **Explicit list**: `tables: [{schema, table}]`
/// - **Regex patterns**: `include_patterns` (AND) then `exclude_patterns` (AND)
pub struct ClickHouseSource {
    pool: ConnectionPool<ArrowFormat>,
    #[expect(dead_code, reason = "stored for introspection / future table-refresh")]    selection: TableSelection,
    tables: Vec<TableRef>,
    current_table_idx: usize,
    current_page: usize,
    #[expect(dead_code, reason = "kept for API completeness")]
    partition_id: i64,
    rows_per_page: usize,
    exhausted: bool,
    _config: ClickHouseSourceConfig,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableRef {
    pub schema_name: String,
    pub table_name: String,
}

impl TableRef {
    #[must_use]
    pub const fn new(schema_name: String, table_name: String) -> Self {
        Self { schema_name, table_name }
    }

    #[must_use]
    pub fn qualified_name(&self) -> String {
        format!("`{}`.`{}`", self.schema_name, self.table_name)
    }
}

/// Table selection strategy.
#[non_exhaustive]
pub enum TableSelection {
    /// Explicit list of tables to transfer.
    Explicit(Vec<TableRef>),
    /// Regex-based include/exclude. Include patterns are AND-ed,
    /// then exclude patterns are AND-ed (a table must match ALL includes
    /// and NONE of the excludes).
    Patterns {
        include_patterns: Vec<Regex>,
        exclude_patterns: Vec<Regex>,
    },
}

impl ClickHouseSource {
    pub async fn new(
        config: ClickHouseSourceConfig,
        partition_id: i64,
    ) -> anyhow::Result<Self> {
        let selection = config.build_selection()?;

        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(&config.connection_string)
            .configure_pool(|p| p.max_size(2))
            .configure_client(|b| {
                let mut builder = b.with_database(&config.database)
                    .with_username(&config.username)
                    .with_password(&config.password)
                    .with_tls(config.use_tls);
                if let Some(ref domain) = config.tls_domain {
                    builder = builder.with_domain(domain);
                }
                builder
            })
            .build().await
            .map_err(|e| anyhow::anyhow!("CH source pool: {e}"))?;

        // Verify connectivity (scoped so client is dropped before discover)
        {
            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("CH source pool get: {e}"))?;
            client.execute("SELECT 1", None).await
                .map_err(|e| anyhow::anyhow!("CH source health check: {e}"))?;
        };

        let tables = match selection {
            TableSelection::Explicit(ref ts) => ts.clone(),
            TableSelection::Patterns { ref include_patterns, ref exclude_patterns } => {
                Self::discover_tables(&pool, &config.database, include_patterns, exclude_patterns).await?
            }
        };

        if tables.is_empty() {
            anyhow::bail!("ClickHouse source: no tables found");
        }

        tracing::info!(
            "CH source: {} tables, partition={}, rows_per_page={}",
            tables.len(), partition_id, config.rows_per_page,
        );

        Ok(Self {
            pool,
            selection,
            tables,
            current_table_idx: 0,
            current_page: 0,
            partition_id,
            rows_per_page: config.rows_per_page,
            exhausted: false,
            _config: config,
        })
    }

    /// Discover tables matching include/exclude patterns.
    async fn discover_tables(
        pool: &ConnectionPool<ArrowFormat>,
        database: &str,
        includes: &[Regex],
        excludes: &[Regex],
    ) -> anyhow::Result<Vec<TableRef>> {
        let client = pool.get().await
            .map_err(|e| anyhow::anyhow!("CH source discover: {e}"))?;

        let query = format!(
            "SELECT database, name FROM system.tables WHERE database = '{database}'",
        );
        let mut response = client.query(&query, None).await
            .map_err(|e| anyhow::anyhow!("CH source discover query: {e}"))?;

        let mut tables = Vec::new();
        while let Some(batch_result) = response.next().await {
            let batch: RecordBatch = batch_result
                .map_err(|e| anyhow::anyhow!("CH source discover batch: {e}"))?;

            let schema_col = batch.column(0);
            let name_col = batch.column(1);

            let schemas = schema_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected String column for schema"))?;
            let names = name_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected String column for name"))?;

            for row in 0..batch.num_rows() {
                let schema_name = schemas.value(row).to_string();
                let table_name = names.value(row).to_string();
                let full_name = format!("{schema_name}.{table_name}");

                // Apply to include patterns (AND): must match ALL
                let included = includes.is_empty() || includes.iter().all(|re| re.is_match(&full_name));
                if !included { continue; }

                // Apply to exclude patterns (AND): must match NONE
                let excluded = excludes.iter().any(|re| re.is_match(&full_name));
                if excluded { continue; }

                tables.push(TableRef { schema_name, table_name });
            }
        }

        Ok(tables)
    }

    /// Read a page of rows from the current table.
    async fn read_current_table_page(&self) -> anyhow::Result<Vec<RecordBatch>> {
        let table = &self.tables[self.current_table_idx];
        let offset = self.current_page * self.rows_per_page;
        let query = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            table.qualified_name(),
            self.rows_per_page,
            offset,
        );

        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("CH source read query: {e}"))?;

        let mut response = client.query(&query, None).await
            .map_err(|e| anyhow::anyhow!("CH source read: {e}"))?;

        let mut batches = Vec::new();
        while let Some(batch_result) = response.next().await {
            let batch: RecordBatch = batch_result
                .map_err(|e| anyhow::anyhow!("CH source read batch: {e}"))?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

impl Source for ClickHouseSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            if self.exhausted {
                return Ok(ReadResult::Exhausted);
            }

            loop {
                if self.current_table_idx >= self.tables.len() {
                    self.exhausted = true;
                    return Ok(ReadResult::Exhausted);
                }

                let batches = match self.read_current_table_page().await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(
                            "CH source: error reading {}: {}",
                            self.tables[self.current_table_idx].qualified_name(), e,
                        );
                        return Ok(ReadResult::Failed(e));
                    }
                };

                if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                    self.current_table_idx += 1;
                    self.current_page = 0;
                    continue;
                }

                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                self.current_page += 1;

                tracing::info!(
                    "CH source: read {} rows from {} (page {})",
                    total_rows,
                    self.tables[self.current_table_idx].qualified_name(),
                    self.current_page,
                );

                // Produce Arrow batches directly — zero-copy into the pipeline.
                // No JSON serialization/parsing roundtrip.
                return Ok(ReadResult::Arrow(batches));
            }
        })
    }

    fn commit_offsets<'ctx>(&'ctx mut self, _marker: &'ctx CommitMarker) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}

