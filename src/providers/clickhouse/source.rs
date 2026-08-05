use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use regex::Regex;

use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::message::{Message, MessageBatch};

/// ClickHouse source: reads tables into Arrow batches and produces JSON messages.
///
/// Table selection via one of two variants:
/// - **Explicit list**: `tables: [{schema, table}]`
/// - **Regex patterns**: `include_patterns` (AND) then `exclude_patterns` (AND)
pub struct ClickHouseSource {
    pool: ConnectionPool<ArrowFormat>,
    tables: Vec<TableRef>,
    current_table_idx: usize,
    current_page: usize,
    partition_id: i64,
    rows_per_page: usize,
    exhausted: bool,
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub schema_name: String,
    pub table_name: String,
}

impl TableRef {
    pub fn qualified_name(&self) -> String {
        format!("`{}`.`{}`", self.schema_name, self.table_name)
    }
}

/// Table selection strategy.
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
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        connection_string: &str,
        database: &str,
        username: &str,
        password: &str,
        use_tls: bool,
        tls_domain: Option<&str>,
        selection: TableSelection,
        partition_id: i64,
        rows_per_page: usize,
    ) -> anyhow::Result<Self> {
        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(connection_string)
            .configure_pool(|p| p.max_size(2))
            .configure_client(|b| {
                let mut b = b.with_database(database)
                    .with_username(username)
                    .with_password(password)
                    .with_tls(use_tls);
                if let Some(domain) = tls_domain {
                    b = b.with_domain(domain);
                }
                b
            })
            .build().await
            .map_err(|e| anyhow::anyhow!("CH source pool: {}", e))?;

        // Verify connectivity (scoped so client is dropped before discover)
        {
            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("CH source pool get: {}", e))?;
            client.execute("SELECT 1", None).await
                .map_err(|e| anyhow::anyhow!("CH source health check: {}", e))?;
        }

        let tables = match selection {
            TableSelection::Explicit(ts) => ts,
            TableSelection::Patterns { include_patterns, exclude_patterns } => {
                Self::discover_tables(&pool, database, &include_patterns, &exclude_patterns).await?
            }
        };

        if tables.is_empty() {
            anyhow::bail!("ClickHouse source: no tables found");
        }

        tracing::info!(
            "CH source: {} tables, partition={}, rows_per_page={}",
            tables.len(), partition_id, rows_per_page,
        );

        Ok(Self {
            pool, tables, current_table_idx: 0, current_page: 0,
            partition_id, rows_per_page, exhausted: false,
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
            .map_err(|e| anyhow::anyhow!("CH source discover: {}", e))?;

        let query = format!(
            "SELECT database, name FROM system.tables WHERE database = '{}'",
            database,
        );
        let mut response = client.query(&query, None).await
            .map_err(|e| anyhow::anyhow!("CH source discover query: {}", e))?;

        let mut tables = Vec::new();
        while let Some(batch_result) = response.next().await {
            let batch: RecordBatch = batch_result
                .map_err(|e| anyhow::anyhow!("CH source discover batch: {}", e))?;

            let schema_col = batch.column(0);
            let name_col = batch.column(1);

            use arrow::array::StringArray;
            let schemas = schema_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected String column for schema"))?;
            let names = name_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected String column for name"))?;

            for row in 0..batch.num_rows() {
                let schema_name = schemas.value(row).to_string();
                let table_name = names.value(row).to_string();
                let full_name = format!("{}.{}", schema_name, table_name);

                // Apply include patterns (AND): must match ALL
                let included = includes.is_empty() || includes.iter().all(|re| re.is_match(&full_name));
                if !included { continue; }

                // Apply exclude patterns (AND): must match NONE
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
            .map_err(|e| anyhow::anyhow!("CH source read query: {}", e))?;

        let mut response = client.query(&query, None).await
            .map_err(|e| anyhow::anyhow!("CH source read: {}", e))?;

        let mut batches = Vec::new();
        while let Some(batch_result) = response.next().await {
            let batch: RecordBatch = batch_result
                .map_err(|e| anyhow::anyhow!("CH source read batch: {}", e))?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

impl Source for ClickHouseSource {
    fn read_batch<'a>(&'a mut self) -> BoxFuture<'a, anyhow::Result<ReadResult>> {
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

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.current_page += 1;

                let messages = batches_to_messages(&batches)?;

                tracing::info!(
                    "CH source: read {} rows from {} (page {})",
                    total_rows,
                    self.tables[self.current_table_idx].qualified_name(),
                    self.current_page,
                );

                return Ok(ReadResult::Batch(MessageBatch {
                    messages,
                    partition_id: self.partition_id,
                    commit_marker: None,
                    dedup_token: None,
                }));
            }
        })
    }

    fn commit_offsets<'a>(&'a mut self, _marker: &'a CommitMarker) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}

/// Convert Arrow RecordBatches to JSON-line messages compatible with the JSON parser.
fn batches_to_messages(batches: &[RecordBatch]) -> anyhow::Result<Vec<Message>> {
    use crate::serializer::Serializer;
    use crate::serializer::json_serializer::JsonSerializer;
    use bytes::Bytes;

    let serializer = JsonSerializer;
    let mut all_bytes = Vec::new();
    for batch in batches {
        let serialized = serializer.serialize_batch(batch)?;
        all_bytes.extend_from_slice(&serialized);
    }

    Ok(vec![Message { value: Bytes::from(all_bytes) }])
}
