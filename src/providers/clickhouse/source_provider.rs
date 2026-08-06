use alloc::sync::Arc;
use std::sync::OnceLock;

use arrow::array::StringArray;
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use regex::Regex;
use serde::Deserialize;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{ColumnDef, ColumnMapping, SchemaConfig};
use crate::pipeline::source::Source;
use crate::providers::clickhouse::source::{ClickHouseSource, TableRef, TableSelection};
use crate::providers::traits::SourceProvider;

#[derive(Debug, Deserialize)]
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

pub struct ClickHouseSourceProvider {
    cfg: ClickHouseSourceConfig,
    /// Column schema derived from the source table via DESCRIBE TABLE.
    /// Populated once during `discover_partitions`, used for DDL.
    derived_schema: Arc<OnceLock<SchemaConfig>>,
}

impl ClickHouseSourceProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: ClickHouseSourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse ClickHouse source config: {e}"))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("ch source: connection_string must not be empty");
        }
        match (&cfg.tables, &cfg.include_patterns) {
            (&Some(_), &Some(_)) => anyhow::bail!(
                "ch source: specify either 'tables' or 'include_patterns', not both"
            ),
            (&None, &None) => anyhow::bail!(
                "ch source: specify either 'tables' or 'include_patterns'"
            ),
            _ => {}
        }
        Ok(Self { cfg, derived_schema: Arc::new(OnceLock::new()) })
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
            .map_or_else(|| Ok(vec![]), |ps| {
                ps.iter().map(|p| Regex::new(p).map_err(|e| anyhow::anyhow!("include regex '{p}': {e}"))).collect()
            })?;
        let excludes: Vec<Regex> = self.cfg.exclude_patterns.as_ref()
            .map_or_else(|| Ok(vec![]), |ps| {
                ps.iter().map(|p| Regex::new(p).map_err(|e| anyhow::anyhow!("exclude regex '{p}': {e}"))).collect()
            })?;
        Ok(TableSelection::Patterns { include_patterns: includes, exclude_patterns: excludes })
    }

    fn first_table_ref(&self) -> Option<TableRef> {
        self.cfg.tables.as_ref().and_then(|ts| ts.first()).map(|t| TableRef {
            schema_name: t.schema.clone(),
            table_name: t.table.clone(),
        })
    }

    /// Connect to the source `ClickHouse`, run DESCRIBE TABLE, and build a
    /// [`SchemaConfig`] with the real column names and types.
    async fn derive_schema(
        connection_string: &str,
        database: &str,
        username: &str,
        password: &str,
        use_tls: bool,
        tls_domain: Option<&str>,
        table_ref: &TableRef,
    ) -> anyhow::Result<SchemaConfig> {
        use clickhouse_arrow::{ArrowFormat, ConnectionPoolBuilder};

        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(connection_string)
            .configure_pool(|p| p.max_size(1))
            .configure_client(|b| {
                let mut builder = b.with_database(database)
                    .with_username(username)
                    .with_password(password)
                    .with_tls(use_tls);
                if let Some(domain) = tls_domain {
                    builder = builder.with_domain(domain);
                }
                builder
            })
            .build().await
            .map_err(|e| anyhow::anyhow!("CH source schema discovery pool: {e}"))?;

        let client = pool.get().await
            .map_err(|e| anyhow::anyhow!("CH source schema discovery: {e}"))?;

        let q = format!("DESCRIBE TABLE {}", table_ref.qualified_name());
        let mut response = client.query(&q, None).await
            .map_err(|e| anyhow::anyhow!("CH source DESCRIBE {}: {}", table_ref.qualified_name(), e))?;

        let mut columns = Vec::new();
        while let Some(batch_result) = response.next().await {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("CH source DESCRIBE batch: {e}"))?;
            let name_col = batch.column(0);
            let type_col = batch.column(1);
            let names = name_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected String for column name"))?;
            let types = type_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected String for column type"))?;
            for row in 0..batch.num_rows() {
                let col_name = names.value(row).to_string();
                let ch_type = types.value(row).to_string();
                let arrow_type = ch_type_to_arrow(&ch_type);
                columns.push(ColumnDef {
                    column_name: col_name,
                    arrow_type,
                    nullable: ch_type.to_lowercase().starts_with("nullable"),
                });
            }
        }

        if columns.is_empty() {
            anyhow::bail!(
                "CH source: DESCRIBE TABLE {} returned zero columns",
                table_ref.qualified_name(),
            );
        }

        let mappings: Vec<ColumnMapping> = columns.into_iter().map(ColumnMapping::from).collect();
        Ok(SchemaConfig {
            columns: mappings,
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: crate::config::yaml::ChunkSplitter::NoSplit,
            skip_null_columns: false,
        })
    }
}

/// Map a `ClickHouse` type string to an Arrow type string compatible with
/// `parse_arrow_type`.
fn ch_type_to_arrow(ch: &str) -> String {
    let base = ch.trim_start_matches("Nullable(").trim_end_matches(')').to_lowercase();
    match base.as_str() {
        "string" | "utf8" => "Utf8".into(),
        "int8" => "Int8".into(),
        "int16" => "Int16".into(),
        "int32" | "int" => "Int32".into(),
        "int64" | "bigint" => "Int64".into(),
        "uint8" => "UInt8".into(),
        "uint16" => "UInt16".into(),
        "uint32" => "UInt32".into(),
        "uint64" => "UInt64".into(),
        "float32" | "float" => "Float32".into(),
        "float64" | "double" => "Float64".into(),
        "bool" | "boolean" => "Boolean".into(),
        "date" | "date32" => "Date32".into(),
        "datetime" | "datetime64" | "date64" => "Timestamp(Second, None)".into(),
        "datetime64(3)" => "Timestamp(Millisecond, None)".into(),
        "datetime64(6)" => "Timestamp(Microsecond, None)".into(),
        "datetime64(9)" => "Timestamp(Nanosecond, None)".into(),
        other => {
            tracing::warn!("CH type '{}' \u{2192} falling back to Utf8", other);
            "Utf8".into()
        }
    }
}

impl SourceProvider for ClickHouseSourceProvider {
    fn build_source(
        &self,
        partition_id: i64,
        _cancel_token: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
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

    fn discover_partitions(
        &self,
        _total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        let need_init = self.derived_schema.get().is_none();
        let conn = self.cfg.connection_string.clone();
        let db = self.cfg.database.clone();
        let user = self.cfg.username.clone();
        let pass = self.cfg.password.clone();
        let tls = self.cfg.use_tls;
        let tls_domain = self.cfg.tls_domain.clone();
        let table_ref = self.first_table_ref();
        let derived = Arc::clone(&self.derived_schema);

        Box::pin(async move {
            if need_init {
                if let Some(ref tr) = table_ref {
                    match Self::derive_schema(
                        &conn, &db, &user, &pass, tls,
                        tls_domain.as_deref(), tr,
                    ).await {
                        Ok(schema) => {
                            tracing::info!(
                                "CH source: derived schema for '{}' ({} columns)",
                                tr.qualified_name(), schema.columns.len(),
                            );
                            if derived.set(schema).is_err() {
                                tracing::debug!("CH source: schema already derived concurrently; keeping existing");
                            }
                        }
                        Err(e) => {
                            tracing::error!("CH source schema derivation failed: {}", e);
                        }
                    }
                }
            }
            let parts = if worker_index == 0 { vec![0] } else { vec![] };
            Ok(parts)
        })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.tables.as_ref()
            .and_then(|ts| ts.first())
            .map(|t| format!("{}.{}", t.schema, t.table))
            .ok_or_else(|| anyhow::anyhow!("ch source: no tables configured"))
    }

    fn parser_config(&self) -> Option<&crate::config::yaml::ParserConfig> {
        // CH source uses Arrow passthrough — no parser.
        None
    }

    fn schema_config(&self) -> Option<&SchemaConfig> {
        self.derived_schema.get()
    }
}