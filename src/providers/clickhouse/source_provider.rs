use alloc::sync::Arc;
use std::sync::OnceLock;

use arrow::array::StringArray;
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{ColumnDef, ColumnMapping};
use crate::config::yaml::SchemaConfig;
use crate::pipeline::source::Source;
use crate::providers::clickhouse::source::{ClickHouseSource, ClickHouseSourceConfig, TableRef};
use crate::providers::traits::SourceProvider;

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
        // Validation now delegates to cfg.build_selection() — just check it parses.
        cfg.build_selection()?;
        Ok(Self { cfg, derived_schema: Arc::new(OnceLock::new()) })
    }

    /// Connect to the source `ClickHouse`, run DESCRIBE TABLE, and build a
    /// [`SchemaConfig`] with the real column names and types.
    async fn derive_schema(
        cfg: &ClickHouseSourceConfig,
        table_ref: &TableRef,
    ) -> anyhow::Result<SchemaConfig> {
        use clickhouse_arrow::{ArrowFormat, ConnectionPoolBuilder};

        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(&cfg.connection_string)
            .configure_pool(|p| p.max_size(1))
            .configure_client(|b| {
                let mut builder = b.with_database(&cfg.database)
                    .with_username(&cfg.username)
                    .with_password(&cfg.password)
                    .with_tls(cfg.use_tls);
                if let Some(ref domain) = cfg.tls_domain {
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
            order_by: vec![],
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
        let cfg = self.cfg.clone();
        Box::pin(async move {
            let src = ClickHouseSource::new(cfg, partition_id).await?;
            Ok(Box::new(src) as Box<dyn Source>)
        })
    }

    fn discover_partitions(
        &self,
        _total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        let need_init = self.derived_schema.get().is_none();
        let cfg = self.cfg.clone();
        let table_ref = self.cfg.first_table_ref();
        let derived = Arc::clone(&self.derived_schema);

        Box::pin(async move {
            if need_init {
                if let Some(ref tr) = table_ref {
                    match Self::derive_schema(&cfg, tr).await {
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