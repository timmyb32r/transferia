use std::future::Future;

use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::StreamExt;

use crate::config::yaml::{parse_arrow_type, SchemaConfig};
use crate::pipeline::sink::Sink;
use crate::types::table_data::TableWrite;

/// Map an Arrow `DataType` to the equivalent ClickHouse column type.
fn arrow_to_clickhouse(dt: &DataType) -> anyhow::Result<String> {
    Ok(match dt {
        DataType::Utf8 | DataType::LargeUtf8 => "String".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Boolean => "Bool".into(),
        DataType::Date32 => "Date32".into(),
        DataType::Date64 => "DateTime64(3)".into(),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => "DateTime".into(),
            TimeUnit::Millisecond => "DateTime64(3)".into(),
            TimeUnit::Microsecond => "DateTime64(6)".into(),
            TimeUnit::Nanosecond => "DateTime64(9)".into(),
        },
        other => anyhow::bail!("No ClickHouse type mapping for Arrow type {:?}", other),
    })
}

pub struct ClickHouseSink {
    pool: ConnectionPool<ArrowFormat>,
}

impl ClickHouseSink {
    pub async fn new(config: &crate::config::yaml::SinkConfig) -> anyhow::Result<Self> {
        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(config.connection_string.as_str())
            .configure_pool(|p| p.max_size(config.max_connections as u32))
            .configure_client(|b| {
                let mut b = b.with_database(config.database.as_str())
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_tls(config.use_tls);
                if let Some(ref domain) = config.tls_domain {
                    b = b.with_domain(domain.as_str());
                }
                b
            })
            .build().await
            .map_err(|e| anyhow::anyhow!("Failed to build ClickHouse pool: {}", e))?;
        {
            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool connection failed: {}", e))?;
            client.execute("SELECT 1", None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {}", e))?;
        }
        tracing::info!("Connected to ClickHouse at {} (pool: {})", config.connection_string, config.max_connections);
        Ok(Self { pool })
    }

    /// Build `(column_name, clickhouse_type)` pairs from a `SchemaConfig`.
    /// Nullable columns are wrapped in `Nullable(...)`. Public so `main.rs`
    /// can pass the result to `create_table` for both main and DLQ tables.
    pub fn schema_columns(settings: &SchemaConfig) -> anyhow::Result<Vec<(String, String)>> {
        settings.columns.iter().map(|c| {
            let dt = parse_arrow_type(&c.arrow_type)?;
            let mut ty = arrow_to_clickhouse(&dt)?;
            if c.nullable {
                ty = format!("Nullable({})", ty);
            }
            Ok((c.column_name.clone(), ty))
        }).collect()
    }

    /// Generic `CREATE TABLE IF NOT EXISTS`. The caller provides the fully-resolved
    /// table name, column definitions, and optional `ORDER BY` clause.
    ///
    /// When `recreate` is true (opt-in via config, off by default), the existing
    /// table is dropped first — useful in dev/bench so schema changes take effect.
    /// NEVER enable in production: existing data IS LOST.
    pub async fn create_table(
        &self,
        name: &str,
        columns: &[(String, String)],
        order_by: &[String],
        recreate: bool,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for create_table: {}", e))?;

        if recreate {
            tracing::warn!("RECREATE_TABLES set — dropping table '{}'", name);
            client.execute(&format!("DROP TABLE IF EXISTS `{}`", name), None).await
                .map_err(|e| anyhow::anyhow!("Failed to drop table '{}': {}", name, e))?;
        }

        let cols = columns.iter()
            .map(|(col, ty)| format!("`{}` {}", col, ty))
            .collect::<Vec<_>>()
            .join(", ");
        let order_clause = if order_by.is_empty() {
            "tuple()".to_string()
        } else {
            order_by.iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{}` ({}) ENGINE = MergeTree ORDER BY ({})",
            name, cols, order_clause,
        );
        client.execute(&ddl, None).await
            .map_err(|e| anyhow::anyhow!("Failed to create table '{}': {}", name, e))?;
        tracing::info!("Ensured table '{}'", name);
        Ok(())
    }

    /// Verify a table exists by running `DESCRIBE TABLE`.
    pub async fn verify_table(&self, name: &str) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for verify: {}", e))?;
        client.execute(&format!("DESCRIBE TABLE `{}`", name), None).await
            .map_err(|e| anyhow::anyhow!("Table '{}' not found: {}", name, e))?;
        tracing::info!("Table '{}' verified", name);
        Ok(())
    }
}

impl Sink for ClickHouseSink {
    fn write(
        &self,
        write: TableWrite,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async fn do_write(slf: &ClickHouseSink, write: TableWrite) -> anyhow::Result<()> {
            if write.batches.is_empty() { return Ok(()); }
            let query = format!("INSERT INTO `{}` VALUES", write.table);
            let client = slf.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;
            let total: usize = write.batches.iter().map(|b| b.num_rows()).sum();
            let n = write.batches.len();
            let mut stream = client.insert_many(&query, write.batches, None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse insert_many failed: {}", e))?;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {}", e))?;
            }
            tracing::info!("Inserted {} rows via insert_many ({} blocks) into '{}'", total, n, write.table);
            Ok(())
        }
        do_write(self, write)
    }
}
