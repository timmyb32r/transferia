use std::future::Future;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::StreamExt;

use crate::config::yaml::{parse_arrow_type, SchemaConfig, SinkConfig};
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

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

/// Resolve the concrete target table name: `<table>` or `<table>.dlq`.
#[inline]
fn target_table(table: &str, dlq: bool) -> String {
    if dlq { format!("{table}.dlq") } else { table.to_string() }
}

pub struct ClickHouseSink {
    pool: ConnectionPool<ArrowFormat>,
}

impl ClickHouseSink {
    pub async fn new(config: &SinkConfig) -> anyhow::Result<Self> {
        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(config.connection_string.as_str())
            .configure_pool(|p| p.max_size(config.max_connections as u32))
            .configure_client(|b| {
                b.with_database(config.database.as_str())
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_tls(true)
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

    /// Create the main (`table`) and DLQ (`table.dlq`) tables if they don't exist.
    ///
    /// The main schema is derived from `settings.columns` (name + arrow_type, wrapped in
    /// `Nullable(...)` for nullable columns). The DLQ schema is fixed and MUST stay in sync
    /// with `parser::json_parser::DLQ_SCHEMA` (raw_bytes, error_message, partition_id, timestamp).
    ///
    /// When `recreate` is true (opt-in via config, off by default), existing tables are
    /// dropped first — useful in dev/bench so schema changes take effect. NEVER enable
    /// in production: existing data IS LOST.
    pub async fn create_tables(
        &self, settings: &SchemaConfig, table: &str, recreate: bool,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for create_tables: {}", e))?;

        let main = target_table(table, false);
        let dlq = target_table(table, true);

        if recreate {
            for t in [&main, &dlq] {
                tracing::warn!("RECREATE_TABLES set — dropping table '{}'", t);
                client.execute(&format!("DROP TABLE IF EXISTS `{}`", t), None).await
                    .map_err(|e| anyhow::anyhow!("Failed to drop table '{}': {}", t, e))?;
            }
        }

        let mut cols = Vec::with_capacity(settings.columns.len());
        for c in &settings.columns {
            let dt = parse_arrow_type(&c.arrow_type)?;
            let mut ty = arrow_to_clickhouse(&dt)?;
            if c.nullable {
                ty = format!("Nullable({})", ty);
            }
            cols.push(format!("`{}` {}", c.column_name, ty));
        }
        let order_clause = if settings.order_by.is_empty() {
            "tuple()".to_string()
        } else {
            settings.order_by.iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let main_ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{}` ({}) ENGINE = MergeTree ORDER BY ({})",
            main, cols.join(", "), order_clause,
        );
        client.execute(&main_ddl, None).await
            .map_err(|e| anyhow::anyhow!("Failed to create table '{}': {}", main, e))?;
        tracing::info!("Ensured table '{}'", main);

        // DLQ schema is fixed — see parser::json_parser::DLQ_SCHEMA.
        let dlq_ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (\
                `raw_bytes` String, `error_message` String, \
                `partition_id` Int64, `timestamp` String\
            ) ENGINE = MergeTree ORDER BY tuple()",
            dlq,
        );
        client.execute(&dlq_ddl, None).await
            .map_err(|e| anyhow::anyhow!("Failed to create table '{}': {}", dlq, e))?;
        tracing::info!("Ensured table '{}'", dlq);

        Ok(())
    }

    pub async fn verify_tables(&self, table: &str) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for verify: {}", e))?;
        // NOTE: `EXISTS TABLE` does NOT error on a missing table — it returns 0/1, so it
        // can't be used for verification via execute(). `DESCRIBE TABLE` raises
        // UnknownTable server-side when the table is absent, which surfaces as an error here.
        for t in [target_table(table, false), target_table(table, true)] {
            client.execute(&format!("DESCRIBE TABLE `{}`", t), None).await
                .map_err(|e| anyhow::anyhow!("Table '{}' not found: {}", t, e))?;
            tracing::info!("Table '{}' verified", t);
        }
        Ok(())
    }
}

impl Sink for ClickHouseSink {
    fn write_batch(&self, batch: &ArrowBatch) -> impl Future<Output = anyhow::Result<()>> + Send {
        async fn do_write(slf: &ClickHouseSink, batch: &ArrowBatch) -> anyhow::Result<()> {
            let query = format!("INSERT INTO `{}` VALUES", target_table(&batch.meta.table, batch.meta.dlq_flag));
            let client = slf.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;
            let mut stream = client.insert(&query, batch.batch.clone(), None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse insert failed: {}", e))?;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("ClickHouse insert stream error: {}", e))?;
            }
            tracing::info!("Inserted {} rows (batch_id={})", batch.batch.num_rows(), batch.meta.batch_id);
            Ok(())
        }
        do_write(self, batch)
    }

    fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        table: Arc<str>,
        dlq_flag: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async fn do_write_many(slf: &ClickHouseSink, batches: Vec<RecordBatch>, table: Arc<str>, dlq_flag: bool) -> anyhow::Result<()> {
            if batches.is_empty() { return Ok(()); }
            let query = format!("INSERT INTO `{}` VALUES", target_table(&table, dlq_flag));
            let client = slf.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            let n = batches.len();
            let mut stream = client.insert_many(&query, batches, None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse insert_many failed: {}", e))?;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {}", e))?;
            }
            tracing::info!("Inserted {} rows via insert_many ({} blocks)", total, n);
            Ok(())
        }
        do_write_many(self, batches, table, dlq_flag)
    }
}
