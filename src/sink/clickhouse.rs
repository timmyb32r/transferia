use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures::StreamExt;

use crate::config::yaml::SinkConfig;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

/// ClickHouse sink backed by a connection pool (`bb8`).
pub struct ClickHouseSink {
    pool: ConnectionPool<ArrowFormat>,
    /// Precomputed INSERT query for main table.
    insert_main: String,
    /// Precomputed INSERT query for DLQ table.
    insert_dlq: String,
    /// Main table name (for verification).
    table_name: String,
    /// DLQ table name (for verification).
    dlq_table_name: String,
}

impl ClickHouseSink {
    pub async fn new(config: &SinkConfig) -> anyhow::Result<Self> {
        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(
            config.connection_string.as_str(),
        )
        .configure_pool(|p| p.max_size(config.max_connections as u32))
        .configure_client(|b| {
            b.with_database(config.database.as_str())
                .with_username(config.username.as_str())
                .with_password(config.password.as_str())
        })
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build ClickHouse connection pool: {}", e))?;

        {
            let client = pool.get().await.map_err(|e| {
                anyhow::anyhow!("ClickHouse pool connection failed: {}", e)
            })?;
            client
                .execute("SELECT 1", None)
                .await
                .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {}", e))?;
        }

        // Precompute INSERT queries — avoids format! per write
        let insert_main = format!("INSERT INTO {} VALUES", config.table_name);
        let insert_dlq = format!("INSERT INTO {} VALUES", config.dlq_table_name);

        tracing::info!(
            "Connected to ClickHouse at {} (pool size: {})",
            config.connection_string,
            config.max_connections,
        );

        Ok(Self {
            pool, insert_main, insert_dlq,
            table_name: config.table_name.clone(),
            dlq_table_name: config.dlq_table_name.clone(),
        })
    }

    pub async fn verify_tables(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await.map_err(|e| {
            anyhow::anyhow!("ClickHouse pool get for table verification: {}", e)
        })?;
        for table in [&self.table_name, &self.dlq_table_name] {
            client
                .execute(&format!("EXISTS TABLE {}", table), None)
                .await
                .map_err(|e| anyhow::anyhow!("Table '{}' not found: {}", table, e))?;
            tracing::info!("Table '{}' verified", table);
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        let query = if batch.meta.dlq_flag { &self.insert_dlq } else { &self.insert_main };

        let client = self.pool.get().await.map_err(|e| {
            anyhow::anyhow!("ClickHouse pool get for insert: {}", e)
        })?;

        let mut stream = client
            .insert(query, batch.batch.clone(), None)
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse insert failed: {}", e))?;

        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow::anyhow!("ClickHouse insert stream error: {}", e))?;
        }

        tracing::debug!(
            "Inserted {} rows (batch_id={}, dlq={})",
            batch.batch.num_rows(),
            batch.meta.batch_id,
            batch.meta.dlq_flag,
        );

        Ok(())
    }

    /// Write multiple RecordBatches in a single INSERT — no `concat_batches` copy.
    async fn write_batches(&self, batches: &[RecordBatch], dlq_flag: bool) -> anyhow::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let query = if dlq_flag { &self.insert_dlq } else { &self.insert_main };

        let client = self.pool.get().await.map_err(|e| {
            anyhow::anyhow!("ClickHouse pool get for insert_many: {}", e)
        })?;

        // clickhouse-arrow insert_many sends all blocks in one query — no client-side concat
        let mut stream = client
            .insert_many(query, batches.to_vec(), None)
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse insert_many failed: {}", e))?;

        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many stream error: {}", e))?;
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::debug!("Inserted {} rows via insert_many ({} blocks, dlq={})", total_rows, batches.len(), dlq_flag);

        Ok(())
    }
}
