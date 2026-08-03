use std::future::Future;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::StreamExt;

use crate::config::yaml::SinkConfig;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

pub struct ClickHouseSink {
    pool: ConnectionPool<ArrowFormat>,
    insert_main: String,
    insert_dlq: String,
    table_name: String,
    dlq_table_name: String,
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
        Ok(Self {
            pool,
            insert_main: format!("INSERT INTO {} VALUES", config.table_name),
            insert_dlq: format!("INSERT INTO {} VALUES", config.dlq_table_name),
            table_name: config.table_name.clone(),
            dlq_table_name: config.dlq_table_name.clone(),
        })
    }

    pub async fn verify_tables(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for verify: {}", e))?;
        for table in [&self.table_name, &self.dlq_table_name] {
            client.execute(&format!("EXISTS TABLE {}", table), None).await
                .map_err(|e| anyhow::anyhow!("Table '{}' not found: {}", table, e))?;
            tracing::info!("Table '{}' verified", table);
        }
        Ok(())
    }
}

impl Sink for ClickHouseSink {
    fn write_batch(&self, batch: &ArrowBatch) -> impl Future<Output = anyhow::Result<()>> + Send {
        async fn do_write(slf: &ClickHouseSink, batch: &ArrowBatch) -> anyhow::Result<()> {
            let query = if batch.meta.dlq_flag { &slf.insert_dlq } else { &slf.insert_main };
            let client = slf.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;
            let mut stream = client.insert(query, batch.batch.clone(), None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse insert failed: {}", e))?;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("ClickHouse insert stream error: {}", e))?;
            }
            tracing::debug!("Inserted {} rows (batch_id={})", batch.batch.num_rows(), batch.meta.batch_id);
            Ok(())
        }
        do_write(self, batch)
    }

    fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        dlq_flag: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async fn do_write_many(slf: &ClickHouseSink, batches: Vec<RecordBatch>, dlq_flag: bool) -> anyhow::Result<()> {
            if batches.is_empty() { return Ok(()); }
            let query = if dlq_flag { &slf.insert_dlq } else { &slf.insert_main };
            let client = slf.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            let n = batches.len();
            let mut stream = client.insert_many(query, batches, None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse insert_many failed: {}", e))?;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {}", e))?;
            }
            tracing::debug!("Inserted {} rows via insert_many ({} blocks)", total, n);
            Ok(())
        }
        do_write_many(self, batches, dlq_flag)
    }
}
