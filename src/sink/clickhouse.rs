use async_trait::async_trait;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures::StreamExt;

use crate::config::yaml::SinkConfig;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

/// ClickHouse sink backed by a connection pool (`bb8`), providing concurrent
/// INSERTs from multiple partition tasks without head-of-line blocking.
///
/// `ArrowClient` is `Client<ArrowFormat>`, produced by `ConnectionPoolBuilder`.
pub struct ClickHouseSink {
    /// Pool of Arrow-format ClickHouse connections.
    pool: ConnectionPool<ArrowFormat>,
    /// Main target table name.
    table_name: String,
    /// Dead-letter queue table name.
    dlq_table_name: String,
}

impl ClickHouseSink {
    /// Create a new `ClickHouseSink` with a connection pool.
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

        // Startup validation — get one connection and run SELECT 1.
        // Pooled connection must be dropped before moving pool into Self.
        {
            let client = pool.get().await.map_err(|e| {
                anyhow::anyhow!("ClickHouse pool connection failed: {}", e)
            })?;
            client
                .execute("SELECT 1", None)
                .await
                .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {}", e))?;
        }

        tracing::info!(
            "Connected to ClickHouse at {} (pool size: {})",
            config.connection_string,
            config.max_connections,
        );

        Ok(Self {
            pool,
            table_name: config.table_name.clone(),
            dlq_table_name: config.dlq_table_name.clone(),
        })
    }

    /// Verify that target and DLQ tables exist at startup.
    pub async fn verify_tables(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await.map_err(|e| {
            anyhow::anyhow!("ClickHouse pool get for table verification: {}", e)
        })?;

        for table in [&self.table_name, &self.dlq_table_name] {
            client
                .execute(&format!("EXISTS TABLE {}", table), None)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Table '{}' not found or inaccessible: {}",
                        table,
                        e
                    )
                })?;
            tracing::info!("Table '{}' verified", table);
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        let table = if batch.meta.dlq_flag {
            &self.dlq_table_name
        } else {
            &self.table_name
        };

        let query = format!("INSERT INTO {} VALUES", table);

        // Get a client from the pool — this will wait if all connections are busy.
        let client = self.pool.get().await.map_err(|e| {
            anyhow::anyhow!("ClickHouse pool get for insert: {}", e)
        })?;

        let mut stream = client
            .insert(&query, batch.batch.clone(), None)
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse insert into '{}' failed: {}", table, e))?;

        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow::anyhow!("ClickHouse insert stream error: {}", e))?;
        }

        tracing::debug!(
            "Inserted {} rows into {} (batch_id={}, dlq={})",
            batch.batch.num_rows(),
            table,
            batch.meta.batch_id,
            batch.meta.dlq_flag,
        );

        Ok(())
    }
}
