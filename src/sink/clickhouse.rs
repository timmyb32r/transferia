use async_trait::async_trait;
use futures::StreamExt;

use crate::config::yaml::SinkConfig;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

/// ClickHouse sink that writes Arrow record batches via the `clickhouse-arrow`
/// crate (ClickHouse Native Protocol, port 9000).
///
/// `ArrowClient` is `Client<ArrowFormat>`, produced by `build_arrow()`.
pub struct ClickHouseSink {
    /// The underlying ClickHouse client (Arrow Flight format).
    client: clickhouse_arrow::ArrowClient,
    /// Main target table name.
    table_name: String,
    /// Dead-letter queue table name.
    dlq_table_name: String,
}

impl ClickHouseSink {
    /// Create a new `ClickHouseSink` from the sink configuration.
    ///
    /// Connects to ClickHouse via the native protocol and runs a `SELECT 1`
    /// health check to validate the connection at startup.
    pub async fn new(config: &SinkConfig) -> anyhow::Result<Self> {
        // The clickhouse-arrow builder uses self-consuming `with_*` methods,
        // so calls must be chained (not on separate lines via `&mut self`).
        let client = clickhouse_arrow::ArrowClient::builder()
            .with_endpoint(config.connection_string.as_str())
            .with_database(config.database.as_str())
            .with_username(config.username.as_str())
            .with_password(config.password.as_str())
            .build_arrow()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to build ClickHouse Arrow client: {}", e))?;

        // Startup validation — execute() discards result data.
        client
            .execute("SELECT 1", None)
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {}", e))?;

        tracing::info!("Connected to ClickHouse at {}", config.connection_string);

        Ok(Self {
            client,
            table_name: config.table_name.clone(),
            dlq_table_name: config.dlq_table_name.clone(),
        })
    }

    /// Verify that the target and DLQ tables exist at startup.
    ///
    /// Both tables must exist before the pipeline starts; otherwise the sink
    /// will refuse to operate.
    pub async fn verify_tables(&self) -> anyhow::Result<()> {
        for table in [&self.table_name, &self.dlq_table_name] {
            self.client
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
    /// Write an Arrow batch to the appropriate ClickHouse table.
    ///
    /// Routes to the DLQ table when `batch.meta.dlq_flag` is `true`, otherwise
    /// to the main table.  Uses the `clickhouse-arrow` `insert()` method which
    /// returns a `Stream<Item = Result<()>>`; the stream is fully consumed to
    /// drive the insert to completion.
    async fn write_batch(&self, batch: &ArrowBatch) -> anyhow::Result<()> {
        let table = if batch.meta.dlq_flag {
            &self.dlq_table_name
        } else {
            &self.table_name
        };

        let query = format!("INSERT INTO {} VALUES", table);

        // clickhouse-arrow insert() returns a Stream that must be consumed.
        let mut stream = self
            .client
            .insert(&query, batch.batch.clone(), None)
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse insert into '{}' failed: {}", table, e))?;

        // Consume the stream — each item is Result<()>.
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
