use futures_util::future::BoxFuture;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};

use crate::pipeline::sink::Sink;
use crate::types::table_data::TableWrite;

/// Parallel ClickHouse insert sink: fans out writes across N independent
/// ClickHouse connections for higher throughput.
///
/// **Weakened guarantee**: assumes all keys in the incoming stream are unique,
/// so parallel out-of-order inserts don't cause consistency issues.
///
/// Not a `Middleware` in the traditional sense — this is a **Sink decorator**
/// that wraps the N connection pools and dispatches writes round-robin across
/// them. The middleware module hosts it because it's an optional pipeline
/// component that sits between the accumulator and the actual ClickHouse I/O.
pub struct ParallelChInsertSink {
    pools: Vec<ConnectionPool<ArrowFormat>>,
    next: std::sync::atomic::AtomicUsize,
}

impl ParallelChInsertSink {
    /// Create a parallel insert sink with `workers` independent connection pools.
    /// Each pool gets `pool_size` connections.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        connection_string: &str,
        database: &str,
        username: &str,
        password: &str,
        use_tls: bool,
        tls_domain: Option<&str>,
        workers: usize,
        pool_size: usize,
    ) -> anyhow::Result<Self> {
        let mut pools = Vec::with_capacity(workers);
        for i in 0..workers {
            let pool = ConnectionPoolBuilder::<ArrowFormat>::new(connection_string)
                .configure_pool(|p| p.max_size(pool_size as u32))
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
                .map_err(|e| anyhow::anyhow!("Parallel CH pool {} failed: {}", i, e))?;

            // Verify connectivity (scoped to drop client before moving pool)
            {
                let client = pool.get().await
                    .map_err(|e| anyhow::anyhow!("Parallel CH pool {} get: {}", i, e))?;
                client.execute("SELECT 1", None).await
                    .map_err(|e| anyhow::anyhow!("Parallel CH pool {} health check: {}", i, e))?;
            }

            pools.push(pool);
        }
        tracing::info!(
            "ParallelChInsert: {} workers with {} connections each",
            workers, pool_size,
        );
        Ok(Self { pools, next: std::sync::atomic::AtomicUsize::new(0) })
    }
}

impl Sink for ParallelChInsertSink {
    fn write<'a>(&'a self, write: TableWrite) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if write.batches.is_empty() {
                return Ok(());
            }

            // Round-robin across pools for even distribution.
            let idx = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.pools.len();
            let pool = &self.pools[idx];

            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("Parallel CH worker {} pool get: {}", idx, e))?;

            // Set dedup token if present (exactly-once support).
            if let Some(ref token) = write.dedup_token {
                let set_query = format!("SET insert_deduplication_token = '{}'", token);
                client.execute(&set_query, None).await
                    .map_err(|e| anyhow::anyhow!("Parallel CH worker {} SET dedup: {}", idx, e))?;
            }

            let query = format!("INSERT INTO `{}` VALUES", write.table);
            let total: usize = write.batches.iter().map(|b| b.num_rows()).sum();
            let n = write.batches.len();

            let mut stream = client.insert_many(&query, write.batches, None).await
                .map_err(|e| anyhow::anyhow!("Parallel CH worker {} insert_many: {}", idx, e))?;

            use futures_util::StreamExt;
            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("Parallel CH worker {} insert error: {}", idx, e))?;
            }

            tracing::info!(
                "Parallel CH worker {}: inserted {} rows ({} blocks) into '{}'",
                idx, total, n, write.table,
            );
            Ok(())
        })
    }
}
