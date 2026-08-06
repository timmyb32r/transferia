use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};

use crate::pipeline::sink::Sink;
use crate::types::table_data::TableWrite;

/// Parallel `ClickHouse` insert sink: fans out writes across N independent
/// `ClickHouse` connections for higher throughput.
///
/// **Architecture note**: This type lives in `middleware/` but implements `Sink`,
/// not `Middleware`. It's a **Sink decorator**, not a data transformer.
/// The `middleware/` crate location is intentional — this component is an
/// optional pipeline plug-in, conceptually sitting between the accumulator
/// and the destination, like other middleware. The naming prioritizes the
/// user's mental model (optional pipeline stage = middleware) over strict
/// type-level taxonomy.
///
/// **Weakened guarantee**: assumes all keys in the incoming stream are unique,
/// so parallel out-of-order inserts don't cause consistency issues.
pub struct ParallelChInsertSink {
    pools: Vec<ConnectionPool<ArrowFormat>>,
    next: core::sync::atomic::AtomicUsize,
}

impl ParallelChInsertSink {
    /// Create a parallel insert sink with `workers` independent connection pools.
    /// Each pool gets `pool_size` connections.
    #[expect(clippy::too_many_arguments, reason = "connection pool builder takes many config knobs; extracting a config struct is overkill")]
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
                .configure_client(|builder| {
                    let mut configured = builder.with_database(database)
                        .with_username(username)
                        .with_password(password)
                        .with_tls(use_tls);
                    if let Some(domain) = tls_domain {
                        configured = configured.with_domain(domain);
                    }
                    configured
                })
                .build().await
                .map_err(|e| anyhow::anyhow!("Parallel CH pool {i} failed: {e}"))?;

            // Verify connectivity (scoped to drop client before moving pool)
            {
                let client = pool.get().await
                    .map_err(|e| anyhow::anyhow!("Parallel CH pool {i} get: {e}"))?;
                client.execute("SELECT 1", None).await
                    .map_err(|e| anyhow::anyhow!("Parallel CH pool {i} health check: {e}"))?;
            };

            pools.push(pool);
        }
        tracing::info!(
            "ParallelChInsert: {} workers with {} connections each",
            workers, pool_size,
        );
        Ok(Self { pools, next: core::sync::atomic::AtomicUsize::new(0) })
    }
}

impl Sink for ParallelChInsertSink {
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if write.batches.is_empty() {
                return Ok(());
            }

            // Round-robin across pools for even distribution.
            let idx = self.next
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed)
                .rem_euclid(self.pools.len());
            let pool = self.pools.get(idx)
                .ok_or_else(|| anyhow::anyhow!("Parallel CH: pool index {idx} out of bounds"))?;

            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("Parallel CH worker {idx} pool get: {e}"))?;

            // Exactly-once: the ExactlyOnceKey descriptor (partition/offset column
            // names in the batch) cannot be converted into an
            // `insert_deduplication_token` string — the incompatibility check
            // (descriptor vs CH dedup token) lands in a later stage. Until then,
            // when a key descriptor is present we keep the guarded block but skip
            // the SET (at-least-once behavior).
            if let Some(ref _key) = write.exactly_once_key {
                // TODO(exactly-once): rework SET insert_deduplication_token for
                // ExactlyOnceKey descriptors (later stage).
            }

            let query = format!("INSERT INTO `{}` VALUES", write.table);
            let total: usize = write.batches.iter().map(arrow::array::RecordBatch::num_rows).sum();
            let n = write.batches.len();

            let mut stream = client.insert_many(&query, write.batches, None).await
                .map_err(|e| anyhow::anyhow!("Parallel CH worker {idx} insert_many: {e}"))?;

            while let Some(item) = stream.next().await {
                item.map_err(|e| anyhow::anyhow!("Parallel CH worker {idx} insert error: {e}"))?;
            }

            tracing::info!(
                "Parallel CH worker {}: inserted {} rows ({} blocks) into '{}'",
                idx, total, n, write.table,
            );
            Ok(())
        })
    }

    fn as_any(&self) -> &dyn core::any::Any { self }
}
