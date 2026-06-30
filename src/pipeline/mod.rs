pub mod source;
pub mod middleware;
pub mod sink;

use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::pipeline::source::Source;
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;

/// Retry configuration for transient errors (exponential backoff).
const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;

/// Run the source → middleware → sink loop for one partition.
///
/// - Reads batches from the source (YDB TopicReader)
/// - Transforms via middleware (JSONPath → Arrow)
/// - Writes to sink (ClickHouse Arrow insert)
/// - Commits offsets after successful write (at-least-once)
///
/// Retries transient errors with exponential backoff.
/// Checks the cancellation token for graceful shutdown.
///
/// # Known limitations
///
/// - `read_batch()` may block on a slow YDB connection — shutdown is not
///   responsive during long polls. Future: wrap with `tokio::time::timeout`.
/// - DLQ batch and valid batch are separate ClickHouse inserts. A crash
///   between them causes re-delivery on restart (at-least-once duplicate).
pub async fn run_partition_pipeline(
    source: &mut impl Source,
    middleware: &impl Middleware,
    sink: &impl Sink,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        // Check for shutdown signal
        if cancel_token.is_cancelled() {
            tracing::info!("Shutdown signal received, stopping partition pipeline");
            return Ok(());
        }

        // Read batch from source (YDB TopicReader — returns TopicReaderBatch directly)
        let raw = match source.read_batch().await {
            Ok(batch) => {
                if batch.data.is_empty() {
                    // No messages — short sleep then retry
                    tokio::select! {
                        _ = sleep(Duration::from_millis(100)) => continue,
                        _ = cancel_token.cancelled() => {
                            tracing::info!("Shutdown during idle wait");
                            return Ok(());
                        }
                    }
                }
                batch
            }
            Err(e) => {
                tracing::error!("Read error: {}. Backing off {}ms", e, backoff_ms);
                tokio::select! {
                    _ = sleep(Duration::from_millis(backoff_ms)) => {},
                    _ = cancel_token.cancelled() => return Ok(()),
                }
                backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
                continue;
            }
        };

        // Reset backoff on successful read
        backoff_ms = INITIAL_BACKOFF_MS;

        // Extract commit marker before moving raw into middleware
        let commit_marker = raw.commit_marker.clone();

        // Transform: JSON → Arrow
        let (valid_batch, dlq_batch) = match middleware.process(raw).await {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Middleware processing error: {}", e);
                continue; // Don't commit — messages re-read on restart
            }
        };

        // Write valid batch to ClickHouse
        if valid_batch.batch.num_rows() > 0 {
            if let Err(e) = sink.write_batch(&valid_batch).await {
                tracing::error!("Sink write error (valid batch): {}. Will retry.", e);
                continue;
            }
        }

        // Write DLQ batch if any malformed rows
        if let Some(ref dlq) = dlq_batch {
            if let Err(e) = sink.write_batch(dlq).await {
                tracing::error!("Sink write error (DLQ batch): {}. Will retry.", e);
                continue;
            }
        }

        // At-least-once: commit offsets after successful writes
        if let Some(ref marker) = commit_marker {
            if let Err(e) = source.commit_offsets(marker).await {
                tracing::error!("Offset commit error: {}. Will retry from read.", e);
                continue;
            }
        }
    }
}
