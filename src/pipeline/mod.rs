pub mod source;
pub mod middleware;
pub mod sink;
pub mod parser;

use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::pipeline::source::Source;
use crate::pipeline::parser::Parser;
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;

pub async fn run_partition_pipeline(
    source: &mut impl Source,
    parser: &impl Parser,
    middlewares: &[Box<dyn Middleware>],
    sink: &impl Sink,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        if cancel_token.is_cancelled() {
            tracing::info!("Shutdown signal received, stopping partition pipeline");
            return Ok(());
        }

        let msg_batch = match source.read_batch().await {
            Ok(batch) => {
                if batch.messages.is_empty() {
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

        backoff_ms = INITIAL_BACKOFF_MS;
        let commit_marker = msg_batch.commit_marker.clone();

        // Parser: Message -> Arrow (sync — CPU-bound, no .await overhead)
        let (valid_batch, dlq_batch) = match parser.parse(
            msg_batch.messages,
            msg_batch.partition_id,
        ) {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Parser error: {}", e);
                continue;
            }
        };

        // Middleware chain (DLQ does NOT go through chain)
        let valid_batch = match apply_middlewares(valid_batch, middlewares).await {
            Ok(batch) => batch,
            Err(e) => {
                tracing::error!("Middleware error: {}", e);
                continue;
            }
        };

        // Write valid batch
        if valid_batch.batch.num_rows() > 0 {
            if let Err(e) = sink.write_batch(&valid_batch).await {
                tracing::error!("Sink write error (valid batch): {}. Will retry.", e);
                continue;
            }
        }

        // Write DLQ batch
        if let Some(ref dlq) = dlq_batch {
            if let Err(e) = sink.write_batch(dlq).await {
                tracing::error!("Sink write error (DLQ batch): {}. Will retry.", e);
                continue;
            }
        }

        // Commit offsets
        if let Some(ref marker) = commit_marker {
            if let Err(e) = source.commit_offsets(marker).await {
                tracing::error!("Offset commit error: {}. Will retry from read.", e);
                continue;
            }
        }
    }
}

async fn apply_middlewares(
    mut batch: ArrowBatch,
    middlewares: &[Box<dyn Middleware>],
) -> anyhow::Result<ArrowBatch> {
    for mw in middlewares {
        batch = mw.process(batch).await?;
    }
    Ok(batch)
}
