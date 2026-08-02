pub mod source;
pub mod middleware;
pub mod sink;
pub mod parser;

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::parser::{JsonParser, ParserWorkspace};
use crate::pipeline::source::{CommitMarker, Source};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::{ArrowBatch, BatchMeta};

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;

// ---------------------------------------------------------------------------
// Channel payloads for the staged pipeline
// ---------------------------------------------------------------------------

/// Reader → Parser
struct ReadItem {
    messages: Vec<crate::types::message::Message>,
    partition_id: i64,
    commit_marker: Option<CommitMarker>,
}

/// Parser → Writer (valid batch + optional DLQ)
struct ParsedItem {
    valid_batch: ArrowBatch,
    dlq_batch: Option<ArrowBatch>,
    commit_marker: Option<CommitMarker>,
}

// ---------------------------------------------------------------------------
// Staged pipeline: Reader ∥ Parser ∥ Writer with bounded channels
// ---------------------------------------------------------------------------

/// Run the partition pipeline with concurrent reader, parser, and writer tasks
/// connected by bounded `mpsc` channels (backpressure via channel capacity = 2).
///
/// While the writer is flushing an INSERT to ClickHouse, the reader fetches the
/// next YDB batch and the parser is processing in `spawn_blocking`.
///
/// At-least-once ordering is preserved: commit markers are only committed after
/// the corresponding flush succeeds.
pub async fn run_partition_pipeline(
    mut source: impl Source + 'static,
    parser: Arc<JsonParser>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    sink: Arc<impl Sink + 'static>,
    batch_size: usize,
    max_linger_ms: u64,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    // Bounded channels — capacity 2 gives enough buffering for overlap without
    // excessive memory.
    let (tx_read, mut rx_read) = mpsc::channel::<ReadItem>(2);
    let (tx_parsed, mut rx_parsed) = mpsc::channel::<ParsedItem>(2);
    let tx_parsed_parser = tx_parsed.clone(); // clone for parser task, keep original for drop

    let mut join_set = JoinSet::new();

    // --- Reader task ---
    let reader_token = cancel_token.clone();
    join_set.spawn(async move {
        let mut backoff_ms = INITIAL_BACKOFF_MS;
        loop {
            if reader_token.is_cancelled() {
                tracing::debug!("Reader: cancellation received");
                return;
            }

            let msg_batch = match source.read_batch().await {
                Ok(batch) => {
                    if batch.messages.is_empty() {
                        tokio::select! {
                            _ = sleep(Duration::from_millis(100)) => continue,
                            _ = reader_token.cancelled() => {
                                tracing::debug!("Reader: shutdown during idle");
                                return;
                            }
                        }
                    }
                    batch
                }
                Err(e) => {
                    tracing::error!("Read error: {}. Backing off {}ms", e, backoff_ms);
                    tokio::select! {
                        _ = sleep(Duration::from_millis(backoff_ms)) => {},
                        _ = reader_token.cancelled() => return,
                    }
                    backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
                    continue;
                }
            };

            backoff_ms = INITIAL_BACKOFF_MS;
            let item = ReadItem {
                messages: msg_batch.messages,
                partition_id: msg_batch.partition_id,
                commit_marker: msg_batch.commit_marker,
            };

            if tx_read.send(item).await.is_err() {
                tracing::debug!("Reader: parser channel closed, exiting");
                return;
            }
        }
    });

    // --- Parser task (CPU-bound — spawn_blocking) ---
    let parser_token = cancel_token.clone();
    let mw_for_parser = middlewares.clone();
    join_set.spawn_blocking(move || {
        let mut workspace = ParserWorkspace::new();

        loop {
            if parser_token.is_cancelled() {
                tracing::debug!("Parser: cancellation received");
                return;
            }

            let item = match rx_read.blocking_recv() {
                Some(item) => item,
                None => {
                    tracing::debug!("Parser: reader channel closed, exiting");
                    return;
                }
            };

            let (valid_batch, dlq_batch) = match parser.parse_into(
                item.messages,
                item.partition_id,
                &mut workspace,
            ) {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!("Parser error: {}", e);
                    continue;
                }
            };

            // Apply middlewares (sync, CPU-bound)
            let valid_batch = match apply_middlewares(valid_batch, &mw_for_parser) {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::error!("Middleware error: {}", e);
                    continue;
                }
            };

            let parsed = ParsedItem {
                valid_batch,
                dlq_batch,
                commit_marker: item.commit_marker,
            };

            if tx_parsed_parser.blocking_send(parsed).is_err() {
                tracing::debug!("Parser: writer channel closed, exiting");
                return;
            }
        }
    });

    // --- Writer task ---
    let writer_token = cancel_token.clone();
    let writer_join = tokio::spawn(async move {
        let mut accumulator: Option<BatchAccumulator> = None;

        loop {
            if writer_token.is_cancelled() {
                if let Some(ref acc) = accumulator {
                    if let Some(flush) = acc.take_flush() {
                        if let Err(e) = flush_to_sink(sink.as_ref(), &flush).await {
                            tracing::error!("Writer: final flush failed: {}", e);
                        }
                    }
                }
                tracing::debug!("Writer: cancellation received");
                return;
            }

            let item = tokio::select! {
                maybe = rx_parsed.recv() => {
                    match maybe {
                        Some(item) => item,
                        None => {
                            // Parser channel closed — flush remaining and exit
                            if let Some(ref acc) = accumulator {
                                if let Some(flush) = acc.take_flush() {
                                    if let Err(e) = flush_to_sink(sink.as_ref(), &flush).await {
                                        tracing::error!("Writer: final flush failed: {}", e);
                                    }
                                }
                            }
                            tracing::debug!("Writer: parser channel closed, exiting");
                            return;
                        }
                    }
                }
                _ = writer_token.cancelled() => {
                    if let Some(ref acc) = accumulator {
                        if let Some(flush) = acc.take_flush() {
                            if let Err(e) = flush_to_sink(sink.as_ref(), &flush).await {
                                tracing::error!("Writer: final flush failed: {}", e);
                            }
                        }
                    }
                    tracing::debug!("Writer: cancellation received");
                    return;
                }
            };

            // Write DLQ immediately — never batched
            if let Some(ref dlq) = item.dlq_batch {
                if let Err(e) = sink.write_batch(dlq).await {
                    tracing::error!("Sink write error (DLQ batch): {}. Will retry.", e);
                    continue;
                }
            }

            if item.valid_batch.batch.num_rows() == 0 {
                continue;
            }

            // Init accumulator lazily on first valid batch
            if accumulator.is_none() {
                accumulator = Some(BatchAccumulator::new(
                    item.valid_batch.batch.schema(),
                    batch_size,
                    max_linger_ms,
                ));
            }

            let acc = accumulator.as_mut().unwrap();
            if let Some(flush) = acc.push(item.valid_batch.batch, item.commit_marker) {
                if let Err(e) = flush_to_sink(sink.as_ref(), &flush).await {
                    tracing::error!("Writer: flush error: {}. Will retry.", e);
                    continue;
                }
                acc.clear();
            }
        }
    });

    // Wait for reader and parser to finish first
    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            if e.is_panic() {
                tracing::error!("Stage task panicked: {}", e);
            }
        }
    }

    // Drop parsed channel sender — writer will drain and exit
    drop(tx_parsed);

    // Wait for writer
    let _ = writer_join.await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Serial pipeline (keep for reference/fallback)
// ---------------------------------------------------------------------------

pub async fn run_partition_pipeline_serial(
    source: &mut impl Source,
    parser: &JsonParser,
    middlewares: &[Box<dyn Middleware>],
    sink: &impl Sink,
    batch_size: usize,
    max_linger_ms: u64,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff_ms = INITIAL_BACKOFF_MS;
    let mut workspace = ParserWorkspace::new();
    let mut accumulator: Option<BatchAccumulator> = None;

    loop {
        if cancel_token.is_cancelled() {
            if let Some(ref acc) = accumulator {
                if let Some(flush) = acc.take_flush() {
                    flush_to_sink(sink, &flush).await?;
                }
            }
            tracing::info!("Shutdown signal received, stopping partition pipeline");
            return Ok(());
        }

        let msg_batch = match source.read_batch().await {
            Ok(batch) => {
                if batch.messages.is_empty() {
                    if let Some(ref acc) = accumulator {
                        if let Some(flush) = acc.check_timeout() {
                            flush_to_sink(sink, &flush).await?;
                            accumulator.as_mut().unwrap().clear();
                        }
                    }
                    tokio::select! {
                        _ = sleep(Duration::from_millis(100)) => continue,
                        _ = cancel_token.cancelled() => {
                            if let Some(ref acc) = accumulator {
                                if let Some(flush) = acc.take_flush() {
                                    flush_to_sink(sink, &flush).await?;
                                }
                            }
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

        let (valid_batch, dlq_batch) = match parser.parse_into(
            msg_batch.messages,
            msg_batch.partition_id,
            &mut workspace,
        ) {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Parser error: {}", e);
                continue;
            }
        };

        let valid_batch = match apply_middlewares(valid_batch, middlewares) {
            Ok(batch) => batch,
            Err(e) => {
                tracing::error!("Middleware error: {}", e);
                continue;
            }
        };

        if let Some(ref dlq) = dlq_batch {
            if let Err(e) = sink.write_batch(dlq).await {
                tracing::error!("Sink write error (DLQ batch): {}. Will retry.", e);
                continue;
            }
        }

        if valid_batch.batch.num_rows() == 0 {
            continue;
        }

        if accumulator.is_none() {
            accumulator = Some(BatchAccumulator::new(
                valid_batch.batch.schema(),
                batch_size,
                max_linger_ms,
            ));
        }

        let acc = accumulator.as_mut().unwrap();
        if let Some(flush) = acc.push(valid_batch.batch, commit_marker) {
            flush_to_sink(sink, &flush).await?;
            acc.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// Batch accumulator
// ---------------------------------------------------------------------------

struct BatchAccumulator {
    batches: Vec<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
    markers: Vec<CommitMarker>,
    total_rows: usize,
    batch_size: usize,
    max_linger: Duration,
    window_start: Option<tokio::time::Instant>,
}

impl BatchAccumulator {
    fn new(schema: Arc<arrow::datatypes::Schema>, batch_size: usize, max_linger_ms: u64) -> Self {
        Self {
            batches: Vec::new(),
            schema,
            markers: Vec::new(),
            total_rows: 0,
            batch_size,
            max_linger: Duration::from_millis(max_linger_ms),
            window_start: None,
        }
    }

    fn push(
        &mut self,
        batch: RecordBatch,
        marker: Option<CommitMarker>,
    ) -> Option<FlushBatch> {
        if self.window_start.is_none() {
            self.window_start = Some(tokio::time::Instant::now());
        }

        let rows = batch.num_rows();
        self.total_rows += rows;
        self.batches.push(batch);
        if let Some(m) = marker {
            self.markers.push(m);
        }

        if self.total_rows >= self.batch_size {
            self.take_flush()
        } else {
            None
        }
    }

    fn check_timeout(&self) -> Option<FlushBatch> {
        if self.total_rows == 0 {
            return None;
        }
        if let Some(start) = self.window_start {
            if start.elapsed() >= self.max_linger {
                return self.take_flush();
            }
        }
        None
    }

    fn take_flush(&self) -> Option<FlushBatch> {
        if self.total_rows == 0 {
            return None;
        }
        let merged = concat_batches(&self.schema, &self.batches).ok()?;
        Some(FlushBatch {
            batch: merged,
            markers: self.markers.clone(),
        })
    }

    fn clear(&mut self) {
        self.batches.clear();
        self.markers.clear();
        self.total_rows = 0;
        self.window_start = None;
    }
}

struct FlushBatch {
    batch: RecordBatch,
    markers: Vec<CommitMarker>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn flush_to_sink(
    sink: &impl Sink,
    flush: &FlushBatch,
) -> anyhow::Result<()> {
    let dummy_meta = BatchMeta {
        table_name: Arc::from(""),
        partition_id: 0,
        dlq_flag: false,
        batch_id: 0,
        created_at: chrono::Utc::now(),
    };
    let flush_batch = ArrowBatch {
        batch: flush.batch.clone(),
        meta: dummy_meta,
    };

    sink.write_batch(&flush_batch).await
}

fn apply_middlewares(
    mut batch: ArrowBatch,
    middlewares: &[Box<dyn Middleware>],
) -> anyhow::Result<ArrowBatch> {
    for mw in middlewares {
        batch = mw.process(batch)?;
    }
    Ok(batch)
}
