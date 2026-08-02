pub mod source;
pub mod middleware;
pub mod sink;
pub mod parser;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::parser::{JsonParser, ParserWorkspace};
use crate::pipeline::source::{CommitMarker, Source};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;
const CHANNEL_CAPACITY: usize = 8;

// ---------------------------------------------------------------------------
// Channel payloads
// ---------------------------------------------------------------------------

/// Reader → Parser
struct ReadItem {
    messages: Vec<crate::types::message::Message>,
    partition_id: i64,
    commit_marker: Option<CommitMarker>,
}

/// Parser → Writer
struct ParsedItem {
    valid_batch: ArrowBatch,
    dlq_batch: Option<ArrowBatch>,
    commit_marker: Option<CommitMarker>,
}

/// Writer → Reader: commit these markers (flush succeeded)
struct CommitAck {
    markers: Vec<CommitMarker>,
}

// ---------------------------------------------------------------------------
// Staged pipeline: Reader ∥ Parser ∥ Writer, with commit feedback
// ---------------------------------------------------------------------------

pub async fn run_partition_pipeline(
    mut source: impl Source + 'static,
    parser: Arc<JsonParser>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    sink: Arc<impl Sink + 'static>,
    batch_size: usize,
    max_linger_ms: u64,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let max_linger = Duration::from_millis(max_linger_ms);

    let (tx_read, mut rx_read) = mpsc::channel::<ReadItem>(CHANNEL_CAPACITY);
    let (tx_parsed, mut rx_parsed) = mpsc::channel::<ParsedItem>(CHANNEL_CAPACITY);
    let tx_parsed_parser = tx_parsed.clone();
    // Feedback: Writer → Reader for commit acks
    let (tx_commit, mut rx_commit) = mpsc::channel::<CommitAck>(CHANNEL_CAPACITY);

    let mut join_set = JoinSet::new();

    // --- Reader task (owns source for read + commit) ---
    let reader_token = cancel_token.clone();
    join_set.spawn(async move {
        let mut backoff_ms = INITIAL_BACKOFF_MS;
        loop {
            // Check for pending commits before reading
            while let Ok(ack) = rx_commit.try_recv() {
                for marker in &ack.markers {
                    if let Err(e) = source.commit_offsets(marker).await {
                        tracing::error!("Reader: commit error: {}", e);
                    }
                }
            }

            if reader_token.is_cancelled() {
                // Drain remaining commit acks
                while let Ok(ack) = rx_commit.try_recv() {
                    for marker in &ack.markers {
                        let _ = source.commit_offsets(marker).await;
                    }
                }
                return;
            }

            let msg_batch = tokio::select! {
                result = source.read_batch() => result,
                // Also check for commits while waiting for reads
                ack = rx_commit.recv() => {
                    if let Some(ack) = ack {
                        for marker in &ack.markers {
                            let _ = source.commit_offsets(marker).await;
                        }
                    }
                    continue;
                }
                _ = reader_token.cancelled() => {
                    while let Ok(ack) = rx_commit.try_recv() {
                        for marker in &ack.markers {
                            let _ = source.commit_offsets(marker).await;
                        }
                    }
                    return;
                }
            };

            let msg_batch = match msg_batch {
                Ok(batch) => {
                    if batch.messages.is_empty() {
                        // Adaptive backoff on empty reads
                        tokio::select! {
                            _ = sleep(Duration::from_millis(backoff_ms)) => {
                                backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
                                continue;
                            }
                            ack = rx_commit.recv() => {
                                if let Some(ack) = ack {
                                    for marker in &ack.markers {
                                        let _ = source.commit_offsets(marker).await;
                                    }
                                }
                                continue;
                            }
                            _ = reader_token.cancelled() => return,
                        }
                    }
                    batch
                }
                Err(e) => {
                    tracing::error!("Read error: {}. Backing off {}ms", e, backoff_ms);
                    tokio::select! {
                        _ = sleep(Duration::from_millis(backoff_ms)) => {},
                        ack = rx_commit.recv() => {
                            if let Some(ack) = ack {
                                for marker in &ack.markers {
                                    let _ = source.commit_offsets(marker).await;
                                }
                            }
                        }
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
                return;
            }
        }
    });

    // --- Parser task (spawn_blocking) ---
    let parser_token = cancel_token.clone();
    let mw_for_parser = middlewares.clone();
    let tx_parsed_clone = tx_parsed_parser.clone();
    join_set.spawn_blocking(move || {
        let mut workspace = ParserWorkspace::new();
        loop {
            if parser_token.is_cancelled() {
                return;
            }
            let item = match rx_read.blocking_recv() {
                Some(item) => item,
                None => return,
            };

            let (valid_batch, dlq_batch) = match parser.parse_into(item.messages, item.partition_id, &mut workspace) {
                Ok(result) => result,
                Err(e) => { tracing::error!("Parser error: {}", e); continue; }
            };

            let valid_batch = match apply_middlewares(valid_batch, &mw_for_parser) {
                Ok(batch) => batch,
                Err(e) => { tracing::error!("Middleware error: {}", e); continue; }
            };

            let parsed = ParsedItem { valid_batch, dlq_batch, commit_marker: item.commit_marker };
            if tx_parsed_clone.blocking_send(parsed).is_err() {
                return;
            }
        }
    });

    // --- Writer task ---
    let writer_token = cancel_token.clone();
    let writer_join = tokio::spawn(async move {
        let mut accumulator: Option<BatchAccumulator> = None;

        loop {
            // Check linger timeout
            let timeout_fired = accumulator.as_ref().and_then(|acc: &BatchAccumulator| {
                acc.window_start.map(|start| start.elapsed() >= max_linger && acc.total_rows > 0)
            }).unwrap_or(false);

            if writer_token.is_cancelled() {
                if let Some(ref mut acc) = accumulator {
                    if let Some(flush) = acc.take_flush() {
                        if flush_to_sink_and_ack(sink.as_ref(), &tx_commit, flush).await.is_ok() {
                            acc.clear();
                        }
                    }
                }
                return;
            }

            if timeout_fired {
                if let Some(ref mut acc) = accumulator {
                    if let Some(flush) = acc.take_flush() {
                        if flush_to_sink_and_ack(sink.as_ref(), &tx_commit, flush).await.is_ok() {
                            acc.clear();
                        }
                    }
                }
                continue;
            }

            let item = tokio::select! {
                maybe = rx_parsed.recv() => {
                    match maybe {
                        Some(item) => item,
                        None => {
                            if let Some(ref mut acc) = accumulator {
                                if let Some(flush) = acc.take_flush() {
                                    let _ = flush_to_sink_and_ack(sink.as_ref(), &tx_commit, flush).await;
                                }
                            }
                            return;
                        }
                    }
                }
                _ = async {
                    if let Some(ref acc) = accumulator {
                        if let Some(start) = acc.window_start {
                            let elapsed = start.elapsed();
                            if elapsed < max_linger {
                                sleep(max_linger - elapsed).await;
                            }
                        }
                    }
                    // Always yield to the top of loop for re-check
                } => continue,
                _ = writer_token.cancelled() => {
                    if let Some(ref mut acc) = accumulator {
                        if let Some(flush) = acc.take_flush() {
                            let _ = flush_to_sink_and_ack(sink.as_ref(), &tx_commit, flush).await;
                        }
                    }
                    return;
                }
            };

            // Write DLQ immediately
            if let Some(ref dlq) = item.dlq_batch {
                if let Err(e) = sink.write_batch(dlq).await {
                    tracing::error!("Sink write error (DLQ batch): {}. Will retry.", e);
                    continue;
                }
            }

            if item.valid_batch.batch.num_rows() == 0 {
                continue;
            }

            if accumulator.is_none() {
                accumulator = Some(BatchAccumulator::new(batch_size));
            }

            let acc = accumulator.as_mut().unwrap();
            if let Some(flush) = acc.push(item.valid_batch.batch, item.commit_marker) {
                if flush_to_sink_and_ack(sink.as_ref(), &tx_commit, flush).await.is_ok() {
                    acc.clear();
                }
            }
        }
    });

    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            if e.is_panic() {
                tracing::error!("Stage task panicked: {}", e);
            }
        }
    }

    drop(tx_parsed);
    let _ = writer_join.await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Batch accumulator
// ---------------------------------------------------------------------------

struct BatchAccumulator {
    batches: Vec<RecordBatch>,
    markers: Vec<CommitMarker>,
    total_rows: usize,
    batch_size: usize,
    window_start: Option<Instant>,
}

impl BatchAccumulator {
    fn new(batch_size: usize) -> Self {
        Self { batches: Vec::new(), markers: Vec::new(), total_rows: 0, batch_size, window_start: None }
    }

    fn push(&mut self, batch: RecordBatch, marker: Option<CommitMarker>) -> Option<FlushBatch> {
        if self.window_start.is_none() {
            self.window_start = Some(Instant::now());
        }
        self.total_rows += batch.num_rows();
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

    fn take_flush(&mut self) -> Option<FlushBatch> {
        if self.total_rows == 0 {
            return None;
        }
        let batches = std::mem::take(&mut self.batches);
        let markers = std::mem::take(&mut self.markers);
        Some(FlushBatch { batches, markers })
    }

    fn clear(&mut self) {
        self.batches.clear();
        self.markers.clear();
        self.total_rows = 0;
        self.window_start = None;
    }
}

struct FlushBatch {
    batches: Vec<RecordBatch>,
    markers: Vec<CommitMarker>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write to sink via insert_many, then send commit markers back to reader.
async fn flush_to_sink_and_ack(
    sink: &(impl Sink + 'static),
    tx_commit: &mpsc::Sender<CommitAck>,
    flush: FlushBatch,
) -> Result<(), ()> {
    if let Err(e) = sink.write_batches(&flush.batches, false).await {
        tracing::error!("Writer: flush error: {}. Will retry.", e);
        return Err(());
    }

    let markers = flush.markers;
    if !markers.is_empty()
        && tx_commit.send(CommitAck { markers }).await.is_err()
    {
        tracing::error!("Writer: commit ack channel closed");
        return Err(());
    }
    Ok(())
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
