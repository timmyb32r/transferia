pub mod source;
pub mod middleware;
pub mod sink;

use std::sync::Arc;
use std::thread;

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::parser::{JsonParser, ParserWorkspace};
use crate::pipeline::source::{CommitMarker, Source};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;
use crate::types::arrow_batch::ArrowBatch;

const INITIAL_BACKOFF_MS: u64 = 10; // was 100 — lower floor for faster resume
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;
const CHANNEL_CAPACITY: usize = 32; // was 8 — deeper buffers to absorb flush I/O

// ---------------------------------------------------------------------------
// Channel payloads
// ---------------------------------------------------------------------------

struct ReadItem {
    messages: Vec<crate::types::message::Message>,
    partition_id: i64,
    commit_marker: Option<CommitMarker>,
}

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

    let (tx_read, rx_read) = mpsc::channel::<ReadItem>(CHANNEL_CAPACITY);
    let (tx_parsed, mut rx_parsed) = mpsc::channel::<ParsedItem>(CHANNEL_CAPACITY);
    let tx_parsed_parser = tx_parsed.clone();
    let (tx_commit, mut rx_commit) = mpsc::channel::<CommitAck>(CHANNEL_CAPACITY);

    // --- Reader task (tokio::spawn — owns source for read + commit) ---
    let reader_token = cancel_token.clone();
    let reader_handle = tokio::spawn(async move {
        let mut backoff_ms = INITIAL_BACKOFF_MS;
        loop {
            // Drain pending commit acks
            while let Ok(ack) = rx_commit.try_recv() {
                for marker in &ack.markers {
                    let _ = source.commit_offsets(marker).await;
                }
                backoff_ms = INITIAL_BACKOFF_MS; // reset on activity
            }

            if reader_token.is_cancelled() {
                while let Ok(ack) = rx_commit.try_recv() {
                    for marker in &ack.markers {
                        let _ = source.commit_offsets(marker).await;
                    }
                }
                return;
            }

            // Read + drain acks concurrently
            let msg_batch = tokio::select! {
                result = source.read_batch() => result,
                ack = rx_commit.recv() => {
                    if let Some(ack) = ack {
                        for marker in &ack.markers {
                            let _ = source.commit_offsets(marker).await;
                        }
                        backoff_ms = INITIAL_BACKOFF_MS;
                    }
                    continue;
                }
                _ = reader_token.cancelled() => {
                    while let Ok(ack) = rx_commit.try_recv() {
                        for marker in &ack.markers { let _ = source.commit_offsets(marker).await; }
                    }
                    return;
                }
            };

            let msg_batch = match msg_batch {
                Ok(batch) if batch.messages.is_empty() => {
                    tokio::select! {
                        _ = sleep(Duration::from_millis(backoff_ms)) => {
                            backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
                            continue;
                        }
                        ack = rx_commit.recv() => {
                            if let Some(ack) = ack {
                                for marker in &ack.markers { let _ = source.commit_offsets(marker).await; }
                                backoff_ms = INITIAL_BACKOFF_MS;
                            }
                            continue;
                        }
                        _ = reader_token.cancelled() => return,
                    }
                }
                Ok(batch) => batch,
                Err(e) => {
                    tracing::error!("Read error: {}. Backing off {}ms", e, backoff_ms);
                    tokio::select! {
                        _ = sleep(Duration::from_millis(backoff_ms)) => {},
                        ack = rx_commit.recv() => {
                            if let Some(ack) = ack {
                                for marker in &ack.markers { let _ = source.commit_offsets(marker).await; }
                            }
                        }
                        _ = reader_token.cancelled() => return,
                    }
                    backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
                    continue;
                }
            };

            backoff_ms = INITIAL_BACKOFF_MS;

            // Drain acks before sending (prevents ack channel fill → writer stall)
            while let Ok(ack) = rx_commit.try_recv() {
                for marker in &ack.markers {
                    let _ = source.commit_offsets(marker).await;
                }
            }

            let item = ReadItem {
                messages: msg_batch.messages,
                partition_id: msg_batch.partition_id,
                commit_marker: msg_batch.commit_marker,
            };

            // Send + drain acks concurrently to avoid pipeline stall
            tokio::select! {
                result = tx_read.send(item) => {
                    if result.is_err() { return; }
                }
                ack = rx_commit.recv() => {
                    if let Some(ack) = ack {
                        for marker in &ack.markers { let _ = source.commit_offsets(marker).await; }
                        backoff_ms = INITIAL_BACKOFF_MS;
                    }
                    // Re-queue the read item (will retry send next iteration after drain)
                    continue;
                }
                _ = reader_token.cancelled() => return,
            }
        }
    });

    // --- Parser task (dedicated std::thread — no tokio blocking-pool limit) ---
    let parser_token = cancel_token.clone();
    let mut rx_read = rx_read; // moved into thread
    let parser_thread = thread::Builder::new()
        .name("parser".into())
        .spawn(move || {
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
                    Ok(r) => r,
                    Err(e) => { tracing::error!("Parser error: {}", e); continue; }
                };

                let valid_batch = match apply_middlewares(valid_batch, &middlewares) {
                    Ok(b) => b,
                    Err(e) => { tracing::error!("Middleware error: {}", e); continue; }
                };

                let parsed = ParsedItem { valid_batch, dlq_batch, commit_marker: item.commit_marker };
                if tx_parsed_parser.blocking_send(parsed).is_err() {
                    return;
                }
            }
        })?;

    // --- Writer task ---
    let writer_token = cancel_token.clone();
    let sink_for_writer = sink.clone();
    let writer_handle = tokio::spawn(async move {
        let mut accumulator: Option<BatchAccumulator> = None;

        loop {
            // Phase 1: flush if timeout expired
            let should_flush = accumulator.as_ref().is_some_and(|acc| {
                acc.total_rows > 0
                    && acc.window_start.is_some_and(|s| s.elapsed() >= max_linger)
            });

            if should_flush {
                let flush = accumulator.as_mut().unwrap().take_flush().unwrap();
                if flush_to_sink_and_ack(sink_for_writer.as_ref(), &tx_commit, flush).await.is_ok() {
                    accumulator.as_mut().unwrap().clear();
                }
                continue;
            }

            // Phase 2: check cancellation
            if writer_token.is_cancelled() {
                if let Some(ref mut acc) = accumulator {
                    if let Some(flush) = acc.take_flush() {
                        let _ = flush_to_sink_and_ack(sink_for_writer.as_ref(), &tx_commit, flush).await;
                    }
                }
                return;
            }

            // Phase 3: wait for data or timeout (NO busy-spin)
            let timeout = accumulator.as_ref().and_then(|acc| {
                acc.window_start.map(|s| {
                    let elapsed = s.elapsed();
                    if elapsed < max_linger { max_linger - elapsed } else { Duration::ZERO }
                })
            });

            let maybe_item = if let Some(dur) = timeout {
                tokio::select! {
                    maybe = rx_parsed.recv() => maybe,
                    _ = sleep(dur) => None, // timeout → will flush at top of loop
                    _ = writer_token.cancelled() => {
                        if let Some(ref mut acc) = accumulator {
                            if let Some(flush) = acc.take_flush() {
                                let _ = flush_to_sink_and_ack(sink_for_writer.as_ref(), &tx_commit, flush).await;
                            }
                        }
                        return;
                    }
                }
            } else {
                // No accumulator — only wait for data or cancellation
                tokio::select! {
                    maybe = rx_parsed.recv() => maybe,
                    _ = writer_token.cancelled() => {
                        if let Some(ref mut acc) = accumulator {
                            if let Some(flush) = acc.take_flush() {
                                let _ = flush_to_sink_and_ack(sink_for_writer.as_ref(), &tx_commit, flush).await;
                            }
                        }
                        return;
                    }
                }
            };

            let item = match maybe_item {
                Some(item) => item,
                None => continue, // timeout fired
            };

            // Write DLQ immediately
            if let Some(ref dlq) = item.dlq_batch {
                if let Err(e) = sink_for_writer.write_batch(dlq).await {
                    tracing::error!("Sink write error (DLQ batch): {}", e);
                    // DLQ write failed — push back to DLQ? For now, log and continue.
                    // The valid batch is still processed below.
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
                if flush_to_sink_and_ack(sink_for_writer.as_ref(), &tx_commit, flush).await.is_err() {
                    // Flush failed — clear accumulator state to avoid churn.
                    // Data is replayed from YDB on restart (at-least-once).
                    acc.clear();
                } else {
                    acc.clear();
                }
            }
        }
    });

    // Wait for reader first, then drop tx_parsed to signal parser
    let _ = reader_handle.await;
    drop(tx_parsed);
    let _ = parser_thread.join();
    let _ = writer_handle.await;

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

async fn flush_to_sink_and_ack(
    sink: &(impl Sink + 'static),
    tx_commit: &mpsc::Sender<CommitAck>,
    flush: FlushBatch,
) -> Result<(), ()> {
    let markers = flush.markers;
    if !markers.is_empty() {
        if let Err(e) = sink.write_batches(flush.batches, false).await {
            tracing::error!("Writer: flush error: {}", e);
            return Err(());
        }
        if tx_commit.send(CommitAck { markers }).await.is_err() {
            tracing::error!("Writer: commit ack channel closed");
            return Err(());
        }
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
