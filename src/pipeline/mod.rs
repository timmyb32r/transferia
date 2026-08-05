pub mod source;
pub mod middleware;
pub mod sink;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::parser::{JsonParser, ParserWorkspace};
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;
use crate::types::table_data::{TableData, TableWrite};

const INITIAL_BACKOFF_MS: u64 = 10; // was 100 — lower floor for faster resume
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;
const CHANNEL_CAPACITY: usize = 32; // was 8 — deeper buffers to absorb flush I/O

/// Guardrail: max consecutive middleware errors on valid data before treating the partition
/// as permanently broken. Prevents a livelock where every valid batch is rejected by a
/// misconfigured middleware.
const MAX_CONSECUTIVE_MW_ERRORS: u32 = 100;

// ---------------------------------------------------------------------------
// Channel payloads
// ---------------------------------------------------------------------------

struct ReadItem {
    messages: Vec<crate::types::message::Message>,
    partition_id: i64,
    commit_marker: Option<CommitMarker>,
    dedup_token: Option<String>,
}

/// After parsing, a batch of messages becomes one or two `TableData` objects
/// (valid + optional DLQ), sharing a single commit marker.
struct ParsedItem {
    valid: TableData,
    dlq: Option<TableData>,
    commit_marker: Option<CommitMarker>,
}

/// Sentinel: too many consecutive middleware errors. Tells the writer to
/// abort the partition so main.rs can retry (or give up after retries).
enum ParsedMsg {
    Item(Box<ParsedItem>),
    Fatal,
}

/// Writer → Reader: commit these markers (flush succeeded)
struct CommitAck {
    markers: Vec<CommitMarker>,
}

// ---------------------------------------------------------------------------
// Multi-table batch accumulator
// ---------------------------------------------------------------------------

struct BatchAccumulator {
    /// Per-table writes. `TableWrite::batches` is accumulated in push order.
    tables: HashMap<Arc<str>, TableWrite>,
    /// Insertion order — guarantees deterministic flush order (main before dlq).
    order: Vec<Arc<str>>,
    markers: Vec<CommitMarker>,
    total_rows: usize,
    /// Global threshold: total rows across **all** tables.
    batch_size: usize,
    window_start: Option<Instant>,
    /// Dedup token aggregated across pushes.
    dedup_token: Option<String>,
}

impl BatchAccumulator {
    fn new(batch_size: usize) -> Self {
        Self {
            tables: HashMap::new(),
            order: Vec::new(),
            markers: Vec::new(),
            total_rows: 0,
            batch_size,
            window_start: None,
            dedup_token: None,
        }
    }

    /// Push a `TableData` into the accumulator. Never triggers a flush (invariant A1).
    /// Empty batches (0 rows) are silently skipped.
    fn push(&mut self, td: TableData, marker: Option<CommitMarker>) {
        if td.batch.num_rows() == 0 {
            return;
        }
        if self.window_start.is_none() {
            self.window_start = Some(Instant::now());
        }
        // Keep the first non-None dedup token; all items in a flush share the same
        // source batch and therefore the same token.
        if self.dedup_token.is_none() {
            self.dedup_token = td.dedup_token.clone();
        }
        let rows = td.batch.num_rows();
        let entry = self.tables.entry(td.table.clone()).or_insert_with(|| {
            self.order.push(td.table.clone());
            TableWrite {
                table: td.table.clone(), batches: Vec::new(),
                dedup_token: None,
            }
        });
        entry.batches.push(td.batch);
        self.total_rows += rows;
        if let Some(m) = marker {
            self.markers.push(m);
        }
    }

    fn should_flush(&self) -> bool {
        self.total_rows >= self.batch_size
    }

    fn is_empty(&self) -> bool {
        self.total_rows == 0
    }

    /// Take ALL buffered data + all markers. Returns `None` when empty.
    fn take_flush(&mut self) -> Option<FlushBatch> {
        if self.is_empty() {
            return None;
        }
        let token = self.dedup_token.take();
        let writes: Vec<TableWrite> = self
            .order
            .iter()
            .filter_map(|name| self.tables.remove(name.as_ref()))
            .filter(|w| !w.batches.is_empty())
            .map(|w| TableWrite { dedup_token: token.clone(), ..w })
            .collect();
        let markers = std::mem::take(&mut self.markers);
        self.order.clear();
        self.tables.clear();
        self.total_rows = 0;
        self.dedup_token = None;
        Some(FlushBatch { writes, markers })
    }

    fn clear(&mut self) {
        self.tables.clear();
        self.order.clear();
        self.markers.clear();
        self.total_rows = 0;
        self.window_start = None;
        self.dedup_token = None;
    }
}

struct FlushBatch {
    writes: Vec<TableWrite>,
    markers: Vec<CommitMarker>,
}

// ---------------------------------------------------------------------------
// Staged pipeline: Reader ∥ Parser ∥ Writer, with commit feedback
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub async fn run_partition_pipeline(
    mut source: Box<dyn Source>,
    parser: Arc<JsonParser>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    sink: Arc<dyn Sink>,
    batch_size: usize,
    max_linger_ms: u64,
    cancel_token: CancellationToken,
    partition_id: i64,
) -> anyhow::Result<()> {
    let max_linger = Duration::from_millis(max_linger_ms);

    let (tx_read, rx_read) = mpsc::channel::<ReadItem>(CHANNEL_CAPACITY);
    let (tx_parsed, mut rx_parsed) = mpsc::channel::<ParsedMsg>(CHANNEL_CAPACITY);
    let tx_parsed_parser = tx_parsed.clone();
    let (tx_commit, mut rx_commit) = mpsc::channel::<CommitAck>(CHANNEL_CAPACITY);

    // Shared fatal-error slot: reader (Failed) or writer (sink error / middleware guard).
    let fatal_error: Arc<std::sync::Mutex<Option<anyhow::Error>>> =
        Arc::new(std::sync::Mutex::new(None));

    // --- Reader task (tokio::spawn — owns source for read + commit) ---
    let reader_token = cancel_token.clone();
    let reader_fatal = fatal_error.clone();
    let reader_handle = tokio::spawn(async move {
        let mut backoff_ms = INITIAL_BACKOFF_MS;
        loop {
            // Drain pending commit acks
            while let Ok(ack) = rx_commit.try_recv() {
                for marker in &ack.markers {
                    if let Err(e) = source.commit_offsets(marker).await {
                        tracing::warn!("Reader: commit_offsets error: {}", e);
                    }
                }
                backoff_ms = INITIAL_BACKOFF_MS; // reset on activity
            }

            if reader_token.is_cancelled() {
                while let Ok(ack) = rx_commit.try_recv() {
                    for marker in &ack.markers {
                        if let Err(e) = source.commit_offsets(marker).await {
                            tracing::warn!("Reader: commit_offsets error on cancel: {}", e);
                        }
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
                            if let Err(e) = source.commit_offsets(marker).await {
                                tracing::warn!("Reader: commit_offsets error: {}", e);
                            }
                        }
                        backoff_ms = INITIAL_BACKOFF_MS;
                    }
                    continue;
                }
                _ = reader_token.cancelled() => {
                    while let Ok(ack) = rx_commit.try_recv() {
                        for marker in &ack.markers {
                            if let Err(e) = source.commit_offsets(marker).await {
                                tracing::warn!("Reader: commit_offsets error on cancel: {}", e);
                            }
                        }
                    }
                    return;
                }
            };

            let msg_batch = match msg_batch {
                Ok(ReadResult::Batch(batch)) if batch.messages.is_empty() => {
                    tokio::select! {
                        _ = sleep(Duration::from_millis(backoff_ms)) => {
                            backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
                            continue;
                        }
                        ack = rx_commit.recv() => {
                            if let Some(ack) = ack {
                                for marker in &ack.markers {
                                    if let Err(e) = source.commit_offsets(marker).await {
                                        tracing::warn!("Reader: commit_offsets error: {}", e);
                                    }
                                }
                                backoff_ms = INITIAL_BACKOFF_MS;
                            }
                            continue;
                        }
                        _ = reader_token.cancelled() => return,
                    }
                }
                Ok(ReadResult::Batch(batch)) => batch,
                Ok(ReadResult::Exhausted) => {
                    while let Ok(ack) = rx_commit.try_recv() {
                        for marker in &ack.markers {
                            if let Err(e) = source.commit_offsets(marker).await {
                                tracing::warn!("Reader: commit_offsets error on exhausted: {}", e);
                            }
                        }
                    }
                    tracing::info!("Reader: source exhausted — terminating pipeline");
                    return;
                }
                Ok(ReadResult::Failed(e)) => {
                    while let Ok(ack) = rx_commit.try_recv() {
                        for marker in &ack.markers {
                            if let Err(e) = source.commit_offsets(marker).await {
                                tracing::warn!("Reader: commit_offsets error before fail: {}", e);
                            }
                        }
                    }
                    tracing::error!("Reader: source failed — {}. Terminating pipeline.", e);
                    // Store fatal error for run_partition_pipeline to propagate.
                    *reader_fatal.lock().unwrap() = Some(e);
                    return;
                }
                Err(e) => {
                    tracing::error!("Read error: {}. Backing off {}ms", e, backoff_ms);
                    tokio::select! {
                        _ = sleep(Duration::from_millis(backoff_ms)) => {},
                        ack = rx_commit.recv() => {
                            if let Some(ack) = ack {
                                for marker in &ack.markers {
                                    if let Err(e) = source.commit_offsets(marker).await {
                                        tracing::warn!("Reader: commit_offsets error: {}", e);
                                    }
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

            // Drain acks before sending (prevents ack channel fill → writer stall)
            while let Ok(ack) = rx_commit.try_recv() {
                for marker in &ack.markers {
                    if let Err(e) = source.commit_offsets(marker).await {
                        tracing::warn!("Reader: commit_offsets error: {}", e);
                    }
                }
            }

            let item = ReadItem {
                messages: msg_batch.messages,
                partition_id: msg_batch.partition_id,
                commit_marker: msg_batch.commit_marker,
                dedup_token: msg_batch.dedup_token.clone(),
            };

            // Send + drain acks concurrently to avoid pipeline stall
            tokio::select! {
                result = tx_read.send(item) => {
                    if result.is_err() { return; }
                }
                ack = rx_commit.recv() => {
                    if let Some(ack) = ack {
                        for marker in &ack.markers {
                            if let Err(e) = source.commit_offsets(marker).await {
                                tracing::warn!("Reader: commit_offsets error: {}", e);
                            }
                        }
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
            let mut mw_error_count: u32 = 0;
            loop {
                if parser_token.is_cancelled() {
                    return;
                }
                let item = match rx_read.blocking_recv() {
                    Some(item) => item,
                    None => return,
                };

                let (valid, dlq) = match parser.parse_into(item.messages, item.partition_id, item.dedup_token, &mut workspace) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("Parser error: {}", e);
                        continue;
                    }
                };

                // Only valid data goes through middlewares; DLQ short-circuits.
                let valid = match apply_middlewares(valid, &middlewares) {
                    Ok(b) => {
                        mw_error_count = 0;
                        b
                    }
                    Err(e) => {
                        mw_error_count += 1;
                        tracing::error!("Middleware error (consecutive={}): {}", mw_error_count, e);
                        if mw_error_count >= MAX_CONSECUTIVE_MW_ERRORS {
                            tracing::error!(
                                "Aborting partition {}: {} consecutive middleware errors",
                                item.partition_id, mw_error_count,
                            );
                            let _ = tx_parsed_parser.blocking_send(ParsedMsg::Fatal);
                            return;
                        }
                        continue;
                    }
                };

                let parsed = ParsedItem { valid, dlq, commit_marker: item.commit_marker };
                if tx_parsed_parser.blocking_send(ParsedMsg::Item(Box::new(parsed))).is_err() {
                    return;
                }
            }
        })?;

    // --- Writer task ---
    let writer_token = cancel_token.clone();
    let sink_for_writer = sink.clone();
    let writer_fatal = fatal_error.clone();
    let writer_handle = tokio::spawn(async move {
        let mut acc = BatchAccumulator::new(batch_size);
        let mut total_flushed: u64 = 0;

        /// Common drain-and-ack helper used by the main loop and all cancel branches.
        /// Flushes all accumulated data; commits only on success.
        async fn drain_and_ack(
            sink: &dyn Sink,
            tx_commit: &mpsc::Sender<CommitAck>,
            acc: &mut BatchAccumulator,
        ) -> Option<usize> {
            let flush = acc.take_flush()?;
            match flush_to_sink_and_ack(sink, tx_commit, flush).await {
                Ok(rows) => {
                    acc.clear();
                    Some(rows)
                }
                Err(_) => {
                    acc.clear();
                    None
                }
            }
        }

        loop {
            // Phase 1: flush if timeout expired
            let should_flush = acc.window_start.is_some_and(|s| {
                acc.total_rows > 0 && s.elapsed() >= max_linger
            });

            if should_flush {
                if let Some(rows) = drain_and_ack(sink_for_writer.as_ref(), &tx_commit, &mut acc).await {
                    total_flushed += rows as u64;
                    tracing::info!(
                        "flush: partition={} total_flushed={} (linger)",
                        partition_id, total_flushed,
                    );
                }
                continue;
            }

            // Phase 2: check cancellation
            if writer_token.is_cancelled() {
                if let Some(rows) = drain_and_ack(sink_for_writer.as_ref(), &tx_commit, &mut acc).await {
                    total_flushed += rows as u64;
                }
                tracing::info!(
                    "partition={} finished: total_flushed={}",
                    partition_id, total_flushed,
                );
                return;
            }

            // Phase 3: wait for data or timeout (NO busy-spin)
            let timeout = acc.window_start.map(|s| {
                let elapsed = s.elapsed();
                if elapsed < max_linger { max_linger - elapsed } else { Duration::ZERO }
            });

            let maybe_item = if let Some(dur) = timeout {
                tokio::select! {
                    maybe = rx_parsed.recv() => maybe,
                    _ = sleep(dur) => None,
                    _ = writer_token.cancelled() => {
                        if let Some(rows) = drain_and_ack(sink_for_writer.as_ref(), &tx_commit, &mut acc).await {
                            total_flushed += rows as u64;
                        }
                        tracing::info!("partition={} finished: total_flushed={}", partition_id, total_flushed);
                        return;
                    }
                }
            } else {
                tokio::select! {
                    maybe = rx_parsed.recv() => maybe,
                    _ = writer_token.cancelled() => {
                        if let Some(rows) = drain_and_ack(sink_for_writer.as_ref(), &tx_commit, &mut acc).await {
                            total_flushed += rows as u64;
                        }
                        tracing::info!("partition={} finished: total_flushed={}", partition_id, total_flushed);
                        return;
                    }
                }
            };

            let msg = match maybe_item {
                Some(msg) => msg,
                None => {
                    // Timeout fired with empty or no accumulator — just loop.
                    continue;
                }
            };

            // Handle fatal sentinel from parser
            let item = match msg {
                ParsedMsg::Fatal => {
                    tracing::error!(
                        "partition={}: parser middleware guard triggered — aborting",
                        partition_id,
                    );
                    // Flush whatever we have (best-effort) then signal fatal.
                    let _ = drain_and_ack(sink_for_writer.as_ref(), &tx_commit, &mut acc).await;
                    *writer_fatal.lock().unwrap() = Some(anyhow::anyhow!(
                        "Parser middleware error limit ({}) exceeded",
                        MAX_CONSECUTIVE_MW_ERRORS,
                    ));
                    return;
                }
                ParsedMsg::Item(item) => *item,
            };

            // Push all tables of this item, attaching the marker once.
            let mut marker = item.commit_marker;
            if item.valid.batch.num_rows() > 0 {
                acc.push(item.valid, marker.take());
            }
            if let Some(dlq) = item.dlq {
                if dlq.batch.num_rows() > 0 {
                    acc.push(dlq, marker.take());
                }
            }
            // Item fully pushed — safe to check flush.
            if acc.should_flush() {
                if let Some(rows) = drain_and_ack(sink_for_writer.as_ref(), &tx_commit, &mut acc).await {
                    total_flushed += rows as u64;
                    tracing::info!(
                        "flush: partition={} total_flushed={} (batch full)",
                        partition_id, total_flushed,
                    );
                }
            }
        }
    });

    // Wait for reader first, then drop tx_parsed to signal parser
    let _ = reader_handle.await;
    drop(tx_parsed);
    let _ = parser_thread.join();
    let _ = writer_handle.await;

    if let Some(e) = fatal_error.lock().unwrap().take() {
        return Err(e);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the number of rows flushed, or an error.
/// **Writes all tables unconditionally, commits markers only after all succeed.**
async fn flush_to_sink_and_ack(
    sink: &dyn Sink,
    tx_commit: &mpsc::Sender<CommitAck>,
    flush: FlushBatch,
) -> Result<usize, ()> {
    let FlushBatch { writes, markers, .. } = flush;
    let total_rows: usize = writes.iter()
        .flat_map(|w| w.batches.iter().map(|b| b.num_rows()))
        .sum();

    // 1. Write ALL tables unconditionally (fixes L1: data without markers must be written).
    for write in &writes {
        if write.batches.is_empty() {
            continue;
        }
        if let Err(e) = sink.write(write.clone()).await {
            tracing::error!("Writer: flush error table={}: {}", write.table, e);
            return Err(());
        }
    }

    // 2. Ack only after ALL tables succeeded (at-least-once invariant).
    if !markers.is_empty()
        && tx_commit.send(CommitAck { markers }).await.is_err()
    {
        tracing::error!("Writer: commit ack channel closed");
        return Err(());
    }
    Ok(total_rows)
}

/// Applies middlewares to a `TableData`. DLQ batches (is_dlq == true) are
/// returned unchanged — middleware implementations are NOT called for DLQ
/// data (avoids schema mismatch panics in `FilterMiddleware`).
fn apply_middlewares(
    data: TableData,
    middlewares: &[Box<dyn Middleware>],
) -> anyhow::Result<TableData> {
    if data.is_dlq {
        return Ok(data);
    }
    let mut data = data;
    for mw in middlewares {
        data = mw.process(data)?;
    }
    Ok(data)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;

    // ---------- accumulator ----------

    fn make_td(table: &str, rows: usize, dlq: bool) -> TableData {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![1i64; rows]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        TableData { table: table.into(), is_dlq: dlq, batch, batch_id: 1, dedup_token: None }
    }

    #[test]
    fn accumulator_single_marker_for_two_tables() {
        let mut acc = BatchAccumulator::new(1000);
        let marker = CommitMarker::new(42usize);
        acc.push(make_td("t", 5, false), Some(marker.clone()));
        acc.push(make_td("t.dlq", 3, true), None); // marker already taken
        assert_eq!(acc.markers.len(), 1);
        let flush = acc.take_flush().unwrap();
        assert_eq!(flush.writes.len(), 2);
        assert_eq!(flush.markers.len(), 1);
    }

    #[test]
    fn accumulator_global_threshold() {
        let mut acc = BatchAccumulator::new(10);
        acc.push(make_td("t", 6, false), None);
        acc.push(make_td("t.dlq", 5, true), None);
        assert!(acc.should_flush()); // 11 >= 10
    }

    #[test]
    fn accumulator_deterministic_order() {
        let mut acc = BatchAccumulator::new(100);
        acc.push(make_td("main", 1, false), None);
        acc.push(make_td("main.dlq", 1, true), None);
        let flush = acc.take_flush().unwrap();
        assert_eq!(flush.writes[0].table.as_ref(), "main");
        assert_eq!(flush.writes[1].table.as_ref(), "main.dlq");
    }

    #[test]
    fn accumulator_empty_batch_skipped() {
        let mut acc = BatchAccumulator::new(100);
        acc.push(make_td("t", 0, false), None); // 0 rows
        assert!(acc.is_empty());
        assert!(acc.take_flush().is_none());
    }

    // ---------- middleware short-circuit ----------

    struct CountingMw {
        count: std::sync::atomic::AtomicU32,
    }
    impl Middleware for CountingMw {
        fn process(&self, data: TableData) -> anyhow::Result<TableData> {
            self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(data)
        }
    }

    #[test]
    fn middleware_skips_dlq() {
        let mw = Arc::new(CountingMw { count: 0.into() });
        let count_ref = mw.clone();
        let _mws: Vec<Box<dyn Middleware>> = vec![Box::new(count_ref.clone())];

        let dlq = make_td("t.dlq", 5, true);
        let _ = apply_middlewares(dlq, &_mws).unwrap();
        assert_eq!(count_ref.count.load(std::sync::atomic::Ordering::Relaxed), 0,
            "DLQ must NOT trigger middleware");
    }

    #[test]
    fn middleware_runs_on_valid() {
        let mw = Arc::new(CountingMw { count: 0.into() });
        let count_ref = mw.clone();
        let mws: Vec<Box<dyn Middleware>> = vec![Box::new(mw)];

        let valid = make_td("t", 5, false);
        let _ = apply_middlewares(valid, &mws).unwrap();
        assert_eq!(count_ref.count.load(std::sync::atomic::Ordering::Relaxed), 1,
            "Valid batch MUST trigger middleware");
    }
}
