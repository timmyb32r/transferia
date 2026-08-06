pub mod source;
pub mod middleware;
pub mod sink;
pub mod poisoning;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::parser::{Parser, ParserWorkspace};
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::Sink;
use crate::types::exactly_once::ExactlyOnceKey;
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

enum ReadItem {
    /// Raw messages for the parser (YDS, S3, PQv1).
    Messages {
        messages: Vec<crate::types::message::Message>,
        partition_id: i64,
        commit_marker: Option<CommitMarker>,
    },
    /// Pre-parsed Arrow batches (ClickHouse source) — passthrough, zero copy.
    Arrow {
        batches: Vec<arrow::record_batch::RecordBatch>,
        partition_id: i64,
        commit_marker: Option<CommitMarker>,
    },
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
    /// Exactly-once key descriptor of the current accumulation window, taken
    /// from the first keyed `TableData` (all items of one flush share the
    /// source's key descriptor). `Some` also marks the window as exactly-once:
    /// the size-limit flush is then evaluated only at Message boundaries
    /// (§3.1). `take_flush`/`clear` reset it, so the next Message starts a
    /// fresh accumulation window.
    exactly_once_key: Option<ExactlyOnceKey>,
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
            exactly_once_key: None,
        }
    }

    /// Push a `TableData` into the accumulator. Never triggers a flush (invariant A1).
    /// Empty batches (0 rows) are silently skipped — they never reach the sink
    /// (with exactly-once, a 0-row batch carries no offsets to deduplicate).
    ///
    /// **Exactly-once (§3.1):** a keyed `TableData` is one complete Message —
    /// an atomic unit. The accumulator never splits a Message between two
    /// `TableWrite` flushes: the size-limit flush is only consulted by the
    /// caller after all tables of a Message have been pushed, and the flush
    /// then carries the current Message whole, even when the flush total
    /// exceeds `batch_size`. A single Message that alone exceeds `batch_size`
    /// cannot be held in a flush window at all — a configuration error,
    /// reported as `message too large for exactly-once batch` (fatal).
    fn push(&mut self, td: TableData, marker: Option<CommitMarker>) -> anyhow::Result<()> {
        if td.batch.num_rows() == 0 {
            return Ok(());
        }
        if td.exactly_once_key.is_some() && td.batch.num_rows() > self.batch_size {
            return Err(anyhow::anyhow!(
                "message too large for exactly-once batch: {} rows exceed batch_size {}; \
                 raise sink_batch_size or reduce the message size",
                td.batch.num_rows(),
                self.batch_size,
            ));
        }
        if self.window_start.is_none() {
            self.window_start = Some(Instant::now());
        }
        // Keep the first non-None exactly_once_key; all items in a flush share
        // the same source batch and therefore the same key descriptor. Its
        // Some-ness marks the current accumulation window as exactly-once: the
        // size-limit flush is then evaluated only at Message boundaries.
        if self.exactly_once_key.is_none() {
            self.exactly_once_key = td.exactly_once_key.clone();
        }
        let rows = td.batch.num_rows();
        let entry = self.tables.entry(td.table.clone()).or_insert_with(|| {
            self.order.push(td.table.clone());
            TableWrite {
                table: td.table.clone(), batches: Vec::new(),
                exactly_once_key: None,
            }
        });
        entry.batches.push(td.batch);
        self.total_rows += rows;
        if let Some(m) = marker {
            self.markers.push(m);
        }
        Ok(())
    }

    /// True when the accumulated rows reach the batch-size limit.
    ///
    /// **Exactly-once contract (§3.1):** while the window is exactly-once
    /// (`exactly_once_key` is `Some`), the caller MUST consult this only
    /// after pushing all tables of one complete Message. A flush then
    /// includes the current Message whole — it is never split between two
    /// `TableWrite`s — even when the flush total exceeds `batch_size`, and
    /// the next Message starts a fresh accumulation window (`take_flush`
    /// fully resets the accumulator state). Splitting a Message would let
    /// the sink's waterline advance past a partially-written offset and
    /// lose the tail of the Message on replay (§0, I5).
    fn should_flush(&self) -> bool {
        self.total_rows >= self.batch_size
    }

    fn is_empty(&self) -> bool {
        self.total_rows == 0
    }

    /// Take ALL buffered data + all markers. Returns `None` when empty.
    ///
    /// In exactly-once mode the flush is atomic per Message: it contains
    /// whole Messages only (never a partial one), and the exactly-once state
    /// is reset so the next push starts a fresh accumulation window.
    fn take_flush(&mut self) -> Option<FlushBatch> {
        if self.is_empty() {
            return None;
        }
        let key = self.exactly_once_key.take();
        let writes: Vec<TableWrite> = self
            .order
            .iter()
            .filter_map(|name| self.tables.remove(name.as_ref()))
            .filter(|w| !w.batches.is_empty())
            .map(|w| TableWrite { exactly_once_key: key.clone(), ..w })
            .collect();
        let markers = std::mem::take(&mut self.markers);
        self.order.clear();
        self.tables.clear();
        self.total_rows = 0;
        self.exactly_once_key = None;
        Some(FlushBatch { writes, markers })
    }

    fn clear(&mut self) {
        self.tables.clear();
        self.order.clear();
        self.markers.clear();
        self.total_rows = 0;
        self.window_start = None;
        self.exactly_once_key = None;
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
    parser: Option<Arc<dyn Parser>>,
    table: Arc<str>,
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

    /// Commit marker to source with up to 10 retries.
    /// Spec §0 (I3): source.commit fails after retries → poison → fatal.
    /// TODO: wire into all reader commit_offsets call sites (currently uses manual backoff).
    #[allow(dead_code)]
    async fn commit_with_retry(source: &mut Box<dyn Source>, marker: &CommitMarker) -> anyhow::Result<()> {
        let mut last_err = None;
        for attempt in 0..10 {
            match source.commit_offsets(marker).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 9 {
                        tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << attempt.min(6)))).await;
                    }
                }
            }
        }
        Err(anyhow::anyhow!("commit_offsets failed after 10 retries: {:?}", last_err))
    }

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
                Ok(ReadResult::Arrow(batches)) if batches.iter().all(|b| b.num_rows() == 0) => {
                    // Empty Arrow batches — backoff, same as empty message batch.
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
                Ok(ReadResult::Arrow(batches)) => {
                    backoff_ms = INITIAL_BACKOFF_MS;
                    // Drain acks before sending
                    while let Ok(ack) = rx_commit.try_recv() {
                        for marker in &ack.markers {
                            if let Err(e) = source.commit_offsets(marker).await {
                                tracing::warn!("Reader: commit_offsets error: {}", e);
                            }
                        }
                    }
                    let item = ReadItem::Arrow {
                        batches,
                        partition_id: 0, // CH source uses a single partition
                        commit_marker: None,
                    };
                    // Send + drain acks concurrently
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
                            continue;
                        }
                        _ = reader_token.cancelled() => return,
                    }
                    continue;
                }
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

            let item = ReadItem::Messages {
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
    let parser_for_thread = parser.clone();
    let table_for_thread = table.clone();
    let middlewares_for_thread = middlewares.clone();
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

                let (valid, dlq, marker) = match item {
                    ReadItem::Messages { messages, partition_id, commit_marker } => {
                        let p = match &parser_for_thread {
                            Some(p) => p,
                            None => {
                                tracing::error!("Messages received but no parser configured");
                                continue;
                            }
                        };
                        let (valid, dlq) = match p.parse_into(messages, partition_id, None, &mut workspace) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Parser error: {}", e);
                                continue;
                            }
                        };
                        (valid, dlq, commit_marker)
                    }
                    ReadItem::Arrow { batches, partition_id: _pid, commit_marker } => {
                        // Passthrough: build TableData directly from Arrow batches.
                        // Zero-copy — RecordBatch is Arc-backed, clones are refcount bumps.
                        let batch_refs: Vec<&arrow::record_batch::RecordBatch> = batches.iter().collect();
                        let single = match arrow::compute::concat_batches(
                            &batches[0].schema(), batch_refs,
                        ) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::error!("Arrow concat_batches error: {}", e);
                                continue;
                            }
                        };
                        let td = TableData {
                            table: table_for_thread.clone(),
                            is_dlq: false,
                            batch: single,
                            batch_id: crate::batch_id(),
                            exactly_once_key: None, // CH source is at-least-once
                        };
                        (td, None, commit_marker)
                    }
                };

                // Only valid data goes through middlewares; DLQ short-circuits.
                let valid = match apply_middlewares(valid, &middlewares_for_thread) {
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
                                mw_error_count,
                                mw_error_count,
                            );
                            let _ = tx_parsed_parser.blocking_send(ParsedMsg::Fatal);
                            return;
                        }
                        continue;
                    }
                };

                let parsed = ParsedItem { valid, dlq, commit_marker: marker };
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

        /// Drain-and-ack: flush, commit on success, propagate sink errors.
        /// **Exactly-once fix (spec §6):** sink error is propagated as Err,
        /// not swallowed. Silent `acc.clear(); None` = data loss.
        async fn drain_and_ack(
            sink: &dyn Sink,
            tx_commit: &mpsc::Sender<CommitAck>,
            acc: &mut BatchAccumulator,
        ) -> anyhow::Result<usize> {
            let flush = acc.take_flush()
                .ok_or_else(|| anyhow::anyhow!("drain_and_ack: no flush data"))?;
            let rows = flush_to_sink_and_ack(sink, tx_commit, flush).await?;
            acc.clear();
            Ok(rows)
        }

        /// Helper: drain or set fatal error and exit writer loop.
        /// Returns `Some(rows)` on success, `None` on empty accumulator,
        /// and sets `writer_fatal` + returns `None` on sink error.
        async fn drain_or_fatal(
            sink: &dyn Sink,
            tx_commit: &mpsc::Sender<CommitAck>,
            acc: &mut BatchAccumulator,
            fatal: &Arc<std::sync::Mutex<Option<anyhow::Error>>>,
        ) -> Option<usize> {
            match drain_and_ack(sink, tx_commit, acc).await {
                Ok(rows) => Some(rows),
                Err(e) => {
                    *fatal.lock().unwrap() = Some(e);
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
                if let Some(rows) = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await {
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
                if let Some(rows) = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await {
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
                        if let Some(rows) = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await {
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
                        if let Some(rows) = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await {
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
                    let _ = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await;
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
                if let Err(e) = acc.push(item.valid, marker.take()) {
                    // Exactly-once: oversized Message — configuration error → fatal.
                    tracing::error!("partition={}: {}", partition_id, e);
                    let _ = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await;
                    *writer_fatal.lock().unwrap() = Some(e);
                    return;
                }
            }
            if let Some(dlq) = item.dlq {
                if dlq.batch.num_rows() > 0 {
                    if let Err(e) = acc.push(dlq, marker.take()) {
                        tracing::error!("partition={}: {}", partition_id, e);
                        let _ = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await;
                        *writer_fatal.lock().unwrap() = Some(e);
                        return;
                    }
                }
            }
            // Item fully pushed — Message boundary reached — safe to check flush
            // (in exactly-once mode the current Message is flushed whole here).
            if acc.should_flush() {
                if let Some(rows) = drain_or_fatal(sink_for_writer.as_ref(), &tx_commit, &mut acc, &writer_fatal).await {
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
) -> anyhow::Result<usize> {
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
            return Err(anyhow::anyhow!("flush error"));
        }
    }

    // 2. Ack only after ALL tables succeeded (at-least-once invariant).
    if !markers.is_empty()
        && tx_commit.send(CommitAck { markers }).await.is_err()
    {
        tracing::error!("Writer: commit ack channel closed");
        return Err(anyhow::anyhow!("flush error"));
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
        TableData { table: table.into(), is_dlq: dlq, batch, batch_id: 1, exactly_once_key: None }
    }

    /// Like `make_td`, but as one complete exactly-once Message (keyed).
    fn make_td_keyed(table: &str, rows: usize, dlq: bool) -> TableData {
        use crate::types::exactly_once::ExactlyOnceColumn;
        let mut td = make_td(table, rows, dlq);
        td.exactly_once_key = Some(ExactlyOnceKey {
            partition: ExactlyOnceColumn { name: "__system_partition".into() },
            offset: ExactlyOnceColumn { name: "__system_offset".into() },
        });
        td
    }

    #[test]
    fn accumulator_single_marker_for_two_tables() {
        let mut acc = BatchAccumulator::new(1000);
        let marker = CommitMarker::new(42usize);
        acc.push(make_td("t", 5, false), Some(marker.clone())).unwrap();
        acc.push(make_td("t.dlq", 3, true), None).unwrap(); // marker already taken
        assert_eq!(acc.markers.len(), 1);
        let flush = acc.take_flush().unwrap();
        assert_eq!(flush.writes.len(), 2);
        assert_eq!(flush.markers.len(), 1);
    }

    #[test]
    fn accumulator_global_threshold() {
        let mut acc = BatchAccumulator::new(10);
        acc.push(make_td("t", 6, false), None).unwrap();
        acc.push(make_td("t.dlq", 5, true), None).unwrap();
        assert!(acc.should_flush()); // 11 >= 10
    }

    #[test]
    fn accumulator_deterministic_order() {
        let mut acc = BatchAccumulator::new(100);
        acc.push(make_td("main", 1, false), None).unwrap();
        acc.push(make_td("main.dlq", 1, true), None).unwrap();
        let flush = acc.take_flush().unwrap();
        assert_eq!(flush.writes[0].table.as_ref(), "main");
        assert_eq!(flush.writes[1].table.as_ref(), "main.dlq");
    }

    #[test]
    fn accumulator_empty_batch_skipped() {
        let mut acc = BatchAccumulator::new(100);
        acc.push(make_td("t", 0, false), None).unwrap(); // 0 rows
        assert!(acc.is_empty());
        assert!(acc.take_flush().is_none());
    }

    #[test]
    fn accumulator_empty_keyed_batch_filtered() {
        // 0-row batches with exactly_once_key = Some must never reach the sink.
        let mut acc = BatchAccumulator::new(100);
        acc.push(make_td_keyed("t", 0, false), None).unwrap();
        assert!(acc.is_empty());
        assert!(acc.take_flush().is_none());
    }

    #[test]
    fn accumulator_exactly_once_flush_at_message_boundary() {
        // §3.1: the size-limit flush fires only after the full Message (all its
        // tables) has been pushed, and includes the current Message whole —
        // even when the flush total exceeds batch_size. The next Message then
        // starts a fresh accumulation window.
        let mut acc = BatchAccumulator::new(10);
        let marker = CommitMarker::new(7usize);
        acc.push(make_td_keyed("main", 8, false), Some(marker.clone())).unwrap();
        assert!(!acc.should_flush(), "mid-Message check must not flush (8 < 10)");
        acc.push(make_td_keyed("t.dlq", 5, true), None).unwrap();
        assert!(acc.should_flush(), "13 >= 10 at the Message boundary");
        let flush = acc.take_flush().unwrap();
        assert_eq!(flush.markers.len(), 1);
        assert_eq!(flush.writes.len(), 2);
        let main = flush.writes.iter().find(|w| w.table.as_ref() == "main").unwrap();
        let dlq = flush.writes.iter().find(|w| w.table.as_ref() == "t.dlq").unwrap();
        assert_eq!(main.batches[0].num_rows(), 8, "current Message never split");
        assert_eq!(dlq.batches[0].num_rows(), 5);
        assert!(main.exactly_once_key.is_some() && dlq.exactly_once_key.is_some());
        // Next Message starts a fresh window (exactly-once state reset).
        assert!(acc.is_empty());
        assert!(acc.exactly_once_key.is_none());
    }

    #[test]
    fn accumulator_exactly_once_oversized_message_fatal() {
        // §3.1: a single Message that alone exceeds batch_size cannot fit in a
        // flush window — configuration error, never split across flushes.
        let mut acc = BatchAccumulator::new(10);
        let err = acc.push(make_td_keyed("main", 11, false), None).unwrap_err();
        assert!(
            err.to_string().contains("message too large for exactly-once batch"),
            "unexpected error: {err}",
        );
        assert!(acc.is_empty(), "oversized Message must not be accumulated");
    }

    #[test]
    fn accumulator_at_least_once_oversized_batch_allowed() {
        // Without a key there is no per-Message atomicity requirement — a batch
        // larger than batch_size is not a configuration error.
        let mut acc = BatchAccumulator::new(10);
        acc.push(make_td("main", 11, false), None).unwrap();
        assert!(acc.should_flush());
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
