//! Per-partition throughput + duty-cycle metrics, printed to the console by a
//! background stats reporter.
//!
//! Two counter sets, both per partition:
//! - [`SourceCounters`] — filled by every source provider (messages, compressed
//!   and decompressed bytes, downloader and decompressor busy time).
//! - [`ParseCounters`] — filled by the parser thread (rows, Arrow bytes, DLQ
//!   rows, source messages, parser busy time).
//!
//! [`MetricsRegistry`] merges them by `partition_id` (source and parse counters
//! are registered independently — the source by the `PQv1` provider inside
//! `build_source`, the parse counters by `main`). [`spawn_stats_reporter`]
//! snapshots the registry every `interval_ms` and prints a per-partition
//! (or aggregated) line via `tracing::info!`.

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use alloc::sync::Arc;
use schemars::JsonSchema;
use serde::Deserialize;
use tokio::task::JoinHandle;

use crate::compatibility::DeliveryGuarantee;

const RELAXED: Ordering = Ordering::Relaxed;
/// Nanoseconds per second (f64) — a typed const avoids the `*_literal_suffix`
/// contradiction (`separated` vs `unseparated` are both enabled here).
const NANOS_PER_SEC_F: f64 = 1_000_000_000.0;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_interval_ms")]
    pub interval_ms: u64,
    #[serde(default)]
    pub per_partition: bool,
}

const fn default_metrics_interval_ms() -> u64 {
    1000
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// Per-partition source counters (`PQv1`). Filled by the background session
/// (bytes + response-wait/decompress duty) and `read_batch` (messages).
pub struct SourceCounters {
    messages: AtomicU64,
    compressed_bytes: AtomicU64,
    decompressed_bytes: AtomicU64,
    response_wait_nanos: AtomicU64,
    decomp_busy_nanos: AtomicU64,
}

impl SourceCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            messages: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            decompressed_bytes: AtomicU64::new(0),
            response_wait_nanos: AtomicU64::new(0),
            decomp_busy_nanos: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_messages(&self, n: u64) {
        self.messages.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_compressed_bytes(&self, n: u64) {
        self.compressed_bytes.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_decompressed_bytes(&self, n: u64) {
        self.decompressed_bytes.fetch_add(n, RELAXED);
    }
    /// Wall time spent awaiting the next PQ server response. This includes
    /// data and control-plane responses and is latency, not CPU utilization.
    #[inline]
    pub fn add_response_wait(&self, d: Duration) {
        self.response_wait_nanos
            .fetch_add(d.as_nanos() as u64, RELAXED);
    }
    /// Decompressor busy = time inside `decompress()`.
    #[inline]
    pub fn add_decomp_busy(&self, d: Duration) {
        self.decomp_busy_nanos
            .fetch_add(d.as_nanos() as u64, RELAXED);
    }
}

impl Default for SourceCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-partition parser-output counters. Filled by the parser thread after
/// `parse_into` (rows/bytes/DLQ/source messages) and around parse work (busy
/// time).
pub struct ParseCounters {
    rows: AtomicU64,
    arrow_bytes: AtomicU64,
    dlq_rows: AtomicU64,
    source_messages: AtomicU64,
    parse_busy_nanos: AtomicU64,
}

impl ParseCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rows: AtomicU64::new(0),
            arrow_bytes: AtomicU64::new(0),
            dlq_rows: AtomicU64::new(0),
            source_messages: AtomicU64::new(0),
            parse_busy_nanos: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_rows(&self, n: u64) {
        self.rows.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_arrow_bytes(&self, n: u64) {
        self.arrow_bytes.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_dlq_rows(&self, n: u64) {
        self.dlq_rows.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_source_messages(&self, n: u64) {
        self.source_messages.fetch_add(n, RELAXED);
    }
    /// Parser busy = time spent parsing one delivery and applying its middlewares.
    #[inline]
    pub fn add_parse_busy(&self, d: Duration) {
        self.parse_busy_nanos
            .fetch_add(d.as_nanos() as u64, RELAXED);
    }
}

impl Default for ParseCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Counters for one partition's sink output.
///
/// The sink actor records successful writes, rows, Arrow bytes, acknowledged
/// source messages, and time actively spent in destination I/O attempts.
pub struct SinkCounters {
    rows: AtomicU64,
    bytes: AtomicU64,
    flushes: AtomicU64,
    source_messages: AtomicU64,
    busy_nanos: AtomicU64,
    sink_retries: AtomicU64,
    buffered_bytes: AtomicU64,
    open_objects: AtomicU64,
    ready_objects: AtomicU64,
    inflight_objects: AtomicU64,
    backpressure_nanos: AtomicU64,
}

impl SinkCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            source_messages: AtomicU64::new(0),
            busy_nanos: AtomicU64::new(0),
            sink_retries: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            open_objects: AtomicU64::new(0),
            ready_objects: AtomicU64::new(0),
            inflight_objects: AtomicU64::new(0),
            backpressure_nanos: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_rows(&self, n: u64) {
        self.rows.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_bytes(&self, n: u64) {
        self.bytes.fetch_add(n, RELAXED);
    }
    #[inline]
    pub fn add_flush(&self) {
        self.flushes.fetch_add(1, RELAXED);
    }
    #[inline]
    pub fn add_source_messages(&self, n: u64) {
        self.source_messages.fetch_add(n, RELAXED);
    }
    /// Sink busy excludes buffering and retry backoff. `ClickHouse` records its
    /// serial INSERT attempt time; S3 sums concurrent object-upload attempts,
    /// so an S3 aggregate can exceed wall-clock time and 100% utilization.
    #[inline]
    pub fn add_busy(&self, d: Duration) {
        self.busy_nanos.fetch_add(d.as_nanos() as u64, RELAXED);
    }
    #[inline]
    pub fn add_retries(&self, retries: u64) {
        self.sink_retries.fetch_add(retries, RELAXED);
    }
    #[inline]
    pub fn set_buffered_bytes(&self, bytes: u64) {
        self.buffered_bytes.store(bytes, RELAXED);
    }
    #[inline]
    pub fn set_open_objects(&self, objects: u64) {
        self.open_objects.store(objects, RELAXED);
    }
    #[inline]
    pub fn set_ready_objects(&self, objects: u64) {
        self.ready_objects.store(objects, RELAXED);
    }
    #[inline]
    pub fn set_inflight_objects(&self, objects: u64) {
        self.inflight_objects.store(objects, RELAXED);
    }
    #[inline]
    pub fn add_backpressure(&self, duration: Duration) {
        self.backpressure_nanos
            .fetch_add(duration.as_nanos() as u64, RELAXED);
    }

    #[must_use]
    pub fn rows_total(&self) -> u64 {
        self.rows.load(RELAXED)
    }
    #[must_use]
    pub fn flushes_total(&self) -> u64 {
        self.flushes.load(RELAXED)
    }
    #[must_use]
    pub fn source_messages_total(&self) -> u64 {
        self.source_messages.load(RELAXED)
    }
    #[must_use]
    pub fn retries_total(&self) -> u64 {
        self.sink_retries.load(RELAXED)
    }
}

impl Default for SinkCounters {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct SourceSnapshot {
    messages: u64,
    compressed_bytes: u64,
    decompressed_bytes: u64,
    response_wait_nanos: u64,
    decomp_busy_nanos: u64,
}

#[derive(Clone, Copy, Default)]
struct ParseSnapshot {
    rows: u64,
    arrow_bytes: u64,
    dlq_rows: u64,
    source_messages: u64,
    parse_busy_nanos: u64,
}

#[derive(Clone, Copy, Default)]
struct SinkSnapshot {
    rows: u64,
    bytes: u64,
    flushes: u64,
    source_messages: u64,
    busy_nanos: u64,
    sink_retries: u64,
    buffered_bytes: u64,
    open_objects: u64,
    ready_objects: u64,
    inflight_objects: u64,
    backpressure_nanos: u64,
}

fn src_snap(c: Option<&Arc<SourceCounters>>) -> SourceSnapshot {
    c.map_or_else(SourceSnapshot::default, |s| SourceSnapshot {
        messages: s.messages.load(RELAXED),
        compressed_bytes: s.compressed_bytes.load(RELAXED),
        decompressed_bytes: s.decompressed_bytes.load(RELAXED),
        response_wait_nanos: s.response_wait_nanos.load(RELAXED),
        decomp_busy_nanos: s.decomp_busy_nanos.load(RELAXED),
    })
}

fn parse_snap(c: Option<&Arc<ParseCounters>>) -> ParseSnapshot {
    c.map_or_else(ParseSnapshot::default, |p| ParseSnapshot {
        rows: p.rows.load(RELAXED),
        arrow_bytes: p.arrow_bytes.load(RELAXED),
        dlq_rows: p.dlq_rows.load(RELAXED),
        source_messages: p.source_messages.load(RELAXED),
        parse_busy_nanos: p.parse_busy_nanos.load(RELAXED),
    })
}

fn sink_snap(c: Option<&Arc<SinkCounters>>) -> SinkSnapshot {
    c.map_or_else(SinkSnapshot::default, |s| SinkSnapshot {
        rows: s.rows.load(RELAXED),
        bytes: s.bytes.load(RELAXED),
        flushes: s.flushes.load(RELAXED),
        source_messages: s.source_messages.load(RELAXED),
        busy_nanos: s.busy_nanos.load(RELAXED),
        sink_retries: s.sink_retries.load(RELAXED),
        buffered_bytes: s.buffered_bytes.load(RELAXED),
        open_objects: s.open_objects.load(RELAXED),
        ready_objects: s.ready_objects.load(RELAXED),
        inflight_objects: s.inflight_objects.load(RELAXED),
        backpressure_nanos: s.backpressure_nanos.load(RELAXED),
    })
}

// ---------------------------------------------------------------------------
// Registry (merges source + parse + sink counters by partition_id)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PartitionMetrics {
    parses_rows: bool,
    delivery_guarantee: Option<DeliveryGuarantee>,
    source: Option<Arc<SourceCounters>>,
    parse: Option<Arc<ParseCounters>>,
    sink: Option<Arc<SinkCounters>>,
}

/// Read-only snapshot of one partition's counters (cloned Arcs), produced by
/// [`MetricsRegistry::snapshot`] for the reporter to iterate without holding
/// the registry lock.
struct PartitionSnapshot {
    pid: i64,
    parses_rows: bool,
    delivery_guarantee: Option<DeliveryGuarantee>,
    source: Option<Arc<SourceCounters>>,
    parse: Option<Arc<ParseCounters>>,
    sink: Option<Arc<SinkCounters>>,
}

pub struct MetricsRegistry {
    inner: Mutex<HashMap<i64, PartitionMetrics>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the MutexGuard must outlive the entry borrow it hands out"
    )]
    pub fn register_source(&self, partition_id: i64, c: Arc<SourceCounters>) {
        let mut m = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_default();
        entry.source = Some(c);
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the MutexGuard must outlive the entry borrow it hands out"
    )]
    pub fn register_parse(&self, partition_id: i64, parses_rows: bool, c: Arc<ParseCounters>) {
        let mut m = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_default();
        entry.parses_rows = parses_rows;
        entry.parse = Some(c);
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the MutexGuard must outlive the entry borrow it hands out"
    )]
    pub fn register_sink(&self, partition_id: i64, c: Arc<SinkCounters>) {
        let mut m = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_default();
        entry.sink = Some(c);
    }

    /// Store the independently inferred end-to-end delivery guarantee.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the MutexGuard must outlive the entry borrow it hands out"
    )]
    pub fn set_delivery_guarantee(&self, partition_id: i64, guarantee: DeliveryGuarantee) {
        let mut m = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_default();
        entry.delivery_guarantee = Some(guarantee);
    }

    fn snapshot(&self) -> Vec<PartitionSnapshot> {
        let m = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        m.iter()
            .map(|(pid, pm)| PartitionSnapshot {
                pid: *pid,
                parses_rows: pm.parses_rows,
                delivery_guarantee: pm.delivery_guarantee,
                source: pm.source.clone(),
                parse: pm.parse.clone(),
                sink: pm.sink.clone(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Reporter
// ---------------------------------------------------------------------------

/// Spawn a background task that prints per-partition (or aggregated) throughput
/// + duty-cycle lines every `interval_ms`. Returns the task handle.
pub fn spawn_stats_reporter(
    registry: Arc<MetricsRegistry>,
    interval_ms: u64,
    per_partition: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_millis(interval_ms.max(1));
        // last snapshot per partition: (source, parse, time)
        let mut last: HashMap<i64, (SourceSnapshot, ParseSnapshot, SinkSnapshot, Instant)> =
            HashMap::new();
        let mut primed = false;
        let mut proc_stats = ProcessStats::new();
        loop {
            tokio::time::sleep(interval).await;
            let now = Instant::now();
            let (cpu_pct, rss) = proc_stats.snapshot();
            let parts = registry.snapshot();

            // First tick: prime snapshots so the next tick has a real delta.
            if !primed {
                for pm in &parts {
                    last.insert(
                        pm.pid,
                        (
                            src_snap(pm.source.as_ref()),
                            parse_snap(pm.parse.as_ref()),
                            sink_snap(pm.sink.as_ref()),
                            now,
                        ),
                    );
                }
                primed = true;
                continue;
            }

            if per_partition {
                for pm in &parts {
                    let cur_src = src_snap(pm.source.as_ref());
                    let cur_parse = parse_snap(pm.parse.as_ref());
                    let cur_sink = sink_snap(pm.sink.as_ref());
                    let (psrc, pparse, psink, ptime) = last
                        .get(&pm.pid)
                        .copied()
                        .unwrap_or((cur_src, cur_parse, cur_sink, now));
                    let wall_ns = now.saturating_duration_since(ptime).as_nanos() as u64;
                    if wall_ns > 0 {
                        tracing::info!(
                            "{}",
                            format_line(
                                pm.pid,
                                pm.parses_rows,
                                pm.delivery_guarantee,
                                cur_src,
                                psrc,
                                cur_parse,
                                pparse,
                                cur_sink,
                                psink,
                                wall_ns,
                                cpu_pct,
                                rss
                            )
                        );
                    }
                    last.insert(pm.pid, (cur_src, cur_parse, cur_sink, now));
                }
            } else {
                let line = aggregate_line(&parts, &mut last, now, cpu_pct, rss);
                if let Some(l) = line {
                    tracing::info!("{l}");
                }
            }
        }
    })
}

fn aggregate_line(
    parts: &[PartitionSnapshot],
    last: &mut HashMap<i64, (SourceSnapshot, ParseSnapshot, SinkSnapshot, Instant)>,
    now: Instant,
    cpu_pct: u64,
    rss: u64,
) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let mut s = SourceSnapshot::default();
    let mut p = ParseSnapshot::default();
    let mut k = SinkSnapshot::default();
    let mut wall_ns_sum: u64 = 0;
    let mut any_row_parser = false;
    let mut delivery_guarantee = None;
    let mut mixed_guarantees = false;
    for pm in parts {
        let cur_src = src_snap(pm.source.as_ref());
        let cur_parse = parse_snap(pm.parse.as_ref());
        let cur_sink = sink_snap(pm.sink.as_ref());
        let (psrc, pparse, psink, ptime) = last
            .get(&pm.pid)
            .copied()
            .unwrap_or((cur_src, cur_parse, cur_sink, now));
        let wall = now.saturating_duration_since(ptime).as_nanos() as u64;
        if wall > 0 {
            s.messages += cur_src.messages.saturating_sub(psrc.messages);
            s.compressed_bytes += cur_src
                .compressed_bytes
                .saturating_sub(psrc.compressed_bytes);
            s.decompressed_bytes += cur_src
                .decompressed_bytes
                .saturating_sub(psrc.decompressed_bytes);
            s.response_wait_nanos += cur_src
                .response_wait_nanos
                .saturating_sub(psrc.response_wait_nanos);
            s.decomp_busy_nanos += cur_src
                .decomp_busy_nanos
                .saturating_sub(psrc.decomp_busy_nanos);
            p.rows += cur_parse.rows.saturating_sub(pparse.rows);
            p.arrow_bytes += cur_parse.arrow_bytes.saturating_sub(pparse.arrow_bytes);
            p.dlq_rows += cur_parse.dlq_rows.saturating_sub(pparse.dlq_rows);
            p.source_messages += cur_parse
                .source_messages
                .saturating_sub(pparse.source_messages);
            p.parse_busy_nanos += cur_parse
                .parse_busy_nanos
                .saturating_sub(pparse.parse_busy_nanos);
            k.rows += cur_sink.rows.saturating_sub(psink.rows);
            k.bytes += cur_sink.bytes.saturating_sub(psink.bytes);
            k.flushes += cur_sink.flushes.saturating_sub(psink.flushes);
            k.source_messages += cur_sink
                .source_messages
                .saturating_sub(psink.source_messages);
            k.busy_nanos += cur_sink.busy_nanos.saturating_sub(psink.busy_nanos);
            k.sink_retries += cur_sink.sink_retries.saturating_sub(psink.sink_retries);
            k.buffered_bytes += cur_sink.buffered_bytes;
            k.open_objects += cur_sink.open_objects;
            k.ready_objects += cur_sink.ready_objects;
            k.inflight_objects += cur_sink.inflight_objects;
            k.backpressure_nanos += cur_sink
                .backpressure_nanos
                .saturating_sub(psink.backpressure_nanos);
            wall_ns_sum += wall;
            any_row_parser |= pm.parses_rows;
            if let Some(current) = pm.delivery_guarantee {
                if delivery_guarantee.is_some_and(|known| known != current) {
                    mixed_guarantees = true;
                } else {
                    delivery_guarantee = Some(current);
                }
            }
        }
        last.insert(pm.pid, (cur_src, cur_parse, cur_sink, now));
    }
    if wall_ns_sum == 0 {
        return None;
    }
    // Average busy% across partitions: total busy / sum of per-partition walls.
    Some(format_line_avg(
        s,
        p,
        k,
        any_row_parser,
        if mixed_guarantees {
            "mixed"
        } else {
            delivery_guarantee_name(delivery_guarantee)
        },
        wall_ns_sum,
        cpu_pct,
        rss,
    ))
}

fn format_line(
    pid: i64,
    parses_rows: bool,
    delivery_guarantee: Option<DeliveryGuarantee>,
    cur_src: SourceSnapshot,
    prev_src: SourceSnapshot,
    cur_parse: ParseSnapshot,
    prev_parse: ParseSnapshot,
    cur_sink: SinkSnapshot,
    prev_sink: SinkSnapshot,
    wall_ns: u64,
    cpu_pct: u64,
    rss: u64,
) -> String {
    let sec = wall_ns as f64 / NANOS_PER_SEC_F;
    let d_msg = cur_src.messages.saturating_sub(prev_src.messages);
    let d_comp = cur_src
        .compressed_bytes
        .saturating_sub(prev_src.compressed_bytes);
    let d_decomp = cur_src
        .decompressed_bytes
        .saturating_sub(prev_src.decompressed_bytes);
    let response_wait_pct = pct(
        cur_src
            .response_wait_nanos
            .saturating_sub(prev_src.response_wait_nanos),
        wall_ns,
    );
    let decomp_pct = pct(
        cur_src
            .decomp_busy_nanos
            .saturating_sub(prev_src.decomp_busy_nanos),
        wall_ns,
    );
    let source_part = format!(
        "source: {} msg/s | comp {} | decomp {} | response-wait {}% | decomp {}% busy",
        ((d_msg as f64) / sec) as u64,
        fmt_bytes(d_comp as f64 / sec),
        fmt_bytes(d_decomp as f64 / sec),
        response_wait_pct,
        decomp_pct,
    );
    let source_message_rate =
        |messages: u64| format!("{} source-msg/s", ((messages as f64) / sec) as u64);
    let parse_part = if parses_rows {
        let d_rows = cur_parse.rows.saturating_sub(prev_parse.rows);
        let d_arrow = cur_parse.arrow_bytes.saturating_sub(prev_parse.arrow_bytes);
        let d_dlq = cur_parse.dlq_rows.saturating_sub(prev_parse.dlq_rows);
        let d_source_messages = cur_parse
            .source_messages
            .saturating_sub(prev_parse.source_messages);
        let parse_pct = pct(
            cur_parse
                .parse_busy_nanos
                .saturating_sub(prev_parse.parse_busy_nanos),
            wall_ns,
        );
        format!(
            "parse: {} rows/s | {} arrow | {} dlq/s | {} | {}% busy",
            ((d_rows as f64) / sec) as u64,
            fmt_bytes(d_arrow as f64 / sec),
            ((d_dlq as f64) / sec) as u64,
            source_message_rate(d_source_messages),
            parse_pct,
        )
    } else {
        "parse: benchmark-discard".to_string()
    };
    let d_sink_rows = cur_sink.rows.saturating_sub(prev_sink.rows);
    let d_sink_bytes = cur_sink.bytes.saturating_sub(prev_sink.bytes);
    let d_sink_flushes = cur_sink.flushes.saturating_sub(prev_sink.flushes);
    let d_sink_source_messages = cur_sink
        .source_messages
        .saturating_sub(prev_sink.source_messages);
    let sink_pct = pct(
        cur_sink.busy_nanos.saturating_sub(prev_sink.busy_nanos),
        wall_ns,
    );
    let backpressure_pct = pct(
        cur_sink
            .backpressure_nanos
            .saturating_sub(prev_sink.backpressure_nanos),
        wall_ns,
    );
    let retries = cur_sink.sink_retries.saturating_sub(prev_sink.sink_retries);
    let sink_part = format!(
        "sink: {} rows/s | {} | {} flushes/s | {} | {}% busy | {} retries | buffered {} | objects {}/{}/{} | {}% backpressure",
        ((d_sink_rows as f64) / sec) as u64,
        fmt_bytes(d_sink_bytes as f64 / sec),
        ((d_sink_flushes as f64) / sec) as u64,
        source_message_rate(d_sink_source_messages),
        sink_pct,
        retries,
        fmt_rss(cur_sink.buffered_bytes),
        cur_sink.open_objects,
        cur_sink.ready_objects,
        cur_sink.inflight_objects,
        backpressure_pct,
    );
    format!(
        "[stats p={pid}] {source_part} || {parse_part} || {sink_part} || guarantee: {} | cpu: {}% rss: {}",
        delivery_guarantee_name(delivery_guarantee),
        cpu_pct,
        fmt_rss(rss)
    )
}

fn format_line_avg(
    s: SourceSnapshot,
    p: ParseSnapshot,
    k: SinkSnapshot,
    any_row_parser: bool,
    delivery_guarantee: &str,
    wall_ns_sum: u64,
    cpu_pct: u64,
    rss: u64,
) -> String {
    let sec = wall_ns_sum as f64 / NANOS_PER_SEC_F;
    let response_wait_pct = pct(s.response_wait_nanos, wall_ns_sum);
    let decomp_pct = pct(s.decomp_busy_nanos, wall_ns_sum);
    let source_part = format!(
        "source: {} msg/s | comp {} | decomp {} | response-wait {}% | decomp {}% busy",
        ((s.messages as f64) / sec) as u64,
        fmt_bytes(s.compressed_bytes as f64 / sec),
        fmt_bytes(s.decompressed_bytes as f64 / sec),
        response_wait_pct,
        decomp_pct,
    );
    let source_message_rate =
        |messages: u64| format!("{} source-msg/s", ((messages as f64) / sec) as u64);
    let parse_part = if any_row_parser {
        let parse_pct = pct(p.parse_busy_nanos, wall_ns_sum);
        format!(
            "parse: {} rows/s | {} arrow | {} dlq/s | {} | {}% busy",
            ((p.rows as f64) / sec) as u64,
            fmt_bytes(p.arrow_bytes as f64 / sec),
            ((p.dlq_rows as f64) / sec) as u64,
            source_message_rate(p.source_messages),
            parse_pct,
        )
    } else {
        "parse: benchmark-discard".to_string()
    };
    let sink_pct = pct(k.busy_nanos, wall_ns_sum);
    let backpressure_pct = pct(k.backpressure_nanos, wall_ns_sum);
    let sink_part = format!(
        "sink: {} rows/s | {} | {} flushes/s | {} | {}% busy | {} retries | buffered {} | objects {}/{}/{} | {}% backpressure",
        ((k.rows as f64) / sec) as u64,
        fmt_bytes(k.bytes as f64 / sec),
        ((k.flushes as f64) / sec) as u64,
        source_message_rate(k.source_messages),
        sink_pct,
        k.sink_retries,
        fmt_rss(k.buffered_bytes),
        k.open_objects,
        k.ready_objects,
        k.inflight_objects,
        backpressure_pct,
    );
    format!(
        "[stats] {source_part} || {parse_part} || {sink_part} || guarantee: {delivery_guarantee} | cpu: {cpu_pct}% rss: {}",
        fmt_rss(rss)
    )
}

const fn delivery_guarantee_name(guarantee: Option<DeliveryGuarantee>) -> &'static str {
    match guarantee {
        Some(DeliveryGuarantee::ExactlyOnce) => "exactly-once",
        Some(DeliveryGuarantee::AtLeastOnce) => "at-least-once",
        Some(DeliveryGuarantee::NoDurability) => "no-durability",
        None => "unknown",
    }
}

#[inline]
fn pct(busy: u64, wall: u64) -> u64 {
    if wall == 0 {
        0
    } else {
        (busy as f64 * 100.0 / wall as f64) as u64
    }
}

/// Human-readable byte rate, IEC 1024-based.
#[must_use]
pub fn fmt_bytes(bps: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    if bps >= GIB {
        format!("{:.1} GiB/s", bps / GIB)
    } else if bps >= MIB {
        format!("{:.1} MiB/s", bps / MIB)
    } else if bps >= KIB {
        format!("{:.1} KiB/s", bps / KIB)
    } else {
        format!("{bps:.0} B/s")
    }
}

// ---------------------------------------------------------------------------
// Process resource usage (CPU %, RSS) — read from /proc/self on Linux.
// ---------------------------------------------------------------------------

/// Tracks process CPU utilisation and resident memory.
///
/// CPU snapshots come from `/proc/self/stat` on Linux and fall back to zero
/// when procfs is unavailable.
pub struct ProcessStats {
    prev_utime: u64,
    prev_stime: u64,
    prev_wall: Instant,
    clock_ticks_per_sec: u64,
}

impl ProcessStats {
    #[must_use]
    pub fn new() -> Self {
        let clock_ticks_per_sec = sysconf_clock_ticks();
        let (utime, stime) = read_proc_stat();
        Self {
            prev_utime: utime,
            prev_stime: stime,
            prev_wall: Instant::now(),
            clock_ticks_per_sec,
        }
    }

    /// Returns `(cpu_pct, rss_bytes)` since the last call.
    /// `cpu_pct` is percent of ONE core (100 = 1 fully loaded core).
    #[must_use]
    pub fn snapshot(&mut self) -> (u64, u64) {
        let now = Instant::now();
        let wall_ns = now.saturating_duration_since(self.prev_wall).as_nanos() as u64;
        let (utime, stime) = read_proc_stat();
        let cpu_delta_ticks = (utime.saturating_sub(self.prev_utime))
            .saturating_add(stime.saturating_sub(self.prev_stime));
        self.prev_utime = utime;
        self.prev_stime = stime;
        self.prev_wall = now;

        let cpu_pct = if wall_ns > 0 && self.clock_ticks_per_sec > 0 {
            // Fraction of one core used. Multiply by 100 for percent.
            let cpu_nanos =
                cpu_delta_ticks.saturating_mul(1_000_000_000) / self.clock_ticks_per_sec;
            cpu_nanos * 100 / wall_ns
        } else {
            0
        };

        let rss = read_proc_rss();
        (cpu_pct, rss)
    }
}

impl Default for ProcessStats {
    fn default() -> Self {
        Self::new()
    }
}

fn read_proc_stat() -> (u64, u64) {
    match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => {
            // Field 14 = utime, 15 = stime (1-indexed, space-separated).
            // The comm field (2) may contain spaces — skip past the closing ')'.
            let after_comm = match s.find(')') {
                Some(pos) => &s[pos + 2..],
                None => return (0, 0),
            };
            let fields: Vec<&str> = after_comm.split_whitespace().collect();
            // After skipping comm, field indices are offset by 2:
            // utime = fields[11], stime = fields[12]
            let utime = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
            let stime = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
            (utime, stime)
        }
        Err(_) => (0, 0),
    }
}

fn read_proc_rss() -> u64 {
    match std::fs::read_to_string("/proc/self/status") {
        Ok(s) => {
            for line in s.lines() {
                if line.starts_with("VmRSS:") {
                    // Format: "VmRSS:   123456 kB"
                    return line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0)
                        * 1024; // kB → bytes
                }
            }
            0
        }
        Err(_) => 0,
    }
}

const fn sysconf_clock_ticks() -> u64 {
    // sysconf(_SC_CLK_TCK) is typically 100 on Linux.
    // We use a simple heuristic: parse from /proc/self/stat or default to 100.
    // On non-Linux this is never called successfully.
    100
}

/// Human-readable RSS string.
fn fmt_rss(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    const KIB: u64 = 1024;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.0} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.0} KiB", bytes as f64 / KIB as f64)
    } else if bytes > 0 {
        format!("{bytes} B")
    } else {
        "N/A".to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
