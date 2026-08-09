//! Per-partition throughput + duty-cycle metrics, printed to the console by a
//! background stats reporter.
//!
//! Two counter sets, both per partition:
//! - [`SourceCounters`] — filled by the YDS/pqv1 source (messages, compressed
//!   and decompressed bytes, downloader and decompressor busy time).
//! - [`ParseCounters`] — filled by the parser thread (rows, Arrow bytes, DLQ
//!   rows, unique offsets, parser busy time).
//!
//! [`MetricsRegistry`] merges them by `partition_id` (source and parse counters
//! are registered independently — the source by the YDS provider inside
//! `build_source`, the parse counters by `main`). [`spawn_stats_reporter`]
//! snapshots the registry every `interval_ms` and prints a per-partition
//! (or aggregated) line via `tracing::info!`.

use std::collections::HashMap;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use core::time::Duration;
use std::time::Instant;

use alloc::sync::Arc;
use tokio::task::JoinHandle;

const RELAXED: Ordering = Ordering::Relaxed;
/// Nanoseconds per second (f64) — a typed const avoids the `*_literal_suffix`
/// contradiction (`separated` vs `unseparated` are both enabled here).
const NANOS_PER_SEC_F: f64 = 1_000_000_000.0;

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// Per-partition source counters (YDS/pqv1). Filled by the pqv1 bg task
/// (bytes + download/decompress duty) and `read_batch` (messages).
pub struct SourceCounters {
    messages: AtomicU64,
    compressed_bytes: AtomicU64,
    decompressed_bytes: AtomicU64,
    download_busy_nanos: AtomicU64,
    decomp_busy_nanos: AtomicU64,
}

impl SourceCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            messages: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            decompressed_bytes: AtomicU64::new(0),
            download_busy_nanos: AtomicU64::new(0),
            decomp_busy_nanos: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_messages(&self, n: u64) { self.messages.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_compressed_bytes(&self, n: u64) { self.compressed_bytes.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_decompressed_bytes(&self, n: u64) { self.decompressed_bytes.fetch_add(n, RELAXED); }
    /// Downloader busy = time a `Read` request is in-flight (`stream.message().await`).
    #[inline]
    pub fn add_download_busy(&self, d: Duration) {
        self.download_busy_nanos.fetch_add(d.as_nanos() as u64, RELAXED);
    }
    /// Decompressor busy = time inside `decompress()`.
    #[inline]
    pub fn add_decomp_busy(&self, d: Duration) {
        self.decomp_busy_nanos.fetch_add(d.as_nanos() as u64, RELAXED);
    }
}

impl Default for SourceCounters {
    fn default() -> Self { Self::new() }
}

/// Per-partition parser-output counters. Filled by the parser thread after
/// `parse_into` (rows/bytes/dlq/unique offsets) and around the parse work (busy
/// time).
pub struct ParseCounters {
    rows: AtomicU64,
    arrow_bytes: AtomicU64,
    dlq_rows: AtomicU64,
    unique_offsets: AtomicU64,
    parse_busy_nanos: AtomicU64,
}

impl ParseCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rows: AtomicU64::new(0),
            arrow_bytes: AtomicU64::new(0),
            dlq_rows: AtomicU64::new(0),
            unique_offsets: AtomicU64::new(0),
            parse_busy_nanos: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_rows(&self, n: u64) { self.rows.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_arrow_bytes(&self, n: u64) { self.arrow_bytes.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_dlq_rows(&self, n: u64) { self.dlq_rows.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_unique_offsets(&self, n: u64) { self.unique_offsets.fetch_add(n, RELAXED); }
    /// Parser busy = time in `parse_read_item` + `guard_middlewares`.
    #[inline]
    pub fn add_parse_busy(&self, d: Duration) {
        self.parse_busy_nanos.fetch_add(d.as_nanos() as u64, RELAXED);
    }
}

impl Default for ParseCounters {
    fn default() -> Self { Self::new() }
}

/// Per-partition sink-output counters. Filled by the writer task around each
/// flush (`sink.write` call): rows, Arrow bytes, number of flushes, unique
/// offsets (exactly-once dedup key), and busy time.
pub struct SinkCounters {
    rows: AtomicU64,
    bytes: AtomicU64,
    flushes: AtomicU64,
    unique_offsets: AtomicU64,
    busy_nanos: AtomicU64,
}

impl SinkCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            unique_offsets: AtomicU64::new(0),
            busy_nanos: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_rows(&self, n: u64) { self.rows.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_bytes(&self, n: u64) { self.bytes.fetch_add(n, RELAXED); }
    #[inline]
    pub fn add_flush(&self) { self.flushes.fetch_add(1, RELAXED); }
    #[inline]
    pub fn add_unique_offsets(&self, n: u64) { self.unique_offsets.fetch_add(n, RELAXED); }
    /// Sink busy = time inside `sink.write()`.
    #[inline]
    pub fn add_busy(&self, d: Duration) {
        self.busy_nanos.fetch_add(d.as_nanos() as u64, RELAXED);
    }
}

impl Default for SinkCounters {
    fn default() -> Self { Self::new() }
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct SourceSnapshot {
    messages: u64,
    compressed_bytes: u64,
    decompressed_bytes: u64,
    download_busy_nanos: u64,
    decomp_busy_nanos: u64,
}

#[derive(Clone, Copy, Default)]
struct ParseSnapshot {
    rows: u64,
    arrow_bytes: u64,
    dlq_rows: u64,
    unique_offsets: u64,
    parse_busy_nanos: u64,
}

#[derive(Clone, Copy, Default)]
struct SinkSnapshot {
    rows: u64,
    bytes: u64,
    flushes: u64,
    unique_offsets: u64,
    busy_nanos: u64,
}

fn src_snap(c: Option<&Arc<SourceCounters>>) -> SourceSnapshot {
    c.map_or_else(SourceSnapshot::default, |s| SourceSnapshot {
        messages: s.messages.load(RELAXED),
        compressed_bytes: s.compressed_bytes.load(RELAXED),
        decompressed_bytes: s.decompressed_bytes.load(RELAXED),
        download_busy_nanos: s.download_busy_nanos.load(RELAXED),
        decomp_busy_nanos: s.decomp_busy_nanos.load(RELAXED),
    })
}

fn parse_snap(c: Option<&Arc<ParseCounters>>) -> ParseSnapshot {
    c.map_or_else(ParseSnapshot::default, |p| ParseSnapshot {
        rows: p.rows.load(RELAXED),
        arrow_bytes: p.arrow_bytes.load(RELAXED),
        dlq_rows: p.dlq_rows.load(RELAXED),
        unique_offsets: p.unique_offsets.load(RELAXED),
        parse_busy_nanos: p.parse_busy_nanos.load(RELAXED),
    })
}

fn sink_snap(c: Option<&Arc<SinkCounters>>) -> SinkSnapshot {
    c.map_or_else(SinkSnapshot::default, |s| SinkSnapshot {
        rows: s.rows.load(RELAXED),
        bytes: s.bytes.load(RELAXED),
        flushes: s.flushes.load(RELAXED),
        unique_offsets: s.unique_offsets.load(RELAXED),
        busy_nanos: s.busy_nanos.load(RELAXED),
    })
}

// ---------------------------------------------------------------------------
// Registry (merges source + parse + sink counters by partition_id)
// ---------------------------------------------------------------------------

struct PartitionMetrics {
    has_parser: bool,
    has_eo_key: bool,
    source: Option<Arc<SourceCounters>>,
    parse: Option<Arc<ParseCounters>>,
    sink: Option<Arc<SinkCounters>>,
}

/// Read-only snapshot of one partition's counters (cloned Arcs), produced by
/// [`MetricsRegistry::snapshot`] for the reporter to iterate without holding
/// the registry lock.
struct PartitionSnapshot {
    pid: i64,
    has_parser: bool,
    has_eo_key: bool,
    source: Option<Arc<SourceCounters>>,
    parse: Option<Arc<ParseCounters>>,
    sink: Option<Arc<SinkCounters>>,
}

pub struct MetricsRegistry {
    inner: Mutex<HashMap<i64, PartitionMetrics>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self { Self::new() }
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }

    #[expect(clippy::significant_drop_tightening, reason = "the MutexGuard must outlive the entry borrow it hands out")]
    pub fn register_source(&self, partition_id: i64, c: Arc<SourceCounters>) {
        let mut m = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_insert_with(|| PartitionMetrics {
            has_parser: false, has_eo_key: false, source: None, parse: None, sink: None,
        });
        entry.source = Some(c);
    }

    #[expect(clippy::significant_drop_tightening, reason = "the MutexGuard must outlive the entry borrow it hands out")]
    pub fn register_parse(&self, partition_id: i64, has_parser: bool, c: Arc<ParseCounters>) {
        let mut m = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_insert_with(|| PartitionMetrics {
            has_parser: false, has_eo_key: false, source: None, parse: None, sink: None,
        });
        entry.has_parser = has_parser;
        entry.parse = Some(c);
    }

    #[expect(clippy::significant_drop_tightening, reason = "the MutexGuard must outlive the entry borrow it hands out")]
    pub fn register_sink(&self, partition_id: i64, c: Arc<SinkCounters>) {
        let mut m = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_insert_with(|| PartitionMetrics {
            has_parser: false, has_eo_key: false, source: None, parse: None, sink: None,
        });
        entry.sink = Some(c);
    }

    /// Mark whether exactly-once keys are active for this partition. When false,
    /// the stats line shows `uniq off/s: unknown (absent exactly_once_keys)`.
    #[expect(clippy::significant_drop_tightening, reason = "the MutexGuard must outlive the entry borrow it hands out")]
    pub fn set_eo_key(&self, partition_id: i64, active: bool) {
        let mut m = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = m.entry(partition_id).or_insert_with(|| PartitionMetrics {
            has_parser: false, has_eo_key: false, source: None, parse: None, sink: None,
        });
        entry.has_eo_key = active;
    }

    fn snapshot(&self) -> Vec<PartitionSnapshot> {
        let m = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        m.iter()
            .map(|(pid, pm)| PartitionSnapshot {
                pid: *pid,
                has_parser: pm.has_parser,
                has_eo_key: pm.has_eo_key,
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
        let mut last: HashMap<i64, (SourceSnapshot, ParseSnapshot, SinkSnapshot, Instant)> = HashMap::new();
        let mut primed = false;
        loop {
            tokio::time::sleep(interval).await;
            let now = Instant::now();
            let parts = registry.snapshot();

            // First tick: prime snapshots so the next tick has a real delta.
            if !primed {
                for pm in &parts {
                    last.insert(pm.pid, (src_snap(pm.source.as_ref()), parse_snap(pm.parse.as_ref()), sink_snap(pm.sink.as_ref()), now));
                }
                primed = true;
                continue;
            }

            if per_partition {
                for pm in &parts {
                    let cur_src = src_snap(pm.source.as_ref());
                    let cur_parse = parse_snap(pm.parse.as_ref());
                    let cur_sink = sink_snap(pm.sink.as_ref());
                    let (psrc, pparse, psink, ptime) =
                        last.get(&pm.pid).copied().unwrap_or((cur_src, cur_parse, cur_sink, now));
                    let wall_ns = now.saturating_duration_since(ptime).as_nanos() as u64;
                    if wall_ns > 0 {
                        tracing::info!(
                            "{}",
                            format_line(pm.pid, pm.has_parser, pm.has_eo_key, cur_src, psrc, cur_parse, pparse, cur_sink, psink, wall_ns)
                        );
                    }
                    last.insert(pm.pid, (cur_src, cur_parse, cur_sink, now));
                }
            } else {
                let line = aggregate_line(&parts, &mut last, now);
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
) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let mut s = SourceSnapshot::default();
    let mut p = ParseSnapshot::default();
    let mut k = SinkSnapshot::default();
    let mut wall_ns_sum: u64 = 0;
    let mut any_parser = false;
    let mut any_eo_key = false;
    for pm in parts {
        let cur_src = src_snap(pm.source.as_ref());
        let cur_parse = parse_snap(pm.parse.as_ref());
        let cur_sink = sink_snap(pm.sink.as_ref());
        let (psrc, pparse, psink, ptime) = last.get(&pm.pid).copied().unwrap_or((cur_src, cur_parse, cur_sink, now));
        let wall = now.saturating_duration_since(ptime).as_nanos() as u64;
        if wall > 0 {
            s.messages += cur_src.messages - psrc.messages;
            s.compressed_bytes += cur_src.compressed_bytes - psrc.compressed_bytes;
            s.decompressed_bytes += cur_src.decompressed_bytes - psrc.decompressed_bytes;
            s.download_busy_nanos += cur_src.download_busy_nanos - psrc.download_busy_nanos;
            s.decomp_busy_nanos += cur_src.decomp_busy_nanos - psrc.decomp_busy_nanos;
            p.rows += cur_parse.rows - pparse.rows;
            p.arrow_bytes += cur_parse.arrow_bytes - pparse.arrow_bytes;
            p.dlq_rows += cur_parse.dlq_rows - pparse.dlq_rows;
            p.unique_offsets += cur_parse.unique_offsets - pparse.unique_offsets;
            p.parse_busy_nanos += cur_parse.parse_busy_nanos - pparse.parse_busy_nanos;
            k.rows += cur_sink.rows - psink.rows;
            k.bytes += cur_sink.bytes - psink.bytes;
            k.flushes += cur_sink.flushes - psink.flushes;
            k.unique_offsets += cur_sink.unique_offsets - psink.unique_offsets;
            k.busy_nanos += cur_sink.busy_nanos - psink.busy_nanos;
            wall_ns_sum += wall;
            any_parser |= pm.has_parser;
            any_eo_key |= pm.has_eo_key;
        }
        last.insert(pm.pid, (cur_src, cur_parse, cur_sink, now));
    }
    if wall_ns_sum == 0 {
        return None;
    }
    // Average busy% across partitions: total busy / sum of per-partition walls.
    Some(format_line_avg(s, p, k, any_parser, any_eo_key, wall_ns_sum))
}

fn format_line(
    pid: i64,
    has_parser: bool,
    has_eo_key: bool,
    cur_src: SourceSnapshot,
    prev_src: SourceSnapshot,
    cur_parse: ParseSnapshot,
    prev_parse: ParseSnapshot,
    cur_sink: SinkSnapshot,
    prev_sink: SinkSnapshot,
    wall_ns: u64,
) -> String {
    let sec = wall_ns as f64 / NANOS_PER_SEC_F;
    let d_msg = cur_src.messages - prev_src.messages;
    let d_comp = cur_src.compressed_bytes - prev_src.compressed_bytes;
    let d_decomp = cur_src.decompressed_bytes - prev_src.decompressed_bytes;
    let dl_pct = pct(cur_src.download_busy_nanos - prev_src.download_busy_nanos, wall_ns);
    let decomp_pct = pct(cur_src.decomp_busy_nanos - prev_src.decomp_busy_nanos, wall_ns);
    let source_part = format!(
        "yds: {} msg/s | comp {} | decomp {} | dl {}% busy | decomp {}% busy",
        ((d_msg as f64) / sec) as u64,
        fmt_bytes(d_comp as f64 / sec),
        fmt_bytes(d_decomp as f64 / sec),
        dl_pct,
        decomp_pct,
    );
    let uniq_off_fmt = |d_uniq: u64| -> String {
        if has_eo_key {
            format!("{} uniq off/s", ((d_uniq as f64) / sec) as u64)
        } else {
            "uniq off/s: unknown (absent exactly_once_keys)".to_string()
        }
    };
    let parse_part = if has_parser {
        let d_rows = cur_parse.rows - prev_parse.rows;
        let d_arrow = cur_parse.arrow_bytes - prev_parse.arrow_bytes;
        let d_dlq = cur_parse.dlq_rows - prev_parse.dlq_rows;
        let d_uniq = cur_parse.unique_offsets - prev_parse.unique_offsets;
        let parse_pct = pct(cur_parse.parse_busy_nanos - prev_parse.parse_busy_nanos, wall_ns);
        format!(
            "parse: {} rows/s | {} arrow | {} dlq/s | {} | {}% busy",
            ((d_rows as f64) / sec) as u64,
            fmt_bytes(d_arrow as f64 / sec),
            ((d_dlq as f64) / sec) as u64,
            uniq_off_fmt(d_uniq),
            parse_pct,
        )
    } else {
        "parse: (no parser)".to_string()
    };
    let d_sink_rows = cur_sink.rows - prev_sink.rows;
    let d_sink_bytes = cur_sink.bytes - prev_sink.bytes;
    let d_sink_flushes = cur_sink.flushes - prev_sink.flushes;
    let d_sink_uniq = cur_sink.unique_offsets - prev_sink.unique_offsets;
    let sink_pct = pct(cur_sink.busy_nanos - prev_sink.busy_nanos, wall_ns);
    let sink_part = format!(
        "sink: {} rows/s | {} arrow | {} flushes/s | {} | {}% busy",
        ((d_sink_rows as f64) / sec) as u64,
        fmt_bytes(d_sink_bytes as f64 / sec),
        ((d_sink_flushes as f64) / sec) as u64,
        uniq_off_fmt(d_sink_uniq),
        sink_pct,
    );
    format!("[stats p={pid}] {source_part} || {parse_part} || {sink_part}")
}

fn format_line_avg(
    s: SourceSnapshot,
    p: ParseSnapshot,
    k: SinkSnapshot,
    any_parser: bool,
    any_eo_key: bool,
    wall_ns_sum: u64,
) -> String {
    let sec = wall_ns_sum as f64 / NANOS_PER_SEC_F;
    let dl_pct = pct(s.download_busy_nanos, wall_ns_sum);
    let decomp_pct = pct(s.decomp_busy_nanos, wall_ns_sum);
    let source_part = format!(
        "yds: {} msg/s | comp {} | decomp {} | dl {}% busy | decomp {}% busy",
        ((s.messages as f64) / sec) as u64,
        fmt_bytes(s.compressed_bytes as f64 / sec),
        fmt_bytes(s.decompressed_bytes as f64 / sec),
        dl_pct,
        decomp_pct,
    );
    let uniq_off_str = |uniq: u64| -> String {
        if any_eo_key {
            format!("{} uniq off/s", ((uniq as f64) / sec) as u64)
        } else {
            "uniq off/s: unknown (absent exactly_once_keys)".to_string()
        }
    };
    let parse_part = if any_parser {
        let parse_pct = pct(p.parse_busy_nanos, wall_ns_sum);
        format!(
            "parse: {} rows/s | {} arrow | {} dlq/s | {} | {}% busy",
            ((p.rows as f64) / sec) as u64,
            fmt_bytes(p.arrow_bytes as f64 / sec),
            ((p.dlq_rows as f64) / sec) as u64,
            uniq_off_str(p.unique_offsets),
            parse_pct,
        )
    } else {
        "parse: (no parser)".to_string()
    };
    let sink_pct = pct(k.busy_nanos, wall_ns_sum);
    let sink_part = format!(
        "sink: {} rows/s | {} arrow | {} flushes/s | {} | {}% busy",
        ((k.rows as f64) / sec) as u64,
        fmt_bytes(k.bytes as f64 / sec),
        ((k.flushes as f64) / sec) as u64,
        uniq_off_str(k.unique_offsets),
        sink_pct,
    );
    format!("[stats] {source_part} || {parse_part} || {sink_part}")
}

#[inline]
fn pct(busy: u64, wall: u64) -> u64 {
    if wall == 0 { 0 } else { (busy as f64 * 100.0 / wall as f64) as u64 }
}

/// Human-readable byte rate, IEC 1024-based.
#[must_use]
pub fn fmt_bytes(bps: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    if bps >= GIB { format!("{:.1} GiB/s", bps / GIB) }
    else if bps >= MIB { format!("{:.1} MiB/s", bps / MIB) }
    else if bps >= KIB { format!("{:.1} KiB/s", bps / KIB) }
    else { format!("{bps:.0} B/s") }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_bytes_iec() {
        assert_eq!(fmt_bytes(0.0), "0 B/s");
        assert_eq!(fmt_bytes(512.0), "512 B/s");
        assert_eq!(fmt_bytes(1234.0), "1.2 KiB/s");
        assert_eq!(fmt_bytes(5_242_880.0), "5.0 MiB/s");
        assert_eq!(fmt_bytes(1.5 * 1024.0 * 1024.0 * 1024.0), "1.5 GiB/s");
    }

    #[test]
    fn pct_clamps() {
        assert_eq!(pct(0, 1_000_000_000), 0);
        assert_eq!(pct(500_000_000, 1_000_000_000), 50);
        assert_eq!(pct(1_000_000_000, 1_000_000_000), 100);
        assert_eq!(pct(1_200_000_000, 1_000_000_000), 120); // can exceed 100 (multi-partition)
        assert_eq!(pct(1, 0), 0); // divide-by-zero guard
    }

    #[test]
    fn registry_merges_source_and_parse() {
        let reg = MetricsRegistry::new();
        let pid = 7;
        reg.register_source(pid, Arc::new(SourceCounters::new()));
        reg.register_parse(pid, true, Arc::new(ParseCounters::new()));
        let snap = reg.snapshot();
        assert_eq!(snap.len(), 1, "merged into one entry");
        let pm = &snap[0];
        assert!(pm.has_parser);
        assert!(pm.source.is_some());
        assert!(pm.parse.is_some());
    }

    #[test]
    fn counters_accumulate() {
        let s = Arc::new(SourceCounters::new());
        s.add_messages(10);
        s.add_compressed_bytes(100);
        s.add_decompressed_bytes(200);
        s.add_download_busy(Duration::from_nanos(42));
        s.add_decomp_busy(Duration::from_nanos(7));
        let snap = src_snap(Some(&s));
        assert_eq!(snap.messages, 10);
        assert_eq!(snap.compressed_bytes, 100);
        assert_eq!(snap.decompressed_bytes, 200);
        assert_eq!(snap.download_busy_nanos, 42);
        assert_eq!(snap.decomp_busy_nanos, 7);
    }
}
