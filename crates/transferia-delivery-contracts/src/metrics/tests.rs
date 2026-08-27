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
    assert!(pm.parses_rows);
    assert!(pm.source.is_some());
    assert!(pm.parse.is_some());
}

#[test]
fn counters_accumulate() {
    let s = Arc::new(SourceCounters::new());
    s.add_records(10);
    s.add_network_raw_bytes(100);
    s.add_network_decoded_bytes(200);
    s.add_response_wait(Duration::from_nanos(42));
    s.add_network_decode_busy(Duration::from_nanos(7));
    let snap = src_snap(Some(&s));
    assert_eq!(snap.records, 10);
    assert_eq!(snap.network_raw_bytes, 100);
    assert_eq!(snap.network_decoded_bytes, 200);
    assert_eq!(snap.response_wait_nanos, 42);
    assert_eq!(snap.network_decode_busy_nanos, 7);
}

#[test]
fn reporter_tolerates_counter_generation_reset() {
    let current_source = SourceSnapshot::default();
    let previous_source = SourceSnapshot {
        records: 100,
        network_raw_bytes: 100,
        network_decoded_bytes: 100,
        response_wait_nanos: 100,
        network_decode_busy_nanos: 100,
    };
    let line = format_line(
        1,
        true,
        Some(DeliveryGuarantee::AtLeastOnce),
        current_source,
        previous_source,
        ParseSnapshot::default(),
        ParseSnapshot {
            rows: 100,
            arrow_bytes: 100,
            dlq_rows: 100,
            source_messages: 100,
            parse_busy_nanos: 100,
        },
        SinkSnapshot::default(),
        SinkSnapshot {
            rows: 100,
            bytes: 100,
            flushes: 100,
            source_messages: 100,
            busy_nanos: 100,
            sink_retries: 100,
            buffered_bytes: 0,
            open_objects: 0,
            ready_objects: 0,
            inflight_objects: 0,
            backpressure_nanos: 100,
        },
        1_000_000_000,
        0,
        0,
    );
    assert!(line.contains("source: 0 records/s"));
    assert!(line.contains("guarantee: at-least-once"));
}
