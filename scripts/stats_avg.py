#!/usr/bin/env python3
"""Parse transferia [stats p=X] log lines, print averages and diagnostics.

Usage:
  python3 stats_avg.py transferia.log
  grep '\\[stats' transferia.log | python3 stats_avg.py
"""

import re
import sys
from collections import defaultdict
from math import sqrt

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

RE_NO_PARSER = re.compile(r"parse:\s+\(no\s+parser\)")
RE_NUM = re.compile(r"[\d]+\.?[\d]*")


def parse_line(line: str) -> dict | None:
    idx = line.find("[stats")
    if idx < 0:
        return None
    line = line[idx:]

    has_parser = not RE_NO_PARSER.search(line)
    nums = [float(m.group(0)) for m in RE_NUM.finditer(line)]
    expected = 16 if has_parser else 11
    if len(nums) < expected:
        return None

    pid = int(nums[0])
    i = 1
    src = {
        "msg": nums[i], "comp": nums[i + 1], "decomp": nums[i + 2],
        "dl": nums[i + 3], "decomp_b": nums[i + 4],
    }
    i += 5
    if has_parser:
        parse = {
            "rows": nums[i], "arrow": nums[i + 1], "dlq": nums[i + 2],
            "uniq_p": nums[i + 3], "parse_b": nums[i + 4],
        }
        i += 5
    else:
        parse = {"rows": 0, "arrow": 0, "dlq": 0, "uniq_p": 0, "parse_b": 0}
    sink = {
        "sink_rows": nums[i], "sink_arrow": nums[i + 1], "sink_flushes": nums[i + 2],
        "uniq_s": nums[i + 3], "sink_b": nums[i + 4],
    }
    i += 5
    # Trailing process-level fields: cpu: N% rss: X GiB/MiB
    cpu_pct = nums[i] if i < len(nums) else 0.0
    rss_val = nums[i + 1] if i + 1 < len(nums) else 0.0
    rss_bytes = 0.0
    rss_pos = line.find("rss: ")
    if rss_pos >= 0:
        rss_tail = line[rss_pos + 5:]
        if "GiB" in rss_tail:      rss_bytes = rss_val * 1024**3
        elif "MiB" in rss_tail:    rss_bytes = rss_val * 1024**2
        elif "KiB" in rss_tail:    rss_bytes = rss_val * 1024
        elif "N/A" not in rss_tail: rss_bytes = rss_val

    return {"pid": pid, **src, **parse, **sink, "cpu_pct": cpu_pct, "rss_bytes": rss_bytes}


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

FIELDS_SOURCE = ["msg", "comp", "decomp", "dl", "decomp_b"]
FIELDS_PARSE  = ["rows", "arrow", "dlq", "uniq_p", "parse_b"]
FIELDS_SINK   = ["sink_rows", "sink_arrow", "sink_flushes", "uniq_s", "sink_b"]
FIELDS_PROC   = ["cpu_pct", "rss_bytes"]


def pct_ticks(values: list[float], threshold: float, above: bool = True) -> float:
    """Fraction of ticks (0-100) where value is above/below the threshold."""
    if not values:
        return 0.0
    if above:
        return sum(1 for v in values if v >= threshold) / len(values) * 100
    else:
        return sum(1 for v in values if v <= threshold) / len(values) * 100


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = avg(values)
    return sqrt(sum((v - m) ** 2 for v in values) / (len(values) - 1))


def cv(values: list[float]) -> float:
    """Coefficient of variation (0-1)."""
    m = avg(values)
    if m == 0:
        return 0.0
    return std(values) / m


def field_values(entries: list[dict], key: str) -> list[float]:
    return [e[key] for e in entries]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    entries = [e for line in lines if (e := parse_line(line)) is not None]
    if not entries:
        print("No stats lines found.", file=sys.stderr)
        sys.exit(1)

    n = len(entries)
    pids = sorted(set(e["pid"] for e in entries))
    has_parser = sum(e["rows"] for e in entries) > 0

    # --- averages & per-field stats ---
    av = {k: avg(field_values(entries, k))
          for k in FIELDS_SOURCE + FIELDS_PARSE + FIELDS_SINK + FIELDS_PROC}
    cv_map = {k: cv(field_values(entries, k))
              for k in FIELDS_SOURCE + FIELDS_PARSE + FIELDS_SINK}

    # --- print averages table ---
    comp_ratio = av["decomp"] / av["comp"] if av["comp"] > 0 else 0.0
    dedup_pct = (av["rows"] - av["sink_rows"]) / av["rows"] * 100 if av["rows"] > 0 else 0.0
    rows_per_msg = av["rows"] / av["msg"] if av["msg"] > 0 else 0.0

    print(f"=== Averages over {n} stats ticks, partitions {pids} ===\n")

    print(f"{'YDS Source':>15}")
    print(f"  msg/s:       {av['msg']:>10.0f}")
    print(f"  comp:        {av['comp']:>10.1f} MiB/s")
    print(f"  decomp:      {av['decomp']:>10.1f} MiB/s")
    print(f"  ratio:       {comp_ratio:>10.1f}x")
    print(f"  dl busy:     {av['dl']:>10.0f}%")
    print(f"  decomp busy: {av['decomp_b']:>10.0f}%")
    print()

    print(f"{'Parser':>15}")
    if has_parser:
        print(f"  rows/s:      {av['rows']:>10.0f}")
        print(f"  Arrow:       {av['arrow']:>10.1f} MiB/s")
        print(f"  dlq/s:       {av['dlq']:>10.0f}")
        print(f"  msg/s:       {av['uniq_p']:>10.0f}")
        print(f"  busy:        {av['parse_b']:>10.0f}%")
        print(f"  rows/msg:    {rows_per_msg:>10.1f}x")
    else:
        print("  (no parser)")
    print()

    print(f"{'Sink':>15}")
    print(f"  rows/s:      {av['sink_rows']:>10.0f}")
    print(f"  Arrow:       {av['sink_arrow']:>10.1f} MiB/s")
    print(f"  flushes/s:   {av['sink_flushes']:>10.1f}")
    print(f"  msg/s:       {av['uniq_s']:>10.0f}")
    print(f"  busy:        {av['sink_b']:>10.0f}%")
    print(f"  dedup:       {dedup_pct:>10.2f}%")

    # ===================================================================
    # Diagnostics
    # ===================================================================

    diag: list[str] = []
    dl_vals   = field_values(entries, "dl")
    decomp_vals = field_values(entries, "decomp_b")
    parse_vals  = field_values(entries, "parse_b")
    sink_vals   = field_values(entries, "sink_b")
    msg_vals    = field_values(entries, "msg")
    flush_vals  = field_values(entries, "sink_flushes")
    dlq_vals    = field_values(entries, "dlq")
    uniq_p_vals = field_values(entries, "uniq_p")
    uniq_s_vals = field_values(entries, "uniq_s")
    rows_vals   = field_values(entries, "rows")
    srows_vals  = field_values(entries, "sink_rows")

    # 0. GOOD NEWS — input throughput ≈ output throughput
    if has_parser and av["msg"] > 0 and av["uniq_s"] > 0:
        in_out_ratio = av["msg"] / av["uniq_s"] if av["uniq_s"] > 0 else 0
        in_out_diff_pct = abs(av["msg"] - av["uniq_s"]) / av["msg"] * 100 if av["msg"] > 0 else 0
        if in_out_diff_pct <= 5:
            diag.append(f"GOOD NEWS: input msg/s ({av['msg']:.0f}) ≈ "
                        f"output msg/s ({av['uniq_s']:.0f}) — "
                        f"difference {in_out_diff_pct:.1f}% ≤ 5%. "
                        f"No throughput loss in source-processing, parsing, "
                        f"middlewares, or sink insertion — the pipeline is "
                        f"end-to-end transparent. "
                        f"The only way to go faster is to make the source produce "
                        f"data faster, or — if possible — parallelize source reads "
                        f"(more partitions / more workers).")

    # 1. YDS bottleneck
    dl_overload   = pct_ticks(dl_vals, 95.0)
    decomp_chill  = pct_ticks(decomp_vals, 95.0)
    parse_chill   = pct_ticks(parse_vals, 95.0) if has_parser else 0.0
    sink_chill    = pct_ticks(sink_vals, 95.0)

    if dl_overload > 50 and decomp_chill < 30 and parse_chill < 30 and sink_chill < 30:
        diag.append(f"BOTTLENECK: YDS downloader — dl ≥95% busy in {dl_overload:.0f}% of ticks, "
                    f"parser ({parse_chill:.0f}%) and sink ({sink_chill:.0f}%) underloaded. "
                    f"Limited by network or broker throughput. "
                    f"NOTE: dl≈100% does NOT mean the source is maxed out — "
                    f"it only means we spend ~100% of wall time waiting on download. "
                    f"If the broker can serve data faster, dl will STILL be ≈100%, "
                    f"but msg/s and overall throughput will increase.")

    # 2. Decompressor bottleneck
    if decomp_chill > 50 and av["decomp_b"] > av["dl"]:
        diag.append(f"BOTTLENECK: decompressor — ≥95% busy in {decomp_chill:.0f}% of ticks. "
                    f"lz4/zstd can't keep up with incoming compressed data. "
                    f"Consider drop_before_decompress (if parsing is not needed).")

    # 3. Parser bottleneck
    if parse_chill > 50:
        diag.append(f"BOTTLENECK: JSON parser — ≥95% busy in {parse_chill:.0f}% of ticks. "
                    f"Rows/s ({av['rows']:.0f}) is at its ceiling. "
                    f"Consider more workers or a faster parser.")

    # 4. Sink bottleneck
    if sink_chill > 50:
        diag.append(f"BOTTLENECK: ClickHouse sink — ≥95% busy in {sink_chill:.0f}% of ticks. "
                    f"Flushes/s: {av['sink_flushes']:.1f}. "
                    f"Check CH disk I/O, network to CH, or MergeTree mutations.")

    # 5. Waterline dedup active
    if has_parser and av["uniq_p"] > av["uniq_s"] * 1.02:  # 2% tolerance
        diag.append(f"WATERLINE DEDUP: parse msg/s ({av['uniq_p']:.0f}) > sink msg/s ({av['uniq_s']:.0f}) — "
                    f"waterline is filtering {av['uniq_p'] - av['uniq_s']:.0f} already-written messages/s. "
                    f"Normal after restart; otherwise check if YDS is replaying old data.")

    # 6. DLQ active
    if has_parser and av["dlq"] > 0:
        dlq_rate = av["dlq"] / av["rows"] * 100 if av["rows"] > 0 else 0
        diag.append(f"DLQ: {av['dlq']:.0f} rows/s to _dlq table ({dlq_rate:.2f}% of total). "
                    f"{'High — check input data format' if dlq_rate > 1 else 'Normal noise.' if dlq_rate < 0.1 else 'Moderate — monitor.'}")

    # 7. Burst traffic
    msg_cv = cv(msg_vals)
    if msg_cv > 0.5:
        diag.append(f"BURST TRAFFIC: msg/s coefficient of variation = {msg_cv:.2f} (>0.5). "
                    f"Topic has uneven load. Channel buffers absorb peaks; no action needed "
                    f"unless a component hits 100% busy during bursts.")

    # 8. Many small flushes
    if has_parser and av["sink_flushes"] > 0:
        rows_per_flush = av["sink_rows"] / av["sink_flushes"]
        if rows_per_flush < 20000:
            diag.append(f"SMALL FLUSHES: {av['sink_flushes']:.1f} flushes/s, "
                        f"avg {rows_per_flush:.0f} rows/flush. "
                        f"Many small INSERTs → overhead on CH round-trips. "
                        f"Consider raising sink_batch_size or max_linger_ms.")
    elif not has_parser and av["sink_flushes"] > 0 and av["sink_rows"] > 0:
        rows_per_flush = av["sink_rows"] / av["sink_flushes"]
        if rows_per_flush < 20000:
            diag.append(f"SMALL FLUSHES: {av['sink_flushes']:.1f} flushes/s, "
                        f"avg {rows_per_flush:.0f} rows/flush. "
                        f"Consider raising sink_batch_size or max_linger_ms.")

    # 9. Compression anomaly
    if comp_ratio < 1.3 and av["comp"] > 1:
        diag.append(f"COMPRESSION: ratio {comp_ratio:.1f}x — data already compressed or binary. "
                    f"decomp CPU is wasted. If parsing is not needed, consider drop_before_decompress.")
    elif comp_ratio > 5.0:
        diag.append(f"COMPRESSION: ratio {comp_ratio:.1f}x — highly compressible (repetitive JSON?). "
                    f"Normal for some log formats; no action needed.")

    # 10. Rows-per-msg change (informational — no baseline to compare)
    if has_parser and rows_per_msg > 0:
        if rows_per_msg < 1.0:
            diag.append(f"ROWS/MSG: {rows_per_msg:.1f} — less than 1 row per YDS message. "
                        f"Possible large JSON objects (not newline-split). "
                        f"In exactly-once mode, check batch_size vs message size.")
        elif rows_per_msg > 20:
            diag.append(f"ROWS/MSG: {rows_per_msg:.1f} — high fan-out. Each YDS message "
                        f"contains many JSON lines (newline-split). "
                        f"In exactly-once mode, ensure batch_size > max rows per message.")

    # 11. Pipeline stalled
    stalled = av["msg"] < 1 and av["dl"] < 1
    if stalled:
        if av["sink_b"] > 5:
            diag.append(f"STALLED: pipeline idle but sink busy at {av['sink_b']:.0f}%. "
                        f"Sink appears blocked — check ClickHouse availability.")
        elif has_parser and av["parse_b"] > 5:
            diag.append(f"STALLED: pipeline idle but parser busy at {av['parse_b']:.0f}%. "
                        f"Parser is stuck processing a large message.")
        else:
            diag.append(f"STALLED: all metrics near zero. Topic may be empty or reader disconnected.")

    # Print diagnosis
    if diag:
        print(f"\n{'='*60}")
        print(" DIAGNOSIS")
        print(f"{'='*60}")
        for d in diag:
            print(f"  • {d}")
    else:
        print(f"\n  ✓ No issues detected — pipeline is balanced.")

    # Print summary line
    if not stalled:
        components = []
        if dl_overload > 50:
            components.append(f"YDS ({dl_overload:.0f}% near-limit)")
        if decomp_chill > 50:
            components.append(f"decomp ({decomp_chill:.0f}% near-limit)")
        if parse_chill > 50:
            components.append(f"parser ({parse_chill:.0f}% near-limit)")
        if sink_chill > 50:
            components.append(f"sink ({sink_chill:.0f}% near-limit)")
        if not components:
            print(f"\n  Headroom: all components below 95% busy in >50% of ticks.")
        else:
            print(f"\n  Limiting: {', '.join(components)}")

    # Efficiency table: throughput per fully-loaded core.
    # CPU% is measured as percent of ONE core (100 = 1 fully-loaded core).
    cpu_avg = av["cpu_pct"]
    if cpu_avg > 1:
        cores_used = cpu_avg / 100.0
        rss_gib = av["rss_bytes"] / (1024**3) if av["rss_bytes"] > 0 else 0.0

        print(f"\n{'='*60}")
        print(" EFFICIENCY (per fully-loaded core, cpu avg: {:.0f}% = {:.2f} cores)".format(cpu_avg, cores_used))
        print(f"{'='*60}")
        print(f"  Process RSS:          {rss_gib:>10.2f} GiB")
        print(f"  msg/s per core:       {av['msg']/cores_used:>10.0f}")
        print(f"  decomp MiB/s per core:{av['decomp']/cores_used:>10.1f}")
        if has_parser:
            print(f"  rows/s per core:      {av['rows']/cores_used:>10.0f}")
            print(f"  arrow MiB/s per core: {av['arrow']/cores_used:>10.1f}")
    else:
        print(f"\n  Efficiency: cpu data unavailable (cpu avg: {cpu_avg:.0f}% — running on macOS?)")


if __name__ == "__main__":
    main()
