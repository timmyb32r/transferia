#!/usr/bin/env python3
"""Aggregate transferia ``[stats p=N]`` log lines from a file or stdin."""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
from typing import Any, Iterable

from run_single_partition_benchmark import NUMERIC_SAMPLE_KEYS, parse_stats_line


def read_samples(lines: Iterable[str]) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, 1):
        if "[stats" not in line:
            continue
        try:
            sample = parse_stats_line(line)
        except ValueError as error:
            raise ValueError(f"invalid stats line {line_number}: {error}") from error
        if sample is not None:
            samples.append(sample)
    if not samples:
        raise ValueError("no per-partition [stats p=N] lines found")
    return samples


def average_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    averages: dict[str, Any] = {
        "sample_count": len(samples),
        "partition_ids": sorted({int(sample["partition_id"]) for sample in samples}),
        "delivery_guarantees": sorted(
            {str(sample["delivery_guarantee"]) for sample in samples}
        ),
    }
    for key in NUMERIC_SAMPLE_KEYS:
        values = [float(sample[key]) for sample in samples if sample.get(key) is not None]
        averages[key] = statistics.fmean(values) if values else None
    return averages


def diagnosis(averages: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if averages["sink_retries"] > 0:
        notes.append("sink retries occurred; do not use this interval as a clean benchmark")
    if averages["sink_backpressure_percent"] >= 50:
        notes.append("sink backpressure dominated at least half of sampled wall time")
    if averages["parse_busy_percent"] is not None and averages["parse_busy_percent"] >= 90:
        notes.append("parser was near saturation")
    if averages["network_decode_busy_percent"] >= 90:
        notes.append("network decoding was near saturation")
    if averages["sink_busy_percent"] >= 90:
        notes.append(
            "sink attempt load was high; for concurrent S3 uploads it may legitimately exceed 100%"
        )
    if averages["source_records_per_s"] == 0:
        notes.append("source produced no records during the sampled ticks")
    return notes


def format_rate(value: float | None) -> str:
    return "N/A" if value is None else f"{value:,.0f}"


def format_bytes(value: float | None, *, rate: bool = True) -> str:
    if value is None:
        return "N/A"
    suffix = "/s" if rate else ""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}{suffix}"
        value /= 1024
    raise AssertionError("unreachable")


def render_text(averages: dict[str, Any]) -> str:
    parse_busy = averages["parse_busy_percent"]
    lines = [
        f"samples: {averages['sample_count']}  partitions: {averages['partition_ids']}",
        f"guarantees: {', '.join(averages['delivery_guarantees'])}",
        "",
        f"source: {format_rate(averages['source_records_per_s'])} records/s, "
        f"{format_bytes(averages['network_raw_bytes_per_s'])} network raw, "
        f"{format_bytes(averages['network_decoded_bytes_per_s'])} network decoded, "
        f"{averages['response_wait_percent']:.0f}% response-wait, "
        f"{averages['network_decode_busy_percent']:.0f}% network-decode busy",
        f"parser: {format_rate(averages['parse_rows_per_s'])} rows/s, "
        f"{format_bytes(averages['parse_arrow_bytes_per_s'])} Arrow, "
        + ("N/A" if parse_busy is None else f"{parse_busy:.0f}% busy"),
        f"sink: {format_rate(averages['sink_rows_per_s'])} rows/s, "
        f"{format_bytes(averages['sink_bytes_per_s'])}, "
        f"{averages['sink_busy_percent']:.0f}% attempt load, "
        f"{averages['sink_backpressure_percent']:.0f}% backpressure, "
        f"{averages['sink_retries']:.1f} retries/tick",
        f"process: {averages['cpu_percent']:.0f}% CPU, "
        f"{format_bytes(averages['rss_bytes'], rate=False)} RSS",
        "",
        "diagnosis:",
    ]
    notes = diagnosis(averages)
    lines.extend(f"- {note}" for note in notes)
    if not notes:
        lines.append("- no obvious pressure or retry signal")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", nargs="?", type=pathlib.Path)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.log is None:
            samples = read_samples(sys.stdin)
        else:
            with args.log.open(encoding="utf-8") as log:
                samples = read_samples(log)
        averages = average_samples(samples)
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    if args.as_json:
        json.dump({"averages": averages, "diagnosis": diagnosis(averages)}, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        print(render_text(averages))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
