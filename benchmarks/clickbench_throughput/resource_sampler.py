#!/usr/bin/env python3
"""Measure one Linux process during a fixed Transferia benchmark window.

The sampler attaches to an already-running process and tails only new stable
``[stats]`` log lines.  It never reads or records the process command line,
environment, configuration, credentials, or raw log text.

CPU is the target process's total user+system CPU time across all of its
threads, excluding child processes and external database services.  Average
CPU cores is the CPU-time delta divided by elapsed monotonic wall time.  RSS is
sampled from ``/proc/<pid>/status``; average RSS is the trapezoidal time average
and peak RSS is the largest sampled value, so spikes shorter than the sampling
interval can be missed.

Rows/s comes from sink rates in log lines completed during the measurement
window by default.  Aggregate ``[stats]`` rates are averaged directly.  For
``[stats p=N]`` output, each partition must have the same sample count and the
result is the sum of the per-partition means.  Mixing both log modes is rejected
to avoid double-counting.  The first/last reporting intervals may overlap the
window boundary by at most one metrics reporting interval.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import os
import pathlib
import re
import statistics
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, BinaryIO


STATS_PREFIX = re.compile(r"\[stats(?: p=(?P<partition>-?\d+))?]")
SOURCE_RATE = re.compile(r"source: (?P<rate>\d+) records/s")
SINK_RATE = re.compile(r"sink: (?P<rate>\d+) rows/s")


@dataclasses.dataclass(frozen=True)
class ProcessReading:
    elapsed_seconds: float
    cpu_seconds: float
    rss_bytes: int
    start_time_ticks: int


@dataclasses.dataclass(frozen=True)
class StatsSample:
    partition: int | None
    rows_per_second: int


def parse_proc_stat(text: str, clock_ticks_per_second: int) -> tuple[float, int]:
    """Return process CPU seconds and start-time ticks from /proc/PID/stat."""

    if clock_ticks_per_second <= 0:
        raise ValueError("clock_ticks_per_second must be positive")
    closing_parenthesis = text.rfind(")")
    if closing_parenthesis < 0:
        raise ValueError("invalid /proc stat: missing process-name terminator")
    fields = text[closing_parenthesis + 1 :].split()
    # `fields[0]` is proc(5) field 3 (state); utime, stime and starttime are
    # fields 14, 15 and 22 respectively.
    if len(fields) < 20:
        raise ValueError("invalid /proc stat: too few fields")
    try:
        cpu_ticks = int(fields[11]) + int(fields[12])
        start_time_ticks = int(fields[19])
    except ValueError as error:
        raise ValueError("invalid numeric field in /proc stat") from error
    return cpu_ticks / clock_ticks_per_second, start_time_ticks


def parse_rss_bytes(status: str) -> int:
    match = re.search(r"^VmRSS:\s+(?P<kib>\d+)\s+kB$", status, re.MULTILINE)
    if match is None:
        raise ValueError("/proc status does not contain VmRSS")
    return int(match.group("kib")) * 1024


def read_process(pid: int, started: float, clock_ticks_per_second: int) -> ProcessReading:
    proc = pathlib.Path("/proc") / str(pid)
    try:
        stat = (proc / "stat").read_text(encoding="utf-8")
        status = (proc / "status").read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise RuntimeError(f"process {pid} exited during the benchmark") from error
    cpu_seconds, start_time_ticks = parse_proc_stat(stat, clock_ticks_per_second)
    return ProcessReading(
        elapsed_seconds=time.monotonic() - started,
        cpu_seconds=cpu_seconds,
        rss_bytes=parse_rss_bytes(status),
        start_time_ticks=start_time_ticks,
    )


def parse_stats_lines(lines: Iterable[str], metric: str) -> list[StatsSample]:
    rate_pattern = {"source": SOURCE_RATE, "sink": SINK_RATE}[metric]
    samples: list[StatsSample] = []
    for line_number, line in enumerate(lines, 1):
        if "[stats" not in line:
            continue
        prefix = STATS_PREFIX.search(line)
        rate = rate_pattern.search(line)
        if prefix is None or rate is None:
            raise ValueError(
                f"unrecognized {metric} throughput in [stats] line {line_number}"
            )
        partition = prefix.group("partition")
        samples.append(
            StatsSample(
                partition=None if partition is None else int(partition),
                rows_per_second=int(rate.group("rate")),
            )
        )
    if not samples:
        raise ValueError("measurement window contains no complete [stats] samples")
    return samples


def summarize_throughput(samples: list[StatsSample]) -> dict[str, Any]:
    aggregate = [sample for sample in samples if sample.partition is None]
    partitioned = [sample for sample in samples if sample.partition is not None]
    if aggregate and partitioned:
        raise ValueError("measurement mixes aggregate and per-partition [stats] lines")
    if aggregate:
        rates = [sample.rows_per_second for sample in aggregate]
        return {
            "rows_per_second": statistics.fmean(rates),
            "stats_mode": "aggregate",
            "stats_samples": len(rates),
            "reporting_intervals": len(rates),
            "partitions": None,
        }

    by_partition: dict[int, list[int]] = defaultdict(list)
    for sample in partitioned:
        assert sample.partition is not None
        by_partition[sample.partition].append(sample.rows_per_second)
    sample_counts = {len(rates) for rates in by_partition.values()}
    if len(sample_counts) != 1:
        raise ValueError(
            "per-partition [stats] sample counts differ; use aggregate stats for a "
            "lossless whole-window rate"
        )
    reporting_intervals = sample_counts.pop()
    return {
        "rows_per_second": sum(
            statistics.fmean(rates) for rates in by_partition.values()
        ),
        "stats_mode": "per_partition",
        "stats_samples": len(samples),
        "reporting_intervals": reporting_intervals,
        "partitions": sorted(by_partition),
    }


def summarize_process(readings: list[ProcessReading]) -> dict[str, Any]:
    if len(readings) < 2:
        raise ValueError("at least two process samples are required")
    if len({reading.start_time_ticks for reading in readings}) != 1:
        raise RuntimeError("target PID was reused during the benchmark")
    elapsed_seconds = readings[-1].elapsed_seconds - readings[0].elapsed_seconds
    cpu_seconds = readings[-1].cpu_seconds - readings[0].cpu_seconds
    if elapsed_seconds <= 0:
        raise ValueError("process sample interval must be positive")
    if cpu_seconds < 0:
        raise RuntimeError("process CPU time decreased during the benchmark")

    rss_integral = 0.0
    for left, right in zip(readings, readings[1:]):
        width = right.elapsed_seconds - left.elapsed_seconds
        if width <= 0:
            raise ValueError("process sample times must be strictly increasing")
        rss_integral += width * (left.rss_bytes + right.rss_bytes) / 2
    return {
        "elapsed_seconds": elapsed_seconds,
        "cpu_seconds": cpu_seconds,
        "cpu_cores_average": cpu_seconds / elapsed_seconds,
        "rss_bytes_average": rss_integral / elapsed_seconds,
        "rss_bytes_peak": max(reading.rss_bytes for reading in readings),
        "process_samples": len(readings),
    }


def collect_process_readings(
    pid: int, duration_seconds: float, sample_interval_seconds: float
) -> list[ProcessReading]:
    started = time.monotonic()
    deadline = started + duration_seconds
    clock_ticks_per_second = os.sysconf("SC_CLK_TCK")
    readings = [read_process(pid, started, clock_ticks_per_second)]
    expected_start_time = readings[0].start_time_ticks
    next_sample = started + sample_interval_seconds
    while True:
        now = time.monotonic()
        if now >= deadline:
            break
        time.sleep(max(0.0, min(next_sample, deadline) - now))
        reading = read_process(pid, started, clock_ticks_per_second)
        if reading.start_time_ticks != expected_start_time:
            raise RuntimeError("target PID was reused during the benchmark")
        readings.append(reading)
        next_sample += sample_interval_seconds
    if readings[-1].elapsed_seconds < duration_seconds:
        readings.append(read_process(pid, started, clock_ticks_per_second))
    return readings


def build_report(
    pid: int,
    requested_duration_seconds: float,
    sample_interval_seconds: float,
    metric: str,
    readings: list[ProcessReading],
    stats_samples: list[StatsSample],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "requested_duration_seconds": requested_duration_seconds,
        "sample_interval_seconds": sample_interval_seconds,
        "throughput_metric": f"{metric}_rows_per_second",
        "process_scope": {
            "pid": pid,
            "includes": "all threads of the target process",
            "excludes": "child processes and external services",
        },
        "measurement_semantics": {
            "cpu_cores_average": "delta(user+system CPU seconds) / monotonic wall seconds",
            "rss_bytes_average": "trapezoidal time average of sampled VmRSS",
            "rss_bytes_peak": "maximum sampled VmRSS; sub-interval spikes may be missed",
            "rows_per_second": (
                "mean aggregate [stats] rate, or sum of equally sampled per-partition means; "
                "boundary overlap is at most one metrics reporting interval"
            ),
        },
        "process": summarize_process(readings),
        "throughput": summarize_throughput(stats_samples),
    }


def private_atomic_write(path: pathlib.Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(document, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_log_window(log: BinaryIO, start_offset: int, end_offset: int) -> list[str]:
    if end_offset < start_offset:
        raise RuntimeError("stats log was truncated during the benchmark")
    log.seek(start_offset)
    expected = end_offset - start_offset
    appended = log.read(expected)
    if len(appended) != expected:
        raise RuntimeError("stats log changed while capturing the measurement boundary")
    return appended.decode("utf-8", errors="replace").splitlines()


def measure(args: argparse.Namespace) -> dict[str, Any]:
    with args.stats_log.open("rb") as log:
        original = os.fstat(log.fileno())
        log.seek(0, os.SEEK_END)
        start_offset = log.tell()
        readings = collect_process_readings(
            args.pid, args.duration_seconds, args.sample_interval_seconds
        )
        current_path = args.stats_log.stat()
        if (current_path.st_dev, current_path.st_ino) != (
            original.st_dev,
            original.st_ino,
        ):
            raise RuntimeError("stats log rotated during the benchmark")
        end_offset = current_path.st_size
        appended_lines = read_log_window(log, start_offset, end_offset)
    samples = parse_stats_lines(appended_lines, args.metric)
    return build_report(
        args.pid,
        args.duration_seconds,
        args.sample_interval_seconds,
        args.metric,
        readings,
        samples,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--stats-log", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=120.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--metric", choices=("source", "sink"), default="sink")
    args = parser.parse_args(argv)
    if args.pid <= 0:
        parser.error("--pid must be positive")
    if not math.isfinite(args.duration_seconds) or args.duration_seconds <= 0:
        parser.error("--duration-seconds must be finite and positive")
    if (
        not math.isfinite(args.sample_interval_seconds)
        or args.sample_interval_seconds <= 0
        or args.sample_interval_seconds > args.duration_seconds
    ):
        parser.error(
            "--sample-interval-seconds must be finite, positive, and no larger than duration"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = measure(args)
        private_atomic_write(args.output, report)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"resource sampler failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
