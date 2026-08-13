#!/usr/bin/env python3
"""Run and compare reproducible single-partition transferia benchmarks."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import queue
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


STATS_PREFIX = re.compile(r"\[stats p=(?P<partition>-?\d+)]")
SOURCE = re.compile(
    r"source: (?P<messages>\d+) msg/s \| comp (?P<compressed>.+?) \| "
    r"decomp (?P<decompressed>.+?) \| "
    r"response-wait (?P<response_wait>\d+)% \| "
    r"decomp (?P<decomp_busy>\d+)% busy"
)
PARSE = re.compile(
    r"parse: (?P<rows>\d+) rows/s \| (?P<arrow>.+?) arrow \| "
    r"(?P<dlq>\d+) dlq/s \| (?P<source_messages>\d+) source-msg/s \| "
    r"(?P<busy>\d+)% busy"
)
SINK = re.compile(
    r"sink: (?P<rows>\d+) rows/s \| (?P<bytes>.+?) \| "
    r"(?P<flushes>\d+) flushes/s \| (?P<source_messages>\d+) source-msg/s \| "
    r"(?P<busy>\d+)% busy \| (?P<retries>\d+) retries \| buffered (?P<buffered>.+?) \| "
    r"objects (?P<open>\d+)/(?P<ready>\d+)/(?P<inflight>\d+) \| "
    r"(?P<backpressure>\d+)% backpressure"
)
TAIL = re.compile(r"guarantee: (?P<guarantee>.+?) \| cpu: (?P<cpu>\d+)% rss: (?P<rss>.+)$")
BYTE_VALUE = re.compile(r"(?P<value>\d+(?:\.\d+)?) (?P<unit>[KMGT]iB|B)(?:/s)?$")
BYTE_MULTIPLIERS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}
FAILURE_MARKERS = (
    "pipeline failed, restarting",
    "partition task failed",
    "non-retryable partition failure",
    "exhausted 5 consecutive failures",
    "panicked at",
    "failed, retrying",
    "retryable s3 upload failure",
)
REPRODUCIBILITY_ENV_KEYS = (
    "PQ_ENDPOINT",
    "PQ_TOPIC",
    "PQ_CONSUMER_NETWORK",
    "PQ_CONSUMER_DECOMPRESS",
    "PQ_CONSUMER_JSON",
    "PQ_CONSUMER_CLICKHOUSE",
    "PQ_CONSUMER_S3",
    "CLICKHOUSE_ENDPOINT",
    "CLICKHOUSE_HTTP_ENDPOINT",
    "CLICKHOUSE_DATABASE",
    "CLICKHOUSE_USERNAME",
    "S3_BUCKET",
    "S3_PREFIX",
    "S3_REGION",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
)
NUMERIC_SAMPLE_KEYS = (
    "source_messages_per_s",
    "compressed_bytes_per_s",
    "decompressed_bytes_per_s",
    "response_wait_percent",
    "decompression_busy_percent",
    "parse_rows_per_s",
    "parse_arrow_bytes_per_s",
    "parse_busy_percent",
    "sink_rows_per_s",
    "sink_bytes_per_s",
    "sink_busy_percent",
    "sink_backpressure_percent",
    "sink_retries",
    "cpu_percent",
    "rss_bytes",
)
CONSUMER_ENV_PREFIX = "PQ_CONSUMER_"


def parse_bytes(text: str) -> float:
    text = text.strip()
    if text == "N/A":
        return 0.0
    match = BYTE_VALUE.fullmatch(text)
    if match is None:
        raise ValueError(f"unsupported byte value: {text!r}")
    return float(match.group("value")) * BYTE_MULTIPLIERS[match.group("unit")]


def parse_stats_line(line: str) -> dict[str, Any] | None:
    partition = STATS_PREFIX.search(line)
    if partition is None:
        return None
    source = SOURCE.search(line)
    sink = SINK.search(line)
    tail = TAIL.search(line)
    if source is None or sink is None or tail is None:
        raise ValueError(f"unrecognized stats line: {line.rstrip()}")
    parse = PARSE.search(line)
    return {
        "partition_id": int(partition.group("partition")),
        "source_messages_per_s": int(source.group("messages")),
        "compressed_bytes_per_s": parse_bytes(source.group("compressed")),
        "decompressed_bytes_per_s": parse_bytes(source.group("decompressed")),
        "response_wait_percent": int(source.group("response_wait")),
        "decompression_busy_percent": int(source.group("decomp_busy")),
        "parse_rows_per_s": int(parse.group("rows")) if parse else None,
        "parse_arrow_bytes_per_s": parse_bytes(parse.group("arrow")) if parse else None,
        "parse_busy_percent": int(parse.group("busy")) if parse else None,
        "sink_rows_per_s": int(sink.group("rows")),
        "sink_bytes_per_s": parse_bytes(sink.group("bytes")),
        "sink_busy_percent": int(sink.group("busy")),
        "sink_backpressure_percent": int(sink.group("backpressure")),
        "sink_retries": int(sink.group("retries")),
        "cpu_percent": int(tail.group("cpu")),
        "rss_bytes": parse_bytes(tail.group("rss")),
        "delivery_guarantee": tail.group("guarantee"),
    }


def validate_sample(sample: dict[str, Any]) -> None:
    if int(sample.get("sink_retries", 0)) != 0:
        raise RuntimeError("sink retry observed during benchmark sample")


def reproducibility_environment(environment: dict[str, str]) -> dict[str, str]:
    return {key: environment[key] for key in REPRODUCIBILITY_ENV_KEYS if key in environment}


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_namespace(output: pathlib.Path, repetition: int) -> str:
    arm = hashlib.sha256(str(output.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"{arm}_{repetition:02d}"


def clickhouse_identifier(value: str) -> str:
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def clickhouse_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def clickhouse_query(environment: dict[str, str], sql: str) -> str:
    endpoint = environment.get("CLICKHOUSE_HTTP_ENDPOINT", "http://localhost:8123").rstrip("/")
    query = urllib.parse.urlencode(
        {
            "database": environment.get("CLICKHOUSE_DATABASE", "default"),
            "query": sql,
        }
    )
    request = urllib.request.Request(
        f"{endpoint}/?{query}",
        headers={
            "X-ClickHouse-User": environment.get("CLICKHOUSE_USERNAME", "default"),
            "X-ClickHouse-Key": environment.get("CLICKHOUSE_PASSWORD", ""),
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode("utf-8")


def cleanup_clickhouse_run(
    environment: dict[str, str],
    table: str,
    timeout_seconds: float = 30,
    *,
    require_existing: bool = False,
) -> None:
    database = environment.get("CLICKHOUSE_DATABASE", "default")
    dlq_table = f"{table}_dlq"
    if require_existing:
        existing_query = (
            "SELECT count() FROM system.tables WHERE database = "
            f"{clickhouse_literal(database)} AND name IN "
            f"({clickhouse_literal(table)}, {clickhouse_literal(dlq_table)})"
        )
        existing = int(clickhouse_query(environment, existing_query).strip())
        if existing != 2:
            raise RuntimeError(
                "ClickHouse cleanup endpoint did not expose the expected both benchmark tables; "
                "check CLICKHOUSE_HTTP_ENDPOINT"
            )
    for name in (table, dlq_table):
        clickhouse_query(
            environment,
            f"DROP TABLE IF EXISTS {clickhouse_identifier(database)}."
            f"{clickhouse_identifier(name)} SYNC",
        )
    merge_query = (
        "SELECT count() FROM system.merges WHERE database = "
        f"{clickhouse_literal(database)} AND table IN "
        f"({clickhouse_literal(table)}, {clickhouse_literal(dlq_table)})"
    )
    deadline = time.monotonic() + timeout_seconds
    while True:
        active = int(clickhouse_query(environment, merge_query).strip())
        if active == 0:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"ClickHouse background merges did not quiesce for table {table}")
        time.sleep(0.1)


def cleanup_run(
    config_text: str,
    environment: dict[str, str],
    namespace: str,
    *,
    require_existing: bool,
) -> None:
    if re.search(r"(?m)^\s{2}clickhouse:\s*(?:\{\})?\s*$", config_text):
        cleanup_clickhouse_run(
            environment,
            f"events_{namespace}",
            require_existing=require_existing,
        )


def run_repetition(
    binary: pathlib.Path,
    config: pathlib.Path,
    config_text: str,
    output: pathlib.Path,
    repetition: int,
    warmup_seconds: float,
    sample_seconds: float,
    min_samples: int,
    environment: dict[str, str],
) -> dict[str, Any]:
    namespace = run_namespace(output, repetition)
    try:
        result = run_once(
            binary,
            config,
            output,
            repetition,
            warmup_seconds,
            sample_seconds,
            min_samples,
        )
    except BaseException as run_error:
        try:
            cleanup_run(config_text, environment, namespace, require_existing=False)
        except BaseException as cleanup_error:
            raise run_error from cleanup_error
        raise
    cleanup_run(config_text, environment, namespace, require_existing=True)
    return result


def comparison_environment(document: dict[str, Any]) -> dict[str, str]:
    environment = document.get("environment")
    if not isinstance(environment, dict):
        raise ValueError("benchmark result has no reproducibility environment")
    return {
        str(key): str(value)
        for key, value in environment.items()
        if not str(key).startswith(CONSUMER_ENV_PREFIX)
    }


def validate_comparison_context(current: dict[str, Any], baseline: dict[str, Any]) -> None:
    if current.get("schema_version") != baseline.get("schema_version"):
        raise ValueError("baseline schema_version does not match the current result")
    for field in ("config_sha256", "parameters", "platform"):
        if current.get(field) != baseline.get(field):
            raise ValueError(f"baseline {field} does not match the current result")
    if comparison_environment(current) != comparison_environment(baseline):
        raise ValueError(
            "baseline environment does not match the current result; only PQ_CONSUMER_* "
            "prefixes may differ"
        )


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, dict[str, float | int]]:
    summary: dict[str, dict[str, float | int]] = {}
    for key in NUMERIC_SAMPLE_KEYS:
        values = [float(sample[key]) for sample in samples if sample.get(key) is not None]
        if not values:
            continue
        median = statistics.median(values)
        deviations = [abs(value - median) for value in values]
        summary[key] = {
            "count": len(values),
            "median": median,
            "mad": statistics.median(deviations),
            "p10": percentile(values, 0.10),
            "p90": percentile(values, 0.90),
            "min": min(values),
            "max": max(values),
        }
    return summary


def compare_primary_runs(
    current: list[float], baseline: list[float], regression_fraction: float = 0.05
) -> dict[str, Any]:
    if not current or len(current) != len(baseline):
        raise ValueError("current and baseline must contain the same non-zero number of runs")
    ratios = [now / before if before > 0 else 1.0 for now, before in zip(current, baseline)]
    regressed_pairs = sum(ratio < 1.0 - regression_fraction for ratio in ratios)
    required_pairs = math.ceil(len(ratios) * 0.8)
    current_median = statistics.median(current)
    baseline_median = statistics.median(baseline)
    median_ratio = current_median / baseline_median if baseline_median > 0 else 1.0
    return {
        "metric": "source_messages_per_s",
        "current_median": current_median,
        "baseline_median": baseline_median,
        "median_ratio": median_ratio,
        "regressed_pairs": regressed_pairs,
        "required_regressed_pairs": required_pairs,
        "regression_fraction": regression_fraction,
        "regression": median_ratio < 1.0 - regression_fraction
        and regressed_pairs >= required_pairs,
    }


def command_output(command: list[str]) -> str:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return "unavailable"


def terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def stream_output(
    process: subprocess.Popen[str], lines: queue.Queue[tuple[float, str] | None], log_path: pathlib.Path
) -> None:
    assert process.stdout is not None
    with log_path.open("w", encoding="utf-8") as log:
        for line in process.stdout:
            timestamp = time.monotonic()
            log.write(line)
            log.flush()
            lines.put((timestamp, line))
    lines.put(None)


def run_once(
    binary: pathlib.Path,
    config: pathlib.Path,
    output: pathlib.Path,
    repetition: int,
    warmup_seconds: float,
    sample_seconds: float,
    min_samples: int,
) -> dict[str, Any]:
    command = [
        str(binary),
        "--config",
        str(config),
        "--total-workers",
        "1",
        "--worker-index",
        "0",
    ]
    environment = os.environ.copy()
    environment.setdefault("RUST_LOG", "info")
    environment.setdefault("NO_COLOR", "1")
    environment["BENCHMARK_REPETITION"] = str(repetition)
    environment["BENCHMARK_RUN_NAMESPACE"] = run_namespace(output, repetition)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=environment,
    )
    log_path = output / f"run-{repetition:02d}.log"
    lines: queue.Queue[tuple[float, str] | None] = queue.Queue()
    reader = threading.Thread(target=stream_output, args=(process, lines, log_path), daemon=True)
    reader.start()
    started = time.monotonic()
    sample_from = started + warmup_seconds
    stop_at = sample_from + sample_seconds
    samples: list[dict[str, Any]] = []
    failures: list[str] = []
    try:
        while time.monotonic() < stop_at:
            if process.poll() is not None:
                raise RuntimeError(f"benchmark process exited early with status {process.returncode}")
            try:
                item = lines.get(timeout=min(0.25, max(0.01, stop_at - time.monotonic())))
            except queue.Empty:
                continue
            if item is None:
                continue
            timestamp, line = item
            lowered = line.lower()
            if any(marker in lowered for marker in FAILURE_MARKERS):
                failures.append(line.rstrip())
            if timestamp < sample_from:
                continue
            parsed = parse_stats_line(line)
            if parsed is not None:
                if parsed["partition_id"] != 0:
                    raise RuntimeError(f"unexpected partition in single-partition run: {line.rstrip()}")
                validate_sample(parsed)
                samples.append(parsed)
    finally:
        terminate(process)
        reader.join(timeout=2)
        if process.stdout is not None and hasattr(process.stdout, "close"):
            process.stdout.close()
    if failures:
        raise RuntimeError("pipeline failures occurred during benchmark: " + "; ".join(failures))
    if len(samples) < min_samples:
        raise RuntimeError(f"only {len(samples)} stats samples captured; expected at least {min_samples}")
    nonzero = sum(sample["source_messages_per_s"] > 0 for sample in samples)
    if nonzero < min_samples:
        raise RuntimeError(f"only {nonzero} non-zero PQ samples captured; backlog may have drained")
    return {
        "repetition": repetition,
        "namespace": environment["BENCHMARK_RUN_NAMESPACE"],
        "log": str(log_path),
        "sample_count": len(samples),
        "summary": summarize_samples(samples),
    }


def load_primary_runs(document: dict[str, Any]) -> list[float]:
    try:
        return [float(run["summary"]["source_messages_per_s"]["median"]) for run in document["runs"]]
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("baseline does not contain per-run source_messages_per_s medians") from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--binary", type=pathlib.Path, default=pathlib.Path("target/release/transferia"))
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--warmup-seconds", type=float, default=30)
    parser.add_argument("--sample-seconds", type=float, default=90)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--min-samples", type=int, default=80)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--regression-percent", type=float, default=5.0)
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.repetitions <= 0 or args.warmup_seconds < 0 or args.sample_seconds <= 0:
        raise SystemExit("repetitions and sample duration must be positive; warmup must be non-negative")
    if args.min_samples <= 0:
        raise SystemExit("min-samples must be positive")
    config = args.config.resolve()
    if not config.is_file():
        raise SystemExit(f"config does not exist: {config}")
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--release", "--bin", "transferia"], check=True)
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary does not exist: {binary}")
    output = args.output_dir or pathlib.Path("benchmark-results") / datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    config_bytes = config.read_bytes()
    config_text = config_bytes.decode("utf-8")
    document: dict[str, Any] = {
        "schema_version": 2,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": str(config),
        "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "binary": str(binary),
        "binary_sha256": sha256_file(binary),
        "git_commit": command_output(["git", "rev-parse", "HEAD"]),
        "git_dirty": bool(command_output(["git", "status", "--porcelain"])),
        "rustc": command_output(["rustc", "--version", "--verbose"]),
        "platform": platform.uname()._asdict(),
        "parameters": {
            "warmup_seconds": args.warmup_seconds,
            "sample_seconds": args.sample_seconds,
            "repetitions": args.repetitions,
            "min_samples": args.min_samples,
        },
        "environment": reproducibility_environment(os.environ),
        "runs": [],
    }
    result_path = output / "result.json"
    try:
        for repetition in range(1, args.repetitions + 1):
            print(f"run {repetition}/{args.repetitions}", flush=True)
            run = run_repetition(
                binary,
                config,
                config_text,
                output,
                repetition,
                args.warmup_seconds,
                args.sample_seconds,
                args.min_samples,
                os.environ,
            )
            document["runs"].append(run)
        primary = load_primary_runs(document)
        document["primary_summary"] = summarize_samples(
            [{"partition_id": 0, "source_messages_per_s": value} for value in primary]
        )["source_messages_per_s"]
        exit_code = 0
        if args.baseline:
            baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
            validate_comparison_context(document, baseline)
            comparison = compare_primary_runs(
                primary,
                load_primary_runs(baseline),
                args.regression_percent / 100.0,
            )
            document["baseline"] = str(args.baseline.resolve())
            document["comparison"] = comparison
            exit_code = 2 if comparison["regression"] else 0
        result_path.write_text(json.dumps(document, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(document.get("comparison", document["primary_summary"]), indent=2))
        print(f"result: {result_path}")
        return exit_code
    except BaseException as error:
        document["error"] = str(error)
        result_path.write_text(json.dumps(document, indent=2, sort_keys=True), encoding="utf-8")
        raise


if __name__ == "__main__":
    sys.exit(main())
