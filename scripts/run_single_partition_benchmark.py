#!/usr/bin/env python3
"""Run and compare reproducible single-partition transferia benchmarks.

Every run receives a fresh 128-bit BENCHMARK_RUN_NAMESPACE. The runner never
deletes external state because it has no persistent connector-owned ownership
proof; the benchmark configuration must use an isolated fixture whose owner
performs verified cleanup.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import platform
import queue
import re
import secrets
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

try:
    from scripts.murmur3_x64_128 import murmur3_x64_128, murmur3_x64_128_file
except ModuleNotFoundError:
    from murmur3_x64_128 import murmur3_x64_128, murmur3_x64_128_file


STATS_PREFIX = re.compile(r"\[stats p=(?P<partition>-?\d+)]")
SOURCE = re.compile(
    r"source: (?P<records>\d+) records/s \| network-raw (?P<network_raw>.+?) \| "
    r"network-decoded (?P<network_decoded>.+?) \| "
    r"response-wait (?P<response_wait>\d+)% \| "
    r"network-decode (?P<network_decode_busy>\d+)% busy"
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
    "PQ_HOST",
    "PQ_PORT",
    "PQ_TOPIC",
    "PQ_CONSUMER_NETWORK",
    "PQ_CONSUMER_DECOMPRESS",
    "PQ_CONSUMER_JSON",
    "PQ_CONSUMER_CLICKHOUSE",
    "PQ_CONSUMER_S3",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_HTTP_ENDPOINT",
    "CLICKHOUSE_DATABASE",
    "CLICKHOUSE_USERNAME",
    "S3_BUCKET",
    "S3_PREFIX",
    "S3_REGION",
    "S3_HOST",
    "S3_PORT",
)
NUMERIC_SAMPLE_KEYS = (
    "source_records_per_s",
    "network_raw_bytes_per_s",
    "network_decoded_bytes_per_s",
    "response_wait_percent",
    "network_decode_busy_percent",
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
CONFIG_PLACEHOLDER = re.compile(
    r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?}"
)


def render_config_template(template: str, environment: dict[str, str]) -> str:
    """Expand the benchmark template explicitly before invoking Transferia."""

    def replacement(match: re.Match[str]) -> str:
        name = match.group("name")
        value = environment.get(name)
        default = match.group("default")
        if value is not None and (value != "" or default is None):
            return value
        if default is not None:
            return default
        raise ValueError(f"benchmark configuration requires environment variable {name}")

    return CONFIG_PLACEHOLDER.sub(replacement, template)


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
        "source_records_per_s": int(source.group("records")),
        "network_raw_bytes_per_s": parse_bytes(source.group("network_raw")),
        "network_decoded_bytes_per_s": parse_bytes(source.group("network_decoded")),
        "response_wait_percent": int(source.group("response_wait")),
        "network_decode_busy_percent": int(source.group("network_decode_busy")),
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
    result = {key: environment[key] for key in REPRODUCIBILITY_ENV_KEYS if key in environment}
    endpoint = result.get("CLICKHOUSE_HTTP_ENDPOINT")
    if endpoint is not None:
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.hostname is None:
            raise ValueError("CLICKHOUSE_HTTP_ENDPOINT must contain a hostname")
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        result["CLICKHOUSE_HTTP_ENDPOINT"] = urllib.parse.urlunsplit(
            (parsed.scheme, f"{host}:{parsed.port}" if parsed.port else host, "", "", "")
        )
    return result


def private_text_write(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(text)


def private_text_output(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    return os.fdopen(descriptor, "w", encoding="utf-8")


def run_namespace(repetition: int) -> str:
    return f"{secrets.token_hex(16)}_{repetition:02d}"


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
    for field in ("config_murmur3_x64_128", "parameters", "platform"):
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
            "mean": statistics.fmean(values),
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
        "metric": "source_records_per_s",
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
    with private_text_output(log_path) as log:
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
    allow_early_completion: bool = False,
) -> dict[str, Any]:
    environment = os.environ.copy()
    environment.setdefault("RUST_LOG", "info")
    environment.setdefault("NO_COLOR", "1")
    environment["BENCHMARK_REPETITION"] = str(repetition)
    environment["BENCHMARK_RUN_NAMESPACE"] = run_namespace(repetition)
    rendered = render_config_template(config.read_text(), environment)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="transferia-benchmark-", suffix=".yaml", delete=False
    ) as rendered_file:
        rendered_file.write(rendered)
        rendered_path = pathlib.Path(rendered_file.name)
    command = [
        str(binary),
        "--config",
        str(rendered_path),
        "--total-workers",
        "1",
        "--worker-index",
        "0",
    ]
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=environment,
        )
    except BaseException:
        rendered_path.unlink(missing_ok=True)
        raise
    log_path = output / f"run-{repetition:02d}.log"
    lines: queue.Queue[tuple[float, str] | None] = queue.Queue()
    reader = threading.Thread(target=stream_output, args=(process, lines, log_path), daemon=True)
    reader.start()
    started = time.monotonic()
    sample_from = started + warmup_seconds
    stop_at = sample_from + sample_seconds
    samples: list[dict[str, Any]] = []
    failures: list[str] = []
    completed_early = False

    def record(item: tuple[float, str] | None) -> None:
        if item is None:
            return
        timestamp, line = item
        lowered = line.lower()
        if any(marker in lowered for marker in FAILURE_MARKERS):
            failures.append("pipeline failure marker")
        if timestamp < sample_from or timestamp > stop_at:
            return
        parsed = parse_stats_line(line)
        if parsed is None:
            return
        if parsed["partition_id"] != 0:
            raise RuntimeError(
                f"unexpected partition in single-partition run: {line.rstrip()}"
            )
        validate_sample(parsed)
        samples.append(parsed)

    try:
        while time.monotonic() < stop_at:
            if process.poll() is not None:
                if not allow_early_completion or process.returncode != 0:
                    raise RuntimeError(
                        f"benchmark process exited early with status {process.returncode}"
                    )
                completed_early = True
                break
            try:
                item = lines.get(timeout=min(0.25, max(0.01, stop_at - time.monotonic())))
            except queue.Empty:
                continue
            record(item)
    finally:
        terminate(process)
        reader.join(timeout=2)
        while True:
            try:
                record(lines.get_nowait())
            except queue.Empty:
                break
        if process.stdout is not None and hasattr(process.stdout, "close"):
            process.stdout.close()
        rendered_path.unlink(missing_ok=True)
    if failures:
        raise RuntimeError(
            "pipeline failures occurred during benchmark; inspect the private run log"
        )
    if len(samples) < min_samples:
        raise RuntimeError(f"only {len(samples)} stats samples captured; expected at least {min_samples}")
    nonzero = sum(sample["source_records_per_s"] > 0 for sample in samples)
    if nonzero < min_samples:
        raise RuntimeError(
            f"only {nonzero} non-zero source samples captured; the finite input may have drained"
        )
    return {
        "repetition": repetition,
        "namespace": environment["BENCHMARK_RUN_NAMESPACE"],
        "log": str(log_path),
        "elapsed_seconds": time.monotonic() - started,
        "completed_naturally": completed_early,
        "sample_count": len(samples),
        "summary": summarize_samples(samples),
    }


def load_primary_runs(document: dict[str, Any]) -> list[float]:
    try:
        return [float(run["summary"]["source_records_per_s"]["median"]) for run in document["runs"]]
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("baseline does not contain per-run source_records_per_s medians") from error


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
    parser.add_argument(
        "--allow-early-completion",
        action="store_true",
        help="accept a finite source that drains after the minimum steady-state samples",
    )
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
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    output.chmod(0o700)
    config_bytes = config.read_bytes()
    document: dict[str, Any] = {
        "schema_version": 5,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": str(config),
        "config_murmur3_x64_128": murmur3_x64_128(config_bytes),
        "binary": str(binary),
        "binary_murmur3_x64_128": murmur3_x64_128_file(binary),
        "git_commit": command_output(["git", "rev-parse", "HEAD"]),
        "git_dirty": bool(command_output(["git", "status", "--porcelain"])),
        "rustc": command_output(["rustc", "--version", "--verbose"]),
        "platform": platform.uname()._asdict(),
        "parameters": {
            "warmup_seconds": args.warmup_seconds,
            "sample_seconds": args.sample_seconds,
            "repetitions": args.repetitions,
            "min_samples": args.min_samples,
            "allow_early_completion": args.allow_early_completion,
        },
        "environment": reproducibility_environment(os.environ),
        "runs": [],
    }
    result_path = output / "result.json"
    try:
        for repetition in range(1, args.repetitions + 1):
            print(f"run {repetition}/{args.repetitions}", flush=True)
            run = run_once(
                binary,
                config,
                output,
                repetition,
                args.warmup_seconds,
                args.sample_seconds,
                args.min_samples,
                args.allow_early_completion,
            )
            document["runs"].append(run)
        primary = load_primary_runs(document)
        document["primary_summary"] = summarize_samples(
            [{"partition_id": 0, "source_records_per_s": value} for value in primary]
        )["source_records_per_s"]
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
        private_text_write(
            result_path,
            json.dumps(document, indent=2, sort_keys=True),
        )
        print(json.dumps(document.get("comparison", document["primary_summary"]), indent=2))
        print(f"result: {result_path}")
        return exit_code
    except BaseException as error:
        document["error"] = str(error)
        private_text_write(
            result_path,
            json.dumps(document, indent=2, sort_keys=True),
        )
        raise


if __name__ == "__main__":
    sys.exit(main())
