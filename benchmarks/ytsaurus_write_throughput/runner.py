#!/usr/bin/env python3
"""Benchmark complete generator-to-YTsaurus static-table deliveries."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import random
import re
import signal
import statistics
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml


@dataclasses.dataclass(frozen=True)
class Candidate:
    name: str
    settings: dict[str, Any]


@dataclasses.dataclass
class ScanResult:
    elapsed_seconds: float
    cpu_seconds: float
    max_rss_bytes: int
    log: str


def required(mapping: dict[str, Any], key: str, path: str) -> Any:
    value = mapping.get(key)
    if value is None or value == "":
        raise ValueError(f"{path}.{key} is required")
    return value


def secret_file(spec: Any, path: str) -> pathlib.Path:
    if not isinstance(spec, dict) or set(spec) != {"file"}:
        raise ValueError(f"{path} must contain only 'file'")
    source = pathlib.Path(os.path.expanduser(str(spec["file"]))).resolve()
    if not source.read_text().strip():
        raise ValueError(f"{path} points to an empty file")
    return source


def read_secret(spec: Any, path: str) -> str:
    return secret_file(spec, path).read_text().strip()


def private_write(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def configured_path(value: Any) -> pathlib.Path:
    return pathlib.Path(os.path.expanduser(str(value))).resolve()


def monitor_process(pid: int, stop: threading.Event, metrics: dict[str, float]) -> None:
    ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    while not stop.wait(0.1):
        try:
            status = pathlib.Path(f"/proc/{pid}/status").read_text()
            stat = pathlib.Path(f"/proc/{pid}/stat").read_text().split()
        except (FileNotFoundError, ProcessLookupError):
            return
        rss = re.search(r"^VmRSS:\s+(\d+) kB$", status, re.MULTILINE)
        if rss:
            metrics["max_rss_bytes"] = max(
                metrics.get("max_rss_bytes", 0), int(rss.group(1)) * 1024
            )
        metrics["cpu_seconds"] = (int(stat[13]) + int(stat[14])) / ticks


def delivery_config(
    raw: dict[str, Any], candidate: Candidate, table_root: str,
    yt_token_file: pathlib.Path, state: pathlib.Path,
) -> dict[str, Any]:
    yt = raw["ytsaurus"]
    sink: dict[str, Any] = {
        "installation": {"type": "cluster", "cluster": required(yt, "cluster", "ytsaurus")},
        "auth": {"type": "token_file", "token_file": str(yt_token_file)},
        "tables": {
            "type": "static_tables",
            "path": table_root,
            "replace_tables": True,
            "format": candidate.settings.get("format", "arrow"),
        },
        "timeout_ms": int(raw["run"].get("timeout_seconds", 900)) * 1_000,
    }
    sink.update({key: value for key, value in candidate.settings.items() if key != "format"})
    return {
        "delivery_id": f"yt-write-{uuid.uuid4()}",
        "durable_storage": {"type": "local_file", "path": str(state)},
        "delivery_type": "batch",
        "source": {
            "data_generator": {
                "table_name": str(raw["run"].get("table_name", "my_table")),
                "preset": {"type": "transfer_logs"},
                "amount": {"type": "rows", "row_count": int(raw["run"].get("rows", 50_000_000))},
            }
        },
        "sink": {"ytsaurus": sink},
        "middlewares": [],
        "pipeline_memory_limit_bytes": int(raw["run"].get("pipeline_memory_limit_bytes", 2 << 30)),
        "metrics": {"interval_ms": 1_000},
    }


def yt_command(raw: dict[str, Any], *arguments: str) -> list[str]:
    tools = raw["tools"]
    return [
        str(configured_path(required(tools, "yt_binary", "tools"))),
        *[str(item) for item in tools.get("yt_arguments", [])],
        *arguments,
    ]


def cleanup(raw: dict[str, Any], path: str, environment: dict[str, str]) -> None:
    completed = subprocess.run(
        yt_command(raw, "remove", path, "--recursive", "--force"),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120,
    )
    if completed.returncode:
        raise RuntimeError(f"failed to remove owned YTsaurus path {path}: {completed.stdout[-1000:]}")


def verify_rows(
    raw: dict[str, Any], table_root: str, environment: dict[str, str]
) -> int:
    table_name = str(raw["run"].get("table_name", "my_table"))
    completed = subprocess.run(
        yt_command(raw, "get", f"{table_root}/{table_name}/@row_count"),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120,
    )
    if completed.returncode:
        raise RuntimeError(f"failed to verify YTsaurus row count: {completed.stdout[-1000:]}")
    rows = int(completed.stdout.strip())
    expected = int(raw["run"].get("rows", 50_000_000))
    if rows != expected:
        raise RuntimeError(f"YTsaurus row count is {rows}, expected {expected}")
    return rows


def run_scan(
    raw: dict[str, Any], candidate: Candidate, repetition: int, scan: int,
    root: pathlib.Path, yt_token_file: pathlib.Path, environment: dict[str, str],
) -> ScanResult:
    run = pathlib.Path(candidate.name) / f"repetition-{repetition}" / f"scan-{scan:03d}"
    work = root / "work" / run
    config = work / "delivery.yaml"
    log = root / "raw" / run.with_suffix(".log")
    table_root = (
        str(required(raw["ytsaurus"], "root", "ytsaurus")).rstrip("/")
        + f"/{root.name}-{candidate.name}-r{repetition}-s{scan}"
    )
    private_write(
        config,
        yaml.safe_dump(
            delivery_config(raw, candidate, table_root, yt_token_file, work / "state"),
            sort_keys=False,
        ),
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    binary = configured_path(required(raw["tools"], "rust_binary", "tools"))
    metrics: dict[str, float] = {}
    started = time.monotonic()
    try:
        with log.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(
                [str(binary), "--config", str(config), "--total-workers", "1", "--worker-index", "0"],
                env=environment,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            stop = threading.Event()
            monitor = threading.Thread(
                target=monitor_process, args=(process.pid, stop, metrics), daemon=True
            )
            monitor.start()
            try:
                exit_code = process.wait(timeout=float(raw["run"].get("timeout_seconds", 900)))
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            finally:
                stop.set()
                monitor.join(timeout=2)
        elapsed = time.monotonic() - started
        if exit_code:
            tail = "\n".join(log.read_text(errors="replace").splitlines()[-40:])
            raise RuntimeError(f"{candidate.name} failed with exit code {exit_code}:\n{tail}")
        verify_rows(raw, table_root, environment)
        return ScanResult(
            elapsed_seconds=elapsed,
            cpu_seconds=float(metrics.get("cpu_seconds", 0)),
            max_rss_bytes=int(metrics.get("max_rss_bytes", 0)),
            log=str(log.relative_to(root)),
        )
    finally:
        cleanup(raw, table_root, environment)


def measure(
    raw: dict[str, Any], candidate: Candidate, repetition: int, root: pathlib.Path,
    yt_token_file: pathlib.Path, environment: dict[str, str],
) -> dict[str, Any]:
    scans: list[ScanResult] = []
    elapsed = 0.0
    target = float(raw["run"].get("measurement_seconds", 120))
    while elapsed < target:
        result = run_scan(
            raw, candidate, repetition, len(scans) + 1, root, yt_token_file, environment
        )
        scans.append(result)
        elapsed += result.elapsed_seconds
    rows = int(raw["run"].get("rows", 50_000_000)) * len(scans)
    cpu = sum(item.cpu_seconds for item in scans)
    return {
        "candidate": candidate.name,
        "settings": candidate.settings,
        "repetition": repetition,
        "scans": len(scans),
        "rows": rows,
        "elapsed_seconds": elapsed,
        "rows_per_second": rows / elapsed,
        "cpu_seconds": cpu,
        "average_cpu_percent": 100 * cpu / elapsed if elapsed else 0,
        "rows_per_core_second": rows / cpu if cpu else 0,
        "max_rss_bytes": max(item.max_rss_bytes for item in scans),
        "scan_results": [dataclasses.asdict(item) for item in scans],
    }


def aggregate(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in results:
        grouped.setdefault(item["candidate"], []).append(item)
    summary = []
    for name, items in grouped.items():
        rates = [item["rows_per_second"] for item in items]
        summary.append({
            "candidate": name,
            "settings": items[0]["settings"],
            "repetitions": len(items),
            "median_rows_per_second": statistics.median(rates),
            "min_rows_per_second": min(rates),
            "max_rows_per_second": max(rates),
            "mean_cpu_percent": statistics.mean(item["average_cpu_percent"] for item in items),
            "median_rows_per_core_second": statistics.median(item["rows_per_core_second"] for item in items),
            "max_rss_bytes": max(item["max_rss_bytes"] for item in items),
        })
    return sorted(summary, key=lambda item: item["median_rows_per_second"], reverse=True)


def write_report(root: pathlib.Path, results: list[dict[str, Any]]) -> None:
    summary = aggregate(results)
    (root / "report.json").write_text(
        json.dumps({"summary": summary, "runs": results}, indent=2) + "\n", encoding="utf-8"
    )
    lines = [
        "# YTsaurus static-table write throughput",
        "",
        "Production generator → Arrow pipeline → YTsaurus runs; every scan completed before timing was accepted.",
        "",
        "| Candidate | Median rows/s | Min | Max | CPU | Rows/core-s | Peak RSS |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary:
        lines.append(
            f"| {item['candidate']} | {item['median_rows_per_second']:,.0f} | "
            f"{item['min_rows_per_second']:,.0f} | {item['max_rows_per_second']:,.0f} | "
            f"{item['mean_cpu_percent']:.0f}% | {item['median_rows_per_core_second']:,.0f} | "
            f"{item['max_rss_bytes'] / (1 << 30):.2f} GiB |"
        )
    (root / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", action="append")
    args = parser.parse_args()
    raw = yaml.safe_load(args.config.read_text())
    candidates = [Candidate(str(item["name"]), dict(item.get("settings", {}))) for item in raw["candidates"]]
    if args.candidate:
        names = set(args.candidate)
        candidates = [candidate for candidate in candidates if candidate.name in names]
    if not candidates:
        raise SystemExit("no candidates selected")

    yt_token_file = secret_file(required(raw["ytsaurus"], "token", "ytsaurus"), "ytsaurus.token")
    mdb_token = read_secret(required(raw["internal_resolver"], "oauth_token", "internal_resolver"), "internal_resolver.oauth_token")
    environment = os.environ.copy()
    environment["YT_PROXY"] = str(required(raw["ytsaurus"], "cluster", "ytsaurus"))
    environment["YT_TOKEN"] = yt_token_file.read_text().strip()
    environment["YT_SECURE_VAULT_robot_mdb_token"] = mdb_token

    result_root = pathlib.Path(raw["run"].get("result_root", "results"))
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = result_root / run_id
    root.mkdir(parents=True, exist_ok=False)
    binary = configured_path(required(raw["tools"], "rust_binary", "tools"))
    (root / "provenance.json").write_text(
        json.dumps(
            {
                "binary": str(binary),
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "config_sha256": hashlib.sha256(args.config.read_bytes()).hexdigest(),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    repetitions = int(raw["run"].get("repetitions", 5))
    schedule = [(candidate, repetition) for repetition in range(1, repetitions + 1) for candidate in candidates]
    random.Random(int(raw["run"].get("shuffle_seed", 20260829))).shuffle(schedule)
    results: list[dict[str, Any]] = []
    for index, (candidate, repetition) in enumerate(schedule, 1):
        print(f"[{index}/{len(schedule)}] {candidate.name} repetition {repetition}", flush=True)
        results.append(measure(raw, candidate, repetition, root, yt_token_file, environment))
        write_report(root, results)
    print(root / "report.md")


if __name__ == "__main__":
    main()
