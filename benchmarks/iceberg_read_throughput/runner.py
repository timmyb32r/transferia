#!/usr/bin/env python3
"""Benchmark production Iceberg snapshot reads against the discard sink.

Each measurement window consists of complete, sequential scans of one immutable
snapshot.  Process startup and catalog planning are intentionally included: the
result is end-to-end source throughput, not a Parquet decoder microbenchmark.
"""

from __future__ import annotations

import argparse
import dataclasses
import fcntl
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
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml


@dataclasses.dataclass(frozen=True)
class Candidate:
    name: str
    settings: dict[str, int]


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


def read_secret(spec: Any, path: str) -> str:
    if not isinstance(spec, dict) or set(spec) != {"file"}:
        raise ValueError(f"{path} must be an object containing only 'file'")
    value = pathlib.Path(os.path.expanduser(str(spec["file"]))).read_text().strip()
    if not value:
        raise ValueError(f"{path} points to an empty file")
    return value


def private_write(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def catalog_get(uri: str, path: str) -> dict[str, Any]:
    with urllib.request.urlopen(uri.rstrip("/") + path, timeout=10) as response:
        return json.load(response)


def snapshot_rows(metadata: dict[str, Any]) -> int:
    snapshot_id = metadata.get("current-snapshot-id")
    snapshots = {item.get("snapshot-id"): item for item in metadata.get("snapshots", [])}
    count = 0
    visited: set[int] = set()
    while snapshot_id is not None:
        if snapshot_id in visited:
            raise ValueError("Iceberg snapshot ancestry is cyclic")
        visited.add(snapshot_id)
        snapshot = snapshots.get(snapshot_id)
        if snapshot is None:
            raise ValueError(f"snapshot {snapshot_id} is absent from table metadata")
        summary = snapshot.get("summary", {})
        if summary.get("operation") in {"overwrite", "replace"}:
            count += int(required(summary, "total-records", "snapshot.summary"))
            break
        count += int(summary.get("added-records", 0))
        count -= int(summary.get("deleted-records", 0))
        snapshot_id = snapshot.get("parent-snapshot-id")
    return count


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
    raw: dict[str, Any], candidate: Candidate, secret: str, state: pathlib.Path
) -> dict[str, Any]:
    iceberg = raw["iceberg"]
    s3 = iceberg["s3"]
    source = {
        "catalog": {
            "uri": required(iceberg, "catalog_uri", "iceberg"),
            "request_timeout_ms": 30_000,
            "warehouse": None,
            "auth": {"type": "none"},
        },
        "installation": {
            "type": "on_premise",
            "storage": {
                "type": "s3",
                "bucket": required(s3, "bucket", "iceberg.s3"),
                "request_timeout_ms": 300_000,
                "retry_max_times": 100,
                "retry_initial_delay_ms": 100,
                "retry_max_delay_ms": 5_000,
                "region": required(s3, "region", "iceberg.s3"),
                "endpoint": required(s3, "endpoint", "iceberg.s3"),
                "credentials": {
                    "access_key": required(s3, "access_key_id", "iceberg.s3"),
                    "secret_key": secret,
                },
                "session_token": None,
                "path_style_access": False,
                "allow_anonymous": False,
            },
        },
        "namespace": required(iceberg, "namespace", "iceberg"),
        "table_names": [required(iceberg, "table", "iceberg")],
    }
    source.update(candidate.settings)
    return {
        "delivery_id": f"iceberg-read-{uuid.uuid4()}",
        "durable_storage": {"type": "local_file", "path": str(state)},
        "delivery_type": "batch",
        "source": {"iceberg": source},
        "sink": {"discard": {}},
        "middlewares": [],
        "pipeline_memory_limit_bytes": int(raw["run"].get("pipeline_memory_limit_bytes", 4 << 30)),
        "metrics": {"interval_ms": 1000},
    }


def run_scan(
    raw: dict[str, Any], candidate: Candidate, repetition: int, secret: str,
    root: pathlib.Path, scan: int,
) -> ScanResult:
    run = pathlib.Path(candidate.name) / f"repetition-{repetition}" / f"scan-{scan:03d}"
    work = root / "work" / run
    config = work / "delivery.yaml"
    log = root / "raw" / run.with_suffix(".log")
    state = work / "state"
    private_write(config, yaml.safe_dump(delivery_config(raw, candidate, secret, state), sort_keys=False))
    log.parent.mkdir(parents=True, exist_ok=True)
    binary = pathlib.Path(os.path.expanduser(str(required(raw["tools"], "rust_binary", "tools"))))
    started = time.monotonic()
    metrics: dict[str, float] = {}
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            [str(binary), "--config", str(config), "--total-workers", "1", "--worker-index", "0"],
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        stop = threading.Event()
        monitor = threading.Thread(target=monitor_process, args=(process.pid, stop, metrics), daemon=True)
        monitor.start()
        try:
            exit_code = process.wait(timeout=float(raw["run"].get("scan_timeout_seconds", 300)))
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise RuntimeError(f"{candidate.name} scan exceeded timeout")
        finally:
            stop.set()
            monitor.join(timeout=2)
    elapsed = time.monotonic() - started
    if exit_code != 0:
        tail = "\n".join(log.read_text(errors="replace").splitlines()[-30:])
        raise RuntimeError(f"{candidate.name} scan failed with exit code {exit_code}:\n{tail}")
    return ScanResult(
        elapsed_seconds=elapsed,
        cpu_seconds=float(metrics.get("cpu_seconds", 0)),
        max_rss_bytes=int(metrics.get("max_rss_bytes", 0)),
        log=str(log.relative_to(root)),
    )


def measure(
    raw: dict[str, Any], candidate: Candidate, repetition: int, rows: int, secret: str,
    root: pathlib.Path,
) -> dict[str, Any]:
    target = float(raw["run"].get("measurement_seconds", 120))
    scans: list[ScanResult] = []
    elapsed = 0.0
    while elapsed < target:
        result = run_scan(raw, candidate, repetition, secret, root, len(scans) + 1)
        scans.append(result)
        elapsed += result.elapsed_seconds
    cpu = sum(item.cpu_seconds for item in scans)
    total_rows = rows * len(scans)
    return {
        "candidate": candidate.name,
        "settings": candidate.settings,
        "repetition": repetition,
        "scans": len(scans),
        "rows": total_rows,
        "elapsed_seconds": elapsed,
        "rows_per_second": total_rows / elapsed,
        "cpu_seconds": cpu,
        "average_cpu_percent": 100 * cpu / elapsed if elapsed else 0,
        "rows_per_core_second": total_rows / cpu if cpu else 0,
        "max_rss_bytes": max(item.max_rss_bytes for item in scans),
        "scan_results": [dataclasses.asdict(item) for item in scans],
    }


def aggregate(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in results:
        grouped.setdefault(item["candidate"], []).append(item)
    output = []
    for name, items in grouped.items():
        rates = [item["rows_per_second"] for item in items]
        output.append({
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
    return sorted(output, key=lambda item: item["median_rows_per_second"], reverse=True)


def write_report(root: pathlib.Path, provenance: dict[str, Any], results: list[dict[str, Any]]) -> None:
    summary = aggregate(results)
    payload = {"provenance": provenance, "runs": results, "summary": summary}
    (root / "results.json").write_text(json.dumps(payload, indent=2) + "\n")
    lines = [
        "# Iceberg source throughput benchmark",
        "",
        f"- Snapshot rows: {provenance['rows']:,}",
        f"- Measurement window: at least {provenance['measurement_seconds']:.0f} s per candidate/repetition",
        f"- Snapshot ID: `{provenance['snapshot_id']}`",
        f"- Binary SHA-256: `{provenance['binary_sha256']}`",
        "",
        "| Candidate | median rows/s | min | max | CPU | rows/core-s | peak RSS |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary:
        lines.append(
            f"| {item['candidate']} | {item['median_rows_per_second']:,.0f} | "
            f"{item['min_rows_per_second']:,.0f} | {item['max_rows_per_second']:,.0f} | "
            f"{item['mean_cpu_percent']:.0f}% | {item['median_rows_per_core_second']:,.0f} | "
            f"{item['max_rss_bytes'] / (1 << 30):.2f} GiB |"
        )
    (root / "report.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    raw = yaml.safe_load(args.config.read_text())
    for section in ("iceberg", "tools", "run"):
        if not isinstance(raw.get(section), dict):
            raise ValueError(f"{section} must be an object")
    candidates = [Candidate(str(item["name"]), dict(item["settings"])) for item in raw["candidates"]]
    names = [item.name for item in candidates]
    if len(names) != len(set(names)):
        raise ValueError("candidate names must be unique")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = (args.output or args.config.parent / "results" / run_id).resolve()
    root.mkdir(parents=True, exist_ok=False)
    lock_path = args.config.parent / ".runner.lock"
    lock = lock_path.open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    secret = read_secret(raw["iceberg"]["s3"]["secret"], "iceberg.s3.secret")
    namespace = urllib.parse.quote(str(required(raw["iceberg"], "namespace", "iceberg")), safe="")
    table = urllib.parse.quote(str(required(raw["iceberg"], "table", "iceberg")), safe="")
    response = catalog_get(str(required(raw["iceberg"], "catalog_uri", "iceberg")), f"/v1/namespaces/{namespace}/tables/{table}")
    metadata = response["metadata"]
    rows = snapshot_rows(metadata)
    binary = pathlib.Path(os.path.expanduser(str(required(raw["tools"], "rust_binary", "tools"))))
    binary_sha256 = subprocess.check_output(["sha256sum", str(binary)], text=True).split()[0]
    provenance = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "snapshot_id": metadata.get("current-snapshot-id"),
        "metadata_location": response.get("metadata-location"),
        "measurement_seconds": float(raw["run"].get("measurement_seconds", 120)),
        "binary": str(binary),
        "binary_sha256": binary_sha256,
        "host": os.uname().nodename,
        "cpu_count": os.cpu_count(),
    }
    results: list[dict[str, Any]] = []
    repetitions = int(raw["run"].get("repetitions", 2))
    for repetition in range(1, repetitions + 1):
        schedule = candidates.copy()
        random.Random(f"{run_id}-{repetition}").shuffle(schedule)
        for candidate in schedule:
            print(f"[{datetime.now(timezone.utc).isoformat()}] {candidate.name} repetition {repetition}", flush=True)
            item = measure(raw, candidate, repetition, rows, secret, root)
            results.append(item)
            write_report(root, provenance, results)
            print(f"{candidate.name}: {item['rows_per_second']:,.0f} rows/s", flush=True)
    print(root / "report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
