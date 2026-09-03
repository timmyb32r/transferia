#!/usr/bin/env python3
"""Run reproducible fixed-window generator-to-sink throughput measurements."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import random
import re
import signal
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from scripts.murmur3_x64_128 import murmur3_x64_128_file


STATS = re.compile(
    r"\[stats(?: p=(?P<partition>-?[0-9]+))?\].*?"
    r"source: (?P<source>[0-9]+) records/s.*?"
    r"sink: (?P<sink>[0-9]+) rows/s.*?"
    r"(?P<retries>[0-9]+) retries.*?cpu: (?P<cpu>[0-9]+)% "
    r"rss: (?P<rss>[0-9.]+) (?P<rss_unit>[GM]iB)"
)
FAILURE_MARKERS = (
    "pipeline failed, restarting",
    "partition task failed",
    "non-retryable partition failure",
    "panicked at",
    "failed, retrying",
)
CONFIG_PLACEHOLDER = re.compile(
    r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}"
)


@dataclass(frozen=True)
class Candidate:
    name: str
    settings: dict[str, Any]


def expand_path(value: Any, base: pathlib.Path) -> pathlib.Path:
    path = pathlib.Path(os.path.expanduser(str(value)))
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def required(mapping: dict[str, Any], key: str, owner: str) -> Any:
    value = mapping.get(key)
    if value is None or value == "":
        raise ValueError(f"{owner}.{key} is required")
    return value


def private_write(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def private_binary_output(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    return os.fdopen(descriptor, "wb")


def environment(raw: dict[str, Any], base: pathlib.Path) -> dict[str, str]:
    result = os.environ.copy()
    for name, spec in raw.get("environment", {}).items():
        if not isinstance(spec, dict) or set(spec) != {"file"}:
            raise ValueError(f"environment.{name} must contain only 'file'")
        value = expand_path(spec["file"], base).read_text().strip()
        if not value:
            raise ValueError(f"environment.{name} points to an empty file")
        result[str(name)] = value
    return result


def render_config_template(template: str, values: dict[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group("name")
        if name in values:
            return values[name]
        default = match.group("default")
        if default is not None:
            return default
        raise ValueError(f"delivery template requires environment variable {name}")

    rendered = CONFIG_PLACEHOLDER.sub(replace, template)
    if CONFIG_PLACEHOLDER.search(rendered):
        raise ValueError("delivery template contains an unresolved environment placeholder")
    return rendered


def rss_bytes(value: str, unit: str) -> float:
    scale = 1024**3 if unit == "GiB" else 1024**2
    return float(value) * scale


def parse_window(log: pathlib.Path, sample_count: int) -> dict[str, Any]:
    samples = []
    for line in log.read_text(errors="replace").splitlines():
        lowered = line.lower()
        if any(marker in lowered for marker in FAILURE_MARKERS):
            raise RuntimeError(
                "pipeline failure occurred during the measurement; inspect the private raw log"
            )
        match = STATS.search(line)
        if match:
            samples.append(match.groupdict())
        elif "[stats" in line:
            raise RuntimeError("measurement contains an unrecognized [stats] line")
    if len(samples) < sample_count:
        raise RuntimeError(
            f"measurement produced {len(samples)} stats samples, expected at least "
            f"{sample_count}"
        )
    samples = samples[-sample_count:]
    partitions = {sample["partition"] for sample in samples}
    if partitions not in ({None}, {"0"}):
        raise RuntimeError(
            "single-worker sink benchmark requires aggregate stats or partition zero only"
        )
    source = [float(sample["source"]) for sample in samples]
    sink = [float(sample["sink"]) for sample in samples]
    cpu = [float(sample["cpu"]) for sample in samples]
    return {
        "samples": len(samples),
        "source_rows_s_mean": statistics.fmean(source),
        "source_rows_s_min": min(source),
        "source_rows_s_max": max(source),
        "sink_rows_s_mean": statistics.fmean(sink),
        "sink_rows_s_min": min(sink),
        "sink_rows_s_max": max(sink),
        "sink_to_source_percent": statistics.fmean(sink)
        / statistics.fmean(source)
        * 100,
        "cpu_percent_mean": statistics.fmean(cpu),
        "rss_bytes_peak": max(
            rss_bytes(sample["rss"], sample["rss_unit"]) for sample in samples
        ),
        "retries": sum(int(sample["retries"]) for sample in samples),
    }


def delivery(
    template: dict[str, Any],
    candidate: Candidate,
    sink_key: str,
    table_name: str,
    rows: int,
    preset: str,
    state_path: pathlib.Path,
) -> dict[str, Any]:
    document = yaml.safe_load(yaml.safe_dump(template))
    document["delivery_id"] = f"sink-window-{uuid.uuid4()}"
    document["durable_storage"] = {
        "type": "local_file",
        "path": str(state_path),
    }
    document["delivery_type"] = "batch"
    document["source"] = {
        "data_generator": {
            "table_name": table_name,
            "preset": {"type": preset},
            "amount": {"type": "rows", "row_count": rows},
        }
    }
    document["metrics"] = {"interval_ms": 1_000}
    sink = document.get("sink", {}).get(sink_key)
    if not isinstance(sink, dict):
        raise ValueError(f"delivery template must contain sink.{sink_key}")
    sink.update(candidate.settings)
    return document


def stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGINT)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def run_once(
    *,
    binary: pathlib.Path,
    template: dict[str, Any],
    candidate: Candidate,
    sink_key: str,
    rows: int,
    warmup: int,
    duration: int,
    root: pathlib.Path,
    repetition: int,
    process_environment: dict[str, str],
    preset: str,
) -> dict[str, Any]:
    run_name = f"{candidate.name}-r{repetition}"
    work = root / "work" / run_name
    config = work / "delivery.yaml"
    log = root / "raw" / f"{run_name}.log"
    table_name = re.sub(
        r"[^A-Za-z0-9_]",
        "_",
        f"sink_window_{root.name}_{candidate.name}_r{repetition}",
    )
    private_write(
        config,
        yaml.safe_dump(
            delivery(
                template,
                candidate,
                sink_key,
                table_name,
                rows,
                preset,
                work / "state",
            ),
            sort_keys=False,
        ),
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with private_binary_output(log) as output:
        process = subprocess.Popen(
            [
                str(binary),
                "--config",
                str(config),
                "--total-workers",
                "1",
                "--worker-index",
                "0",
            ],
            env=process_environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            deadline = started + warmup + duration + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"{run_name} exited before the measurement window completed"
                    )
                if time.monotonic() - started >= warmup + duration:
                    break
                time.sleep(0.5)
        finally:
            stop_process(process)
    result = parse_window(log, duration)
    result.update(
        {
            "candidate": candidate.name,
            "repetition": repetition,
            "duration_seconds": duration,
            "warmup_seconds": warmup,
            "table_name": table_name,
            "generated_config": str(config.relative_to(root)),
            "raw_log": str(log.relative_to(root)),
        }
    )
    private_write(
        root / "results" / f"{run_name}.json",
        json.dumps(result, indent=2, sort_keys=True) + "\n",
    )
    return result


def write_report(root: pathlib.Path, results: list[dict[str, Any]]) -> None:
    groups: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        groups.setdefault(str(result["candidate"]), []).append(result)
    rows = []
    for name, runs in groups.items():
        sink = [float(run["sink_rows_s_mean"]) for run in runs]
        rows.append(
            (
                statistics.fmean(sink),
                name,
                len(runs),
                min(sink),
                max(sink),
                statistics.fmean(float(run["cpu_percent_mean"]) for run in runs),
                max(float(run["rss_bytes_peak"]) for run in runs),
                statistics.fmean(
                    float(run["sink_to_source_percent"]) for run in runs
                ),
                sum(int(run["retries"]) for run in runs),
            )
        )
    rows.sort(reverse=True)
    lines = [
        "# Windowed sink throughput",
        "",
        "| Candidate | n | Sink rows/s | Min–max | CPU | Peak RSS | Sink/source | Retries |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for mean, name, count, minimum, maximum, cpu, rss, retention, retries in rows:
        lines.append(
            f"| `{name}` | {count} | {mean:,.0f} | {minimum:,.0f}–{maximum:,.0f} | "
            f"{cpu:.0f}% | {rss / 1024**3:.2f} GiB | {retention:.1f}% | {retries} |"
        )
    private_write(root / "REPORT.md", "\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=pathlib.Path)
    args = parser.parse_args()
    source = args.config.resolve()
    raw = yaml.safe_load(source.read_text())
    base = source.parent
    run = raw["run"]
    tools = raw["tools"]
    process_environment = environment(raw, base)
    binary = expand_path(required(tools, "rust_binary", "tools"), base)
    template_path = expand_path(
        required(run, "delivery_template_file", "run"), base
    )
    template = yaml.safe_load(
        render_config_template(template_path.read_text(), process_environment)
    )
    candidates = [
        Candidate(str(item["name"]), dict(item.get("settings", {})))
        for item in run["candidates"]
    ]
    if not candidates:
        raise ValueError("run.candidates must not be empty")
    warmup = int(run.get("warmup_seconds", 30))
    duration = int(run.get("duration_seconds", 120))
    repetitions = int(run.get("repetitions", 2))
    preset = str(run.get("preset", "transfer_logs"))
    if preset not in {"clickbench", "numeric", "transfer_logs"}:
        raise ValueError(
            "run.preset must be one of clickbench, numeric, or transfer_logs"
        )
    if min(warmup, duration, repetitions) <= 0:
        raise ValueError("warmup, duration, and repetitions must be positive")
    run_id = str(
        run.get(
            "id",
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        )
    )
    root = expand_path(run.get("results_dir", "./results"), base) / run_id
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    root.chmod(0o700)
    provenance = {
        "fingerprint_algorithm": "murmur3_x64_128",
        "binary_murmur3_x64_128": murmur3_x64_128_file(binary),
        "benchmark_config_murmur3_x64_128": murmur3_x64_128_file(source),
        "delivery_template_murmur3_x64_128": murmur3_x64_128_file(template_path),
        "warmup_seconds": warmup,
        "duration_seconds": duration,
        "repetitions": repetitions,
        "preset": preset,
    }
    private_write(
        root / "provenance.json",
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
    )
    schedule = [
        (candidate, repetition)
        for repetition in range(1, repetitions + 1)
        for candidate in candidates
    ]
    random.Random(int(run.get("seed", 20260829))).shuffle(schedule)
    results = []
    for candidate, repetition in schedule:
        results.append(
            run_once(
                binary=binary,
                template=template,
                candidate=candidate,
                sink_key=str(required(run, "sink_key", "run")),
                rows=int(run.get("rows", 2_000_000_000)),
                warmup=warmup,
                duration=duration,
                root=root,
                repetition=repetition,
                process_environment=process_environment,
                preset=preset,
            )
        )
        write_report(root, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
