#!/usr/bin/env python3
"""Run the bounded database source/sink Speedtest tournament on a benchmark host."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


MIB = 1024 * 1024


def postgres_config(batch_rows: int, copy_to: str, copy_from: str) -> dict[str, Any]:
    connection = {
        "database": "bench",
        "username": "bench",
        "password": os.environ["TRANSFERIA_BENCH_PASSWORD"],
        "installation": {
            "type": "on_premise",
            "host": "127.0.0.1",
            "port": 15432,
            "trusted_plaintext": True,
            "tls_ca_file": None,
        },
    }
    return delivery(
        {
            "postgres": {
                **connection,
                "tables": [{"schema": "public", "name": "perf_rows"}],
                "batch_rows": batch_rows,
                "copy_to_format": copy_to,
                "replication": None,
            }
        },
        {
            "postgres": {
                **connection,
                "create_tables": True,
                "copy_from_format": copy_from,
            }
        },
    )


def mysql_config(
    batch_rows: int, insert_rows: int, read_protocol: str
) -> dict[str, Any]:
    connection = {
        "database": "bench",
        "username": "bench",
        "password": os.environ["TRANSFERIA_BENCH_PASSWORD"],
        "installation": {
            "type": "on_premise",
            "host": "127.0.0.1",
            "port": 13306,
            "trusted_plaintext": True,
            "tls_ca_file": None,
        },
    }
    return delivery(
        {
            "mysql": {
                **connection,
                "tables": [{"name": "perf_rows"}],
                "batch_rows": batch_rows,
                "read_protocol": read_protocol,
            }
        },
        {
            "mysql": {
                **connection,
                "create_tables": True,
                "insert_rows": insert_rows,
            }
        },
    )


def opensearch_connection() -> dict[str, Any]:
    return {
        "installation": {
            "type": "on_premise",
            "hosts": ["127.0.0.1"],
            "port": 19200,
            "trusted_plaintext": True,
            "tls_ca_file": None,
        },
        "auth": {"type": "anonymous"},
        "request_timeout_ms": 30_000,
        "max_response_bytes": 128 * MIB,
    }


def opensearch_source_config(page_rows: int, concurrency: int) -> dict[str, Any]:
    return delivery(
        {
            "opensearch": {
                **opensearch_connection(),
                "indices": [{"name": "perf_rows"}],
                "page_rows": page_rows,
                "read_concurrency": concurrency,
                "pit_keep_alive_ms": 300_000,
                "retry_initial_ms": 100,
                "retry_max_ms": 10_000,
                "retry_max_attempts": 10,
            }
        },
        {"discard": {}},
    )


def opensearch_sink_config(
    bulk_rows: int, bulk_bytes: int, concurrency: int
) -> dict[str, Any]:
    pg_connection = {
        "database": "bench",
        "username": "bench",
        "password": os.environ["TRANSFERIA_BENCH_PASSWORD"],
        "installation": {
            "type": "on_premise",
            "host": "127.0.0.1",
            "port": 15432,
            "trusted_plaintext": True,
            "tls_ca_file": None,
        },
    }
    return delivery(
        {
            "postgres": {
                **pg_connection,
                "tables": [{"schema": "public", "name": "perf_rows"}],
                "batch_rows": 65_536,
                "copy_to_format": "binary",
                "replication": None,
            }
        },
        {
            "opensearch": {
                **opensearch_connection(),
                "create_indices": True,
                "routed_identity": "fail",
                "bulk_target_rows": bulk_rows,
                "bulk_target_bytes": bulk_bytes,
                "bulk_concurrency": concurrency,
                "flush_interval_ms": 250,
                "retry_initial_ms": 100,
                "retry_max_ms": 10_000,
                "retry_max_attempts": 10,
            }
        },
    )


def delivery(source: dict[str, Any], sink: dict[str, Any]) -> dict[str, Any]:
    return {
        "delivery_type": None,
        "source": source,
        "sink": sink,
        "middlewares": [],
        "pipeline_memory_limit_bytes": 512 * MIB,
        "metrics": None,
    }


def candidates(group: str) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    result: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    if group in {"all", "postgres"}:
        for batch_rows in (16_384, 65_536, 262_144):
            for copy_to in ("binary", "text"):
                for copy_from in ("binary", "text"):
                    params = {
                        "batch_rows": batch_rows,
                        "copy_to_format": copy_to,
                        "copy_from_format": copy_from,
                    }
                    result.append(
                        (
                            "postgres",
                            params,
                            postgres_config(batch_rows, copy_to, copy_from),
                        )
                    )
    if group in {"all", "mysql"}:
        for batch_rows in (16_384, 65_536, 262_144):
            for read_protocol in ("text", "binary"):
                for insert_rows in (1_000, 4_000, 12_000, 16_000):
                    params = {
                        "batch_rows": batch_rows,
                        "insert_rows": insert_rows,
                        "read_protocol": read_protocol,
                    }
                    result.append(
                        (
                            "mysql",
                            params,
                            mysql_config(batch_rows, insert_rows, read_protocol),
                        )
                    )
    if group in {"all", "opensearch_source"}:
        for page_rows in (2_500, 5_000, 10_000):
            for concurrency in (1, 2, 4, 8):
                params = {"page_rows": page_rows, "read_concurrency": concurrency}
                result.append(
                    (
                        "opensearch_source",
                        params,
                        opensearch_source_config(page_rows, concurrency),
                    )
                )
    if group in {"all", "opensearch_sink"}:
        for bulk_rows in (2_500, 10_000, 20_000):
            for concurrency in (1, 2, 4, 8):
                params = {
                    "bulk_target_rows": bulk_rows,
                    "bulk_target_bytes": 16 * MIB,
                    "bulk_concurrency": concurrency,
                }
                result.append(
                    (
                        "opensearch_sink",
                        params,
                        opensearch_sink_config(bulk_rows, 16 * MIB, concurrency),
                    )
                )
        for bulk_bytes in (4 * MIB, 64 * MIB):
            params = {
                "bulk_target_rows": 10_000,
                "bulk_target_bytes": bulk_bytes,
                "bulk_concurrency": 4,
            }
            result.append(
                (
                    "opensearch_sink",
                    params,
                    opensearch_sink_config(10_000, bulk_bytes, 4),
                )
            )
    return result


def estimate(base_url: str, config: dict[str, Any], duration: int) -> dict[str, Any]:
    body = json.dumps(
        {
            "config": config,
            "duration_seconds": duration,
            "cleanup_timeout_seconds": 60,
        }
    ).encode()
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/v1/speedtest/estimate",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=duration * 4 + 120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        payload = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {error.code}: {payload}") from error


def write_results(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "group",
        choices=(
            "all",
            "postgres",
            "mysql",
            "opensearch_source",
            "opensearch_sink",
        ),
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--duration-seconds", type=int, default=5)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.duration_seconds <= 0 or args.repetitions <= 0:
        parser.error("duration and repetitions must be positive")

    selected = candidates(args.group)[args.start :]
    if args.limit is not None:
        selected = selected[: args.limit]
    rows: list[dict[str, Any]] = []
    for connector, params, config in selected:
        for repetition in range(1, args.repetitions + 1):
            started = time.monotonic()
            record: dict[str, Any] = {
                "connector": connector,
                "parameters": params,
                "repetition": repetition,
            }
            try:
                record["result"] = estimate(
                    args.base_url, config, args.duration_seconds
                )
                record["status"] = "ok"
            except Exception as error:  # the durable output records every failed arm
                record["status"] = "error"
                record["error"] = str(error)
            record["wall_seconds"] = time.monotonic() - started
            rows.append(record)
            write_results(args.output, rows)
            source = record.get("result", {}).get("source", {}).get(
                "rows_per_second", 0.0
            )
            sink = record.get("result", {}).get("destination", {}).get(
                "rows_per_second", 0.0
            )
            print(
                f"{connector} rep={repetition} params={params} "
                f"status={record['status']} source={source:.0f} sink={sink:.0f}",
                flush=True,
            )


if __name__ == "__main__":
    main()
