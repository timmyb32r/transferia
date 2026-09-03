#!/usr/bin/env python3
"""Create one deterministic OpenSearch throughput fixture without local files."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from typing import Any


def request(url: str, method: str, body: bytes | None = None) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if body is not None and url.endswith("/_bulk"):
        headers["Content-Type"] = "application/x-ndjson"
    command = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(command, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        payload = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenSearch HTTP {error.code}: {payload}") from error


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:19200")
    parser.add_argument("--index", default="perf_rows")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--primary-shards", type=int, default=4)
    parser.add_argument("--bulk-rows", type=int, default=5_000)
    args = parser.parse_args()
    if args.rows <= 0 or args.primary_shards <= 0 or args.bulk_rows <= 0:
        parser.error("rows, primary-shards, and bulk-rows must be positive")

    base_url = args.base_url.rstrip("/")
    request(
        f"{base_url}/{args.index}",
        "PUT",
        json.dumps(
            {
                "settings": {
                    "number_of_shards": args.primary_shards,
                    "number_of_replicas": 0,
                    "refresh_interval": "-1",
                },
                "mappings": {
                    "properties": {
                        "value1": {"type": "long"},
                        "value2": {"type": "double"},
                        "payload": {"type": "keyword"},
                    }
                },
            }
        ).encode(),
    )

    for start in range(0, args.rows, args.bulk_rows):
        stop = min(start + args.bulk_rows, args.rows)
        lines: list[str] = []
        for row in range(start, stop):
            lines.append(json.dumps({"index": {"_index": args.index, "_id": str(row)}}))
            lines.append(
                json.dumps(
                    {
                        "value1": row * 17,
                        "value2": row / 10.0,
                        "payload": f"payload-{row % 10_000:05}",
                    }
                )
            )
        response = request(
            f"{base_url}/_bulk", "POST", ("\n".join(lines) + "\n").encode()
        )
        if response.get("errors") is not False:
            raise RuntimeError(f"OpenSearch bulk failed for rows [{start}, {stop})")
        print(f"loaded {stop}/{args.rows}", flush=True)

    request(f"{base_url}/{args.index}/_refresh", "POST")
    count = request(f"{base_url}/{args.index}/_count", "GET").get("count")
    if count != args.rows:
        raise RuntimeError(f"OpenSearch count is {count}, expected {args.rows}")
    request(
        f"{base_url}/{args.index}/_settings",
        "PUT",
        json.dumps({"index.blocks.write": True}).encode(),
    )
    print(
        f"fixture ready: index={args.index} rows={count} primaries={args.primary_shards}",
        flush=True,
    )


if __name__ == "__main__":
    main()
