#!/usr/bin/env python3
"""Build a compact, deterministic distribution sketch for ClickBench hits.csv.

The input is intentionally sampled in equally spaced byte windows. This keeps
the analysis bounded for the 80+ GiB reference file while avoiding the strong
time/order bias of reading only its prefix. Invalid UTF-8 is preserved because
ClickHouse String is a byte string, not a Unicode contract.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone


COLUMNS = (
    "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime", "EventDate",
    "CounterID", "ClientIP", "RegionID", "UserID", "CounterClass", "OS",
    "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID",
    "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth",
    "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor",
    "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor",
    "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone",
    "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID",
    "SearchEngineID", "SearchPhrase", "AdvEngineID", "IsArtifical",
    "WindowClientWidth", "WindowClientHeight", "ClientTimeZone",
    "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2",
    "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion",
    "IsLink", "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID",
    "IsOldCounter", "IsEvent", "IsParameter", "DontCountHits", "WithHash",
    "HitColor", "LocalEventTime", "Age", "Sex", "Income", "Interests",
    "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength",
    "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction",
    "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming",
    "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
    "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID",
    "ParamCurrency", "ParamCurrencyID", "OpenstatServiceName",
    "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID", "UTMSource",
    "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm", "FromTag",
    "HasGCLID", "RefererHash", "URLHash", "CLID",
)

TEMPORAL = {
    "EventTime": "timestamp",
    "EventDate": "date",
    "ClientEventTime": "timestamp",
    "LocalEventTime": "timestamp",
}

STRINGS = {
    "Title", "URL", "Referer", "FlashMinor2", "UserAgentMinor",
    "MobilePhoneModel", "Params", "SearchPhrase", "PageCharset", "OriginalURL",
    "HitColor", "BrowserLanguage", "BrowserCountry", "SocialNetwork",
    "SocialAction", "SocialSourcePage", "ParamOrderID", "ParamCurrency",
    "OpenstatServiceName", "OpenstatCampaignID", "OpenstatAdID",
    "OpenstatSourceID", "UTMSource", "UTMMedium", "UTMCampaign", "UTMContent",
    "UTMTerm", "FromTag",
}


@dataclass
class Sketch:
    values: list[int] = field(default_factory=list)
    uniques: set[object] = field(default_factory=set)
    empty: int = 0


def quantiles(values: list[int]) -> list[int]:
    values.sort()
    if not values:
        return []
    result = []
    for numerator in range(101):
        index = round((len(values) - 1) * numerator / 100)
        result.append(values[index])
    return result


def parse_scalar(name: str, value: str) -> int:
    kind = TEMPORAL.get(name)
    if kind == "timestamp":
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return int(parsed.replace(tzinfo=timezone.utc).timestamp())
    if kind == "date":
        return (date.fromisoformat(value) - date(1970, 1, 1)).days
    return int(value)


def estimated_cardinality(distinct: int, rows: int, estimated_total_rows: int) -> int:
    if distinct == 0 or rows == 0:
        return 0
    ratio = distinct / rows
    expansion = estimated_total_rows / rows
    if ratio >= 0.90:
        return max(distinct, round(estimated_total_rows * ratio))
    if ratio >= 0.50:
        return max(distinct, round(distinct * expansion**0.90))
    if ratio >= 0.10:
        return max(distinct, round(distinct * expansion**0.65))
    if ratio >= 0.01:
        return max(distinct, round(distinct * expansion**0.35))
    return max(distinct, round(distinct * 1.05))


def sampled_rows(path: str, windows: int, window_bytes: int):
    size = os.path.getsize(path)
    with open(path, "rb", buffering=0) as source:
        for index in range(windows):
            offset = round(size * (index + 0.5) / windows)
            source.seek(offset)
            source.readline()
            data = source.read(window_bytes)
            data = data[: data.rfind(b"\n") + 1]
            reader = csv.reader(io.StringIO(data.decode("latin1"), newline=""))
            for row in reader:
                if len(row) == len(COLUMNS):
                    yield row


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--windows", type=int, default=128)
    parser.add_argument("--window-bytes", type=int, default=512 * 1024)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--reference-rows", type=int, default=99_997_497)
    parser.add_argument("--output")
    args = parser.parse_args()

    sketches = {name: Sketch() for name in COLUMNS}
    rows = 0
    sampled_bytes = 0
    for row in sampled_rows(args.path, args.windows, args.window_bytes):
        if rows == args.max_rows:
            break
        sampled_bytes += sum(len(value.encode("latin1")) for value in row) + len(row)
        for name, value in zip(COLUMNS, row, strict=True):
            sketch = sketches[name]
            if name in STRINGS:
                raw = value.encode("latin1")
                sketch.values.append(len(raw))
                sketch.uniques.add(raw)
                sketch.empty += not raw
            else:
                scalar = parse_scalar(name, value)
                sketch.values.append(scalar)
                sketch.uniques.add(scalar)
                sketch.empty += scalar == 0
        rows += 1

    if rows == 0:
        raise SystemExit("no complete 105-column ClickBench rows were sampled")
    mean_csv_row_bytes = sampled_bytes / rows
    estimated_total_rows = args.reference_rows
    fixed_widths = {
        "timestamp": 8,
        "date": 4,
        "integer": 8,
    }
    mean_arrow_row_bytes = 0.0
    for name in COLUMNS:
        if name in STRINGS:
            mean_arrow_row_bytes += sum(sketches[name].values) / rows + 4
        else:
            mean_arrow_row_bytes += fixed_widths[TEMPORAL.get(name, "integer")]
    output = {
        "source_bytes": os.path.getsize(args.path),
        "sample_rows": rows,
        "mean_csv_row_bytes": round(mean_csv_row_bytes, 3),
        "mean_arrow_row_bytes_upper_bound": math.ceil(mean_arrow_row_bytes),
        "estimated_total_rows": estimated_total_rows,
        "columns": [],
    }
    for name in COLUMNS:
        sketch = sketches[name]
        distinct = len(sketch.uniques)
        output["columns"].append(
            {
                "name": name,
                "kind": "binary" if name in STRINGS else TEMPORAL.get(name, "integer"),
                "zero_or_empty_ppm": round(sketch.empty * 1_000_000 / rows),
                "sample_distinct": distinct,
                "estimated_cardinality": estimated_cardinality(
                    distinct, rows, estimated_total_rows
                ),
                "mean": round(sum(sketch.values) / len(sketch.values), 3),
                "percentiles_0_to_100": quantiles(sketch.values),
                "nonzero_percentiles_0_to_100": quantiles(
                    [value for value in sketch.values if value != 0]
                ),
            }
        )
    rendered = json.dumps(output, indent=2, ensure_ascii=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="ascii") as destination:
            destination.write(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
