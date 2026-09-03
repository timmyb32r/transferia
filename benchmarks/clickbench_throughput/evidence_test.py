#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import statistics
import unittest


ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results" / "2026-09-03"
SUMMARY = RESULTS / "exact-prefix-summary.json"
RUNS = RESULTS / "exact-prefix-runs.json"


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load(path: Path) -> object:
    return json.loads(path.read_text(), object_pairs_hook=_unique_object)


def _arms(result: dict[str, object]):
    for name in ("baseline", "candidate", "selected"):
        value = result.get(name)
        if isinstance(value, dict):
            yield value
    rejected = result.get("rejected")
    if isinstance(rejected, dict):
        yield rejected
    elif isinstance(rejected, list):
        yield from (value for value in rejected if isinstance(value, dict))


class ClickBenchEvidenceTest(unittest.TestCase):
    def test_json_has_unique_keys_and_all_connector_roles(self) -> None:
        summary = _load(SUMMARY)
        _load(RUNS)
        self.assertEqual(summary["schema_version"], 2)
        self.assertEqual(
            {(item["connector"], item["role"]) for item in summary["results"]},
            {
                ("ytsaurus", "source"),
                ("ytsaurus", "destination"),
                ("clickhouse", "source"),
                ("clickhouse", "destination"),
                ("iceberg", "source"),
                ("iceberg", "destination"),
                ("postgres", "source"),
                ("postgres", "destination"),
                ("mysql", "source"),
                ("mysql", "destination"),
                ("opensearch", "source"),
                ("opensearch", "destination"),
            },
        )

    def test_embedded_run_means_and_parallel_arrays_are_consistent(self) -> None:
        summary = _load(SUMMARY)
        for result in summary["results"]:
            for arm in _arms(result):
                rates = arm.get("rows_per_second_runs")
                if rates is None:
                    continue
                self.assertAlmostEqual(
                    arm["rows_per_second_mean"],
                    statistics.fmean(rates),
                    delta=1.0,
                    msg=f"{result['connector']} {result['role']} {arm['configuration']}",
                )
                for field in (
                    "duration_ms_runs",
                    "wall_seconds_runs",
                    "rows_runs",
                    "persisted_rows_runs",
                    "process_cpu_cores_runs",
                    "peak_rss_bytes_runs",
                ):
                    values = arm.get(field)
                    if values is not None:
                        self.assertEqual(
                            len(values),
                            len(rates),
                            msg=f"{result['connector']} {result['role']} {field}",
                        )

    def test_later_series_keep_recalculable_per_run_evidence(self) -> None:
        summary = _load(SUMMARY)
        for result in summary["results"]:
            if result["connector"] not in {"ytsaurus", "iceberg"} and not (
                result["role"] == "source"
                and result["connector"] in {"postgres", "mysql"}
            ):
                continue
            for arm in _arms(result):
                if "rows_per_second_runs" not in arm:
                    continue
                timing = arm.get("duration_ms_runs") or arm.get("wall_seconds_runs")
                row_counts = arm.get("rows_runs") or arm.get("persisted_rows_runs")
                self.assertIsNotNone(timing)
                self.assertIsNotNone(row_counts)
                self.assertIn("process_cpu_cores_runs", arm)
                self.assertIn("peak_rss_bytes_runs", arm)

    def test_public_evidence_contains_no_private_benchmark_identifiers(self) -> None:
        public = "\n".join(
            path.read_text()
            for path in (ROOT / "REPORT.md", ROOT / "PROVENANCE.md", SUMMARY, RUNS)
        ).lower()
        for forbidden in (
            "/home/",
            "tm-10373",
            "clickbench-item7-",
            "hume",
            "timmyb32r",
            "password",
            "authorization:",
        ):
            self.assertNotIn(forbidden, public)

    def test_role_efficiency_is_not_derived_from_request_wide_cpu(self) -> None:
        report = (ROOT / "REPORT.md").read_text().lower()
        self.assertNotIn("rows/core-s", report)
        for path in (SUMMARY, RESULTS / "summary.json"):
            document = _load(path)
            self.assertIn("process_resource_measurement_scope", document)
            self.assertNotIn("rows_per_core_second", path.read_text())


if __name__ == "__main__":
    unittest.main()
