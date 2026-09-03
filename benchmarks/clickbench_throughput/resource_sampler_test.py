#!/usr/bin/env python3

import json
import io
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import resource_sampler as sampler


class ResourceSamplerTest(unittest.TestCase):
    def test_log_window_does_not_include_stats_appended_after_deadline(self) -> None:
        prefix = b"old\n"
        in_window = b"[stats] source: 1 records/s || sink: 2 rows/s\n"
        after_deadline = b"[stats] source: 9 records/s || sink: 99 rows/s\n"
        log = io.BytesIO(prefix + in_window + after_deadline)

        lines = sampler.read_log_window(
            log,
            len(prefix),
            len(prefix) + len(in_window),
        )

        self.assertEqual(lines, [in_window.decode().rstrip()])

    def test_proc_stat_handles_spaces_and_parentheses_in_process_name(self) -> None:
        fields_3_to_22 = ["S"] + ["0"] * 19
        fields_3_to_22[11] = "250"
        fields_3_to_22[12] = "75"
        fields_3_to_22[19] = "123456"

        cpu_seconds, start_time = sampler.parse_proc_stat(
            "42 (worker (clickbench)) " + " ".join(fields_3_to_22), 100
        )

        self.assertEqual(cpu_seconds, 3.25)
        self.assertEqual(start_time, 123456)

    def test_process_summary_uses_cpu_delta_and_time_weighted_rss(self) -> None:
        readings = [
            sampler.ProcessReading(0.0, 10.0, 100, 7),
            sampler.ProcessReading(1.0, 11.5, 300, 7),
            sampler.ProcessReading(3.0, 16.0, 500, 7),
        ]

        result = sampler.summarize_process(readings)

        self.assertEqual(result["elapsed_seconds"], 3.0)
        self.assertEqual(result["cpu_seconds"], 6.0)
        self.assertEqual(result["cpu_cores_average"], 2.0)
        self.assertAlmostEqual(result["rss_bytes_average"], 1000 / 3)
        self.assertEqual(result["rss_bytes_peak"], 500)

    def test_aggregate_stats_are_averaged_without_persisting_raw_lines(self) -> None:
        samples = sampler.parse_stats_lines(
            [
                "ignored startup line secret=must-not-appear",
                "[stats] source: 100 records/s || sink: 80 rows/s | rest",
                "[stats] source: 120 records/s || sink: 100 rows/s | rest",
            ],
            "sink",
        )

        result = sampler.summarize_throughput(samples)

        self.assertEqual(result["rows_per_second"], 90)
        self.assertEqual(result["stats_mode"], "aggregate")
        self.assertEqual(result["stats_samples"], 2)

    def test_per_partition_stats_sum_equally_sampled_partition_means(self) -> None:
        samples = sampler.parse_stats_lines(
            [
                "[stats p=0] source: 1 records/s || sink: 100 rows/s | rest",
                "[stats p=1] source: 1 records/s || sink: 200 rows/s | rest",
                "[stats p=0] source: 1 records/s || sink: 140 rows/s | rest",
                "[stats p=1] source: 1 records/s || sink: 240 rows/s | rest",
            ],
            "sink",
        )

        result = sampler.summarize_throughput(samples)

        self.assertEqual(result["rows_per_second"], 340)
        self.assertEqual(result["stats_mode"], "per_partition")
        self.assertEqual(result["partitions"], [0, 1])
        self.assertEqual(result["reporting_intervals"], 2)

    def test_mixed_or_unbalanced_stats_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "mixes aggregate"):
            sampler.summarize_throughput(
                [sampler.StatsSample(None, 10), sampler.StatsSample(0, 10)]
            )
        with self.assertRaisesRegex(ValueError, "sample counts differ"):
            sampler.summarize_throughput(
                [
                    sampler.StatsSample(0, 10),
                    sampler.StatsSample(0, 10),
                    sampler.StatsSample(1, 10),
                ]
            )

    def test_report_and_private_output_contain_no_log_or_process_secrets(self) -> None:
        report = sampler.build_report(
            pid=42,
            requested_duration_seconds=120,
            sample_interval_seconds=0.25,
            metric="sink",
            readings=[
                sampler.ProcessReading(0.0, 1.0, 100, 7),
                sampler.ProcessReading(120.0, 241.0, 200, 7),
            ],
            stats_samples=[sampler.StatsSample(None, 1234)],
        )

        encoded = json.dumps(report)
        self.assertNotIn("command", encoded)
        self.assertNotIn("environment", encoded)
        self.assertNotIn("stats_log", encoded)
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "result.json"
            sampler.private_atomic_write(output, report)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(output.read_text()), report)


if __name__ == "__main__":
    unittest.main()
