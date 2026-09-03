#!/usr/bin/env python3

import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("run_single_partition_benchmark.py")
SPEC = importlib.util.spec_from_file_location("single_partition_benchmark", SCRIPT)
BENCH = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(BENCH)


def comparison_document(**overrides):
    document = {
        "schema_version": 5,
        "config_murmur3_x64_128": "same-config",
        "binary_murmur3_x64_128": "binary",
        "rustc": "rustc 1.0",
        "platform": {"system": "test", "machine": "test"},
        "parameters": {
            "warmup_seconds": 30,
            "sample_seconds": 90,
            "repetitions": 5,
            "min_samples": 80,
        },
        "environment": {},
    }
    document.update(overrides)
    return document


class StatsParsingTest(unittest.TestCase):
    def test_benchmark_template_expansion_is_explicit_and_strict(self):
        template = "host: ${HOST:-localhost}\nport: ${PORT}\nempty: ${EMPTY:-fallback}\n"

        self.assertEqual(
            BENCH.render_config_template(
                template,
                {"PORT": "2135", "EMPTY": ""},
            ),
            "host: localhost\nport: 2135\nempty: fallback\n",
        )
        with self.assertRaisesRegex(ValueError, "PORT"):
            BENCH.render_config_template("port: ${PORT}", {})

    def test_parses_partition_zero_stats_line(self):
        line = (
            "[stats p=0] source: 1200 records/s | network-raw 1.5 MiB/s | "
            "network-decoded 2.0 GiB/s | response-wait 20% | network-decode 40% busy || "
            "parse: 1100 rows/s | 2.5 MiB/s arrow | "
            "3 dlq/s | 1000 source-msg/s | 60% busy || sink: 1000 rows/s | "
            "3.0 MiB/s | 2 flushes/s | 1000 source-msg/s | 70% busy | 1 retries | "
            "buffered 4 MiB | objects 1/2/3 | 10% backpressure || "
            "guarantee: at-least-once | cpu: 88% rss: 512 MiB"
        )

        sample = BENCH.parse_stats_line(line)

        self.assertEqual(sample["partition_id"], 0)
        self.assertEqual(sample["source_records_per_s"], 1200)
        self.assertEqual(sample["network_raw_bytes_per_s"], 1.5 * 1024 * 1024)
        self.assertEqual(sample["network_decoded_bytes_per_s"], 2 * 1024**3)
        self.assertEqual(sample["response_wait_percent"], 20)
        self.assertEqual(sample["parse_rows_per_s"], 1100)
        self.assertEqual(sample["sink_rows_per_s"], 1000)
        self.assertEqual(sample["sink_retries"], 1)
        self.assertEqual(sample["rss_bytes"], 512 * 1024 * 1024)

    def test_ignores_other_lines_and_exposes_discard_mode(self):
        self.assertIsNone(BENCH.parse_stats_line("unrelated log line"))
        line = (
            "[stats p=0] source: 42 records/s | network-raw 42 B/s | "
            "network-decoded 0 B/s | response-wait 1% | network-decode 0% busy || "
            "parse: benchmark-discard || "
            "sink: 0 rows/s | 0 B/s | 0 flushes/s | 0 source-msg/s | 0% busy | "
            "0 retries | buffered N/A | objects 0/0/0 | 0% backpressure || "
            "guarantee: destructive-benchmark | cpu: 5% rss: N/A"
        )
        sample = BENCH.parse_stats_line(line)
        self.assertIsNone(sample["parse_rows_per_s"])
        self.assertEqual(sample["rss_bytes"], 0)

    def test_rejects_removed_downloader_busy_field(self):
        line = (
            "[stats p=0] source: 42 records/s | network-raw 42 B/s | "
            "network-decoded 0 B/s | response-wait 1% | downloader 7% busy | "
            "network-decode 0% busy || parse: benchmark-discard || "
            "sink: 0 rows/s | 0 B/s | 0 flushes/s | 0 source-msg/s | 0% busy | "
            "0 retries | buffered N/A | objects 0/0/0 | 0% backpressure || "
            "guarantee: destructive-benchmark | cpu: 5% rss: N/A"
        )
        with self.assertRaisesRegex(ValueError, "unrecognized stats line"):
            BENCH.parse_stats_line(line)

    def test_rejects_sample_with_sink_retry(self):
        sample = {"sink_retries": 1}
        with self.assertRaisesRegex(RuntimeError, "sink retry"):
            BENCH.validate_sample(sample)

    def test_reproducibility_environment_never_records_secrets(self):
        environment = {
            "PQ_HOST": "broker",
            "PQ_PORT": "2135",
            "PQ_TOKEN": "super-secret",
            "CLICKHOUSE_PASSWORD": "also-secret",
            "S3_SECRET_KEY": "third-secret",
            "S3_ACCESS_KEY": "access-credential",
            "S3_BUCKET": "benchmark",
            "CLICKHOUSE_HTTP_ENDPOINT": "https://user:password@clickhouse:8443/api?token=query-secret",
        }
        result = BENCH.reproducibility_environment(environment)

        self.assertEqual(
            result,
            {
                "PQ_HOST": "broker",
                "PQ_PORT": "2135",
                "S3_BUCKET": "benchmark",
                "CLICKHOUSE_HTTP_ENDPOINT": "https://clickhouse:8443",
            },
        )
        self.assertNotIn("access-credential", json.dumps(result))
        self.assertNotIn("query-secret", json.dumps(result))


class SummaryTest(unittest.TestCase):
    def test_reports_robust_distribution(self):
        samples = [
            {"partition_id": 0, "source_records_per_s": value}
            for value in (10, 20, 30, 40, 1000)
        ]
        summary = BENCH.summarize_samples(samples)
        metric = summary["source_records_per_s"]
        self.assertEqual(metric["count"], 5)
        self.assertEqual(metric["median"], 30)
        self.assertEqual(metric["mad"], 10)
        self.assertEqual(metric["p10"], 14)
        self.assertAlmostEqual(metric["p90"], 616)

    def test_requires_repeatable_five_percent_regression(self):
        baseline = [100, 100, 100, 100, 100]
        noisy = [94, 94, 101, 101, 101]
        repeated = [94, 94, 94, 94, 101]

        self.assertFalse(BENCH.compare_primary_runs(noisy, baseline)["regression"])
        result = BENCH.compare_primary_runs(repeated, baseline)
        self.assertTrue(result["regression"])
        self.assertEqual(result["regressed_pairs"], 4)

    def test_comparison_rejects_a_different_config(self):
        baseline = comparison_document(config_murmur3_x64_128="baseline")
        current = comparison_document(config_murmur3_x64_128="candidate")

        with self.assertRaisesRegex(ValueError, "config_murmur3_x64_128"):
            BENCH.validate_comparison_context(current, baseline)

    def test_comparison_allows_distinct_consumer_prefixes_only(self):
        baseline = comparison_document(
            environment={
                "PQ_HOST": "broker",
                "PQ_PORT": "2135",
                "PQ_CONSUMER_JSON": "transferia-json-baseline",
            },
            binary_murmur3_x64_128="baseline-binary",
        )
        current = comparison_document(
            environment={
                "PQ_HOST": "broker",
                "PQ_PORT": "2135",
                "PQ_CONSUMER_JSON": "transferia-json-candidate",
            },
            binary_murmur3_x64_128="candidate-binary",
        )

        BENCH.validate_comparison_context(current, baseline)

        current["environment"]["PQ_HOST"] = "other-broker"
        with self.assertRaisesRegex(ValueError, "environment"):
            BENCH.validate_comparison_context(current, baseline)

    def test_murmur3_file_identifies_the_actual_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = pathlib.Path(directory) / "transferia"
            binary.write_bytes(b"candidate-binary")

            self.assertEqual(
                BENCH.murmur3_x64_128_file(binary),
                "3e1d1c42b473abe1c2793abe7f6d7963",
            )

    def test_run_namespace_uses_a_fresh_128_bit_random_identity(self):
        with mock.patch.object(
            BENCH.secrets,
            "token_hex",
            side_effect=["a" * 32, "b" * 32],
        ) as token_hex:
            first = BENCH.run_namespace(1)
            second = BENCH.run_namespace(1)

        self.assertEqual(first, "a" * 32 + "_01")
        self.assertEqual(second, "b" * 32 + "_01")
        self.assertNotEqual(first, second)
        self.assertEqual(token_hex.call_args_list, [mock.call(16), mock.call(16)])


class RunnerTest(unittest.TestCase):
    def test_run_once_captures_only_the_sample_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "fake-transferia.py"
            binary.write_text(
                "#!/usr/bin/env python3\n"
                "import os, signal, time\n"
                "def stop(*_):\n"
                "    raise SystemExit(0)\n"
                "signal.signal(signal.SIGTERM, stop)\n"
                "print('rep=' + os.environ['BENCHMARK_REPETITION'], flush=True)\n"
                "print('destination=' + os.environ['BENCHMARK_RUN_NAMESPACE'], flush=True)\n"
                "line = '[stats p=0] source: 42 records/s | network-raw 42 B/s | "
                "network-decoded 0 B/s | response-wait 1% | network-decode 0% busy || "
                "parse: benchmark-discard || "
                "sink: 0 rows/s | 0 B/s | 0 flushes/s | 0 source-msg/s | 0% busy | "
                "0 retries | buffered N/A | objects 0/0/0 | 0% backpressure || "
                "guarantee: destructive-benchmark | cpu: 5% rss: N/A'\n"
                "while True:\n"
                "    print(line, flush=True)\n"
                "    time.sleep(0.01)\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            config = root / "config.yaml"
            config.write_text("unused: true\n", encoding="utf-8")

            result = BENCH.run_once(binary, config, root, 3, 0.05, 0.5, 3)

            self.assertGreaterEqual(result["sample_count"], 3)
            self.assertEqual(result["summary"]["source_records_per_s"]["median"], 42)
            log = (root / "run-03.log").read_text()
            self.assertIn("[stats p=0]", log)
            self.assertIn("rep=3", log)
            self.assertIn("destination=" + result["namespace"], log)

    def test_run_once_rejects_pipeline_restart(self):
        process = mock.Mock()
        process.poll.return_value = None
        process.stdout = iter(
            [
                "pipeline failed, restarting: bearer super-secret\n",
                "[stats p=0] source: 42 records/s | network-raw 42 B/s | "
                "network-decoded 0 B/s | response-wait 1% | network-decode 0% busy || "
                "parse: benchmark-discard || "
                "sink: 0 rows/s | 0 B/s | 0 flushes/s | 0 source-msg/s | 0% busy | "
                "0 retries | buffered N/A | objects 0/0/0 | 0% backpressure || "
                "guarantee: destructive-benchmark | cpu: 5% rss: N/A\n",
            ]
        )
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            BENCH.subprocess, "Popen", return_value=process
        ), mock.patch.object(BENCH, "terminate"):
            config = pathlib.Path(directory) / "config.yaml"
            config.write_text("unused: true\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "pipeline failures occurred") as raised:
                BENCH.run_once(
                    pathlib.Path("unused"),
                    config,
                    pathlib.Path(directory),
                    1,
                    0,
                    0.02,
                    1,
                )
            self.assertNotIn("super-secret", str(raised.exception))
            self.assertEqual(
                (pathlib.Path(directory) / "run-01.log").stat().st_mode & 0o777,
                0o600,
            )

    def test_private_result_file_is_never_group_or_world_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            result = pathlib.Path(directory) / "results" / "result.json"
            BENCH.private_text_write(result, '{"status":"ok"}')

            self.assertEqual(result.stat().st_mode & 0o777, 0o600)
            self.assertEqual(result.parent.stat().st_mode & 0o777, 0o700)

    def test_run_once_can_measure_a_finite_source_that_drains_early(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "finite-transferia.py"
            line = (
                "[stats p=0] source: 420 records/s | network-raw 42 B/s | "
                "network-decoded 0 B/s | response-wait 1% | network-decode 0% busy || "
                "parse: benchmark-discard || "
                "sink: 0 rows/s | 0 B/s | 0 flushes/s | 0 source-msg/s | 0% busy | "
                "0 retries | buffered N/A | objects 0/0/0 | 0% backpressure || "
                "guarantee: destructive-benchmark | cpu: 125% rss: 16 MiB"
            )
            binary.write_text(
                "#!/usr/bin/env python3\n"
                f"line = {line!r}\n"
                "for _ in range(4):\n"
                "    print(line, flush=True)\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            config = root / "config.yaml"
            config.write_text("unused: true\n", encoding="utf-8")

            result = BENCH.run_once(
                binary,
                config,
                root,
                1,
                0,
                30,
                3,
                allow_early_completion=True,
            )

            self.assertEqual(result["sample_count"], 4)
            self.assertEqual(result["summary"]["source_records_per_s"]["median"], 420)
            self.assertTrue(result["completed_naturally"])
            self.assertGreater(result["elapsed_seconds"], 0)

    def test_run_once_still_rejects_unexpected_early_completion_by_default(self):
        process = mock.Mock()
        process.poll.return_value = 0
        process.stdout = iter(())
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            BENCH.subprocess, "Popen", return_value=process
        ), mock.patch.object(BENCH, "terminate"):
            config = pathlib.Path(directory) / "config.yaml"
            config.write_text("unused: true\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "exited early"):
                BENCH.run_once(
                    pathlib.Path("unused"),
                    config,
                    pathlib.Path(directory),
                    1,
                    0,
                    1,
                    1,
                )


if __name__ == "__main__":
    unittest.main()
