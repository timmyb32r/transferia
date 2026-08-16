#!/usr/bin/env python3

import importlib.util
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
        "schema_version": 2,
        "config_sha256": "same-config",
        "binary_sha256": "binary",
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
            "[stats p=0] source: 1200 msg/s | comp 1.5 MiB/s | decomp 2.0 GiB/s | "
            "response-wait 20% | decomp 40% busy || parse: 1100 rows/s | 2.5 MiB/s arrow | "
            "3 dlq/s | 1000 source-msg/s | 60% busy || sink: 1000 rows/s | "
            "3.0 MiB/s | 2 flushes/s | 1000 source-msg/s | 70% busy | 1 retries | "
            "buffered 4 MiB | objects 1/2/3 | 10% backpressure || "
            "guarantee: at-least-once | cpu: 88% rss: 512 MiB"
        )

        sample = BENCH.parse_stats_line(line)

        self.assertEqual(sample["partition_id"], 0)
        self.assertEqual(sample["source_messages_per_s"], 1200)
        self.assertEqual(sample["compressed_bytes_per_s"], 1.5 * 1024 * 1024)
        self.assertEqual(sample["decompressed_bytes_per_s"], 2 * 1024**3)
        self.assertEqual(sample["response_wait_percent"], 20)
        self.assertEqual(sample["parse_rows_per_s"], 1100)
        self.assertEqual(sample["sink_rows_per_s"], 1000)
        self.assertEqual(sample["sink_retries"], 1)
        self.assertEqual(sample["rss_bytes"], 512 * 1024 * 1024)

    def test_ignores_other_lines_and_exposes_discard_mode(self):
        self.assertIsNone(BENCH.parse_stats_line("unrelated log line"))
        line = (
            "[stats p=0] source: 42 msg/s | comp 42 B/s | decomp 0 B/s | "
            "response-wait 1% | decomp 0% busy || parse: benchmark-discard || "
            "sink: 0 rows/s | 0 B/s | 0 flushes/s | 0 source-msg/s | 0% busy | "
            "0 retries | buffered N/A | objects 0/0/0 | 0% backpressure || "
            "guarantee: destructive-benchmark | cpu: 5% rss: N/A"
        )
        sample = BENCH.parse_stats_line(line)
        self.assertIsNone(sample["parse_rows_per_s"])
        self.assertEqual(sample["rss_bytes"], 0)

    def test_rejects_removed_downloader_busy_field(self):
        line = (
            "[stats p=0] source: 42 msg/s | comp 42 B/s | decomp 0 B/s | "
            "dl 7% busy | decomp 0% busy || parse: benchmark-discard || "
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
            "S3_BUCKET": "benchmark",
        }
        self.assertEqual(
            BENCH.reproducibility_environment(environment),
            {"PQ_HOST": "broker", "PQ_PORT": "2135", "S3_BUCKET": "benchmark"},
        )


class SummaryTest(unittest.TestCase):
    def test_reports_robust_distribution(self):
        samples = [
            {"partition_id": 0, "source_messages_per_s": value}
            for value in (10, 20, 30, 40, 1000)
        ]
        summary = BENCH.summarize_samples(samples)
        metric = summary["source_messages_per_s"]
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
        baseline = comparison_document(config_sha256="baseline")
        current = comparison_document(config_sha256="candidate")

        with self.assertRaisesRegex(ValueError, "config_sha256"):
            BENCH.validate_comparison_context(current, baseline)

    def test_comparison_allows_distinct_consumer_prefixes_only(self):
        baseline = comparison_document(
            environment={
                "PQ_HOST": "broker",
                "PQ_PORT": "2135",
                "PQ_CONSUMER_JSON": "transferia-json-baseline",
            },
            binary_sha256="baseline-binary",
        )
        current = comparison_document(
            environment={
                "PQ_HOST": "broker",
                "PQ_PORT": "2135",
                "PQ_CONSUMER_JSON": "transferia-json-candidate",
            },
            binary_sha256="candidate-binary",
        )

        BENCH.validate_comparison_context(current, baseline)

        current["environment"]["PQ_HOST"] = "other-broker"
        with self.assertRaisesRegex(ValueError, "environment"):
            BENCH.validate_comparison_context(current, baseline)

    def test_sha256_file_identifies_the_actual_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = pathlib.Path(directory) / "transferia"
            binary.write_bytes(b"candidate-binary")

            self.assertEqual(
                BENCH.sha256_file(binary),
                "b75afc06019cb1f4b81851ad4d23fe7586c10564e26d06166ab124a9ff406233",
            )

    def test_run_namespace_is_stable_and_isolates_every_repetition(self):
        first = BENCH.run_namespace(pathlib.Path("/results/baseline"), 1)

        self.assertEqual(first, BENCH.run_namespace(pathlib.Path("/results/baseline"), 1))
        self.assertNotEqual(first, BENCH.run_namespace(pathlib.Path("/results/baseline"), 2))
        self.assertNotEqual(first, BENCH.run_namespace(pathlib.Path("/results/candidate"), 1))
        self.assertRegex(first, r"^[a-f0-9]{12}_01$")

    def test_clickhouse_cleanup_drops_both_tables_and_waits_for_merges(self):
        queries = []

        def query(environment, sql):
            queries.append((environment, sql))
            return "0\n" if sql.startswith("SELECT") else ""

        environment = {
            "CLICKHOUSE_HTTP_ENDPOINT": "http://clickhouse:8123",
            "CLICKHOUSE_DATABASE": "bench-db",
        }
        with mock.patch.object(BENCH, "clickhouse_query", side_effect=query):
            BENCH.cleanup_clickhouse_run(environment, "events_deadbeef_01")

        self.assertEqual(
            [sql for _, sql in queries],
            [
                "DROP TABLE IF EXISTS `bench-db`.`events_deadbeef_01` SYNC",
                "DROP TABLE IF EXISTS `bench-db`.`events_deadbeef_01_dlq` SYNC",
                "SELECT count() FROM system.merges WHERE database = 'bench-db' "
                "AND table IN ('events_deadbeef_01', 'events_deadbeef_01_dlq')",
            ],
        )

    def test_successful_clickhouse_run_rejects_cleanup_on_the_wrong_endpoint(self):
        with mock.patch.object(BENCH, "clickhouse_query", return_value="0\n"):
            with self.assertRaisesRegex(RuntimeError, "expected both benchmark tables"):
                BENCH.cleanup_clickhouse_run(
                    {}, "events_deadbeef_01", require_existing=True
                )

    def test_successful_clickhouse_run_requires_and_drops_both_tables(self):
        queries = []

        def query(_environment, sql):
            queries.append(sql)
            if "system.tables" in sql:
                return "2\n"
            return "0\n"

        with mock.patch.object(BENCH, "clickhouse_query", side_effect=query):
            BENCH.cleanup_clickhouse_run({}, "events_deadbeef_01", require_existing=True)

        self.assertIn("SELECT count() FROM system.tables", queries[0])
        self.assertEqual(sum(sql.startswith("DROP TABLE") for sql in queries), 2)

    def test_clickhouse_cleanup_times_out_if_merges_do_not_quiesce(self):
        with mock.patch.object(BENCH, "clickhouse_query", return_value="1\n"), mock.patch.object(
            BENCH.time, "monotonic", side_effect=[0.0, 0.0, 31.0]
        ), mock.patch.object(BENCH.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "background merges"):
                BENCH.cleanup_clickhouse_run({}, "events_deadbeef_01", timeout_seconds=30)

    def test_clickhouse_query_keeps_credentials_out_of_the_url(self):
        environment = {
            "CLICKHOUSE_HTTP_ENDPOINT": "http://clickhouse:8123",
            "CLICKHOUSE_DATABASE": "bench-db",
            "CLICKHOUSE_USERNAME": "bench-user",
            "CLICKHOUSE_PASSWORD": "secret",
        }
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"0\n"
        with mock.patch.object(BENCH.urllib.request, "urlopen", return_value=response) as urlopen:
            BENCH.clickhouse_query(environment, "SELECT 1")

        request = urlopen.call_args.args[0]
        self.assertNotIn("bench-user", request.full_url)
        self.assertNotIn("secret", request.full_url)
        self.assertEqual(request.get_header("X-clickhouse-user"), "bench-user")
        self.assertEqual(request.get_header("X-clickhouse-key"), "secret")

    def test_cleanup_detection_uses_the_sink_config_not_environment_defaults(self):
        config = "source:\n  pqv1: {}\nsink:\n  clickhouse: {}\n"
        with mock.patch.object(BENCH, "cleanup_clickhouse_run") as cleanup:
            BENCH.cleanup_run(config, {}, "deadbeef_01", require_existing=True)

        cleanup.assert_called_once_with(
            {}, "events_deadbeef_01", require_existing=True
        )

        with mock.patch.object(BENCH, "cleanup_clickhouse_run") as cleanup:
            BENCH.cleanup_run(
                "sink:\n  discard: {}\n", {}, "deadbeef_01", require_existing=True
            )
        cleanup.assert_not_called()

    def test_repetition_cleanup_runs_even_when_the_benchmark_fails(self):
        config_text = "sink:\n  clickhouse: {}\n"
        output = pathlib.Path("/results/failed")
        namespace = BENCH.run_namespace(output, 2)
        with mock.patch.object(BENCH, "run_once", side_effect=RuntimeError("benchmark failed")), \
             mock.patch.object(BENCH, "cleanup_run") as cleanup:
            with self.assertRaisesRegex(RuntimeError, "benchmark failed"):
                BENCH.run_repetition(
                    pathlib.Path("transferia"),
                    pathlib.Path("config.yaml"),
                    config_text,
                    output,
                    2,
                    30,
                    90,
                    80,
                    {},
                )

        cleanup.assert_called_once_with(config_text, {}, namespace, require_existing=False)

    def test_cleanup_failure_does_not_mask_the_benchmark_failure(self):
        run_error = RuntimeError("benchmark failed")
        cleanup_error = RuntimeError("cleanup failed")
        with mock.patch.object(BENCH, "run_once", side_effect=run_error), mock.patch.object(
            BENCH, "cleanup_run", side_effect=cleanup_error
        ):
            with self.assertRaisesRegex(RuntimeError, "benchmark failed") as raised:
                BENCH.run_repetition(
                    pathlib.Path("transferia"),
                    pathlib.Path("config.yaml"),
                    "sink:\n  clickhouse: {}\n",
                    pathlib.Path("/results/failed"),
                    1,
                    30,
                    90,
                    80,
                    {},
                )

        self.assertIs(raised.exception.__cause__, cleanup_error)

    def test_successful_repetition_requires_tables_on_the_cleanup_endpoint(self):
        config_text = "sink:\n  clickhouse: {}\n"
        output = pathlib.Path("/results/success")
        with mock.patch.object(
            BENCH,
            "run_once",
            return_value={"namespace": BENCH.run_namespace(output, 1)},
        ), mock.patch.object(BENCH, "cleanup_run") as cleanup:
            BENCH.run_repetition(
                pathlib.Path("transferia"),
                pathlib.Path("config.yaml"),
                config_text,
                output,
                1,
                30,
                90,
                80,
                {},
            )

        cleanup.assert_called_once_with(
            config_text,
            {},
            BENCH.run_namespace(output, 1),
            require_existing=True,
        )


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
                "line = '[stats p=0] source: 42 msg/s | comp 42 B/s | decomp 0 B/s | "
                "response-wait 1% | decomp 0% busy || parse: benchmark-discard || "
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
            self.assertEqual(result["summary"]["source_messages_per_s"]["median"], 42)
            log = (root / "run-03.log").read_text()
            self.assertIn("[stats p=0]", log)
            self.assertIn("rep=3", log)
            self.assertIn("destination=" + BENCH.run_namespace(root, 3), log)

    def test_run_once_rejects_pipeline_restart(self):
        process = mock.Mock()
        process.poll.return_value = None
        process.stdout = iter(
            [
                "pipeline failed, restarting: boom\n",
                "[stats p=0] source: 42 msg/s | comp 42 B/s | decomp 0 B/s | "
                "response-wait 1% | decomp 0% busy || parse: benchmark-discard || "
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
            with self.assertRaisesRegex(RuntimeError, "pipeline failures occurred"):
                BENCH.run_once(
                    pathlib.Path("unused"),
                    config,
                    pathlib.Path(directory),
                    1,
                    0,
                    0.02,
                    1,
                )


if __name__ == "__main__":
    unittest.main()
