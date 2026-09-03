#!/usr/bin/env python3

import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import windowed_sink_throughput as benchmark


class WindowedSinkThroughputTest(unittest.TestCase):
    def test_template_expansion_uses_explicit_environment_and_defaults(self) -> None:
        rendered = benchmark.render_config_template(
            "password: ${PASSWORD}\nport: ${PORT:-5432}\n",
            {"PASSWORD": "private-value"},
        )

        self.assertEqual(rendered, "password: private-value\nport: 5432\n")

    def test_template_expansion_rejects_a_missing_required_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "MISSING"):
            benchmark.render_config_template("password: ${MISSING}\n", {})

    def test_private_raw_log_is_never_group_or_world_readable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "raw" / "run.log"
            with benchmark.private_binary_output(log) as output:
                output.write(b"credential-bearing diagnostic")

            self.assertEqual(log.stat().st_mode & 0o777, 0o600)
            self.assertEqual(log.parent.stat().st_mode & 0o777, 0o700)

    def test_artifact_fingerprint_is_non_cryptographic_murmur3(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact = pathlib.Path(directory) / "artifact"
            artifact.write_bytes(b"candidate-binary")

            self.assertEqual(
                benchmark.murmur3_x64_128_file(artifact),
                "3e1d1c42b473abe1c2793abe7f6d7963",
            )

    def test_delivery_uses_the_explicit_clickbench_preset(self) -> None:
        template = {"sink": {"postgres": {"create_tables": True}}}

        document = benchmark.delivery(
            template,
            benchmark.Candidate("baseline", {}),
            "postgres",
            "clickbench_rows",
            100_000_000,
            "clickbench",
            pathlib.Path("/tmp/state"),
        )

        self.assertEqual(
            document["source"]["data_generator"]["preset"],
            {"type": "clickbench"},
        )
        self.assertEqual(
            document["source"]["data_generator"]["amount"],
            {"type": "rows", "row_count": 100_000_000},
        )

    def test_parse_window_accepts_current_partition_stats_prefix(self) -> None:
        line = (
            "[stats p=0] source: 123 records/s || sink: 100 rows/s | "
            "0 retries | cpu: 250% rss: 1.50 GiB\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "transferia.log"
            log.write_text(line, encoding="utf-8")

            result = benchmark.parse_window(log, 1)

        self.assertEqual(result["source_rows_s_mean"], 123)
        self.assertEqual(result["sink_rows_s_mean"], 100)
        self.assertEqual(result["cpu_percent_mean"], 250)
        self.assertEqual(result["rss_bytes_peak"], 1.5 * 1024**3)

    def test_parse_window_fails_closed_on_pipeline_failure_or_unknown_stats(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "transferia.log"
            log.write_text("pipeline failed, restarting: bearer super-secret\n")
            with self.assertRaisesRegex(RuntimeError, "pipeline failure occurred") as raised:
                benchmark.parse_window(log, 1)
            self.assertNotIn("super-secret", str(raised.exception))

            log.write_text("[stats p=0] changed metrics contract\n")
            with self.assertRaisesRegex(RuntimeError, "unrecognized"):
                benchmark.parse_window(log, 1)

    def test_parse_window_rejects_non_single_worker_partition_stats(self) -> None:
        line = (
            "[stats p=1] source: 123 records/s || sink: 100 rows/s | "
            "0 retries | cpu: 250% rss: 1.50 GiB\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "transferia.log"
            log.write_text(line)
            with self.assertRaisesRegex(RuntimeError, "partition zero"):
                benchmark.parse_window(log, 1)

    def test_candidate_settings_do_not_change_the_template_object(self) -> None:
        template = {"sink": {"mysql": {"insert_rows": 1_000}}}

        benchmark.delivery(
            template,
            benchmark.Candidate("probe", {"insert_rows": 250}),
            "mysql",
            "clickbench_rows",
            1,
            "clickbench",
            pathlib.Path("/tmp/state"),
        )

        self.assertEqual(template["sink"]["mysql"]["insert_rows"], 1_000)


if __name__ == "__main__":
    unittest.main()
