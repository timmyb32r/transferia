import importlib.util
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
SPEC = importlib.util.spec_from_file_location("stats_avg", SCRIPT_DIR / "stats_avg.py")
STATS = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(STATS)


def stats_line(*, sink_busy: int, retries: int, objects: str) -> str:
    return (
        "2026-08-13 INFO [stats p=0] source: 1200 records/s | network-raw 1.5 MiB/s | "
        "network-decoded 3.0 MiB/s | response-wait 20% | network-decode 40% busy || "
        "parse: 1100 rows/s | 2.5 MiB/s arrow | 3 dlq/s | "
        "1000 source-msg/s | 60% busy || sink: 1000 rows/s | 3.0 MiB/s | "
        f"2 flushes/s | 1000 source-msg/s | {sink_busy}% busy | {retries} retries | "
        f"buffered 4 MiB | objects {objects} | 10% backpressure || "
        "guarantee: at-least-once | cpu: 88% rss: 512 MiB\n"
    )


def postgres_stats_line() -> str:
    return (
        "2026-08-13 INFO [stats p=0] source: 40000 records/s | network-raw 0 B/s | "
        "network-decoded 24.0 MiB/s | response-wait 0% | network-decode 0% busy || "
        "parse: 40000 rows/s | 24.0 MiB/s arrow | 0 dlq/s | "
        "40000 source-msg/s | 5% busy || sink: 40000 rows/s | 18.0 MiB/s | "
        "4 flushes/s | 40000 source-msg/s | 65% busy | 0 retries | "
        "buffered 0 MiB | objects 0/0/0 | 0% backpressure || "
        "guarantee: at-least-once | cpu: 90% rss: 300 MiB\n"
    )


def ytsaurus_stats_line() -> str:
    return (
        "2026-08-13 INFO [stats p=0] source: 300 records/s | network-raw 0 B/s | "
        "network-decoded 4.0 MiB/s | response-wait 0% | network-decode 0% busy || "
        "parse: 300 rows/s | 4.0 MiB/s arrow | 0 dlq/s | "
        "300 source-msg/s | 0% busy || sink: 300 rows/s | 4.0 MiB/s | "
        "2 flushes/s | 300 source-msg/s | 55% busy | 0 retries | "
        "buffered 0 B | objects 0/0/0 | 0% backpressure || "
        "guarantee: at-least-once | cpu: 10% rss: 80 MiB\n"
    )


def clickhouse_source_stats_line() -> str:
    return (
        "2026-08-13 INFO [stats p=0] source: 20000 records/s | network-raw 0 B/s | "
        "network-decoded 16.0 MiB/s | response-wait 65% | network-decode 0% busy || "
        "parse: 20000 rows/s | 16.0 MiB/s arrow | 0 dlq/s | "
        "20000 source-msg/s | 4% busy || sink: 20000 rows/s | 12.0 MiB/s | "
        "3 flushes/s | 20000 source-msg/s | 50% busy | 0 retries | "
        "buffered 0 B | objects 0/0/0 | 0% backpressure || "
        "guarantee: at-least-once | cpu: 45% rss: 160 MiB\n"
    )


def s3_source_stats_line() -> str:
    return (
        "2026-08-13 INFO [stats p=0] source: 500 records/s | network-raw 8.0 MiB/s | "
        "network-decoded 8.0 MiB/s | response-wait 0% | network-decode 0% busy || "
        "parse: 500 rows/s | 7.0 MiB/s arrow | 0 dlq/s | "
        "500 source-msg/s | 35% busy || sink: 500 rows/s | 6.0 MiB/s | "
        "2 flushes/s | 500 source-msg/s | 45% busy | 0 retries | "
        "buffered 0 B | objects 0/0/0 | 0% backpressure || "
        "guarantee: at-least-once | cpu: 25% rss: 120 MiB\n"
    )


class StatsAverageTest(unittest.TestCase):
    def test_aggregates_pqv1_to_clickhouse_logs(self):
        samples = STATS.read_samples(
            [stats_line(sink_busy=70, retries=0, objects="0/0/0")] * 2
        )
        averages = STATS.average_samples(samples)

        self.assertEqual(averages["sample_count"], 2)
        self.assertEqual(averages["source_records_per_s"], 1200)
        self.assertEqual(averages["sink_busy_percent"], 70)
        self.assertEqual(STATS.diagnosis(averages), [])

    def test_aggregates_pqv1_to_s3_concurrent_attempt_load(self):
        samples = STATS.read_samples(
            [stats_line(sink_busy=170, retries=1, objects="1/2/3")]
        )
        averages = STATS.average_samples(samples)
        notes = STATS.diagnosis(averages)

        self.assertEqual(averages["sink_busy_percent"], 170)
        self.assertTrue(any("retries" in note for note in notes))
        self.assertTrue(any("exceed 100%" in note for note in notes))

    def test_rejects_a_stats_line_that_no_longer_matches_the_contract(self):
        with self.assertRaisesRegex(ValueError, "invalid stats line 1"):
            STATS.read_samples(["[stats p=0] obsolete format\n"])

    def test_aggregates_native_postgres_connector_logs(self):
        averages = STATS.average_samples(STATS.read_samples([postgres_stats_line()]))

        self.assertEqual(averages["source_records_per_s"], 40000)
        self.assertEqual(averages["sink_rows_per_s"], 40000)
        self.assertEqual(averages["sink_busy_percent"], 65)

    def test_aggregates_native_ytsaurus_connector_logs(self):
        averages = STATS.average_samples(STATS.read_samples([ytsaurus_stats_line()]))

        self.assertEqual(averages["source_records_per_s"], 300)
        self.assertEqual(averages["sink_rows_per_s"], 300)
        self.assertEqual(averages["sink_busy_percent"], 55)

    def test_aggregates_native_clickhouse_source_logs(self):
        averages = STATS.average_samples(
            STATS.read_samples([clickhouse_source_stats_line()])
        )

        self.assertEqual(averages["source_records_per_s"], 20000)
        self.assertEqual(averages["response_wait_percent"], 65)

    def test_aggregates_ordered_s3_source_logs(self):
        averages = STATS.average_samples(STATS.read_samples([s3_source_stats_line()]))

        self.assertEqual(averages["source_records_per_s"], 500)
        self.assertEqual(averages["network_raw_bytes_per_s"], 8 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
