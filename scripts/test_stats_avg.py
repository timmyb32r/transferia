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
        "2026-08-13 INFO [stats p=0] pqv1: 1200 msg/s | comp 1.5 MiB/s | "
        "decomp 3.0 MiB/s | response-wait 20% | decomp 40% busy || "
        "parse: 1100 rows/s | 2.5 MiB/s arrow | 3 dlq/s | "
        "1000 source-msg/s | 60% busy || sink: 1000 rows/s | 3.0 MiB/s | "
        f"2 flushes/s | 1000 source-msg/s | {sink_busy}% busy | {retries} retries | "
        f"buffered 4 MiB | objects {objects} | 10% backpressure || "
        "guarantee: at-least-once | cpu: 88% rss: 512 MiB\n"
    )


class StatsAverageTest(unittest.TestCase):
    def test_aggregates_pqv1_to_clickhouse_logs(self):
        samples = STATS.read_samples(
            [stats_line(sink_busy=70, retries=0, objects="0/0/0")] * 2
        )
        averages = STATS.average_samples(samples)

        self.assertEqual(averages["sample_count"], 2)
        self.assertEqual(averages["pq_messages_per_s"], 1200)
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


if __name__ == "__main__":
    unittest.main()
