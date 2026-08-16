# Single-partition benchmarks

The five configurations in `benchmarks/` form a measurement ladder:

1. PQv1 network and read loop, discarding before decompression;
2. network plus decompression;
3. network, decompression, and JSON parsing;
4. the full ClickHouse path;
5. the full S3 path.

All profiles assign only partition `0`. Their default consumer prefixes are
different, so running one profile never advances another profile's offsets. The
runner appends repetition numbers `1..5`. It also derives a stable arm/repetition
namespace from the output directory: ClickHouse repetitions use separate tables,
and S3 repetitions use separate prefixes. After each ClickHouse repetition the
runner uses the HTTP endpoint to synchronously drop its main/DLQ tables and waits
for their background merges to disappear before starting the next sample. Set
`CLICKHOUSE_HTTP_ENDPOINT` when it is not `http://localhost:8123`; after a
successful sample, cleanup first verifies that both unique tables exist there,
so a forgotten endpoint fails the run instead of cleaning the wrong server. Use two distinct consumer prefixes at
the same starting offset for baseline and candidate, or reset all five
consumers to that offset after collecting the baseline. A manual `cargo run`
uses the suffix `manual`. Preload enough data that the backlog remains non-empty
for every warmup and sample period. A drained backlog invalidates the run.

Every endpoint and credential in the YAML files has an environment override:
`PQ_HOST`, `PQ_PORT`, `PQ_TOPIC`, `PQ_TOKEN`, the profile-specific `PQ_CONSUMER_*`,
`CLICKHOUSE_*`, and `S3_*`. Defaults describe local plaintext development
services; production credentials should be supplied only through the
environment.

These files are benchmark templates, not runtime configuration files. The
benchmark runner explicitly expands their `${NAME}` and `${NAME:-default}`
placeholders into a private temporary YAML file and deletes it after each run.
Transferia itself parses YAML literally and never performs implicit environment
expansion.

Pinned ClickHouse, YDB Local, and LocalStack/S3 development services are
declared in `docker-compose/docker-compose.yaml`; LocalStack creates the default
benchmark bucket on startup. Start them with:

```shell
docker compose -f docker-compose/docker-compose.yaml up -d
```

The Compose services make the downstream configurations editable and
repeatable, but YDB Local still must pass the legacy PQv1 protocol check noted
below before it can supply benchmark input.

Build and collect five 30-second warmups followed by 90-second samples:

```shell
PQ_CONSUMER_JSON=transferia-json-baseline \
PQ_HOST=broker PQ_PORT=2135 \
PQ_TOPIC=/benchmark/events \
PQ_TOKEN=... \
python3 scripts/run_single_partition_benchmark.py \
  --config benchmarks/config_bench_pqv1_json_parser_to_discard.yaml \
  --output-dir benchmark-results/baseline
```

The runner builds the release binary once, always launches worker `0/1`, keeps
the complete process log for every repetition, and writes `result.json` with the
git revision, dirty flag, binary and configuration hashes, Rust version, OS,
non-secret endpoint overrides, raw sample count, median, MAD, p10, and p90. A run is
rejected if it sees another partition, any sink retry or pipeline restart,
fewer than 80 non-zero samples, or early process exit.

To compare a candidate with a baseline collected under the same broker,
backlog, machine, and downstream state:

```shell
PQ_CONSUMER_JSON=transferia-json-candidate \
PQ_HOST=broker PQ_PORT=2135 \
PQ_TOPIC=/benchmark/events \
PQ_TOKEN=... \
python3 scripts/run_single_partition_benchmark.py \
  --config benchmarks/config_bench_pqv1_json_parser_to_discard.yaml \
  --output-dir benchmark-results/candidate \
  --baseline benchmark-results/baseline/result.json
```

The comparator rejects a baseline produced from another configuration, machine,
timing/sample policy, broker/topic, or downstream endpoint. Only the
profile-specific `PQ_CONSUMER_*` prefix is expected to differ. The recorded
binary hashes identify the exact baseline and candidate executables even for
`--skip-build` or dirty worktrees.

The command exits with status `2` only when median PQ message throughput falls
by more than 5% and at least four of five paired repetitions also fall by more
than 5%. Treat CPU, RSS, response wait, busy time, backpressure, and stage rates
in the JSON as diagnostics rather than replacing the primary throughput
criterion. `response_wait_percent` is wall time awaiting any PQ server response,
including control-plane traffic; it is not downloader CPU utilization.

The current PQv1 session deliberately allows one source batch in progress per
partition to keep source memory bounded and control-plane ACK/Release messages
responsive. A localhost broker hides the round-trip cost of that decision.
Run the ladder against the real broker, or at controlled 0/10/30/60 ms RTT, and
record RTT next to each result directory. For a one-partition run,
`decompression_concurrency: 1` and `4` should produce equivalent throughput;
the setting creates concurrency between partition sessions, not within one
partition.

This repository's deterministic fault-injection tests cover ambiguous
ClickHouse replay, partial S3 epoch replay, and PQ control traffic under blocked
data dispatch. The local Compose file is a development convenience, not proof
that YDB Local implements the legacy `MigrationStreamingRead` protocol. A live
PQv1 benchmark must first demonstrate that the configured broker supports that
API.
