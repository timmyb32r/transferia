# Snapshot comparison protocol

Deadline: 2026-09-21 07:34 UTC. This document is the protocol, not a results claim.

## Workload and endpoints

- Source: managed PostgreSQL 17.11, cluster `mdbf9cs75hqtqsvdah9t`.
- PostgreSQL destination: a separate cluster `mdb2mf6h3fqnotfghglb`.
- ClickHouse destination: cluster `mdbppa6uaripnodn0034`, one shard / two replicas.
- Execute engines on `timmyb32r-dev4`, never on the Mac. The Mac downloads artifacts only.
- Snapshot only. CDC engines may require slots/publications internally, but there are no concurrent source mutations. Live replication performance is outside scope.
- Primary common fixtures: one million narrow rows and one million wide rows; scaling fixture: ten million narrow rows, BIGSERIAL PK, two signed integer fields and two text fields. Narrow payload: 96 bytes; wide payload: 1024 bytes. Values are deterministic; these compressible fixtures do not represent all production distributions. Separate richer-type checks may supplement them.
- Precreate the PostgreSQL destination with an identical PK and NOT NULL constraints. Record metadata columns or staging added by an engine. Preparation and verification are outside transfer timing.

## Parallelism

Both PostgreSQL endpoints use SESSION pooling for all reportable measurements, after transaction pooling broke Estuary prepared statements during smoke checks. Source user receives mdb_replication.

Compare one and four parts/readers. For explicit range readers, cuts are 250001, 500001 and 750001 on the one-million-row fixture. Report native partitioning, native automatic/capped plans and external multi-process orchestration separately. A concurrency setting is not evidence of actual concurrent source scans. Observe queries and engine logs before assigning a result to the four-reader comparison.

Native planners may choose physical ranges, arithmetic key ranges, sampled key ranges or hash scans. Their results answer a different question than identical-predicate runs and must carry that qualification. Unsupported modes are not zeros on a chart.

## Resources and timing

Finite engines run in a fresh Docker cgroup with CPU affinity 0–15 and an aggregate memory limit of 24 GiB. Multi-process sharding shares that one limit. The server has no swap. Persistent helper services require explicit aggregate accounting and may not run unaccounted during another participant's measurement.

Capture cgroup user+system CPU time and sample aggregate process-tree RSS every 100 ms. Keep the container alive briefly after engine completion to read its final CPU counter before removal. Report cgroup peak memory separately: it includes cache and is not interchangeable with RSS. Aggregate RSS can double-count shared pages between processes.

- throughput = verified rows / elapsed transfer seconds;
- average occupied logical CPUs = CPU seconds / elapsed seconds;
- CPU percent = 100 × average occupied logical CPUs;
- rows per CPU-second = verified rows / aggregate CPU seconds;
- throughput per allocated logical CPU = throughput / 16 (different from CPU efficiency).

Completion markers are polled every 100 ms, so very short runs have approximately 0–100 ms boundary quantization. The 10M-row follow-up reduces the relative importance of this and fixed startup costs. Source data is small enough for a warm cache; no host cache dropping or cold-cache claim is made.

Start finite-job timing immediately before container start. Stop when the engine has returned successfully after writing/committing its data. Persistent engines need a separately documented completion boundary. Loading images, DDL, source preparation, and post-run verification are excluded. Runtime startup, planning, TLS connections, transfer and engine finalization are included. No throughput measurement overlaps another workload or release compilation.

Managed database CPU/RAM is not part of the client cgroup. Any database statistics are reported in a separate scope, never silently added to or confused with client utilization. Optional pg_stat_statements deltas aggregate the current user/database and exclude monitoring queries; they are not per-process counters. COPY/FETCH/utility work can be absent with track_utility=off. Unrelated same-user SQL or counter eviction/reset could contaminate these diagnostics, so they are not the throughput/CPU scoring source.

## Correctness and repetitions

Verify destination count, distinct PK count, min/max PK, and every source field against its deterministic expression. A process exit of zero alone is insufficient. A post-run destination schema audit additionally checks that connector DDL retained the expected PK/NOT NULL constraints and ClickHouse engine/order/field types. Airbyte reuses a table per dataset, so its audit is of the currently retained schema rather than a historical DDL trace. Invalid rows, duplicates, missing rows and failed runs are excluded from performance rankings and retained in the failure table.

First run 10,000-row smoke tests. Then prioritize coverage of all tools before three measured repeats. Use randomized sequential order for repeated comparisons. Record actual sample count and min/max variation; do not manufacture confidence intervals from too few runs.

## Fairness interpretation

Keep data, endpoint hosts, TLS, CPU/memory budget and destination constraints common. Most JDBC adapters use fetch size 8192 and batches of 1000. Sqoop retains its default fetch size 1000 and export grouping (100 records/statement, 100 statements/transaction); this exception is recorded rather than treated as equal batching. Spark commits at partition/task boundaries, not after every 1000-row executeBatch. Native COPY/block writers retain their efficient protocol; forcing them through JDBC would measure an artificial implementation. Record batching, encoding, compression, staging/upsert, planning, startup, intermediate brokers and commit semantics as explanations to investigate. A mechanism seen in source is not by itself proof that it caused a measured difference.

## Large-table follow-up priority

After common 1M coverage and repeats, run the 10M narrow fixture. Schedule all P4 cases before P1 cases, randomized within each group, so every tool has a chance at the large parallel workload before the fixed cutoff. Before the scale phase began, the common P4 deadline was set to 900 seconds based on the 1M runtime projection; P1 follow-ups retain 600 seconds. Timed-out transfers are failures, not zero-throughput observations. The absolute scheduling cutoff is 06:35 UTC; an in-flight P4 job can finish by 06:50, leaving 44 minutes before the user deadline. Charts/tables must expose any cases not reached. Large-table runs are single exploratory measurements unless explicitly repeated.

## P16 infrastructure correction

An additional Debezium-only 16-part check is outside the common P1/P4 ranking. Its initial attempts exhausted the managed PG user pool limit of 50 because each JDBC sink task initializes a five-connection pool. At 02:44 UTC both source and destination user1 limits were raised to 200 through yc. Preflight runs before/during this change are excluded and retained with reasons; accepted P16 repeats begin after the update completed. The main P1/P4 jobs did not reach the previous limit.

## Build and verification evidence

Transferia uses the repository's release profile: opt-level 3, fat LTO, one codegen unit, panic=abort; Cargo fingerprint on the server confirms `-C target-cpu=native`. Auto and the explicitly separate exact-range executable use the same profile. Competitors use the pinned published binaries/distributions, and JVMs use their own JIT. This is not a comparison of identical compiler toolchains.

`just check-affected` passed the compile-only repository gate in 0.99 seconds (final rerun). Its fallback was `cargo check --workspace --all-targets --all-features` because the standalone benchmark Cargo project is outside the package selector map. No workspace tests/Clippy/release gate were run. Benchmark Rust helpers were separately built optimized on the server and exercised by real validated transfers.

## Flink large-chunk memory correction

The first 10M/P4 Flink CDC attempt exhausted Java heap with TaskManager process size 8 GiB. The waiting SQL client was terminated after the fatal error was captured. The retained retry uses TaskManager process size 18 GiB and managed-memory fraction 0.1, keeping the common 24-GiB cgroup and four native chunks. Main 1M settings are unchanged; scale figures/CSV label this exception. A separate repetition-zero diagnostic changes only the backfill-skip flag, on the immutable source, and is excluded from ranking.
