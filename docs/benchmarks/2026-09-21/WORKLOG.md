# Fair snapshot benchmark — obligation ledger

Started: 2026-09-21 00:34 UTC. Deadline: 2026-09-21 07:34 UTC.

## Scope and acceptance

All measurements execute on timmyb32r-dev4. The three managed clusters are authorized test resources. Transferia Rust uses release mode. Server has no Internet: fetch artifacts on Mac and transfer them. No credentials in committed artifacts.

| Obligation | Status | Evidence / next step |
|---|---|---|
| Inventory server, prior harness, binaries, images and cluster resources | verified | Three SQL endpoints verified; pinned distributions transferred; release Rust built |
| Define reproducible equal-work protocol: data, boundaries, concurrency, resources, commit and correctness | verified | Primary PG→PG and PG→CH snapshots; 1 and 4 readers; distinguish native from externally coordinated partitioning |
| Transferia Rust measurement | verified | Release executable from 27b0f4e1629d; Auto and isolated exact-range variant |
| Transferia Go measurement | verified | User-provided transferctl binary |
| SeaTunnel measurement | verified | All primary P1/P4 narrow/wide cases: three verified repeats |
| Sling measurement | verified | All primary P1/P4 narrow/wide cases: three verified repeats |
| DataX measurement | verified | All primary P1/P4 narrow/wide cases: three verified repeats |
| Airbyte measurement | verified | Official connector pair + explicit platform-counting bridge; native P4 verified |
| Flink CDC measurement | verified | All primary P1/P4 narrow/wide cases: three verified repeats |
| Debezium measurement, explicitly evaluate snapshot chunking | verified | Narrow P16 baseline/bulk: three verified repeats each; 3.6.3.Final; P4 logs prove four 250k-row chunks; baseline and separate bulk-tuned variant |
| Estuary Flow measurement | verified | Official batch capture/materialize local preview; staging included |
| Apache InLong measurement | verified | Standalone Sort data plane with bounded JDBC cursor; P1/P4 verified |
| Meltano measurement | verified | Meltano 4.2.2 / tap 0.10.0 / target 0.8.0; P1/P4 verified |
| Apache Spark measurement | verified | JDBC parallel snapshot |
| Apache Sqoop measurement | verified | Archived distribution and supported full destination path |
| Throughput, aggregate CPU time, RAM, rows per CPU-second | verified | Include helper processes; separate client and database resource scopes |
| Repeats and destination correctness checks | verified | Count, uniqueness and deterministic value verification |
| Inspect documentation/source/config causes of performance differences | verified | Record exact versions and evidence, distinguish hypotheses from measured causes |
| Clear report with attractive histograms and raw measurements | verified | REPORT, TABLES, DIAGNOSTICS, ten PNG/SVG figures and screened raw/configuration exports |
| Exploratory 10M scale follow-up | bounded completion | All 22 P4 cases verified; P1: 2 verified, 2 timeouts, 18 not run before cutoff; no missing case scored |
| Final obligation audit and repository verification | verified | final-audit.json, schema audit, cleanup.json; compile-only gate passed |

## Plan

1. Inventory and pin protocol; recover reusable adapters and build missing artifacts.
2. Establish one instrumented, validated end-to-end run; expand to every participant.
3. Run isolated measurements with common partition boundaries and resource accounting. Prioritize coverage before repeats; record explicit deployment/time blockers.
4. Reserve the final hour for verification, source-based interpretation, charts, report and audit.

## Decisions

- No overlapping measured workloads on the benchmark server.
- Four processes are not reported as native four-way partitioning. Native and external orchestration are labeled.
- Rows/core-second means verified rows divided by total client CPU seconds, not rows/second divided by allocated cores. Also report allocated-core-normalized throughput where useful.
- SQL/client readiness probes are not benchmark measurements.

## 01:22 UTC checkpoint

- Verified PG→PG smoke transfers: Rust release, Go, Sling, DataX, Spark, Meltano, Sqoop, Flink CDC, InLong, Debezium + Kafka + JDBC. These are diagnostics, not reportable comparison repeats.
- SeaTunnel JSON plugin syntax and Airbyte incomplete source stream remain under investigation. Estuary images and flowctl are available; adapter pending.
- Fixtures: 10k smoke, 1M narrow, 1M wide (1024-byte payload), 10M narrow (96-byte payload). Verification checks every cell and key identity after timing.
- All measured processes will share CPU 0–15, 24 GiB cgroup. Existing background containers moved to CPU 16–31; original affinities saved on server and MUST be restored.
- Source replication role granted. Destination user search_path set to fair21,public for Rust sink's unqualified table names.
- Rust native P4 uses physical ctid ranges; Go P4 uses PK quantiles. Add a separately labeled exact-range Rust variant before claiming identical predicates.
- CH route, main randomized repeats, source analysis, charts and report remain pending.

## 01:46 UTC checkpoint

All 13 PG→PG adapters passed at least one end-to-end smoke (including local Estuary batch capture/materialize preview). Rust exact-PK-range release binary built separately from untouched Auto. Five engine families passed PG→CH smoke: Rust, Go, Sling, SeaTunnel, Spark.

Primary coverage now starts on the common 1M narrow and 1M wide datasets, both P1/P4, randomized sequentially. Follow with repeats, then the already prepared 10M narrow scaling fixture. This covers slow engines within the fixed deadline; full finite-job startup remains explicitly included.

Both PG endpoints now SESSION pooling; pg_stat_statements enabled through yc. Its optional SQL execution/work deltas are not CPU and may omit utility/COPY work.

Estuary CDC source rejects Yandex mdb_replication membership despite actual replication working with Debezium/Flink. Use official source-postgres-batch instead. Local preview requires externally bounded capture (known immutable row count) and staged JSONL fixture; label this separately from managed Flow. Tiny release Rust adapter copies bytes and inserts commit markers; all its CPU/RAM is charged.

## 02:16 UTC checkpoint

Primary coverage is running without overlapping engine jobs. Completed main runs already include Rust Auto/exact-PK, Go, Sling, Airbyte P4, Flink CDC, Debezium, InLong, Meltano and Spark. Every accepted run passed full cell validation. DataX additionally passed the ClickHouse smoke.

Airbyte native four-CTID-part source was confirmed from its own logs. Directly piping its parallel checkpoints to destination 3.0.18 produced partition-local sourceStats counts inconsistent with the merged transport. Added an explicit release Rust platform-accounting bridge: preserves record bytes/checkpoint data, counts records on the merged transport, logs differences and forwards stream counts. This connector-pair adapter is not the full Airbyte platform; its cost is included. Both native binaries remain unchanged. Four-part smoke and 1M main run passed.

Debezium's initial main P1 result was excluded and rerun: completion polling previously counted the growing target once per second. The replacement checks cheap insertion statistics and counts only at the completion boundary. Enabled snapshot-only INFO logging to establish actual chunks; no record logging.

## 02:27 UTC checkpoint

All 13 tools plus Rust exact-PK completed both narrow PG→PG cases with verified data (28 accepted groups). Wide coverage is in progress. Actual four splits confirmed in SeaTunnel/Flink/Airbyte logs. The earlier Airbyte native-connection-budget main result was explicitly excluded and rerun with verified exact CTID configuration and platform-counting bridge.

Added a separately labeled Debezium bulk-tuned variant, preserving baseline. Kafka producer batch 256 KiB, linger 5 ms, LZ4, consumer minimum fetch 1 MiB / max wait 50 ms. First narrow P4 completed and validated; repetitions are still required before interpreting its speedup.

## 02:47 UTC checkpoint

P16 uncovered the managed destination connection limit: 16 JDBC sink tasks × default pool minimum 5 = 80, exceeding user1 limit 50. Increased both PG user limits to 200 via yc (completed 02:44 UTC), verified destination search_path remained fair21,public. Pre-change/during-change P16 attempts explicitly excluded. First stable-limit P16 baseline passed full validation; three repeats of baseline and bulk variant are running with fail-fast orchestration before the main campaign resumes.

All original 13 participants plus Rust exact-PK have successful wide P1 results as well. Remaining wide P4 and ClickHouse coverage, repeated primary cases, 10M follow-up, final figures/report and affinity restoration remain active obligations.

## 03:08 UTC checkpoint

P16 baseline and bulk variants completed all three stable-limit repeats successfully. Found and corrected two harness configuration issues before repeats: InLong now explicitly disables scan autocommit for bounded pgJDBC cursor fetch (smoke passed; four old main cases excluded/rerun). Go→CH had a byte-vs-row threshold mistake and default one-second throttle; aborted that invalid run, excluded it, and configured 65536-row trigger / 256 MiB byte ceiling / disabled interval. New configuration verification and ClickHouse coverage continue. These are adapter corrections, not product performance findings.

Compile-only gate already passed (0.94 s); subsequent edits are Python benchmark configuration/docs. Active sequential controller: coverage → repeats → scale, cutoff 06:10 UTC.

## 03:24 UTC checkpoint

Coverage reached all products/routes, with one actionable SeaTunnel wide/P4→CH failure: launcher silently overrode JAVA_TOOL_OPTIONS 4g with its 512m client heap. Explicit documented `-DJvmOption=-Xmx4g` fixes the full 1M-wide validation. All earlier SeaTunnel measured cases retained but superseded; rerun all with uniform heap within unchanged 24 GiB cgroup. Sequential coverage/repeats/scale controller resumed. Added explicit expected-matrix coverage audit and report draft; these are not final result claims yet.

## 03:55 UTC checkpoint

Primary matrix: 126/264 accepted runs at the last export; all 88 primary groups have a successful first result. P16: 6/6. Main randomized repetitions continue without overlap.

Completed separate diagnostics (all repetition=0, excluded from rankings): 12 randomized pgJDBC threshold 0/5 checks (DataX/Spark), 6 Estuary delta_updates checks, 12 DataX completion-poll checks. Full cell validation passed. DataX's 10-second scheduler completion check explains a substantial finite-job timing penalty; explicit 100-ms polling is reported separately. Estuary delta mode reduces keyed Load work and improved local-preview throughput in this fixture. Exact medians await final primary controls in DIAGNOSTICS.md.

Before any 10M run, adjusted the common P4 scale timeout to 900 s based on 1M projections. P1 remains 600 s. Scheduling cutoff extended to 06:35 UTC because report/figures are already drafted; worst in-flight P4 completion 06:50 leaves 44 minutes before 07:34. Large P4 coverage retains priority over exploratory P1. Added safe, explicit pruning of verified staging under the measurement lock before scale; file sizes and removal journal survive, logs/configs/samples remain.

## 04:31 UTC checkpoint

Second primary pass completed; third is running (183/264 at last status, no unresolved main failures). Destination schema audit passed for 119 PostgreSQL and 56 ClickHouse retained tables: PK/NOT NULL and engine/order/types remained as declared. Re-run this audit after the final measurements.

Added and completed 12 Go homogeneous-mode diagnostics (NoHomo=false, PG→PG P1/P4, narrow/wide, three each). Main Go remains explicitly labeled typed path. All diagnostic transfers passed; total configuration diagnostics now 42, outside the primary ranking. Report includes their separate scope and notes that later blocks are not temporally paired with main controls.

## 04:56 UTC checkpoint

Primary progress: 225/264 accepted, no unresolved failures; P16 remains 6/6. All 12 JDBC batch8192 diagnostics passed, bringing the separate configuration diagnostics to 54. DataX narrow throughput improves with a larger writer batch while rows/CPU-second declines; report explicitly distinguishes latency from CPU efficiency. Main repetitions continue, followed automatically by prioritized P4 10M cases. No additional diagnostic families are scheduled.

## 05:22 UTC checkpoint

Primary progress 254/264, no unresolved failures. Final safe yc hardware snapshot saved as cluster-resources.json; both PostgreSQL users retain limit 200, synchronous_commit is on, server swap is zero.

Pre-scale cleanup ownership check found root-owned Docker staging. Updated the guarded pruner to use noninteractive sudo only for validated directory targets. Under the shared measurement lock it successfully removed 72 completed staging paths and retained 52 size records plus logs/configs/samples. The scale phase will run the same idempotent pruning again. This prevents a permission error from stopping the phase transition.

## 05:28 UTC checkpoint

All 264/264 primary measurements and 6/6 Debezium P16 measurements passed full verification. All 54 configuration diagnostics also passed, kept separate from the rankings. Scale phase started successfully and already has verified Go→PG and Rust exact-PK→PG results; server disk has 171 GiB free after staging cleanup. Main medians and interpretation are now frozen from three repeats per primary case. Large-table results, final exports/schema audit, restoration and final report audit remain pending.

## 06:15 UTC checkpoint

21 large P4 cases are verified; Estuary is running. Flink's first 10M attempt hit fatal Java heap OOM and its SQL client kept waiting. Captured JVM stderr evidence, paused scheduling, terminated the waiting client, retained/excluded the failed configuration, and resumed with TaskManager 18 GiB / managed fraction 0.1 inside the unchanged 24-GiB cgroup. The retry passed all-cell validation. Source release 3.4.0 confirms whole-chunk HashMap buffering before watermark completion. One additional bounded diagnostic (same heap, skip-backfill flag only, immutable source, repetition zero) is queued under the shared lock.

Also corrected Airbyte's mechanism description: runtime logs show DirectLoadTableAppendStreamLoader/PostgresInsertBuffer and 100k-row buffers; no persistent fair21_raw tables or _airbyte fields in the audited final table. Do not infer legacy raw/type-and-dedupe behavior from config parameter names. Captured observations and source-version qualification are in airbyte-destination-evidence.json.

## 06:22 UTC checkpoint

All 22/22 large P4 routes/variants are now verified, with the explicitly labeled Flink large-heap correction. The skip-backfill diagnostic passed: about 108.1k rows/s, 66.2k rows/CPU-s, 11.6 GiB RSS versus buffered 90.7k, 23.5k, 16.9 GiB, same 18-GiB TaskManager / 24-GiB cgroup. Configuration diagnostics now total 55 accepted runs (the new 10M case is one exploratory run, not three repeats). Additional large P1 cases continue until the fixed 06:35 scheduling cutoff. Main series remains 264/264, P16 6/6.

## Final checkpoint — 06:50 UTC

294 accepted comparison runs: primary 264/264, P16 6/6, scale 24 (all 22 P4 plus two P1). Another 55 verified diagnostics remain separate. Sqoop and Debezium bulk 10M P1 timed out at 600 seconds; 18 large P1 cases were not scheduled before cutoff. Raw export retains 442 attempts, including smoke, superseded and failed runs.

Final schema audit: 241 PG and 98 CH tables, no issues, all 55 diagnostic run names included. Accounting audit: all 349 accepted runs have the same configured resource budget, positive consistent metrics, no timing overlaps, no negative SQL counter deltas. Public exports passed plaintext and URL-encoded credential screening. Ten PNG/SVG figures regenerated and visually reviewed.

No fair21 container remains. Removed only own inactive CDC state; original active managed slot remains. Restored all five prior containers to all 32 online CPUs. Docker ignores empty cpuset updates, so restoration is explicitly `0-31`, not identical empty persisted config; future CPU hotplug requires extending it. Successful staging pruned with journal; failed staging retained; 168 GiB free. Test DB settings and tables retained for audit.

Final compile-only gate passed in 0.99 seconds; Python AST/JSON and local documentation links checked. No production change or Git commit made. Preexisting competitor-catalog modification retained untouched. Optional large P1 coverage is explicitly incomplete; required primary and prioritized P4 comparisons are complete.
