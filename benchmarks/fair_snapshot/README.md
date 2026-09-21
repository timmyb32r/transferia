# Fair finite snapshot benchmark

This directory contains the harness for the [2026-09-21 report](../../docs/benchmarks/2026-09-21/REPORT.md). It is an experiment, not product configuration or a production deployment recipe.

The controller is Python; named engines move the records. Transferia and the two small protocol adapters are optimized Rust binaries. Spark's Python frontend constructs a JVM execution plan. Meltano/DataX retain their own upstream runtimes.

## Contract

An accepted measurement must finish successfully and pass all-cell destination verification. A timeout, nonzero exit, missing/duplicate row, or altered value is retained as a failure and excluded from rankings. Source fixtures are immutable. This benchmark does not establish crash recovery or concurrent-source consistency.

All processes for one participant run in one fresh host-network Docker cgroup, CPU 0–15, memory 24 GiB. The resource probe expects the measured server's Linux cgroup-v1 cpuacct/memory layout. The global `measurement.lock` serializes runs. Wall time includes runtime startup, planning, TLS setup and finalization; preparation and verification are outside it. Read the [protocol](../../docs/benchmarks/2026-09-21/PROTOCOL.md) before comparing numbers.

## Server setup

The server root is `$HOME/benchmark-20260921`. Scripts expect pinned images, distribution archives, JVM drivers and release executables from `provenance.json`. The source server has no Internet: download images/archives on a connected machine, transfer them, and load Docker images on the server. Dockerfiles for the two experiment-specific images are in `images/`.

Create a mode-0600 file `private/connections.json` outside the repository:

```json
{
  "source_host": "SOURCE_HOST",
  "pg_host": "DESTINATION_PG_HOST",
  "ch_host": "DESTINATION_CH_HOST",
  "database": "db1",
  "username": "user1",
  "password": "SECRET"
}
```

`setup.py` owns endpoint/TLS paths. The managed PostgreSQL endpoints use session pooling. The source user needs replication privileges for Flink/Debezium; destinations need table/schema creation and insert privileges. PostgreSQL and ClickHouse CA verification stays enabled. ClickHouse uses a precreated single-host MergeTree table; replicated durability is not measured.

The controller needs Python with psycopg, psutil and PyYAML. In the original environment these were already installed under `$HOME/benchmark-tools-20260919/python`; set `PYTHONPATH` accordingly. Keep credentials and generated execution configs under `private/`, never in Git.

## Executing

```sh
python3 setup.py --prepare
python3 setup.py --prepare --large
python3 runner.py rust --dataset smoke --parts 4 --rep 0 --timeout 120
python3 runner.py rust --dataset narrow --parts 4 --rep 1 --route pg-pg
python3 campaign.py --phase coverage
python3 parallelism.py  # Debezium-only P16 follow-up; sufficient DB connections required
python3 campaign.py --phase repeats
python3 campaign.py --phase scale
```

`campaign.py` intentionally contains the original experiment's absolute cutoff, 2026-09-21 06:35 UTC. Set a new explicit cutoff for a new campaign. A `pause-campaign` file stops scheduling at the next run boundary. Resume uses durable result identity; excluded results are rerun. Do not copy old measurements into a new experiment directory.

`rust` uses the original release `bin/transferia-auto`. `rust_ranges` uses a second isolated source copy with `patch_rust_ranges.py`; it forces benchmark-only exact PK cuts and is not a production setting. Keep both binaries separate. Never overwrite the user's working checkout to apply this patch.

In the isolated `~/benchmark-20260921/source` copy at the recorded revision, with dependencies already available:

```sh
export TRANSFERIA_SKIP_SERVER_UI=1
cargo build --locked --release -p transferia-composition --bin transferia
cp target/release/transferia ../bin/transferia-auto
python3 ../patch_rust_ranges.py .
cargo build --locked --release -p transferia-composition --bin transferia
cp target/release/transferia ../bin/transferia-ranges
```

The repository's release profile and `target-cpu=native` apply to both. UI assets are outside this CLI data-plane benchmark.

`airbyte_bridge/` counts records in the merged Airbyte protocol stream and supplies checkpoint statistics expected by the destination; record lines remain byte-for-byte unchanged. This replaces only local platform accounting, not connector logic. Build with `cargo build --release --manifest-path airbyte_bridge/Cargo.toml` and put the binary under `bin/`.

`estuary_capture.rs` bounds official Flow local preview by the known fixture count and stages its JSONL fixture with explicit commit markers. Build using `rustc -O`; this is not a managed Flow deployment or a general completion protocol. Both staging and materialization are measured.

## Evidence and figures

```sh
python3 export_results.py
python3 partition_evidence.py
# Copy only credential-screened results, never private/.
python3 analyze.py /path/to/report-directory
python3 report_tables.py /path/to/report-directory
python3 coverage.py /path/to/report-directory
python3 diagnostic_tables.py /path/to/report-directory
```

`analyze.py` needs Matplotlib and NumPy. It reads `runs.jsonl`, excludes diagnostics (`repetition=0`), failures and explicitly superseded runs, and writes summary CSV/JSON plus PNG/SVG figures. Whiskers show observed min–max, not confidence intervals.

Optional `jdbc_probe.py`, `estuary_delta_probe.py`, `datax_poll_probe.py`, `go_homo_probe.py` `jdbc_batch_probe.py` and `flink_backfill_probe.py` are separate configuration diagnostics. They share the measurement lock and all-cell verification, use repetition zero, and write dedicated result JSON files. Their numbers must not be silently mixed into the primary defaults. The report's diagnostic tables state the changed setting and comparison limitations.

Raw engine logs, 100-ms samples and private configs remain on the benchmark server. Exported public configs are redacted. `partition-evidence.json` retains selected engine statements; `source-queries.json` retains observed SQL. Query sampling may miss short scans and FETCH/COPY execution, so zero observed concurrency is not evidence of single-threaded reading.

Stop scheduling with `pause-campaign`, wait for active jobs to finish, then run `cleanup.py` to restore any preexisting containers' CPU affinities from `results/background-affinity-before.json` after the campaign. Docker ignores an empty cpuset update: when the original setting was empty, cleanup restores the explicit online CPU set and records this distinction in cleanup.json; future CPU hotplug then requires updating that set. Clean only this experiment's containers and inactive replication slots/publications; never prune unrelated volumes.

Before the 10M phase, the campaign calls `prune_intermediates.py --apply` under the measurement lock. It records file sizes and removes only finished, verified runs' Kafka data, Sqoop staging and Flow fixture files. Logs, configs, samples, binaries and database tables remain. The removal journal is retained, and later size exports preserve the recorded measurements even after files are gone. Removing Docker-owned staging directories requires noninteractive sudo; removal targets pass exact run-name and private-directory checks first.

The 10M Flink CDC follow-up uses a TaskManager process size of 18 GiB and managed-memory fraction 0.1 after a recorded 8-GiB-process heap failure. The total client cgroup remains 24 GiB. This exception is labeled separately in figures/CSV. `flink_backfill_probe.py` changes only the backfill-skip flag, retaining that same heap and four parts; it is valid here because the source is immutable and is not a production recommendation for changing sources.
