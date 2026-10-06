# ClickHouse extraction measurements

The timed client is `crates/transferia-connector-clickhouse/examples/extraction_probe.rs`, built with `cargo build --release -p transferia-connector-clickhouse --example extraction_probe`. It uses Transferia's Arrow/Parquet and native ClickHouse dependencies, and its shared TLS/HTTP boundary. The example is a transport/decoder probe, not the full production delivery pipeline. `production_matrix.py` separately runs the real release CLI source → discard.

The Python scripts are outside the timed client path: they load fixtures, schedule serial Rust processes, collect CPU/RSS and save results. The 2026-10-06 deployment uses `/home/timmyb32r/ch-extraction-20261006`, private credentials at mode 0600, and the pre-existing private connection JSON on that host. These paths are campaign-specific; adapt them explicitly for a new environment. Never commit private configurations or logs containing secrets.

Authoritative experiment requirements, matrix, dataset manifest and caveats are in `docs/benchmarks/2026-10-06-clickhouse/`. Raw results and query-log verification are published there; do not treat warmup files or incomplete processes as primary results.

`raw` drains response chunks without retaining the whole response. `decode` streams ArrowStream batches with bounded backpressure, or buffers finite Parquet bytes and decodes its row groups concurrently. Native TCP uses the same `clickhouse-arrow` implementation as production. Every decoded query must return exactly the configured row count. Time limits are minimum observation durations: finite files must finish before publishing a decoded sample. Report process CPU-seconds separately from elapsed seconds and allocated cores.

## Reproduce the campaign

Build the release example on the measurement host, then preserve that binary under the campaign's `bin/probe-v2`. Baseline V1 is archived separately: its source is in the report's `provenance/probe-v1.rs.txt`. Recompiling V2 does not reproduce V1 identically.

The private `base.json` contains `host`, `user`, `password`, and `ca`. Set mode 0600. Copy `control.py` to `/tmp/ch_control.py`; controllers import that untimed SQL helper. Prepare the exact fixture with `create.sql` and the published part manifest. The loader is campaign-specific and intentionally skips the independently loaded first part; inspect it before reuse.

Run these phases **sequentially**, with no overlapping builds, downloads, or competing benchmark clients:

```sh
python3 run_matrix.py
python3 run_controls.py repeat
python3 run_controls.py verify
python3 cpp_matrix.py
python3 curl_control.py
python3 run_controls.py controls
python3 run_controls.py final
python3 production_matrix.py
python3 run_controls.py robustness
python3 run_controls.py confirm_row_group
python3 collect_query_log.py
```

`cpp_matrix.py` expects the two official pinned binaries under `cpp/usr/bin/clickhouse` and `cpp-old/usr/bin/clickhouse`. `production_matrix.py` expects `bin/transferia-fixed`, a fresh release CLI built after the LZ4 negotiation fix. The first matrix requires `bin/probe-v1`; later phases use V2. Controller paths, fixed source table and reference date are deliberately explicit rather than a generic benchmark framework. Results are resumable by cell filename: use a fresh directory when changing a configuration instead of reusing stale results.

Copy only public results, never `private/`. Preserve mtimes when collecting V1; then archive `file-timestamps.json` beside `raw/`, as in the published report. The validator reads that manifest because Git checkout does not preserve mtimes. Validate and render:

```sh
python3 validate_results.py REPORT_DIR/raw REPORT_DIR/query-log.jsonl REPORT_DIR/query-validation.json
python3 analyze.py REPORT_DIR/raw REPORT_DIR --validation REPORT_DIR/query-validation.json
python3 report.py REPORT_DIR
```

Use the exact argument help of `analyze.py` if adapting paths. `query-validation.json` must confirm complete server responses before a result enters a chart. Failed cells remain in `FAILURES.md`; they are not zero-valued bars. Timed Python parsing or pandas materialization is outside this campaign's contract.

The observed Native regression and its two focused tests are described in the report. `native_regression.py` runs them in an isolated vendored-crate copy because Cargo cannot test a non-member dependency from this workspace and unrelated old upstream unit modules currently fail to compile. Those modules are disabled only in the temporary harness; no production code is replaced. `--old-lz4-behavior` is a negative control and must fail. Offline execution needs the crate's dev-dependencies cached. This does not replace or claim the full upstream test suite.
