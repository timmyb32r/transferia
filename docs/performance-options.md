# Connector performance options

`Performance options` is a collapsed disclosure beside `Advanced settings` in
each source/destination editor. Its fields come from the connector's Rust JSON
Schema (`x-ui.section: performance`), not a frontend list. A grouped reader,
writer, buffer or upload object retains its nested configuration paths. Selected
parser/format tuning is displayed in the owning endpoint's disclosure; parser
selection and data-schema editing stay in their original locations.

Moving a field does not change its type, value, default, validation or runtime
semantics. Opening/closing a disclosure never emits a configuration change. No
new tuning setting is invented for endpoints without configurable controls.

## Benchmark-backed inventory

Paths below are relative to the named source/sink configuration. The automatic
search registry is an additional, narrower inventory: its parameter bounds are
optimizer search bounds, not delivery safety limits.

| Endpoint | Benchmarked controls | Evidence |
| --- | --- | --- |
| ClickHouse source | `batch_rows`, `snapshot_reader.{type,compression,max_threads,row_group_rows,decode_threads}` | ClickBench [results](../benchmarks/clickbench_throughput/results/2026-09-03/exact-prefix-summary.json); historical ClickHouse write report at `f95f0e4e` compares native/Parquet readers and thread counts |
| ClickHouse sink | `insert_format`, `compression`, `insert_target_rows`, `insert_target_bytes`, `insert_concurrency`, `parquet_row_group_rows` | [Write benchmark](../benchmarks/clickhouse_write_throughput/README.md), [configuration](../benchmarks/clickhouse_write_throughput/config.example.yaml); historical report at `f95f0e4e` compares native/ArrowStream/Parquet |
| Iceberg source | `read_batch_rows`, `read_data_file_concurrency`, `read_manifest_concurrency`, `parquet_metadata_size_hint_bytes`, `parquet_range_coalesce_bytes`, `parquet_range_fetch_concurrency` | [Read report](../benchmarks/iceberg_read_throughput/REPORT.md), [configuration](../benchmarks/iceberg_read_throughput/config.example.yaml) |
| Iceberg sink | `parquet_compression`, `parquet_row_group_rows`, `write_concurrency`, `target_file_size_bytes`, `commit_target_size_bytes` | [Write benchmark](../benchmarks/iceberg_write_throughput/README.md), [configuration](../benchmarks/iceberg_write_throughput/config.example.yaml) |
| PostgreSQL source / sink | Source `batch_rows`, `copy_to_format`; sink `copy_from_format` | [Database tournament](../benchmarks/database_throughput/run_server_tournament.py), ClickBench results |
| MySQL source / sink | Source `batch_rows`, `read_protocol`; sink `insert_rows` | Database tournament and ClickBench results |
| OpenSearch source / sink | Source `page_rows`, `read_concurrency`; sink `bulk_target_rows`, `bulk_target_bytes`, `bulk_concurrency` | Database tournament and ClickBench results |
| YTsaurus source | `batch_rows`, `read_ordering.type` and distributed-read settings | ClickBench results; historical ClickHouse write benchmark uses PartitionTables |
| YTsaurus sink | `write_target_bytes`, `write_concurrency`, `write_row_buffer_bytes`, `table_writer.desired_chunk_size` | ClickBench results; historical YTsaurus write report at `bea398ef` |
| Logbroker source | `pqv1_decompression_concurrency` | [PQv1 benchmark notes](benchmarks.md); this operates across partition sessions, not within one partition |
| S3 sink | `rotation.{max_rows,max_bytes}`, `buffering.{max_epoch_buffers,max_pending_upload_objects,max_buffered_bytes,max_epoch_bytes}`, `upload.{multipart_threshold,part_size,parallel_parts,max_in_flight_objects}` | [Single-partition S3 profile](../benchmarks/config_bench_pqv1_json_parser_to_s3.yaml) |

Measured profiles also explicitly configure ClickHouse flush/retry controls,
OpenSearch request/response limits, PIT lifetime, flush/retry controls, and S3
upload operation timeout/retry backoffs. These are included, but the profiles do
not establish that each was independently varied.

The same section contains related existing controls: Kafka batch/in-flight and
request limits; Logbroker's YDB-driver read buffer; S3 request timeout and Parquet
batch/encoding settings; YDB batch/RPC size limits; MySQL byte/row limits;
ClickHouse format threads and waited async INSERT; YTsaurus native reader/writer
and flush settings. No measured speedup is claimed for those additional controls.

## Boundaries

- Driver-specific Logbroker settings retain their backend validation. PQv1
  concurrency must stay at its default for the YDB driver; the YDB read buffer
  must stay at its default for PQv1. Descriptions explain this restriction.
- YTsaurus read ordering remains an explicit semantic choice: only ordered reads
  resume. Unordered and distributed reads remain labelled non-resumable.
- Credentials, table identity, parser conversion/framing, primary-key policies,
  TTL and replication semantics are not reclassified as performance settings.
- Destructive benchmark-discard modes stay separate. Obsolete historical knobs
  are not restored. Workload selectors, benchmark duration and pipeline-wide
  memory/worker settings are not duplicated into endpoint configuration.
- S3 rotation continues to define deterministic object boundaries; placing its
  existing controls in this section does not weaken replay/commit validation.

## Regression coverage

The control-plane catalog tests check every registered automatic tuning pointer
and the manual inventory through references and union branches. Frontend tests
cover all endpoint schemas, unchanged values/defaults on disclosure toggles,
nested edits, and detached S3 parser settings. `npm run test:performance-options`
(with the fixture served by Vite) checks header coordinates at the document
bottom, both opening orders, keyboard activation, mobile/desktop and both themes.
