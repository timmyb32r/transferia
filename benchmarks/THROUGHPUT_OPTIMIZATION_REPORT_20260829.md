# Source and sink throughput optimization summary

Date: 2026-08-29

All production measurements used release builds on `timmyb32r-dev4`. Each
comparison kept the dataset, schema, source/sink pair, logical Transferia
partition count, and destination semantics constant within its tournament.
YTsaurus results use five winner repetitions because the shared cluster is
noisy; ClickHouse and Iceberg use two or three repetitions where the backing
service remained available.

| Stage | Old/default rows/s | Selected rows/s | Improvement | Selected production profile |
|---|---:|---:|---:|---|
| Iceberg source → discard | 3,310,975 | 3,310,975 | 1.00x | 65,536-row batches, 32 data files, 32 manifests |
| Generator → YTsaurus static table | 14,890 | 97,904 | **6.58x** | v4 distributed write, c4, 512 MiB groups, 2 GiB chunks |
| YTsaurus → ClickHouse | 496,869 | 5,752,430 | **11.58x** | native TCP, ZSTD, c32, 1M rows / 640 MiB |
| ClickHouse → Iceberg | 99,888 | 999,312 | **10.00x** | ZSTD Parquet, c8, 250k-row groups, 512 MiB commits |

## Decisions

- The Iceberg source was already at the observed storage/network ceiling.
  Doubling file concurrency did not improve the median and raised peak RSS by
  76%, so the existing bounded profile remains the default.
- YTsaurus distributed write with four fragments is the concurrency knee.
  Eight and sixteen fragments were slower and used more memory; 512-MiB groups
  materially beat 256 MiB without exceeding the configured pipeline budget.
  For schemas with a primary key, this ingestion stage is followed by a
  no-ack-before-completion server sort and atomic replacement into a
  `unique_keys=true` table. The detailed report separates that fixed/final sort
  latency from sustained writer throughput.
- ClickHouse insertion is fastest through its negotiated native TCP block
  protocol. Stable Parquet and ArrowStream bodies add an unnecessary
  Arrow-to-format encode plus server-side decode on writes.
- ClickHouse snapshot extraction has the opposite result: production Parquet
  ZSTD reaches 10,954,493 rows/s versus 894,717 rows/s for the strongest native
  TCP reader. The server's parallel Parquet encoder and Transferia's parallel
  row-group decoder make Parquet **12.24x** faster on this read workload.
- Iceberg writes now encode and upload bounded shards concurrently, then commit
  all data files in one atomic `fast_append`. ZSTD and LZ4 were within the
  five-percent selection margin at concurrency eight; ZSTD won on compression
  and peak memory rather than selecting a noisy marginal LZ4 result.

Detailed methodology, candidate tables, CPU, rows/core, RSS, min/max results,
and limitations are recorded in:

- `iceberg_read_throughput/REPORT.md`
- `ytsaurus_write_throughput/REPORT.md`
- `clickhouse_write_throughput/REPORT.md`
- `iceberg_write_throughput/REPORT.md`

The final Iceberg writer precision series was cut short by versioned-object
quota in the dedicated test bucket. Failed quota runs are excluded. The valid
production results and the implementation's compile-only affected gate are
retained; the Iceberg writer report states the reduced repetition count
explicitly rather than presenting the external failure as a successful run.
