# ClickHouse snapshot-read performance

Date: 2026-08-28  
Benchmark host: `timmyb32r-dev4`  
Source: `db1.my_table`, 50,000,000 transfer-log rows

## Summary

- The highest measured stable wire throughput is **Parquet + LZ4 at 11.30 million rows/s** with 32 ClickHouse threads.
- The production Parquet reader reaches **11.13 million rows/s in the data plane** with ZSTD, 32 ClickHouse encoding threads, 250,000-row groups, and 16 parallel Rust decoders. Its 11.04–11.23 million rows/s range is indistinguishable from the 11.10 million rows/s wire ceiling for the same server profile.
- The lower-memory production profile uses eight Rust decoders and reaches **9.63 million rows/s** with 1.45 GiB peak RSS instead of 2.77 GiB.
- The fastest streaming wire format is **RowBinaryWithNamesAndTypes at 2.11 million rows/s**. Eight threads are the practical setting: they deliver 2.09 million rows/s, within 0.8% of the 16-thread result.
- The fastest Arrow wire setting is **ArrowStream + LZ4 with eight threads at 1.76 million rows/s**.
- The optimized production native-protocol reader reaches **883,053 rows/s** with ZSTD and 16 server threads, **12.3% above** the previous 786,289 rows/s baseline.

The wire tournament discards response bytes and measures ClickHouse serialization plus network extraction. Production tournaments additionally decode the transport into Arrow, add source metadata, traverse the Transferia pipeline, and acknowledge through the discard sink. Production data-plane timing excludes process startup, MDB resolution, TLS setup, and schema discovery; process throughput retains that overhead.

## Stable wire-format tournament

Every variant ran two independent repetitions. A repetition continued for at least 30 seconds and completed at least two full 50-million-row queries. The client wrote response bytes directly to `/dev/null`; it did not decode them. Runs were shuffled with a fixed seed.

| Rank | Format and setting | Mean rows/s | Min–max rows/s | Wire MiB/s | Bytes/row |
|---:|---|---:|---:|---:|---:|
| 1 | Parquet, LZ4, 32 threads, 1M-row group | **11,303,330** | 11,226,514–11,380,146 | 86.2 | 7.99 |
| 2 | Parquet, ZSTD, 32 threads, 250k-row group | 11,099,082 | 11,081,944–11,116,220 | 49.7 | 4.69 |
| 3 | Parquet, ZSTD, 32 threads, 1M-row group | 11,043,881 | 10,996,945–11,090,817 | 42.2 | 4.01 |
| 4 | Parquet, LZ4, 16 threads, 1M-row group | **10,453,672** | 10,322,363–10,584,982 | 79.7 | 7.99 |
| 5 | Parquet, LZ4, 8 threads, 1M-row group | 7,107,949 | 7,106,967–7,108,931 | 54.2 | 7.99 |
| 6 | RowBinaryWithNamesAndTypes, 16 threads | **2,107,557** | 2,101,040–2,114,075 | 756.4 | 376.33 |
| 7 | RowBinaryWithNamesAndTypes, 8 threads | **2,090,547** | 2,064,936–2,116,158 | 750.3 | 376.33 |
| 8 | JSONCompactEachRowWithNamesAndTypes, 32 threads | 1,791,297 | 1,779,326–1,803,268 | 739.8 | 433.04 |
| 9 | ArrowStream, LZ4, 8 threads | **1,757,360** | 1,754,270–1,760,451 | 117.2 | 69.95 |
| 10 | ORC, ZSTD, 32 threads | 1,438,421 | 1,434,971–1,441,872 | 0.3 | 0.23 |
| 11 | ArrowStream, uncompressed, 8 threads | 1,376,636 | 1,374,383–1,378,888 | 588.5 | 448.24 |

The complete ranking and exact ClickHouse settings are in the [main tournament report](./clickhouse-read-20260828/REPORT.md) and [finalist report](./clickhouse-read-finalists-20260828/REPORT.md).

### Interpretation

Parquet wins the finite snapshot wire benchmark decisively, but a reader cannot emit the final row groups until it receives the file footer. It is therefore a strong finite-snapshot candidate, not an unbounded streaming transport. Decoder and end-to-end pipeline benchmarking remain a separate decision.

RowBinaryWithNamesAndTypes is the best streaming extraction format in this tournament. Raising its server thread ceiling from 8 to 16 improves throughput by only 0.8%, and 32 threads regress slightly. Eight threads are the resource-aware choice.

ArrowStream benefits from LZ4 frame compression, but additional server threads hurt this workload: eight threads beat 16 and 32. JSONCompact moves many bytes quickly but would require textual parsing and therefore is not a preferred production contract.

`FORMAT Native` was deliberately excluded. ClickHouse documents its persisted output as an internal representation that is not a stable interchange contract. Transferia's existing version-negotiated native TCP protocol is a different contract and remains eligible.

## Production native reader

The production benchmark transferred every source row through the release binary into the discard sink and required the source to report exactly 50,000,000 completed rows. Every completed variant ran twice. CPU is summed over the process group; RSS is the maximum sampled aggregate resident set.

| Rank | Variant | Mean rows/s | Min–max rows/s | CPU | Peak RSS | Rows/s/core |
|---:|---|---:|---:|---:|---:|---:|
| 1 | ZSTD, 16 threads | **883,053** | 881,515–884,592 | 98% | 1.15 GiB | 904,568 |
| 2 | LZ4, 16 threads | 825,592 | 819,890–831,293 | 86% | 0.26 GiB | **962,430** |
| 3 | LZ4, 32 threads | 823,580 | 821,358–825,801 | 86% | 0.37 GiB | 957,328 |
| 4 | LZ4, 8 threads | 820,604 | 819,973–821,234 | 87% | 0.27 GiB | 948,157 |
| 5 | Previous release baseline | 786,289 | 777,697–794,882 | 102% | 0.60 GiB | 768,890 |

An uncompressed native-protocol smoke produced only about 65,000 rows/s and 30 MiB/s. It was stopped after proving more than 12 times slower than LZ4; completing two approximately 13-minute passes would not have changed the selection.

The production source now:

- requests `preferred_block_size_bytes=0` and an explicit `max_block_size`, avoiding ClickHouse's approximately 1 MiB preferred result blocks for this wide schema;
- returns decoded server blocks directly instead of concatenating small Arrow batches and copying every array;
- exposes `max_threads`, compression, and maximum block rows as advanced source settings;
- counts decoded transport payload before synthetic system columns are added;
- defaults to ZSTD, 16 threads, and 65,409 maximum block rows for maximum measured throughput.

This native implementation remains available as an explicit compatibility profile. The measured default is now the substantially faster production Parquet reader below.

The full machine-readable production data is in [production-report.json](./clickhouse-native-production-20260828/production-report.json), with the compact table in [PRODUCTION_REPORT.md](./clickhouse-native-production-20260828/PRODUCTION_REPORT.md).

## Production Parquet reader

The production reader downloads one explicitly bounded finite Parquet response, reads its footer locally, and decodes independent row groups concurrently on blocking worker threads. The source emits ordinary Arrow `RecordBatch` values into the same connector-neutral pipeline used by every sink. The measurement below is ClickHouse source to the production discard sink in the release binary; every repetition completed and accounted for exactly 50,000,000 rows.

| Rank | Profile | n | Data-plane rows/s | Min–max rows/s | Whole-process rows/s | CPU | Peak RSS | Rows/s/core |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | ZSTD, 32 server threads, 250k row groups, 16 decoders | 3 | **11,130,528** | 11,043,195–11,229,250 | 9,680,633 | 259% | 2.77 GiB | 3,731,577 |
| 2 | ZSTD, 32 server threads, 250k row groups, 8 decoders | 3 | 9,626,312 | 9,537,069–9,756,861 | 8,544,320 | 216% | 1.45 GiB | **3,953,926** |

The 16-decoder profile is the throughput winner and the new default. Its mean is 0.28% above the corresponding wire-only mean and well inside the wire repetition spread, so no measurable extraction throughput is lost to footer parsing, Parquet decoding, Arrow construction, source metadata, or the discard pipeline. The apparent positive difference is ordinary run-to-run noise, not processing faster than the wire.

LZ4 with one-million-row groups remains configurable and retains the highest wire-only result. ZSTD/250k won the production design because it reaches the same ceiling with materially fewer compressed bytes per row and much lower decoder memory pressure than the aggressive LZ4 profiles. The broad production sweep showed that raising Rust decoders from 16 to 32 regressed throughput and increased RSS to 5–15 GiB depending on compression; it is deliberately not the default.

The reader is intended for finite snapshots whose compressed response fits the explicit `max_response_bytes` contract (2 GiB by default). It never follows HTTP redirects. Host failover occurs only before any batch is emitted, so a failed partial download cannot duplicate a partially delivered snapshot. The full precision data is in [production-report.json](./clickhouse-parquet-final-20260828/production-report.json), with raw logs and per-run JSON preserved beside it. The exploratory eight-profile sweep is in [clickhouse-parquet-production-20260828](./clickhouse-parquet-production-20260828/).

## Scope and limitations

- Results describe this ClickHouse version, two-data-host managed cluster, schema, network path, and benchmark host. The runners preserve all exact settings and raw per-run JSON for repetition.
- The wire tournament does not measure client decoding or Arrow conversion. It answers which stable server output moves the snapshot fastest.
- Both native TCP and Parquet were subsequently measured through their production Arrow readers. RowBinary remains only a wire control and is not a production connector mode.
- The 50-million-row source is repeated inside wire repetitions to meet the minimum measurement window. Production repetitions are separate full deliveries.
- No result relies on ClickHouse's unstable persisted `FORMAT Native` output.
