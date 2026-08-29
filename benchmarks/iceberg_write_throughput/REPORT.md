# Iceberg sink throughput

Date: 2026-08-29

Benchmark host: `timmyb32r-dev4`

Storage: Iceberg REST catalog backed by the dedicated benchmark S3 bucket

## Method

Every measurement used the release data plane, one logical Transferia
partition, the same 50-million-row ClickHouse transfer-log table, and the
production Iceberg sink. Screening windows used a ten-second warm-up followed
by at least thirty measured seconds. The sink committed every buffered delivery
group atomically with Iceberg `fast_append`; no benchmark result below includes
a retry or a failed upload.

The old sink encoded and uploaded one Parquet stream at a time. The optimized
sink keeps the same atomic commit boundary, distributes buffered Arrow batches
across bounded writer shards, encodes and uploads those shards concurrently,
then commits all resulting data files in one transaction. The 512-MiB commit
target is independent of writer concurrency, so increasing concurrency cannot
silently multiply the required pipeline memory budget.

## Results

| Production writer | Sink rows/s | Process CPU | Peak RSS |
|---|---:|---:|---:|
| ZSTD, concurrency 8, 250k-row groups | **999,312** | 145% | 1.94 GiB |
| LZ4, concurrency 8, 1M-row groups, mean of two runs | 963,305 | 141% | 2.25 GiB |
| ZSTD, concurrency 4, 250k-row groups | 543,900 | 87% | 1.30 GiB |
| LZ4, concurrency 4, 1M-row groups | 505,900 | 79% | 1.40 GiB |
| ZSTD, concurrency 1, 250k-row groups | 155,000 | — | — |
| Uncompressed, concurrency 1, 1M-row groups | 148,600 | — | — |
| Old trunk writer | 99,888 | — | — |

The two valid independent LZ4/concurrency-8 runs measured 858,555 and
1,068,055 rows/s. The adjacent ZSTD/concurrency-8 run measured 999,312 rows/s.
Their difference is inside five percent of the combined observations, while
ZSTD used less peak memory and produces materially smaller persisted data.
Following the resource-aware selection rule, ZSTD is therefore the default;
LZ4 is not selected for a marginal and noisy throughput advantage.

The production default is ZSTD, eight concurrent writers, 250,000 rows per
Parquet row group, 128-MiB rolling files, and a 512-MiB commit target. The
measured before/after improvement is **10.0x** (99,888 to 999,312 rows/s).

## 120-second reproducibility gate

The internal versioned bucket reached its service-wide quota before a long
precision series could finish. To keep the two-minute acceptance gate
reproducible rather than shortening it, the selected implementation was also
measured against a local pinned MinIO server and the same pinned Iceberg REST
catalog used by the project tests. Both profiles used the same release binary,
transfer-log generator, REST catalog, storage, 30-second warm-up, and exactly
120 one-second production stats samples.

| Local-storage profile | Window | Sink rows/s | Min–max | CPU | Peak RSS | Retries |
|---|---:|---:|---:|---:|---:|---:|
| Previous one-writer shape, ZSTD | 120 s | 941,092 | 0–1,898,524 | 99% | 1.40 GiB | 0 |
| Selected c8 writer, ZSTD | 120 s | **1,478,953** | 945,680–1,899,808 | 205% | 1.90 GiB | 0 |

The c8 writer is **1.57x** faster than the one-writer shape on identical local
storage and retains 100.0% of the generator throughput. This local result is a
stability and concurrency-isolation gate; it does not replace the absolute
internal-S3 before/after numbers above. The pinned MinIO image digest, binary
SHA-256, and machine-readable aggregates are stored in
`results/precision-120s.json`.

## External precision limitation

The planned second and third ZSTD precision repetitions could not persist new
files after the test Object Storage service reached its total-size quota.
Iceberg catalog purge removed table metadata, but the versioned bucket retained
6,157 non-current objects totalling 2.94 GB; those versions continued to count
towards the service quota. Failed quota runs are excluded from every number in
this report. This limitation affects the final repetition count, not the
production implementation or the valid measurements above.

`../windowed_sink_throughput.py` and `config.example.yaml` preserve the exact
fixed-window method for future reruns with a private credential-bearing delivery
template.
