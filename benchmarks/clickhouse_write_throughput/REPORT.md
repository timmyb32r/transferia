# ClickHouse sink throughput

Date: 2026-08-29

Benchmark host: `timmyb32r-dev4`

Destination: two-data-host managed ClickHouse cluster, `ReplicatedMergeTree`

## Method

Every measurement used the release data plane, one logical Transferia
partition, the same YTsaurus `PartitionTables` source, the same transfer-log
schema and the production ClickHouse sink. Screening windows used a ten-second
warm-up followed by thirty measured seconds. The selected profile then ran in
two independent 45-second precision windows, each adjacent to a production
source-to-discard ceiling measurement. All persisted precision data passed the
full value and four-column primary-key verification; no run retried an INSERT.

`native` below means ClickHouse's version-negotiated TCP protocol on port 9440:
the client sends typed columnar `Block` packets. It does not mean the persisted
ClickHouse `FORMAT Native` file representation. Parquet and ArrowStream are
stable, self-describing HTTP request bodies parsed by ClickHouse.

## Stable format screening

| Rank | Production INSERT transport | Sink rows/s | Source rows/s | Process CPU | Peak RSS |
|---:|---|---:|---:|---:|---:|
| 1 | native TCP, ZSTD, concurrency 32, 640 MiB target | **6,185,240** | 6,351,418 | 565% | 8.80 GiB |
| 2 | native TCP, LZ4, concurrency 32, 640 MiB target | 5,599,113 | 5,657,274 | 513% | 7.60 GiB |
| 3 | ArrowStream, LZ4, concurrency 8 | 5,578,662 | 6,165,030 | 438% | 18.70 GiB |
| 4 | ArrowStream, ZSTD, concurrency 8 | 5,162,454 | 5,322,441 | 467% | 9.50 GiB |
| 5 | Parquet, ZSTD, concurrency 8, 250k-row groups | 4,725,733 | 4,903,496 | 554% | 20.20 GiB |
| 6 | Parquet, LZ4, concurrency 8, 1M-row groups | 4,064,513 | 4,082,611 | 475% | 21.70 GiB |
| 7 | Parquet, ZSTD, concurrency 4, 250k-row groups | 2,424,070 | 2,411,431 | 311% | 20.70 GiB |
| 8 | Parquet, LZ4, concurrency 4, 1M-row groups | 1,969,850 | 1,972,888 | 285% | 24.70 GiB |

Parquet dominates ClickHouse snapshot extraction because the server performs a
highly parallel columnar encode and the Rust source decodes row groups in
parallel. It does not dominate insertion: the sink already owns Arrow columns,
so Parquet adds both an Arrow-to-Parquet client encode and a Parquet-to-Block
server decode. Native TCP serializes those columns directly into protocol
blocks. ArrowStream avoids part of Parquet's work but still pays an extra stable
format encode/decode boundary.

## Precision result

The selected native-ZSTD profile averaged **5,752,430 rows/s** over three
precision repetitions (5,412,911–6,083,924). Source arrival averaged 5,826,546
rows/s, so ClickHouse retained **98.7%** of the immediately available source
throughput. The adjacent production-decode discard ceiling averaged 8,028,926
rows/s. Total process CPU averaged 535%, peak RSS was 12.13 GiB, and INSERT
retries remained zero.

The old single-INSERT LZ4 profile measured 496,869 rows/s in the same fresh
tournament. The precision winner is therefore **11.92x faster** than the old
default. The production winner is **11.58x faster** than that old default in the
three-run precision comparison.

## Native concurrency knee

| Native TCP profile | Sink rows/s | Source rows/s | Process CPU | Peak RSS |
|---|---:|---:|---:|---:|
| ZSTD, concurrency 32 | **6,185,374** | 6,407,018 | 565% | 10.7 GiB |
| ZSTD, concurrency 16 | 5,751,619 | 5,928,706 | 556% | 11.2 GiB |
| ZSTD, concurrency 24 | 5,732,673 | 5,831,644 | 529% | 10.0 GiB |
| ZSTD, concurrency 8 | 4,879,806 | 5,052,513 | 467% | 23.4 GiB |
| uncompressed, concurrency 32 | 2,632,155 | 2,493,725 | 399% | 20.3 GiB |
| uncompressed, concurrency 24 | 1,698,877 | 1,698,612 | 313% | 22.6 GiB |
| uncompressed, concurrency 16 | 1,489,344 | 1,480,285 | 237% | 19.3 GiB |

Concurrency 32 is 7.5% faster than concurrency 16 and did not increase client
CPU or RSS in the matched screening window. That is a material throughput gain,
not a one-to-five-percent resource-for-speed trade. Disabling compression is
2.3x slower even at concurrency 32 and consumes much more memory. The selected
default is therefore native TCP, ZSTD, one-million-row/640-MiB batching targets,
and concurrency 32. The pipeline's configured memory budget remains the hard
backpressure boundary; the sink cannot unboundedly fill all 32 slots when the
delivery grants less memory.

## Read-path cross-check

A fresh current-release source-to-discard comparison on the same 50-million-row
ClickHouse table confirms that the write-path result must not be projected onto
reading:

| Production reader | Data-plane rows/s | Min–max | CPU | Peak RSS | Rows/s/core |
|---|---:|---:|---:|---:|---:|
| Parquet ZSTD, 32 server threads, 250k row groups, 16 decoders | **11,107,089** | 11,095,080–11,119,097 | 255% | 2.72 GiB | 3,668,450 |
| native TCP ZSTD, 16 threads | 887,448 | 878,075–896,822 | 98% | 1.41 GiB | 895,138 |

The production Parquet snapshot reader is 12.52x faster by rows/s and 4.10x
faster by rows/s/core. Reading benefits from ClickHouse's parallel Parquet
encoder and Transferia's parallel row-group decoder. Writing starts with Arrow
already materialized, so native TCP avoids the extra Parquet encode/decode pair.
