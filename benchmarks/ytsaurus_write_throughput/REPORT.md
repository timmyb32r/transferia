# YTsaurus static-table write benchmark

Date: 2026-08-29. All measurements used the release build on the same benchmark
host and test cluster. The source was the built-in transfer-log generator and
the destination was a unique static table. Every measured scan completed, its
exact row count was verified through YTsaurus, and only the run-owned path was
then removed. Each reported candidate accumulated at least 120 seconds of
completed scans; the selected result was repeated five times.

## Result

The selected implementation buffers 512 MiB of Arrow input, opens an official
YTsaurus v4 distributed-write session, uploads four contiguous fragments in
parallel, and finishes the session with fragment results in source order. The
server performs the final metadata attachment. A failed fragment therefore does
not advance the destination, and source deliveries are committed only after the
finish request succeeds.

The desired YTsaurus chunk size remains 2 GiB. Reducing it to 512 MiB changed
throughput by only 0.07% in the screen while creating roughly four times as many
chunks, so that candidate was rejected.

| Implementation | Median rows/s | Min | Max | CPU | Rows/core-s | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| Original trunk, one request per Arrow batch | 14,890 | 14,890 | 14,890 | 2% | 666,667 | 0.67 GiB |
| Batched 256 MiB, sequential write | 59,147 | 57,751 | 62,113 | 8% | 767,754 | 1.80 GiB |
| Distributed c4, 512 MiB, 2 GiB chunks | 97,904 | 89,066 | 108,714 | 12% | 854,179 | 1.92 GiB |

The selected implementation is **6.58× faster** than the original trunk and
**1.66× faster** than sequential 256 MiB batching. The five winner repetitions
processed 12–14 million rows each across six or seven complete scans. Their
rows/s values were 97,904, 108,714, 105,057, 89,066, and 94,978; the spread is
why YTsaurus uses five repetitions rather than the shorter stable-storage gate.

## Primary-key snapshot finalization

After the throughput tournament, the default semantics for a schema carrying a
primary key were strengthened: Transferia writes the complete finite snapshot
to an attempt-owned unsorted staging table, starts a server-side sort by every
primary-key column into a `unique_keys=true` table, and atomically moves that
table over the destination. No source progress is committed before the sort and
move finish. Duplicate keys therefore fail the YTsaurus sort instead of being
silently reduced or dropped. `preserve_rows` remains an explicit non-default
unsorted mode.

This mode intentionally accepts only one finite source partition. Letting each
partition independently replace the destination would lose all other
partitions; the discovery contract now rejects that topology before destination
preparation.

A release smoke on Hume generated 10,000 transfer-log rows and verified the
exact final row count, `@sorted=true`, and `@schema/@unique_keys=true` before
removing its owned table. It completed in about 30 seconds (334 rows/s), almost
all of which was fixed scheduler/startup latency for the separate sort
operation. This tiny-table number is not comparable to the sustained writer
throughput above. Large-snapshot end-to-end throughput includes the additional
server-side sort; the 97,904 rows/s result measures the optimized ingestion
stage itself.

## Distributed-write screening

Each screening row is one independent 120-second measurement. The final winner
is reported separately above using five repetitions.

| Candidate | Rows/s | CPU | Rows/core-s | Peak RSS |
|---|---:|---:|---:|---:|
| c4, target 512 MiB, chunk 2 GiB | 88,665 | 11% | 816,882 | 1.53 GiB |
| c4, target 256 MiB, chunk 512 MiB | 78,544 | 10% | 758,150 | 1.27 GiB |
| c4, target 256 MiB, chunk 2 GiB | 78,491 | 10% | 771,605 | 1.24 GiB |
| c8, target 256 MiB, chunk 2 GiB | 65,494 | 8% | 822,199 | 1.69 GiB |
| c2, target 256 MiB, chunk 2 GiB | 64,501 | 8% | 813,008 | 1.20 GiB |
| c16, target 256 MiB, chunk 2 GiB | 58,296 | 7% | 821,355 | 1.94 GiB |
| c4, target 128 MiB, chunk 2 GiB | 48,959 | 6% | 796,813 | 1.04 GiB |

Concurrency above four was rejected: it was slower and used more memory. Four
writers materially outperformed two writers, while the 512 MiB target provided
a useful gain over 256 MiB without approaching the configured 1 GiB pipeline
memory limit with buffered Arrow data. The higher process RSS includes runtime,
Arrow, allocator, and transport memory in addition to the leased pipeline data.

## Reproduction

Copy `config.example.yaml` outside the repository, supply a test-only cluster,
root and credential file, and run `runner.py`. The runner stores immutable raw
logs, private generated delivery configurations, binary/configuration SHA-256
provenance, per-repetition JSON, and a generated Markdown summary below the
configured result directory.
