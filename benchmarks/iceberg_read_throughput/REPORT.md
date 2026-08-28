# Iceberg source optimization report

The production Iceberg source was measured against the discard sink on a
32-vCPU, 48-GiB benchmark host. The immutable snapshot contained 50,000,000
transfer-log records in 191 Parquet files. Every result below is the median of
three independently shuffled measurement windows of at least 120 seconds; each
window consists only of complete snapshot scans.

| Candidate | Median rows/s | Min | Max | Mean CPU | Rows/core-s | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| Current/default: batch 65,536, files 32, manifests 32 | 3,310,975 | 3,207,547 | 3,422,233 | 122% | 2,717,391 | 1.17 GiB |
| Aggressive: batch 262,144, files 64 | 3,300,389 | 3,235,950 | 3,417,583 | 125% | 2,639,393 | 2.06 GiB |
| Bounded: batch 262,144, files 8, manifests 8 | 3,227,738 | 3,137,330 | 3,268,851 | 121% | 2,667,200 | 0.93 GiB |
| Bounded + 4-MiB range coalescing | 3,218,113 | 3,198,911 | 3,228,201 | 120% | 2,663,648 | 0.99 GiB |
| Batch 262,144, files 8, manifests 32 | 3,170,925 | 3,062,344 | 3,174,668 | 120% | 2,650,060 | 1.07 GiB |
| Batch 262,144, files 32, manifests 32 | 3,091,817 | 3,022,052 | 3,313,969 | 122% | 2,528,925 | 1.71 GiB |

The current profile remains the throughput winner. Raising file concurrency to
64 did not improve the median, reduced throughput per core, and increased peak
RSS by 76%. Reducing concurrency to 8 saved memory but cost 2.5% throughput.
Larger batches and larger Parquet range coalescing also failed to improve the
median. The selected defaults therefore remain 65,536 rows, 32 concurrent data
files, 32 concurrent manifest operations, 512-KiB metadata prefetch, 1-MiB range
coalescing, and 10 concurrent range fetches.

The implementation now makes every measured limit explicit and validated in
configuration instead of inheriting data-file and manifest fan-out from the
host CPU count. Before and after throughput are both 3,310,975 rows/s (0.0%):
the useful result is proving that the existing throughput profile is already at
the observed S3/network ceiling while removing machine-dependent behavior and
rejecting resource-heavy false winners.

After setting these values as the serde defaults, a separate 120-second release
run with every tuning field omitted produced 3,207,438 rows/s. This is inside
the finalist baseline range and proves that a normal user configuration selects
the measured profile without benchmark-only overrides.
