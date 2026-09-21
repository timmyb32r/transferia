# Configuration diagnostics

These repetition=0 runs are deliberately excluded from the primary rankings. They use the same server, immutable fixtures, resource budget and all-cell validation. Most cases use P4; Go homogeneous mode explicitly includes P1 and P4.

## pgJDBC prepareThreshold: 0 versus 5

Randomized diagnostic block, narrow 1M fixture. The threshold changes in both source and destination URLs. This does not isolate their individual contributions.

| Engine | Threshold | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |
|---|---:|---:|---:|---:|---:|
| datax | 0 | 3 | 86,909 | 68,938 | 0.48 |
| datax | 5 | 3 | 86,881 | 71,695 | 0.47 |
| spark | 0 | 3 | 85,702 | 38,459 | 0.81 |
| spark | 5 | 3 | 89,226 | 38,400 | 0.83 |

datax: threshold 5 / threshold 0 throughput = **1.000×**.
spark: threshold 5 / threshold 0 throughput = **1.041×**.

Three repeats give descriptive evidence only; the roughly 4% Spark difference is not a demonstrated statistical or universal improvement.

## Estuary local preview: delta_updates

Exploratory comparison with the standard materialization series. The delta runs were later in a separate block, so temporal conditions are not paired. This is full-row insertion into an empty PK table; changing-source reductions/update semantics are outside scope. It is not managed Flow performance.

| Dataset | Path | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |
|---|---|---:|---:|---:|---:|
| narrow | Standard keyed | 3 | 32,727 | 12,125 | 0.39 |
| narrow | delta_updates=true | 3 | 55,690 | 15,678 | 0.37 |
| wide | Standard keyed | 3 | 20,540 | 7,342 | 0.47 |
| wide | delta_updates=true | 3 | 37,965 | 9,986 | 0.43 |

narrow: delta / standard throughput = **1.702×**.
wide: delta / standard throughput = **1.848×**.

## DataX scheduler completion polling

The upstream default checks job completion every 10000 ms. This diagnostic sets only core.container.job.sleepInterval=100. The data path, batch settings, P4 and JVM remain unchanged. Main-series defaults are retained; this later diagnostic block is not temporally paired.

| Route | Dataset | Poll ms | Verified n | Rows/s | CPU seconds | Wall seconds |
|---|---|---:|---:|---:|---:|---:|
| pg-pg | narrow | 10000 | 3 | 84,922 | 14.71 | 11.78 |
| pg-pg | narrow | 100 | 3 | 87,598 | 14.57 | 11.42 |
| pg-pg | wide | 10000 | 3 | 46,185 | 30.96 | 21.65 |
| pg-pg | wide | 100 | 3 | 59,302 | 31.51 | 16.86 |
| pg-ch | narrow | 10000 | 3 | 85,995 | 22.00 | 11.63 |
| pg-ch | narrow | 100 | 3 | 213,687 | 22.42 | 4.68 |
| pg-ch | wide | 10000 | 3 | 86,589 | 44.27 | 11.55 |
| pg-ch | wide | 100 | 3 | 106,802 | 43.64 | 9.36 |

A shorter final polling delay improves finite-job latency, not the underlying steady-state row-transfer rate. No fixed ten seconds are subtracted from the original observations.

## Transferia Go: PostgreSQL homogeneous mode

The primary series deliberately uses NoHomo=true for the generic typed path. This diagnostic sets NoHomo=false on PG→PG; the native provider selects its homogeneous representation. Binary source result format, sharding, rename, destination constraints and validation stay the same. Later separate block, not temporally paired.

| Dataset | Parts | NoHomo | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |
|---|---:|---|---:|---:|---:|---:|
| narrow | 1 | true: main typed path | 3 | 108,924 | 78,615 | 2.71 |
| narrow | 1 | false: homogeneous | 3 | 112,379 | 73,533 | 2.08 |
| narrow | 4 | true: main typed path | 3 | 168,555 | 74,783 | 3.55 |
| narrow | 4 | false: homogeneous | 3 | 191,742 | 78,803 | 3.33 |
| wide | 1 | true: main typed path | 3 | 96,618 | 68,364 | 1.90 |
| wide | 1 | false: homogeneous | 3 | 95,811 | 68,748 | 2.42 |
| wide | 4 | true: main typed path | 3 | 126,551 | 54,096 | 4.65 |
| wide | 4 | false: homogeneous | 3 | 139,324 | 60,699 | 4.25 |

## JDBC writer batch: 1000 versus 8192 rows

PG→PG, P4. DataX controls come from its 100-ms completion-poll diagnostic; Spark controls come from the main series. Same driver, JVM and source settings. Later blocks are not temporally paired.

| Tool | Dataset | Writer rows | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |
|---|---|---:|---:|---:|---:|---:|
| datax | narrow | 1000 | 3 | 87,598 | 68,645 | 0.47 |
| datax | narrow | 8192 | 3 | 122,031 | 59,936 | 0.49 |
| datax | wide | 1000 | 3 | 59,302 | 31,731 | 1.13 |
| datax | wide | 8192 | 3 | 65,402 | 22,404 | 1.12 |
| spark | narrow | 1000 | 3 | 85,723 | 38,921 | 0.82 |
| spark | narrow | 8192 | 3 | 93,496 | 37,817 | 0.82 |
| spark | wide | 1000 | 3 | 67,861 | 31,340 | 1.46 |
| spark | wide | 8192 | 3 | 70,403 | 28,268 | 2.60 |

This diagnostic is not an exhaustive search for an optimal batch size. PostgreSQL commit cadence still differs between DataX and Spark.

## Flink CDC: immutable-source backfill skip

Single 10M/P4 diagnostic, not a repeated ranking. Both paths use TaskManager 18 GiB / managed fraction 0.1 within the same 24-GiB cgroup. Only scan.incremental.snapshot.backfill.skip changes. Skipping backfill changes guarantees on a mutating source; this is not a production default recommendation.

| Path | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |
|---|---:|---:|---:|---:|
| Buffered backfill, primary scale | 1 | 90,719 | 23,507 | 16.90 |
| Skip backfill, diagnostic | 1 | 108,102 | 66,163 | 11.60 |

The original 8-GiB TaskManager attempt failed with Java heap OOM and is retained separately; it is not assigned a throughput score.

