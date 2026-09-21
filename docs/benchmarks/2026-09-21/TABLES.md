# Detailed benchmark tables

Medians of verified runs. `n` = accepted repeats P1/P4. CPU efficiency counts the whole client cgroup; managed database hosts are outside this scope.

## PG-PG · narrow

| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| Transferia Rust Auto | 228 843 | 418 601 | 1.83× | 485 244 | 0.89 | 0.65 | 3/3 |
| Transferia Rust exact PK¹ | 230 831 | 416 301 | 1.80× | 518 107 | 0.85 | 0.67 | 3/3 |
| Sail JDBC + sink⁷ | 246 750 | 350 362 | 1.42× | 170 075 | 2.07 | 0.45 | 3/3 |
| Sail ADBC buffered + sink⁷ | 196 396 | 303 179 | 1.54× | 162 936 | 1.77 | 0.61 | 3/3 |
| Transferia Go · typed path | 108 924 | 168 555 | 1.55× | 74 783 | 2.25 | 3.55 | 3/3 |
| Sling² | 131 917 | 119 547 | 0.91× | 106 291 | 1.12 | 0.53 | 3/3 |
| Spark | 45 726 | 85 723 | 1.87× | 38 921 | 2.21 | 0.82 | 3/3 |
| DataX · default polling | 24 020 | 84 922 | 3.54× | 67 970 | 1.25 | 0.47 | 3/3 |
| Airbyte connectors³ | 82 809 | 82 474 | 1.00× | 22 686 | 3.60 | 1.51 | 3/3 |
| SeaTunnel | 36 022 | 76 269 | 2.12× | 34 352 | 2.22 | 1.30 | 3/3 |
| InLong Sort | 28 716 | 54 013 | 1.88× | 21 269 | 2.55 | 4.06 | 3/3 |
| Meltano² | 10 803 | 38 737 | 3.59× | 7 222 | 5.36 | 1.31 | 3/3 |
| Flink CDC | 20 631 | 33 352 | 1.62× | 13 053 | 2.70 | 4.68 | 3/3 |
| Estuary local preview² ³ | 9 451 | 32 727 | 3.46× | 12 125 | 2.71 | 0.39 | 3/3 |
| Sqoop import + export | 12 708 | 29 707 | 2.34× | 27 573 | 1.08 | 0.44 | 3/3 |
| Debezium bulk-tuned⁴ | 7 399 | 16 874 | 2.28× | 3 771 | 4.47 | 2.63 | 3/3 |
| Debezium + Kafka + JDBC | 3 787 | 11 002 | 2.91× | 3 037 | 3.62 | 2.77 | 3/3 |

## PG-PG · wide

| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| Transferia Rust Auto | 114 008 | 187 639 | 1.65× | 146 110 | 1.29 | 2.19 | 3/3 |
| Transferia Rust exact PK¹ | 104 489 | 179 595 | 1.72× | 90 040 | 1.91 | 2.22 | 3/3 |
| Sail JDBC + sink⁷ | 102 304 | 166 999 | 1.63× | 60 506 | 2.88 | 1.57 | 3/3 |
| Sail ADBC buffered + sink⁷ | 88 336 | 143 556 | 1.63× | 58 145 | 2.43 | 2.38 | 3/3 |
| Transferia Go · typed path | 96 618 | 126 551 | 1.31× | 54 096 | 2.36 | 4.65 | 3/3 |
| Sling² | 69 469 | 77 384 | 1.11× | 57 453 | 1.35 | 0.53 | 3/3 |
| Spark | 33 601 | 67 861 | 2.02× | 31 340 | 2.20 | 1.46 | 3/3 |
| SeaTunnel | 25 126 | 54 779 | 2.18× | 21 488 | 2.53 | 1.70 | 3/3 |
| DataX · default polling | 16 262 | 46 185 | 2.84× | 32 298 | 1.43 | 1.14 | 3/3 |
| InLong Sort | 20 476 | 43 467 | 2.12× | 17 420 | 2.45 | 4.18 | 3/3 |
| Airbyte connectors³ | 39 499 | 38 872 | 0.98× | 13 724 | 2.83 | 2.89 | 3/3 |
| Flink CDC | 17 117 | 29 166 | 1.70× | 9 346 | 3.15 | 5.45 | 3/3 |
| Meltano² | 8 242 | 27 708 | 3.36× | 6 290 | 4.42 | 1.86 | 3/3 |
| Sqoop import + export | 10 040 | 24 691 | 2.46× | 15 299 | 1.61 | 0.39 | 3/3 |
| Estuary local preview² ³ | 7 422 | 20 540 | 2.77× | 7 342 | 2.81 | 0.47 | 3/3 |
| Debezium bulk-tuned⁴ | 7 067 | 15 684 | 2.22× | 3 539 | 4.52 | 2.85 | 3/3 |
| Debezium + Kafka + JDBC | 3 083 | 9 251 | 3.00× | 2 644 | 3.44 | 2.87 | 3/3 |

## PG-PG · narrow10m

| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| Sail ADBC buffered + sink⁷ | — | 682 656 | — | 337 199 | 2.02 | 3.29 | 0/1 |
| Transferia Rust Auto | — | 669 893 | — | 485 115 | 1.38 | 1.99 | 0/1 |
| Transferia Rust exact PK¹ | — | 623 612 | — | 526 139 | 1.19 | 2.00 | 0/1 |
| Transferia Go · typed path | — | 418 130 | — | 98 491 | 4.25 | 9.45 | 0/1 |
| Sling² | — | 325 687 | — | 121 270 | 2.69 | 0.53 | 0/1 |
| Spark | 70 192 | 220 180 | 3.14× | 198 677 | 1.11 | 0.84 | 1/1 |
| SeaTunnel | — | 141 535 | — | 80 251 | 1.76 | 1.66 | 0/1 |
| InLong Sort | — | 133 388 | — | 107 054 | 1.25 | 4.21 | 0/1 |
| Airbyte connectors³ | — | 125 384 | — | 52 308 | 2.40 | 4.96 | 0/1 |
| DataX · default polling | — | 98 421 | — | 121 532 | 0.81 | 0.48 | 0/1 |
| Flink CDC · large heap18GiB⁵ | — | 90 719 | — | 23 507 | 3.86 | 16.90 | 0/1 |
| Meltano² | — | 43 807 | — | 7 686 | 5.70 | 1.68 | 0/1 |
| Sqoop import + export | — | 37 467 | — | 75 085 | 0.50 | 0.39 | 0/1 |
| Estuary local preview² ³ | — | 35 428 | — | 11 990 | 2.95 | 0.39 | 0/1 |
| Debezium bulk-tuned⁴ | — | 32 434 | — | 8 210 | 3.95 | 3.11 | 0/1 |
| Debezium + Kafka + JDBC | — | 15 811 | — | 5 870 | 2.69 | 2.86 | 0/1 |

## PG-CH · narrow

| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| Transferia Rust exact PK¹ | 438 164 | 530 230 | 1.21× | 530 194 | 1.00 | 0.51 | 3/3 |
| Transferia Rust Auto | 429 608 | 525 028 | 1.22× | 511 454 | 1.03 | 0.45 | 3/3 |
| Sail JDBC + sink⁷ | 292 182 | 498 488 | 1.71× | 192 460 | 2.53 | 0.43 | 3/3 |
| Sail ADBC buffered + sink⁷ | 266 422 | 408 868 | 1.53× | 178 295 | 2.30 | 0.51 | 3/3 |
| Transferia Go · typed path | 132 838 | 242 988 | 1.83× | 70 784 | 3.46 | 1.90 | 3/3 |
| Sling² | 144 966 | 134 883 | 0.93× | 99 300 | 1.37 | 0.71 | 3/3 |
| SeaTunnel | 65 685 | 107 743 | 1.64× | 29 193 | 3.80 | 1.68 | 3/3 |
| Spark | 77 929 | 96 405 | 1.24× | 35 430 | 2.81 | 0.86 | 3/3 |
| DataX · default polling | 85 949 | 85 995 | 1.00× | 45 444 | 1.89 | 1.11 | 3/3 |

## PG-CH · wide

| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| Transferia Rust Auto | 192 889 | 317 945 | 1.65× | 159 614 | 1.85 | 1.16 | 3/3 |
| Transferia Rust exact PK¹ | 210 686 | 303 865 | 1.44× | 137 199 | 2.21 | 1.31 | 3/3 |
| Sail JDBC + sink⁷ | 132 919 | 283 187 | 2.13× | 67 345 | 4.23 | 1.29 | 3/3 |
| Sail ADBC buffered + sink⁷ | 122 751 | 239 857 | 1.95× | 68 396 | 3.61 | 1.56 | 3/3 |
| Transferia Go · typed path | 102 038 | 196 125 | 1.92× | 49 991 | 3.92 | 4.48 | 3/3 |
| Sling² | 70 116 | 106 139 | 1.51× | 49 223 | 2.16 | 2.10 | 3/3 |
| DataX · default polling | 31 563 | 86 589 | 2.74× | 22 588 | 3.83 | 1.18 | 3/3 |
| SeaTunnel | 33 842 | 68 624 | 2.03× | 15 503 | 4.53 | 2.87 | 3/3 |
| Spark | 30 974 | 67 590 | 2.18× | 27 630 | 2.45 | 3.22 | 3/3 |

## PG-CH · narrow10m

| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| Sail ADBC buffered + sink⁷ | — | 1 547 492 | — | 400 888 | 3.86 | 2.16 | 0/1 |
| Transferia Rust exact PK¹ | — | 1 376 332 | — | 528 834 | 2.60 | 1.41 | 0/1 |
| Transferia Rust Auto | — | 1 278 093 | — | 535 274 | 2.39 | 1.34 | 0/1 |
| Spark | — | 460 854 | — | 214 429 | 2.15 | 1.11 | 0/1 |
| Sling² | — | 430 650 | — | 103 894 | 4.15 | 0.72 | 0/1 |
| Transferia Go · typed path | — | 421 211 | — | 73 433 | 5.74 | 2.13 | 0/1 |
| DataX · default polling | — | 315 976 | — | 50 672 | 6.24 | 1.16 | 0/1 |
| SeaTunnel | 116 628 | 311 904 | 2.67× | 64 215 | 4.86 | 1.99 | 1/1 |

## Debezium · 16 parts · narrow PG→PG

Separate scaling check; not ranked against competitors using four parts.

| Variant | Rows/s | Rows/CPU-s | Mean CPUs | Peak RSS GiB | n |
|---|---:|---:|---:|---:|---:|
| Debezium + Kafka + JDBC | 18 484 | 3 048 | 5.99 | 3.05 | 3 |
| Debezium bulk-tuned⁴ | 18 381 | 3 180 | 5.75 | 2.83 | 3 |

¹ Benchmark-only exact-PK override. ² External parallel pipelines. ³ Local connector/preview path, not the full managed platform. ⁴ Separate Kafka buffering/compression configuration. ⁵ Flink 10M uses TaskManager 18 GiB / managed fraction 0.1 instead of 8 GiB; total cgroup remains 24 GiB.

⁷ Sail uses the explicit benchmark Arrow sink and common TLS proxy. It is a later comparison block; fresh interleaved Rust/Spark controls are in SAIL.md. See `summary.csv` for all medians/min/max, including P1 memory, CPU time, cgroup peak memory and wall time. Min/max are descriptive, not confidence intervals.

## Failed measured runs

| Run | Outcome | Elapsed s | Comparison |
|---|---|---:|---|
| debezium_bulk_narrow10m_p1_r1_1789972467 | configured run deadline exceeded | 600.09 | Current failed case |
| debezium_bulk_narrow_p16_r1_1789958506 | odyssey: c4f7f6a2f4a7d: too many active clients for user (pool_size for user db1.user1 reached 50) | 22.75 | Superseded; reason below |
| debezium_narrow_p16_r1_1789958468 | odyssey: ce7656a595669: too many active clients for user (pool_size for user db1.user1 reached 50) | 21.75 | Superseded; reason below |
| debezium_narrow_p16_r2_1789958560 | odyssey: cf5cdeb3c4978: too many active clients for user (pool_size for user db1.user1 reached 50) | 19.59 | Superseded; reason below |
| flink_narrow10m_p4_r1_1789970552 | engine exit code 143 | 416.34 | Superseded; reason below |
| go_narrow_p1_r1_1789959507_ch | engine exit code 125 | 513.51 | Superseded; reason below |
| seatunnel_wide_p4_r1_1789960407_ch | engine exit code 1 | 18.95 | Superseded; reason below |
| sqoop_narrow10m_p1_r1_1789971620 | configured run deadline exceeded | 600.09 | Current failed case |
| sail_jdbc_narrow10m_p4_r0_1790000266 | configured run deadline exceeded | 600.10 | Current failed case |
| sail_jdbc_narrow10m_p4_r0_1790000870_ch | configured run deadline exceeded | 600.10 | Current failed case |

## Superseded measured runs

| Run | Reason |
|---|---|
| airbyte_narrow_p4_r1_1789955779 | Superseded adapter: native part count not established; rerun with exact CTID split and explicit merged-stream checkpoint accounting. |
| debezium_bulk_narrow_p16_r1_1789958506 | Preflight with 50-connection user pool or during its update; rerun after both PG user limits are stably 200. |
| debezium_bulk_narrow_p16_r2_1789958639 | Preflight with 50-connection user pool or during its update; rerun after both PG user limits are stably 200. |
| debezium_narrow_p16_r1_1789958468 | Preflight with 50-connection user pool or during its update; rerun after both PG user limits are stably 200. |
| debezium_narrow_p16_r2_1789958560 | Preflight with 50-connection user pool or during its update; rerun after both PG user limits are stably 200. |
| debezium_narrow_p1_r1_1789955468 | superseded completion probe: full COUNT scan each second replaced with cheap insertion counter; rerun required |
| flink_narrow10m_p4_r1_1789970552 | Large-table TaskManager Java heap OOM at 8-GiB process size; preserved failed attempt, retry uses 18-GiB TM / managed fraction 0.1 within unchanged 24-GiB cgroup. |
| go_narrow_p1_r1_1789959507_ch | Harness configuration error: BufferTriggingSize is bytes, not rows; 64 KiB batches plus default 1-second Interval throttled the transfer. Aborted and rerun with 65536-row trigger, 256 MiB byte ceiling and disabled interval throttle. |
| inlong_narrow_p1_r1_1789955811 | Superseded JDBC auto-commit default; rerun with scan.auto-commit=false so fetch-size=8192 uses a bounded PostgreSQL cursor. |
| inlong_narrow_p4_r1_1789956401 | Superseded JDBC auto-commit default; rerun with scan.auto-commit=false so fetch-size=8192 uses a bounded PostgreSQL cursor. |
| inlong_wide_p1_r1_1789957195 | Superseded JDBC auto-commit default; rerun with scan.auto-commit=false so fetch-size=8192 uses a bounded PostgreSQL cursor. |
| inlong_wide_p4_r1_1789957784 | Superseded JDBC auto-commit default; rerun with scan.auto-commit=false so fetch-size=8192 uses a bounded PostgreSQL cursor. |
| seatunnel_narrow_p1_r1_1789956922 | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_narrow_p1_r1_1789960252_ch | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_narrow_p4_r1_1789956956 | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_narrow_p4_r1_1789960426_ch | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_narrow_p4_r2_1789960513 | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_wide_p1_r1_1789957437 | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_wide_p1_r1_1789960185_ch | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_wide_p4_r1_1789957577 | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
| seatunnel_wide_p4_r1_1789960407_ch | SeaTunnel local launcher overrides JAVA_TOOL_OPTIONS with 512 MiB heap; wide P4 CH exhausted it. All cases rerun with explicit -DJvmOption=-Xmx4g within unchanged 24 GiB cgroup. |
