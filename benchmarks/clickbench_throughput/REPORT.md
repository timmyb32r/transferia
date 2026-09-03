# ClickBench connector throughput tournament

Status: **bounded exact-prefix tournament complete for all six connectors
(2026-09-03)**. ClickHouse, PostgreSQL, MySQL, Apache Iceberg, and YTsaurus were
measured from the first 2,000,000 complete records of the supplied `hits.csv`.
OpenSearch used a 500,000-row immutable subset because its source representation
is an opaque JSON envelope. Iceberg used an ephemeral local REST catalog and
S3-compatible store. YTsaurus used ten explicitly authorized, run-unique scratch
paths on a shared test cluster; they were retained because the benchmark runner
has no ownership-safe generic deletion path. The earlier synthetic-profile
tournament is retained below only as qualified tuning history.

The tournament measures source and destination throughput for YTsaurus,
ClickHouse, Apache Iceberg, PostgreSQL, MySQL, and OpenSearch using the
105-column ClickBench `hits` workload. The exact input and logical schema are
recorded in [PROVENANCE.md](PROVENANCE.md).

## Methodology

Measure each connector role independently before running any end-to-end pair:

- Source ceiling: an immutable native fixture to the discard destination.
- Destination ceiling: the ClickBench generator to an exclusively owned
  scratch destination.
- End-to-end confirmation: the exact fixture through the selected source and
  destination defaults, when a pair is needed to explain an interaction.

The destination ceiling replays a bounded empirical Arrow sample obtained from
the exact-prefix ClickHouse source. A separate finite source-to-destination run
verifies persisted row count, full primary-key uniqueness, temporal ranges, and
representative binary byte totals. Speedtest throughput and persisted-integrity
checks are distinct evidence and are reported as such.

For every candidate:

1. Start from the user-visible default configuration.
2. Change one bounded parameter group at a time.
3. Record rows/s, request-wide mean process CPU cores, peak process RSS,
   retries, and service rejection or throttling counters. Do not derive
   role-level rows/core from CPU sampled across a different time boundary.
4. Confirm a finalist with at least two independent windows after warm-up;
   use at least three when variance is material or the service is shared.
5. Reject a small gain when it requires disproportionate requests,
   concurrency, memory, retries, or service load.
6. Repeat the selected profile with all tuning fields omitted to prove that
   ordinary defaults materialize the measured configuration.

Speedtest's measured window excludes fixture creation, table/index creation,
schema discovery, profiling, cleanup, and result serialization. Report the
window duration, host CPU count, host memory, connector concurrency, batch or
page size, dataset row count, and fixture layout alongside every raw result.

YTsaurus and Iceberg destination measurements use a deliberately different,
explicit finite-snapshot metric because Iceberg Speedtest isolation is
fail-closed and the shared YTsaurus runner cannot safely delete arbitrary
tables: `2,000,000 / complete process wall time`. Their wall time includes
configuration/discovery, destination preparation, source reading, sink writes,
flush/finalization, and process shutdown. These figures are not comparable to a
pure Speedtest destination ceiling; they compare only same-connector tuning
arms with identical fixture/setup semantics. Process CPU and RSS cover the same
complete interval.

## Safety and validity gates

An exact-prefix measured row is publishable only after all applicable gates pass:

- The selected prefix digest, exact byte size, exact row count, and schema
  fingerprint are recorded. Full-file claims additionally require a complete
  full-file row count, which this bounded tournament does not make.
- CSV parsing fails on malformed quoting, a field count other than 105,
  numeric overflow, or invalid temporal syntax. No row may be skipped.
- All 105 columns are non-nullable and preserve the Arrow types in the
  provenance document.
- `EventTime`, `ClientEventTime`, and `LocalEventTime` remain timezone-free
  timestamps with second precision. A physical microsecond representation is
  valid only after checked widening and exact reverse validation.
- `EventDate` remains a temporal date, not formatted text.
- Binary columns remain byte-preserving. No UTF-8 replacement, trimming, or
  normalization is allowed.
- The complete primary key is checked for duplicates before destination
  commit. A hash alone is not proof of equality.
- Every fixture is immutable during source measurements.
- Every destination uses a run-specific scratch namespace with explicit
  ownership proof. Cleanup may remove only that namespace.
- Credentials come from the process environment or secret storage and never
  appear in configuration snapshots, logs, manifests, or result files.
- Concurrency, memory, request rate, and retry behavior remain bounded. A
  service rejection or sustained retry storm invalidates the candidate.
- Destination row count and lossless temporal/binary probes match the input
  before the result is accepted.

`scripts/analyze_clickbench_csv.py` is a bounded distribution profiler, not a
fixture validator: it samples byte windows and ignores rows whose sampled
field count is not 105. It must not be used as evidence that the complete file
passed the gates above.

## Known schema limitation

The official ClickBench primary-key order is:

```text
CounterID, EventDate, UserID, EventTime, WatchID
```

The current connector-neutral schema marks primary-key membership with a
boolean but does not represent key ordinal. The generator therefore exposes
the same key set in physical field order:

```text
WatchID, EventTime, EventDate, CounterID, UserID
```

The two orders identify the same logical row, but they can produce different
sorting, clustering, index locality, and throughput. Until key ordinal is
represented explicitly, every result must record the actual physical key
order. Results that compare sorted or indexed layouts with different key
orders are provisional and must not drive a default change.

Every typed exact-prefix fixture and destination used the same actual key-member
order, `WatchID, EventTime, EventDate, CounterID, UserID`; this was read back
from ClickHouse, PostgreSQL, and MySQL metadata. OpenSearch destination encoded
those members in that order into its sole physical `_id`; OpenSearch source
exposed the physical composite identity `_id, _routing_key`. Candidate decisions
compare only runs with the same physical key layout. The per-role values are
recorded in `exact-prefix-summary.json`.

## Exact-prefix measured results

The source ceiling is the timed second pass of Speedtest. Source CPU/RSS covers
the complete two-pass request and is reported only as a request-wide resource
envelope, not as source-stage efficiency. The destination
ceiling replays the exact empirical sample until cancellation and then waits for
connector quiescence; each result's actual `duration_ms`, not the requested
15-second ceiling, is used for rows/s. CPU counts only the transfer process, not
the local database containers.

| Connector | Role | Baseline | Baseline rows/s | Selected | Selected rows/s | Request CPU cores | Peak RSS | Evidence |
|---|---|---|---:|---|---:|---:|---:|---|
| ClickHouse | Source | batch 65,409 | 650,048 | unchanged | 650,048 | 1.071 | 5.64 GiB | 2 runs; batch 262K = 642,281 |
| ClickHouse | Destination | concurrency 32 | 945,157 | unchanged | 945,157 | 2.055 | 6.12 GiB | 7 runs per arm; C16 = 950,754 (+0.6%, order-sensitive noise) |
| YTsaurus | Source | ordered, batch 65,536 | 391,360 | unchanged | 391,360 | 1.495 | 2.86 GiB | 5 runs per arm; batch16K = 409,226 (+4.6%, below threshold and shared-cluster variance) |
| YTsaurus | Destination | row buffer 1 MiB | 40,178 | row buffer 512 KiB | 45,067 | 0.192 | 2.92 GiB | 5 order-balanced runs per arm, +12.2% |
| PostgreSQL | Source | binary COPY, batch 65,536 | 71,468 | binary COPY, batch 16,384 | 89,240 | 1.303 | 2.01 GiB | 2 temporal-v2 runs per arm; text16K = 27,521 |
| PostgreSQL | Destination | binary COPY | 87,007 | unchanged | 87,007 | 0.691 | 5.29 GiB | 2 runs; text = 42,628 |
| MySQL | Source | binary protocol, batch 16,384 | 98,179 | unchanged | 98,179 | 1.127 | 1.80 GiB | 2 temporal-v2 runs; binary65K = 91,484; text16K = 73,296 |
| MySQL | Destination | insert rows 1,000 | 19,991 | insert rows 250 | 29,975 | 0.521 | 4.25 GiB | 2 runs per arm, +49.9% |
| OpenSearch | Source | page 10K, concurrency 4 | 90,994 | page 10K, concurrency 2 | 94,348 | 1.125 | 3.62 GiB | 4-primary-shard fixture; C1 = 49,283 |
| OpenSearch | Destination | 20K rows, 16 MiB, concurrency 8 | 11,018 | concurrency 4 | 11,646 | 0.440 | 4.35 GiB | 3 runs per arm, +5.7%; 4 MiB adds requests without gain |
| Apache Iceberg | Source | batch 65,536, files 32, coalesce 1 MiB | 780,701 | unchanged | 780,701 | 1.763 | 5.55 GiB | 5 baseline runs; batch262K = 792,680 (+1.5%, order-sensitive) |
| Apache Iceberg | Destination | row group 250K | 311,466 | unchanged | 311,466 | 2.051 | 3.33 GiB | 3 runs per arm; row group 1M = 323,754 (+3.9%, below threshold) |

Exact persisted-integrity probes passed for local destinations and bounded
read-back probes passed for the shared YTsaurus fixture:

- ClickHouse, PostgreSQL, and MySQL each persisted 2,000,000 rows with
  2,000,000 distinct complete official primary keys. `EventDate`, `EventTime`,
  `Title`, and `URL` aggregate probes match the source exactly; ClickHouse also
  matched `ClientEventTime` and `LocalEventTime` bounds.
- OpenSearch persisted and returned the 500,000-row immutable subset. Epoch-day
  and epoch-second ranges and base64 lengths for `Title`/`URL` match the source.
  Its source throughput is not representation-comparable to the typed Arrow
  sources because it emits `_id/_routing/_source/_routing_key`.
- OpenSearch destination writes use `POST /_bulk`; every item status is checked,
  retries are bounded, and acknowledgement happens only after the successful
  bulk response.
- All six Iceberg destination tables and all ten YTsaurus destination tables
  were read completely through their production sources and returned exactly
  2,000,000 rows with `completed=true`. The scans preserved `EventDate` as
  `Date32`, timestamps as timezone-free microseconds, and binary columns as
  `LargeBinary`/`Binary`. These two read-backs did not independently recompute
  complete destination primary-key distinctness or binary checksums; that
  narrower qualification is explicit in the machine-readable summary.

The exact machine-readable values and verification probes are in
[`exact-prefix-summary.json`](results/2026-09-03/exact-prefix-summary.json).
The sanitized per-run evidence is in
[`exact-prefix-runs.json`](results/2026-09-03/exact-prefix-runs.json); it records
the actual source and destination durations, row counts, sampled profile size,
process CPU, average and peak RSS, and changed tuning values for the initial 44
local windows. The later temporal-v2 PostgreSQL/MySQL, Iceberg, and YTsaurus
series are recorded as per-run rate/duration/row arrays directly in
`exact-prefix-summary.json`. Failed probes and raw service logs are not averaged
into these results.

## Exact-prefix default decisions

| Connector | Role | Decision | Reason |
|---|---|---|---|
| YTsaurus | Source | Retain ordered batch 65,536 | Batch16K was only +4.6%, below threshold and dwarfed by shared-cluster variance |
| YTsaurus | Destination | Change row buffer 1 MiB → 512 KiB | +12.2% across five order-balanced exact-prefix runs and +1.3% peak RSS |
| ClickHouse | Both | Retain previous defaults | Larger source batch was slower; C16/C32 sink difference was noisy and below 5% |
| Apache Iceberg | Both | Retain previous defaults | Source candidates were within 1.5%; destination row-group candidate was +3.9%, below threshold and used more CPU |
| PostgreSQL | Source | Change batch 65,536 → 16,384 | +24.9% temporal-v2 throughput; binary remains lossless and much faster than text |
| PostgreSQL | Destination | Retain binary COPY | Text was 51.0% slower |
| MySQL | Source | Retain binary protocol / batch 16,384 | Both larger binary batch and text protocol were slower |
| MySQL | Destination | Change insert rows 1,000 → 250 | +49.9% on the wide 105-column exact-prefix workload |
| OpenSearch | Source | Change concurrency 4 → 2 | +3.7% throughput with lower request concurrency on four primaries |
| OpenSearch | Destination | Change concurrency 8 → 4 | +5.7% throughput and lower request concurrency; retain 16 MiB batch cap |

## Synthetic-profile appendix

All rows use the synthetic profile described above. Source figures are the
timed second pass of Speedtest (native source to discard); process CPU and RSS
cover the complete two-pass request and are only request-wide resource
envelopes. Destination
figures use a 5-second warm-up followed by a 15-second steady-state window.
CPU counts only the transfer process, not the external database service.

| Connector | Role | Input kind | Baseline configuration | Baseline rows/s | Selected configuration | Selected rows/s | Request CPU cores | Peak RSS | Repeats / dispersion |
|---|---|---|---|---:|---|---:|---:|---:|---|
| YTsaurus | Source | Synthetic native fixture | ordered, batch 65,536 | 410,976 | ordered, batch 16,384 | 576,153 | 1.647 | 2.05 GiB | 4 selected: 507,395–656,463; 3 baseline: 238,051–577,461 |
| YTsaurus | Destination | Synthetic generator | row buffer 1 MiB, concurrency 4 | 46,950 | unchanged | 46,950 | 0.229 | 1.60 GiB | 3: 46,922–46,975 |
| ClickHouse | Source | Synthetic native fixture | zstd, batch 65,409, row group 250K, decode 16 | 1,133,306 | unchanged | 1,133,306 | 1.766 | 2.51 GiB | 3: 1,092,032–1,182,126 |
| ClickHouse | Destination | Synthetic generator | zstd, 1M rows, 640 MiB, concurrency 32 | 334,564 | unchanged | 334,564 | 1.959 | 2.40 GiB | 2: 315,979–353,148 |
| Apache Iceberg | Source | Synthetic native fixture | batch 65,536, file concurrency 32, coalesce 1 MiB | 680,107 | unchanged | 680,107 | 1.174 | 4.00 GiB | 3: 666,216–695,022 |
| Apache Iceberg | Destination | Synthetic generator | zstd, concurrency 8, row group 250K | 328,526 | unchanged | 328,526 | 2.438 | 2.20 GiB | 2, high variance: 281,525–375,527 |
| PostgreSQL | Source | Synthetic native fixture | binary COPY, batch 65,536 | 84,259 | binary COPY, batch 16,384 | 103,886 | 1.347 | 1.80 GiB | 3 selected: 97,792–111,016; 2 baseline: 84,159–84,358 |
| PostgreSQL | Destination | Synthetic generator | binary COPY | 77,124 | unchanged | 77,124 | 0.505 | 0.87 GiB | 2: 76,468–77,780 |
| MySQL | Source | Synthetic native fixture | binary protocol, batch 16,384 | 202,684 | unchanged | 202,684 | 1.113 | 0.83 GiB | 3: 194,675–214,399 |
| MySQL | Destination | Synthetic generator | insert rows 1,000 | 16,679 | insert rows 250 | 26,075 | 0.316 | 0.74 GiB | 3 selected: 25,030–26,598; 3 baseline: 15,634–18,763 |
| OpenSearch | Source | Synthetic native fixture | page 10K, concurrency 4 | 46,028 | unchanged | 46,028 | 0.861 | 1.19 GiB | 3: 45,220–46,941 |
| OpenSearch | Destination | Synthetic generator | 20K rows, 16 MiB, concurrency 8 | 12,777 | unchanged | 12,777 | 0.324 | 1.30 GiB | 3: 12,515–13,300 |

CPU is expressed as mean fully utilized cores, not a host-relative percentage.
For Speedtest rows it spans the complete request rather than the role timing
window, so it is not combined with rows/s into a role-efficiency metric. Peak
RSS is the maximum resident set size over that same sampled process interval.

### Synthetic-profile decisions (historical, not authoritative)

| Connector | Role | Earlier default | Selected default | Throughput change | Resource trade-off | Decision evidence |
|---|---|---|---|---:|---|---|
| YTsaurus | Source | ordered batch 65,536 | ordered batch 16,384 | +40.2% | Same concurrency; selected run used 2.05 GiB peak RSS | Change |
| YTsaurus | Destination | row buffer 1 MiB | unchanged | reference | 512 KiB was −0.04% and used more peak RSS | Retain |
| ClickHouse | Source | zstd / 65,409 / 250K / decode 16 | unchanged | reference | Alternatives were slower | Retain |
| ClickHouse | Destination | zstd / 1M / 640 MiB / concurrency 32 | unchanged | reference | Concurrency 16 was −17.8% | Retain |
| Apache Iceberg | Source | 65,536 / file concurrency 32 / coalesce 1 MiB | unchanged | reference | Alternatives were slower | Retain |
| Apache Iceberg | Destination | zstd / concurrency 8 / row group 250K | unchanged | reference | Row group 1M was stable at 375,380, but overlaps the best noisy baseline and lacks the required repeat count | Retain |
| PostgreSQL | Source | binary COPY / batch 65,536 | binary COPY / batch 16,384 | +23.3% | Selected run used 1.80 GiB peak RSS | Change |
| PostgreSQL | Destination | binary COPY | unchanged | reference | Text COPY was −45.4% | Retain |
| MySQL | Source | binary protocol / batch 16,384 | unchanged | reference | Larger batch and text protocol were slower | Retain |
| MySQL | Destination | insert rows 1,000 | insert rows 250 | +56.3% | Higher request-wide process CPU | Change |
| OpenSearch | Source | page 10K / concurrency 4 | unchanged | reference | Page 5K was only +2.8% and doubled request rate | Retain |
| OpenSearch | Destination | 20K rows / 16 MiB / concurrency 8 | unchanged | reference | Concurrency 4 was −6.1% | Retain |

Synthetic-only decisions are not applied. The exact-prefix decisions above
supersede this table. YTsaurus source kept its previous batch default, its sink
row buffer changed to 512 KiB, and Iceberg remained unchanged.

## Reproduction record

- Benchmark date: 2026-09-03
- Measured code revision: unavailable because the benchmark executable was
  built from a dirty working tree. No later commit is claimed as its identity.
- Measured executable Murmur3 x64 128 values:
  `5b34310f0e7ad2c41fdecf0994b02999` for the initial local series and
  `d2b2d0ad3b033e8bd68e8f9b69127763` for temporal-v2 PostgreSQL/MySQL,
  Iceberg, and YTsaurus. The machine summary records the exact scope of each.
- Host: 32 logical CPUs, 50,530,660,352 bytes physical memory.
- Exact-prefix manifest and schema fingerprint: recorded in `PROVENANCE.md` and
  `exact-prefix-summary.json`; the complete source-file row count remains
  unverified and is not inferred.
- Exact-prefix local service versions: ClickHouse 25.8.2.29, PostgreSQL 17,
  MariaDB 11.8.3, OpenSearch 3.2.0, Iceberg REST 1.6.0 with an ephemeral local
  S3-compatible store. The shared YTsaurus test-cluster version was not recorded.
  Historical synthetic runs used the versions in their machine summary.
- Machine-readable exact-prefix aggregate:
  `results/2026-09-03/exact-prefix-summary.json`; sanitized per-run timings and
  resource measurements: `results/2026-09-03/exact-prefix-runs.json`.
  Historical synthetic aggregate: `results/2026-09-03/summary.json`.
- Source Speedtest duration ceiling: 15 seconds per pass. Finite fixtures often
  completed sooner; rows/s uses the actual timed second-pass elapsed duration.
  Destination windows: 5-second warm-up plus 15 seconds measured.
- Raw logs remain private. The public results contain no credentials, endpoint
  addresses, scratch paths, or raw error bodies. The authorized YTsaurus tables
  were not deleted because no ownership-safe cleanup path exists in the ordinary
  finite benchmark runner; only their aggregate non-secret measurements are
  published.

The original raw run logs lived only in private temporary storage and are not
referenced here because paths and endpoint details are not public provenance.
