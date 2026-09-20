# Snapshot planner evidence

The production planner is tested against counterexamples, not a permanent list
of winning strategy names. The policy/queue unit tests own their invariants;
`tests/e2e_snapshot_planner.rs` owns real PostgreSQL exactness and the 34 named
research scenarios. The release example measures the actual production planner,
SQL predicates and queue implementation, including pending-task adaptation.
Current implementation and verification status are recorded in the
[planner specification](../../../../../docs/postgres-snapshot-planner.md).

Connector E2E tests also drain multiple real source lanes in both COPY formats
and both delivery modes. They compare every primary key and payload, retain the
snapshot across a concurrent update, withhold acknowledgements to check the
phase barrier, and observe that update through the subsequent CDC stream. A
separate test attaches previously populated inheritance members after discovery
and checks that they stay outside the frozen table scope. Cancellation coverage
holds a conflicting topology lock until the abandoned preparation backend has
actually disconnected. A protocol regression also cancels snapshot source
construction while its connection handshake is stalled. The real PostgreSQL
corpus replaces a disposable NUMERIC histogram with unordered, duplicate,
arbitrary-precision and non-finite boundaries, then checks native exact coverage
at several part counts.
Another fixture shadows builtin comparison/hash operators, JSON functions and
the `text` type through an explicit `search_path`. It checks exact split coverage,
unchanged session settings and equivalent ordinary-path query plans. This
fixture exercises the planner adapter, not a complete hostile-path source
discovery/guard lifecycle.

## Reproducible fixtures

`corpus.json` describes six deterministic SQL profiles. `profiles.sql` defines
their insertion order, composite keys, nullable fields, skew and TOAST storage.
These are new versioned datasets inspired by the original generator research;
they do not claim to reproduce the original generator output byte for byte.
In particular, the v1 SQL `uniform_numeric` and its derived profiles use
`BIGINT`; the original generator tables have eight PostgreSQL `NUMERIC(20,0)`
fields. The SQL analogues do not reproduce that text-encoding cost. Original
generator setup and derivative-table recipes are retained in the
[original reproduction archive](../../../../../docs/research/pg-snapshot-benchmark-2026-09-20/reproduction.zip).
Use those recipes and the evaluator's explicit `--table` path when reproducing
the original numeric, empty-prefix or numeric-with-TOAST timings.
`corners.sql` is a compact additional value corpus. `research.sql` retains the
small data recipes from the archived 2026-09-20 experiments. Neither contains
credentials, database dumps, destructive cleanup or generated measurement data.

Preparation requires a **new** schema. Existing schemas fail explicitly; the
runner never drops or truncates them. Use at least 1,000 rows, divisible by 1,000.
The example runs plain VACUUM separately from transactions and then ANALYZE for
performance profiles. It verifies row/column counts, retained empty heap prefix,
the 90% dense key prefix and the 1% 64-KiB payload tail before timing anything.
Functional tests preserve deliberately missing and stale statistics separately.
The research value corpus forces every applicable strategy at an independent
part grid through 16, plus the planner's proposed lane and task counts. Small
datasets therefore exercise real boundaries even when Auto reasonably chooses
a single reader.

Supply `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGPASSWORD` through the
environment. TLS defaults to `verify-full`; `PGSSLROOTCERT` supplies an extra
CA file. Plaintext requires explicit `PGSSLMODE=disable` for a trusted test
service. Connection secrets are absent from the report.

```sh
cargo run --release -p transferia-connector-postgres \
  --example snapshot_planner_benchmark -- \
  --prepare --schema planner_corpus_v1 --rows 100000 \
  --max-parts 8 --repetitions 31 \
  --output target/snapshot-planner-evaluation
```

To reuse existing data, omit `--prepare`. A single preloaded table, including an
original generator dataset, can be evaluated without rebuilding any generator:

```sh
cargo run --release -p transferia-connector-postgres \
  --example snapshot_planner_benchmark -- \
  --schema benchmark --table events --case-id production_counterexample_v1 \
  --max-parts 8 --repetitions 31
```

The PostgreSQL crate has no dependency on the heavyweight all-connectors or
generator crate. Exact original generator profiles can be prepared separately
with an existing Transferia executable; preparation is outside the measurements.

## Measurement and acceptance

Debug executables reject measurement. Auto competes with applicable production
strategies at an independent `1, 2, 4, 8, ...` part grid up to the evaluator's
`--max-parts` (default 8), plus the model's proposed lane and task counts. `--max-parts
auto` leaves Auto uncapped and compares a grid through 8 plus its proposed
points. This is an explicit benchmark matrix, not a hidden production limit.
The same part ceiling applies to queue refinement. Eligibility is checked using
production capabilities; unexpected planning/SQL errors fail the run.

The candidate set includes a single scan, equal-page CTID ranges, weighted
CTID ranges, indexed typed ranges and CTID hash buckets. Weighted ranges use
the production physical observation to balance estimated heap and output
work. Candidate discovery independently attempts that observation on applicable
native heaps when Auto skipped it, so a bad probe-pay decision cannot hide its
own counterexample. Reports distinguish ordinary Auto eligibility from
eligibility established by the evaluator's extra observation. An empty
observation leaves weighted ranges unavailable; query failures still fail the
run. Their forced runs repeat and include the observation cost; ordinary forced CTID/hash
or single scans do not pay for unused Auto probes. The `empty_prefix` and
`toast_tail` recipes preserve the physical/output skew that motivated this
candidate after earlier equal-range regressions. Empty or unsampled spans
remain part of every complete physical layout.

v6 obtains eligible integer/NUMERIC histogram boundaries in the first catalog
query, ordered and deduplicated by PostgreSQL with exact numeric values.
Other supported key types retain their deferred type-aware query. The cost
model charges only remaining indexed-boundary work; already materialized cuts
are charged once in metadata time. Additional queued tasks add their query
cost, without repeating metadata or lane startup costs.

Ordinary observations use sparse `SYSTEM` sampling with 16 physical bins. A
conditional `SYSTEM (100)` observation avoids missing a concentrated external
text/bytea tail when width inspection is cheap and its estimated saving repays
reading every page. It requires dominant external storage, a heap-inspection
prior within reader setup cost, and row/field inspection below the estimated
two-way external-output saving. Complete observations have page resolution up
to 4096 equal bins; the ceiling bounds estimator state, not rows or tasks.
NUMERIC may accompany native columns using `pg_column_size` as a cheap storage
proxy. Its production text size remains uncertain, especially for compressed
or unbounded values; custom/JSON/array text conversion stays sampled. TOAST
size itself is an estimate affected by compression and dead tuples.

For dominant TOAST, Auto additionally compares the observation price with
the modeled saving of ideal balanced ranges over an available single/hash
plan. It skips information that cannot repay this alternative, retaining
explicit uncertainty about concentration and conservative range scoring.
The evaluator still measures forced weighted candidates, including their
observation cost, so this decision can acquire its own counterexamples.

Every round shuffles the variants with a recorded deterministic seed. The
primary elapsed time includes metadata/probes needed by that candidate,
planning, reader connections, snapshot import, queue work, binary COPY framing
and a checked same-transaction SQL completion barrier. COPY EOF alone is not
accepted as successful SQL completion. An explicit discard consumer immediately
acknowledges complete task markers; there is no destination, Arrow decoding or
full delivery measurement. Task rows/bytes/time drive the real queue's adaptive
decisions. The source file is reused directly, rather than copied into a second
benchmark scheduler.

COPY uses the exact production snapshot projection and the batch default
`unsupported_types=to_string` policy. In particular, PostgreSQL `numeric`
columns use the same `::text` projection as the source reader. Measuring native
binary NUMERIC would have different server conversion and payload costs.
Report schema version 3 records source/projected type OIDs, projection policy,
and fingerprints of the reader and shared type mapping. Earlier native-COPY
reports remain exploratory evidence and are not production-projection
acceptance results. The evaluator still excludes Arrow decoding and destination
work, so this remains a snapshot scan/planning comparison rather than a full
delivery benchmark.

The v6 policy adds a validated 100 ns row-processing prior per NUMERIC field
actually projected to text; its count cannot exceed the total field count.
Nonnumeric rates and cheap storage-width observation costs stay unchanged.
This is a total-processing correction with uncertain digit/NULL and hardware
effects, not a measured isolated conversion cost. Recorded v6 measurements
have not passed the overall 5% gate; the retained empty-prefix case remains
a counterexample. See the specification for exact results and source provenance.

One retained snapshot and guard are shared by a table's comparison. Their
initial connection/export/lock costs, fixture preparation, reference COUNT,
applicability and projection-metadata preflight and report writes are outside
each timed sample. Auto's timed preparation always uses its ordinary probe
policy; the extra candidate-discovery observation is never injected into Auto.
Reference validation and previous runs warm the database: this is **not** a
cold-cache measurement. Timed runs check complete binary framing and the exact
row count; the E2E corpus separately compares exact complete row multisets,
including payload and duplicate multiplicity.
Current model rates remain priors: observed catalog-query duration includes
server work and only serves as an upper-bound setup/coordination proxy, not a
pure RTT measurement. Cache residency and available source capacity are unknown;
the reports do not claim to measure either.

The accepted tolerance remains **5%**. The default is 31 paired rounds; at least
five are required. Select the round count before measuring. Do not repeatedly
append rounds or restart unchanged experiments until a preferred verdict
appears: that invalidates the stated error rates.

Each candidate gets two exact one-sided binomial sign tests around a paired
Auto/candidate time ratio of 1.05, at significance level 0.05. Threshold ties
support neither direction and remain in the sample count, conservatively.
A pass requires evidence below the threshold for **every** candidate, and
Auto's measured median time divided by the fastest candidate's measured median
must also be at most 1.05. This intersection-union rule does not need a multiple
comparison correction for the overall pass. A regression requires evidence
above the threshold with Holm correction across that table's candidates; the
regression significance budget is additionally divided by the preselected
number of tables in the invocation. Separate `--table` commands remain separate
inference families and do not imply a joint 95% regression guarantee.

Five identical signs give a one-candidate p-value of 1/32; four out of five
are inconclusive. With 31 rounds, 21 below-threshold pairs can establish a
pass, whereas a fixed 80% rule would require 25. More candidates require
stronger regression evidence after correction. Higher sample counts can resolve
moderate noise, but 31 rounds do not guarantee a conclusive result. The tests
assume independent round signs; shared load trends or other serial dependence
require a separately designed experiment. No normal timing distribution is
assumed. The probability calculation starts at the binomial mode, avoiding
false significance from numerical underflow at large repetition counts.

Mixed or insufficient evidence is **inconclusive**, returns failure and must
not be reported as a pass. Reports retain every pair, sign counts, raw p-values,
Holm-adjusted regression p-values and inference scope. Min/max paired ratios
describe observed spread; they are not confidence intervals. The test follows
the [NIST sign-test description](https://www.itl.nist.gov/div898/software/dataplot/refman1/auxillar/signtest.htm);
the [R statistical documentation](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/p.adjust.html)
describes Holm's correction, including its validity under arbitrary dependence
between candidate comparisons.

Results include every completed timed run, candidate applicability, limits,
server settings, client architecture and Murmur3-128 fingerprints of the binary,
planner, adapter, queue, evaluator, comparator, fixture recipe, Cargo.lock,
workspace Cargo.toml, toolchain and Cargo configuration. The latter two are
also recorded as exact text. Compare
alternatives on the same current environment; old absolute seconds are not a
baseline for different hardware or PostgreSQL versions. No universal optimality
claim follows from a passing corpus.

All generated reports belong under `target/`. Do not add loose result files or
database dumps to Git. No scheduled job or GitHub workflow is installed.

## Add a counterexample

1. Give the case a new versioned ID and record why it defeats the current plan.
2. Add its deterministic SQL recipe and manifest entry. Preserve the property
   that matters: types/collation, insertion order, indexes, TOAST policy,
   DELETE/VACUUM history, statistics state and seed where relevant.
3. Assert that preparation actually produced that property. A failed fixture
   precondition invalidates the experiment.
4. Keep a small exactness version in E2E and a realistic scale in the explicit
   release comparison. Special concurrency scenarios belong in the test driver.
5. Compare Auto with actual eligible alternatives; do not assert that one named
   strategy must remain the winner forever.

The policy can also be tested entirely on its own, using only the standard
library. From the repository root:

```sh
mkdir -p target
rustc --edition=2021 --test \
  crates/transferia-connector-postgres/src/connectors/postgres/src_batch/planner.rs \
  -o target/snapshot-planner-policy-tests
target/snapshot-planner-policy-tests
```

This independent path passed all 28 policy tests for v6. It checks policy
contracts, not database behavior or the 5% performance gate.

Run the focused correctness target with an available Docker-compatible runtime:

```sh
cargo test -p transferia-connector-postgres --test e2e_snapshot_planner
```

This target starts pinned PostgreSQL 17.6 and does not silently skip tests when
the runtime is unavailable. The release example is an explicit performance
task, separate from the repository's normal compile-only development gate.
