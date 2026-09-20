# PostgreSQL snapshot planner specification

Status, 2026-09-21: implemented; focused correctness and compile checks passed.
The 5% performance gate has **not passed**. The retained empty-prefix case
exposes a model limitation; performance evidence and remaining uncertainty
are recorded below.

[Reproduction and measurement archive](research/postgres-snapshot-planner-2026-09-21.zip)
contains raw timings, source snapshots, the exact summary and verification logs.

## Goal

Automatically choose and execute an efficient, lossless table snapshot plan.
Use the same planner for `batch` and the initial snapshot of `batch_and_stream`.
Preserve the latter's exact replication-slot boundary and transition to CDC.

Keep the decision component small and independently testable. Explain its
inputs, candidate strategies, decisions and adaptations through structured
English diagnostics. Maintain an extensible, reproducible evaluation corpus
that can demonstrate improvements and expose counterexamples over time.

## Agreed product decisions

- Runtime worker and pipelines-per-worker settings are outside this change.
  Do not introduce a PostgreSQL-specific readers setting.
- Table splitting is automatic. The only new user control is an optional
  maximum number of parts per table; it is a ceiling, not a requested count.
  A limit of four allows one, two, three or four parts.
- Do not expose heap-pass limits, cost coefficients or strategy selection in
  the product UI. Repeated scanning is an internal cost consideration.
- Add a simple connector-owned queue now. Do not wait for the future runtime
  scheduler. A pipeline can process several queued chunks.
- Adaptation may change only unassigned work and must remain understandable
  from logs. Already assigned and completed work retains its boundaries.
- Minimize completion time while respecting configured constraints. There is
  currently no independent source CPU/I/O budget or runtime reader budget;
  neither may be invented or advertised as enforced.

## Component boundaries

`src_batch/planner.rs` owns the decision model, candidate applicability,
cost comparisons, validated plan descriptions and pending-range refinement.
Document its inputs, units, invariants, uncertainty, strategy rules and examples
with English module-level rustdoc in that same file.

The policy core uses only the standard library. Its logical interface is:

```text
validated table facts + constraints + observations -> decision and explanation
pending work + new observations -> unchanged plan or validated replacement
```

It must not open connections, execute SQL, decode Arrow, own UI types or depend
on delivery orchestration. Database collection/rendering, the shared queue,
snapshot ownership and source acknowledgements stay in their existing owning
connector modules. Avoid both a giant all-in-one file and a new generic
scheduling framework. Put tests in the batch component's separate test subtree.

Input types distinguish known facts, estimates and unavailable information.
Validate intrinsic invariants at construction; do not interpret missing
statistics as zero rows. Plans contain checked, nonoverlapping descriptors,
stable identities, explicit selected lane/chunk counts and reason codes.

Current ownership:

| Module under the PostgreSQL connector | Responsibility |
| --- | --- |
| `src_batch/planner.rs` | Pure eligibility, costs, validated decisions and range coverage; English rustdoc lives beside the policy. |
| `src_batch/planning.rs` | Observed catalog/physical facts, typed boundaries, SQL predicates and decision diagnostics. |
| `src_batch/queue.rs` | Per-table task ownership, pending refinement and atomic acknowledgements. |
| `src_batch/snapshot.rs` | Retained MVCC epoch, relation guards and frozen relation membership. |
| `src_batch/reader.rs`, `source/connector.rs` | Lane readers, COPY completion and snapshot/CDC phase integration. |
| `common.rs` | Owned protocol drivers and TLS-preserving PostgreSQL cancellation. |

## Automatic selection

The implementation evaluates five candidates:

| Candidate | Boundaries and applicability |
| --- | --- |
| Single | One complete scan; preserves the captured scope when other methods are ineligible. |
| Equal CTID ranges | Half-open physical page ranges on a guarded heap, using PostgreSQL 14+ native TID range scans. |
| Weighted CTID ranges | The same lossless physical ranges, with unequal page spans derived from a physical output sample. |
| Indexed ranges | Native typed cuts for a supported single-column primary key with a valid ascending default B-tree order and matching deterministic collation. |
| CTID hash buckets | Complete native-TID hash buckets; each reader pays for a full heap scan. |

Composite keys, unusual index orders and unsupported scalar key types do not
qualify for indexed ranges; a guarded heap can still use type-independent CTID
strategies. Selected parents with children retain one scoped scan rather than
applying a physical CTID range across several relations.

For eligible integer and NUMERIC keys, the initial catalog query sorts and
deduplicates the stored histogram using exact PostgreSQL numeric comparisons
and returns exact text boundaries. It reads histogram metadata, not table rows,
and does not round keys through Rust floating-point values. Other supported
key types retain their type/collation-aware deferred boundary query.
`indexed_boundary_seconds` prices only the remaining work: eager cuts or cuts
already materialized are included in measured metadata time and add no second
request. Queued plans likewise add query cost only for tasks beyond their
initial lanes; metadata and reader startup are not charged again per task.

Statistics and sampling propose boundaries; they never define the complete
row set. Every physical layout covers empty and unsampled spans and keeps its
last upper tail open. Weighted boundaries combine estimated heap-read, row and
output work. Every part retains at least one page, and cost comparison uses
each range's actual unequal page span. Forced weighted benchmark candidates
also pay for their observation.

The ordinary observation uses sparse `SYSTEM` sampling and 16 physical bins.
It can miss a concentrated wide-value tail entirely. A conditional
`SYSTEM (100)` observation inspects every page when raw external text/bytea
lengths are available, other widths are cheap native values or NUMERIC storage
proxies, external storage dominates the heap, and the estimated full inspection
repays its cost. The decision compares heap inspection with one reader's setup
prior and total row/field inspection with a two-way external-output saving.
Complete observations use one bin per page up to 4096 equal physical bins;
larger heaps retain within-bin uncertainty. This bounds estimator metadata,
not table size, rows, source eligibility or task counts.

Admissible observations are not automatically purchased. When physical TOAST
storage exceeds the heap, Auto first prices the proposed observation against
the saving of an ideal balanced physical plan over the best available
single/hash plan. If even that modeled saving cannot cover the price, Auto
skips the observation. It retains an explicit `UnobservedConcentration` state
and scores range output conservatively, instead of assuming an even byte
distribution. This does not assert that skew or a warm cache was observed.
The output prior is the larger of the catalog estimate and physical TOAST
size; both its origin and uncertainty appear in diagnostics. Ordinary
non-dominant-TOAST behavior is unchanged. Forced weighted evaluation still
collects and pays for its observation.

The information-value calculation uses the same cost priors and part ceiling
as execution. It compares an available alternative, rather than only comparing
probing with a slow single scan. Hash balance and all estimated costs remain
assumptions to test, not an absolute physical bound or a source-load guarantee.

`octet_length` observes raw text/bytea payload lengths. NUMERIC uses
`pg_column_size` during observation to avoid full text conversion; it remains
a storage-width proxy, not the byte length of production `numeric::text`.
Compressed or unbounded NUMERIC values can differ substantially. Custom, JSON
and array text projections remain sampled. TOAST size is a cost/risk hint,
not a lower bound on output, because compression and dead tuples affect it.

Earlier v2/v3 native-COPY experiments exposed retained-empty-heap and
concentrated-TOAST counterexamples that motivated weighted ranges and better
observations. Those runs used `SELECT *` and native binary NUMERIC, whereas
the production reader projects NUMERIC to text. Their timings and fitted rates
are exploratory evidence, not calibration or acceptance of the production
path. Cohorts v4 onward measure that production projection explicitly.

Eligibility precedes performance scoring. Consider relation kind and table
access method, PostgreSQL capabilities, physical leaf identity, locks, key
ordering and uniqueness, index suitability, NULL/MCV behavior and query scope.
Use native PostgreSQL comparisons and collation for typed boundaries. A
composite primary key's column membership alone does not establish its order.

Coverage-critical SQL explicitly names `pg_catalog` operators, functions and
types. A role can place another schema before `pg_catalog` in `search_path`
and shadow even an operator on builtin types. Native histogram ordering and
range predicates must use the same ordering. The connector does not change
the session path, preserving its meaning for user expressions and RLS.

Estimate work from heap pages, live rows, output bytes and query/setup cost.
Include the extra heap scans of hash strategies. Refine estimates using actual
chunk observations. A table fitting in memory is not proof that it is cached.
Do not infer source capacity from client CPU count or PostgreSQL max_connections.

The current `request_seconds` observation includes execution of the catalog
facts query. It is an upper-bound timing proxy for setup/coordination estimates,
not a measurement of pure network round-trip time or marginal reader cost.
Cost rates remain visible priors; catalog latency, encoding-width uncertainty,
unknown cache state and shared resource contention limit their accuracy.

v6 prices the actual NUMERIC-to-text projection separately from generic field
handling: an additional 100 ns per projected NUMERIC field per row. The count
must not exceed the total column count; nonnumeric rates remain unchanged.
This is an uncertain correction to total row work, not a measurement of an
isolated conversion or a guarantee for arbitrary digit counts and NULL density.
Matched projected/native cases suggested a smaller incremental conversion
cost, while matched dense/sparse projected cases exposed missing generic work.
Cheap `pg_column_size` observations retain the ordinary field prior because
they do not perform that text conversion.

Start with cheap metadata. Additional probes must be cancellable and justified
by their expected benefit; do not automatically run full COUNT, ANALYZE or
exact quantiles before every snapshot. Missing estimates remain unknown and
use explicit priors; invalid or contradictory intrinsic facts fail validation.

The planner makes an explicit initial lane recommendation as part of Auto.
This is an estimated useful concurrency, not a user-approved load budget.
Keep lanes distinct from chunks and log both. Future runtime capacity can
constrain execution without changing chunk coverage or row identity.

## Simple queue and adaptation

Use a shared in-process queue per table, owned by the snapshot epoch. A fixed
set of source pipelines consumes it. A lane reuses its imported snapshot
transaction where possible, rather than connecting for each chunk.

The queue stores descriptors and small completion records, not row payloads.
Existing pipeline memory accounting and backpressure continue to own data.
Do not add a semaphore held inside source construction that prevents all
pipelines from completing startup.

Task lifecycle:

```text
Pending -> Owned(lane, attempt) -> ReadComplete -> DurableComplete
```

- Ownership is exclusive. Only Pending tasks may be split or reassigned.
- Claim work and start COPY lazily on the first read, after pipeline startup
  succeeds. Use an ownership guard so dropping a partially constructed source
  cannot strand a task; source construction precedes sink construction today.
- Replacing one pending parent with children is atomic; children have exactly
  the parent's coverage with no overlap. Keep open outer tails where needed.
- The maximum part count includes completed, active and pending leaf tasks.
  Splitting cannot evade it by forgetting completed work. Retired parents do
  not count as leaves; retries do not count as new parts.
- The current rule considers the largest pending CTID range when fewer tasks
  remain pending than there are lanes. It estimates elapsed time per page from
  a completed task and splits only when halving the expected tail repays an
  observed COPY startup. Rows and bytes remain task telemetry. It neither
  changes methods nor rewrites assigned boundaries.
- If no pending tasks remain, a lane can finish reading and drain its pending
  acknowledgements. It must not wait inside read_batch for its own commit
  callback; that would prevent the pipeline from delivering the callback.
- The queue is not a distributed scheduler. No work migration between
  processes, persistent task service or recovery of a dead snapshot epoch.

## Snapshot and delivery correctness

Acquire required relation/topology guards before establishing the snapshot.
Capture each selected table's physical relation membership during guarded
discovery, before exporting the MVCC row snapshot. Read ordinary leaves with
`ONLY`; for a selected parent, retain its captured OID closure in the query.
Later attachments are outside this discovery result. Existing parents retain
topology locks and existing descendants retain relation locks, so captured
members cannot disappear during the epoch. Log the membership cutoff and OIDs.
Read actual physical sizes afterward. Check physical relation identity as well
as schema/type identity before executing a task. CTIDs identify a physical
leaf under the retained snapshot and guards, not durable cross-snapshot rows.

For partitioned tables, preserve selected-table routing and coverage while
handling leaves and topology explicitly. If a strategy's required contract
cannot be established, exclude that strategy and use a correct applicable
path. Insufficient privileges must be visible; do not acquire stronger locks
silently as an incidental side effect of metadata collection.

For batch_and_stream, guards must precede the replication slot's exported
snapshot. All lanes import that exact snapshot. CDC begins only after every
snapshot task and destination durability barrier has completed.

ReadComplete requires valid complete COPY framing and checked final SQL
completion. COPY EOF alone is not a destination commit. A typed commit marker
identifies epoch, task, lane/attempt and acknowledged progress; transition to
DurableComplete only through the existing post-sink commit callback. Empty
tasks also emit a task-end marker through an empty typed delivery. Validate
each complete acknowledgement group before atomically updating queue state.
Completion-marker misuse,
wrong epochs and unexpected duplicate acknowledgements fail explicitly.

A shared MVCC snapshot fixes a row set, not an implicit SQL row order. Stable
lane ownership alone does not make partial replay safe. The first simple queue
may retry an attempt proven to have emitted no batch. If a partially emitted
task has failed or its outcome is ambiguous, fail the epoch rather than blindly
requeueing it. Already durable tasks remain complete. More permissive recovery
requires a separately proven deterministic cursor or exact replay mechanism.

Keeper/process loss invalidates the epoch. Do not silently import a new
snapshot and continue old chunks. Preserve current explicit interrupted-snapshot
failure semantics for batch_and_stream. Cancellation sends a PostgreSQL
CancelRequest before closing the owned protocol driver, with the operation's
configured cleanup deadline and TLS policy. Closing TCP alone cannot reliably
interrupt a server lock wait. Cancellation transport failures stay visible;
cleanup must not turn incomplete work into successful completion. The real
PostgreSQL regression test observes backend disappearance while its conflicting
topology lock remains held.

## Integration with current execution

Discovery advertises one colocated partition per table. Preparation refines the
finite colocated Snapshot topology to the fixed lane set before destination
preparation. The integration contract:

- preserve phase order, phase kind, finite flags and worker ownership;
- preserve Stream topology and the existing transition barrier;
- retain authoritative discovery, semantic and sink-limit validation;
- replace table-array-index lookup with an immutable lane-to-table mapping.

The current runtime starts every published lane. The queue separates the
number of chunks from that lane count. New worker/pipeline configuration,
cross-process work sharing and global source resource enforcement remain
outside this change.

## User interface and observability

The existing schema-driven performance section exposes optional
`max_snapshot_parts`, titled **Maximum snapshot parts per table**. Its positive
integer contract is validated in the backend as well. An absent value means
automatic planning; a present value limits all task leaves, including completed
ones. It does not specify simultaneous readers. There is no custom runtime page
or strategy dropdown.

Structured English events cover:

1. Planning started: selected table/epoch context and configured constraints.
2. Facts collected: values, units, origin, estimated/known/unknown status and
   probe duration; missing or stale statistics remain explicit.
3. Candidates evaluated: eligible/excluded, reasons and estimated cost terms.
4. Plan selected: strategy, lanes, initial parts, confidence and explanation.
5. Pending work adapted: previous/new task identities, counts, observed
   evidence, expected benefit and reason; no change when it is not justified.
6. Completion/failure: actual rows/bytes/times, task states and the relevant
   stable reason code.

Use INFO for decisions and meaningful changes, DEBUG for detailed candidate
evaluation and task observations. Do not log row values, sampled keys, raw
predicates, credentials or unredacted driver errors. New external requests
use shared external-request instrumentation; decision logs do not replace it.

## Long-term evidence and adding counterexamples

Use three separate layers:

1. Pure tests verify valid construction, eligibility, coverage, caps and
   deterministic decision/explanation replay. Queue tests replay observations
   and prove ownership, atomic refinement, acknowledgement and failure rules.
2. Real PostgreSQL tests compare exact output identities and payloads against
   one query under the same snapshot. Reuse the 34 research scenarios with
   production-generated predicates, plus queue and actual delivery integration
   for both snapshot modes and binary/text COPY. Unsupported methods must be
   excluded explicitly; partitioning tests do not expand Arrow type support.
3. An explicitly invoked Rust RELEASE benchmark runs the real production
   planner and queue against a single scan and applicable forced strategies.
   Forced selection belongs to the evaluator, not the product UI.

Keep a versioned case manifest and deterministic setup recipes under the
PostgreSQL connector's tests. A case records DDL, seed or exact fixture data,
load order, indexes, DELETE/VACUUM/TOAST/ANALYZE history and assertions proving
that the intended distribution/physical condition was produced. Provide a
small correctness scale and a representative performance scale. Failed fixture
preconditions invalidate a measurement instead of producing a misleading win.

Start with the six measured data profiles and the corner-case corpus. Invoke
the existing generator separately for preparation when exact reproduction
requires it; never depend on the heavyweight all-connectors crate from the
policy core. SQL approximations receive distinct case IDs and provenance.

On every performance evaluation, compare alternatives on the same current
machine, PostgreSQL version, cache scenario and execution constraints. Include
metadata/probes, planning, connection setup and checked reading completion in
elapsed time. The implemented tournament consumes binary COPY into an immediate
discard consumer using the production queue; it measures source-side planning
and reading, not Arrow decoding, a destination or full delivery throughput.
It reuses the production snapshot projection with the batch default
`unsupported_types=to_string` and records source/projected type OIDs. Report
schema version 3 distinguishes this from earlier native-COPY cohorts. The
column policy is a benchmark setting, not a new conversion introduced by the
planner; the source's existing startup validation still applies to delivery.
It reports rows/bytes, timings, task counts, estimated heap passes and per-case
regret. It does not measure source CPU or physical I/O. Shared snapshot export,
reference COUNT and applicability preflight are outside each timed sample.
Reference reads warm the database; this is not a cold-cache comparison.
Every eligible strategy competes on an independent part grid plus proposed
lane/task counts. Forced cheap candidates skip irrelevant Auto-only probes.
Evaluator preflight independently attempts the observation required by weighted
ranges even when ordinary Auto skips it, so Auto cannot exclude its own missed
candidate from comparison. That extra applicability work is outside timing;
every timed weighted run repeats and pays for its required observation.

The comparator keeps the 5% tolerance and uses exact one-sided sign tests on
paired time ratios. A pass needs evidence for every candidate and observed
Auto/best median time at most 1.05. Regression uses Holm correction across
candidates and a significance budget divided across the invocation's tables.
Threshold ties and insufficient evidence remain inconclusive. Select the
repetition count before measuring; do not append rounds until a pass appears.
The inference assumes independent round signs. See the
[fixture and evaluation guide](../crates/transferia-connector-postgres/tests/fixtures/snapshot_planner/README.md)
for the complete statistical contract and counterexample workflow.

Selection regret is Auto time divided by the best measured applicable
candidate's time. A dataset must not permanently assert that CTID or another
named strategy is the winner. New counterexamples extend this corpus; new
strategies join the same evaluator. Historical seconds do not establish
performance on different hardware or a newer PostgreSQL version.

Generated results stay under target/. No new GitHub workflow, database dumps
or loose generated CSV/PNG collections are added to Git. Keep one concise
evaluation report or archive when a result needs publishing.

## Acceptance

- All agreed UI/scope decisions above are implemented without extra settings.
- Policy code and English documentation are independently understandable.
- A real queued snapshot executes in both modes with correct acknowledgements.
- All applicable corner cases and failure rules pass production-path tests.
- The six-profile release comparison produces auditable repeated results.
- No universal optimality claim: evidence applies to the versioned corpus,
  tested environments, applicable candidates and declared constraints.

A reproducible Auto slowdown greater than **5%** versus the best measured
applicable candidate fails performance acceptance. Noise and inconclusive
comparisons are reported separately and require further evidence.

Implementation checklist:

- [x] Pure planner and English module documentation.
- [x] PostgreSQL facts, predicates and structured decision logging.
- [x] Simple queue, pending adaptation and durable completion.
- [x] Guarded snapshots and execution integration in both modes.
- [x] One optional maximum-parts control and backend validation.
- [x] Correctness corpus, integration and failure tests.
- [x] Rust release tournament with reproducible provenance and statistical gate.
- [x] Complete six-profile, 31-round release comparison with retained raw results.
- [ ] Overall 5% performance acceptance (four passes, one inconclusive, one regression).

Focused verification for the final tree on 2026-09-21 passed: 205 PostgreSQL library tests
and 14 planner E2E target tests including the 34 named research scenarios.
Earlier checks in this implementation also passed six snapshot/CDC tests,
four snapshot consistency tests and one delivery topology regression.
They cover real multi-lane binary/text reads in both modes, exact values and
multiplicity, acknowledgement barriers, retained snapshots, frozen inheritance
membership, blocked-preparation cancellation, source-construction cancellation,
and native normalization of unordered/duplicate NUMERIC histogram bounds with
exact fractions, NaN and infinities. A hostile `search_path` fixture shadows
NUMERIC/TID comparisons, catalog identity comparisons, hash arithmetic, JSON
functions/operators and the `text` type. It checks exact row multisets for all
split strategies and verifies unchanged native query plans on the ordinary
path. This fixture exercises the planning adapter; integrated source discovery
and guards have ordinary-path E2E coverage and separate SQL review, not a full
hostile-path end-to-end test. The final `just check-affected`
passed in 10.34 seconds. These are focused checks, not a full workspace/release
gate, and do not establish performance acceptance.

The final focused commands were:

```sh
cargo test -p transferia-connector-postgres --lib --test e2e_snapshot_planner
just check-affected
```

The evidence archive also retains an earlier failed E2E run: its disposable
`pg_statistic` fixture initially assigned an array with the wrong declared
result type. The fixture now uses PostgreSQL's declared `anyarray` result;
both the corrected run and the final hardened-tree run passed.

Compiling `planner.rs` directly with `rustc --edition=2021 --test` also passed
all 28 policy tests, independently of Cargo and every external dependency.
The fixture guide includes the complete standalone command.

An optimized standalone replay compared v4 and v5 on 7,890 matched policy
inputs: 38,295 candidate comparisons had zero differences, including bitwise
cost values. It covers the former absent-observation/`UniformPrior` behavior,
observed distributions and forced/rejected constraints. It does not cover the
new `UnobservedConcentration` state, adapter I/O or end-to-end timing.

### Release observations from this implementation

The measured consumer uses the production column projection, immediate discard
acknowledgements, and at most eight parts. Each original-data case has 31
preselected randomized rounds. Results are per-invocation inference families;
separate case commands are not one joint 95% regression claim. Cache state is
uncontrolled after warm-up. The table below uses only the v6 cohort. Historical
v4/v5 results remain separate in the evidence archive. The v6 source snapshot
precedes the final `search_path` qualification: the policy is unchanged, but
these timing reports do not establish performance acceptance of the later SQL
bytes. The final-tree correctness checks are recorded separately.

| Original case | Auto median | Best measured median | Regret | Gate |
|---|---:|---:|---:|---|
| Concentrated TOAST tail | 325.58 ms | 314.36 ms, hash P8 | +3.57% | Pass |
| Nullable transfer logs | 655.10 ms | 641.23 ms, CTID P8 | +2.16% | Pass |
| Wide ClickBench profile | 764.63 ms | 757.79 ms, CTID P8 | +0.90% | Pass |
| Numeric key skew | 411.83 ms | 413.29 ms, CTID P8 | -0.35% | Pass |
| Empty heap prefix | 200.45 ms | 156.71 ms, hash P8 | +27.91% | Regression |
| Numeric, 2 million rows | 438.15 ms | 418.59 ms, CTID P8 | +4.67% | Inconclusive |

TOAST selected hash P5 in seven rounds and P6 in 24. Planning took 20.31 ms
by median. The previous v4 observation-based plan spent 371.3 ms in planning
and took 697.94 ms overall; skipping information that cannot repay an available
alternative improved this case without excluding weighted competitors. These
are different fingerprinted cohorts, not paired treatment measurements.

Numeric selected CTID P8 in all 31 rounds, also the best measured candidate.
Its median ratio is inside 5%, but paired comparisons against every competitor
do not establish the statistical pass. No extra rounds were appended to change
that verdict.

The empty-prefix counterexample remains unresolved. Auto chose indexed P4 in
all rounds; planning cost 33.41 ms versus 20.41 ms for forced indexed P4. The
physical observation added about 13 ms and was not useful to the final indexed
plan. Pricing observation value against an already available index is a
candidate improvement, but removing that cost alone does not achieve 5%.
Forced indexed P4 took 175.55 ms, indexed P8 took 194.66 ms, and hash P8 took
156.71 ms. The current model makes balanced indexed ranges cheaper than hash
at equal concurrency and correlation 1, while these measurements show different
strategy scaling. A heap-rate adjustment alone cannot repair that ordering.

Catalog-request timing also overprices marginal reader setup/coordination;
it contains server work as well as network time. Live throughput, cache
residency and strategy-specific contention remain unobserved. The simple queue
cannot repartition an already assigned tail. Follow-up model work should
separate metadata cost from reader startup and use measured strategy scaling,
with a new predeclared evaluation cohort. Do not hide this counterexample with
a table-name rule, silently relax the 5% threshold, or claim universal
optimality from individual passes.
