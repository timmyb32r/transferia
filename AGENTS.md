# Project Instructions for Coding Agents

These instructions apply to the entire repository. This is an experimental demo
whose purpose is to crystallize good concepts quickly, not to preserve old APIs.

## Product priorities

1. **Do not preserve backward compatibility.** Breaking configuration, APIs,
   schemas, object layouts, names, or behavior is expected when it produces a
   cleaner design. Update all callers, examples, documentation, and tests in the
   same change. Delete aliases, migrations, deprecation shims, legacy readers,
   hypothesis-era options, stale names, and superseded implementations instead
   of carrying them forward.
2. Optimize for **speed of evolution, maximum runtime efficiency, and zero
   technical debt**. Prefer deletion and one clear source of truth over adapters,
   compatibility layers, duplicated contracts, or speculative abstractions.
3. Treat the principles in this file as strong defaults, not substitutes for
   engineering judgment. A principle may be violated when concrete evidence and
   common sense show that doing so is better. Keep the exception narrow and state
   the reason and trade-off in the handoff. The prohibition on silent user-visible
   transformations below is not covered by this exception.

## Data preservation is the highest priority

- **Above all else, do not lose user data.** When safety, convenience,
  performance, availability, or implementation simplicity conflict with data
  preservation, preserve the data and make the failure explicit.
- Every configuration default must be lossless. Defaults must retain source
  records and fields, preserve ordering and commit guarantees where promised,
  and preserve the full supported value, type, precision, scale, timezone, and
  encoding. A destructive behavior such as dropping records, ignoring unknown
  fields, skipping malformed input, or acknowledging unpersisted data must
  require an explicit, deliberate user choice and must never be the default.
- Never silently or quietly truncate, round, saturate, narrow, coerce, replace,
  reinterpret, or otherwise reduce the precision or fidelity of user data. This
  prohibition applies to values, identifiers, schemas, timestamps, numeric
  types, strings, binary payloads, offsets, and metadata at every stage of the
  delivery.
- Never conceal or continue through detected corruption. Fail closed before the
  next irreversible side effect, preserve replayability, and report which value,
  record, partition, or persisted state violated the contract without exposing
  secrets.
- Validate losslessness during configuration parsing or discovery whenever it
  can be known statically, and validate it again at runtime before buffering,
  writing, uploading, committing, or acknowledging data. Cover both validation
  boundaries with regression tests for every new conversion or destination
  constraint.

## User-visible semantics: never guess or silently transform

- **Never make a product or UX decision on the user's behalf by silently changing
  their identifiers or data.** This includes renaming tables or columns, hashing,
  truncating or escaping names or values, normalizing values, coercing types,
  changing precision or timezone, rewriting paths, substituting defaults for
  invalid input, dropping fields or rows, and any similar transformation visible
  at the source or destination.
- Do not invent an automatic fallback for a destination limitation. For example,
  if S3 imposes a key-length limit and a source value can exceed it, reject the
  configuration during discovery when possible and otherwise reject the offending
  runtime value. **Do not silently replace the value with a hash, shortened form,
  encoded alias, or generated name.**
- Prefer fail-fast behavior for every unsupported edge case. Validate static
  constraints while parsing the configuration or during delivery discovery,
  before connecting workers or creating destination state. Revalidate
  data-dependent constraints at runtime before buffering, INSERT, upload, commit,
  or another side effect.
- A transformation is allowed only when both conditions hold:
  1. the user explicitly requests it through a deliberate, documented
     configuration choice; and
  2. the exact transformation is explicitly implemented, named, validated, and
     covered by startup and runtime tests.
- An explicit transformation must have deterministic, documented semantics and
  must be observable in configuration and diagnostics. It must never be enabled
  implicitly for compatibility, convenience, robustness, or performance.
- When requirements call for a new transformation but the user has not selected
  its semantics, stop and ask rather than choosing a policy in code.

## Frontend interaction stability: zero unexpected layout shift

- **Every user interaction must receive immediate visible feedback.** The
  pressed state must appear on pointer-down/keyboard activation, and the
  resulting state transition must be rendered synchronously or on the next
  animation frame. A click must never appear to have been ignored.
- If an operation waits for network, storage, discovery, validation, worker
  startup, or any other asynchronous work, render a pending state immediately,
  before awaiting it. Use a spinner, skeleton, progress state, or explicit
  status text appropriate to the control. Preserve the control's dimensions and
  surrounding layout across idle, pressed, pending, success, and error states.
- Prevent accidental duplicate activation while an operation is pending. Use a
  disabled/busy state, request deduplication, or an explicitly safe idempotent
  interaction model. Keep enough visible feedback to make it obvious that the
  first activation was accepted; disabling a control without explaining the
  pending state is not sufficient.
- Success and failure must also produce immediate, visible, accessible feedback.
  Associate it with the initiating control or stable status region, expose busy
  state through appropriate ARIA semantics, and restore focus deliberately.
- **“Delayed interaction feedback + layout shift causes an accidental
  second-click activation” is a forbidden failure mode.** Never allow delayed
  content, a popup, a notification, or a newly enabled action to appear beneath
  the pointer location where a user may repeat a seemingly ignored click. Delay
  the new hit target until pointer release/movement when necessary, or place it
  outside that interaction coordinate while preserving layout.
- **Unexpected layout shift is forbidden at any cost.** A situation where a
  notification or asynchronous update moves the interface and can cause an
  accidental click is absolutely unacceptable. Treat stable target coordinates
  during interaction as a correctness and safety property, not visual polish.
- Never insert an unexpected toast, snackbar, notification, validation message,
  or asynchronous status into normal document flow. Render transient feedback
  in a fixed overlay that does not move existing content.
- When a banner or message must participate in normal flow, reserve a stable,
  fixed-size region for it before the content appears. Showing, hiding, loading,
  success, and error states must occupy the same layout footprint.
- Do not change layout while the user has an active pointer press, focused
  control, drag, scroll, tap, or equivalent interaction. Defer non-critical UI
  updates until the interaction ends. If a change is unavoidable, preserve the
  position and hit target of the control being used.
- Declare dimensions for dynamic content in advance. Use explicit `width` and
  `height`, `aspect-ratio`, stable containers, and correctly sized placeholders
  or skeletons for images, previews, asynchronous blocks, and other late content.
- Never place destructive or irreversible actions where a moving layout can
  bring them under an existing pointer or touch target. `Delete`, `Buy`, `Send`,
  `Confirm`, and equivalent actions require an additional safety barrier such as
  confirmation, undo, or delayed enablement. This barrier supplements layout
  stability and must never be used as a substitute for it.
- Every frontend change that introduces conditional, asynchronous, lazy-loaded,
  expanded, collapsed, validated, or notification content must include a
  regression test proving that surrounding interactive controls do not move
  unexpectedly. Every new asynchronous interaction must additionally test its
  immediate pressed/pending feedback and duplicate-activation protection.

## Performance and design

- Every operational or safety limit must come from an explicit user-visible
  configuration value and be validated before execution. A hardcoded constant
  must never reject, truncate, or otherwise break a delivery that satisfies its
  configured limits. Constants may define implementation capacities only when
  exceeding them is structurally impossible or the corresponding constraint is
  explicitly exposed and validated in configuration.
- Strive for the most efficient practical implementation: bounded memory,
  explicit backpressure, minimal copies and allocations, useful concurrency,
  deterministic behavior, and no blocking work on async executor threads.
- Do not trade throughput or memory efficiency for convenience. A measured
  regression of at most **5%** is acceptable only when it buys a materially
  better interface or substantially more readable and maintainable code. Measure
  representative hot paths before accepting such a regression; do not guess.
- Correctness, durability, and liveness remain mandatory. If one of them requires
  a larger performance trade-off, document the evidence and choose the safe
  design rather than hiding the trade-off.
- Prefer native Rust libraries and in-process implementations. Avoid `libffi`,
  language bindings, helper sidecars, and equivalent cross-runtime machinery.
  Use one only when a native Rust solution is demonstrably impractical and the
  operational and performance cost is explicitly justified.
- Keep interfaces sink-neutral and parser-neutral. Put destination constraints
  in the sink contract and validate them during discovery and again before side
  effects. Do not leak ClickHouse or S3 details into generic pipeline code.

## Repository architecture

- `crates/transferia-core/` is the compiler-enforced stable data-plane API. It owns provider-neutral messages,
  Arrow datasets and schemas, discovery and sink-limit contracts, memory leases,
  and the runtime `Source`/`Sink` ports. `core` may depend on external primitive
  libraries, but never on providers, parsers, delivery preparation/execution,
  runtime adapters, or server code. The application crate re-exports it as
  `transferia::core`; do not recreate core types or compatibility wrappers under
  `src/`. Export the most important contracts from the core crate root so callers
  do not need to discover their storage layout.
- `crates/transferia-delivery/` owns delivery orchestration. `delivery/config/`
  owns runnable configuration; `delivery/preparation/` owns resolution,
  discovery, validation, and construction of a resolved `DeliveryPlan`; and
  `delivery/execution/` owns delivery-level partition startup and restart.
  Shared semantics, parser, middleware, retry, tracker, and metrics contracts
  belong to `crates/transferia-delivery-contracts/`. The provider-neutral
  read/parse/write/commit loop belongs to `crates/transferia-pipeline/`.
- `crates/transferia-registry/` defines configured provider and middleware
  factory boundaries, immutable component registration, UI definitions, and the
  provider-neutral `Composition` port. It is intentionally not
  `core::Source`/`core::Sink`: factories assemble parser, metrics,
  durable-storage, and runtime-port implementations around those core data-plane
  ports. Delivery orchestration depends on this neutral port and must never
  depend on concrete provider crates.
- `crates/transferia-provider-support/` owns provider-neutral parser,
  serializer, schema-registry, durable-storage, and address helpers. It must not
  depend on any concrete provider crate.
- Every heavyweight provider lives in its own `crates/transferia-provider-*`
  crate. Provider crates may depend on core, registry, delivery contracts, and
  provider support, but never on a sibling provider crate. Heavy client
  dependencies belong exclusively to their provider crate; the architecture
  checker enforces both rules.
- Every middleware implementation lives in its own
  `crates/transferia-middleware-*` crate and registers its typed configuration,
  runtime factory, and optional preview capability through `transferia-registry`.
  Middleware crates may depend on core, registry, and delivery contracts, but
  never on delivery orchestration, providers, or sibling middleware crates.
  Heavy execution engines such as DataFusion belong exclusively to their
  middleware crate; delivery and control-plane code must use registry ports.
- `crates/transferia-provider-logbroker/` owns Logbroker discovery, generated
  YDB protocol types, YDB Topic and PQv1 transports, protocol decoding, and
  source/sink behavior. Do not expose Logbroker/YDB transport details through
  provider-neutral modules.
- Provider source implementations live in mode-specific `src_batch/`,
  `src_stream/`, or `src_dblog/` modules. Keep provider-wide configuration and
  transport in the provider root; each mode extends those common pieces with
  its own settings. Do not create empty mode modules before an implementation
  exists.
- `crates/transferia-runtime/` defines the environment-neutral worker-runtime
  boundary. `crates/transferia-runtime-local/` owns local process supervision.
  Executable CLI/worker composition belongs to `crates/transferia-composition/`.
  Future Kubernetes or EC2 implementations must be sibling runtime adapters.
  Delivery execution itself remains in `crates/transferia-delivery/` and the
  provider-neutral per-partition pipeline lives in `crates/transferia-pipeline/`.
  Name provider components after their responsibility, such as `reader`,
  `writer`, `client`, or `actor`; never use a generic `runtime` module for
  provider logic.
- `crates/transferia-provider-clickhouse/` and
  `crates/transferia-provider-s3/` own all destination-specific validation and
  runtime behavior. The same ownership rule applies to Kafka, PostgreSQL, and
  YTsaurus in their respective provider crates.
- `tests/` contains cross-component integration and end-to-end tests.

Preserve the startup sequence: source discovery, semantic validation, sink-limit
validation, destination preparation, then workers. Runtime batches must be
validated before INSERT, upload, commit, or any other irreversible side effect.

## Testing rules

- **Never mix test bodies and production code in one file.** A production module
  may contain only a `#[cfg(test)] mod tests;` declaration. Put its tests in a
  sibling `tests.rs` or `tests/` subtree. Put cross-component tests in the root
  `tests/` directory. Test helpers shared by integration tests belong in a
  dedicated test-support module, not in production modules.
- When several production modules in one component have separate test files,
  collect them under that component's `tests/` directory (for example,
  `json_parser/tests/parser.rs`). Never create a directory named after a
  production file solely to hold its `tests.rs`.
- Every sink must have an automated end-to-end test that exercises its real
  wire/storage implementation. Use `testcontainers` or an equivalent hermetic,
  pinned service fixture. Fake transports are useful unit-test seams but do not
  satisfy this requirement.
- For external sinks, the E2E test must start the real compatible service (for
  example ClickHouse or an S3-compatible service), create required state, write
  through the production sink, verify destination data, and clean up. It must
  cover at least the durability/commit barrier and one representative failure or
  replay scenario. An in-process sink still needs a full pipeline integration
  test even when no container is meaningful.
- E2E tests must be part of the normal automated test command. Do not mark them
  ignored, silently skip them when Docker is absent, or claim completion without
  running them. If the required runtime is unavailable, report the task as not
  fully verified.
- Every bug fix needs a regression test that fails for the old behavior. Every
  destination-contract change needs startup-validation and runtime-validation
  coverage.
- Every constraint validated in the frontend must also be validated by the
  backend. Frontend validation is immediate UX feedback, never a trust or
  correctness boundary.

## Verification gates

### Hard prohibition outside release work

**Never run workspace-wide Clippy outside an explicit release/merge task.** In
particular, agents must not run this command during ordinary implementation,
refactoring, bug fixing, investigation, or completion:

```sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Only release/merge automation or a user request that explicitly asks for full
Clippy authorizes it. Generic requests such as "verify", "finish", "check the
change", "make sure it works", or "run the required gate" do **not** authorize
workspace Clippy. The same restriction applies to full workspace tests, E2E,
Docker checks, and full release builds. If another instruction presents the
release commands as a normal completion gate, this section is the project's
newer and more specific policy and takes precedence.

During implementation and before completing an ordinary repository change, run
`just check-affected`. This is deliberately a compile-only development gate:
it runs package-scoped `cargo check` for affected Rust packages and their
necessary dependents, and `tsc --noEmit` for frontend changes. It must not run
rustfmt, Clippy, tests, E2E, Docker, bundling/linking, code generation, contract
freshness checks, or architecture linters. Unknown or cross-cutting Rust changes
fall back only to workspace-wide `cargo check`, never to the release gate.
Inspect the exact plan with `just test-affected-dry`.

Do not run tests, Clippy, rustfmt, E2E, or full binary builds during ordinary
agent development or completion. Run a focused command only when the user asks
for it or when executing the code is essential to diagnose an observed runtime
failure. The compile-only gate is the normal completion evidence.

Run `just check-release` only in release/merge automation or when the user
explicitly requests the complete quality gate:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

The release test command includes all sink E2E/testcontainers tests. Do not
claim that the release gate passed when only the compile-only development gate
ran.

## Fast development and Cargo cache hygiene

Development latency is a product requirement. Preserve the following setup and
workflow instead of compensating for slow builds with broader parallelism or
repeated full-workspace commands.

### Use the smallest stable compilation surface

- Start with `just test-affected-dry` when the affected scope is not obvious,
  then run `just check-affected` once on the final tree. Do not repeatedly run
  it after every small edit.
- Prefer `cargo check` to `cargo build`: checking avoids code generation and
  linking. Build a binary only when the user needs to execute that binary.
- Keep one canonical development profile and feature set. Alternating between
  check/build/test/clippy, different feature combinations, `RUSTFLAGS`, target
  triples, or profiles creates distinct Cargo fingerprints and duplicates most
  artifacts.
- Package-scoped checks must include only the changed crate and the necessary
  reverse-dependency closure. A `Cargo.lock` change alone is not a reason to
  compile every workspace target. Treat actual compiler configuration,
  toolchain, protocol, or vendor changes as cross-cutting.
- Rust and frontend compile checks may run concurrently when independent. Do
  not run multiple Cargo processes against the same target directory: they
  serialize on Cargo's build lock and make elapsed time harder to diagnose.
- Record command durations. The affected selector writes its current report to
  `target/affected-tests-timings.json` and history to the adjacent JSONL file;
  use this evidence before broadening or optimizing a gate.

### Preserve crate-local ownership

- Provider-specific integration and E2E targets belong under that provider
  crate's `tests/` directory. The root `tests/` directory is only for genuinely
  cross-crate behavior. Otherwise checking one provider reconstructs a large
  monolithic root test target.
- Share integration-only utilities through `transferia-test-support`; never
  make production crates depend on root-test helpers.
- Keep generators lightweight. Server DTO/schema generation belongs to
  `transferia-server-contracts` and must not construct the provider catalog.
  Provider catalog generation is a separate, explicitly heavy operation. Do
  not make `cargo build`, normal typechecking, or API contract generation pull
  every provider or DataFusion into the dependency graph.
- Builds and checks should consume generated artifacts without rewriting them.
  Regeneration is an explicit task performed only when the owning contract
  changes.

### Keep caches bounded and reusable

- The repository pins Rust in `rust-toolchain.toml`. Do not casually change the
  toolchain, dev/test profiles, global rustflags, or workspace-wide features:
  each change invalidates a large portion of both Cargo and compiler caches.
- The project uses `.cargo/rustc-wrapper.sh` and `sccache`. Keep the sccache
  budget bounded (currently 50 GiB). A normal developer shell should use
  sccache; the wrapper may bypass it only in an environment whose sandbox blocks
  sccache IPC.
- `target/` is not a bounded cache. Do not use `cargo clean` routinely because
  a healthy target directory is valuable. Clean it only after evidence of
  pathological fragmentation, incompatible build variants, or severe disk
  pressure. Never delete `~/.cargo` as a generic build-speed fix; registry and
  source caches are reusable.
- Keep at least roughly 15–20% filesystem capacity free. A nearly full APFS or
  equivalent filesystem plus a huge flat `target/*/deps` directory can turn
  metadata operations into minutes of I/O wait even when rustc consumes almost
  no CPU.
- Development profiles intentionally disable incremental compilation and split
  debug info so sccache can reuse outputs and Cargo does not create enormous
  populations of tiny files. Change this only with measured evidence from this
  workspace.

### Diagnose a slow or apparently hung build before retrying

- Do not start a second Cargo command. Inspect the existing Cargo process and
  its rustc child first. Cargo often appears idle because it is waiting for one
  compiler, linker, archiver, filesystem operation, or the build-directory
  lock.
- Cargo timing output identifies the slow crate but not necessarily the blocked
  syscall. On macOS, trace the actual rustc child PID, not the parent Cargo PID,
  with `sample <rustc-pid>` and `sudo fs_usage -w -f filesystem <rustc-pid>`;
  correlate it with `iostat` and `vm_stat`. A trace of only the Cargo parent
  cannot prove where rustc waited.
- Check `df -h`, `du -sh target`, the population of `target/*/deps`, duplicate
  `.fingerprint` variants, and `sccache --show-stats`. Near-zero rustc CPU plus
  a very large, slow-to-enumerate target directory is evidence of storage and
  metadata pressure, not insufficient Cargo job parallelism.
- Cargo already schedules work across available CPUs. Raising `jobs` far above
  the CPU count does not shorten a dependency-chain critical path and can worsen
  memory and I/O contention. Optimize dependency boundaries and cache reuse
  before changing parallelism.

## Performance log contract

- Every new source and sink must report through `SourceCounters`, `ParseCounters`,
  and `SinkCounters` so the standard reporter emits one stable, parseable line per
  partition and interval. Do not invent provider-local throughput log formats.
- Preserve the named `[stats p=<partition>]` sections and units emitted by
  `src/metrics/mod.rs`: source message/compressed/decompressed rates and response
  wait, parser rows/Arrow/DLQ/source-message rates, sink rows/bytes/flushes/source-
  message rates, attempt load, retries, buffering/object gauges, backpressure,
  delivery guarantee, CPU, and RSS.
- A provider that does not perform a stage must report zero/`N/A` through the
  common counters; it must not remove or reorder fields. Sink `busy` is attempt
  load, not CPU utilization, and may exceed 100% when operations are concurrent.
- Any deliberate log-contract change must update `scripts/stats_avg.py`,
  `scripts/run_single_partition_benchmark.py`, their separate tests, and
  `docs/benchmarks.md` in the same commit.
- Before accepting a new provider, feed representative provider log lines through
  `scripts/stats_avg.py` and add a regression fixture proving they parse. Use the
  restored aggregator as `python3 scripts/stats_avg.py transferia.log` or pipe
  logs on stdin; use `--json` for automation.

## Change hygiene

- In every non-empty Rust configuration struct and struct-like configuration
  enum variant, separate adjacent fields with exactly one blank line. A field's
  doc comments and attributes belong to that field: keep them together without
  blank lines, and put the separator before the comments or attributes of the
  following field. Apply this convention to shared, nested, provider, parser,
  sink, source, and internal configuration types alike, even when a helper type
  does not have a `Config` suffix.
- Search for and remove obsolete names, options, comments, tests, and docs after
  each conceptual change.
- Keep the working tree's unrelated user changes intact.
- Make failures explicit and typed; deterministic configuration/schema errors
  fail fast, while genuinely transient external failures use bounded, observable
  retry behavior unless the process-level policy explicitly says otherwise.
- Keep credentials out of logs and require an explicit trust decision for
  plaintext transports.
- Handoffs must state what changed, what was deleted, performance implications,
  exact verification commands, and any principle exception or unverified risk.
