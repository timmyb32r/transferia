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

## Performance and design

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
- `src/delivery/` owns delivery orchestration. `delivery/config/` owns the
  runnable configuration; `delivery/semantics.rs` owns cross-provider delivery
  compatibility; `delivery/preparation/` owns configuration resolution,
  discovery, validation, and construction of a resolved `DeliveryPlan`;
  `delivery/execution/` owns partition execution, pipeline flow, retries,
  middleware, and commit ordering.
- `src/providers/traits.rs` defines configured provider factory boundaries. It
  is intentionally not `core::Source`/`core::Sink`: factories assemble parser,
  metrics, durable-storage, and runtime-port implementations around those core
  data-plane ports.
- `src/providers/logbroker/` owns Logbroker discovery, generated YDB protocol
  types, YDB Topic and PQv1 transports, protocol decoding, and source/sink
  behavior. Do not expose Logbroker/YDB transport details at crate root or in
  generic provider modules.
- Provider source implementations live in mode-specific `src_batch/`,
  `src_stream/`, or `src_dblog/` modules. Keep provider-wide configuration and
  transport in the provider root; each mode extends those common pieces with
  its own settings. Do not create empty mode modules before an implementation
  exists.
- `src/runtime/` defines the worker-runtime boundary. Environment-specific
  process ownership, readiness, shutdown, and parent-worker control belong in
  `src/runtime/local/`; future Kubernetes or EC2 implementations must be sibling
  runtime adapters. Delivery execution itself remains in `delivery/execution/`.
  Name provider components after their responsibility, such as `reader`,
  `writer`, `client`, or `actor`; never use a generic `runtime` module for
  provider logic.
- `src/providers/clickhouse/` and `src/providers/s3/` own all destination-specific
  validation and runtime behavior.
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

## Mandatory completion gate

Before completing **every repository-changing task**, run all of the following
from the repository root on the final, stable tree:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

The test command must include and pass all sink E2E/testcontainers tests. Also run
focused stress, replay, or benchmark checks proportional to the risk of the
change. Do not reuse results from before the last edit, do not dismiss failures
as unrelated, and do not report completion while any required gate is red or was
not executed.

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
