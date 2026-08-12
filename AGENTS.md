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
   the reason and trade-off in the handoff.

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

- `src/delivery.rs` owns discovered dataset contracts and declarative sink
  limits.
- `src/providers/traits.rs` defines source and sink provider boundaries.
- `src/pipeline/` owns delivery flow, memory accounting, retries, middleware,
  and commit ordering.
- `src/providers/pqv1/` owns PQv1 discovery, transport, protocol, decoding, and
  source behavior.
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

## Mandatory completion gate

Before completing **every repository-changing task**, run all of the following
from the repository root on the final, stable tree:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

The test command must include and pass all sink E2E/testcontainers tests. Also run
focused stress, replay, or benchmark checks proportional to the risk of the
change. Do not reuse results from before the last edit, do not dismiss failures
as unrelated, and do not report completion while any required gate is red or was
not executed.

## Change hygiene

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
