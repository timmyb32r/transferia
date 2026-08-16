# Core architecture hardening specification

Status: approved for implementation on 2026-08-16.

## Goal

Turn the current internal architecture into a compiler-enforced foundation for
future providers, runtimes, parsers, and UI features. The change must preserve
all observable delivery behavior and data-path performance while reducing the
number of places that can violate core contracts.

## Non-goals

- Backward compatibility for internal Rust module paths or traits.
- A new distributed runtime, provider SDK, parser feature, or UI redesign.
- Abstracting Tokio out of the local data-plane API before a second executor
  exists.
- Splitting files solely to satisfy a line-count target.
- Changing configuration, HTTP, persistence, or provider wire formats.

## Constraints

- `transferia-core` is a separate workspace crate and cannot depend on the
  application crate, providers, parsers, metrics, durable storage, runtime, or
  server.
- The application crate may re-export `transferia-core` as `transferia::core`,
  but core dependency direction is enforced by Cargo rather than convention.
- Data-path changes remain allocation-neutral unless a measured regression is
  justified. Commit-marker erasure may not add a per-message allocation.
- Provider construction remains object-safe and supports runtime catalog
  selection.
- Existing provider, parser, sink, server, plugin, and E2E behavior remains
  unchanged.
- Tests remain outside production files, in accordance with `AGENTS.md`.

## Decisions

### 1. Workspace and core boundary

- The repository becomes a Cargo workspace containing the existing `transferia`
  application package and a private `transferia-core` library package.
- Central data-plane contracts move physically into `transferia-core`: messages,
  schemas, system columns, discovered delivery contracts, topology, source and
  sink interfaces, and pipeline memory accounting.
- The application package publicly re-exports the crate as `transferia::core`
  so external composition crates have one stable import surface.
- Core owns only dependencies required by those contracts. It does not acquire
  provider, parser, server, extension, or control-plane dependencies.

### 2. Provider phase contexts

- Provider traits receive named request/context values instead of positional
  bundles:
  - `SourceDiscoveryContext` contains the discovery request and cancellation;
  - `SourceBuildContext` contains partition, cancellation, memory, and durable
    services;
  - `SinkBuildContext` contains partition, counters, projection policy,
    discovery, and durable services.
- The contexts model existing phases only; no speculative common context or
  service locator is introduced.
- All providers and direct tests use these contexts, preventing parameter-order
  mistakes and allowing individual phases to evolve independently.

### 3. Commit-marker contract

- Provider implementations no longer manually downcast public `Any` payloads.
- Marker erasure and type checking live behind a core-owned API. Each marker
  carries a stable Rust type identity, and extraction returns a typed error that
  identifies the expected marker type rather than permitting a panic.
- Batch commit remains one provider operation over a marker slice and marker
  cloning remains `Arc`-cheap.
- Regression tests prove that matching markers round-trip and mismatched markers
  fail explicitly without panicking.

### 4. Catalog decomposition

- The catalog remains one source of provider truth, but its implementation is
  separated by responsibility:
  - public definitions and typed endpoint specifications;
  - static provider descriptors and output contracts;
  - compiled runtime registry and validation;
  - builtin installation schemas and overlay application.
- No registration metadata or validation rule is duplicated by the split.
- Existing catalog and extension contract tests continue to exercise the public
  composition path rather than private helpers.

### 5. JSON parser decomposition and invariant failures

- The parser is split along existing internal responsibilities: Arrow builders
  and conversion, system-column materialization, JSON extraction/session state,
  and orchestration.
- Hot per-row functions remain monomorphic and inlineable; the split may not add
  dynamic dispatch or intermediate JSON values.
- Runtime message metadata required by enabled system columns is validated at a
  fallible boundary. Missing metadata produces an explicit parser error instead
  of reaching `expect` in a release binary configured with `panic = "abort"`.
- Internal impossible builder-shape states remain documented invariants, while
  provider-supplied runtime data never relies on process-aborting assertions.

### 6. Frontend decomposition

- Existing large components are split only where state and tests already expose
  a stable boundary: application orchestration, delivery actions/navigation, and
  schema widgets.
- Async causal guards remain centralized; child components emit commands and do
  not independently own API request races.
- Form-local state continues to be keyed by editor session. No visual or workflow
  change is part of this work.

## Acceptance criteria

1. Cargo metadata shows a distinct `transferia-core` package with no dependency
   path back to `transferia`.
2. Existing users can import public contracts through `transferia::core`, while
   the Arcadia extension can depend directly on `transferia-core` where useful.
3. No provider build implementation accepts the former positional source tuple
   or the old generic `SinkContext` name.
4. No provider source calls `CommitMarker::downcast_ref`; a marker mismatch is a
   regular typed error covered by a regression test.
5. Missing topic, partition, offset, or timestamp metadata for an enabled system
   column returns an error and cannot abort the process.
6. Catalog and parser modules are separated by responsibility without duplicate
   authoritative tables or new data-path allocations.
7. Frontend unit tests demonstrate unchanged editor/session and schema behavior
   after component extraction.
8. The Arcadia compile-time extension builds and its tests pass against the new
   package structure.
9. `cargo fmt --all -- --check`, Clippy with warnings denied, and all Rust,
   frontend, plugin, and real sink E2E tests pass on the final stable tree.

## Risks and mitigations

- A workspace split can accidentally duplicate dependency versions. Workspace
  dependency resolution and `cargo tree -d` inspection are used to prevent it.
- Type-erased provider selection inherently needs an erased marker boundary.
  Erasure stays private to core and every mismatch becomes a typed error.
- Moving large parser sections can produce subtle hot-path regressions. The code
  is moved without algorithm changes and focused parser tests run before the full
  gate.
- Mechanical module moves can obscure user changes. The work starts from a clean
  tree and uses narrow patches with repeated compile checks.

## Unresolved questions

None. Where multiple abstractions were possible, this specification chooses the
smallest design that enforces a current boundary and rejects speculative runtime
or UI frameworks.
