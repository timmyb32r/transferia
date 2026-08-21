# Foundation hardening specification

Status: approved on 2026-08-16.

## Goal

Remove the remaining architectural debt in the core data-plane contracts,
extension API, frontend controller/schema renderer, and verification gates
without changing user-visible delivery semantics or regressing hot-path
performance.

## Non-goals

- No new connector, parser, sink, or user-facing redesign.
- No compatibility layer for internal Rust APIs.
- No `ya make`.

## Decisions

1. Every mandatory Cargo quality command covers the complete workspace.
2. Source and sink contracts return an explicit retryable/fatal data-plane
   failure; the execution layer never infers disposition from an untyped error.
3. Installation schemas, initial values, resolver inputs, and resolver outputs
   are registered through typed Rust values. Raw JSON/YAML is private erasure.
4. Dynamic-option work has an explicit cancellation token and deadline from
   browser request through extension I/O.
5. Frontend async workflows live in focused hooks/controllers; `App` composes
   view state and commands.
6. Generic schema traversal dispatches presentation through `x-ui` widgets and
   does not branch on connector/config property names.
7. Labels target real controls with schema-path-stable IDs.
8. JSON strings remain zero-copy. Their validity and source-buffer lifetime are
   encoded in the stored value; no byte range may be paired with another buffer.
9. JSON parsing orchestration, conversion/builders, typed scratch, framing, and
   memory estimation are separate responsibilities without per-row dispatch or
   allocation regressions.

## Acceptance criteria

- `just check` executes `transferia-core` tests and lints.
- Every source/sink failure has an explicit disposition; commit-marker type
  mismatches are fatal and transient transports remain retryable.
- Extension installation input/output types drive runtime decoding and schemas;
  resolvers cannot emit undeclared connector fields.
- Cancelling a dynamic-options request stops backend pagination and stale
  results cannot update a newer control.
- Existing navigation/save/poll/YAML/discovery race tests remain green and each
  extracted controller has focused tests.
- A new schema widget is added by registry entry; unsupported widgets fail at
  catalog compilation.
- Labels focus scalar, enum, password, and repeated-row controls.
- Zero-copy JSON has a local, Miri-covered safety contract and remains within
  the repository's 5% hot-path performance budget.
- Core and plugin formatting, strict clippy, tests, frontend tests/build, real
  E2E tests, Miri (where available), benchmark comparison, and `git diff
  --check` pass on the final tree.

## Risks

- Failure typing changes every connector boundary at once.
- Typed installation registration changes the separately located Yandex plugin
  in lockstep.
- Parser lifetime changes are performance-sensitive and require benchmark and
  Miri evidence.
- Frontend extraction must preserve all existing causal response guards.

## Unresolved questions

None. Zero-copy with explicit source-buffer identity was selected.
