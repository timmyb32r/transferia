# Table selection autopilot obligation ledger

Authoritative scope: `docs/table-selection.md` and the approved interview.
Invocation started 2026-09-06. No accepted obligations cancelled.

## Current verification

- Compile-only `TRANSFERIA_SKIP_SERVER_UI=1 just check-affected`: passed,
  final all-empty policy tree: Rust 14.03s, TypeScript 3.82s. Not the release gate.
- Final empty-selection regression checks: registry matcher 20 passed;
  table-selection UI 7 passed (3.02s). API regenerated with `no_tables` issue.
- MySQL library: 116 tests passed, including New tables schema/default.
- Live MySQL 8.4.6 CREATE/replay/ack/restart: passed for stream and batch+stream,
  across databases without a connection database. Both verify RENAME rejection
  without checkpoint advancement. Two tests: 6.88s.
- Live MySQL startup catalog race and explicit Ignore policy: passed, 5.71s.
- Pipeline admission: 4 passed (drain, durable-read barrier, rejection,
  cancellation without acknowledgment).
- Delivery admission coordinator: prepares only the new table and rejects
  duplicate names before side effects; passed.
- Frontend table-selection and editor chrome: 26 passed, 1.95s.
- ClickHouse source unit tests rerun: 15 passed.
- Earlier evidence: registry matcher 19; PostgreSQL source 11; frontend 474;
  real MySQL exact snapshot handoff and ClickHouse source E2E.
- Browser fixture verified gating, immediate invalidation, preserved rule text,
  5/40-row expansion in the same 140px viewport and stable document coordinates.
  It caught missing text-input types, now fixed and regression-covered.
  Include/Exclude both measure 441px on the 980px fixture.
- API/catalog generated. No workspace tests, Clippy, formatting, release build
  or commits performed.

## Obligations

| ID | Obligation | Status | Evidence / remaining work |
| --- | --- | --- | --- |
| R1 | Shared exact/glob/regex, lossless qualified names, per-card exclusions and both conflicts | verified | 19 registry tests; shared preview/startup/admission classifier |
| R2 | Authenticated accessible PG/MySQL/CH catalog, structured namespace/name | verified | PG privilege filters; MySQL SELECT probe; CH CHECK GRANT; network-only cannot enable editor |
| R3 | Fixed PG/CH selection and empty-match policy | implemented | Latest decision: every empty card always fails; configurable policy removed from UI, YAML and schemas. Regression tests updated. |
| R4 | MySQL optional database and cross-database restart identity | verified | Live cross-database CREATE/restart in both modes with database omitted |
| R5 | MySQL sink override/namespace, missing namespace and collision rejection | verified | Qualified SQL, 16 focused sink tests, startup and admission collision checks |
| R6 | New tables auto/default and fixed option, prepare before rows, atomic checkpoint and restart | verified | Live CREATE/restart, locked startup race, Ignore, pipeline/coordinator barriers. All-empty startup remains R3. |
| R7 | Rename fails before rows/ack with old/new names and rule; DBLog future | verified | AST forms, decoder position regression and real MySQL in both modes |
| R8 | Rule cards, suggestions, modes, bounded previews | verified | UI tests and browser fixture; wide inputs, fixed viewport |
| R9 | Authenticated check gates add/launch; connection edits invalidate but preserve rules | verified | Hook pending/dedup/fingerprint tests; browser invalidation |
| R10 | Question-mark syntax/exclusion/empty/fixed/creation-only help | verified | Widget and generated New tables descriptions; help explicitly distinguishes individual empty rules from invalid empty combined selection |
| R11 | Contracts, fixtures, examples and documentation | verified | API/catalog generated; obsolete source-list fixtures migrated |
| R12 | Compile, focused tests, browser and final reconciliation | verified | Final all-empty policy compile and focused tests passed; all obligations reconciled. |

All product choices are resolved. The all-empty policy is shared by preview and
startup resolution for PostgreSQL, MySQL and ClickHouse.
