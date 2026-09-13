# Validation boundaries and migration audit

The governing policy is **Maximum shift-left validation and explicit contracts**
in `AGENTS.md`. This document records the initial audit, not certification of the
whole repository. Vendor code, generated contracts, and dependency internals are
not migration targets. Existing runtime defenses must remain until their inputs
are demonstrably protected by construction and cannot change afterward.

## Required boundary model

1. Raw configuration and UI drafts can be incomplete. Their types must not claim
   execution guarantees. Parsing must reject malformed representations.
2. Constructors/factories validate all intrinsic constraints before publishing
   operational objects. Private state and invariant-preserving mutations protect
   the guarantee afterward; successful parsing alone is not a validated plan.
3. Discovery establishes external schema and identity contracts. Validate the
   complete delivery before preparing destinations or starting workers.
4. Arrow/runtime construction checks newly received data. Runtime checks still
   protect against drift, changed external state, and destination constraints
   that depend on actual records, before irreversible side effects.

## Implemented in the initial pass

- `crates/transferia-core/src/delivery.rs`: discovery projection validation now
  rejects repeated system-column kinds even when their names differ. Each semantic
  role must have one unambiguous physical representation. Runtime validation stays.
- `crates/transferia-delivery/src/delivery/preparation/mod.rs`: middleware factories
  and pattern compilation run before connector construction/source discovery.
  Schema-dependent middleware validation still follows discovery. This applies to
  each pipeline; it is not yet a delivery-wide static preflight for all pipelines.
- `crates/transferia-connector-clickhouse/src/connectors/clickhouse/src_batch/`:
  Parquet decode channel capacity is validated in config validation and again in
  the transport constructor, using the same function. Positive decoder count must
  fit two channel slots per decoder within Tokio's semaphore capacity. The private
  transport stores the checked capacity; it no longer clamps or saturates it when
  opening a stream. No extra per-record validation or allocation was introduced.

Regression cases accompany these changes. Compile-only checking is not execution
of those tests and is not proof of repository-wide contract completeness.

## Open migration work identified by the audit

| Owner | Gap | Required migration |
| --- | --- | --- |
| Core `data/system_columns.rs`, `data/table_data.rs` | Unchecked metadata construction; mutable batch/metadata pairing | Validate unique roles/indexes/names at metadata construction, schema-relative properties at batch pairing; close mutable bypasses and migrate callers. |
| Core `delivery.rs` | Public topology variants accept empty/negative/duplicate partitions | Validated private partition collection preserving exact order; migrate construction and assignment. |
| Core `memory.rs` | Zero admission budget panics in constructor | Nonzero input or fallible construction; propagate configuration errors and migrate callers. |
| Core `sink.rs`, contracts `delivery_tracker.rs` | Saturating delivery sequence increment can repeat an identity at exhaustion | Checked advancement before publishing/tracking; explicit terminal error without overwriting pending state. |
| Delivery `preparation/mod.rs` | Public mutable plans can bypass established validation | Seal execution artifacts, provide read-only inspection and controlled consuming execution API. |
| Delivery `execution/runner.rs` | Execution phase shape is checked during startup, pipeline by pipeline | Validate every phase plan before any pipeline starts; preserve authoritative rediscovery checks. |
| Contracts `metrics.rs` | Zero interval silently becomes one millisecond | Positive interval type at parsing/construction, remove normalization, update emitted configuration schema and callers. |
| Logbroker `pqv1/src_stream/mod.rs` | Decompression semaphore only validates positivity | Validate Tokio capacity at outer and direct constructors using one contract, expose constraint, test boundaries. |
| Logbroker public PQv1 API | Direct API lacks outer factory's plaintext trust decision | Restrict API or carry the explicit trust contract; migrate direct callers/tests. |
| ClickHouse `src_batch/config.rs` | Unsupported-type default is potentially irreversible `ToString` | Resolve lossless default (`Fail`) and explicit conversion opt-in consistently across catalog, docs, and tests. |

These are findings, not an exhaustive list. The first pass inspected core
data/discovery/memory/sink contracts, delivery preparation/execution, registry and
pipeline boundaries, connector factory validators, middleware constructors, and
selected control-plane entry points. It did not read and prove every source file
or every alternate constructor, mutation, deserializer, and runtime path. Frontend
draft-state and all remaining contracts still require systematic review.

For each migration, document the precise guarantee next to the owning type,
update all construction paths together, and cover rejected input plus valid edge
cases. Do not replace this work with blanket `validate()` calls, panics, or a claim
that a search for constructors proves correctness.
