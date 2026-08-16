# Local control plane architecture

The local control plane owns delivery drafts, validation, worker processes, and the embedded web UI. It is intentionally local and single-user: it binds to loopback by default and has no remote authentication boundary.

## Boundaries

| Module | Responsibility |
| --- | --- |
| `application/delivery_plan.rs` | The one startup/preflight sequence shared by CLI workers and the control plane |
| `providers/catalog.rs` | The only provider registration catalog: runtime factories, schemas, initial form values, and supported delivery modes |
| `server/service.rs` | Delivery use cases and state transitions; depends only on the storage and supervisor ports |
| `server/store.rs` | `DeliveryStore` port and the transactional local JSON implementation |
| `server/supervisor.rs` | `WorkerSupervisor` port and the local child-process implementation |
| `server/api_contract.rs` | The Rust-owned HTTP request/response DTOs and generated JSON Schema root |
| `server/http.rs` | Routing, error classification, body limits, headers, and embedded assets |
| `server/ui_catalog.rs` | The root form schema composed with the provider catalog |
| `web/src/api/` | Runtime decoding of every server response against the generated Rust contract |
| `web/src/delivery/` | Delivery-editor composition and feature views |
| `web/src/schema/` | Strict JSON Schema compiler, generic form controls, and specialized schema widgets |
| `web/src/ui/` | Reusable overlay and select primitives |
| `web/src/app.tsx` | Application orchestration only: sessions, effects, navigation, and commands |

Do not let HTTP DTOs, JSON persistence, or `tokio::process::Child` enter `ControlPlane`. A replacement database implements `DeliveryStore`; a container or remote launcher implements `WorkerSupervisor`. Neither change should alter application use cases.

## Delivery lifecycle

Draft edits increment the configuration `revision` and invalidate its previous
validation. Every persisted mutation, including runtime-only transitions, uses a
separate monotonic `record_version` for compare-and-swap. Each activation gets a
fresh `run_id`; worker events and stop requests are applied only when that ID
still matches the delivery's current run. A delayed exit from an old process can
therefore never overwrite a newer activation.

Every update, validate, activate, and stop request carries both the expected
configuration revision and expected record version. Stop additionally carries
the expected run ID. The application verifies these causal preconditions before
persisting a transition or contacting the supervisor; delayed requests fail
with `409 Conflict` instead of acting on newer state.

Validation records either `ready(revision)` or `invalid(revision)`. Activation
accepts only the current persisted and validated revision and performs the full
preflight once in the parent. Installation resolvers produce the exact
`ResolvedDeliveryConfig` that was validated. The worker receives that config in
a private `0600`, create-new file, verifies the compiled-composition fingerprint,
and does not invoke installation resolution again. The file is removed after
the worker reports readiness.

The server owns every worker from the moment it is spawned, including while it
is starting. Its authenticated loopback control channel provides readiness and
stop commands. Normal server termination stops starting and running workers;
loss of the parent control channel also cancels the worker. Persisted transient
states are normalized to `stopped` at the next server start and advance the
record version; restart never activates them automatically.

## Persistence and secrets

Only one server process may own a state directory at a time; a lifetime-held
file lock makes a second open fail fast. The JSON store uses `record_version`
CAS, writes a complete candidate to a private temporary file, synchronizes it,
and atomically renames it. Failures before rename leave memory and disk
unchanged and remove the temporary file. Rename is the explicit logical commit
point: if the following directory sync fails, memory follows the committed disk
state and emits an explicit high-severity durability diagnostic while the
application continues from the committed state. The state directory is mode `0700`; state, locks, configs,
and logs are mode `0600` on Unix.

The state directory contains its own deny-all `.gitignore`, and the repository ignores `.transferia-server/`. Delivery lists expose metadata only. Full delivery configuration is returned only by the detail endpoint needed by the local editor; internal errors are logged server-side and converted to a stable generic HTTP error.

## UI schema contract

The browser compiles a deliberately small JSON Schema subset: local `$ref`,
objects, arrays, scalar types, string enums/constants, nullable schemas,
numeric ranges/formats, and `oneOf`/`anyOf`. Recursive references, unsupported
formats/keywords, and schema-valued `additionalProperties` fail fast instead of
silently rendering the wrong control. A Rust test feeds the real compiled
provider catalog through this TypeScript compiler, so backend schema evolution
cannot bypass the UI contract.

Editor session IDs, local revisions, persisted record versions, and per-operation
request IDs bind every asynchronous response to the state that created it. Save,
open, polling, actions, YAML, discovery, and dynamic options all discard stale
successes and stale failures. YAML values are parsed literally in both UI and
runtime; there is no whole-document environment expansion. Frontend validation
is only guidance: activation always repeats authoritative server-side
validation.

## Server API contract

`server/api_contract.rs` is the only source of truth for control-plane request
and response shapes. Its Rust DTOs derive `JsonSchema`; the
`generate-server-api` binary materializes that schema as
`contracts/server-api.schema.json`. The web generator projects the committed
schema into `web/src/generated/apiContract.ts`, and the API client validates
every successful response and every structured error at runtime before it can
enter application state. Frontend code must import these generated types through
`web/src/types.ts`; it must not restate server DTOs with handwritten TypeScript
interfaces or unchecked casts.

Run `just api-contract` after changing a server DTO. The temporary
`TRANSFERIA_SKIP_SERVER_UI` build flag deliberately breaks the generation cycle:
the Rust generator can compile before the frontend consumes the newly generated
schema. Normal builds never set this flag. Rust freshness tests compare the
committed schema and interop fixture to their Rust-generated values, while
Vitest decodes that exact Rust serialization fixture and rejects malformed
responses.

## Verification contracts

`cargo build` regenerates TypeScript types, type-checks, and bundles the embedded
UI from the committed Rust schema. The normal Cargo test
suite invokes Vitest and also runs the real Rust catalog through the TypeScript
schema compiler. `just check`/`just ci` enforce formatting, all-target/all-feature
Clippy, Rust tests, UI contracts, and configured sink E2E tests. The internal
extension has its own Cargo tests and never requires `ya make`.
