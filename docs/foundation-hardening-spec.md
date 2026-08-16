# Foundation hardening specification

Status: approved for implementation on 2026-08-16.

## Goal

Make the control plane, UI, provider catalog, worker lifecycle, and extension
boundary safe foundations for further growth. The system must preserve the exact
user-authored configuration, make every state transition causally attributable,
fail before side effects when a contract is invalid, and expose one executable
contract to the server, worker, UI, and compile-time extensions.

## Non-goals

- Backward compatibility with experimental HTTP, persisted-state, catalog, or
  extension APIs.
- Distributed control-plane operation or a remote worker backend.
- Authentication for non-loopback deployment. This iteration keeps the server
  loopback-only and defends that boundary explicitly.
- Automatic recovery or restart of deliveries after server restart. Workers stop
  with the server and deliveries remain `Stopped` until manual activation.
- A frontend framework rewrite or visual redesign.

## Constraints

- A provider is registered exactly once in the core catalog. UI metadata,
  capabilities, configuration codec/schema, initial value, installation output
  contract, and runtime factory are derived from that registration.
- Extensions depend on core; core never depends on an Arcadia extension.
- Resolvers and workers are cancellable and bounded. No unbounded pagination,
  retry, memory growth, or startup wait is allowed.
- No silent user-visible transformation is allowed. In particular, a delivery
  name with leading or trailing whitespace is rejected explicitly; it is never
  trimmed.
- Every frontend validation rule that protects correctness has an equivalent
  authoritative backend validation.
- Secrets remain outside logs and resolved worker artifacts use owner-only
  permissions and are removed after worker readiness or failure.
- All tests live outside production files as required by `AGENTS.md`.

## Decisions

### 1. Command and concurrency contract

- Every mutable delivery has an opaque `record_version`, serialized over HTTP as
  a decimal string so JavaScript cannot lose integer precision.
- Update, validate, activate, and stop commands require
  `expected_record_version`. Stop additionally requires `expected_run_id`.
- A stale token fails with HTTP 409 before filesystem, process, or provider side
  effects.
- Validate returns the authoritative post-command `DeliveryRecord` together with
  optional discovery data. A semantically invalid delivery is a successful
  command result with persisted `Invalid` validation state, not a transport
  failure followed by a compensating GET.
- Create/update returns the committed record independently of best-effort sidebar
  refresh. Projection refresh errors are displayed separately and never change
  mutation success.

### 2. Exact user input

- Delivery names must be non-empty, at most 128 bytes, and equal to their own
  `trim()` result. Leading/trailing whitespace produces a typed validation error.
- No server-side trim, normalization, or substitution occurs.
- Numeric UI controls represent empty input as absence, never as zero.

### 3. Browser-local security boundary

- The server remains loopback-only.
- Requests with a present `Host` or `Origin` outside localhost/IPv4-loopback/
  IPv6-loopback are rejected before routing. Missing `Origin` remains valid for
  CLI clients; malformed headers are rejected.
- CSP includes `frame-ancestors 'none'`, `base-uri 'none'`, and
  `form-action 'none'`. API responses remain `no-store`.
- Reverse-proxy/custom-host operation is deliberately unsupported until it has an
  explicit authenticated deployment mode.

### 4. UI causal consistency

- Every asynchronous result is bound to editor session, delivery id, local edit
  revision, persisted config revision, and record version as applicable.
- Opening another delivery invalidates pending save/validate/action/poll/YAML/
  discovery results and remounts form-local state. Selected rows and password
  visibility cannot cross sessions.
- YAML shown for editing must correspond to the current local revision. Switching
  tabs waits for or requests that exact revision; stale YAML is never copied into
  the editor.
- Discovery completeness includes common delivery configuration as well as both
  endpoints.
- Renaming or disabling a selected system column updates/removes its primary-key
  reference atomically. The backend independently rejects dangling keys.
- Dynamic selects retain and visibly mark an existing configured value when the
  option source is unavailable or no longer returns it.

### 5. Worker lifecycle and resolved-plan provenance

- A worker reports READY only after every assigned partition has successfully
  constructed its source and sink for the first attempt. The existing bounded
  supervisor startup timeout is the activation deadline.
- Shutdown cancellation begins worker termination immediately, concurrently with
  HTTP graceful drain. Server exit waits for both and aggregates errors.
- Startup cancellation and stop failures are observable terminal failures; kill
  errors are never converted to `Stopped`.
- An activation passes one opaque `ResolvedDeliveryConfig` envelope from the
  exact successful plan to the supervisor. It contains its own composition
  fingerprint; callers cannot stamp or pair unrelated YAML and fingerprints.
- Unresolved and resolved configurations are distinct types. A resolved config is
  not run through installation resolution again in the child.
- Runtime topology is modeled once. Static source partitions and dynamic worker
  lanes are distinct, and discovery/runtime assignment consume the same topology
  contract.

### 6. Catalog and extension boundary

- One typed provider specification owns identity, title, supported roles,
  delivery capabilities, partition model, config codec/schema/default, runtime
  factory, installation replacement contract, and explicit contract version.
- Catalog definitions are compiled once into immutable shared data and reused by
  UI, validation, and runtime factory materialization.
- Installation registration is typed at its input boundary. Schema is derived
  from the Rust input type. Its UI draft seed is allowed to omit a deliberately
  unselected required choice, but every present field must satisfy the schema;
  executable decoding remains mandatory once the form is complete.
- Resolver output is a provider-role-specific typed patch. It must contain exactly
  the declared replacement fields and may not overwrite any other configuration.
- Extension registrations for unknown provider/role pairs, duplicate/default
  ambiguity, invalid initial values, or unsupported UI schema dialect fail during
  composition.
- Composition fingerprint covers explicit core/extension ABI versions and the
  canonical executable contract. Contract behavior changes require a version
  change and produce a different fingerprint.

### 7. Resolver safety and Yandex extension policy

- Resolve context carries cancellation and a deadline. Source and sink resolution
  may run concurrently and must stop on cancellation/deadline.
- External pagination rejects repeated page tokens and enforces explicit page and
  item limits.
- MDB cluster ids are validated before I/O as lowercase ASCII alphanumeric ids of
  bounded length and are inserted as URL path segments, never interpolated into a
  raw URL suffix.
- PostgreSQL managed resolution has no implicit role fallback. Sink requires one
  alive master. Source selection is an explicit installation choice; an absent or
  ambiguous selection fails before returning a host.
- Logbroker managed resolution requires an explicit plaintext trust
  acknowledgement until a verified TLS transport is implemented. The resolver
  never injects trust by itself.
- Plugin-generated schemas are run through the same supported UI-schema contract
  gate as the public core catalog.

### 8. API and schema contracts

- HTTP request DTOs deny unknown fields.
- Rust is the source of truth for server DTO schemas. TypeScript types and runtime
  response validators are generated from or checked against those schemas; blind
  casts at the network boundary are forbidden.
- Optional Rust fields and TypeScript nullability agree exactly.
- The supported JSON Schema plus `x-ui` dialect is explicit and fail-fast.
  Unsupported keywords, widgets, or malformed hints reject catalog composition.
- Structural schemas are compiled once per catalog revision rather than repeatedly
  during render.

## Acceptance criteria

1. A create/update name containing leading or trailing whitespace fails with a
   stable typed error and the exact submitted name is never persisted in changed
   form.
2. A validate response alone moves the UI to the returned authoritative
   `record_version`; no follow-up GET is required.
3. Save succeeds even when the subsequent list refresh fails, with a distinct
   projection warning.
4. A delayed stop for run A cannot stop run B and returns 409 without calling the
   supervisor.
5. Hostile Host/Origin headers and iframe embedding policy have automated route
   tests.
6. Switching deliveries while requests are pending cannot alter the newly opened
   editor; form-local selections and secret visibility reset.
7. READY is withheld when any initial source or sink construction fails and is
   emitted only after all assigned partitions cross the startup barrier.
8. Server cancellation attempts to stop starting and running workers immediately,
   waits for worker and HTTP completion, and reports all failures.
9. A resolved plan cannot be paired with another composition fingerprint.
10. Catalog composition rejects schema/default/codec drift, ambiguous defaults,
    unknown extension targets, invalid resolver patches, and unsupported UI hints.
11. Resolver tests cover cancellation, deadline, repeated pagination tokens,
    page/item bounds, MDB path injection, PostgreSQL role ambiguity, and explicit
    Logbroker trust.
12. Core and plugin catalog payloads pass the real TypeScript schema compiler and
    HTTP response validators.
13. Required Rust gates, web typecheck/build/unit tests, plugin Rust gates, and
    focused lifecycle/security/race tests all pass on the final stable tree.

## Risks and mitigations

- The API and persisted state intentionally break. State schema version is bumped
  and stale experimental files fail with a clear migration-free error.
- Startup waits longer because READY now proves constructability. The existing
  timeout bounds the wait and produces a precise failing partition diagnostic.
- Stricter plugin resolution may reject clusters previously accepted by fallback.
  This is intentional: the UI exposes the missing explicit choice instead of
  silently selecting infrastructure.
- Shared generated contracts add build work. Generated artifacts are deterministic
  and checked in CI; catalog compilation remains off the data hot path.

## Unresolved questions

None. Product choices that would otherwise be ambiguous use the strict,
fail-fast behavior above. This document is already approved for implementation by
the user instruction that requested the specification and immediate execution.
