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
| `server/http.rs` | Versioned HTTP DTOs, routing, error classification, limits, and embedded assets |
| `server/ui_catalog.rs` | The root form schema composed with the provider catalog |
| `web/` | Preact UI, strict JSON Schema compiler, reducer state, and cancellable effects |

Do not let HTTP DTOs, JSON persistence, or `tokio::process::Child` enter `ControlPlane`. A replacement database implements `DeliveryStore`; a container or remote launcher implements `WorkerSupervisor`. Neither change should alter application use cases.

## Delivery lifecycle

Draft edits increment the revision and invalidate its previous validation. Validation records either `ready(revision)` or `invalid(revision)`. Activation accepts only the current persisted and validated revision, repeats full preflight, waits for child readiness, and then records `running(pid)`.

The server owns every worker it launches. Its authenticated loopback control channel provides readiness and stop commands. A worker cancels itself when the channel closes, so normal shutdown, crashes, and loss of the parent all terminate workers. Persisted transient states are normalized to `stopped` at the next server start; restart never activates them automatically.

## Persistence and secrets

The JSON store writes a complete candidate state to a private temporary file, synchronizes it, renames it atomically, synchronizes the directory, and only then publishes the candidate in memory. The state directory is mode `0700`; state, configs, and logs are mode `0600` on Unix.

The state directory contains its own deny-all `.gitignore`, and the repository ignores `.transferia-server/`. Delivery lists expose metadata only. Full delivery configuration is returned only by the detail endpoint needed by the local editor; internal errors are logged server-side and converted to a stable generic HTTP error.

## UI schema contract

The browser compiles a deliberately small JSON Schema subset: local `$ref`, objects, arrays, scalar types, enums/constants, nullable schemas, and `oneOf`/`anyOf`. `x-ui` selects presentation such as password fields and folded sections. Unsupported keywords fail fast instead of silently rendering the wrong control.

All YAML and discovery requests are latest-only. A new edit aborts the old request and a stale response cannot overwrite the current preview. Background discovery starts only after the selected provider schemas are structurally complete. Activation always performs authoritative server-side validation again.
