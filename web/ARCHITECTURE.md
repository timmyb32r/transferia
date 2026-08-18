# Frontend architecture

The control-plane UI is a compile-time composed modular monolith. Features are
local modules, not remotely loaded plugins.

## Dependency direction

```text
app (composition root)
  -> delivery workspace
  -> feature widget registry
  -> infrastructure adapters

delivery / features -> application ports + schema runtime + UI primitives
schema              -> application capabilities + UI primitives
application         -> generated DTOs and JSON values
infrastructure      -> application ports
ui                  -> Preact only
```

`npm run check:architecture` enforces the important negative dependencies and
allows `fetch` only in the HTTP control-plane adapter.

## Composition

`src/app.tsx` is the composition root. It selects the concrete
`ControlPlanePort` and the production `WidgetRegistry`, then mounts the delivery
workspace. Tests can replace the control-plane port without module mocking.

`ControlPlanePort` is the application-facing API. Transport paths, HTTP methods,
headers, and response decoding remain in
`src/infrastructure/controlPlane/httpControlPlane.ts`.

## Schema forms

The schema compiler validates the supported JSON Schema dialect and converts
raw `x-ui` objects into typed `UiHints`. All catalog schemas are compiled before
the editor becomes interactive, so a malformed provider cannot break only when
the user selects it.

Generic form code depends on the `WidgetRegistry` interface. Provider/parser
features live under `src/features/` and are registered at the composition root;
the generic schema runtime never imports them. A widget declaration that lacks
its promised renderer fails during module initialization.

## Asynchronous work

`TaskRegistry` groups latest-wins jobs by lifecycle scope:

- `global` survives editor navigation;
- `session` belongs to the opened/new delivery;
- `revision` is invalidated by a local edit.

Polling schedules its next tick only after the current reads settle. Slow
requests therefore cannot be repeatedly aborted by a fixed interval, and poll
failures are visible in operation diagnostics.

## Contracts and gates

Rust owns the API JSON Schema and the real public catalog fixture. TypeScript
generation is intentional (`just api-contract`); normal build/test commands are
read-only and fail when generated artifacts are stale. The frontend contract
test compiles every schema in the Rust fixture. Rust tests never spawn npm.

Use `just check-affected` during development. Full Clippy, all targets, and
container E2E tests remain merge/release gates or are selected for changes to
core, build inputs, dependencies, or provider data paths.
