# Workspace architecture

Transferia is a Cargo workspace whose crate graph enforces the principal runtime
boundaries. The root `transferia` package is a compatibility-free public facade:
it contains no business logic and only re-exports focused crates.

```text
transferia-core
    ↓
transferia-delivery-contracts
    ↓
transferia-pipeline     transferia-connectors
            ↘             ↙
             transferia-delivery
                    ↓
transferia-runtime     transferia-server-contracts
          ↓                    ↓
transferia-runtime-local  transferia-control-plane
                 ↘          ↙
                transferia-composition
```

## Ownership

- `transferia-core` is the stable connector-neutral data-plane API.
- `transferia-delivery-contracts` owns parser, middleware, retry, semantics, and
  metrics contracts shared by connectors and execution.
- `transferia-pipeline` owns the connector-neutral per-partition read/parse/write/
  commit pipeline. It intentionally does not depend on connector implementations.
- `transferia-connectors` owns concrete connectors, parsers, serializers, durable
  storage implementations, catalog composition, and extension registration.
- `transferia-delivery` owns runnable configuration, preparation/discovery, and
  delivery-level execution/restart orchestration.
- `transferia-runtime` owns environment-neutral worker supervision contracts.
- `transferia-runtime-local` implements local child-process ownership.
- `transferia-server-contracts` owns persistent and wire-stable server models and
  the generated API schema/interop fixture.
- `transferia-control-plane` owns HTTP, application services, persistence, logs,
  and the embedded web UI.
- `transferia-composition` is the executable composition root. It is the only
  crate that wires the control plane, local runtime, delivery, and connectors.

Dependencies must point downward through this graph. A lower-level crate must
not use a dev-dependency back to a higher-level crate; cross-layer tests belong
at the highest participating layer or require extraction of a narrower contract.
