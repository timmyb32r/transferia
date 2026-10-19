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

## PostgreSQL snapshot execution

PostgreSQL owns its snapshot planning policy, guarded snapshot epoch and shared
task queue inside `transferia-connector-postgres`. Delivery preparation may
refine a finite, co-located snapshot topology before destination preparation;
it must preserve datasets, schemas and delivery semantics. The per-partition
pipeline acknowledges task markers only after destination durability. All
snapshot tasks must be acknowledged before an exact transition to replication.
See the [snapshot planner contract](postgres-snapshot-planner.md) for ownership,
pending-task adaptation, cancellation, diagnostics and evaluation evidence.

## Runtime dataset contracts

`TableData` carries an `Arc<DatasetSchema>` beside its Arrow `RecordBatch`.
The pipeline preserves that contract and the exact namespace when constructing
`SinkBatch`. Middleware which changes fields must also replace the carried
schema; filtering rows and renaming the target retain it. The sink boundary
checks the Arrow fields and semantic metadata against the carried contract
before buffering or external writes. A batch with contradictory schema metadata
is rejected, never coerced to match startup discovery.

Startup discovery still validates every schema already known before execution.
`SchemaOrigin::ParserRuntime` explicitly represents parsers whose writer schema
is available only from messages. Such discovery declares the envelope DLQ but
never invents an empty or JSON main-table schema. Main schemas are admitted at
runtime, after middleware, before preparing a destination or writing records.

Parser sessions return ordered main-table batches plus an optional envelope DLQ.
A Schema Registry session decodes each message using its own schema id. Avro and
Protobuf values become native Arrow numeric, binary, temporal, decimal, struct,
list and map arrays; JSON Schema objects become typed fields. Contiguous messages
with the same physical schema share an output batch. Different writer schemas,
even incompatible ones for the same table, remain separate ordered batches.
The parser never compares them with previous versions or destination schemas.
Unsupported representations fail explicitly: examples include recursive types,
Avro unions with several non-null alternatives, open JSON objects, omitted JSON
fields and unbounded JSON `number`. No value is silently moved into a JSON catch-all
column, narrowed, rounded, or replaced with NULL. Ordinary JSON serializers reject
non-finite floats; the explicitly selected Debezium encoding retains its named
non-finite representation.

`SinkConnector::prepare_runtime` owns compatibility with an already admitted
schema. The default creates new destinations through normal preparation and
rejects changes to existing schemas. Queue and object sinks explicitly accept
schema changes and revalidate serialization and value-dependent constraints before
writing. Table sinks can implement a non-destructive evolution policy here;
startup replace/drop policies must never run implicitly for an evolving table.
There is no shared automatic ALTER or parser-side compatibility policy.

The admission driver closes and drains the current input session before changing
contracts. `Sink::run_session` borrows the same sink object; after successful
preparation, `update_discovery` installs the catalog and another session resumes.
The sink is neither rebuilt nor replaced. Existing connection, producer, retry
and delivery-tracker state stays with its owner. Source-authored dataset barriers
are committed only after preparation; a schema discovered from row data retains
its original delivery id and is acknowledged only after the data itself is durable.
Cancellation or rejection drains preceding writes and never acknowledges the
rejected barrier or its records.

Finite sources with runtime schemas explicitly register output identities in the
snapshot row tracker; registration contributes zero rows until a durable source
commit. Destinations requiring exact snapshot reconciliation currently require
startup-known schemas for finite sources and reject that unsupported combination
before starting workers. YTsaurus runtime admission requires
`replace_tables=false`, so startup replacement cannot delete another writer's
already populated target.

Logbroker uses each incoming table name verbatim as its producer identity (YDB Topic `producer_id`, PQv1 message group). Sessions and sequence counters are separate per table, including when several tables share one destination topic. No main-table schema is required to construct the sink.
