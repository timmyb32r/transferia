# transferia

A performance-oriented Rust data integrator. The active runtime
creates one logical pipeline and sink actor per stream partition or batch split, while connectors
share expensive connection pools and upload clients:

```text
stream: Logbroker (YDB or PQv1) | Kafka
batch:  PostgreSQL | MySQL | OpenSearch | YDB | ClickHouse | S3 | Iceberg | YTsaurus | data generator
replication: PostgreSQL (pgoutput or wal2json)
                                    |
                                    v
                         parser or native Arrow
                                    |
                                    v
                               middlewares
                                    |
                                    v
Logbroker | Kafka | PostgreSQL | MySQL | OpenSearch | YDB | ClickHouse | S3 | Iceberg | YTsaurus | discard
```

Source and sink connectors are selected from the runtime registry; parser kinds
are validated explicitly. Logbroker and Kafka provide streaming sources and
sinks. PostgreSQL provides finite snapshots, ordinary logical replication, and
a sink; MySQL, OpenSearch, YDB, ClickHouse, S3, Iceberg, and YTsaurus provide
finite-snapshot sources and sinks. YDB writes production Arrow IPC batches with `BulkUpsert` and
requires an explicit logical-to-physical table mapping plus a non-null primary
key. The batch-only data generator and non-durable `discard` sink are explicit
benchmark components.

The generator includes numeric, transfer-log, and ClickBench `hits` presets.
The ClickBench preset keeps the reference dataset's 105-column Arrow schema,
temporal types, primary-key column set, value ranges, empty-value rates, string
lengths, and cardinalities. Its compact distribution profile is reproducibly
derived from bounded, evenly spaced samples of `hits.csv` by
`scripts/analyze_clickbench_csv.py`; the source file itself is never embedded in
the binary.

Shared source configuration, discovery, and mode dispatch live in `source`.
`src_batch` contains finite snapshot readers, while `src_stream` contains live
queue streams and ordinary database replication. `src_dblog` is reserved for
incremental snapshots coordinated with a replication log and has not been
implemented yet. PostgreSQL replication supports both `pgoutput` and `wal2json`;
both decoders emit the same Arrow changelog contract, including operation,
source position, changed-column presence, and old values for
`REPLICA IDENTITY FULL`. Connector-wide transport and credentials remain at the
connector root, while each mode owns only its specific settings and reader.

PostgreSQL snapshots use real `COPY TO STDOUT`, and the sink uses real
`COPY FROM STDIN`. Both expose an advanced `binary`/`text` wire-format choice;
`binary` is the lossless high-throughput default. The two implementations share
the same Arrow contract, including PostgreSQL internal `"char"`, `oid`, `bytea`,
dates, microsecond timestamps, nulls, and COPY escaping. Values PostgreSQL cannot
store losslessly, such as a nanosecond timestamp outside microsecond precision,
fail before a COPY request is sent.

MySQL snapshots expose the text and prepared-statement binary result protocols.
Both feed the same lossless row-to-Arrow conversion; binary with 16,384 rows per
batch is the measured default. The sink keeps the measured 1,000-row INSERT
knee. PostgreSQL, MySQL, and OpenSearch candidate grids and before/after numbers
are recorded in [the database throughput report](benchmarks/database_throughput/REPORT.md).

For the demonstration control plane, run:

```bash
transferia --server --bind 127.0.0.1:8080 --state-dir .transferia-server
```

To expose the web control plane on every IPv4 network interface, opt in
explicitly and select its port:

```bash
transferia --server --listen-all 8080 --state-dir .transferia-server
```

This exposes the control plane to the surrounding network. Restrict access with
the host firewall or another trusted network boundary.

The embedded Preact UI saves incomplete drafts, renders connector forms from the
Rust-generated schema catalog, and keeps a copyable runnable YAML preview.
Validation and activation share the same complete preflight sequence as a CLI
worker. The local supervisor waits for child readiness, tracks exits, and stops
every owned worker when the server exits or its parent-control connection is
lost. Deliveries remain stopped after a server restart until manually activated.
See [the control-plane architecture](docs/server.md) for the storage, launcher,
API, and UI boundaries.

The editor's **Speedtest** view becomes available as soon as the required source
and destination settings are complete; the delivery name and shared pipeline
settings do not gate it. **Estimate maximum performance** measures one logical
source stream without committing its cursor. An untimed first pass records a
bounded empirical sequence of post-parser Arrow deliveries; a fresh second pass
measures source throughput without profiler or spool work. The destination test
replays the exact sampled dataset, batch, and DLQ mix into a connector-owned
scratch destination and reports when the configured pipeline-memory limit
truncated that profile. **Tune optimal parameters** evaluates source and
destination candidates concurrently, but may change only finite, safe values
declared by each connector. Its optional time budget starts after the one-time
profile and source baseline, which are each bounded by the visible trial
duration. Results compare the winner with connector-authored defaults.

Every request has an explicit cleanup timeout. Cleanup is tracked independently
of the HTTP client and retries idempotently until that deadline; exhaustion
returns the exact credential-free scratch targets for manual cleanup. A
connector that cannot prove non-disruptive source isolation, exclusive scratch
ownership, and safe cleanup fails before source I/O. Speedtest never falls back
to a production consumer, replication slot, table, object prefix, or
durable-state directory. S3 and Iceberg destinations are currently rejected:
the generic object-store API cannot prove removal of historical object versions,
while the available HDFS writer does not yet share the hardened outbound-HTTP
boundary required for a destructive probe.

## Quality checks

```bash
just check
```

`just check` is the fast, compile-only affected gate used during development.
Before a merge or release, `just check-release` runs workspace formatting,
strict Clippy, and the complete test/E2E suite.

## Configuration

`logbroker` uses the official low-level Rust YDB gRPC crate and the Topic API
`StreamRead` protocol. Partition assignment, rebalancing, and auto-partitioning
are owned by the protocol. Each topic may optionally restrict the partitions it
reads; an empty `partitions` list subscribes to every partition dynamically:

```yaml
source:
  logbroker:
    installation:
      type: on_premise
      host: topic.example.net
      port: 2135
      trusted_plaintext: true
      auth:
        type: token_file
        token_file: "/path/to/token"
    topics:
      - path: cdc/project/topic
        partitions: [0] # omit or leave empty to read every partition
    consumer_name: /cdc/project/consumer
    driver: ydb
    allow_ttl_rewind: false
    parser: # same parser contract as pqv1
      common:
        table_naming: { type: from_config, name: events }
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        json_framing: single_document
        columns:
          - { jsonpath: "$.id", column_name: id, json_data_type: string, arrow_type: Utf8, nullable: false }
```

Set `driver: pqv1` in the same `logbroker` source to use the legacy wire
protocol. PQv1 currently accepts exactly one topic with an explicit, non-empty
`partitions` list; the backend rejects dynamic or multi-topic PQv1
configurations instead of silently changing their meaning.

`examples/logbroker_read_one.rs` is a credential-safe connectivity probe. It
opens one dynamic Topic API reader, reports only topic/partition/offset and byte
counts for the first non-empty batch, and deliberately exits without committing
consumer offsets.

```yaml
delivery_id: pqv1-s3-production
delivery_type: stream
durable_storage:
  type: local_file
  path: .transferia-state

source:
  logbroker:
    installation:
      type: on_premise
      host: localhost
      port: 2135
      trusted_plaintext: true
      auth:
        type: token_file
        token_file: "/path/to/token"
    topics:
      - path: "/cdc/prod/events"
        partitions: [0]
    consumer_name: "transferia-consumer"
    driver: pqv1
    parser:
      common:
        table_naming:
          type: from_config
          name: events
        system_columns:
          topic: _system_topic
          partition: source_partition
          offset: source_offset
          message_index: _system_message_index
          write_timestamp_ms: _system_write_timestamp_ms
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        keys: [id, source_partition, source_offset]
        json_framing: single_document
        columns:
          - jsonpath: "$.id"
            column_name: id
            json_data_type: number
            arrow_type: Int64
            nullable: false
          - jsonpath: "$.tenant"
            column_name: tenant
            json_data_type: string
            arrow_type: Utf8
            nullable: false
            low_cardinality: true
            max_length: 128

middlewares: []

pipeline_memory_limit_bytes: 268435456

sink:
  s3:
    bucket: transfer-bucket
    object_layout_version: 5
    prefix: streams
    region: us-east-1
    host: s3.example.net
    port: 443
    credentials:
      access_key: "replace-me"
      secret_key: "replace-me"

    partitioning:
      type: source
      # Alternatives:
      # type: fields
      # columns: [tenant]
      #
      # type: record_time
      # window: 1h
      # timezone: Europe/Moscow
      # path: "year=%Y/month=%m/day=%d/hour=%H"

    rotation:
      max_rows: 100000
      max_bytes: 128MiB
      record_time_interval: null
      wall_clock_interval: null
      on_partition_path_change: keep_epoch # rotate = close between atomic source messages when the path changes

    buffering:
      # All buffering and upload-concurrency limits are per partition actor.
      max_epoch_buffers: 128
      max_pending_upload_objects: 512
      max_buffered_bytes: 256MiB
      # Stable retained epoch budget (payload + routing metadata).
      max_epoch_bytes: 64MiB

    upload:
      multipart_threshold: 25MiB
      part_size: 25MiB
      parallel_parts: 4
      max_in_flight_objects: 4
      operation_timeout: 1m

    retry:
      initial_backoff: 200ms
      max_backoff: 30s
      max_attempts: 20

metrics:
  interval_ms: 1000
  per_partition: true
```

The JSON conversion contract is deliberately explicit. `json_data_type` is one
of `string`, `number`, or `boolean`; compatible Arrow targets are strings,
numeric and temporal types, and booleans. Temporal targets additionally require
`time_conversion: { type: epoch, unit: ... }` or
`time_conversion: { type: string, format: ... }`. Parse and conversion failures
can be sent to DLQ, dropped, or fail the delivery. Unknown top-level fields can
be dropped, fail the row, or be captured as compact JSON in the explicitly
named rest column. `keys` uses physical output
names, including renamed enabled system columns; ClickHouse derives `ORDER BY`
from it directly. `low_cardinality` and
`max_length` are Arrow field metadata and are revalidated at the sink boundary;
ClickHouse materializes `LowCardinality(String)`.

Run it with:

```bash
transferia --config ./config.yaml --total-workers 1 --worker-index 0
```

YAML values are parsed literally and are never implicitly expanded from the
process environment. This keeps the UI preview, validation, and worker startup
semantically identical. Credential files may use the connector's documented path
handling; explicit environment references can be added later as a typed config
feature. Byte sizes accept `B`, `KiB`, `MiB`,
and `GiB`; durations accept `ms`, `s`, `m`, `h`, and `d`.

### OpenSearch

The OpenSearch source is a finite snapshot reader. Every configured name must
identify one exact concrete index: aliases and wildcard expansion are rejected.
It opens one non-partial point-in-time snapshot per index and assigns one
logical slice to each discovered primary shard. Slices page concurrently with
`_doc`/`search_after`, bounded by the configured reader limit. Retryable
requests reuse the same PIT and exact slice cursor; the reader never silently
opens a newer snapshot after emitting rows. Each document is represented
losslessly as non-null `_id`, nullable `_routing`, the original `_source` JSON
text, and a non-null `_routing_key`. The composite (`_id`, `_routing_key`)
primary key preserves documents whose equal IDs coexist under different custom
routing. Keeping JSON as an `arrow.json` envelope avoids guessing
scalar-versus-array shape from an OpenSearch mapping and preserves numeric
lexemes beyond ordinary JSON number precision.

The sink accepts append-only datasets with a non-null primary key. An incoming
OpenSearch envelope keeps its exact `_id`, `_routing`, and `_source`; other
Arrow schemas use an injective, version-stable encoding of the complete primary
key as the document ID. IDs over OpenSearch's 512-byte limit, unsupported Arrow
types, non-finite floats, schema drift, and invalid index names fail before the
corresponding bulk request. The sink never lowercases names, sanitizes fields,
or replaces long IDs with hashes. Created indices use strict mappings and
request-durable translog writes. Bulk concurrency, batch limits, flush timing,
and bounded retry policy are explicit advanced settings, and progress is
acknowledged only after every bulk item succeeds. As required by the default
primary-key contract, the source must provide one logical record per complete
primary key; the sink detects duplicates in its bounded buffered and in-flight
window without carrying an unbounded all-history key set.

The measured sink defaults are 20,000 rows per bulk request and eight concurrent
requests. The source keeps 10,000 rows per page and four concurrent shard
readers; larger ordinary pages exceed OpenSearch's default result-window limit.

Custom-routed OpenSearch source documents and flat rows fail by default because
preserving their original ID against a target with different shard geometry is
not safe.
The explicit `routed_identity: encode_identity` mode injectively encodes the
complete source identity into the destination ID and still preserves routing;
the transformed ID is rejected rather than truncated or hashed if it exceeds
OpenSearch's limit.

Speedtest writes only to exclusively created random indices carrying an opaque
owner marker. Ownership and the exact schema are revalidated before writes and
deletion; an ambiguous or foreign index is preserved and reported instead of
being guessed safe. Managed OpenSearch installation discovery is supplied by
the internal extension; the public connector contains only the ordinary
host/port/TLS/auth contract.

### YTsaurus static tables

Each YTsaurus source table requires only its absolute `path`. Its final path
component is the logical dataset name; paths in one source must therefore have
unique final components. A sink selects a base directory and creates or checks
one child table per discovered dataset, using the dataset name unchanged as the
child path segment. The runtime never escapes, truncates, hashes, or otherwise
rewrites these names. Discovery rejects dynamic source tables, unsupported or
drifting schemas, duplicate paths or derived dataset names, column names over
256 characters, reserved `@` column names, and unsupported Arrow types before
workers start. Representative configs are
`config_ytsaurus_source_to_clickhouse.yaml` and `config_ytsaurus_sink.yaml`.

The ordinary source uses Transferia's in-process, pure-Rust YTsaurus Bus/RPC
client. It requests Arrow rowsets only after `@chunk_format_statistics` proves
that every physical chunk is `table_unversioned_columnar`; a server-side Arrow
fallback in that state is an explicit fatal protocol error. Other physical
layouts use native YT-wire rowsets and are decoded directly into Arrow arrays.
The common source metrics report both bytes received from the network and bytes
materialized after network decoding.

`read_ordering: { type: ordered }` is the default and resumes at the exact row
index after a transient stream failure. The explicit
`read_ordering: { type: unordered }` mode lets YTsaurus return ready blocks out
of order for maximum single-stream throughput, but fails on interruption because
an exact continuation would risk silent loss. The third advanced mode,
`read_ordering: { type: partition_tables, ... }`, asks YTsaurus to partition the
table into cookies with embedded node descriptors and reads those partitions
concurrently. It is intended for maximum distributed throughput and is also
explicitly non-resumable. Every mode still emits one logical Transferia source
partition and uses bounded backpressure before the ordinary pipeline memory
budget.

The YTsaurus sink writes static tables. `format: arrow` is the default and uses
Arrow IPC streaming directly; `format: yson` is an explicit alternative for
benchmarking. For a finite single-partition snapshot whose schema has a primary
key, the default `unique_sorted` semantics require `replace_tables: true`: the
sink writes an attempt-owned staging table, sorts it server-side by the complete
primary key into a `unique_keys=true` table, and atomically replaces the
destination only after the sort succeeds. Duplicate keys fail the sort instead
of being silently collapsed. `preserve_rows` is the explicit unsorted
alternative.

For schemas without a primary key, `replace_tables: true` removes and recreates
the destination tables before writing. With `replace_tables: false`, every table
must already exist with exactly the discovered schema. Every runtime batch is
revalidated, including the 128MiB static-row limit, before any write. Unsorted
append completion is at-least-once: an ambiguous append can be replayed and
duplicated.

## Semantics

The S3 sink uploads several ready objects concurrently, within its configured
object and multipart limits. Buffering, memory, object-upload concurrency, and
multipart concurrency are all enforced per PQv1 partition actor; they are not
global limits across a worker. A rotation closes every main and DLQ object in a
deterministic commit epoch. Before the first PUT, the sink persists an `OPEN`
manifest containing every object key, payload digest and size. Only after every
PUT succeeds does it atomically transition the manifest to `CLOSED`; source
progress is committed only after that transition. A restart replays a matching
`OPEN` epoch and recovers commit from a matching `CLOSED` epoch without another
PUT. Payload or key drift fails fatally. The per-pipeline memory budget and S3 buffering limit
propagate backpressure to PQv1. One oversized source message is admitted
atomically with a warning.

Delivery semantics are inferred from configuration and logged as a structured
report. Before any partition actor starts, delivery discovery checks the PQv1
topic metadata and derives the logical main/DLQ schemas from the configured
parser (PQv1 stores opaque message bytes, so it has no native row schema).
Each sink publishes a machine-readable limits contract. The discovered table
and column names, Arrow types, system columns, and destination-specific limits
are validated against it before destination preparation. The same contract is
checked again for every runtime batch before ClickHouse INSERT or S3 upload, so
schema drift fails closed before a source offset can be committed.

Deterministic source/field/record-time partitioning and deterministic rotation
are exactly-once through idempotent object overwrite plus the durable epoch
state machine. `delivery_id` is the explicit ASCII identity of that state, and
`durable_storage.path` selects its crash-safe local-file root. Enabling
`wall_clock_interval` makes the delivery at-least-once because restart timing
can change object boundaries; the report includes a remediation.

The S3 exactly-once statement assumes that parser, middleware and projection
settings (including configured parser system columns), destination identity
(bucket/endpoint/region), S3 prefix, `object_layout_version`, partitioning, rotation thresholds,
`buffering.max_epoch_buffers`, and `buffering.max_epoch_bytes` remain unchanged
while uncommitted source data can replay. Treat those fields as semantic state
during deployments; changing them can produce different object boundaries,
keys, or payloads.
`object_layout_version: 5` pins the deterministic key/payload/epoch contract.
Data-derived topic, field, and record-time path components are preserved exactly;
invalid components and keys longer than 1024 UTF-8 bytes fail before upload and
are never silently encoded, shortened, or replaced with hashes. This binary
rejects unknown versions instead of silently changing replay semantics.
The normalized `(bucket, prefix)` namespace must be owned exclusively by one
logical PQ/Logbroker source. Its workers and partitions may share that namespace
because keys include source topic and partition, but an independent source must
use another prefix. Object keys intentionally do not include cluster identity,
so topic names from different clusters must never collide in one namespace.
When omitted, `max_epoch_bytes` has a fixed 128MiB default; it must fit both
`max_buffered_bytes` and `pipeline_memory_limit_bytes`, otherwise configuration
validation fails instead of risking a backpressure deadlock.
The threshold counts serialized payload, routing-string UTF-8 lengths, and a
fixed 128-byte logical overhead per row, so epoch boundaries do not depend on
the Rust ABI or machine architecture.

Field partitioning requires non-null scalar partition columns and
parser-generated source identity columns. Record-time partitioning currently
uses the source `_system_write_timestamp_ms`; user-field timestamps are not a
supported partitioning input.

JSON objects are compact NDJSON compatible with the Confluent S3 JSON shape:
one object per line, explicit nulls, and a final newline. System columns are
included in the sink schema whenever they are enabled in the parser.

PQv1 → ClickHouse is at-least-once: an ambiguous INSERT followed by replay can
produce duplicate rows. See `docs/pqv1-clickhouse-delivery.md` for the precise
runtime contract and unimplemented design options.

Parser failures are written to the DLQ with the original payload encoded in
`raw_base64`; this is lossless for arbitrary non-UTF-8 source bytes. DLQ rows
also store nullable `source_write_timestamp_ms` copied from source metadata,
never parser wall-clock time or an invented zero. To keep parser allocation bounded, a delivery whose conservative output
working set exceeds 256MiB, or which contains a JSON record larger than 4MiB,
causes the delivery to fail before materialization. The parser never silently
changes valid input into a DLQ row because of an internal safety limit. These
semantics are part of S3 `object_layout_version: 5`.
For ClickHouse, DLQ schema changes are intentionally fail-closed: create a new
empty `<table>_dlq` with the current schema (or move the old table aside).

The ClickHouse native hop is plaintext because the bundled client cannot verify
server certificates. Use a verified local TLS tunnel and keep only the trusted
local hop plaintext. Every ClickHouse sink config must set
`trusted_plaintext: true`; the process otherwise refuses to start, so this
deployment assumption cannot be accepted accidentally.
Connection/request deadlines and finite retries are configurable. The
configured connect timeout is a strict deadline for the caller and does not
block Tokio workers. Because the underlying client uses a non-cancellable
30-second socket connect, its current socket call may finish later on the
blocking pool; an internal deadline then stops the remaining connection work,
and reconnects reuse that single in-flight attempt instead of accumulating more
work. Existing tables are checked against the Arrow schema before ingestion.

Ready-to-edit benchmark configurations live in `benchmarks/`. The three
`discard` variants isolate network, decompression, and parsing; separate configs
exercise the full ClickHouse and S3 paths. Repository tests parse and validate
every benchmark config against the registered connector schemas. Run and compare
them with `scripts/run_single_partition_benchmark.py`; the complete procedure,
environment overrides, backlog requirements, and regression rule are in
`docs/benchmarks.md`.
