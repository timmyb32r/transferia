# transferia

A performance-oriented Rust data integrator. The active runtime
creates one logical pipeline and sink actor per stream partition or batch split, while connectors
share expensive connection pools and upload clients:

```text
stream: Logbroker (YDB or PQv1) | Kafka
batch:  PostgreSQL | MySQL | OpenSearch | YDB | ClickHouse | S3 | Iceberg | YTsaurus | data generator
replication: PostgreSQL (pgoutput or wal2json) | MySQL 8 (row binlog) | YDB (Changefeed)
batch + stream: PostgreSQL (exact exported-slot boundary) | MySQL 8 (exact FTWRL/GTID boundary)
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
sinks. PostgreSQL and MySQL 8 provide finite snapshots, ordinary database
replication, coordinated batch-and-stream sources, and sinks. YDB provides
finite snapshots, ordinary stream-only Changefeed replication, and a sink.
OpenSearch, ClickHouse, S3, Iceberg, and YTsaurus provide finite-snapshot
sources and sinks. Iceberg additionally applies PostgreSQL and MySQL changelogs
as primary-keyed replicas. YDB writes production Arrow IPC batches with
`BulkUpsert` and requires an explicit logical-to-physical table mapping plus a
non-null primary key. The batch-only data generator and non-durable `discard`
sink are explicit benchmark components.

The generator includes numeric, transfer-log, and ClickBench `hits` presets.
The ClickBench preset keeps the reference dataset's 105-column Arrow schema,
temporal types, primary-key column set, value ranges, empty-value rates, string
lengths, and cardinalities. Its compact distribution profile is reproducibly
derived from bounded, evenly spaced samples of `hits.csv` by
`scripts/analyze_clickbench_csv.py`; the source file itself is never embedded in
the binary.

Shared source configuration, discovery, and mode dispatch live in `source`.
`src_batch` contains finite snapshot readers, while `src_stream` contains live
queue streams and ordinary database replication. `src_batch_and_stream` owns
only the coordination that hands PostgreSQL or MySQL's exact finite snapshot to
its ordinary replication reader.
PostgreSQL replication supports both `pgoutput` and `wal2json`;
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

A PostgreSQL batch source opens one coordinator `REPEATABLE READ READ ONLY`
transaction, exports its MVCC snapshot, and imports that snapshot into every
table reader before issuing schema or data queries. Table partitions still run
concurrently, but they are deliberately co-located on worker zero so a
process-local transaction can own the snapshot for their complete lifetime;
distributing those partitions across workers would create inconsistent
snapshots and is therefore not attempted.

Select `delivery_type: batch_and_stream` explicitly to run a PostgreSQL snapshot
followed by logical replication; merely configuring `source.postgres.replication`
does not silently change the delivery mode. Before destination preparation, the
assigned worker issues `CREATE_REPLICATION_SLOT ... LOGICAL ... EXPORT_SNAPSHOT`.
PostgreSQL atomically returns the slot's consistent WAL position `P` and exported
MVCC snapshot `S`. Every table reader imports `S` in one co-located finite
snapshot phase. Only after that phase has committed and its completion checks
have succeeded does the source durably record the handoff, release the snapshot
owner, and start the stream at `P`.

This protocol provides a source boundary with no gap or overlap between snapshot
rows and later WAL changes. It does not upgrade the destination's delivery
guarantee: the standard per-partition stats still report the separately inferred
sink guarantee, including at-least-once where applicable. An exported snapshot
cannot be recovered after its owning replication session ends. On restart, the
durable phase and replication offset are accepted only for the exact non-secret
delivery/configuration revision that created them. Restarting an unchanged
revision resumes it, while editing a stopped delivery fails closed instead of
reusing source progress under different parser, middleware, or destination
semantics. Such an edit requires a deliberate reset of the affected destination
attempt and durable state before starting a new snapshot boundary. The connector
may bootstrap again from a pre-snapshot `claimed` state only after
proving that its slot is absent on the same PostgreSQL cluster. Once the durable
state reached `snapshot`, destination rows may already exist: startup therefore
fails closed even if the slot was removed, and requires a deliberate reset of
that destination snapshot attempt and its durable state. Transferia never drops
or replaces the slot and never replays an indeterminate snapshot automatically.

For `pgoutput`, the configured publication must contain every configured table
as the exact discovered relation, publish all columns and only `INSERT`,
`UPDATE`, and `DELETE`, and have neither row filters nor
`publish_via_partition_root`. `TRUNCATE` is intentionally rejected because the
row-change contract cannot represent a table-level truncation. A compatible
publication can be created with
`CREATE PUBLICATION transferia_publication FOR TABLE ... WITH (publish = 'insert, update, delete')`.
The connector validates this contract before claiming or creating the slot,
again inside the exported snapshot, at stream startup, and in the same
transaction immediately before every logical peek.

MySQL snapshots expose the text and prepared-statement binary result protocols.
Both feed the same row-to-Arrow conversion when the selected physical types can
be represented losslessly; discovery rejects text protocol for `FLOAT`, whose
server text representation cannot preserve every IEEE-754 `f32` value. Binary
with 16,384 rows per batch is the measured default. Snapshot memory is controlled explicitly by
`batch_target_bytes` (8 MiB by default, valid range 1 byte through 1 GiB) and
`max_row_bytes` (1 GiB by default, valid range 1 KiB through the 1 GiB MySQL
client packet maximum). The first is a retained decoded-row target and may be
crossed only by one indivisible final row; the second is the exact wire-packet
limit for one row, while decoded object and Arrow allocation overhead is
accounted separately. The sink uses a 250-row INSERT batch for the wide
105-column ClickBench workload; the earlier narrow database fixture measured a
1,000-row knee, so this is workload-specific rather than a universal optimum.
The narrow fixture is recorded in
[the database throughput report](benchmarks/database_throughput/REPORT.md), and
the exact-prefix ClickBench measurements, CPU/RSS data, persisted-integrity
checks, and explicit read-back qualifications for all six connectors are recorded in
[the ClickBench report](benchmarks/clickbench_throughput/REPORT.md).

MySQL 8 replication is enabled explicitly with `source.mysql.replication` and
requires `delivery_type: stream` or `delivery_type: batch_and_stream`; a snapshot
configuration continues to expose batch only. The replication source requires
an explicit nonzero replica `server_id`, GTID mode and consistency enforcement,
row-format binary logging with full row images and full row metadata, CRC32
binlog checksums, empty row-value options, transaction compression disabled, and
InnoDB tables with real primary keys. MariaDB remains supported for finite
snapshots but is rejected before replication state is created because its GTID
and event contracts differ.

MySQL snapshot and replication fields carry a versioned Arrow extension payload
with the exact physical column declaration: signedness and `ZEROFILL`, decimal
precision/scale, temporal precision, character set/collation, `ENUM`/`SET`
members, spatial SRID, visibility, generation mode, and other source modifiers.
Native numbers keep native Arrow storage. `ENUM` uses its physical `UInt16`
ordinal and `SET` its physical `UInt64` bitset, while their declarations remain
in extension metadata, so empty members and comma-containing members cannot
collapse into the same value. Decimal and zero-capable temporal values use
canonical exact text so zero and partial-zero dates are not coerced; non-UTF-8
`latin1` text uses Arrow `Binary`; JSON uses MySQL-compatible canonical JSON
text; and spatial values remain exact binary payloads. The same extension
name, payload, storage type, and value representation are emitted by the finite
snapshot and row-binlog paths, including full old values for updates and deletes.

Replication rejects virtual generated columns, character sets other than
`ascii`, `utf8mb3`, `utf8mb4`, and byte-preserving `latin1`, unsupported physical
families, incomplete or mismatched full table-map metadata, and partial JSON
updates. Stored generated and invisible columns are accepted only when MySQL's
full row image proves their exact value and physical identity. These checks run
during discovery and again on every table map, before a row can be buffered or
acknowledged; replication never guesses a conversion whose binlog representation
cannot be proven equal to the snapshot schema.

Writes and schema changes for selected tables must remain in MySQL's binary log.
A privileged session that deliberately sets `sql_log_bin=OFF` removes those
events from the source protocol itself; no binlog consumer can reconstruct or
validate data that the server omitted.

For `batch_and_stream`, the coordinator retains a MySQL named execution lock,
takes `FLUSH TABLES WITH READ LOCK`, starts every configured table's read-only
repeatable-read consistent snapshot while writes are blocked, and holds each
table's metadata lock. It then captures one exact binary-log filename/position,
executed GTID set, and source timestamp, durably records that boundary, and only
then unlocks writes. All table readers consume their already-open transactions.
After every finite snapshot row has reached the sink and the snapshot phase is
durably completed, the binlog reader starts at that exact boundary. There is no
independent second snapshot and no estimated handoff position.

On the first `stream` start without a durable offset, the connector uses the same
write lock and authoritative schema recheck to capture an exact starting
filename/position and executed GTID set before streaming. On restart, both
delivery modes resume from the durable committed GTID frontier instead of
capturing a replacement boundary.

The binlog reader validates CRC32-protected events, buffers a complete source
transaction within `replication.max_transaction_bytes` (64 MiB by default,
valid range 19 bytes through 1 GiB) and `replication.max_events` (4,096 by
default, applied independently to the transaction's binlog-event count and
decoded-row count), and emits it only after its XID or COMMIT. Crossing either
explicit limit fails closed before emission.
Durable progress advances only after the corresponding sink acknowledgement;
an unacknowledged transaction is replayed. Empty transactions
after table filtering still carry an explicit checkpoint. Restart state is
bound to the exact non-secret delivery revision, source UUID, database, replica
server ID, start boundary, and structured physical table identity. An
interrupted connection-owned snapshot, changed replay identity or schema,
purged unacknowledged GTID, reset binlog history, partial row image, compressed
transaction payload, or lost execution lock fails closed instead of choosing a
newer position. Purging files whose transactions are already included in the
durable GTID frontier is supported; restart uses GTID auto-position rather than
requiring the old filename to remain present.

YDB table replication is enabled explicitly with `source.ydb.replication` and
requires `delivery_type: stream`. Every configured table must already have the
named `FORMAT JSON`, `NEW_AND_OLD_IMAGES` Changefeed with
`VIRTUAL_TIMESTAMPS` enabled; schema-change events, resolved timestamps, and the
initial scan must be disabled. The Changefeed topic must have exactly one fixed
partition, with automatic partition splitting and merging disabled. This keeps
the Topic offset a valid monotonic row version; multi-partition and
auto-partitioned Changefeeds are rejected before execution rather than risking
a newer row receiving a lower partition-local version. Its topic must already
have the named important consumer, with `read_from` unset and RAW records
allowed. The consumer must set
the persistent attributes `transferia.delivery_id` to the delivery's exact
`delivery_id` and `transferia.coordination_node_path` to the configured
`replication.coordination_node_path`; that Coordination node must also already
exist. Transferia validates these resources but does not create or silently
replace them.

For `NEW_AND_OLD_IMAGES`, an `update` or `reset` event with `newImage` and no
`oldImage` becomes create (`c`), the same event with a complete `oldImage`
becomes update (`u`), and `erase` with a complete `oldImage` becomes delete
(`d`). Composite primary-key order and complete old values for updates and
deletes are preserved. Nullable YDB `Json` and `JsonDocument` columns are
rejected because Changefeed JSON cannot distinguish SQL `NULL` from JSON
`null` losslessly.

Source progress advances only after YDB returns the matching server offset
acknowledgement. A read or shutdown before that acknowledgement leaves the
cursor unchanged, so restart replays the exact unacknowledged records. An
expired retained cursor or detected schema drift fails closed without advancing
the cursor. This is ordinary CDC: it is not DBLog, does not expose
`batch_and_stream`, and makes no atomic initial-snapshot handoff claim.

The Debezium serializer accepts YDB Changefeed discovery explicitly as the
`ydb` dialect. It emits complete `before`/`after` images, composite keys,
`c`/`u`/`d` operations, delete tombstones, and a YDB source block with the
exact virtual `step` and `txId`; the top-level processing timestamp remains the
Topic write timestamp. YDB `String`/Yson bytes use Kafka Connect base64,
`Datetime` uses epoch milliseconds, `Timestamp` and `Interval` use
microseconds, UUID uses canonical text, and `DyNumber` uses Debezium's lossless
variable-scale decimal structure. The serializer validates the complete stream
metadata and full old-image schema before destination work and rejects unknown
Arrow/extension combinations rather than guessing a representation.

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

Finite snapshots permanently count rows at the connector-neutral Arrow
boundary, after parsing and middlewares and before destination-specific
encoding. Totals are exact per main/DLQ dataset. A delivery contributes only
after the sink acknowledgement and source commit both succeed; committed
prefixes survive an in-process partition retry, while uncommitted replay does
not double-count. Every finite source therefore reports its exact completed
output totals regardless of connector.

When a destination exposes an exact O(1) metadata count, the runner also
reconciles persisted rows before declaring the snapshot complete. Iceberg uses
the snapshot summary's `total-records` and a durable pre-write baseline, so the
required equality is `final = baseline + output`. A lossless replacement into
YTsaurus uses `@row_count` and requires `final = output`; verification is not
claimed when replacement is disabled or the explicit oversized-value policy
may drop rows. Missing targets, malformed/approximate metadata, changed
physical destinations, overflow, and count mismatches fail explicitly. Other
destinations still receive and log exact output totals, but do not run an
expensive `COUNT(*)` or substitute approximate catalog statistics. Destination
reconciliation is currently performed only by a single-worker snapshot; a
multi-worker run reports per-worker output totals and explicitly marks the
destination check unavailable rather than guessing a global sum.

Iceberg replica mode accepts only PostgreSQL or MySQL changelog discovery with
a complete primary key and a complete old image. Each source transaction is
collapsed to its final keyed mutations, then written as Iceberg v2 equality
deletes plus replacement data files in one atomic row-delta snapshot. Primary
key changes therefore delete the old identity and insert the new identity; a
delete writes only the equality-delete key. The destination table is bound to
the exact delivery and replay identities, and every commit stores the exact
source-coordinate identity in both durable storage and the active Iceberg
snapshot lineage. A retry after any ambiguous failure either proves that exact
snapshot already committed and becomes a no-op, or commits it once. Different
payloads, rolled-back snapshots, changed ownership, schema drift, unsupported
sources, and incomplete replica images fail closed before source acknowledgement.
The source cursor advances only after every affected Iceberg table has
acknowledged its atomic snapshot, so restart produces neither lost changes nor
duplicate logical rows.

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

The measured sink defaults are 20,000 rows per bulk request and four concurrent
requests. The source keeps 10,000 rows per page and two concurrent shard
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
benchmarking. Its 512 KiB writer row-buffer default was selected by five
order-balanced exact-prefix ClickBench runs; the previous 1 MiB value was 12.2%
slower end-to-end on that wide schema with 1.3% lower peak process RSS. For a
finite single-partition snapshot whose schema has a primary
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
