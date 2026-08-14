# transferia

A performance-oriented Rust data integrator. The active runtime
creates one logical pipeline and sink actor per stream partition or batch split, while providers
share expensive connection pools and upload clients:

```text
PQv1 / YDB Topic / PostgreSQL / YTsaurus -> parser or native Arrow -> middlewares
                                                                -> ClickHouse | PostgreSQL | S3 | YTsaurus
```

Source and sink providers are selected from a small runtime registry; parser
kinds are validated explicitly. The executable registers `pqv1`, `ydb_topic`, plus
finite-snapshot `postgres` and static-table `ytsaurus` sources; `clickhouse`,
`postgres`, `s3`, and `ytsaurus` sinks; and the non-durable `discard` sink used by
explicit benchmark configurations.

Source implementations are grouped by delivery mode inside each provider:
`src_batch` contains finite snapshot readers and `src_stream` contains live
streams. `src_dblog` is reserved for database-log readers and will be added with
the first implementation. Provider-wide transport, credentials, and shared
configuration remain at the provider root; mode-specific configuration belongs
to the corresponding source module. A provider may expose more than one source
mode without duplicating its common contract.

For the demonstration control plane, run:

```bash
transferia --server --bind 127.0.0.1:8080 --state-dir .transferia-server
```

The printed URL opens a dependency-free JavaScript UI. It stores delivery
definitions atomically in the local state directory, reruns real source
discovery and sink-limit validation whenever YAML changes, previews the exact
table schemas, and saves a valid definition as `created`. Activating a delivery
writes an immutable config file and starts a child `transferia --config ...`
process with stdout/stderr redirected to its own log file. This is intentionally
a demo process launcher, not a scheduler or production process supervisor.

## Quality checks

```bash
just fmt
just clippy
cargo test --all-targets
```

`just clippy` runs the strict lint policy from `Cargo.toml` with warnings as
errors. Every quality recipe runs `rustfmt` first.

## Configuration

`ydb_topic` uses the official low-level Rust YDB gRPC crate and the Topic API
`StreamRead` protocol. YDB-native topics can discover their active partitions
through `topology_discovery: topic_api`. Legacy Logbroker names may be readable
through `StreamRead` while absent from the YDB scheme API; configure their
partition IDs explicitly instead of guessing or rewriting the topic path:

```yaml
source:
  ydb_topic:
    host: sas.logbroker.yandex.net
    port: 2135
    topic_path: cdc/project/topic
    consumer_name: /cdc/project/consumer
    topology_discovery: configured
    partition_ids: [0]
    trusted_plaintext: true
    auth:
      type: token_file
      token_file: "${HOME}/.logbroker/token"
    parser: # same parser contract as pqv1
      common:
        table_naming: { type: from_config, name: events }
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        chunk_splitter: one-message-one-row
        columns:
          - { jsonpath: "$.id", column_name: id, json_data_type: string, arrow_type: Utf8, nullable: false }
```

`examples/ydb_topic_read_one.rs` is a credential-safe connectivity probe. It
opens every configured partition concurrently, reports only partition/offset
and byte counts for the first non-empty batch, and deliberately exits without
committing consumer offsets.

```yaml
delivery_id: pqv1-s3-production
durable_storage:
  type: local_file
  path: .transferia-state

source:
  pqv1:
    # Plaintext HTTP/2 only; use a trusted local endpoint or tunnel.
    host: localhost
    port: 2135
    topic_path: "/cdc/prod/events"
    consumer_name: "transferia-consumer"
    # Required: consumer-session assignments are not authoritative topic metadata.
    partition_group_ids: [0]
    network_timeout_ms: 30000
    decompression_concurrency: 4 # shared across this provider's partitions
    auth:
      type: access_token
      token_file: "${HOME}/.logbroker/token"
    parser:
      common:
        table_naming:
          type: from_config
          name: events
        system_columns:
          topic: true
          partition: true
          offset: true
          message_index: true
          write_timestamp_ms: true
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        primary_key: [id, source_partition, source_offset]
        system_column_names:
          partition: source_partition
          offset: source_offset
        chunk_splitter: one-message-one-row
        columns:
          - jsonpath: "$.id"
            column_name: id
            json_data_type: integer
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
keep_system_columns_in_sink: false

sink:
  s3:
    bucket: transfer-bucket
    object_layout_version: 5
    prefix: streams
    region: ru-central1
    host: storage.yandexcloud.net
    port: 443
    credentials:
      access_key: "${S3_ACCESS_KEY}"
      secret_key: "${S3_SECRET_KEY}"

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
  enabled: true
  interval_ms: 1000
  per_partition: true
```

The JSON conversion contract is deliberately explicit. `json_data_type` is one
of `string`, `integer`, `unsigned_integer`, `number`, or `boolean`; compatible
Arrow targets are strings, matching signed/unsigned integers, floating point,
booleans, and temporal types. Temporal targets additionally require
`time_conversion: { type: epoch, unit: ... }` or
`time_conversion: { type: string, format: ... }`. Conversion failures either
enter DLQ (`conversion_error: dlq`) or stop the delivery (`fail`). Unknown
top-level fields either fail validation of the row or are captured as compact
JSON in the explicitly named rest column. `primary_key` uses physical output
names, including renamed enabled system columns; ClickHouse derives `ORDER BY`
from it directly. `low_cardinality` and
`max_length` are Arrow field metadata and are revalidated at the sink boundary;
ClickHouse materializes `LowCardinality(String)`.

Run it with:

```bash
transferia --config ./config.yaml --total-workers 1 --worker-index 0
```

YAML supports environment expansion. Byte sizes accept `B`, `KiB`, `MiB`,
and `GiB`; durations accept `ms`, `s`, `m`, `h`, and `d`.

### YTsaurus static tables

YTsaurus source mappings always require both `path` and `output_name`; sink
mappings always require both `dataset` and `path`. The runtime never derives,
renames, escapes, truncates, or hashes identifiers. Discovery rejects dynamic
tables, unsupported or drifting schemas, duplicate mappings, column names over
256 characters, reserved `@` column names, and unsupported Arrow types before
workers start. Representative configs are
`config_ytsaurus_source_to_clickhouse.yaml` and `config_ytsaurus_sink.yaml`.

The YTsaurus sink appends to static tables. `format: arrow` is the default and
uses Arrow IPC streaming directly; `format: yson` is an explicit alternative
for benchmarking. `replace_tables: true` is an explicit destructive setup
choice that removes and recreates mapped tables. With `replace_tables: false`,
the tables must already exist with exactly the discovered schema. Every runtime
batch is revalidated, including the 128MiB static-row limit, before any write.
Completion is at-least-once: an ambiguous append can be replayed and duplicated.

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
settings (including `keep_system_columns_in_sink`), destination identity
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
projected out unless `keep_system_columns_in_sink: true`.

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
every benchmark config against the registered provider schemas. Run and compare
them with `scripts/run_single_partition_benchmark.py`; the complete procedure,
environment overrides, backlog requirements, and regression rule are in
`docs/benchmarks.md`.
