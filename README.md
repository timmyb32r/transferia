# transferia

A performance-oriented Rust data integrator proof of concept. The active runtime
creates one logical pipeline and sink actor per PQv1 partition, while providers
share expensive connection pools and upload clients:

```text
PQv1 -> JSON parser -> middlewares -> ClickHouse | S3
     \-> benchmark discard modes ----------> discard sink
```

Source and sink providers are selected from a small runtime registry; parser
kinds are validated explicitly. The executable registers the `pqv1` source,
durable `clickhouse` and `s3` sinks, and the non-durable `discard` sink used by
explicit benchmark configurations.

## Quality checks

```bash
just fmt
just clippy
cargo test --all-targets
```

`just clippy` runs the strict lint policy from `Cargo.toml` with warnings as
errors. Every quality recipe runs `rustfmt` first.

## Configuration

```yaml
source:
  pqv1:
    # Plaintext HTTP/2 only; use a trusted local endpoint or tunnel.
    connection_string: "grpc://localhost:2135"
    topic_path: "/cdc/prod/events"
    consumer_name: "transferia-consumer"
    # Required: consumer-session assignments are not authoritative topic metadata.
    partition_ids: [0]
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
          topic_name: true
          partition_num: true
          offset: true
          message_index: true
          write_timestamp_ms: true
      json_parser:
        chunk_splitter: one-message-one-row
        columns:
          - jsonpath: "$.id"
            column_name: id
            arrow_type: Int64
            nullable: false
          - jsonpath: "$.tenant"
            column_name: tenant
            arrow_type: Utf8
            nullable: false

middlewares: []

pipeline_memory_limit_bytes: 268435456
keep_system_columns_in_sink: false

sink:
  s3:
    bucket: transfer-bucket
    prefix: streams
    region: ru-central1
    endpoint: "https://storage.yandexcloud.net"
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
      on_partition_change: keep_open # rotate = Confluent-compatible

    buffering:
      max_open_objects: 128
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

Run it with:

```bash
transferia --config ./config.yaml --total-workers 1 --worker-index 0
```

YAML supports environment expansion. Byte sizes accept `B`, `KiB`, `MiB`,
and `GiB`; durations accept `ms`, `s`, `m`, `h`, and `d`.

## Semantics

The S3 sink uploads several ready objects concurrently, within its configured
object and multipart limits. A rotation closes every main and DLQ object in a
deterministic commit epoch, and source progress is committed only after the
whole epoch is durable. The per-pipeline memory budget and S3 buffering limit
propagate backpressure to PQv1. One oversized source message is admitted
atomically with a warning.

Delivery semantics are inferred from configuration and logged as a structured
report. Deterministic source/field/record-time partitioning and deterministic
rotation are exactly-once through idempotent object overwrite. Enabling
`wall_clock_interval` makes the delivery at-least-once because restart timing
can change object boundaries; the report includes a remediation.

The S3 exactly-once statement assumes that parser, middleware and projection
settings (including `keep_system_columns_in_sink`), destination identity
(bucket/endpoint/region), S3 prefix, partitioning, rotation thresholds,
`buffering.max_open_objects`, and `buffering.max_epoch_bytes` remain unchanged
while uncommitted source data can replay. Treat those fields as semantic state
during deployments; changing them can produce different object boundaries,
keys, or payloads.
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

ClickHouse currently requires an explicit `use_tls: false`. The bundled native
client cannot verify server certificates, so `use_tls: true` is rejected; use a
verified local TLS tunnel and keep only the trusted local hop plaintext.
Connection/request deadlines and finite retries are configurable. The
underlying client still performs a blocking TCP connect with its own 30-second
bound, so a smaller configured connect timeout is not a strict wall-clock
interrupt. Existing tables are checked against the Arrow schema before
ingestion.

Ready-to-edit benchmark configurations live in `benchmarks/`. The three
`discard` variants isolate network, decompression, and parsing; separate configs
exercise the full ClickHouse and S3 paths. Repository tests parse and validate
every benchmark config against the registered provider schemas.
