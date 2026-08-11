# transferia

A performance-oriented Rust data integrator proof of concept. The active runtime
path creates one completely independent pipeline per PQv1 partition:

```text
PQv1 -> JSON parser -> middlewares -> asynchronous S3 sink
```

Providers and parsers are selected dynamically from registries. The executable
currently registers only the `pqv1` source and `s3` sink.

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
    connection_string: "grpcs://sas.logbroker.yandex.net:2135"
    topic_path: "/cdc/prod/events"
    consumer_name: "transferia-consumer"
    partition_ids: [0] # optional; otherwise discovered
    auth:
      type: access_token
      token_file: "~/.logbroker/token"
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
      # type: time
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
      max_buffered_bytes: 256MiB

    upload:
      multipart_threshold: 25MiB
      part_size: 25MiB
      parallel_parts: 4

    retry:
      initial_backoff: 200ms
      max_backoff: 30s

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

The sink permits exactly one object upload at a time while accumulating the
next deterministic commit epoch. A rotation closes every main and DLQ object
in that epoch, and source progress is committed only after all of them are
durable. The per-pipeline memory budget and S3 buffering limit propagate
backpressure to PQv1. One oversized source message is admitted atomically with
a warning.

Delivery semantics are inferred from configuration and logged as a structured
report. Deterministic source/field/source-time partitioning and deterministic
rotation are exactly-once through idempotent object overwrite. Enabling
`wall_clock_interval` makes the delivery at-least-once because restart timing
can change object boundaries; the report includes a remediation.

Field partitioning requires `one-message-one-row`, non-null scalar partition
columns, and parser-generated source identity columns. Time partitioning uses
only `_system_write_timestamp_ms`; extracting time from user fields remains
intentionally forbidden until a persistent fenced state machine exists.

JSON objects are compact NDJSON compatible with the Confluent S3 JSON shape:
one object per line, explicit nulls, and a final newline. System columns are
projected out unless `keep_system_columns_in_sink: true`.
