# transferia

Proof-of-concept data integrator written in Rust. The active path is one fully
independent pipeline per YDS/PQv1 partition:

```text
PQv1 source -> JSON parser -> middlewares -> asynchronous ClickHouse sink
```

Source, parser, middleware, and sink implementations are selected through
registries. The supported runtime path registers `pqv1`, `json_parser`,
`filter`, and `clickhouse`; other provider paths are intentionally disabled for
now.

## Build and quality checks

```bash
cargo build --release
just fmt
just clippy
just test
```

`just clippy` uses the lint policy from `Cargo.toml` and treats warnings as
errors.

## Run

```bash
transferia --config ./config.yaml --total-workers 1 --worker-index 0
```

| Flag | Environment | Default | Meaning |
|---|---|---:|---|
| `--config` | `CONFIG_PATH` | — | YAML configuration path |
| `--total-workers` | — | `1` | Number of workers used to shard partitions |
| `--worker-index` | — | `0` | Zero-based index of this worker |

YAML values support `${ENV_VAR}` and `$ENV_VAR` expansion.

## Configuration

```yaml
source:
  pqv1:
    connection_string: "grpcs://sas.logbroker.yandex.net:2135"
    topic_path: "/cdc/prod/logs"
    consumer_name: "transferia-consumer"
    partition_ids: [0] # optional; otherwise discovered from PQv1
    auth:
      type: access_token
      token_file: "~/.logbroker/token"
    parser:
      table_naming:
        type: from_config # or from_topic
        name: "logs"
      json_parser:
        chunk_splitter: new-line
        columns:
          - jsonpath: "$.id"
            column_name: id
            arrow_type: Utf8
            nullable: false
          - jsonpath: "$.timestamp"
            column_name: timestamp
            arrow_type: "Timestamp(Millisecond, UTC)"
            nullable: false
          - jsonpath: "$.event_name"
            column_name: event_name
            arrow_type: Utf8
            nullable: true

middlewares:
  - filter:
      field: event_name
      value: page_view

# Hard retained-memory budget for each independent partition pipeline.
pipeline_memory_limit_bytes: 268435456

sink:
  clickhouse:
    connection_string: "cluster.example.net:9440"
    database: default
    username: transferia
    password: "${CLICKHOUSE_PASSWORD}"
    sorting_key: [id, timestamp]
    recreate_tables: false
    max_insert_rows: 100000
    max_insert_bytes: 67108864
    flush_interval_ms: 100
    retry_initial_ms: 50
    retry_max_ms: 30000
    retry_max_attempts: null
    use_tls: true
    tls_domain: null

metrics:
  enabled: true
  interval_ms: 1000
  per_partition: true
```

Configuration ownership follows runtime ownership:

- the root contains only provider-neutral pipeline composition, memory, and
  metrics settings;
- YDS owns authentication and PQv1 connection settings;
- the JSON parser owns column mappings, Arrow type syntax, and chunk framing;
- ClickHouse owns `sorting_key`, table recreation, insert sizing, retry, and TLS
  settings;
- each middleware owns the value nested below its registry key.

`sorting_key` is ClickHouse `MergeTree ORDER BY`, not a source primary-key
declaration. An empty list produces `ORDER BY tuple()`.

## Asynchronous sink behavior

Each partition gets its own sink and shares no buffers or connections with
other partition pipelines. At most one INSERT is in flight per sink. While it
is running, the sink may accumulate the next INSERT. The shared per-pipeline
memory budget applies backpressure to the source; an individual allocation
larger than the configured limit is temporarily admitted with a warning so the
pipeline can still make progress.

Source progress is acknowledged only after every output associated with the
delivery has been inserted successfully. Exactly-once table semantics are not
implemented yet.

## ClickHouse TLS

Managed ClickHouse normally uses the native TLS port `9440`; `8443` is HTTPS,
not the native protocol. For an explicit local TLS proxy, configure the sink to
connect to the proxy and set `use_tls: false` only on that local hop.
