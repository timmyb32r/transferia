# transferia

Multi-source, multi-sink data transfer pipeline. Reads from YDB Topic, Logbroker PQv1, S3, and ClickHouse; parses JSON into Apache Arrow columnar format; applies middleware (filters); writes to ClickHouse, S3, YDS, or Empty sink.

## Sources

| Source | Type | Purpose |
|--------|------|---------|
| **YDB Topic** | streaming | CDC replication from YDB |
| **PQv1** (Logbroker) | streaming | CDC replication via MigrationStreamingRead |
| **S3** | snapshot | One-time/periodic JSON file import |
| **ClickHouse** | batch | Table-to-table transfer |

## Sinks

| Sink | Type | Purpose |
|------|------|---------|
| **ClickHouse** | native protocol | Primary analytical storage |
| **S3** | object storage | Snapshot export |
| **YDS** | streaming | Forward to YDB Topic |
| **Empty** | dev/null | Benchmarking / discard |

## Build

```bash
cargo build --release
```

Binary: `./target/release/transferia`

## Quick Start

```bash
transferia --config ./config.yaml --total-workers 1 --worker-index 0
```

### CLI

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--config` | `CONFIG_PATH` | — | Path to YAML config |
| `--total-workers` | — | `1` | Worker count (partition sharding for YDB/PQv1) |
| `--worker-index` | — | `0` | Current worker index (0-based) |

### Logging

```bash
RUST_LOG=debug transferia --config ./config.yaml   # verbose
RUST_LOG=error transferia --config ./config.yaml   # errors only
```

## Configuration

YAML with `${ENV_VAR}` and `$ENV_VAR` expansion (shellexpand). Source is selected by the key inside `source:`.

### YDB Topic

```yaml
source:
  topic:
    connection_string: "grpc://localhost:2136/local"
    topic_path: "/local/my-topic"
    consumer_name: "replicator"
    auth:
      type: anonymous
    parser:
      table_naming:
        type: from_topic
      parser_type: json_parser
      settings:
        chunk_splitter: new-line
        columns:
          - jsonpath: "$.user_id"
            column_name: "user_id"
            arrow_type: "Int64"
          - jsonpath: "$.event_name"
            column_name: "event_name"
            arrow_type: "Utf8"
```

### PQv1 (Logbroker)

```yaml
source:
  pqv1:
    connection_string: ""
    topic_path: "/cdc/prod/logs"
    consumer_name: "cdc/prod/my-consumer"
    discovery_endpoint: "grpcs://sas.logbroker.yandex.net:2135"
    partition_ids: [0]
    auth:
      type: access_token
      token_file: "~/.logbroker/token"
    parser:
      table_naming:
        type: from_config
        name: "logs"
      parser_type: json_parser
      settings:
        chunk_splitter: new-line
        columns: [...]
```

### S3 (snapshot)

```yaml
source:
  s3:
    bucket: my-bucket
    prefix: data/2024/
    region: us-east-1
    endpoint: https://s3.custom.com     # optional (MinIO, etc.)
    allow_http: false
    credentials:                        # optional (defaults to standard AWS chain)
      access_key: "${S3_ACCESS_KEY}"
      secret_key: "${S3_SECRET_KEY}"
    chunk_size_bytes: 16777216          # 16 MiB default
    max_retries: 3
    parser:
      table_naming:
        type: from_config
        name: "events"
      parser_type: json_parser
      settings:
        chunk_splitter: new-line        # required for S3
        columns: [...]
```

**S3 source notes:**
- Streaming reads: files are read in chunks (16 MiB), never fully loaded into memory
- Retries: transport errors (network, S3 API) are retried with exponential backoff
- Exit code: 0 — complete snapshot, 1 — any error (partial snapshot)
- Workers other than worker 0 exit immediately (S3 is not sharded)
- At-least-once: data failing during flush → replay from YDB; for S3, re-read from offset

### ClickHouse (table-to-table transfer)

```yaml
source:
  clickhouse:
    connection_string: "localhost:9000"
    database: "default"
    username: "default"
    password: ""
    tables:
      - schema: "db1"
        table: "events"
    # OR use regex patterns:
    # include_patterns:
    #   - "prod_.*"
    # exclude_patterns:
    #   - ".*_tmp"
```

**ClickHouse source notes:**
- Reads tables page by page (configurable `rows_per_page`, default 10000)
- Schema is derived automatically from the source table — no `parser` config needed
- Serializes Arrow batches to JSON for pipeline processing (Arrow → JSON → Parser → Arrow)

### Sink (ClickHouse)

```yaml
sink:
  clickhouse:
    connection_string: "localhost:9000"
    database: "default"
    batch_size: 10000
    max_linger_ms: 500
    max_connections: 4
    username: "default"
    password: ""
    use_tls: true
    tls_domain: null            # optional SNI override
```

### Sink (S3 export)

```yaml
sink:
  s3:
    bucket: my-bucket
    prefix: snapshots/
    region: us-east-1
    endpoint: https://s3.custom.com     # optional
    access_key: "${S3_ACCESS_KEY}"      # optional
    secret_key: "${S3_SECRET_KEY}"      # optional
    serializer_type: json
```

### Sink (YDS forward)

```yaml
sink:
  yds:
    connection_string: "grpc://localhost:2135/local"
    topic_path: "/Root/my-topic"
    serializer_type: json
```

### Sink (Empty / dev-null)

```yaml
sink:
  empty:
    batch_size: 10000
```

### Middlewares

```yaml
middlewares:
  - type: filter
    field: event_name
    value: page_view
```

## Local Dev

```bash
cd docker-compose
docker-compose up -d clickhouse ydb
cargo run --release -- --config ./config.yaml
```

## Production (Yandex Cloud Managed ClickHouse)

Yandex Cloud Managed ClickHouse requires TLS on port 9440. If `clickhouse-arrow` fails the handshake, use `stunnel` as a TLS proxy:

```bash
sudo apt install stunnel4
```

`/etc/stunnel/clickhouse.conf`:
```ini
[clickhouse]
client = yes
accept = 127.0.0.1:19000
connect = <cluster-FQDN>:9440
```

```yaml
sink:
  clickhouse:
    connection_string: "127.0.0.1:19000"
    use_tls: false
```

```
transferia ──plain TCP──→ stunnel ──TLS──→ ClickHouse
```

## Delivery Guarantees

**At-least-once** for main and DLQ streams. The commit marker in YDB is set only after successful write of all tables (main + DLQ) to the sink. On failure — replay from the last committed offset.

**Exactly-once** (opt-in): on ClickHouse sinks with `ReplicatedMergeTree`, uses waterline deduplication by `(partition, offset)`. Requires ClickHouse ≥ 22.8 for `select_sequential_consistency`.

## Architecture

```
Source ──→ Reader (tokio) ──→ Parser (std::thread) ──→ Writer (tokio) ──→ Sink
  │              │                      │                       │
  │         mpsc channel           mpsc channel           accumulator
  │                                                       + flush
  └── CommitMarker ──────────────────────────────────────→ commit ↲
```

Three async stages with backpressure via bounded mpsc channels. The parser runs on a dedicated thread (independent of the tokio blocking pool). The writer accumulates `TableWrite`s and flushes by size or timeout.

### Stack

- **Rust** + tokio (async runtime)
- **Apache Arrow** 57 — columnar data format
- **clickhouse-arrow** — ClickHouse native protocol
- **object_store** (DataFusion) — S3 access
- **simd-json** — accelerated JSON parsing
- **ydb** — YDB Topic API
- **tonic/prost** — gRPC for PQv1

## Data Schema

The schema is defined in the YAML config (columns + Arrow types + JSONPath). Based on this:

1. An Arrow schema is built for `RecordBatch`
2. `CREATE TABLE` DDL is generated for ClickHouse (Arrow types → ClickHouse types)
3. The parser extracts values from JSON and populates columnar builders

DLQ table (`<table>.dlq`) is created automatically — fixed schema: `raw_bytes`, `error_message`, `partition_id`, `timestamp` (at-least-once) or `raw_bytes`, `error_message`, `timestamp`, `<partition_key>`, `<offset_key>` (exactly-once).
