# ch-loader

Мульти-источниковый загрузчик в ClickHouse через Apache Arrow. Читает из YDB Topic (CDC), Logbroker PQv1 и S3 (снапшоты), парсит JSON в колоночный формат, пишет в ClickHouse через нативный протокол.

## Источники

| Источник | Тип | Назначение |
|----------|-----|------------|
| **YDB Topic** | стриминг | CDC-репликация из YDB |
| **PQv1** (Logbroker) | стриминг | CDC-репликация через MigrationStreamingRead |
| **S3** | снапшот | Разовая/периодическая загрузка JSON-файлов |

## Сборка

```bash
cargo build --release
```

Бинарник: `./target/release/ch-loader`

## Быстрый старт

```bash
ch-loader --config ./config.yaml --total-workers 1 --worker-index 0
```

### CLI

| Флаг | Env | По умолчанию | Описание |
|------|-----|-------------|----------|
| `--config` | `CONFIG_PATH` | — | Путь к YAML-конфигу |
| `--total-workers` | — | `1` | Воркеров (шардирование партиций для YDB/PQv1) |
| `--worker-index` | — | `0` | Индекс текущего воркера (0-based) |

### Логирование

```bash
RUST_LOG=debug ch-loader --config ./config.yaml   # детальные
RUST_LOG=error ch-loader --config ./config.yaml   # только ошибки
```

## Конфигурация

YAML с поддержкой `${ENV_VAR}` и `$ENV_VAR` (shellexpand). Источник выбирается ключом внутри `source:`:

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

### S3 (снапшот)

```yaml
source:
  s3:
    bucket: my-bucket
    prefix: data/2024/
    region: us-east-1
    endpoint: https://s3.custom.com     # опционально (MinIO и т.п.)
    allow_http: false
    credentials:                        # опционально (без них — стандартная AWS-цепочка)
      access_key: "${S3_ACCESS_KEY}"
      secret_key: "${S3_SECRET_KEY}"
    chunk_size_bytes: 16777216          # 16 MiB по умолчанию
    max_retries: 3
    parser:
      table_naming:
        type: from_config
        name: "events"
      parser_type: json_parser
      settings:
        chunk_splitter: new-line        # обязательно new-line для S3
        columns: [...]
```

**Особенности S3-источника:**
- Потоковое чтение: файлы читаются кусками (16 MiB), не загружаются в память целиком
- Ретраи: Transport-ошибки (сеть, S3 API) ретраятся с экспоненциальным backoff
- Контракт exit code: 0 — полный снапшот, 1 — любая ошибка (частичный снапшот)
- Все воркеры кроме worker 0 выходят сразу (S3 не шардируется)
- At-least-once: данные, сбой при flush → реплей с YDB; для S3 перечитывание с оффсета

### Sink (ClickHouse)

```yaml
sink:
  connection_string: "localhost:9000"
  database: "default"
  batch_size: 10000
  max_linger_ms: 500
  max_connections: 4
  username: "default"
  password: ""
  use_tls: true
  tls_domain: null            # опциональный SNI
  recreate_tables: false      # только dev/bench
```

### Middlewares

```yaml
middlewares:
  - type: filter
    field: event_name
    value: page_view
```

## Локальный запуск (dev)

```bash
cd docker-compose
docker-compose up -d clickhouse ydb
cargo run --release -- --config ./config.yaml
```

## Продакшн (Yandex Cloud ClickHouse)

Yandex Cloud Managed ClickHouse требует TLS на порту 9440. Если `clickhouse-arrow` не проходит рукопожатие — используй `stunnel` как TLS-прокси:

```bash
sudo apt install stunnel4
```

`/etc/stunnel/clickhouse.conf`:
```ini
[clickhouse]
client = yes
accept = 127.0.0.1:19000
connect = <FQDN-кластера>:9440
```

```yaml
sink:
  connection_string: "127.0.0.1:19000"
  use_tls: false
```

```
ch-loader ──plain TCP──→ stunnel ──TLS──→ ClickHouse
```

## Гарантии доставки

**At-least-once** для основного и DLQ-потоков. Commit-маркер в YDB выставляется только после успешной записи всех таблиц (main + DLQ) в ClickHouse. При сбое — реплей с последнего закоммиченного оффсета.

## Архитектура

```
Source ──→ Reader (tokio) ──→ Parser (std::thread) ──→ Writer (tokio) ──→ ClickHouse
  │              │                      │                       │
  │         mpsc channel           mpsc channel           accumulator
  │                                                       + flush
  └── CommitMarker ──────────────────────────────────────→ commit ↲
```

Три асинхронных этапа с backpressure через bounded mpsc-каналы. Парсер — выделенный поток (не зависит от tokio blocking pool). Writer аккумулирует `TableWrite` и флашит по размеру или таймауту.

### Стек

- **Rust** + tokio (async runtime)
- **Apache Arrow** 57 — колоночный формат данных
- **clickhouse-arrow** — нативный протокол ClickHouse
- **object_store** (DataFusion) — S3-доступ
- **simd-json** — ускоренный JSON-парсинг
- **ydb** — YDB Topic API
- **tonic/prost** — gRPC для PQv1

## Схема данных

Схема задаётся в YAML-конфиге (колонки + Arrow-типы + JSONPath). На основе неё:
1. Строится Arrow-схема для `RecordBatch`
2. Генерируется `CREATE TABLE` DDL для ClickHouse (Arrow-типы → ClickHouse-типы)
3. Парсер извлекает значения из JSON и заполняет колоночные билдеры

DLQ-таблица (`<table>.dlq`) создаётся автоматически — фиксированная схема: `raw_bytes`, `error_message`, `partition_id`, `timestamp`.
