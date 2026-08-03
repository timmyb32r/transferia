# ydb-ch-replicator

YDB Topic (CDC) → ClickHouse репликатор.

## Сборка

```bash
cargo build --release
```

Бинарник: `./target/release/ydb-ch-replicator`

## Конфигурация

Файл конфига — YAML с поддержкой `${ENV_VAR}` и `$ENV_VAR`:

```bash
ydb-ch-replicator --config ./config_bench.yaml --total-workers 1 --worker-index 0
```

### CLI-аргументы

| Флаг | Env | По умолчанию | Описание |
|------|-----|-------------|----------|
| `--config` | `CONFIG_PATH` | — | Путь к YAML-конфигу |
| `--total-workers` | — | `1` | Общее количество воркеров (для шардирования партиций) |
| `--worker-index` | — | `0` | Индекс текущего воркера (0-based) |

### Логирование

Уровень логов по умолчанию — `info`. Переопределяется через `RUST_LOG`:

```bash
RUST_LOG=debug ./target/release/ydb-ch-replicator --config ./config_bench.yaml  # детальные
RUST_LOG=error ./target/release/ydb-ch-replicator --config ./config_bench.yaml  # только ошибки
```

## Локальный запуск (dev, без TLS)

```bash
# Поднять ClickHouse и YDB
cd docker-compose
docker-compose up -d

# Запустить репликатор
cargo run --release -- --config ./config.yaml
```

Конфиг для локального запуска (`config.yaml`):

```yaml
source:
  source_type: "pqv1"
  connection_string: "grpc://localhost:2136/local"
  topic_path: "/cdc/prod/logs"
  consumer_name: "cdc/prod/timmyb32r-test-consumer-00"
  auth:
    type: access_token
    token_file: "~/.logbroker/token"
  ...

sink:
  connection_string: "localhost:9000"   # plain TCP, без TLS
  use_tls: false
  database: "default"
  username: "default"
  password: ""
  ...
```

## Продакшн-запуск (Yandex Cloud ClickHouse)

Yandex Cloud Managed ClickHouse требует TLS (порт 9440 — native protocol, **не** 8443).
Из-за [бага](https://github.com/0x6767/clickhouse-arrow/issues) `aws-lc-rs` на Linux `clickhouse-arrow`
не может пройти TLS-рукопожатие с яндексовыми сертификатами.

**Решение: `stunnel` как TLS-прокси.**

### 1. Установить сертификаты Yandex

```bash
sudo mkdir -p /usr/local/share/ca-certificates/Yandex

sudo wget "https://crls.yandex.net/YandexInternalRootCA.crt" \
  -O /usr/local/share/ca-certificates/Yandex/RootCA.crt

sudo wget "https://crls.yandex.net/YandexInternalCA.crt" \
  -O /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt

sudo chmod 644 /usr/local/share/ca-certificates/Yandex/*.crt
sudo update-ca-certificates
```

### 2. Установить и настроить stunnel

```bash
sudo apt install stunnel4
```

Конфиг `/etc/stunnel/clickhouse.conf`:

```ini
[clickhouse]
client = yes
accept = 127.0.0.1:19000
connect = klg-v4c8sf9k2aejn05u.db.yandex.net:9440
```

Замени `klg-...` на FQDN своего кластера.

Запустить:

```bash
sudo stunnel /etc/stunnel/clickhouse.conf
```

Проверить:

```bash
ss -tlnp | grep 19000        # должен слушать порт
timeout 3 nc -v 127.0.0.1 19000  # должно показать Connected
```

### 3. Конфиг репликатора для stunnel

```yaml
sink:
  connection_string: "127.0.0.1:19000"   # localhost → stunnel → ClickHouse
  use_tls: false                          # stunnel сам делает TLS
  database: "db1"
  username: "user1"
  password: "your-password"
  recreate_tables: true
```

### 4. Запуск

```bash
./target/release/ydb-ch-replicator --config ./config_bench.yaml
```

### Схема

```
┌──────────────┐    plain TCP     ┌──────────┐    TLS (OpenSSL)    ┌─────────────────────┐
│  replicator  │ ───────────────→ │ stunnel  │ ─────────────────→ │  Yandex ClickHouse  │
│              │ 127.0.0.1:19000  │          │  klg-...:9440      │                     │
└──────────────┘                   └──────────┘                     └─────────────────────┘
```

## Конфигурация sink (ClickHouse)

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `connection_string` | string | — | ClickHouse native protocol адрес |
| `database` | string | — | Имя базы данных |
| `batch_size` | usize | `10000` | Размер батча INSERT |
| `max_linger_ms` | u64 | `500` | Макс. ожидание перед flush частичного батча |
| `max_connections` | usize | `4` | Размер пула соединений |
| `username` | string | `"default"` | Пользователь ClickHouse |
| `password` | string | `""` | Пароль |
| `use_tls` | bool | `true` | Использовать TLS |
| `tls_domain` | string? | — | SNI-домен (если отличается от хоста в connection_string) |
| `recreate_tables` | bool | `false` | **Только dev/bench:** DROP + CREATE таблиц при старте |
