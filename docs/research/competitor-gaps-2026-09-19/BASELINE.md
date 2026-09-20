# Проверенная база Transferia Rust

Дата: 2026-09-19. Локальный checkout репозитория, commit `5282923723023b6cf7b6193393a0ef4a6baff73c`; перед исследованием worktree был чистым. Это аудит встроенной композиции данного checkout, не всех внутренних расширений, внешних скриптов или удалённых deployment. Проверялись production-контракты, конфигурации и пути выполнения; тесты и benchmarks не запускались.

Ссылки фиксируют расположение в этой ревизии. README местами отстаёт от кода: например, текущие PG publication и YDB batch-and-stream возможности шире некоторых описаний README. При конфликте ниже приоритет у исходников.

## T1 — Snapshot splitting PostgreSQL и MySQL

- [PG discovery](../../../crates/transferia-connector-postgres/src/connectors/postgres/source/connector.rs#L883): `0..tables.len()`; batch и batch-and-stream используют `CoLocatedStaticPartitions`.
- [PG reader](../../../crates/transferia-connector-postgres/src/connectors/postgres/src_batch/reader.rs#L118): один `SELECT ... FROM schema.table` внутри COPY; пользовательских range predicates/chunk keys нет в [config](../../../crates/transferia-connector-postgres/src/connectors/postgres/source/config.rs#L12).
- [MySQL discovery](../../../crates/transferia-connector-mysql/src/connectors/mysql/src_batch/connector.rs#L1574): один partition на таблицу, [reader](../../../crates/transferia-connector-mysql/src/connectors/mysql/src_batch/reader.rs#L202) читает таблицу целиком.
- Вывод: параллельность между таблицами есть; native split одной PG/MySQL таблицы на несколько readers отсутствует. Размер Arrow batch не является split таблицы.

## T2 — Что уже параллельно

- [OpenSearch config](../../../crates/transferia-connector-opensearch/src/opensearch/src_batch/config.rs#L35), [reader](../../../crates/transferia-connector-opensearch/src/opensearch/src_batch/source.rs#L255): PIT, slices по shard count, ограничение одновременных запросов.
- [YTsaurus read ordering](../../../crates/transferia-connector-ytsaurus/src/connectors/ytsaurus/config.rs#L260): Ordered resumable, Unordered и PartitionTables non-resumable; последнее с размером раздела, concurrency и max partition count.
- [ClickHouse config](../../../crates/transferia-connector-clickhouse/src/connectors/clickhouse/src_batch/config.rs#L100): server-side `max_threads`, Parquet decode threads; [discovery](../../../crates/transferia-connector-clickhouse/src/connectors/clickhouse/src_batch/connector.rs#L388) по-прежнему один logical partition на таблицу.
- Нельзя писать «в Transferia нет параллелизма/разбиения таблиц вообще».

## T3 — Snapshot recovery / CDC handoff

- PG и MySQL имеют точный snapshot→log handoff и durable phase/offset state.
- [PG recovery](../../../crates/transferia-connector-postgres/src/connectors/postgres/src_batch_and_stream/phase.rs#L131) явно останавливается после потери exported snapshot, требует сознательного сброса попытки destination.
- [MySQL recovery](../../../crates/transferia-connector-mysql/src/connectors/mysql/src_batch_and_stream/phase.rs#L136) аналогично: connection-owned snapshot не переживает процесс.
- Это безопасный отказ, а не потеря данных. Пробел — restartable chunk-level initial load и online watermark-based incremental snapshot/backfill; не отсутствие CDC или checkpoints.

## T4 — DDL и изменение набора таблиц

- [PG config](../../../crates/transferia-connector-postgres/src/connectors/postgres/source/config.rs#L19) фиксирует membership; [relation validation](../../../crates/transferia-connector-postgres/src/connectors/postgres/src_stream/relation_identity.rs#L246) отвергает schema drift.
- [MySQL config](../../../crates/transferia-connector-mysql/src/connectors/mysql/src_batch/config.rs#L52): `new_tables=include` уже поддерживает новые CREATE TABLE. [DDL admission](../../../crates/transferia-connector-mysql/src/connectors/mysql/src_stream/ddl.rs#L31) разрешает доказанное создание пустой permanent table; rename существующей таблицы требует нового snapshot.
- [Core SourceBatch::Dataset](../../../crates/transferia-core/src/data/message.rs#L57) и [admission coordinator](../../../crates/transferia-delivery/src/delivery/execution/admission.rs) уже имеют ordered admission barrier. Это не универсальное ADD/ALTER/DROP schema evolution существующих datasets.
- [PG pgoutput](../../../crates/transferia-connector-postgres/src/connectors/postgres/src_stream/pgoutput.rs#L51) отвергает TRUNCATE.

## T5 — Топология и transforms

- [Runnable config](../../../crates/transferia-delivery/src/delivery/config/yaml.rs#L13): один source, один sink и ordered middleware list.
- Но [resolution](../../../crates/transferia-delivery/src/delivery/preparation/mod.rs#L217) разворачивает несколько installations в source×sink независимые pipelines. Поэтому «несколько destinations вообще невозможны» — неверно.
- Пробел: один read → общий DAG/несколько ветвей с согласованными ack; контентная маршрутизация между независимыми sinks, multi-source join.
- [Registry](../../../crates/transferia-connectors/src/connectors/catalog.rs#L324): filter, rename_table, DataFusion.
- [DataFusion](../../../crates/transferia-middleware-datafusion/src/lib.rs#L47) регистрирует один `input`; [execute](../../../crates/transferia-middleware-datafusion/src/lib.rs#L61) создаёт новый SessionContext на batch. SQL calculations, casts, projection и batch-local aggregations не отсутствуют. Нет maintained cross-batch state, multi-input tables, durable streaming windows/join.
- [Merge validation](../../../crates/transferia-delivery/src/delivery/preparation/mod.rs#L437): одинаковые append-only схемы можно свести rename; PK merges отвергаются без cross-source conflict contract.

## T6 — Распределённость и durable state

- [SourceTopology](../../../crates/transferia-core/src/delivery.rs#L96): StaticPartitions, CoLocatedStaticPartitions, DynamicWorkerLanes. Static assignment — modulo worker count; co-located — worker 0.
- [Phase validation](../../../crates/transferia-delivery/src/delivery/execution/runner.rs#L749): multi-phase execution требует co-location до появления distributed phase barrier.
- Есть CLI worker count/index и broker-managed динамические lanes; нет поставляемого cluster scheduler/rebalancer/runtime adapter кроме [local supervisor](../../../crates/transferia-runtime-local/src/supervisor.rs).
- [DurableStorageConfig](../../../crates/transferia-registry/src/durable.rs#L88): только LocalFile. Trait/CAS/leases существуют; shared remote storage backend не реализован этим enum.

## T7 — Control plane и эксплуатация

- [Architecture](../../server.md#L3): local single-user, без remote authentication boundary. Stateful CAS/revision/run_id, private file permissions и loopback protections уже есть.
- [HTTP routes](../../../crates/transferia-control-plane/src/server/http.rs#L152): create/update/delete, validate, activate, stop, logs, discovery, preview, speedtest. Нет маршрутов scheduler, savepoint, historical version restore, user/role/workspace, table-resync или audit export.
- [Lifecycle](../../server.md#L52): сервер владеет workers; после restart deliveries остаются stopped. Это осознанный local lifecycle, не cluster HA.
- [MetricsConfig](../../../crates/transferia-delivery-contracts/src/metrics.rs#L36) — interval/per_partition; сам модуль ведёт подробные counters и пишет structured stats. В проверенном control-plane API нет metrics exporter/alert manager/time-series API.

## T8 — Проверки данных и recovery guarantees уже существуют

- [Shared preparation](../../../crates/transferia-delivery/src/delivery/preparation/mod.rs#L274): discovery, semantic validation, sink limits до destination preparation.
- [Snapshot reconciliation](../../../crates/transferia-delivery/src/delivery/execution/runner.rs#L35): точные output row counts; для поддерживающих sinks сравнение destination counts, только single-worker. Это не checksum/row diff/online repair.
- [Semantics](../../../crates/transferia-delivery-contracts/src/semantics.rs#L108): row-kind-aware CDC sinks, не только append-only. Конкретные guarantees различаются; нельзя считать «exactly-once отсутствует» общей характеристикой.
- [Delivery guarantees](../../../crates/transferia-delivery-contracts/src/semantics.rs#L381): ClickHouse/PostgreSQL/MySQL и ряд других sinks явно классифицированы как at-least-once; ambiguous INSERT/COPY/COMMIT допускает повторный эффект. Source-commit barrier после sink completion сам по себе не является атомарной связкой destination effects и source progress.
- [Pipeline commit ordering](../../../crates/transferia-pipeline/src/lib.rs#L409) и [Sink port](../../../crates/transferia-core/src/sink.rs#L32) не объединяют SQL transaction и source checkpoint. В проверенных [PG writer](../../../crates/transferia-connector-postgres/src/connectors/postgres/sink/writer.rs#L40) / [MySQL writer](../../../crates/transferia-connector-mysql/src/connectors/mysql/sink/writer.rs#L57) и их конфигурациях нет XA/2PC или destination-side source-coordinate commit ledger для append workloads. Обычная DB transaction не закрывает ambiguous-commit replay gap.
- S3 deterministic commit epochs, Iceberg replica row-delta commits, source offsets/identity validation, retries и memory backpressure уже документированы и реализованы соответствующими компонентами.

## T9 — S3 и форматы

- [Source config](../../../crates/transferia-connector-s3/src/connectors/s3/src_batch/config.rs#L21): finite prefix/table/parser; JSON, Parquet, discard.
- [Connector](../../../crates/transferia-connector-s3/src/connectors/s3/src_batch/connector.rs#L231) допускает только partition 0.
- [Reader](../../../crates/transferia-connector-s3/src/connectors/s3/src_batch/reader.rs#L56) держит один active Parquet stream; JSON читает один object за раз; [commit_offsets](../../../crates/transferia-connector-s3/src/connectors/s3/src_batch/reader.rs#L222) ничего не сохраняет.
- Нет native continuous object notifications/poll+durable listing cursor и independently scheduled file/row-group splits. Не путать с уже имеющимся concurrent multipart/upload S3 sink.
- [Parser catalog](../../../crates/transferia-connector-support/src/parsers/config.rs#L17) включает JSON, TSKV, Schema Registry, Debezium, raw-to-table, discard. Schema Registry Avro/Protobuf и JSON unknown-field/DLQ политики уже есть.

## T10 — Каталог, extensibility, ограничения вывода

- [Built-in endpoints](../../../crates/transferia-connectors/src/connectors/catalog/descriptor.rs#L142): Logbroker, Kafka, MySQL, OpenSearch, PostgreSQL, ClickHouse, S3, Iceberg, YDB, YTsaurus; generator source/discard sink.
- [Extension ports](../../../crates/transferia-connectors/src/extension/mod.rs#L50), registry factories и compiled composition fingerprint — уже расширяемая архитектура. Отсутствует готовый универсальный no-code connector builder/внешний plugin package lifecycle, а не возможность писать коннекторы вообще.
- [Speedtest/tuning](../../../crates/transferia-registry/src/tuning.rs#L21) уже имеет connector-authored safe search space; generic «autotuning отсутствует» исключён из backlog.
- Общего query-pushdown, timestamp-cursor extraction, quality-rule catalog, SCD2, connector-independent persisted transform-state интерфейсов нет в проверенных config/registry/middleware execution paths. Это архитектурный вывод по перечисленным boundaries, не утверждение, что нужную операцию нельзя собрать внешними скриптами или написать новым Rust-компонентом.
