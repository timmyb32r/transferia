# Проверенная база Transferia Rust

Дата: 2026-09-19. Локальный checkout `/Users/timmyb32r/cursor/ai/005_rust`, commit `5282923723023b6cf7b6193393a0ef4a6baff73c`; перед исследованием worktree был чистым. Это аудит встроенной композиции данного checkout, не всех внутренних расширений, внешних скриптов или удалённых deployment. Проверялись production-контракты, конфигурации и пути выполнения; тесты и benchmarks не запускались.

Ссылки фиксируют расположение в этой ревизии. README местами отстаёт от кода: например, текущие PG publication и YDB batch-and-stream возможности шире некоторых описаний README. При конфликте ниже приоритет у исходников.

## T1 — Snapshot splitting PostgreSQL и MySQL

- [PG discovery](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/source/connector.rs:883): `0..tables.len()`; batch и batch-and-stream используют `CoLocatedStaticPartitions`.
- [PG reader](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/src_batch/reader.rs:118): один `SELECT ... FROM schema.table` внутри COPY; пользовательских range predicates/chunk keys нет в [config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/source/config.rs:12).
- [MySQL discovery](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-mysql/src/connectors/mysql/src_batch/connector.rs:1574): один partition на таблицу, [reader](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-mysql/src/connectors/mysql/src_batch/reader.rs:202) читает таблицу целиком.
- Вывод: параллельность между таблицами есть; native split одной PG/MySQL таблицы на несколько readers отсутствует. Размер Arrow batch не является split таблицы.

## T2 — Что уже параллельно

- [OpenSearch config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-opensearch/src/opensearch/src_batch/config.rs:35), [reader](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-opensearch/src/opensearch/src_batch/source.rs:255): PIT, slices по shard count, ограничение одновременных запросов.
- [YTsaurus read ordering](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-ytsaurus/src/connectors/ytsaurus/config.rs:260): Ordered resumable, Unordered и PartitionTables non-resumable; последнее с размером раздела, concurrency и max partition count.
- [ClickHouse config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-clickhouse/src/connectors/clickhouse/src_batch/config.rs:100): server-side `max_threads`, Parquet decode threads; [discovery](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-clickhouse/src/connectors/clickhouse/src_batch/connector.rs:388) по-прежнему один logical partition на таблицу.
- Нельзя писать «в Transferia нет параллелизма/разбиения таблиц вообще».

## T3 — Snapshot recovery / CDC handoff

- PG и MySQL имеют точный snapshot→log handoff и durable phase/offset state.
- [PG recovery](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/src_batch_and_stream/phase.rs:131) явно останавливается после потери exported snapshot, требует сознательного сброса попытки destination.
- [MySQL recovery](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-mysql/src/connectors/mysql/src_batch_and_stream/phase.rs:136) аналогично: connection-owned snapshot не переживает процесс.
- Это безопасный отказ, а не потеря данных. Пробел — restartable chunk-level initial load и online watermark-based incremental snapshot/backfill; не отсутствие CDC или checkpoints.

## T4 — DDL и изменение набора таблиц

- [PG config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/source/config.rs:19) фиксирует membership; [relation validation](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/src_stream/relation_identity.rs:246) отвергает schema drift.
- [MySQL config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-mysql/src/connectors/mysql/src_batch/config.rs:52): `new_tables=include` уже поддерживает новые CREATE TABLE. [DDL admission](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-mysql/src/connectors/mysql/src_stream/ddl.rs:31) разрешает доказанное создание пустой permanent table; rename существующей таблицы требует нового snapshot.
- [Core SourceBatch::Dataset](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-core/src/data/message.rs:57) и [admission coordinator](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/execution/admission.rs) уже имеют ordered admission barrier. Это не универсальное ADD/ALTER/DROP schema evolution существующих datasets.
- [PG pgoutput](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/src_stream/pgoutput.rs:51) отвергает TRUNCATE.

## T5 — Топология и transforms

- [Runnable config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/config/yaml.rs:13): один source, один sink и ordered middleware list.
- Но [resolution](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/preparation/mod.rs:217) разворачивает несколько installations в source×sink независимые pipelines. Поэтому «несколько destinations вообще невозможны» — неверно.
- Пробел: один read → общий DAG/несколько ветвей с согласованными ack; контентная маршрутизация между независимыми sinks, multi-source join.
- [Registry](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connectors/src/connectors/catalog.rs:324): filter, rename_table, DataFusion.
- [DataFusion](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-middleware-datafusion/src/lib.rs:47) регистрирует один `input`; [execute](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-middleware-datafusion/src/lib.rs:61) создаёт новый SessionContext на batch. SQL calculations, casts, projection и batch-local aggregations не отсутствуют. Нет maintained cross-batch state, multi-input tables, durable streaming windows/join.
- [Merge validation](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/preparation/mod.rs:437): одинаковые append-only схемы можно свести rename; PK merges отвергаются без cross-source conflict contract.

## T6 — Распределённость и durable state

- [SourceTopology](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-core/src/delivery.rs:96): StaticPartitions, CoLocatedStaticPartitions, DynamicWorkerLanes. Static assignment — modulo worker count; co-located — worker 0.
- [Phase validation](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/execution/runner.rs:749): multi-phase execution требует co-location до появления distributed phase barrier.
- Есть CLI worker count/index и broker-managed динамические lanes; нет поставляемого cluster scheduler/rebalancer/runtime adapter кроме [local supervisor](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-runtime-local/src/supervisor.rs).
- [DurableStorageConfig](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-registry/src/durable.rs:88): только LocalFile. Trait/CAS/leases существуют; shared remote storage backend не реализован этим enum.

## T7 — Control plane и эксплуатация

- [Architecture](/Users/timmyb32r/cursor/ai/005_rust/docs/server.md:3): local single-user, без remote authentication boundary. Stateful CAS/revision/run_id, private file permissions и loopback protections уже есть.
- [HTTP routes](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-control-plane/src/server/http.rs:152): create/update/delete, validate, activate, stop, logs, discovery, preview, speedtest. Нет маршрутов scheduler, savepoint, historical version restore, user/role/workspace, table-resync или audit export.
- [Lifecycle](/Users/timmyb32r/cursor/ai/005_rust/docs/server.md:52): сервер владеет workers; после restart deliveries остаются stopped. Это осознанный local lifecycle, не cluster HA.
- [MetricsConfig](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery-contracts/src/metrics.rs:36) — interval/per_partition; сам модуль ведёт подробные counters и пишет structured stats. В проверенном control-plane API нет metrics exporter/alert manager/time-series API.

## T8 — Проверки данных и recovery guarantees уже существуют

- [Shared preparation](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/preparation/mod.rs:274): discovery, semantic validation, sink limits до destination preparation.
- [Snapshot reconciliation](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery/src/delivery/execution/runner.rs:35): точные output row counts; для поддерживающих sinks сравнение destination counts, только single-worker. Это не checksum/row diff/online repair.
- [Semantics](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery-contracts/src/semantics.rs:108): row-kind-aware CDC sinks, не только append-only. Конкретные guarantees различаются; нельзя считать «exactly-once отсутствует» общей характеристикой.
- [Delivery guarantees](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-delivery-contracts/src/semantics.rs:381): ClickHouse/PostgreSQL/MySQL и ряд других sinks явно классифицированы как at-least-once; ambiguous INSERT/COPY/COMMIT допускает повторный эффект. Source-commit barrier после sink completion сам по себе не является атомарной связкой destination effects и source progress.
- [Pipeline commit ordering](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-pipeline/src/lib.rs:409) и [Sink port](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-core/src/sink.rs:32) не объединяют SQL transaction и source checkpoint. В проверенных [PG writer](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-postgres/src/connectors/postgres/sink/writer.rs:40) / [MySQL writer](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-mysql/src/connectors/mysql/sink/writer.rs:57) и их конфигурациях нет XA/2PC или destination-side source-coordinate commit ledger для append workloads. Обычная DB transaction не закрывает ambiguous-commit replay gap.
- S3 deterministic commit epochs, Iceberg replica row-delta commits, source offsets/identity validation, retries и memory backpressure уже документированы и реализованы соответствующими компонентами.

## T9 — S3 и форматы

- [Source config](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-s3/src/connectors/s3/src_batch/config.rs:21): finite prefix/table/parser; JSON, Parquet, discard.
- [Connector](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-s3/src/connectors/s3/src_batch/connector.rs:231) допускает только partition 0.
- [Reader](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-s3/src/connectors/s3/src_batch/reader.rs:56) держит один active Parquet stream; JSON читает один object за раз; [commit_offsets](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-s3/src/connectors/s3/src_batch/reader.rs:222) ничего не сохраняет.
- Нет native continuous object notifications/poll+durable listing cursor и independently scheduled file/row-group splits. Не путать с уже имеющимся concurrent multipart/upload S3 sink.
- [Parser catalog](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connector-support/src/parsers/config.rs:17) включает JSON, TSKV, Schema Registry, Debezium, raw-to-table, discard. Schema Registry Avro/Protobuf и JSON unknown-field/DLQ политики уже есть.

## T10 — Каталог, extensibility, ограничения вывода

- [Built-in endpoints](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connectors/src/connectors/catalog/descriptor.rs:142): Logbroker, Kafka, MySQL, OpenSearch, PostgreSQL, ClickHouse, S3, Iceberg, YDB, YTsaurus; generator source/discard sink.
- [Extension ports](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-connectors/src/extension/mod.rs:50), registry factories и compiled composition fingerprint — уже расширяемая архитектура. Отсутствует готовый универсальный no-code connector builder/внешний plugin package lifecycle, а не возможность писать коннекторы вообще.
- [Speedtest/tuning](/Users/timmyb32r/cursor/ai/005_rust/crates/transferia-registry/src/tuning.rs:21) уже имеет connector-authored safe search space; generic «autotuning отсутствует» исключён из backlog.
- Общего query-pushdown, timestamp-cursor extraction, quality-rule catalog, SCD2, connector-independent persisted transform-state интерфейсов нет в проверенных config/registry/middleware execution paths. Это архитектурный вывод по перечисленным boundaries, не утверждение, что нужную операцию нельзя собрать внешними скриптами или написать новым Rust-компонентом.
