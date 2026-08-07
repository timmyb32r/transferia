# ClickHouse: полный список способов поставлять данные с гарантиями exactly-once

> Исследование проведено по исходникам ClickHouse `/Users/timmyb32r/tmp/ClickHouse` (src + docs/ru + docs/en).  
> Дата: 2026-08-07.

---

## 1. Как ClickHouse понимает exactly-once

ClickHouse не реализует классический двухфазный коммит или distributed transaction log для ingestion.  
Основная модель — **at-least-once доставка + идемпотентная дедупликация на стороне получателя**:

* источник/клиент может повторять отправку;
* сервер вычисляет идентификатор вставляемого блока (хеш данных или пользовательский токен) и сохраняет его в локальном логе или в ClickHouse Keeper/ZooKeeper;
* повторный блок с тем же идентификатором отбрасывается.

Таким образом, в ClickHouse **exactly-once** достигается не на уровне транспорта, а на уровне приёмника: «если retry приходит в пределах окна дедупликации, он не создаёт дубликатов».  Для полноценной exactly-once семантики со стороны источника (Kafka, S3, CDC и т.п.) требуется, чтобы источник отслеживал свой прогресс и мог повторно отправить тот же детерминированный batch, а ClickHouse при этом отфильтрует уже записанные строки.

---

## 2. Встроенные механизмы идемпотентности вставок

### 2.1 `INSERT INTO ReplicatedMergeTree`

* **Механизм:** при каждой вставке вычисляется unified-хеш блока (или берётся `insert_deduplication_token`) и атомарно бронируется эфемерная нода в Keeper/ZooKeeper: `/table_path/deduplication_hashes/<hash>`.  Если нода уже существует, вставка считается дубликатом, и сервер возвращает `INSERT_WAS_DEDUPLICATED`.
* **Где:** `ReplicatedMergeTreeSink::commitPart()` → `StorageReplicatedMergeTree::allocateBlockNumber()` → `AsyncBlockIDsCache`.
* **Настройки:**
  * `replicated_deduplication_window` (UInt64, default 10000) — число хранимых хешей;
  * `replicated_deduplication_window_seconds` (UInt64, default 3600) — время жизни хешей;
  * `insert_deduplicate` / `deduplicate_insert` — включить дедупликацию;
  * `insert_deduplication_token` — пользовательский детерминированный токен.
* **Ограничения:** дедупликация работает по партициям; окно ограничено по времени и количеству; при сетевом сбое между коммитом в Keeper и ответом клиенту статус может быть `UNKNOWN_STATUS_OF_INSERT`.
* **Файлы:**
  * `src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp`
  * `src/Storages/StorageReplicatedMergeTree.cpp`
  * `src/Interpreters/InsertDeduplication.cpp`
  * `src/Storages/MergeTree/AsyncBlockIDsCache.cpp`

### 2.2 `INSERT INTO MergeTree` (non-replicated) с локальным окном

* **Механизм:** хеши блоков хранятся в локальном `MergeTreeDeduplicationLog` (`<store>/deduplication_logs/`).  Повторный блок на том же сервере отбрасывается.
* **Настройки:** `non_replicated_deduplication_window` на уровне таблицы.
* **Ограничения:** only single-node; не даёт кластерной exactly-once.
* **Файлы:**
  * `src/Storages/MergeTree/MergeTreeSink.cpp`
  * `src/Storages/MergeTree/MergeTreeDeduplicationLog.cpp`

### 2.3 Пользовательский токен `insert_deduplication_token`

* **Механизм:** вместо хеша данных в unified hash добавляется строка `user-token-<token>`.  Повторная вставка с тем же токеном в ту же партицию дедуплицируется, даже если данные отличаются.
* **Настройка:** `SETTINGS insert_deduplication_token = 'batch-2024-01-01'`.
* **Ограничения:** токен отслеживается **по партиции**; при `async_insert` разные токены могут группироваться в один блок, но каждая entry помнит свой токен.
* **Файлы:**
  * `src/Interpreters/InsertDeduplication.cpp`
  * `src/Interpreters/AsynchronousInsertQueue.cpp`
  * `src/Processors/Transforms/DeduplicationTokenTransforms.cpp`
  * `tests/queries/0_stateless/02124_insert_deduplication_token.sql`

### 2.4 `async_insert` + дедупликация

* **Механизм:** сервер буферизует мелкие вставки в памяти и сбрасывает их фоном.  Каждая исходная вставка несёт свой `async_dedup_token`; при сбросе они участвуют в unified дедупликации.
* **Настройки:**
  * `async_insert` (Bool, в 26.2 default `true`);
  * `async_insert_deduplicate` (Bool, default `false`) — включить дедупликацию async-вставок;
  * `wait_for_async_insert` (Bool, default `true`) — ждать фактического коммита;
  * `wait_for_async_insert_timeout`;
  * `async_insert_max_data_size`, `async_insert_max_query_number`, `async_insert_busy_timeout_*`.
* **Ограничения:** `wait_for_async_insert=0` теряет durability; с материализованными представлениями, изменяющими число строк, ограничены сценарии.
* **Файлы:**
  * `src/Interpreters/AsynchronousInsertQueue.h`
  * `src/Interpreters/AsynchronousInsertQueue.cpp`
  * `src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp`

### 2.5 `INSERT INTO Distributed` → `ReplicatedMergeTree`

* **Механизм:** `Distributed` сам по себе не дедуплирует, но пересылает вставку на шарды.  Если целевая таблица — `ReplicatedMergeTree`, дедупликация срабатывает на шарде.  В синхронном режиме `INSERT` ждёт завершения на всех шардах; в асинхронном режиме данные сначала пишутся в файловую очередь, а затем отправляются фоном с экспоненциальным backoff и ретраями.
* **Настройки:**
  * `distributed_foreground_insert` (Bool, default `false`) — синхронная отправка;
  * `fsync_after_insert`, `fsync_directories` — durability файловой очереди;
  * `background_insert_*` — параметры фоновой отправки;
  * `insert_deduplication_token` — передаётся в настройках удалённой вставки.
* **Ограничения:** `Distributed` не транзакционен как целое; при сбое часть шардов может быть записана, часть — нет.  Фоновый режим без `fsync` теряет данные при power loss.
* **Файлы:**
  * `src/Storages/StorageDistributed.cpp`
  * `src/Storages/Distributed/DistributedSink.cpp`
  * `src/Storages/Distributed/DistributedAsyncInsertDirectoryQueue.cpp`

### 2.6 `INSERT INTO ... SELECT` с дедупликацией

* **Механизм:** результат `SELECT` вставляется в целевую таблицу с дедупликацией.  Если `SELECT` детерминированный, повторный retry отбрасывается.
* **Настройки:** `deduplicate_insert_select` (`enable_when_possible` / `force_enable` / `disable`).
* **Ограничения:** недетерминированный `SELECT` (например, с `rand()`) может дать другой хеш и не дедуплицироваться; ClickHouse автоматически отключает дедупликацию в таких случаях.
* **Файлы:**
  * `src/Interpreters/InterpreterInsertQuery.cpp`
  * `src/Interpreters/InsertDeduplication.cpp`

### 2.7 Материализованные представления (Materialized Views)

* **Механизм:** при `INSERT` в источник данные пушатся в цепочку MV.  Каждый chunk несёт `DeduplicationInfo`, и целевые таблицы MV дедуплицируются по source block number + view block number.
* **Настройки:**
  * `deduplicate_blocks_in_dependent_materialized_views` (Bool, default `true`);
  * `wait_for_part_commit_in_dependent_materialized_views` (Bool, default `false`);
  * `materialized_views_ignore_errors` (Bool, default `false`) — **при `true` ломает exactly-once**, потому что ошибка в MV не откатывает source;
  * `parallel_view_processing` — при опасных целевых движках (Buffer, Distributed) переводится в single-stream.
* **Ограничения:** MV, изменяющие число строк (например, `GROUP BY`), ограничивают async дедупликацию; целевые `Buffer` и `Distributed` нарушают нумерацию блоков.
* **Файлы:**
  * `src/Interpreters/InsertDependenciesBuilder.cpp`
  * `src/Processors/Transforms/DeduplicationTokenTransforms.cpp`
  * `tests/queries/0_stateless/02912_ingestion_mv_deduplication.sql`

### 2.8 Транзакции `BEGIN / COMMIT / ROLLBACK` (experimental)

* **Механизм:** ClickHouse поддерживает ACID-транзакции для `MergeTree` таблиц в `Atomic` базах данных.  `TransactionLog` в Keeper/ZooKeeper присваивает CSN (Commit Sequence Number), `VersionMetadata` на диске хранит creation/removal TID для каждой части.  При `COMMIT` все изменения становятся видны по одному CSN, при `ROLLBACK` — откатываются.
* **Настройки:**
  * `allow_experimental_transactions` в конфиге сервера;
  * `implicit_transaction` — оборачивать каждый запрос в транзакцию;
  * `transaction_log.zookeeper_path`.
* **Ограничения:**
  * только non-replicated `MergeTree` и `Atomic` database;
  * не поддерживаются `ReplicatedMergeTree`, кросс-хостовые и кросс-шардовые транзакции;
  * не охватывают внешние системы (Kafka, S3, Distributed и т.д.).
  * Если клиент не получил ответ, он не знает, была ли транзакция закоммичена; exactly-once достигается за счёт повторной попытки с дедупликацией MergeTree.
* **Файлы:**
  * `src/Interpreters/MergeTreeTransaction.h`, `src/Interpreters/MergeTreeTransaction.cpp`
  * `src/Interpreters/TransactionLog.cpp`
  * `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp`

---

## 3. Интеграционные движки и способы, которые могут дать exactly-once

### 3.1 `Kafka` engine + MV → `ReplicatedMergeTree`

* **Механизм:** классический `Kafka` engine читает сообщения через consumer group и пушит их в материализованные представления.  Offset коммитится в Kafka **после** успешной вставки.  При сбое после записи, но до коммита, сообщения будут переобработаны, но дедупликация `ReplicatedMergeTree` отфильтрует уже записанные строки, если batch детерминированен.
* **Гарантия:** базовый engine — **at-least-once**; exactly-once возможна **только при комбинации** с дедупликацией целевой таблицы и при условии, что сообщения вставляются одним и тем же детерминированным блоком.
* **Настройки:**
  * `kafka_broker_list`, `kafka_topic_list`, `kafka_group_name`, `kafka_format`;
  * `kafka_num_consumers`, `kafka_max_block_size`;
  * `kafka_commit_every_batch` (default `false`) — `true` увеличивает риск дубликатов;
  * `kafka_commit_on_select` — для отладки;
  * `kafka_skip_broken_messages`;
  * `deduplicate_blocks_in_dependent_materialized_views=true`.
* **Ограничения:** при сбое между вставкой и коммитом offset будут дубли; exactly-once не гарантирована на уровне Kafka engine.
* **Файлы:**
  * `src/Storages/Kafka/StorageKafka.cpp`
  * `src/Storages/Kafka/KafkaSource.cpp`
  * `src/Storages/Kafka/KafkaConsumer.cpp`
  * `docs/reference/engines/table-engines/integrations/kafka.mdx`

### 3.2 `Kafka2` / `StorageKafka2` (experimental, offsets in Keeper)

* **Механизм:** экспериментальный движок, в котором смещения и размер последнего батча (`intent`) хранятся в ClickHouse Keeper, а не в Kafka.  При сбое можно повторно вставить тот же batch, а дедупликация `ReplicatedMergeTree` уберёт дубли.
* **Настройки:**
  * `allow_experimental_kafka_offsets_storage_in_keeper`;
  * `kafka_keeper_path`, `kafka_replica_name`;
  * `kafka_partition_shard_num`, `kafka_shard_count`;
  * `kafka_commit_on_select`;
  * `kafka_thread_per_consumer` (обязательно при `kafka_num_consumers > 1`).
* **Ограничения:** экспериментальный; не рекомендуется для production; быстрое drop/recreate таблицы с тем же keeper path может привести к проблемам.
* **Файлы:**
  * `src/Storages/Kafka/StorageKafka2.cpp`
  * `src/Storages/Kafka/KeeperHandlingConsumer.cpp`

### 3.3 `S3Queue` / `AzureQueue` / `GCSQueue` / `ObjectStorageQueue`

* **Механизм:** очередь файлов из объектного хранилища.  Состояние обработанных файлов хранится в Keeper/ZooKeeper.  В `unordered` mode хранится множество обработанных файлов; в `ordered` — максимальное имя файла и retry-файлы.  С 25.8+ `use_persistent_processing_nodes` устраняет дубли при истечении keeper-сессии до коммита.
* **Гарантия:** по умолчанию **at-least-once**; близко к exactly-once при `deduplication_v2` / `use_persistent_processing_nodes` и дедуплицирующей целевой таблице.
* **Настройки:**
  * `mode` (`unordered` / `ordered`);
  * `after_processing` (`keep`, `delete`, `move`, `tag`);
  * `keeper_path`;
  * `tracked_files_limit`, `tracked_file_ttl_sec`;
  * `max_processed_files_before_commit`, `max_processed_rows_before_commit`, `max_processed_bytes_before_commit`, `max_processing_time_sec_before_commit`;
  * `use_persistent_processing_nodes` (25.8+), `persistent_processing_node_ttl_seconds`;
  * `deduplicate_blocks_in_dependent_materialized_views=true`.
* **Ограничения:** дубли возможны при исключении в середине файла и ретраях; `after_processing=delete` + `fsync_after_insert=0` может привести к потере строк.
* **Файлы:**
  * `src/Storages/ObjectStorageQueue/ObjectStorageQueueSource.cpp`
  * `src/Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.cpp`
  * `src/Storages/ObjectStorageQueue/StorageObjectStorageQueue.cpp`
  * `docs/reference/engines/table-engines/integrations/s3queue.mdx`

### 3.4 `RabbitMQ` engine

* **Механизм:** читает сообщения из RabbitMQ и пишет в MV.  Сообщение ack-ается после успешной вставки.  Для записи в RabbitMQ используются `publisher confirms` и `message_id`.
* **Гарантия:** **at-least-once** для чтения и записи; RabbitMQ сам по себе не гарантирует exactly-once.  Для приближения к exactly-once нужна дедуплицирующая целевая таблица + детерминированный `message_id`.
* **Настройки:**
  * `rabbitmq_host_port`, `rabbitmq_exchange_name`, `rabbitmq_format`;
  * `rabbitmq_num_consumers`, `rabbitmq_num_queues`, `rabbitmq_queue_base`;
  * `rabbitmq_persistent` — durable сообщения;
  * `rabbitmq_commit_on_select`;
  * `rabbitmq_handle_error_mode`.
* **Файлы:**
  * `src/Storages/RabbitMQ/StorageRabbitMQ.cpp`
  * `src/Storages/RabbitMQ/RabbitMQProducer.cpp`
  * `docs/reference/engines/table-engines/integrations/rabbitmq.mdx`

### 3.5 `NATS` engine (JetStream)

* **Механизм:** JetStream durable pull consumer; ack сообщения происходит только после успешной вставки в MV.
* **Гарантия:** JetStream — **at-least-once**; Core NATS — **at-most-once** (не подходит для надёжной ingestion).  Для exactly-once нужна дедупликация в целевой таблице.
* **Настройки:**
  * `nats_url`, `nats_subjects`, `nats_format`;
  * `nats_stream`, `nats_consumer_name` — JetStream;
  * `nats_commit_on_select`;
  * `nats_handle_error_mode`.
* **Файлы:**
  * `src/Storages/NATS/StorageNATS.cpp`
  * `src/Storages/NATS/NATSJetStreamConsumer.cpp`
  * `docs/reference/engines/table-engines/integrations/nats.mdx`

### 3.6 `FileLog` engine

* **Механизм:** следит за файлами в директории и читает новые строки.  Имена файлов обрабатываются «exactly once» в рамках одной сессии, но при перезапуске без сохранённого offset может быть повторная обработка.
* **Гарантия:** **at-least-once**; для exactly-once требуется дедуплицирующая целевая таблица.
* **Файлы:**
  * `src/Storages/FileLog/StorageFileLog.cpp`
  * `docs/reference/engines/table-engines/special/filelog.mdx`

### 3.7 `MaterializedPostgreSQL` (PostgreSQL CDC)

* **Механизм:** логическая репликация из PostgreSQL; LSN используется для восстановления прогресса.  Внутренние целевые таблицы — `ReplacingMergeTree(_version)` с `_sign`/`_version`/`_peerdb_is_deleted` колонками; дедупликация выполняется на чтение через `FINAL`.
* **Гарантия:** **at-least-once + дедупликация на уровне ReplacingMergeTree**; при правильном использовании `_version` даёт exactly-once по строкам.
* **Настройки:** параметры подключения PostgreSQL + слот репликации.
* **Файлы:**
  * `src/Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.cpp`
  * `docs/reference/engines/database-engines/materialized-postgresql.mdx`

### 3.8 `TimeSeries` engine + Prometheus remote-write

* **Механизм:** экспериментальный движок для приёма Prometheus remote-write.  Данные распределяются в таблицы `samples`, `tags`, `metrics`.  `tags` хранит `id` + `ReplacingMergeTree`, что даёт внутреннюю дедупликацию тегов.
* **Гарантия:** приём данных — at-least-once; внутренние таблицы дедуплицируются по `id` и версии.
* **Файлы:**
  * `src/Storages/TimeSeries/` (регистрация через `registerStorages.cpp`)

---

## 4. Managed / облачные / коннекторные способы

### 4.1 ClickPipes for Kafka

* **Механизм:** managed ingestion из Kafka.  По умолчанию **at-least-once**.  В англоязычной документации указано, что опционально доступна **exactly-once** семантика: отслеживается high-water mark и pending ranges, каждый вставляемый блок помечается детерминированным токеном дедупликации вида `topic:partition:firstOffset-lastOffset`.
* **Ограничения:** exactly-once ограничена окнами `replicated_deduplication_window` / `replicated_deduplication_window_seconds`.
* **Файлы:**
  * `docs/integrations/clickpipes/kafka/best-practices.mdx`
  * `docs/resources/changelogs/cloud/2026.mdx`

### 4.2 ClickPipes for Object Storage (S3 / GCS / Azure Blob)

* **Механизм:** one-time и continuous ingestion из объектных хранилищ с **exactly-once** семантикой.  Реализовано через временные staging-таблицы: данные сначала вставляются в staging, при успехе партиции перемещаются в целевую таблицу через `ALTER TABLE MOVE PARTITION`.
* **Файлы:**
  * `docs/integrations/clickpipes/object-storage/amazon-s3/overview.mdx`
  * `docs/integrations/clickpipes/object-storage/google-cloud-storage/overview.mdx`
  * `docs/integrations/clickpipes/object-storage/azure-blob-storage/overview.mdx`

### 4.3 ClickPipes for PostgreSQL (CDC)

* **Механизм:** CDC-репликация использует `ReplacingMergeTree(_peerdb_version)` + `_peerdb_is_deleted` для удалений.  Дедупликация выполняется на уровне запросов через `FINAL`.
* **Гарантия:** **at-least-once + ReplacingMergeTree dedup**; row-level exactly-once при правильной работе с `_version`.
* **Файлы:**
  * `docs/integrations/clickpipes/postgres/deduplication.mdx`

### 4.4 ClickPipes for Pub/Sub / Kinesis / MongoDB CDC

* **Pub/Sub:** по умолчанию **at-least-once**.  Для exactly-once рекомендуется дедупликация на стороне потребителя по виртуальной колонке `_message_id`.
* **Kinesis:** по умолчанию **at-least-once**.
* **MongoDB CDC:** Change Streams + resume tokens; **at-least-once**.
* **Файлы:**
  * `docs/integrations/clickpipes/pubsub/overview.mdx`

### 4.5 Kafka Connect Sink (clickhouse-kafka-connect)

* **Механизм:** официальный sink-коннектор `com.clickhouse.kafka.connect.ClickHouseSinkConnector`.  Режим `exactlyOnce=true` использует `KeeperMap` как хранилище состояния (offset + batch state).  Требуется `wait_for_async_insert=1` и `bufferCount=0`.
* **Гарантия:** **exactly-once** при правильной конфигурации.
* **Файлы:**
  * `docs/integrations/connectors/data-ingestion/kafka/kafka-clickhouse-connect-sink.mdx`

### 4.6 Apache Flink connector

* **Механизм:** коннектор Flink пишет в ClickHouse.  В текущих версиях документация указывает **only at-least-once**; exactly-once отслеживается в GitHub issue #106.
* **Файлы:**
  * `docs/integrations/connectors/data-ingestion/apache-flink.mdx`

### 4.7 Apache Beam

* **Механизм:** дедупликация при вставке в `ReplicatedMergeTree` или `Distributed` поверх `ReplicatedMergeTree`.  Обычная `MergeTree` без репликации может дать дубликаты при повторе.
* **Файлы:**
  * `docs/integrations/connectors/data-ingestion/etl-tools/apache-beam.mdx`

### 4.8 dbt

* **Механизм:** `allow_automatic_deduplication` для включения автоматической дедупликации Replicated-таблиц.
* **Файлы:**
  * `docs/integrations/connectors/data-ingestion/etl-tools/dbt/features-and-configurations.mdx`

---

## 5. Протоколы и интерфейсы: как клиент поставляет данные

### 5.1 HTTP / HTTPS

* **Механизм:** `POST /?query=INSERT INTO ... FORMAT ...` с телом запроса.  Поддерживаются параметры URL `async_insert`, `wait_for_async_insert`, `insert_deduplication_token`, `insert_deduplicate`, сжатие (`Content-Encoding: lz4`, `zstd`).
* **Гарантия:** exactly-once через серверную дедупликацию; сам протокол не транзакционный.
* **Файлы:**
  * `docs/concepts/features/interfaces/http.mdx`

### 5.2 Native TCP

* **Механизм:** порт 9000/9440; используется `clickhouse-client`, Go/C++/Python/Java-клиентами.  Блочная передача Native формата со сжатием.
* **Гарантия:** exactly-once через серверную дедупликацию.
* **Файлы:**
  * `docs/concepts/features/interfaces/tcp.mdx`
  * `docs/reference/interfaces/specs/NativeProtocol.mdx`
  * `docs/reference/interfaces/specs/NativeFormat.mdx`

### 5.3 gRPC

* **Механизм:** порт `grpc_port` (например, 9100).  Поддерживает запросы, INSERT, сессии, сжатие, внешние таблицы.
* **Гарантия:** exactly-once через серверную дедупликацию.
* **Файлы:**
  * `src/Server/grpc_protos/clickhouse_grpc.proto`
  * `docs/concepts/features/interfaces/grpc.mdx`

### 5.4 `clickhouse-client` / `clickhouse-local`

* **Механизм:** CLI клиент.  Поддерживает `INSERT INTO ... FROM INFILE 'file.csv' FORMAT CSV`, `INSERT INTO FUNCTION remote(...)`.
* **Гарантия:** exactly-once через серверную дедупликацию.
* **Файлы:**
  * `docs/concepts/features/interfaces/client.mdx`
  * `docs/concepts/features/tools-and-utilities/clickhouse-local.mdx`
  * `docs/reference/statements/insert-into.mdx`

### 5.5 JDBC / ODBC / драйверы

* **Механизм:** JDBC/ODBC драйверы отправляют INSERT через TCP/HTTP.  Драйвер может сам ретраить, но exactly-once достигается только серверной дедупликацией.
* **Гарантия:** depends on target table + `insert_deduplication_token`.
* **Файлы:**
  * Драйверы вне дерева ClickHouse (отдельные репозитории), но в docs есть упоминания.

---

## 6. Табличные функции и внешние источники (через `INSERT INTO ... SELECT`)

Сами по себе табличные функции **не дают exactly-once**; exactly-once возможна только при вставке результата в таблицу с дедупликацией (`ReplicatedMergeTree` или `MergeTree` с `non_replicated_deduplication_window`) и/или с `insert_deduplication_token`.

| Табличная функция / источник | Примечание |
|------------------------------|------------|
| `s3`, `s3Cluster` | чтение/запись в S3; ingestion через `INSERT INTO ... SELECT` |
| `hdfs`, `hdfsCluster` | чтение/запись в HDFS |
| `gcs`, `azureBlobStorage`, `azureBlobStorageCluster` | чтение/запись в GCS / Azure |
| `iceberg`, `icebergCluster`, `deltalake`, `deltalakeCluster`, `hudi`, `hudiCluster`, `paimon`, `paimonCluster` | read-only data lake |
| `file`, `fileCluster`, `url`, `urlCluster` | чтение/запись локальных файлов / HTTP |
| `mysql`, `postgresql`, `mongodb`, `redis`, `odbc`, `jdbc` | чтение из внешних БД |
| `remote`, `remoteSecure` | удалённый ClickHouse |
| `input` | вставка из входного потока |
| `timeSeriesData`, `timeSeriesMetrics`, `timeSeriesSamples`, `timeSeriesSelector`, `timeSeriesTags`, `prometheusQuery`, `prometheusQueryRange` | TimeSeries / Prometheus |
| `executable`, `executablePool` | вызов внешней программы |
| `merge` | объединение таблиц |

**Файлы:**

* `src/TableFunctions/registerTableFunctions.cpp`
* `src/TableFunctions/TableFunctionObjectStorage.cpp`
* `docs/reference/functions/table-functions/`

---

## 7. Движки таблиц, которые НЕ поддерживают exactly-once сами по себе

| Движок | Почему не exactly-once |
|--------|------------------------|
| `Buffer` | **Ломает exactly-once**: буферизует в RAM, меняет размеры блоков и порядок строк, теряет данные при краше.  Рекомендуется заменить на `async_insert`. |
| `File` | read/write локального файла; нет встроенной дедупликации |
| `URL` | HTTP read/write; нет встроенной дедупликации |
| `Executable` / `ExecutablePool` | вызов внешней программы; нет гарантий |
| `Redis` | key-value; нет дедупликации |
| `EmbeddedRocksDB` | key-value; нет дедупликации |
| `MySQL`, `PostgreSQL`, `SQLite`, `MongoDB`, `ODBC`, `JDBC` | external DB engine; дедупликация только на стороне ClickHouse-приёмника |
| `Hive` | read-only |
| `YTsaurus` | read/write; без встроенной exactly-once |
| `S3`, `COSN`, `OSS`, `GCS`, `AzureBlobStorage`, `HDFS` | object-storage read/write; exactly-once только через `S3Queue` / ClickPipes / staging |
| `Iceberg`, `DeltaLake`, `Hudi`, `Paimon` | read-only data lake |
| `GenerateRandom` | генерация данных |
| `Null`, `Set`, `Join`, `View`, `Alias`, `Merge`, `Remote` | не являются ingestion path |

---

## 8. Сводная таблица всех способов и гарантий

| # | Способ | Гарантия | Ключевой механизм exactly-once | Ключевые настройки |
|---|--------|----------|-------------------------------|-------------------|
| 1 | `INSERT INTO ReplicatedMergeTree` | exactly-once при retry | unified hash в Keeper | `replicated_deduplication_window`, `insert_deduplication_token` |
| 2 | `INSERT INTO MergeTree` с `non_replicated_deduplication_window` | single-node exactly-once | локальный `MergeTreeDeduplicationLog` | `non_replicated_deduplication_window` |
| 3 | `INSERT` с `insert_deduplication_token` | user-defined exactly-once | токен в unified hash | `insert_deduplication_token` |
| 4 | `async_insert` + дедупликация | exactly-once при retry | `AsynchronousInsertQueue` + unified hash | `async_insert`, `async_insert_deduplicate`, `wait_for_async_insert` |
| 5 | `INSERT INTO Distributed` → `ReplicatedMergeTree` | per-shard exactly-once | передача токена + дедупликация на шарде | `distributed_foreground_insert`, `fsync_after_insert`, `insert_deduplication_token` |
| 6 | `INSERT INTO ... SELECT` | exactly-once при retry | дедупликация результата | `deduplicate_insert_select` |
| 7 | Materialized Views | exactly-once downstream | source/view block IDs | `deduplicate_blocks_in_dependent_materialized_views` |
| 8 | Транзакции `BEGIN/COMMIT` | ACID для non-replicated MergeTree | CSN + VersionMetadata | `allow_experimental_transactions`, `implicit_transaction` |
| 9 | `Kafka` engine → MV | at-least-once (exactly-once с дедупликацией MV) | Kafka consumer group + ReplicatedMergeTree dedup | `kafka_*`, `deduplicate_blocks_in_dependent_materialized_views` |
| 10 | `Kafka2` engine | ближе к exactly-once (experimental) | Keeper offsets + intent | `kafka_keeper_path`, `kafka_replica_name` |
| 11 | `S3Queue` / `AzureQueue` / `GCSQueue` | at-least-once / near-EO | Keeper file metadata + dedup target | `mode`, `keeper_path`, `use_persistent_processing_nodes` |
| 12 | `RabbitMQ` engine | at-least-once | ack after insert + dedup target | `rabbitmq_*` |
| 13 | `NATS` JetStream | at-least-once | durable consumer + dedup target | `nats_stream`, `nats_consumer_name` |
| 14 | `FileLog` engine | at-least-once | file offset tracking | `poll_*` |
| 15 | `MaterializedPostgreSQL` | at-least-once + row dedup | PostgreSQL LSN + ReplacingMergeTree | `_version`, `_sign`, `_peerdb_is_deleted` |
| 16 | `TimeSeries` / Prometheus remote-write | internal dedup | `id` + ReplacingMergeTree | TimeSeries tables |
| 17 | ClickPipes Kafka | at-least-once / optional exactly-once | offset-based dedup token | `replicated_deduplication_window*` |
| 18 | ClickPipes Object Storage | exactly-once | staging tables + `MOVE PARTITION` | ClickPipes config |
| 19 | ClickPipes PostgreSQL CDC | at-least-once + row dedup | ReplacingMergeTree `_peerdb_version` | `_peerdb_version`, `_peerdb_is_deleted` |
| 20 | ClickPipes Pub/Sub / Kinesis / MongoDB | at-least-once | consumer-side dedup по `_message_id` | `_message_id` |
| 21 | Kafka Connect Sink | exactly-once | `KeeperMap` state store | `exactlyOnce=true`, `wait_for_async_insert=1` |
| 22 | HTTP interface | exactly-once при retry | серверная дедупликация | `insert_deduplication_token`, `async_insert`, `wait_for_async_insert` |
| 23 | Native TCP | exactly-once при retry | серверная дедупликация | `insert_deduplication_token` |
| 24 | gRPC | exactly-once при retry | серверная дедупликация | `insert_deduplication_token` |
| 25 | `clickhouse-client` / `clickhouse-local` | exactly-once при retry | серверная дедупликация | `insert_deduplication_token` |
| 26 | JDBC/ODBC/драйверы | exactly-once при retry | серверная дедупликация | `insert_deduplication_token` |
| 27 | Табличные функции + `INSERT SELECT` | exactly-once при retry | дедупликация целевой таблицы | `deduplicate_insert_select`, `insert_deduplication_token` |
| 28 | `Distributed DDL` (`ON CLUSTER`) | exactly-once для DDL | идемпотентные задачи в ZK | `distributed_ddl_task_timeout` |

---

## 9. Ключевые настройки, связанные с exactly-once

| Настройка | Уровень | Default | Назначение |
|-----------|---------|---------|------------|
| `insert_deduplicate` | session | `true` | Дедупликация `INSERT INTO` |
| `deduplicate_insert` | session | `enable` | Современный переключатель дедупликации INSERT |
| `deduplicate_insert_select` | session | `enable_when_possible` | Дедупликация `INSERT SELECT` |
| `async_insert_deduplicate` | session | `false` | Дедупликация async-вставок |
| `insert_deduplication_token` | session | `""` | Пользовательский токен |
| `deduplicate_blocks_in_dependent_materialized_views` | session | `true` | Дедупликация в MV |
| `non_replicated_deduplication_window` | table | `0` | Окно хешей для `MergeTree` |
| `replicated_deduplication_window` | table | `10000` | Число хешей для `ReplicatedMergeTree` |
| `replicated_deduplication_window_seconds` | table | `3600` | Время жизни хешей |
| `async_insert` | session | `true` (26.2) | Буферизация вставок |
| `wait_for_async_insert` | session | `true` | Ждать фактический коммит |
| `wait_for_async_insert_timeout` | session | — | Таймаут ожидания |
| `distributed_foreground_insert` | session | `false` | Синхронная отправка в Distributed |
| `fsync_after_insert` | table | `false` | fsync файлов очереди Distributed |
| `fsync_directories` | table | `false` | fsync директорий очереди Distributed |
| `insert_quorum` | session | `0` | Кворум записи |
| `insert_quorum_timeout` | session | `600000` | Таймаут кворума |
| `materialized_views_ignore_errors` | session | `false` | При `true` ломает exactly-once |
| `allow_experimental_transactions` | server | `0` | ACID-транзакции |
| `allow_experimental_kafka_offsets_storage_in_keeper` | session | `0` | Kafka2 engine |
| `use_persistent_processing_nodes` | S3Queue | `0` (до 25.8) | Persistent processing nodes |
| `kafka_commit_every_batch` | Kafka | `false` | `true` → больше риска дубликатов |
| `rabbitmq_commit_on_select` | RabbitMQ | `false` | Ручной коммит offset |
| `nats_commit_on_select` | NATS | `false` | Ручной коммит offset |

---

## 10. Практические рекомендации по сценариям

### 10.1 Прямой INSERT из приложения

* Используйте `ReplicatedMergeTree` или `MergeTree` с `non_replicated_deduplication_window`.
* Генерируйте уникальный `insert_deduplication_token` на уровне батча.
* При сетевой ошибке повторяйте вставку с тем же токеном и теми же данными.
* Не используйте `Buffer` как целевую таблицу.

### 10.2 INSERT из Kafka

* Для managed решения: ClickPipes Kafka с включённой опцией exactly-once.
* Для self-managed: Kafka Connect Sink с `exactlyOnce=true` + `KeeperMap`.
* Для встроенного engine: `Kafka2` (experimental) с Keeper offsets или `Kafka` + `ReplicatedMergeTree` + `deduplicate_blocks_in_dependent_materialized_views=true`.
* Убедитесь, что `wait_for_async_insert=1` при использовании Kafka Connect Sink exactly-once.

### 10.3 INSERT из S3 / GCS / Azure

* Для exactly-once: ClickPipes Object Storage (managed).
* Для self-managed: `S3Queue` / `AzureQueue` / `GCSQueue` с `mode='unordered'`, `deduplication_v2=true` / `use_persistent_processing_nodes=true`, `deduplicate_blocks_in_dependent_materialized_views=true` и целевой `ReplicatedMergeTree`.
* Используйте `after_processing='keep'` или `move` вместо `delete`, если критична durability.

### 10.4 PostgreSQL CDC

* Используйте `MaterializedPostgreSQL` database engine или ClickPipes Postgres.
* Запросы к целевым таблицам выполняйте с `FINAL` для дедупликации.

### 10.5 Асинхронные потоки

* `async_insert=1` + `wait_for_async_insert=1`.
* При необходимости `async_insert_deduplicate=1` (без MV, изменяющих число строк).

### 10.6 Многотабличные транзакции

* Только experimental и only non-replicated `MergeTree`.
* Не используйте в ClickHouse Cloud и с `ReplicatedMergeTree`.

---

## 11. Ограничения и антипаттерны, ломающие exactly-once

1. **Buffer engine** — fire-and-forget, не атомарен, теряет данные при краше, ломает дедупликацию.
2. **`wait_for_async_insert=0`** — клиент не ждёт коммита; данные могут быть потеряны при сбое сервера.
3. **`materialized_views_ignore_errors=true`** — ошибка в MV не откатывает source, что нарушает целостность.
4. **Окно дедупликации** — если retry произойдёт после `replicated_deduplication_window` / `_seconds`, дубль может пройти.
5. **Изменение данных в retry** — дедупликация по хешу работает только при идентичном блоке; `ORDER BY` и формат должны совпадать.
6. **`Distributed` как целое** — не транзакционен; при сбое часть шардов может быть записана, часть — нет.
7. **Транзакции** — не поддерживают `ReplicatedMergeTree`, DDL, системные таблицы и внешние системы.
8. **`kafka_commit_every_batch=true`** — увеличивает риск дубликатов при сбое.
9. **S3Queue `after_processing='delete'` + `fsync_after_insert=0`** — может привести к потере строк при power loss.
10. **Параллельная обработка MV с опасными целевыми движками** (`Buffer`, `Distributed`) — нарушает нумерацию блоков, ClickHouse переводит в single-stream.

---

## 12. Ссылки на ключевые исходные файлы

### Core deduplication

* `src/Interpreters/InsertDeduplication.h`
* `src/Interpreters/InsertDeduplication.cpp`
* `src/Processors/Transforms/DeduplicationTokenTransforms.cpp`
* `src/Interpreters/AsynchronousInsertQueue.cpp`
* `src/Interpreters/InsertDependenciesBuilder.cpp`
* `src/Core/Settings.cpp`

### MergeTree / ReplicatedMergeTree

* `src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp`
* `src/Storages/MergeTree/ReplicatedMergeTreeSink.h`
* `src/Storages/MergeTree/MergeTreeSink.cpp`
* `src/Storages/MergeTree/MergeTreeDeduplicationLog.cpp`
* `src/Storages/MergeTree/AsyncBlockIDsCache.cpp`
* `src/Storages/StorageReplicatedMergeTree.cpp`
* `src/Storages/MergeTree/MergeTreeSettings.cpp`

### Distributed

* `src/Storages/StorageDistributed.cpp`
* `src/Storages/Distributed/DistributedSink.cpp`
* `src/Storages/Distributed/DistributedAsyncInsertDirectoryQueue.cpp`

### Kafka

* `src/Storages/Kafka/StorageKafka.cpp`
* `src/Storages/Kafka/StorageKafka2.cpp`
* `src/Storages/Kafka/KafkaSource.cpp`
* `src/Storages/Kafka/KafkaConsumer.cpp`
* `src/Storages/Kafka/KafkaSettings.cpp`
* `src/Storages/Kafka/KeeperHandlingConsumer.cpp`

### RabbitMQ / NATS / FileLog / S3Queue

* `src/Storages/RabbitMQ/StorageRabbitMQ.cpp`
* `src/Storages/RabbitMQ/RabbitMQSettings.cpp`
* `src/Storages/NATS/StorageNATS.cpp`
* `src/Storages/NATS/NATSSettings.cpp`
* `src/Storages/FileLog/StorageFileLog.cpp`
* `src/Storages/FileLog/FileLogSettings.cpp`
* `src/Storages/ObjectStorageQueue/StorageObjectStorageQueue.cpp`
* `src/Storages/ObjectStorageQueue/ObjectStorageQueueSource.cpp`
* `src/Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.cpp`
* `src/Storages/ObjectStorageQueue/ObjectStorageQueueSettings.cpp`

### Transactions / PostgreSQL / TimeSeries

* `src/Interpreters/MergeTreeTransaction.cpp`
* `src/Interpreters/TransactionLog.cpp`
* `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp`
* `src/Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.cpp`
* `src/Storages/TimeSeries/`

### Документация

* `docs/ru/concepts/features/operations/insert/deduplication.mdx`
* `docs/ru/concepts/features/operations/insert/deduplicating-inserts-on-retries.mdx`
* `docs/ru/concepts/features/operations/insert/transactions.mdx`
* `docs/ru/reference/settings/session-settings/insert.mdx`
* `docs/ru/reference/settings/session-settings/materialized-views.mdx`
* `docs/ru/reference/engines/table-engines/integrations/kafka.mdx`
* `docs/ru/reference/engines/table-engines/integrations/s3queue.mdx`
* `docs/ru/integrations/clickpipes/kafka/best-practices.mdx`
* `docs/ru/integrations/clickpipes/object-storage/amazon-s3/overview.mdx`
* `docs/ru/integrations/clickpipes/postgres/deduplication.mdx`
* `docs/ru/integrations/connectors/data-ingestion/kafka/kafka-clickhouse-connect-sink.mdx`
* `docs/ru/integrations/connectors/data-ingestion/apache-flink.mdx`
* `docs/ru/integrations/connectors/data-ingestion/etl-tools/apache-beam.mdx`
* `docs/ru/integrations/connectors/data-ingestion/etl-tools/dbt/features-and-configurations.mdx`

---

## 13. Итог

В ClickHouse **нет единого “exactly-once” на всех ingestion-путях**.  Базовая гарантия — **at-least-once** плюс идемпотентная дедупликация вставляемых блоков на стороне приёмника.  Полноценная exactly-once достижима, когда выполняются одновременно:

1. Источник умеет отслеживать прогресс (offset, LSN, файл, batch ID) и повторять отправку детерминированного батча.
2. ClickHouse принимает данные в движок, который умеет дедуплицировать повторные вставки (`ReplicatedMergeTree`, `MergeTree` с `non_replicated_deduplication_window`, или managed ClickPipes staging).
3. Для каскадов из материализованных представлений включена `deduplicate_blocks_in_dependent_materialized_views=true` и не используется `materialized_views_ignore_errors=true`.
4. При необходимости используется `insert_deduplication_token` для детерминированной идентификации батча.

**Краткая матрица «что выбрать»:**

| Источник | Рекомендуемый путь к exactly-once |
|----------|-----------------------------------|
| Приложение / HTTP / TCP / gRPC | `ReplicatedMergeTree` + `insert_deduplication_token` + retry |
| Kafka | ClickPipes Kafka (EO) или Kafka Connect Sink (`exactlyOnce=true`) или `Kafka2` engine (experimental) |
| S3 / GCS / Azure | ClickPipes Object Storage (EO) или `S3Queue`/`AzureQueue`/`GCSQueue` + persistent nodes + ReplicatedMergeTree |
| PostgreSQL CDC | `MaterializedPostgreSQL` или ClickPipes Postgres |
| RabbitMQ / NATS | Engine + MV → `ReplicatedMergeTree` + дедупликация (at-least-once на транспорте) |
| Многотабличные операции | Experimental `BEGIN/COMMIT` (non-replicated MergeTree) |
| Async потоки | `async_insert=1` + `wait_for_async_insert=1` + `async_insert_deduplicate=1` (без dangerous MV) |

---

*Файл подготовлен на основе исходного кода ClickHouse и встроенной документации.*
