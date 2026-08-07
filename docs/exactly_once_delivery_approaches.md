# Exactly-once поставка данных в ClickHouse: полный каталог подходов

> Репорт описывает **все практические способы** организовать exactly-once доставку в ClickHouse — как из потоковых источников (Kafka/YDS/Pub/Sub), так и из батчевых (PostgreSQL snapshot, S3, файлы).  
> Упор сделан на паттерны, которые можно реализовать в коде поставщика (producer / ETL / pipeline), с использованием ClickHouse-фич там, где это уместно.  
> Дата: 2026-08-07.

---

## 1. Что значит «exactly-once» в контексте ClickHouse

ClickHouse сам по себе не имеет встроенного «exactly-once протокола» для внешних источников.  Базовая гарантия большинства путей — **at-least-once**: источник может повторить отправку, а ClickHouse либо отбрасывает дубли, либо их записывает.

**End-to-end exactly-once** достигается, когда одновременно выполняются три условия:

1. **Источник отслеживает прогресс** и может повторить **точно тот же детерминированный батч** (offset, LSN, file, batch ID).
2. **Приёмник идемпотентен** — повторный батч не создаёт дубликатов внутри целевой таблицы.
3. **Точка коммита в источнике сдвигается только после того, как батч гарантированно записан** в приёмнике (или может быть отфильтрован при replay).

Если выполняются только (1) и (3), но не (2) — получается **at-least-once**.  Если выполняются (1) и (2), но (3) нет — при сбое может произойти replay, но дубли будут отфильтрованы; с точки зрения конечного состояния таблицы это всё ещё exactly-once.

---

## 2. Базовые примитивы, из которых строятся решения

| Примитив | Что даёт | ClickHouse-фича / инструмент |
|----------|----------|------------------------------|
| **Offset / LSN / file ID** | Уникальная позиция в источнике | Колонки `_partition`, `_offset`, `_file`, `_lsn` в данных |
| **Водяная линия (waterline)** | Повторно не вставлять уже записанные строки | `SELECT max(_offset) FROM t WHERE _partition = ...` |
| **Idempotency key** | Идентификатор батча, по которому ClickHouse дедуплицирует | `insert_deduplication_token` |
| **Встроенная дедупликация вставок** | ClickHouse отбрасывает идентичные блоки | `ReplicatedMergeTree` / `MergeTree` + `non_replicated_deduplication_window` |
| **Staging table** | Промежуточная таблица, в которую пишем с возможностью отката | `CREATE TABLE ... ENGINE = MergeTree` |
| **Atomic MOVE PARTITION** | Перемещение готовых партиций из staging в target | `ALTER TABLE ... MOVE PARTITION ... TO TABLE ...` |
| **Version / sign** | Дедупликация/компакция на уровне строк | `ReplacingMergeTree`, `VersionedCollapsingMergeTree` |
| **Transactions** | Атомарная группа INSERT в несколько MergeTree-таблиц | `BEGIN / COMMIT / ROLLBACK` (experimental, non-replicated MergeTree) |

---

# Часть I. Потоковые источники

## 3. Подход 1: Waterline в целевой таблице (`_partition` + `_offset`)

### Идея

В целевую таблицу добавляются две служебные колонки:

```sql
CREATE TABLE events (
    -- бизнес-колонки
    id String,
    ts DateTime64(3),
    ...
    -- служебные exactly-once колонки
    _partition Int64,
    _offset Int64
) ENGINE = ReplicatedMergeTree
ORDER BY (_partition, _offset, id);
```

При старте поставщик для каждой партиции спрашивает:

```sql
SELECT max(_offset) FROM events
WHERE _partition = 0
HAVING count() > 0
SETTINGS select_sequential_consistency = 1;
```

Это значение — **waterline**.  Все сообщения с `offset <= waterline` отбрасываются, остальные вставляются.

### Алгоритм (одна партиция)

1. Читаем батч сообщений `[offset_a, offset_b]` из источника.
2. `ensure_loaded(waterline)` — загружаем `max(_offset)` из ClickHouse (с `select_sequential_consistency = 1`).
3. Если `offset_b <= waterline` — skip, не вставляем ничего.
4. Если `offset_a > waterline` — вставляем весь батч.
5. Если `offset_a <= waterline < offset_b` — вставляем только строки с `offset > waterline`.
6. После успешного INSERT обновляем waterline в памяти и **коммитим offset в источнике**.

### Почему это работает

* При сбое до commit offset: источник replay-ит с того же места, waterline загружается из ClickHouse, дубли отбрасываются.
* При сбое после commit offset: источник не replay-ит, данные уже в таблице.
* При сбое после INSERT, но до commit: источник replay-ит, waterline уже в таблице, дубли отбрасываются.

### Плюсы

* Простая реализация.
* Не требует внешних хранилищ состояния.
* Работает с любым источником, который даёт partition + offset.
* Хорошо масштабируется по партициям: каждая партиция — независимая waterline.

### Минусы

* Требует `ReplicatedMergeTree` + `select_sequential_consistency = 1`, иначе чтение waterline может не увидеть свежих вставок.
* Не работает с `Distributed` engine (данные уходят на шарды асинхронно, waterline в `Distributed` лжёт).
* Не работает с `Buffer` engine (разбивает батчи, меняет порядок).
* Если в одном батче offset идут не строго по возрастанию, нужно фильтровать построчно, а не по `max(offset)`.
* Системные колонки `_partition` / `_offset` занимают место и должны быть в `ORDER BY` или хотя бы доступны для фильтрации.
* При ручной очистке таблицы waterline сбрасывается → replay приведёт к дублям.

### Когда использовать

* Kafka / YDS / Kinesis / Pub/Sub с партициями.
* Прямой INSERT в `ReplicatedMergeTree`.
* Когда нет возможности использовать встроенную дедупликацию ClickHouse (например, данные приходят в разном порядке или батчи разбиваются).

### Пример кода (псевдокод)

```rust
struct Waterline {
    cache: HashMap<i64, i64>, // partition -> max_offset
}

impl Waterline {
    async fn ensure_loaded(&mut self, ch: &Client, partition: i64) -> Result<()> {
        if self.cache.contains_key(&partition) { return Ok(()); }
        let q = format!(
            "SELECT max(_offset) FROM events \
             WHERE _partition = {partition} HAVING count() > 0 \
             SETTINGS select_sequential_consistency = 1"
        );
        let max = ch.query_one(&q).await?.map(|row| row.get::<i64, _>(0));
        self.cache.insert(partition, max.unwrap_or(-1));
        Ok(())
    }

    fn is_committed(&self, partition: i64, offset: i64) -> bool {
        self.cache.get(&partition).map_or(false, |w| offset <= *w)
    }

    fn mark_committed(&mut self, partition: i64, offset: i64) {
        self.cache.entry(partition).and_modify(|w| *w = max(*w, offset)).or_insert(offset);
    }
}

async fn write_batch(ch: &Client, wl: &mut Waterline, msgs: &[Message]) -> Result<()> {
    let partition = msgs[0].partition;
    wl.ensure_loaded(ch, partition).await?;

    let to_insert: Vec<_> = msgs.iter()
        .filter(|m| !wl.is_committed(partition, m.offset))
        .collect();

    if !to_insert.is_empty() {
        ch.insert("events", rows_with_keys(to_insert)).await?;
        let max_offset = to_insert.iter().map(|m| m.offset).max().unwrap();
        wl.mark_committed(partition, max_offset);
    }
    source.commit(msgs.last().unwrap().offset).await?;
    Ok(())
}
```

---

## 4. Подход 2: Внешний offset store + идемпотентные вставки

### Идея

Состояние waterline хранится **не в ClickHouse**, а в отдельном хранилище: Redis, PostgreSQL, YDB, ZooKeeper, etcd, SQLite на диске.  Перед вставкой поставщик атомарно:

1. Читает последний закоммиченный offset из внешнего store.
2. Вставляет данные в ClickHouse.
3. Записывает новый offset во внешний store только после успешного INSERT.

### Алгоритм

```text
while true:
    last_committed = store.read("offset:partition:0")  // -1 если нет
    batch = source.read_from(last_committed + 1)
    if batch.empty(): continue

    // two-phase
    ch.insert(batch)                       // шаг 1
    store.cas("offset:partition:0",        // шаг 2: атомарное сравнение-и-запись
              expected = last_committed,
              new = batch.max_offset)
```

### Почему это работает

* Внешний store гарантирует, что offset сдвигается только после успешного INSERT.
* Если поставщик упал между INSERT и store update, при перезапуске он увидит старый offset и вставит данные **повторно**.  Поэтому ClickHouse-вставки должны быть идемпотентными (см. подход 3).

### Плюсы

* Можно использовать с любыми таблицами ClickHouse, включая `MergeTree` без `Replicated`.
* Внешний store может быть быстрее и надёжнее, чем `SELECT max(offset)` из большой таблицы.
* Подходит для мульти-табличных пайплайнов: один offset в store, несколько целевых таблиц.

### Минусы

* Двухфазная логика: если store update не прошёл, нужно ретраить.
* Нужно обеспечить идемпотентность вставок в ClickHouse (иначе при retry будут дубли).
* Требуется внешнее хранилище с CAS / транзакциями.
* Split-brain: несколько инстансов поставщика могут одновременно писать в одну партицию, если store не защищает от этого.

### Когда использовать

* Когда ClickHouse не `ReplicatedMergeTree` или вставки идут через `Distributed`.
* Когда нужно отслеживать прогресс по нескольким целевым таблицам одновременно.
* Когда есть уже готовый offset store (Kafka Consumer Group, Redis, YDB).

### Пример: Redis + `insert_deduplication_token`

```python
last_offset = redis.get(f"ch:offset:{partition}") or -1
batch = kafka.poll(start=last_offset + 1)
token = f"events:p{partition}:{batch.first_offset}-{batch.last_offset}"
ch.insert(batch, settings={"insert_deduplication_token": token})
redis.set(f"ch:offset:{partition}", batch.last_offset)
```

`insert_deduplication_token` делает повторную вставку с тем же токеном безопасной.

---

## 5. Подход 3: ClickHouse built-in insert deduplication + `insert_deduplication_token`

### Идея

Не хранить waterline вовсе, а полагаться на то, что ClickHouse сам отбрасывает повторные вставки по хешу блока или по пользовательскому токену.

```sql
INSERT INTO events (id, ts, ...)
SETTINGS insert_deduplication_token = 'events:p0:1000-1099'
VALUES (...);
```

При retry тот же токен и те же данные → ClickHouse вернёт `INSERT_WAS_DEDUPLICATED`, дубли не появятся.

### Алгоритм

1. Источник читает батч и генерирует token = `topic:partition:firstOffset-lastOffset`.
2. Вставляет в ClickHouse с `insert_deduplication_token = token`.
3. Коммитит offset в источнике.
4. Если коммит не прошёл, при replay вставка с тем же токеном будет дедуплицирована.

### Плюсы

* Не нужны служебные колонки `_partition` / `_offset` в таблице.
* Не нужен внешний offset store.
* Работает через `Distributed` (токен передаётся на шарды).
* Работает с `async_insert`.

### Минусы

* Токен отслеживается **по партиции**; если вставка разбивается на несколько партиций, каждая проверяется отдельно.
* Окно дедупликации ограничено (`replicated_deduplication_window`, `replicated_deduplication_window_seconds`).  Если retry случится слишком поздно, дубли пройдут.
* Для `MergeTree` без репликации нужно включать `non_replicated_deduplication_window`.
* Для `INSERT SELECT` нужно использовать `deduplicate_insert_select` и убедиться, что `SELECT` детерминированный.
* При сетевом сбое между коммитом в ClickHouse Keeper и ответом клиенту статус вставки может быть `UNKNOWN_STATUS_OF_INSERT` — клиент должен уметь retry.

### Когда использовать

* Когда источник даёт детерминированные, непересекающиеся батчи.
* Когда вставки идут напрямую в `ReplicatedMergeTree`.
* Когда не хочется добавлять системные колонки.

### Пример

```python
def token(partition, first_offset, last_offset):
    return f"events:p{partition}:{first_offset}-{last_offset}"

for batch in kafka.poll():
    ch.execute(
        "INSERT INTO events SETTINGS insert_deduplication_token = %(token)s VALUES %(rows)s",
        {"token": token(batch.partition, batch.first_offset, batch.last_offset)},
    )
    kafka.commit(batch.last_offset)
```

---

## 6. Подход 4: Kafka engine + Materialized View + `ReplicatedMergeTree` dedup

### Идея

Использовать встроенный `Kafka` engine ClickHouse:

```sql
CREATE TABLE kafka_queue (
    ...
) ENGINE = Kafka(...);

CREATE TABLE events (...) ENGINE = ReplicatedMergeTree ORDER BY ...;

CREATE MATERIALIZED VIEW mv TO events AS
SELECT *, _topic, _partition, _offset FROM kafka_queue;
```

Kafka engine коммитит offset в Kafka **после** успешной вставки в MV.  При сбое после вставки, но до коммита, Kafka replay-ит батч, но `ReplicatedMergeTree` отбросит дубли по хешу блока.

### Плюсы

* Всё внутри ClickHouse, не нужен внешний поставщик.
* Нативная интеграция с Kafka.

### Минусы

* Базовый Kafka engine — **at-least-once**; exactly-once достигается только благодаря дедупликации `ReplicatedMergeTree`.
* Если батч при replay окажется неидентичным (например, из-за перекомпакции Kafka), дубли пройдут.
* Нужно `deduplicate_blocks_in_dependent_materialized_views = true`.
* `kafka_commit_every_batch = true` увеличивает риск дубликатов.
* Не даёт контроля над waterline: если таблица была очищена, дубли будут.

### Когда использовать

* Self-managed Kafka → ClickHouse.
* Когда батчи из Kafka стабильны и детерминированы.
* Когда приемлема at-least-once семантика с дедупликацией на приёмнике.

---

## 7. Подход 5: Kafka2 engine (offsets в ClickHouse Keeper)

### Идея

Экспериментальный `Kafka2` engine хранит offset и размер последнего батча (`intent`) в ClickHouse Keeper, а не в Kafka.  При сбое он может повторить тот же батч, и дедупликация `ReplicatedMergeTree` отфильтрует дубли.

```sql
CREATE TABLE kafka_queue2 (...) ENGINE = Kafka2(
    kafka_broker_list = '...',
    kafka_topic_list = 'events',
    kafka_group_name = '...',
    kafka_keeper_path = '/kafka_queue2/...',
    kafka_replica_name = '...'
);
```

### Плюсы

* Ближе к настоящему exactly-once, чем обычный Kafka engine.
* Offset и intent хранятся в Keeper, а не в Kafka.

### Минусы

* Экспериментальный, не рекомендуется для production.
* Требует `allow_experimental_kafka_offsets_storage_in_keeper`.
* Сложная настройка шардирования и rebalance.
* Быстрое drop/recreate таблицы с тем же keeper path может сломать состояние.

### Когда использовать

* Только для экспериментов / тестов.
* Не для production в текущих версиях ClickHouse.

---

## 8. Подход 6: Kafka Connect Sink (`exactlyOnce=true`)

### Идея

Официальный sink-коннектор `com.clickhouse.kafka.connect.ClickHouseSinkConnector` поддерживает режим exactly-once.  Состояние хранится в `KeeperMap` (topic → partition → offset + batch state).

```properties
exactlyOnce=true
wait_for_async_insert=1
bufferCount=0
```

### Плюсы

* Готовое решение.
* Использует Kafka Connect экосистему (fault tolerance, task restart, scale-out).
* KeeperMap даёт распределённое состояние.

### Минусы

* Зависит от Kafka Connect и KeeperMap.
* Требует `wait_for_async_insert=1`.
* Буферизация несовместима с exactly-once.
* Необходимо настроить `replicated_deduplication_window` побольше, чтобы retry влезал в окно.

### Когда использовать

* Managed / self-managed Kafka Connect.
* Когда нужно готовое, протестированное решение.

---

## 9. Подход 7: ClickPipes for Kafka (managed)

### Идея

Managed сервис ClickHouse Cloud.  Для Kafka-источника ClickPipes использует:
* high-water mark и pending ranges;
* детерминированный `insert_deduplication_token = topic:partition:firstOffset-lastOffset`;
* `ReplicatedMergeTree` дедупликацию как fallback.

### Плюсы

* Полностью managed.
* Можно включить опцию exactly-once.
* Не нужно писать код.

### Минусы

* Только в ClickHouse Cloud.
* Exactly-once ограничено окном дедупликации ClickHouse.
* Дороже, чем self-managed.

### Когда использовать

* ClickHouse Cloud + Kafka.
* Когда нет ресурсов поддерживать self-managed exactly-once.

---

## 10. Подход 8: S3Queue / YDS / ObjectStorageQueue

### Идея

Для объектных хранилищ и очередных источников ClickHouse предоставляет `S3Queue`, `AzureQueue`, `GCSQueue`, `ObjectStorageQueue`.  Состояние обработанных файлов хранится в Keeper:

* `unordered` mode — множество обработанных файлов.
* `ordered` mode — максимальное имя файла и retry-файлы.

```sql
CREATE TABLE s3_queue (...) ENGINE = S3Queue(
    mode = 'unordered',
    after_processing = 'delete',
    keeper_path = '/s3queue/...'
);
```

### Плюсы

* Нативная интеграция с S3/GCS/Azure.
* Встроенная отслеживание файлов в Keeper.
* С 25.8+ `use_persistent_processing_nodes` устраняет дубли при истечении keeper-сессии до коммита.

### Минусы

* По умолчанию at-least-once; exactly-once требует комбинации с дедупликацией целевой таблицы.
* Дубли возможны при исключении в середине файла и ретраях.
* `after_processing = delete` + `fsync_after_insert = 0` может привести к потере строк.
* Ordered mode с несколькими серверами имеет ограничения по retry.

### Когда использовать

* S3 / GCS / Azure → ClickHouse.
* One-time или continuous ingestion файлов.

---

# Часть II. Батчевые источники

## 11. Подход 9: Staging tables + `ALTER TABLE MOVE PARTITION`

### Идея

Данные сначала загружаются в промежуточную (staging) таблицу, идентичную целевой.  После успешной загрузки готовые партиции атомарно перемещаются в целевую таблицу.

```sql
-- staging
CREATE TABLE events_staging (...) ENGINE = ReplicatedMergeTree ORDER BY ...;
-- target
CREATE TABLE events        (...) ENGINE = ReplicatedMergeTree ORDER BY ...;

INSERT INTO events_staging ...;
-- после проверки
ALTER TABLE events_staging MOVE PARTITION '2024-01' TO TABLE events;
```

### Алгоритм

1. Делим снапшот на партиции (по дате, региону и т.д.).
2. Загружаем партицию в `events_staging`.
3. Проверяем контрольные суммы / количество строк.
4. `MOVE PARTITION` в `events`.
5. Если сеть падает на шаге 2 — при повторе staging перезаписывается/дедуплицируется.
6. Если сеть падает после шага 3, но до шага 4 — повторяем `MOVE PARTITION` (он идемпотентен).

### Почему это работает

* `MOVE PARTITION` — атомарная операция в ClickHouse.
* Staging table изолирована от читателей target.
* При сбое можно просто заново загрузить партицию в staging.

### Плюсы

* Настоящая атомарность для батчевых загрузок.
* Подходит для больших снапшотов.
* Читатели target не видят частично загруженных данных.

### Минусы

* Требует места на диске под staging.
* `MOVE PARTITION` работает только внутри одного сервера/реплики для нереплицированных таблиц; для `ReplicatedMergeTree` синхронизируется через Keeper.
* Не подходит для потоковых данных ( staging будет расти).
* Нужно уметь разбивать снапшот на партиции.

### Когда использовать

* PostgreSQL / MySQL snapshot → ClickHouse.
* S3 → ClickHouse (one-time bulk load).
* ETL, который грузит данные по дням/партициям.

### Пример: PostgreSQL → ClickHouse staging

```python
for partition in pg.get_partitions('events', by='month'):
    rows = pg.export(f"SELECT * FROM events WHERE month = '{partition}'")
    ch.execute("INSERT INTO events_staging FORMAT CSV", rows)
    ch.execute(f"ALTER TABLE events_staging MOVE PARTITION '{partition}' TO TABLE events")
    state.save(f"pg_snapshot:{partition}")
```

---

## 12. Подход 10: Deterministic batch IDs + waterline

### Идея

Для батчевых источников, не имеющих offset, генерируем **детерминированный batch ID** и храним его в целевой таблице или внешнем store.  Повторный батч с тем же ID не вставляется.

Batch ID может быть:
* `table_name:partition:chunk_number` (для PG snapshot);
* `file_name:byte_range` (для S3);
* `txid:chunk_number` (для PostgreSQL по `xmin`);
* `lsn:chunk_number` (для WAL).

```sql
CREATE TABLE events (
    ...,
    _batch_id String
) ENGINE = ReplicatedMergeTree ORDER BY ...;
```

### Алгоритм

```text
for chunk in snapshot.chunks():
    batch_id = f"pg.events.2024-01.chunk{chunk.index}"
    if ch.exists("SELECT 1 FROM events WHERE _batch_id = ?", batch_id):
        continue
    ch.insert(chunk.rows, _batch_id=batch_id)
```

### Плюсы

* Простая идемпотентность на уровне батча.
* Не нужен внешний offset store (если batch ID хранится в ClickHouse).
* Хорошо работает для файлов и снапшотов.

### Минусы

* Нужна служебная колонка `_batch_id`.
* Проверка `exists` делает лишний запрос на каждый батч.
* Если батч был частично вставлен, повторная вставка даст дубли внутри батча (если нет дополнительной дедупликации по строкам).

### Когда использовать

* S3 / файлы / PostgreSQL snapshot, разбитый на chunks.
* Когда батчи чётко разделены и не пересекаются.

### Усиление с `insert_deduplication_token`

Можно не хранить `_batch_id` в таблице, а использовать `insert_deduplication_token = batch_id`.  Тогда повторная вставка с тем же batch ID будет отброшена ClickHouse, но только в пределах окна дедупликации.

---

## 13. Подход 11: Snapshot + CDC с PostgreSQL / MySQL

### Идея

Для PostgreSQL:
* Сначала делаем снапшот через `SELECT *` с определённого LSN / `xmin`.
* Затем переключаемся на logical decoding (`pgoutput` / `wal2json`) и применяем изменения.
* Каждая строка снапшота и каждое CDC-событие помечаются `lsn` + `transaction_id`.
* В ClickHouse используется `ReplacingMergeTree(_version)` с колонкой `_sign` / `_is_deleted`.

```sql
CREATE TABLE events (
    id String,
    ...,
    _version UInt64,
    _is_deleted UInt8
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);
```

### Алгоритм

1. Запоминаем стартовый LSN: `SELECT pg_current_wal_lsn()`.
2. Грузим снапшот до этого LSN (каждая строка получает `_version = 0`, `_is_deleted = 0`).
3. Начинаем читать CDC с этого LSN.
4. Каждое CDC-событие: `INSERT`/`UPDATE` → `_version = lsn`, `_is_deleted = 0`; `DELETE` → `_version = lsn`, `_is_deleted = 1`.
5. При replay дублирующиеся события с тем же `lsn` компактируются `ReplacingMergeTree`.
6. Запросы выполняются с `FINAL` или с `GROUP BY` + `argMax(..., _version)`.

### Плюсы

* Естественный CDC-путь.
* `ReplacingMergeTree` отлично подходит для дедупликации по версии.
* Можно использовать `MaterializedPostgreSQL` database engine в ClickHouse.

### Минусы

* `ReplacingMergeTree` даёт финальное состояние только при `SELECT ... FINAL` или после `OPTIMIZE`; до этого могут быть дубли.
* Нужно правильно обрабатывать `DELETE`.
* Snapshot может быть большим; нужно разбивать на chunks с batch ID.
* При failover поставщика нужно восстановить LSN из внешнего store или из ClickHouse.

### Когда использовать

* PostgreSQL / MySQL → ClickHouse в реальном времени.
* Когда нужны не только вставки, но и обновления/удаления.

### Пример: `MaterializedPostgreSQL`

```sql
CREATE DATABASE pg_replica
ENGINE = MaterializedPostgreSQL(
    'host:port', 'database', 'user', 'password'
)
SETTINGS materialized_postgresql_tables_list = 'events';
```

ClickHouse сам делает snapshot и CDC, внутренние таблицы — `ReplacingMergeTree(_version)`.

---

## 14. Подход 12: Файловый waterline (S3 / файлы)

### Идея

Для файловых источников храним множество уже обработанных имён файлов.  Новый файл обрабатываем только если его имя выше waterline.

* **Unordered mode:** `HashSet<String>` обработанных файлов.
* **Ordered mode:** `max(file_name)` и список retry-файлов.

### Реализация через ClickHouse

Можно хранить обработанные файлы в отдельной таблице:

```sql
CREATE TABLE processed_files (
    file_name String,
    processed_at DateTime64(3)
) ENGINE = ReplicatedMergeTree ORDER BY (file_name);
```

Перед обработкой файла:

```sql
SELECT count() FROM processed_files WHERE file_name = 's3://bucket/path/file.json'
```

После успешной загрузки:

```sql
INSERT INTO processed_files VALUES ('s3://bucket/path/file.json', now());
```

### Плюсы

* Просто для файлов.
* Можно комбинировать с `S3Queue` engine.

### Минусы

* `processed_files` может разрастаться; нужен TTL.
* Не атомарно: проверка и INSERT в `processed_files` — два запроса.  Можно получить дубли при race condition.
* Если файл обработан частично (ошибка в середине), повторная обработка может дать дубли внутри файла.

### Усиление

* Использовать `S3Queue` с `use_persistent_processing_nodes = true` — ClickHouse сам делает атомарное отслеживание файлов в Keeper.
* Для ordered mode: `max(file_name)` в `processed_files` + чтение только файлов с именем > max.

---

## 15. Подход 13: Внешний checkpoint store (YDB / Redis / ZooKeeper / PostgreSQL)

### Идея

Checkpoint — это позиция в источнике, которую можно сохранить и восстановить.  Для батчевых источников checkpoint может быть:
* последний обработанный primary key (`id > last_id`);
* последний обработанный LSN;
* последний обработанный файл/offset;
* последняя обработанная партиция.

```text
checkpoint = store.read("pg_snapshot:events")
rows = pg.execute("SELECT * FROM events WHERE id > %s ORDER BY id LIMIT 10000", checkpoint)
ch.insert(rows)
store.write("pg_snapshot:events", rows.max_id)
```

### Плюсы

* Универсально: работает с любыми источниками.
* Checkpoint store может быть общим для нескольких целевых таблиц.
* Подходит для long-running snapshot loads.

### Минусы

* Нужно обеспечить идемпотентность вставок в ClickHouse (иначе retry даст дубли).
* Checkpoint и INSERT — две операции; между ними может произойти сбой.
* Требуется надёжное внешнее хранилище.

### Усиление с ClickHouse

* Использовать `insert_deduplication_token = f"pg_snapshot:events:{checkpoint}"` для каждого батча.
* Или использовать `ReplacingMergeTree` с `_version = checkpoint`.

---

# Часть III. Гибридные и application-level подходы

## 16. Подход 14: Transactional Outbox

### Идея

Вместо прямой отправки события в ClickHouse приложение сначала пишет его в **outbox таблицу** в своей транзакционной БД (PostgreSQL, MySQL) в рамках бизнес-транзакции.  Отдельный ретранслятор читает outbox и отправляет события в ClickHouse.

```sql
-- в PostgreSQL
BEGIN;
  INSERT INTO orders ...;
  INSERT INTO outbox (topic, payload, created_at) VALUES ('orders', ..., now());
COMMIT;
```

Ретранслятор:

```python
for row in pg.outbox.unsent():
    ch.insert(row.payload)
    pg.outbox.mark_sent(row.id)
```

### Плюсы

* Атомарность бизнес-операции и события в исходной БД.
* Outbox гарантирует at-least-once доставку; ClickHouse дедуплицирует по idempotency key.
* Устойчив к сбоям ретранслятора.

### Минусы

* Дополнительная БД и ретранслятор.
* Нужна идемпотентность в ClickHouse (outbox row может быть отправлен повторно).
* Задержка между бизнес-транзакцией и появлением в ClickHouse.

### Когда использовать

* OLTP → OLAP (orders, payments, events).
* Когда важна атомарность с бизнес-операцией.

---

## 17. Подход 15: Application-level idempotency keys

### Идея

Приложение само генерирует уникальный idempotency key для каждой операции и записывает его в ClickHouse.  При retry повторная вставка с тем же key либо отбрасывается ClickHouse (`insert_deduplication_token`), либо фильтруется приложением.

```sql
CREATE TABLE events (
    request_id String,  -- idempotency key
    ...
) ENGINE = ReplicatedMergeTree ORDER BY (request_id, ...);
```

### Плюсы

* Максимально простой контроль.
* Работает с HTTP/TCP/gRPC клиентами.

### Минусы

* Приложение отвечает за генерацию уникальных ключей.
* Окно дедупликации ClickHouse ограничено.

### Когда использовать

* API → ClickHouse, event ingestion, user actions.

---

## 18. Подход 16: Two-phase commit / Saga

### Идея

Для сложных цепочек (например, PG → ClickHouse + отправка email) использовать распределённую транзакцию или saga:

1. Подготовить данные в ClickHouse (staging).
2. Подготовить side effect (email).
3. Коммитить все шаги.
4. Если один шаг не прошёл — компенсирующие действия.

ClickHouse не поддерживает XA, но можно использовать staging + MOVE PARTITION как «подготовка».

### Плюсы

* Консистентность между несколькими системами.

### Минусы

* Сложность.
* ClickHouse не поддерживает XA / 2PC natively.

### Когда использовать

* Когда данные в ClickHouse должны быть согласованы с внешними системами.

---

## 19. Подход 17: `ReplacingMergeTree` / `VersionedCollapsingMergeTree` для дедупликации строк

### Идея

Вместо предотвращения дубликатов на ingestion, позволяем им появляться, но компактируем их при чтении.

```sql
CREATE TABLE events (
    id String,
    ...,
    _version UInt64,
    _sign Int8 DEFAULT 1
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (id);
```

Каждое событие несёт версию (offset / LSN / timestamp).  При повторе вставки та же строка с той же версией или более новой версией заменит старую.

### Плюсы

* Работает с любыми источниками, даже если дубли неизбежны.
* Подходит для CDC с обновлениями/удалениями.

### Минусы

* `SELECT ... FINAL` медленный на больших таблицах.
* До `OPTIMIZE` дубли могут занимать место.
* Нужно правильно выбирать версию.

### Когда использовать

* CDC, медленно меняющиеся данные.
* Когда ingestion гарантирует at-least-once, а финальное состояние должно быть exactly-once.

---

## 20. Подход 18: `async_insert` + `insert_deduplication_token`

### Идея

Сервер ClickHouse буферизует мелкие вставки и сбрасывает их фоном.  Каждая вставка несёт `insert_deduplication_token`.  Повторная вставка с тем же токеном дедуплицируется.

```sql
SET async_insert = 1, wait_for_async_insert = 1;
INSERT INTO events SETTINGS insert_deduplication_token = 'batch-123' VALUES ...;
```

### Плюсы

* Высокая пропускная способность для потока мелких событий.
* Дедупликация работает на уровне сервера.

### Минусы

* `async_insert_deduplicate` требует осторожности с materialized views.
* `wait_for_async_insert = 0` теряет durability.
* Окно дедупликации ограничено.

### Когда использовать

* Высокочастотный ingestion через HTTP/TCP.
* Когда клиенты сами могут повторять вставки.

---

# Часть IV. Сравнение и рекомендации

## 21. Сводная таблица подходов

| # | Подход | Тип источника | Состояние | ClickHouse-фича | Exactly-once в конечном состоянии | Сложность |
|---|--------|---------------|-----------|-----------------|-----------------------------------|-----------|
| 1 | Waterline в таблице | потоковый | в ClickHouse | `select_sequential_consistency`, `ReplicatedMergeTree` | Да | Низкая |
| 2 | Внешний offset store | потоковый / батчевый | внешнее | `insert_deduplication_token` | Да | Средняя |
| 3 | Built-in dedup + token | потоковый / батчевый | ClickHouse | `insert_deduplication_token`, `ReplicatedMergeTree` | Да (в окне) | Низкая |
| 4 | Kafka engine + MV | потоковый | Kafka offsets | `ReplicatedMergeTree` dedup | Частично | Низкая |
| 5 | Kafka2 engine | потоковый | Keeper offsets | `ReplicatedMergeTree` dedup | Ближе к да | Высокая |
| 6 | Kafka Connect Sink | потоковый | KeeperMap | `insert_deduplication_token`, `async_insert` | Да | Средняя |
| 7 | ClickPipes Kafka | потоковый | managed | `insert_deduplication_token` | Да (managed) | Низкая |
| 8 | S3Queue / ObjectStorageQueue | файловый / потоковый | Keeper | `ReplicatedMergeTree` dedup | Частично | Средняя |
| 9 | Staging + MOVE PARTITION | батчевый | staging table | `ALTER TABLE MOVE PARTITION` | Да | Средняя |
| 10 | Deterministic batch IDs | батчевый | в таблице / token | `insert_deduplication_token` | Да | Низкая |
| 11 | Snapshot + CDC | батчевый + потоковый | LSN / xmin | `ReplacingMergeTree` | Да | Высокая |
| 12 | Файловый waterline | файловый | в таблице / Keeper | `S3Queue`, `ReplicatedMergeTree` | Частично | Средняя |
| 13 | Внешний checkpoint | батчевый | внешнее | `insert_deduplication_token` | Да | Средняя |
| 14 | Transactional Outbox | application-level | PostgreSQL | `insert_deduplication_token` | Да | Высокая |
| 15 | App idempotency keys | application-level | приложение | `insert_deduplication_token` | Да | Низкая |
| 16 | 2PC / Saga | application-level | координатор | staging + MOVE PARTITION | Да | Очень высокая |
| 17 | ReplacingMergeTree dedup | любой | ClickHouse | `ReplacingMergeTree` | Да (при чтении FINAL) | Средняя |
| 18 | async_insert + token | потоковый | ClickHouse | `async_insert`, `insert_deduplication_token` | Да | Низкая |

## 22. Рекомендации по сценариям

| Сценарий | Рекомендуемый подход | Почему |
|----------|----------------------|--------|
| **Kafka → ClickHouse, self-managed, одна партиция** | Подход 1 (waterline `_partition`/`_offset`) или Подход 3 (`insert_deduplication_token`) | Просто, контроль, работает с `ReplicatedMergeTree`. |
| **Kafka → ClickHouse, high-throughput** | Подход 6 (Kafka Connect Sink) или Подход 7 (ClickPipes) | Готовое масштабируемое решение. |
| **YDS / YDB Topic → ClickHouse** | Подход 1 (waterline) или Подход 2 (внешний offset store) | Нативный ClickHouse Kafka/YDS engine пока нет; пишем свой поставщик. |
| **PostgreSQL snapshot → ClickHouse** | Подход 9 (staging + MOVE PARTITION) или Подход 10 (batch IDs) | Атомарность батчевой загрузки. |
| **PostgreSQL CDC → ClickHouse** | Подход 11 (Snapshot + CDC) или `MaterializedPostgreSQL` | Обновления/удаления + дедупликация по версии. |
| **S3 files → ClickHouse** | Подход 8 (S3Queue) или Подход 10 (batch IDs) + Подход 12 (файловый waterline) | S3Queue делает большую часть работы. |
| **Application events → ClickHouse** | Подход 14 (Outbox) или Подход 15 (idempotency keys) | Атомарность с бизнес-операцией. |
| **ClickHouse Cloud** | Подход 7 (ClickPipes) | Managed exactly-once. |
| **Нужны обновления и удаления в ClickHouse** | Подход 11 + `ReplacingMergeTree` | Версионирование строк. |
| **Нет ReplicatedMergeTree, только один MergeTree** | Подход 2 (внешний offset store) + `insert_deduplication_token` | Встроенная дедупликация локальна. |

---

## 23. Пример: полный потоковый поставщик с waterline (одна партиция)

```python
import clickhouse_driver
import kafka

CH_TABLE = "events"
PARTITION = 0

ch = clickhouse_driver.Client("localhost")
consumer = kafka.KafkaConsumer(
    "events",
    partition_assignment_strategy=[kafka.RangePartitionAssignor],
    enable_auto_commit=False,
)
tp = kafka.TopicPartition("events", PARTITION)
consumer.assign([tp])

# 1. Загружаем waterline
waterline = ch.execute(
    f"SELECT max(_offset) FROM {CH_TABLE} WHERE _partition = {PARTITION} "
    "HAVING count() > 0 SETTINGS select_sequential_consistency = 1"
)
waterline = waterline[0][0] if waterline else -1

# 2. Начинаем читать с waterline + 1
consumer.seek(tp, waterline + 1)

buffer = []
for msg in consumer:
    if msg.offset <= waterline:
        continue
    buffer.append((msg.key, msg.value, msg.timestamp, PARTITION, msg.offset))
    if len(buffer) >= 1000:
        flush(buffer)
        buffer.clear()


def flush(rows):
    if not rows:
        return
    # max offset в батче
    max_offset = max(r[4] for r in rows)
    # вставляем сразу с системными колонками
    ch.execute(
        f"INSERT INTO {CH_TABLE} (key, value, ts, _partition, _offset) VALUES",
        rows,
    )
    consumer.commit_async()
    # локальная waterline необязательна, но полезна
    global waterline
    waterline = max(waterline, max_offset)
```

---

## 24. Пример: батчевая загрузка PostgreSQL с batch ID

```python
import psycopg2
import clickhouse_driver

ch = clickhouse_driver.Client("localhost")
pg = psycopg2.connect("...")

cursor = pg.cursor("server_side_cursor")
cursor.execute("SELECT * FROM events ORDER BY id")

chunk_idx = 0
while True:
    rows = cursor.fetchmany(10000)
    if not rows:
        break
    batch_id = f"pg.events.snapshot.chunk{chunk_idx}"

    # idempotency: ClickHouse сам отбросит дубль
    ch.execute(
        f"INSERT INTO events SETTINGS insert_deduplication_token = %(token)s VALUES",
        {"token": batch_id},
        rows,
    )
    chunk_idx += 1
```

---

## 25. Антипаттерны, ломающие exactly-once

| Антипаттерн | Почему ломает |
|-------------|---------------|
| `Buffer` engine | Меняет батчи, теряет данные при краше, ломает waterline. |
| `Distributed` engine для waterline | Данные уходят на шарды асинхронно, `SELECT max(offset)` врёт. |
| `wait_for_async_insert = 0` | Insert считается успешным до фактического коммита. |
| `MergeTree` + `select_sequential_consistency` | Setting игнорируется для не-реплицированных таблиц. |
| Fire-and-forget source commit | Offset может не зафиксироваться, но это скорее at-least-once, а не data loss. |
| Ручное удаление целевой таблицы | Waterline / token state сбрасывается → дубли при replay. |
| Детерминированный batch ID с перекрывающимися батчами | Дубли внутри батча. |
| Игнорирование `materialized_views_ignore_errors` | Ошибка в MV не откатывает source, нарушает целостность. |

---

## 26. Итоговая ментальная модель

```text
exactly-once = (source progress tracking) + (idempotent destination) + (commit after persistence)
```

Для **потоковых** источников проще всего использовать **водяную линию по `_partition`/`_offset`** внутри целевой `ReplicatedMergeTree` таблицы или **внешний offset store + `insert_deduplication_token`**.

Для **батчевых** источников оптимальны **staging tables + `MOVE PARTITION`** (для больших снапшотов) или **детерминированные batch IDs** (для файлов / chunks).

Для **CDC** лучше всего подходит `ReplacingMergeTree(_version)` + LSN/offset как версия, либо готовый `MaterializedPostgreSQL` engine.

Если нужен managed вариант — **ClickPipes** (Kafka / S3 / PostgreSQL CDC).

---

*Файл подготовлен как продолжение анализа `/Users/timmyb32r/cursor/ai/005_rust/docs/clickhouse_exactly_once_ingestion.md`, который фокусируется на встроенных возможностях ClickHouse.*
