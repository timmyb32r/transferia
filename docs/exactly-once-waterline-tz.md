# Exactly-Once → CH: In-Memory Waterline + Composite Exactly-Once Key

> **Status: unimplemented design proposal.** The current PQv1 → ClickHouse
> runtime is at-least-once and contains no waterline or composite exactly-once
> key. See `pqv1-clickhouse-delivery.md` for the active contract. The material
> below is retained only as design history and must not be used as an operator
> guide.

## Техническое задание для реализации

> Версия 7. Шестой раунд ревью (критик + архитектор). Основные изменения против v6:
> — I5 переформулирован: атомарная единица — CH-**парт**, не native-блок; граница парта
>   совпадает с группой `(partition, offset)` **только** если `PARTITION BY` таблицы зависит
>   исключительно от `__system_partition` (или `tuple()`). Условие вынесено в §13;
> — durability `insert_many`: `Ok` возвращается только после `EndOfStream` сервера → парт
>   виден последующему `SELECT` на том же хосте немедленно (проверено по clickhouse-arrow 0.2.1);
> — B2: явно проговорено, что дедуп идёт против **in-memory** waterline (грузится разово), а
>   не против live-CH-проверки на каждую запись → single-writer — несущий инвариант;
> — B3: добавлен раздел «Rejected alternatives» (почему waterline, а не native CH-dedup);
> — retention/TTL/DROP/TRUNCATE внутри CH — **вне контракта** пайплайна (H5);
> — Replicated: чтение/запись только с pin к конкретному хосту; балансировщик — вне гарантий (H4);
> — S3: bounded-LRU держит только файл(ы) в работе, а не историю всех ключей (H6);
> — M-фиксы: `partition == None` → fatal; `mark_committed` чистит `loaded_also_empty` в коде;
>   Distributed → однозначно деградация до AT_LEAST_ONCE (не FATAL); S3 row number = физическая
>   строка (не зависит от DLQ-роутинга); `DEFAULT -1` убран из рекомендаций.
>
> Версия 6. Пятый раунд ревью: 34 дыры из gap analysis (критик + архитектор + автор).
> Основные изменения против v5:
> — P3 переписан под CH Native протокол (не Arrow IPC stream);
> — S3: `__system_filename` = полный S3-ключ, файлы не удаляются процессом;
> — I1 расширен: детерминизм маршрутизации main/DLQ между запусками;
> — I2 переформулирован: waterline продвигается только до офсетов, доказуемо присутствующих в CH;
> — DDL: авто-вывод движка и схемы, проверка версии CH ≥ 22.8, числа реплик ≥ insert_quorum;
> — DLQ: динамическая схема из ExactlyOnceKey;
> — ensure_loaded: negative caching для пустых партиций, backtick для `{p}`;
> — writer-loop: ошибка синка пробрасывается как Err (не глотается);
> — source.commit(): до 10 ретраев → fatal;
> — S3 row number: описаны NewLine и NoSplit, всегда перечитывать с 0;
> — graceful shutdown (SIGTERM) в дополнение к graceful drain по ошибке.

---

## 0. Терминология и инварианты (читать первым)

**Ключ уникальности (uniqueness key)** — пара колонок в данных, идентифицирующая позицию
записи в источнике:
- `partition` — «пространство офсетов» (YDS-партиция `Int64`; S3 — полный ключ объекта `Utf8`);
- `offset` — монотонный офсет внутри партиции (`Int64`).

**Waterline** — in-memory кеш «максимальный **уже записанный** offset на партицию в таблице»,
`HashMap<WaterlineKey, i64>`, где `WaterlineKey = (Arc<str> /*table*/, PartitionKey)`.
Загружается лениво из ClickHouse. **Принадлежит синку, а синк — партиции** (YDS: свой синк
на каждую партицию; S3: один синк на пайплайн с много-ключевым LRU). Общего состояния между
партициями нет → лок не нужен (§4.1).

**Дедуп идёт против in-memory waterline, а не против live-CH на каждую запись.** `ensure_loaded`
ходит в CH **разово** на партицию (при первой встрече), кладёт `max` в память; дальше синк
фильтрует против **памяти** и инкрементит waterline локально (`mark_committed`) без повторных
запросов в CH. Следствие: **single-writer — несущий инвариант (I4/§4.1), а не оптимизация.** Если
партицию одновременно пишут два процесса, их in-memory waterline не видят записей друг друга
(каждый закешировал свой `max` разово) → дубли/потеря. Будь дедуп live-проверкой CH на каждый
offset — гонка check-and-insert всё равно осталась бы; дизайн специально серийный
пер-партиционный, чтобы атомарная check-and-insert была не нужна. Гарантия «один процесс на
партицию» обеспечивается деплоем (§13), а не кодом.

Инварианты, на которых держится корректность (нарушение любого = баг):

- **I1. Стабильность ключа и маршрутизации.** Один и тот же логический source-record при реплее
  получает **тот же** `(partition, offset)` и **ту же маршрутизацию** (main vs DLQ).
  Офсеты — **стабильные логические**: серверные (YDS/Kafka) или производные от иммутабельных
  свойств источника (S3: row number при неизменном файле). Синтетических (счётчик на стороне
  читателя без привязки к данным) офсетов не бывает — если источник не может дать стабильный
  offset, он не выставляет ключ и работает в at-least-once. **Детерминизм парсера между
  запусками:** изменение конфига/кода парсера между первым проходом и реплеем может изменить
  маршрутизацию offset'а (main↔DLQ) → потеря данных. Ответственность оператора; защита —
  отпечаток конфига парсера при рестарте (§13).
- **I2. Waterline продвигается только до офсетов, доказуемо присутствующих в CH.**
  `mark_committed` вызывается после успешного INSERT — waterline покрывает только что
  вставленные офсеты. В ветке 5.b.3 waterline может быть продвинут до исходного `max_off`
  группы даже если часть строк отфильтрована — но эти строки гарантированно уже в CH
  (иначе waterline не был бы ≥ их offset). Никакого продвижения «наперёд».
- **I3. Commit источника — только после полного успеха флеша.** Офсет в источнике коммитится
  строго после того, как **все** таблицы флеша (main + DLQ) записаны. **Механизм:**
  writer-таск для каждого батча формирует `TableWrite` для main и (если есть) DLQ,
  вызывает `sink.write(main)` затем `sink.write(dlq)`, и **только после успеха обоих**
  дёргает `source.commit()`. Отдельный коммит после каждого `TableWrite` — ошибка
  (при падении между ними потеряются строки незакоммиченной таблицы, см. §7).
  **`source.commit()` при ошибке ретраится до 10 раз; исчерпание ретраев → poison → fatal**
  (неограниченный рост незакоммиченного окна недопустим).
- **I4. Сериализация и монотонность записи в пределах партиции.** Записи одной партиции
  сериализованы (следующий флеш не стартует до завершения предыдущего) и коммитятся строго
  по возрастанию offset — никакой старший offset не поднимается в waterline раньше, чем
  закоммичен младший. Waterline — скаляр (`max`), и опирается на это: конкурентная или
  out-of-order запись одной партиции = **потеря данных** (младшие offset'ы отфильтруются как
  «дубликаты», хотя не записаны). Сегодня обеспечивается моделью «одна партиция = один
  writer-таск = свой синк» (§4.1) и серийным writer'ом. Синки с out-of-order fan-out
  (напр. `ParallelChInsertSink`) с exactly-once **несовместимы** (§6.2).
- **I5. Атомарность группы `(partition, offset)`.** Все строки одной группы
  `(partition, offset)` пишутся в пределах **одного атомарного CH-парта**; атомарная единица
  вставки никогда не дробит группу. **Атомарная единица в ClickHouse — это парт, а не
  native-блок.** Один INSERT атомарен ⟺ создаёт ровно один парт; парт создаётся по одному на
  каждую затронутую **партицию таблицы** (`PARTITION BY`). Поэтому I5 держится на двух условиях:
  - **(a) весь Message — в одном native-блоке** (offset пер-Message P1 + весь Message в одном
    `RecordBatch` P2 + клиент не чанкует P3), и
  - **(b) все строки блока попадают в одну CH-партицию** — иначе сервер расщепит блок на
    несколько партов, коммитящихся независимо, и группа `(partition, offset)` фрагментируется.
    Условие (b) выполняется ⟺ `PARTITION BY` таблицы зависит **только** от `__system_partition`
    (или `tuple()`), т.к. `__system_partition` константен на батч (§3.1). `PARTITION BY` по
    колонкам данных (напр. `toYYYYMMDD(ts)`) разбросает строки одного Message по CH-партициям
    и нарушит I5 — **ограничение вынесено в §13 как ответственность оператора** (автосоздаваемые
    таблицы `PARTITION BY` не задают → `tuple()` → безопасны by default).

  Без I5 частичный сбой `insert_many` мог бы записать часть строк одного offset, поднять
  waterline до него и потерять остаток на реплее (§6.1).

  **Почему (a) выполняется (P1/P2/P3):** клиент `clickhouse-arrow 0.2.1` использует **CH Native
  протокол** (TCP, порт 9000), а не Arrow IPC Stream. `ArrowFormat::FORMAT = "Arrow"` — это
  native-блоки ClickHouse: каждый `RecordBatch` сериализуется в ровно один native-блок
  (`client/internal.rs:send_insert` → один `send_data` на батч). Клиент **не чанкует**
  `RecordBatch` — граница батча ≡ граница native-блока. Сервер обрабатывает блоки in-order и
  **не разрезает** их; мелкие блоки могут быть **склеены** через
  `min_insert_block_size_rows`/`bytes` (склейка целых блоков безопасна для I5). В пределах одной
  CH-партиции один принятый блок → один парт (squashing только склеивает, уже принятый блок не
  режет). Таким образом при выполнении (b): `RecordBatch` ≡ native-блок ≡ один парт ≡ атомарная
  единица.

  **Durability `insert_many`** (проверено по `clickhouse-arrow 0.2.1`, `client.rs:542`): метод
  шлёт блоки, затем ждёт `EndOfStream` от сервера — `Ok` возвращается **только после
  подтверждения INSERT сервером**. Значит парт создан и **виден** последующему `SELECT max` на
  том же хосте немедленно (см. §4/§9 про pin-to-host для Replicated). Остаточный риск — только
  реальный краш хоста до fsync (`fsync_after_insert=0` по умолчанию), узкое окно (§13).

  **Проверки в реализации:**
  (а) middleware не дробит `RecordBatch` (FilterMiddleware режет только целые строки — ок);
  (б) интеграционный тест: `insert_many` с N offset-группами → kill mid-stream → в CH ровно K
  групп (K ≤ N, целые группы) — на таблице с безопасным `PARTITION BY` (tuple или по
  `__system_partition`);
  (в) зафиксировать версию `clickhouse-arrow = "0.2"`; при апгрейде — перепроверить поведение
  сериализации блоков;
  (г) `PARTITION BY` по колонке данных — ответственность оператора (§13); проверка на старте
  не делается (решение автора: не усложнять DDL-верификацию).

Из I2+I3: **любой неопределённый исход записи откатывается через рестарт** — waterline
пересоздаётся из ClickHouse, источник переотдаёт всё незакоммиченное, оно примиряется с
фактически записанным.

**Дополнительный инвариант плумбинга:** пустые батчи (0 rows) с `exactly_once_key = Some`
не доходят до синка — аккумулятор отсеивает их до flush.

**Предположение о владении партицией:** waterline — состояние **per-process**, не разделяемое
между воркерами. Корректность требует, чтобы в каждый момент партицию писал **ровно один
процесс**:
- **YDS:** эксклюзивная аренда партиции ридером (один консюмер на партицию).
- **S3:** в текущей версии — один процесс на префикс. Параллельная работа нескольких воркеров
  над одним префиксом с непересекающимися подмножествами файлов — отдельный дизайн.
- Перекрытие двух процессов на одной партиции (rolling update без drain, смена
  `total_workers` на лету) — за рамками гарантий, см. §13.

**S3: иммутабельность и retention.** S3-файлы иммутабельны и не удаляются в течение всего
жизненного цикла пайплайна. Процесс-воркер **никогда** не удаляет файлы из S3 — ни после
успешной обработки, ни при ошибках. Удаление — зона ответственности внешнего retention-
механизма (S3 Lifecycle Policy) с окном, гарантированно превышающим время обработки + время
восстановления после сбоя.

**Retention CH — вне контракта пайплайна (важно).** Как ClickHouse удаляет данные у себя внутри
(`TTL`, `DROP PARTITION`, `TRUNCATE`, мутации) — **за рамками гарантий** этого дизайна.
Waterline хранит состояние дедупа в тех же данных, что чистит retention; если CH удалит строки,
которые waterline считает записанными, возможна тихая потеря на реплее затронутых офсетов.
Единственное требование к оператору: **retention CH ≥ retention источника + окно восстановления
после сбоя**, и не выполнять `TTL`/`DROP`/`TRUNCATE`/мутации по колонкам ключа на работающей
exactly-once таблице. Пайплайн **не детектирует** и **не защищается** от нарушения этого
допущения (best-effort детектор регрессии в §9 — не гарантия, а удобство). См. §9/§13.

---

## 1. Суть подхода

Дедупликация на стороне Rust. Источник добавляет в каждую строку колонки составного ключа.
Синк держит in-memory `Waterline` и перед INSERT фильтрует строки с
`offset ≤ waterline(partition)`. Waterline для партиции подгружается из ClickHouse **лениво**
при первой встрече партиции и кешируется.

```
                    первая встреча партиции P
Каждый батч ──►  ensure_loaded(P): SELECT max(offset) WHERE partition=P  (разово, кешируется, включая None)
                    │
                    ▼
              waterline(P): Option<i64>   ── O(1) на последующих батчах
                    │
                    ▼
   filter(rows where offset > waterline) → INSERT → mark_committed(P, max_offset)
                    │
      ошибка INSERT ──► poison(sink) ──► fatal ──► выход процесса ──► рестарт (waterline перечитается)
```

При старте в ClickHouse **не ходим** — waterline наполняется по мере появления партиций
в данных. Синк полностью data-driven: имена колонок ключа берёт из батча.

### 1.1. Rejected alternatives (почему не нативный дедуп CH)

Рассматривались и отвергнуты три альтернативы серверного дедупа; фиксируем причины, чтобы к ним
не возвращались без нового аргумента.

- **content-hash дедуп ReplicatedMergeTree** (идемпотентный retry по хэшу блока). Срабатывает
  только если повторно прислан **идентичный** блок (те же строки, тот же порядок). **Пайплайн
  не может гарантировать одинаковый состав батчей между рестартами** (другой тайминг аккумуляции,
  другая группировка Message, частичные чтения) → другой хэш → дедуп не срабатывает → дубли.
  **Это — ключевая причина отказа от серверного дедупа.**
- **`SET insert_deduplication_token`** — та же проблема: стабильный токен пришлось бы вычислять
  независимо от состава батча, а прежний `compute_dedup_token` хешил
  `(partition_id, msg_count, first/last_msg_prefix)`, т.е. **зависит от состава батча** → разные
  батчи после рестарта → разный токен → дубли. Плюс латентный баг утечки `SET`-токена на
  пулированном коннекте (§14, Шаг 1).
- **ReplacingMergeTree по `ORDER BY (partition, offset)`** — единственная batch-independent
  альтернатива (схлопывает дубли по ключу на мердже). Отвергнута, т.к. даёт дедуп **отложенно**
  (только на мердже/`FINAL`): дубли видны читателю до схлопывания, а каждое чтение обязано нести
  `FINAL`/`argMax` — стоимость и ответственность переносятся на downstream.

**Почему waterline.** Дедуп по пер-строчному ключу `(partition, offset)` **не зависит** от
разбиения на батчи (в отличие от content-hash/token) и даёт дедуп **на записи** — downstream
видит чистые данные без `FINAL` (в отличие от ReplacingMergeTree). Цена — in-memory состояние,
single-writer инвариант и ленивые сканы; она принимается осознанно (см. §13).

---

## 2. Составной ключ уникальности

Парсер объявляет ключ (на основе конфига `ParserConfig`) и называет колонки.
Источник НЕ знает про exactly-once — он только заполняет `Message.offset`/`partition`
сырыми значениями, из которых парсер строит колонки ключа в `RecordBatch`.

```rust
/// Одна колонка составного ключа уникальности.
#[derive(Debug, Clone)]
pub struct ExactlyOnceColumn {
    pub name: Arc<str>,
}

/// Составной ключ. Колонки физически лежат в RecordBatch; дескриптор называет их роли.
#[derive(Debug, Clone)]
pub struct ExactlyOnceKey {
    pub partition: ExactlyOnceColumn,  // Тип в RecordBatch определяет семантику:
    pub offset:    ExactlyOnceColumn,  // Int64 → монотонный offset (YDS); Utf8 → S3 full key
}

/// Значение ключа партиции — ключ HashMap'а waterline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    Int(i64),
    Str(String),  // S3: полный объектный ключ (не базовое имя)
}

impl PartitionKey {
    /// SQL-литерал для подстановки в `WHERE partition = {val}`.
    fn to_sql_literal(&self) -> String {
        match self {
            PartitionKey::Int(v) => v.to_string(),
            // НИКАКОГО ручного экранирования: кодируем байты в hex и подставляем через unhex().
            // hex — это [0-9a-f], спецсимволов нет → экранировать нечего в принципе.
            // Почему не строковый литерал / не binding драйвера:
            //   - CH-литералы и даже bound-параметры {p:String} применяют C-style unescape →
            //     backslash в имени (`dir\batch`) теряется без экранирования (проверено на CH 25.4);
            //   - clickhouse-arrow 0.2.1 сам экранирует только `'`, не `\` (encode_field_dump),
            //     т.е. его binding для таких имён СЛОМАН — см. §13/заметку.
            // unhex('...') корректность делегирует тривиальному hex::encode, а не ручному escape.
            PartitionKey::Str(v) => format!("unhex('{}')", hex::encode(v.as_bytes())),
        }
    }
}
```

**Умолчания по источникам** (имена задаёт источник; переопределение имён — на будущее; при
переопределении добавить валидацию backtick для кастомных имён колонок):

| Источник   | partition            | offset            | Тип partition        |
|------------|----------------------|-------------------|----------------------|
| YDS topic  | `__system_partition` | `__system_offset` | Int64                |
| YDS pqv1   | `__system_partition` | `__system_offset` | Int64                |
| S3         | `__system_filename`  | `__system_offset` | Utf8                 |
| CH-source  | ключ не выставляется | —                 | —                    |

> **CH-source:** стабильный ключ не определён → exactly-once не поддерживается.
> При `add_exactly_once_key: true` + CH-source → **WARN** + деградация до `AT_LEAST_ONCE`.

**S3 `__system_filename`:** полный S3 object key (как в `self.files[current_idx].location`),
**не** базовое имя. Например: `"prefix-a/2024/data.json"`, не `"data.json"`. Это исключает
коллизию имён из разных префиксов.

**Именование:** префикс `__` зарезервирован под системные колонки; пользовательские колонки
с `__` запрещаются валидацией конфига.

**Защита от коллизии в данных (runtime):** валидация конфига проверяет только сконфигурированные
колонки. Если в данных (JSON/YDS-сообщении) приходит поле с префиксом `__` (например,
`__system_partition` от пользователя), **не объявленное в конфиге**, парсер может создать
колонку с таким именем, а затем попытаться добавить системную колонку с тем же именем →
дубликат в Arrow Schema → panic/ошибка. Поэтому парсер при `exactly_once` **перед созданием
батча** проверяет, что имена системных колонок (`__system_partition`, `__system_offset`,
`__system_filename`) отсутствуют среди DataFusion-колонок данных. При конфликте — **fatal**
с читаемым сообщением: `"Column '__system_partition' conflicts with a user data field; rename the field or disable exactly_once"`.

**Опт-ин:** булев флаг `add_exactly_once_key: true` в конфиге источника. При `false` источник не
выставляет ключ → at-least-once (§11).

---

## 3. Проброс ключа через пайплайн

Ключ едет **вместе с данными**; значения — в колонках батча. Синк не конфигурируется именами
колонок и не нуждается ни в ключе, ни в списке партиций на старте — всё берётся из `TableWrite`.

```
Message{value, offset, partition}          ← источник заполняет offset/partition (I1)
   ▼
Parser::parse_into                          ← добавляет колонки ключа в RecordBatch
   │  + partition_column (константа на батч)
   │  + offset_column    (per-row: offset породившего Message)
   ▼
TableData{ batch, exactly_once_key: Option<ExactlyOnceKey> }
   ▼
BatchAccumulator                            ← просто копит батчи (без агрегации метаданных)
   ▼
TableWrite{ batches, exactly_once_key }
   ▼
Sink::write                                 ← сам читает partition/offset из колонок,
                                              вычисляет min/max, фильтрует, INSERT
```

Изменения в типах:
- `Message` (`types/message.rs`): `offset: Option<i64>`, `partition: Option<PartitionKey>`.
- `TableData` (`types/table_data.rs`): `exactly_once_key: Option<ExactlyOnceKey>`.
- `TableWrite` (`types/table_data.rs`): `exactly_once_key: Option<ExactlyOnceKey>`.
- Поля `min_offset`, `max_offset`, `single_partition`, `partition`, `dedup_token` **отсутствуют**
  во всех типах. Синк извлекает всё необходимое напрямую из колонок `RecordBatch`.

### 3.1. Заполнение колонок в парсере

`offset` — **пер-строчный**: все строки одного Message получают его offset; строки из разных
Message — разные офсеты. `partition` — **константа на батч** (YDS: один пайплайн = одна
партиция; S3: один файл).

В `JsonParser::parse_into` (все 4 ветки `AllRootField`/`Mixed` × `NewLine`/`NoSplit`):
для **каждой успешно распарсенной** строки в лок-степ с data-колонками аппендить `partition`
и `msg.offset` в два дополнительных builder'а. Строки, уходящие в DLQ, в offset-колонку
**main** не аппендят (у DLQ свои колонки ключа — §7). Схема парсера (`arrow_schema`) при
`exactly_once` расширяется двумя полями ключа в конце.

**Колонки ключа — non-nullable:** `Field::new("__system_offset", DataType::Int64, false)` и
`__system_partition`/`__system_filename` тоже non-nullable. При `add_exactly_once_key: true`
источник **обязан** заполнять и `Message.offset = Some(...)`, и `Message.partition = Some(...)`.
`Message.offset == None` **или** `Message.partition == None` при exactly-once → **fatal** на входе
в парсер (до построения батча) — правила симметричны. Соответственно `group_by_partition` при
NULL в partition-колонке → fatal (§5.f), а не молчаливое схлопывание в `Int(0)`.

**Никакой агрегации в аккумуляторе:** `BatchAccumulator` только накапливает `RecordBatch`-и
из `TableData` в `TableWrite`. Ни `min_offset`, ни `max_offset`, ни `single_partition` не
вычисляются — синк сам читает колонки и принимает решения.

**Граница флеша — только по границе Message.** При exactly-once аккумулятор **не имеет права**
разрезать Message между двумя `TableWrite`. Flush происходит либо после завершения полного
`RecordBatch` одного Message, либо при достижении лимита по размеру — но в этом случае
**текущий** Message доводится до конца и флешится целиком (даже с превышением лимита),
а следующий Message начинает новый батч. Если Message настолько велик, что не помещается
в памяти, — это ошибка конфигурации («message too large for exactly-once batch» → fatal).
Нарушение этого правила = разрезание offset'а между флешами = waterline поднят для части
строк → потеря хвоста Message при падении на втором `TableWrite` (I5 закрывает атомарность
внутри одного флеша, но не между флешами).

> **Про размер Message и атомарность.** Единственный настоящий предел размера Message — **память**
> (весь Message должен уместиться в один `RecordBatch`). Со стороны CH дополнительного предела
> нет: в пределах одной CH-партиции сервер **не режет** уже принятый native-блок на несколько
> партов (squashing только склеивает), поэтому большой multi-row Message с константным
> `__system_partition` уходит одним партом атомарно (при безопасном `PARTITION BY`, см. I5/§0).
> `max_insert_block_size` относится к чтению сервером из источника (SELECT/файл), а не к блокам,
> принятым по native-протоколу от клиента.

---

## 4. Waterline (in-memory, ленивая загрузка)

```rust
/// Ключ waterline: (таблица, партиция). Разные таблицы (main и DLQ) имеют
/// независимые waterline в пределах одного синка.
type WaterlineKey = (Arc<str>, PartitionKey);

/// Кеш max committed offset на (таблицу, партицию).
/// `None` для ключа = «ещё не видели / нет в CH» (НЕ то же, что offset 0).
pub struct Waterline {
    committed: HashMap<WaterlineKey, i64>,
    /// Negative cache: ключи, загруженные из CH и пустые (нет строк). Всегда disjoint
    /// с `committed` (см. `mark_committed`). Защищает от повторных `SELECT max` на пустую партицию.
    loaded_also_empty: HashSet<WaterlineKey>,
    /// LRU-порядок для эвикта при переполнении cap (bounded память для S3-ключей).
    lru: /* bounded LRU */,
    cap: usize,
}

impl Waterline {
    /// Гарантирует, что waterline (таблицы, партиции) загружен в кеш. Разово ходит в CH.
    ///
    /// **API note:** `&mut self` — Waterline single-owner (§4.1), конкурентного доступа нет.
    /// Ни `RwLock`, ни `tokio::Mutex` не нужны. Синк вызывает `write()` строго последовательно,
    /// writer ждёт завершения каждого флеша перед следующим.
    async fn ensure_loaded(
        &mut self,
        client: &Client,
        table: &Arc<str>,
        key: &ExactlyOnceKey,
        pid: &PartitionKey,
    ) -> anyhow::Result<()> {
        let wk: WaterlineKey = (table.clone(), pid.clone());
        // Проверка кеша (включая ранее закешированные None — negative caching).
        if self.committed.contains_key(&wk) { return Ok(()); }
        // loaded_also_empty — отдельный HashSet для ключей, которые загружены и пусты.
        // Защищает от повторных SELECT max на пустую партицию каждый батч.
        if self.loaded_also_empty.contains(&wk) { return Ok(()); }
        let mut q = format!(
            "SELECT max(`{o}`) FROM `{t}` WHERE `{p}` = {val} \
             HAVING count() > 0",
            o = key.offset.name, t = table, p = key.partition.name,
            val = pid.to_sql_literal(),
        );
        // ReplicatedMergeTree: включаем select_sequential_consistency, чтобы чтение
        // (возможно, с другой реплики после рестарта) видело все кворумно-закоммиченные
        // вставки. Для plain MergeTree накладные расходы нулевые — настройка игнорируется
        // не-Replicated таблицами, поэтому применяем unconditionally (без ветвления по
        // флагу is_replicated).
        q.push_str(" SETTINGS select_sequential_consistency = 1");
        // ВАЖНО: без `HAVING count() > 0` ClickHouse на пустой выборке вернёт НЕ пусто, а одну
        // строку со значением 0 (проверено на MergeTree 25.4) → query_one дал бы Some(0),
        // а не None → первое сообщение партиции (offset 0) отфильтровалось бы. С `HAVING`:
        // пустая партиция → 0 строк → None. См. пояснение «Почему Option» ниже.
        // API: clickhouse-arrow 0.2.1 — query_one::<Option<i64>>(&q).await?
        let max: Option<i64> = client.query_one::<Option<i64>>(&q).await?;
        // None ⟺ строк нет (HAVING отсёк)
        if let Some(m) = max {
            self.insert_lru(wk, m);
        } else {
            // Negative caching: запоминаем, что партиция пуста — не ходить в CH повторно.
            self.loaded_also_empty.insert(wk);
        }
        Ok(())
    }

    /// Максимальный записанный offset. `None` = не видели.
    #[inline]
    pub fn committed(&self, table: &Arc<str>, pid: &PartitionKey) -> Option<i64> {
        self.committed.get(&(table.clone(), pid.clone())).copied()
    }

    /// Insert-or-max в `committed` + обновление recency в `lru` — **атомарно** (единственная
    /// точка мутации `committed`+`lru`, чтобы инвариант «оба обновляются вместе» держался, §4).
    /// При превышении `cap` эвиктит LRU-ключ из `committed` и `lru` одновременно.
    #[inline]
    fn insert_lru(&mut self, wk: WaterlineKey, offset: i64) {
        self.committed.entry(wk.clone())
            .and_modify(|v| *v = (*v).max(offset))
            .or_insert(offset);
        self.lru.touch(wk);                 // обновить recency
        while self.committed.len() > self.cap {
            if let Some(evicted) = self.lru.pop_lru() { self.committed.remove(&evicted); }
        }
    }

    /// Обновление после успешного INSERT. Монотонно (max) — дешёвая страховка.
    #[inline]
    pub fn mark_committed(&mut self, table: &Arc<str>, pid: PartitionKey, offset: i64) {
        let wk: WaterlineKey = (table.clone(), pid);
        // КРИТИЧНО: ключ покидает negative-cache и переходит в committed. Без этого remove
        // ключ застрянет в обоих множествах; после эвикта из `committed` он бы остался в
        // `loaded_also_empty` → следующий ensure_loaded вернётся рано с committed()==None →
        // пропуск CH-чтения → 5.b.2 вставит всё без фильтра → дубли. Два множества всегда
        // disjoint (debug_assert).
        self.loaded_also_empty.remove(&wk);
        self.insert_lru(wk, offset);        // committed+lru атомарно (см. §4)
        debug_assert!(self.committed.keys().all(|k| !self.loaded_also_empty.contains(k)));
    }
}
```

**Почему `Option`, а не `unwrap_or(0)`:** YDS-офсеты начинаются с **0**. При `unwrap_or(0)` +
фильтре `offset > 0` первое сообщение (offset 0) отфильтровалось бы и **потерялось**. `Option`
разводит «не видели» (пропускаем всё) и «записан max=0». Т.к. `ADD COLUMN` мы не делаем (§8),
легаси-строк с фантомным `partition=0/offset=0` не бывает — off-by-one закрыт полностью.
**Критично:** различение `None` vs `Some(0)` работает только благодаря `HAVING count() > 0`
в `ensure_loaded` — без него CH вернул бы `0` на пустой партиции (не NULL), `Option` схлопнулся
бы в `Some(0)` и первое сообщение (offset 0) потерялось бы. Именно этот `HAVING` делает
комментарий «`None` ⟺ строк нет» правдой.

**Negative caching:** ключи без строк в CH (`ensure_loaded` → `None`) сохраняются в
`loaded_also_empty: HashSet<WaterlineKey>` чтобы избежать повторных `SELECT max` на каждый
батч пустой партиции. При первом же успешном `mark_committed` ключ удаляется из
`loaded_also_empty` и переносится в `committed` (через `insert_lru`). Множества `committed` и
`loaded_also_empty` всегда **disjoint** (гарантируется `mark_committed`); LRU-эвикт затрагивает
только `committed`+`lru` (эвикнутый ключ безопасно перечитывается из CH). `loaded_also_empty`
ограничен числом ключей «в работе» (для S3 — файлы, читаемые, но ещё не записанные), т.к. запись
сразу переносит ключ в `committed`.

**`__system_offset = NULL` в данных невозможен по конструкции:** `CREATE TABLE` задаёт
`__system_offset Int64` (не Nullable); парсер всегда заполняет offset из
`Message.offset = Some(...)`; `ADD COLUMN` не делается. Ручная вставка с NULL — вне гарантий.

**Почему lazy, а не eager-скан на старте:** синк не знает списка партиций заранее в общем
случае (S3-файлы раскрываются в рантайме), и не должен зависеть от ключа/партиций до первого
батча. Ленивая загрузка единообразна для всех источников. Плата: на рестарте YDS — по одному
`SELECT max WHERE partition=P` на партицию (вместо одного `GROUP BY`); при типичном небольшом
числе партиций на воркер это дёшево (первый скан холодный, остальные тёплые из page-cache CH).
Если когда-нибудь упрёмся в «много партиций на воркер × огромная таблица» — добавим опциональный
bulk-preload одним `GROUP BY`.

**Размер `cap` для S3 (важно для производительности).** Для S3 `ensure_loaded` это
`SELECT max(offset) WHERE __system_filename = X` по **строковому** ключу — если `__system_filename`
не в первичном ключе таблицы, это скан. При потоке файлов больше `cap` LRU постоянно
эвиктит-и-перечитывает → скан почти на каждый файл → деградация CH. Но по факту waterline для S3
нужен **только** для файла, который сейчас реально перечитывается после сбоя внутри него:
иммутабельный уже обработанный файл больше не встречается (§10.1). Поэтому **`cap` для S3 держать
маленьким** — файл(ы) в работе + небольшой запас, а не история всех ключей; тогда bounded-LRU
почти не нагружается и точечных сканов по строковому ключу почти нет. (Держать историю всех
когда-либо виденных ключей — антипаттерн.)

**Эвикт (bounded-LRU) корректен:** повторная встреча эвикнутой партиции → `ensure_loaded`
перечитает из CH; значение монотонно не меньше записанного (мы — единственный writer партиции,
ходим в конкретный хост → парт виден сразу после `insert_many` → staleness нет, см. I5/§0) → ни
дублей, ни потерь. Для YDS кеш мал и до cap не доходит; эвикт реально работает только для потока
S3-ключей. **Синхронизация:**
`committed` и `lru` мутируются **только** через `insert_lru` (единая точка: insert-or-max +
touch recency + эвикт при переполнении `cap`) — иначе `contains_key` мог бы вернуть устаревший
`true` после эвикта из LRU. `loaded_also_empty` поддерживается disjoint-но (`ensure_loaded`
кладёт, `mark_committed` снимает) и в LRU-эвикте не участвует (деталь реализации, критичная для
корректности).

**Изменение `cap` между рестартами безопасно в любую сторону.** Уменьшение → чаще эвикт,
увеличение → реже; оба варианта корректны, потому что эвикция всегда перечитывает актуальное
состояние из CH, а не теряет данные.

### 4.1. Владение синком и waterline (по партиции, без общего лока)

**Один синк на партицию.** Для YDS каждая партиция получает **собственный** экземпляр
синка (свой коннект + свой `Waterline`). Синк строится **внутри спавна партиционного
таска** (по одному на `pid`), а НЕ один на воркер. Для S3 понятия партиции на уровне
пайплайна нет — один `S3Source` = один пайплайн = один синк, внутри которого `Waterline`
держит много ключей (по `filename`) с bounded-LRU.

**Общего мутабельного состояния между партициями нет.** Коннект, waterline и poison —
приватные для синка. Глобальное выключение при отказе одной партиции координирует
task supervisor (CancellationToken / abort), а не poison-флаг (§6.1). (Пул коннектов
как общий ресурс — опциональная будущая оптимизация; в базовом дизайне один коннект на
партиционный синк, что согласовано с fail-fast: обрыв = фатал = рестарт.)

**Лока нет.** Каждый синк принадлежит ровно одному writer-таску. **В каждый момент времени
в синке может быть не более одного активного вызова `write()`** — следующий вызов не
начинается до завершения предыдущего (writer ждёт завершения каждого флеша перед следующим).
Это жёсткий инвариант: конкурентные вызовы `write()` на одном синке = гонка на waterline =
потеря данных или дубли. Значит `Waterline` — single-owner: конкурентного доступа к нему
не бывает, никакой `Mutex`/`RwLock` не нужен. `&mut self` достаточно.

**Cross-partition гонок нет by construction.** Раз waterline у каждой партиции свой,
исчезает гонка «эвикт одной партиции ломает чтение другой»: LRU-эвикт вообще возможен
только внутри одного S3-синка, где writer единственный и серийный, т.е. и там конкуренции
нет.

**Несколько источников → одна таблица:** допустимо. Разные YDS-партиции (или S3-файлы)
пишут в одну CH-таблицу; waterline каждой партиции независим (ключ = table+partition).

---

## 5. Алгоритм записи (ClickHouse Sink)

```text
write(TableWrite w):
    # Нет ключа → at-least-once: обычный INSERT.
    if w.exactly_once_key is None:
        insert(w.table, w.batches)
        return

    key = w.exactly_once_key

    # ── 5.a. Найти уникальные партиции в данных ──
    # Читаем колонку partition из RecordBatch; для каждой партиции
    # формируем группу (pid, Vec<row_idx>).
    partitions = group_by_partition(w.batches, key.partition.name)
    # partitions: HashMap<PartitionKey, Vec<(batch_idx, row_idx, offset)>>

    for (pid, rows) in partitions:
        ensure_loaded(w.table, pid)                 # разово для (таблица, партиция), включая negative cache
        wl = waterline.committed(w.table, pid)      # Option<i64> (без лока — §4.1)

        # ── 5.b. Три случая на группу ──
        max_off = max(row.offset for row in rows)
        min_off = min(row.offset for row in rows)

        # 5.b.1: все строки ≤ waterline → дубликат
        # INSERT не делается, mark_committed не нужен (waterline уже ≥ max_off).
        # write() возвращает Ok(()) → источник закоммитит офсет.
        # Это корректно: данные гарантированно в CH (waterline — тому подтверждение).
        if wl is Some(v) and max_off <= v:
            continue

        # 5.b.2: все строки > waterline → INSERT как есть (без фильтра)
        if wl is None or min_off > wl.unwrap():
            insert_rows(w.table, w.batches, rows)   # собираем RecordBatch из отобранных row_idx
            waterline.mark_committed(w.table, pid, max_off)
            continue

        # 5.b.3: частичное перекрытие — фильтрация
        # Формируем mask по колонке offset: offset > waterline AND offset IS NOT NULL
        keep_mask = gt(offset_col, wl.unwrap())     # NULL > wl → NULL → false (безопасно)
        filtered_rows = [r for r in rows if keep_mask[r.row_idx]]
        if filtered_rows not empty:
            insert_rows(w.table, w.batches, filtered_rows)
        # mark_committed от исходного max_off вызывается ВСЕГДА — даже если
        # filtered_rows пуст (все строки ≤ waterline). Отброшенные строки уже в CH
        # (waterline покрывает их offset), поэтому waterline можно безопасно поднять
        # до исходного max_off. Это асимметрично с 5.b.1: там waterline УЖЕ ≥ max_off,
        # а здесь — ещё нет.
        waterline.mark_committed(w.table, pid, max_off)
```

### 5.c. `mark_committed` всегда от оригинального `max_off`

После фильтрации (5.b.3) waterline обновляется от `max_offset` **исходной** группы, не
отфильтрованной. Отброшенные строки уже в ClickHouse (поэтому их и отбросили) — waterline
можно безопасно поднять до их максимума. Если бы использовали filtered-max, то при полной
фильтрации группы waterline не обновился бы — на следующем реплее те же строки снова
отфильтровались бы (лишняя работа, но не потеря). С исходным max_off — холостых проходов
нет.

### 5.d. Производительность

- Чтение колонки offset/partition из Arrow: O(N) по строкам, один SIMD-проход. На фоне
  сетевого INSERT — шум.
- **5.b.1** (`max_off <= waterline`): return без Arrow-фильтрации — O(N) на min/max.
- **5.b.2** (`None` или `min_off > waterline`): INSERT как есть, без `compute::filter`.
- **5.b.3**: дорогой путь (`compute::gt` + `filter_record_batch` + `concat_batches`),
  возникает только на реплее после рестарта. В steady state — всегда 5.b.2.

### 5.e. Много-партиционный флеш

`group_by_partition` естественно обрабатывает случай, когда в одном `TableWrite` оказались
строки из нескольких партиций (S3: аккумулятор склеил батчи из разных файлов). Каждая
группа проходит свой `ensure_loaded` и фильтрацию независимо. Никаких дополнительных
флагов или специальных путей не нужно — `group_by_partition` унифицирует single/multi.

### 5.f. `group_by_partition`: построение `PartitionKey` по типу колонки

Колонка партиции может быть `Int64` (YDS) или `Utf8` (S3). `group_by_partition` читает
`DataType` колонки и строит правильный вариант `PartitionKey`:

```
match col.data_type() {
    DataType::Int64  → PartitionKey::Int(value)
    DataType::Utf8   → PartitionKey::Str(value)
    other            → fatal (неизвестный тип ключа)
}
// NULL-значение в partition-колонке → fatal (не молчаливое схлопывание в Int(0)/"").
// При exactly-once колонка non-nullable, но проверка на всякий случай.
```

Аналогично в `ensure_loaded` для SQL: `Int` подставляется как число, `Str` — как
`unhex('<hex>')` (hex-кодирование байтов, без экранирования — см. §2 и заметку про драйвер
в §13). Exposed как `PartitionKey::to_sql_literal(&self) -> String`.

**Несколько строк с одним offset** (newline-splitter, §3.1): `group_by_partition` группирует
все строки одного `(partition, offset)` в одну группу; `min_off` и `max_off` для такой группы
равны. Это корректно: waterline-фильтр применяется пер-строка, и если группа частично
перекрывается с waterline (5.b.3), каждая строка фильтруется индивидуально. На практике
частичное перекрытие для строк с одним offset невозможно — они все принадлежат одному Message,
который либо записан целиком, либо нет (I5).

### 5.g. `insert_rows`: построение RecordBatch из отобранных строк

`insert_rows(table, batches, rows)` принимает `rows: Vec<(batch_idx, row_idx, offset)>` —
строки, отобранные фильтром из исходных батчей. Механизм сборки:
- Строки фильтруются через `arrow::compute::filter_record_batch` для каждого исходного батча;
- Результаты объединяются через `arrow::compute::concat_batches` (схемы идентичны — гарантируется
  парсером);
- Итоговый `RecordBatch` отправляется в `client.insert_many(table, &[batch])`.

---

## 6. Fail-fast при ошибке INSERT или `ensure_loaded` + poisoning-обёртка

Waterline — скаляр на партицию; он **не умеет** выразить «дыру» в диапазоне (записаны
[40..45] и [48..50], но не [46..47]). `insert_many` пишет несколько блоков **неатомарно** —
частичный сбой оставил бы дыру. **Решение: любая ошибка `ensure_loaded` или `INSERT` инвалидирует синк и роняет процесс**

```
INSERT error
   → poison(sink)                 # синк больше не примет ни одной записи (§6.1)
   → ошибка пробрасывается как Err из drain_and_ack → run_partition_pipeline возвращает Err
   → процесс выходит с ненулевым кодом (exit(1))
   → супервизор (k8s/systemd) перезапускает
   → waterline пуст; ensure_loaded перечитает актуальный max из CH (source of truth)
   → источник переотдаёт незакоммиченное (I3) → примиряется с waterline
```

Разбор «[40..45] записан, [46..50] упал»: маркер не закоммичен (I3) → источник переотдаёт с
offset < 40 → `ensure_loaded` даёт waterline=45 → [40..45] фильтруются, [46..50] вставляются.
Ни потери, ни дубля.

**Разбор корректен в силу I5:** граница native-блока всегда совпадает с границей группы
`(partition, offset)` — весь Message в одном `RecordBatch` = одном native-блоке (см. P3 в §0).
Частичный сбой `insert_many` теряет только **целые** блоки, не фрагменты offset'а. Поэтому
«поднять waterline до частично записанного offset» невозможно: либо весь блок записан →
`mark_committed` вызывается, либо нет → ошибка → waterline не сдвинут.

**Критично: ошибка синка НЕ глотается в writer-цикле.** Текущий код
(`pipeline/mod.rs:417-433`) делает `acc.clear(); None` при ошибке `flush_to_sink_and_ack` —
это молчаливая потеря данных (аккумулятор очищен, курсор источника продвинут, процесс жив).
**Исправление:** `drain_and_ack` пробрасывает `Err` → `run_partition_pipeline` возвращает
`Err` → задача падает → `exit(1)`. Никакого продолжения после ошибки синка.

**Удаляется из текущего кода:** in-process ретрай партиции с переиспользованием того же синка
(`main.rs:53-82`, до 5 попыток) — на нём дыра выживала бы. Ошибка синка = сразу fatal.
**Исправить:** `main.rs` возвращает `Err`/`exit(1)` при фатале (сейчас `Ok(())` — супервизор
не рестартует).

**Компромисс (документируется):** при флапающем ClickHouse — crash-loop с повторными ленивыми
запросами. Безопасно (данные не портятся), шумно; лечится backoff супервизора.

### 6.1. `PoisoningSink`

Poison — **per-sink**: каждый экземпляр синка (каждая партиция) держит **собственный**
`AtomicBool`. После ошибки INSERT флаг взводится — этот конкретный синк больше не
принимает `write()`:

```rust
pub struct PoisoningSink { inner: Arc<dyn Sink>, poisoned: AtomicBool }

impl Sink for PoisoningSink {
    fn write(&self, w: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if self.poisoned.load(Ordering::Acquire) {
                anyhow::bail!("sink poisoned by a prior insert failure");
            }
            match self.inner.write(w).await {
                Ok(()) => Ok(()),
                Err(e) => { self.poisoned.store(true, Ordering::Release); Err(e) }
            }
        })
    }
}
```

**Координация между партициями — через task supervisor, не через poison.** Poison защищает
от повторного использования **своего** синка после ошибки. Когда задача партиции возвращает
`Err`, task supervisor (рантайм пайплайна, `main.rs`) отменяет **все** остальные партиционные
задачи через CancellationToken / abort JoinHandle. Это даёт тот же эффект — процесс выходит
целиком, — но механизм разделён: poison = защита синка от reuse, supervisor = глобальное
выключение.

**Замечание по TOCTOU в PoisoningSink:** проверка `poisoned.load()` и вызов `inner.write()`
не атомарны. При двух параллельных вызовах `write()` оба могли бы пройти проверку до того, как
первый взведёт poison. Это допустимо: по дизайну **только один вызов `write()` может быть активен
в любой момент времени** (§4.1), и параллельных вызовов не бывает. `AtomicBool` — страховка,
а не полноценный лок. **Enforcement в реализации:** `PoisoningSink` должен проверять отсутствие
in-flight `write()` (например, `AtomicBool` «write in progress») и паниковать при нарушении —
молчаливая гонка на waterline недопустима.

In-process enforcement инварианта «после ошибки записи синк не зовётся»; выход процесса — уже
восстановление.

### 6.2. Исторический `ParallelChInsertSink` был несовместим с exactly-once

В удалённом прототипе `ParallelChInsertSink`
(`middleware/parallel_ch_insert.rs`, файла больше нет) раскидывал записи round-robin по N
пулам и в докстринге декларировал *«assumes all keys unique, parallel **out-of-order**
inserts»*. Под предложенным здесь exactly-once это ломалось дважды:
- **нарушает I4**: out-of-order вставки одной партиции ⇒ waterline (скаляр `max`) занизит/
  переставит порядок ⇒ потеря;
- допущение «все ключи уникальны» **ложно**: newline-splitter даёт несколько строк с одним
  offset, а реплей повторяет offset'ы.

Кроме того, его единственным механизмом exactly-once был
`SET insert_deduplication_token`, который шаг 1 исторического чеклиста предлагал удалить.
Предложенная здесь проверка должна была завершать запуск с fatal при сочетании
`add_exactly_once_key: true` и `ParallelChInsertSink`. Waterline-дедуп предполагал серийный
пер-партиционный синк (I4/§4.1).

### 6.3. Graceful drain in-flight операций при exit

#### 6.3.1. Drain при ошибке

Когда задача партиции возвращает `Err`, task supervisor инициирует глобальное выключение:
отменяет остальные задачи (CancellationToken) и ждёт завершения in-flight `write()`-вызовов
(graceful drain с таймаутом, например 30 сек). Отмена через CancellationToken останавливает
**новые** вызовы `write()`, но не прерывает уже выполняющиеся (например, тяжёлый INSERT
другой партиции). Корректность **не требует** drain — даже при hard kill (SIGKILL)
сохраняется гарантия exactly-once:

- Если in-flight INSERT успел закоммититься в CH **и** `mark_committed` вызван → waterline
  обновлён, источник закоммичен — всё корректно.
- Если in-flight INSERT успел закоммититься в CH, но `mark_committed` **не** вызван (убиты
  до) → данные в CH, waterline не обновлён, источник не закоммичен (I3) → на рестарте
  `ensure_loaded` прочитает реальный max из CH → дубли отфильтруются — корректно.
- Если in-flight INSERT **не** успел → данные не в CH, waterline не обновлён, источник
  переотдаст — корректно.

**Сценарий main-OK/dlq-in-flight:** `sink.write(main)` завершён, waterline(main) обновлён,
`sink.write(dlq)` в процессе → таймаут drain → `exit(1)`. На рестарте: `ensure_loaded(main)`
= актуальный max, `ensure_loaded(dlq)` = реальность (блок либо закоммичен, либо нет —
оба варианта корректны). Источник переотдаст → main отфильтрует, dlq допишет если нужно.

Вывод: graceful drain — best-effort (уменьшает холостую работу на рестарте), но
корректность гарантирована в любом случае.

**Поведение при превышении таймаута drain:** если in-flight flush'ы не завершились за
отведённое время (например, 30 сек) → форсированный выход: `WARN` в лог
(«graceful drain timed out with N pending flushes — forcing exit, correctness unaffected»)
→ `exit(1)`. Незавершённые INSERT'ы либо закоммичены в CH (тогда `max(offset)` покроет
их при рестарте), либо отброшены сервером (тогда источник переотдаст). Оба варианта
корректны — см. выше.

#### 6.3.2. Graceful shutdown (SIGTERM без ошибок)

При штатном завершении (SIGTERM, k8s preStop):
1. CancellationToken выставляется → новые вызовы `write()` блокируются.
2. Ждём завершения in-flight flush'а (текущий `sink.write()` — main + DLQ).
3. После успеха: `source.commit()` (с ретраями, до 10).
4. `exit(0)`.

Если in-flight flush завершился ошибкой → poison → `exit(1)` (штатный рестарт с
восстановлением). Таймаут graceful shutdown — 30 сек; при превышении → `exit(1)`.

---

## 7. DLQ и exactly-once

DLQ дедуплицируется **тем же ключом**, что и main. Синк не различает `events` и `events.dlq` —
обе получают `TableWrite` с `exactly_once_key` и проходят waterline-проверку по своему
(независимому) состоянию.

- **Динамическая DLQ-схема:** `DLQ_SCHEMA`/`DLQ_CH_COLUMNS` больше не статический `LazyLock`.
  Схема строится динамически из `ExactlyOnceKey` парсера:
  - YDS: `__system_partition Int64`, `__system_offset Int64`
  - S3: `__system_filename Utf8`, `__system_offset Int64`
- Существующий `partition_id` **убирается** (заменяется на системную partition-колонку ключа).
- `dlq_payloads` расширяется до
  `Vec<(Bytes, DlqReason, i64 /*offset*/, PartitionKey)>` — offset/partition из текущего
  Message.

**Независимые waterline main и DLQ самокорректируют неатомарность между таблицами.** Крах между
INSERT в main и INSERT в DLQ: на рестарте каждая таблица перечитывается отдельно; источник
переотдаёт незакоммиченное (I3); строки допишутся ровно туда, где их нет.

**Координация коммита источника (main + DLQ).** Writer-таск формирует оба `TableWrite`
(main + DLQ) из одного батча, затем последовательно вызывает `sink.write(main)` и
`sink.write(dlq)`. Коммит источника происходит **единожды после успеха обоих**, а не после
каждого `TableWrite`. Сценарий:

```
sink.write(main)  → OK, waterline(main) обновлён
sink.write(dlq)   → FAIL → poison → fatal → restart
```

На рестарте:
- `ensure_loaded(main)` → актуальный max (main-строки в CH)
- `ensure_loaded(dlq)` → отсутствуют (DLQ-строки не попали)
- Источник переотдаёт (I3 — коммита не было, т.к. не полный успех)
- Main фильтрует дубли (waterline корректен)
- DLQ дописывает недостающее

Если бы коммит происходил после каждого `TableWrite` независимо, то при падении DLQ
коммит main уже прошёл бы → источник не переотдал → DLQ-строки **потеряны**. Поэтому
коммит — только после полного успеха всей группы таблиц.

При `exactly_once: false` DLQ — как сейчас, без ключа.

---

## 8. DDL и стартовые проверки

**Колонки ключа создаются сразу в `CREATE TABLE` — `ALTER TABLE ADD COLUMN` не делается
никогда.** Это убирает легаси-строки с фантомными значениями ключа (и весь класс off-by-one на
миграции).

**Пользователь не задаёт DDL вручную.** DDL выводится автоматически:
- Схема = колонки данных (из парсера) + колонки ключа (из источника).
- Движок определяется топологией кластера (см. ниже).
- `CREATE TABLE IF NOT EXISTS` → `DESCRIBE` → если фактическая схема ≠ ожидаемая → fatal с
  читаемым diff'ом.

При `add_exactly_once_key: true`:

1. **Авто-выбор движка:**
   - `SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table = '{t}'`
   - **1 реплика** (или `system.replicas` пуст) → `MergeTree` (кворум не нужен,
     `select_sequential_consistency` игнорируется сервером → без накладных расходов).
   - **≥ 2 реплик** → `ReplicatedMergeTree` с авто-путём в ZooKeeper.
     Проверить `insert_quorum ≥ (replica_count / 2 + 1)` (большинство). Если меньше →
     **`FATAL`** («ReplicatedMergeTree requires insert_quorum ≥ majority for exactly-once»).
     Проверить `replica_count ≥ insert_quorum` (на 1-репликовом кластере `insert_quorum=2` —
     каждый INSERT будет таймаутиться). Если меньше → **`FATAL`**.
   - **`Distributed`** → **деградация до `AT_LEAST_ONCE` + `WARN`** (НЕ fatal): async block
     forwarding ломает консистентность waterline (`ensure_loaded` не видит блоки, ещё не
     доставленные на шарды). Рекомендация в WARN — писать напрямую в целевую `MergeTree`.
   - **Прочие неподдерживаемые движки** (`ReplacingMergeTree`, `CollapsingMergeTree`,
     `VersionedCollapsingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `Buffer`, etc.)
     → **`FATAL`**: они схлопывают/суммируют/модифицируют `__system_offset` → waterline
     необратимо сломан.

2. **Проверка версии ClickHouse ≥ 22.8:**
   - `SELECT version()` → если `< 22.8` → **`FATAL`** («ClickHouse 22.8+ required for
     exactly-once (select_sequential_consistency)»).

3. **`CREATE TABLE IF NOT EXISTS`** — колонки ключа включены в определение:
   - YDS: `__system_partition Int64`, `__system_offset Int64`
   - S3: `__system_filename Utf8`, `__system_offset Int64`
   - Свежая таблица создаётся корректной.

4. **verify после create** (`DESCRIBE`): убедиться, что таблица (возможно, уже существовавшая)
   реально содержит колонки ключа нужного типа. Если нет → **fatal**:
   ```
   table 'events' exists without exactly-once key columns.
   Recreate the table with these columns or disable exactly_once. Auto-migration is not performed.
   ```

5. **Никаких `ALTER TABLE ADD COLUMN`.** Даже если пользователь делает это вручную —
   **опасность DEFAULT:** `ALTER TABLE ADD COLUMN __system_offset Int64 DEFAULT 0` заполнит
   все легаси-строки значением `0` → `ensure_loaded` вернёт `Some(0)` → первое настоящее
   сообщение с offset 0 будет отфильтровано и **потеряно**. **Единственный безопасный путь
   миграции — создать новую таблицу с колонками ключа в `CREATE TABLE` и перезалить данные.**
   `DEFAULT -1` **не рекомендуется**: он сохраняет только корректность offset, но засоряет
   таблицу фантомными строками (`offset=-1`, `partition=<default>`), схлопывает все легаси-
   партиции в дефолтную и попадает в `SELECT * EXCEPT(__system_*)` как мусор; `-1` также
   противоречит «offset неотрицателен, монотонен» (I1). Проверка при старте **не покрывает** этот
   случай (колонки есть, типы верны), поэтому фиксируем как ответственность оператора.

6. То же для DLQ-таблицы (её всегда создаём мы → колонки будут).

7. **Проверка имени таблицы:**
   - Backtick (`` ` ``) в имени → **fatal**.
   - Точка (`.`) в имени → **fatal**.

8. **В `engine_full` найдена `TTL`-клауза → `INFO`** («TTL will delete key columns → waterline
   may be lowered for affected offsets; at-least-once on replayed tail»).

9. **`add_exactly_once_key: true` + sink = `ParallelChInsertSink` → fatal** (несовместим, §6.2).

10. **Вывод и лог режима гарантий** (§11).

Итог: **отсутствие колонок ключа → fatal**; **неподдерживаемый движок (Replacing/Collapsing/
Summing/Aggregating/Buffer/…) → fatal**; **`Distributed` → деградация до AT_LEAST_ONCE + WARN**
(не fatal); **CH < 22.8 → fatal**; **backtick/точка в имени таблицы → fatal**.

---

## 9. Поддерживаемые движки ClickHouse и retention

`ensure_loaded` (`SELECT max(offset) WHERE partition=P`) корректен только на движках, где строки
**не мутируют и не схлопывают числовые колонки**. Поддерживаются ровно два движка:

| Движок | Статус | Комментарий |
|--------|--------|-------------|
| `MergeTree` | ✅ | Строки не схлопываются → `max(offset)` = истинный максимум. `FINAL` не нужен. Используется на 1-репликовых кластерах. |
| `ReplicatedMergeTree` | ⚠️ | Строки не схлопываются. `insert_quorum ≥ majority` обязателен — стартовый `FATAL` при меньшем значении. `select_sequential_consistency = 1` в `ensure_loaded` страхует от staleness при чтении с другой реплики после рестарта (для не-Replicated таблиц SETTINGS игнорируется сервером → накладных расходов нет). |
| `Distributed` | ⚠️ | НЕ fatal → **деградация до `AT_LEAST_ONCE` + `WARN`**: async block forwarding, `ensure_loaded` не видит недоставленные блоки. Писать напрямую в целевую `MergeTree`. |
| **Всё остальное** | ❌ | `ReplacingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `Buffer`, etc. — стартовый `FATAL`. Эти движки арифметически изменяют или схлопывают `__system_offset` → waterline необратимо сломан. |

**Про Replicated и staleness:** `ensure_loaded` **всегда** включает
`SETTINGS select_sequential_consistency = 1` (см. §4). В steady state процесс ходит в один и тот же
хост → чтение-своих-записей выполняется тривиально, настройка не нужна, но и не вредит. При
**рестарте на другой реплике** (A упала → супервизор поднял процесс на B) запрос идёт на B, которая
могла не успеть получить последние кворумно-закоммиченные вставки с A — здесь
`select_sequential_consistency = 1` заставляет B дождаться всех подтверждённых записей, предотвращая
занижение `max(offset)` → дубли. Для не-Replicated таблиц (MergeTree) сервер игнорирует эту
настройку → накладных расходов нет. Поэтому она применяется **безусловно**, без ветвления по флагу
`is_replicated`.

**Требование pin-to-host (Replicated).** `ensure_loaded` корректен только если процесс читает и
пишет через **конкретный хост реплики**, а не через балансировщик/DNS-round-robin/k8s Service.
Логика «мы — единственный writer, ходим в конкретный хост → staleness нет» (§4) опирается на это.
Если чтение идёт через **балансировщик**, `select_sequential_consistency` **не помогает** — LB
может увести `SELECT max` на отставшую реплику (которая получила блок, но вне кворумной группы)
→ занижение `max(offset)` → дубли. Подключение к CH через балансировщик при Replicated exactly-once
— **вне гарантий** (см. §13); основная защита от staleness при вынужденном рестарте на другой
реплике — кворумная запись `insert_quorum ≥ majority` + `select_sequential_consistency = 1`.
Экзотика с `Distributed`-шардированием не по партиции — тоже §13.

**Про retention/TTL (вне контракта — см. §0).** Как CH удаляет данные внутри себя — **за рамками
гарантий** дизайна. Waterline хранит состояние дедупа в тех же данных, что чистятся; при
`TTL`/`TRUNCATE`/`DROP PARTITION`/мутациях на **работающей** таблице waterline закеширован
высоким значением → удаление невидимо для кеша → строки с offset ≤ wl фильтруются → **тихая
потеря**. Особо коварен `TTL` по **времени данных**: он может выесть середину диапазона офсетов,
при этом `max(offset)` не меняется — такую потерю не увидит **никакой** детектор, опирающийся на
`max`. Требование к оператору (не enforced): retention CH ≥ retention источника + окно
восстановления; не выполнять `TTL`/`DROP`/`TRUNCATE`/мутации по колонкам ключа на работающей
exactly-once таблице.

Best-effort детектор (не гарантия): если `ensure_loaded` при повторной встрече ключа даёт max
**меньше** кешированного → `ERROR` («waterline regression detected»). Он ловит лишь узкий случай
(удаление хвоста + повторный `ensure_loaded` после эвикта/рестарта) и **не покрывает** ни горячие
партиции в кеше (для них `ensure_loaded` больше не вызывается), ни «выеденную середину». Оставлен
как сигнал, не как защита.

**CH-мутации по системным колонкам:** `ALTER TABLE ... UPDATE __system_offset = ...` или
`DELETE WHERE __system_partition = ...` искажают `max(offset)` → waterline необратимо сломан →
дубли или потеря. **Не выполнять мутации, затрагивающие `__system_offset`/`__system_partition`,
на exactly-once таблицах.**

---

## 10. Стабильные офсеты по источникам (инвариант I1)

| Источник | Источник offset | Стабилен? |
|----------|-----------------|-----------|
| YDS pqv1 | `message_data.offset` (`pq_v1.rs:331`) — серверный офсет Logbroker | ✅ |
| YDS topic | `TopicReaderMessage.offset` + `get_partition_id()` (ydb 0.13.5, `topicreader/messages.rs:99,129`) | ✅ |
| S3 | `__system_filename` (полный S3-ключ) + row number (начиная с 0 в каждом файле) | ✅ при неизменном файле (см. §10.1) |
| CH-source | ключ не выставляется | — (деградация до AT_LEAST_ONCE) |

**Историческое предложение по пробросу offset (YDS):**
- pqv1: добавить `offset` в существовавший тогда `DecodedMessage` → `Message.offset`;
  `partition = Int(partition_id)`;
- topic: читать `msg.offset` и `msg.get_partition_id()` в удалённой реализации
  `ydb_topic.rs` вместо отбрасывания → `Message`.

**Синтетических офсетов нет.** Источник без стабильного логического offset ключ не выставляет →
at-least-once (§11).

### 10.1. Историческое предложение по доработке S3-источника

Удалённый прототип `S3Source` (`s3/source.rs`, файла больше нет) листил все файлы под
префиксом и шёл по ним последовательно с единым `partition_id`; `Message` нёс только `value`,
имя файла и офсет не прокидывались. Документ предлагал следующую доработку для exactly-once:

- **`__system_filename`** = `self.files[current_idx].location` — **полный S3 object key**
  (включая префикс: `"prefix-a/2024/data.json"`). Кладётся в `Message.partition = Str(full_key)`.
  Это исключает коллизию имён из разных префиксов.
- **`__system_offset`** = номер записи от начала файла (начиная с 0). Определяется фреймером
  (`ChunkSplitter` из конфига, `src/config/yaml.rs:215-218`):
  - **`NewLine`** (разделитель `\n`): row number инкрементится для каждой **полной** строки
    после сборки из чанков. `safe_split_at` на границе чанка переносит остаток разорванной
    строки в следующий чанк; счётчик инкрементится только после склейки всех частей строки →
    row number **не зависит** от границ чанков и размера буфера.
    **Row number = индекс физической записи от начала файла, независимо от исхода парсинга.**
    Счётчик инкрементится для **каждой** физической строки — успешной, ушедшей в DLQ или пустой —
    т.е. является чистой функцией от байтов файла, а не от результата парсера. Иначе смена
    конфига/кода парсера между проходами (строка, ранее падавшая в DLQ, теперь парсится) сдвинула
    бы offset всех последующих строк → нарушение I1 → рассинхрон waterline. (DLQ-строки несут свой
    row number как offset в DLQ-таблице, §7; в offset-колонку **main** они не аппендятся, §3.1 —
    но сам счётчик общий и по физическим строкам.)
  - **`NoSplit`**: весь файл — одна запись, row number = 0.
  - Счётчик сбрасывается в 0 при переходе к следующему файлу.
- **Чтение всегда с начала файла — всегда, при любой ошибке.** S3-источник **не может**
  возобновлять чтение с середины файла (байтовая позиция) — это сломало бы row number и
  нарушило I1. При транспортной ошибке (in-run retry после разрыва соединения) файл
  перечитывается с начала; waterline отфильтровывает уже записанные строки (5.b.1/5.b.3).
  > **TODO-оптимизация:** при транспортных ошибках можно восстанавливать счётчик строк по
  > байтовой позиции (`GetRange`) и возобновлять чтение с неё же — это уменьшит повторное
  > скачивание, но требует корректной имплементации row-counter-restore. Пока — простейший
  > вариант (всегда с 0), waterline всё отфильтрует.
- **Пустые файлы пропускаются.** S3-источник при обнаружении файла нулевого размера не создаёт
  `Message`-ов, не выставляет ключ и переходит к следующему файлу. Waterline для пустого файла не
  загружается — это корректно (нечего дедуплицировать).
  > **Допущение (ответственность оператора):** объекты листятся **только полностью записанными**.
  > S3 list-then-get не транзакционно-консистентен с созданием объекта: если листинг вернёт
  > 0-байтный объект, ещё дописываемый продюсером, он будет пропущен **навсегда** (источник не
  > перелистывает его). Многочастная/незавершённая загрузка не должна попадать в листинг префикса
  > (напр. писать в staging-префикс и атомарно переименовывать/копировать в рабочий). Это вне
  > контроля пайплайна.
- **Файлы иммутабельны.** Файл под тем же именем не должен переписываться другим содержимым
  (иначе row number перестают соответствовать записям) — I1.
- **Процесс НЕ удаляет файлы из S3.** Ни после успешной обработки, ни при ошибках. Удаление —
  зона ответственности S3 Lifecycle Policy с retention ≥ времени обработки + времени
  восстановления после сбоя.

Пока эта доработка не сделана, S3 работает в at-least-once (ключ не выставлен, §11). Архитектура
(ключ, кеш, флаги) под неё уже заложена.

---

## 11. Вывод режима гарантий на старте

Режим выводится из `(источник выставляет ключ?) && (синк умеет waterline-dedup?) && (движок MergeTree/ReplicatedMergeTree?) && (quorum OK? для Replicated) && (не Distributed?)` и **логируется**:

```
Guarantee mode: EXACTLY_ONCE  (key: __system_partition + __system_offset, sink: clickhouse, engine: MergeTree)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: ClickHouse version < 22.8 (select_sequential_consistency not available)
  → to enable EXACTLY_ONCE: upgrade ClickHouse to 22.8+
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: source 'topic' has exactly_once disabled
  → to enable EXACTLY_ONCE set source.<...>.add_exactly_once_key: true
    (adds __system_partition/__system_offset; table will be auto-created with these columns)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: table 'events_dist' has engine Distributed — async block forwarding breaks waterline consistency
  → to enable EXACTLY_ONCE: write directly to the underlying MergeTree table instead
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: ReplicatedMergeTree with insert_quorum < majority
  → to enable EXACTLY_ONCE: ALTER TABLE ... MODIFY SETTING insert_quorum = <majority>
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: CH-source does not provide stable exactly-once key
```

At-least-once — легитимный режим: ключ не выставлен → синк делает обычный INSERT.

---

## 12. Что происходит при рестарте (сводно)

```
1. Падение / выход процесса (в т.ч. fail-fast по ошибке INSERT, §6; graceful shutdown, §6.3.2)
2. Источник не получил commit для незакоммиченных офсетов (I3)
3. Рестарт процесса:
   a. Проверка версии CH ≥ 22.8
   b. Авто-выбор движка и схемы; CREATE TABLE IF NOT EXISTS; DESCRIBE-verify
   c. Проверка движка/реплик/quorum/TTL → fatal/warn при необходимости (§8/§9)
   d. Спавн партиционных задач; waterline ПУСТ
4. Источник переотдаёт с last_committed+1
5. Первый батч каждой партиции → ensure_loaded(P) читает max(offset) из CH
   (чтение с конкретного хоста), кладёт в кеш (включая negative caching для пустых)
6. Дубликаты фильтруются (5.b.1/5.b.3), новое пишется (5.b.2)
```

Стоимость: по одному `SELECT max WHERE partition=P` на партицию при её первой встрече
(§4). Пустые партиции кешируются как `None` → повторных запросов нет. Bulk-preload одним
`GROUP BY` — опциональная будущая оптимизация, если понадобится.

---

## 13. Известные ограничения

| Ограничение | Пояснение | Митигизация |
|-------------|-----------|-------------|
| CH-retention ≥ source-retention | TTL/TRUNCATE/DROP PARTITION при работающем процессе занижают waterline (данные удалены, кеш не знает) → потеря на затронутых офсетах | Не выполнять TRUNCATE/DROP на работающей exactly-once таблице. Детектор: `ensure_loaded` max < кешированного → `ERROR` |
| Существующая таблица без колонок ключа | `ADD COLUMN` не делаем; ручное `ALTER TABLE ADD COLUMN ... DEFAULT 0` создаст фантомные offset=0 → потеря первого настоящего сообщения. `DEFAULT -1` засоряет таблицу фантомными строками и схлопывает партиции | Fatal на старте (§8); миграция — **только** новым `CREATE TABLE` + перезаливка данных |
| `PARTITION BY` по колонкам данных (напр. `toYYYYMMDD(ts)`) | Строки одного Message разлетаются по разным CH-партициям → INSERT создаёт несколько партов, коммитящихся независимо → частичный сбой фрагментирует группу `(partition, offset)` → нарушение I5 → потеря хвоста | Ответственность оператора: `PARTITION BY` должен зависеть только от `__system_partition` (или `tuple()`). Автосоздаваемые таблицы `PARTITION BY` не задают → безопасны. Стартовая проверка **не делается** (решение автора) |
| Подключение к Replicated через балансировщик/Service | `select_sequential_consistency` не спасает — LB уводит `SELECT max` на отставшую реплику → занижение → дубли | Требование: pin к конкретному хосту реплики (§9). Балансировщик при Replicated exactly-once — вне гарантий |
| Distributed-шардирование не по партиции | Партиция размазана по шардам → `max(offset)` неконсистентен | Модель «партиция→один воркер→одна таблица»; экзотику не поддерживаем |
| Мульти-реплика: отказ реплики между INSERT и рестартом | INSERT на реплику A с `insert_quorum < majority` → OK, `mark_committed`, реплика A падает до репликации на B/C → `ensure_loaded` на B занижен → **дубли** | `insert_quorum ≥ majority` обязателен (§8); `select_sequential_consistency = 1` страхует от staleness. Надёжнее всего: не-Replicated таблицы (данные реплицируются на уровне Kafka/YDS) |
| Мульти-реплика: перманентный отказ кворумных реплик | `insert_quorum` = 2, 3 реплики. INSERT подтверждён A+B → `mark_committed`. A и B оба перманентно падают до репликации на C → рестарт на C → `select_sequential_consistency` бессилен → данные с A/B никогда не придут → `max(offset)` занижен → **дубли** | Экстремальный сценарий (потеря большинства реплик одновременно). Восстановление: ручной `UNDROP` реплики, либо принять дубли (downstream-дедупликация по `(partition, offset)` обязательна при использовании ReplicatedMergeTree) |
| Перекрытие записи одной партиции двумя процессами | Waterline — per-process. YDS: rolling update без drain, смена `total_workers` на лету → окно перекрытия → дубли. S3: в текущей версии — один процесс на префикс (параллельная работа — отдельный дизайн) | YDS: drain старого процесса перед стартом нового (k8s: preStop/`maxUnavailable=0`); не менять `total_workers` без полной остановки. S3: конфиг-валидация уникальности префикса на воркер |
| S3: переписывание файла под тем же именем | Row number перестаёт соответствовать записям | I1: файлы иммутабельны под своим именем |
| Детерминизм парсера между запусками | Изменение конфига/кода парсера между первым проходом и реплеем может изменить маршрутизацию offset'а (main↔DLQ) → потеря | Ответственность оператора. Опциональная защита: отпечаток конфига парсера, сохраняемый при первом проходе; при рестарте с другим отпечатком → `WARN`/`FATAL` «parser config changed; exactly-once window may contain DLQ'd offsets» |
| Много партиций на воркер × огромная таблица | N ленивых сканов на рестарте | Обычно N мало; negative caching убирает повторные запросы для пустых; при необходимости — bulk-preload одним `GROUP BY` |
| Crash-loop при флапающем CH | Любая ошибка `ensure_loaded` или `INSERT` → poison → fatal → рестарт → снова ошибка → цикл | Безопасно (данные не портятся); backoff супервизора (k8s: `restartPolicy: OnFailure` + `backoffLimit`). CH-данные — source of truth |
| Две системные колонки в `SELECT *` | ~доли % overhead | `SELECT * EXCEPT(__system_*)` / документирование |
| `clickhouse-arrow 0.2.1`: binding параметров экранирует только `'`, не `\` | `encode_field_dump` (`query.rs:161`) + сам `{p:String}` разэкранирует значение → backslash в S3-ключе терялся бы | Не используем binding для ключа; подставляем `unhex('<hex>')` (§2). Проверено на CH 25.4. Минимальная версия клиента: `clickhouse-arrow = "0.2"` |
| `Distributed`-движок | Distributed принимает INSERT и асинхронно пересылает блоки на шарды. `ensure_loaded` читает из Distributed и не видит блоки, ещё не доставленные на целевые шарды → waterline занижен → дубли | Стартовая деградация до `AT_LEAST_ONCE` + `WARN`. Exactly-once с Distributed невозможен: пишите напрямую в целевую MergeTree |
| Неподдерживаемый движок | ReplacingMergeTree, CollapsingMergeTree, SummingMergeTree, Buffer, etc. — схлопывают/суммируют/модифицируют `__system_offset` → waterline необратимо сломан | Стартовый `FATAL` (§8). Поддерживаются только MergeTree и ReplicatedMergeTree (§9) |
| `ON CLUSTER` DDL | `CREATE TABLE IF NOT EXISTS` без `ON CLUSTER` создаст таблицу только на одной реплике. На кластерных инсталляциях пользователь должен создать таблицу на всех репликах самостоятельно | TODO: поддержка `ON CLUSTER` в конфиге — будет добавлена отдельно. Пока что DDL через `ON CLUSTER` — зона ответственности пользователя; DESCRIBE-verify поймает отсутствие таблицы на реплике с читаемым fatal |
| Редкая гонка: fsync-сталл на MergeTree | INSERT закоммичен в CH, но парты финализируются (fsync) после `ensure_loaded` нового процесса → дубли. Окно узкое (рестарт — секунды, финализация — мс) | Для Replicated закрывается quorum; для MergeTree — остаточный риск (документируется) |

---

## 14. Чеклист реализации

### Шаг 1 — Типы ключа
- [ ] `ExactlyOnceColumn { name }`, `ExactlyOnceKey { partition, offset }`, `PartitionKey { Int, Str }` в `types/`
- [ ] Зависимость `hex` (для `PartitionKey::Str::to_sql_literal` → `unhex('<hex>')`, §2); без ручного экранирования
- [ ] `Message`: `offset: Option<i64>`, `partition: Option<PartitionKey>`
- [ ] `TableData`/`TableWrite`: `exactly_once_key: Option<ExactlyOnceKey>`. Никаких `min_offset`/`max_offset`/`single_partition`/`partition`. Поле `dedup_token` **удалить**.
- [ ] Удалить старый механизм: `compute_dedup_token`, `SET insert_deduplication_token`, `non_replicated_deduplication_window` из DDL. **Мотивация:** помимо архитектурной чистоты, это чинит латентный баг утечки `SET`-токена на пулированном коннекте (`clickhouse/sink.rs:148-151`).

### Шаг 2 — Источники (стабильный offset, I1)
- [ ] pqv1: `offset` в `DecodedMessage` → `Message.offset`; `partition = Int(partition_id)`
- [ ] topic: `msg.offset` + `get_partition_id()` → `Message`
- [ ] Флаг `add_exactly_once_key: bool` в конфиге источника; при `false` ключ не выставляется
- [ ] Валидация: запрет пользовательских колонок с префиксом `__`
- [ ] **CH-source + exactly-once → WARN + деградация до AT_LEAST_ONCE** (нет стабильного ключа)
- [ ] (S3, отдельная задача §10.1) `__system_filename` (полный S3-ключ) + row number (NewLine/NoSplit) → `Message`; выставить ключ; всегда перечитывать файл с начала при ошибке

### Шаг 3 — Парсер
- [ ] `JsonParser::new`: при exactly_once расширить `arrow_schema` колонками ключа
- [ ] `parse_into`: per-row `offset` + const `partition` во все 4 ветки
- [ ] **Колонка offset — non-nullable:** `Field::new("__system_offset", DataType::Int64, false)`
- [ ] **Валидация ключа:** `Message.offset == None` **или** `Message.partition == None` при exactly-once → **fatal** (до построения батча); `__system_partition` тоже non-nullable; `group_by_partition` при NULL в partition → fatal
- [ ] **Защита от коллизии имён (§2):** перед созданием батча проверить, что `__system_partition`/`__system_offset`/`__system_filename` отсутствуют среди имён data-колонок; при конфликте → fatal с читаемым сообщением
- [ ] DLQ: **динамическая схема** из `ExactlyOnceKey` (YDS: `__system_partition Int64`; S3: `__system_filename Utf8`); `dlq_payloads` тащит offset+partition

### Шаг 4 — Аккумулятор
- [ ] `BatchAccumulator`: **только копит батчи**, без агрегации метаданных. `TableWrite` содержит только `batches` и `exactly_once_key`
- [ ] **Граница флеша = граница Message** (§3.1): flush только после полного `RecordBatch` одного Message; при лимите размера — текущий Message завершается и флешится целиком; слишком большой Message → fatal
- [ ] Пустые батчи (0 rows) с ключом отсеиваются, не доходят до синка

### Шаг 5 — Waterline (lazy)
- [ ] `Waterline { HashMap<(Arc<str>, PartitionKey), i64>, bounded-LRU, cap, loaded_also_empty: HashSet<WaterlineKey> }`: `committed(table, pid)->Option`, `mark_committed(table, pid, offset)`, `ensure_loaded(table, pid)`
- [ ] `ensure_loaded`: `SELECT max(\`offset\`) FROM \`{t}\` WHERE \`{p}\` = {val} HAVING count()>0 SETTINGS select_sequential_consistency = 1` (backtick для `{p}` и `{o}`; SETTINGS игнорируется не-Replicated таблицами). **`HAVING count()>0` обязателен** — иначе CH вернёт `0` (не пусто) на несуществующей партиции → `Some(0)` вместо `None` → потеря offset 0 (§4)
- [ ] **Negative caching:** `None` → запись в `loaded_also_empty` (не ходить в CH повторно); `mark_committed` удаляет из `loaded_also_empty` и вставляет через `insert_lru`. `committed`/`loaded_also_empty` всегда disjoint; LRU-эвикт затрагивает только `committed`+`lru`
- [ ] **API:** `client.query_one::<Option<i64>>(&q).await?` (clickhouse-arrow 0.2.1; `query_one_scalar` не существует)
- [ ] Waterline — **приватное поле пер-партиционного синка**, single-owner, **без `RwLock`/`Mutex`**. `&mut self` достаточно — writer серийный (§4.1)
- [ ] Синк НЕ инициализирует waterline на старте

### Шаг 6 — Синк: запись
- [ ] Нет ключа → обычный INSERT (at-least-once)
- [ ] `group_by_partition`: сгруппировать строки по значению partition-колонки (читать колонку из RecordBatch)
- [ ] Для каждой группы: `ensure_loaded` → `min_off`/`max_off` из колонки offset → 5.b.1/5.b.2/5.b.3
- [ ] `mark_committed` от исходного `max_off` группы (не отфильтрованного)
- [ ] `insert_rows`: `filter_record_batch` → `concat_batches` → `insert_many` (один итоговый RecordBatch). **I5:** не дробить RecordBatch на под-блоки мельче Message (клиент не чанкует native-блоки — см. P3 в §0)
- [ ] Фильтр 5.b.3: `offset > wl AND offset IS NOT NULL`

### Шаг 7 — Fail-fast + poisoning
- [ ] **Синк строится на партицию**: убрать единый `build_sink()` из `main.rs` до цикла; строить синк внутри спавна на каждую `pid` (YDS → N синков; S3 → один синк на пайплайн). `deps` больше не несёт готовый `snk` — вместо этого `main.rs` создаёт CancellationToken для глобального выключения и передаёт его в партиционные таски; каждый таск сам создаёт свой синк (коннект + Waterline) при старте
- [ ] `PoisoningSink` (`AtomicBool`): флаг **per-sink** (приватный для каждого экземпляра); глобальное выключение при отказе партиции — task supervisor (CancellationToken / abort JoinHandle) (§6.1). **Enforcement:** проверка «не более одного `write()` одновременно» (in-flight guard: `AtomicBool` + panic при нарушении) — молчаливая гонка на waterline недопустима
- [ ] **Ошибка синка НЕ глотается в writer-цикле.** `drain_and_ack` пробрасывает `Err` → `run_partition_pipeline` возвращает `Err` → задача падает → `exit(1)` (§6)
- [ ] Ошибка INSERT → poison → fatal → **процесс выходит с ненулевым кодом** (сейчас `main` возвращает `Ok(())` даже при фатале — исправить: фатал таска → `Err`/`exit(1)`, иначе супервизор не рестартует)
- [ ] Убрать in-process ретрай синка в `main.rs` (`spawn_partition_task`, retry до 5); commit источника только после полного успеха флеша (I3)
- [ ] **`source.commit()`: до 10 ретраев → ошибка → poison → fatal** (неограниченный рост незакоммиченного окна недопустим)

### Шаг 8 — DDL и стартовые проверки
- [ ] **Авто-выбор движка:** 1 реплика → `MergeTree`; ≥2 → `ReplicatedMergeTree` (c авто-ZK-путём, `insert_quorum` = majority)
- [ ] **Проверка числа реплик ≥ insert_quorum** (на 1-репликовом кластере `insert_quorum ≥ 2` → таймаут каждого INSERT → crash-loop) → **`FATAL`**
- [ ] **Проверка версии CH ≥ 22.8** (`SELECT version()`) → **`FATAL`**
- [ ] `CREATE TABLE` с колонками ключа (main + DLQ), только при exactly_once; **без `ADD COLUMN`**; авто-DDL (пользователь не пишет DDL вручную)
- [ ] `DESCRIBE` verify: фактические колонки и типы = ожидаемые → **fatal** при расхождении
- [ ] Проверка движка: неподдерживаемый (ReplacingMergeTree, CollapsingMergeTree, SummingMergeTree, Buffer, etc.) → **`FATAL`**; Distributed → деградация до `AT_LEAST_ONCE` + `WARN`
- [ ] В `engine_full` найдена `TTL`-клауза → `INFO`
- [ ] `Buffer`-обёртка → `WARN`
- [ ] **Детектор регрессии waterline:** `ensure_loaded` max < кешированного → `ERROR`
- [ ] `add_exactly_once_key: true` + sink = `ParallelChInsertSink` → **fatal** (несовместим, §6.2)
- [ ] Вывод и лог режима гарантий (§11)

### Шаг 9 — Тесты
- [ ] Юнит: `committed`/`mark_committed`; `None` vs `Some(0)` (offset 0 не теряется)
- [ ] Интеграция: **`ensure_loaded` на несуществующей партиции возвращает `None`, а не `Some(0)`** (проверка `HAVING count()>0`; регрессия на реальном CH)
- [ ] Юнит: `ensure_loaded` — загрузка/кеш/negative caching/эвикт+reload
- [ ] Юнит: фильтрация 5.b.1/5.b.2/5.b.3, single vs multi-partition
- [ ] Юнит: `PoisoningSink` — после Err не зовёт inner (per-sink poison); интеграция: отказ одной партиции → глобальное выключение через task supervisor → процесс выходит с ненулевым кодом
- [ ] Интеграция: запись → фильтрация дублей; рестарт → ленивая перезагрузка → нет дублей/потерь
- [ ] Интеграция: частичный/сбойный INSERT → fatal → **ненулевой код выхода** → рестарт → нет дублей/потерь
- [ ] Интеграция: **ошибка синка в writer-цикле НЕ глотается** → процесс выходит с ненулевым кодом (проверка что `drain_and_ack` пробрасывает `Err`)
- [ ] Интеграция: newline-splitter — N строк с одним offset; DLQ с ключом
- [ ] Интеграция (**I5/P3**): newline-Message из N строк (один offset) + kill mid-insert_many → рестарт → ровно N строк, без потерь и дублей (проверка границы native-блока на реальном CH)
- [ ] Интеграция: существующая таблица без колонок ключа → fatal на старте
- [ ] Интеграция: CH недоступен при `ensure_loaded` → fatal → процесс выходит с ненулевым кодом
- [ ] Старт: `ParallelChInsertSink` + exactly_once → fatal (§6.2)
- [ ] Старт: неподдерживаемый движок (ReplacingMergeTree, CollapsingMergeTree, SummingMergeTree, Buffer, etc.) → `FATAL` (§8/§9)
- [ ] Старт: Distributed-движок → деградация до `AT_LEAST_ONCE` + `WARN` (§8)
- [ ] Старт: CH < 22.8 → fatal
- [ ] Старт: число реплик < insert_quorum → fatal
- [ ] Юнит+интеграция: `to_sql_literal` = `unhex(hex(bytes))` для S3-ключа с `\` и `'` → `ensure_loaded` находит реальную строку (регрессия на CH: ключ с backslash матчится; без экранирования)
- [ ] Юнит: DLQ динамическая схема (YDS: `__system_partition Int64`; S3: `__system_filename Utf8`)

### Шаг 10 — Observability (TODO: отдельным PR)
- [ ] Метрики: `waterline_cache_hit` (counter), `waterline_rows_filtered` (counter), `waterline_rows_inserted` (counter), `waterline_guarantee_mode` (gauge: 1 = EXACTLY_ONCE, 0 = AT_LEAST_ONCE)
- [ ] Метрика/алерт: `waterline_regression_detected` (counter) — `ensure_loaded` вернул max < кешированного
- [ ] Лог режима гарантий на старте (§11)
