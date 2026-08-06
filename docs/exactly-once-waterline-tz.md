# Exactly-Once → CH: In-Memory Waterline + Composite Exactly-Once Key

## Техническое задание для реализации

> Версия 5. Четвёртый раунд ревью: 12 дыр из gap analysis —
> I5/Arrow IPC framing, ReplicatedMergeTree insert_quorum + select_sequential_consistency,
> S3 row number спецификация, LRU cap изменение, ручная миграция DEFAULT, CH-мутации,
> граница флеша аккумулятора, Replacing полный коллапс, backtick валидация,
> NULL offset, graceful drain таймаут, CH-source TODO.

---

## 0. Терминология и инварианты (читать первым)

**Ключ уникальности (uniqueness key)** — пара колонок в данных, идентифицирующая позицию
записи в источнике:
- `partition` — «пространство офсетов» (YDS-партиция `Int64`; S3 — имя файла `Utf8`);
- `offset` — монотонный офсет внутри партиции (`Int64`).

**Waterline** — in-memory кеш «максимальный **уже записанный** offset на партицию в таблице»,
`HashMap<WaterlineKey, i64>`, где `WaterlineKey = (Arc<str> /*table*/, PartitionKey)`.
Загружается лениво из ClickHouse. **Принадлежит синку, а синк — партиции** (YDS: свой синк
на каждую партицию; S3: один синк на пайплайн с много-ключевым LRU). Общего состояния между
партициями нет → лок не нужен (§4.1).

Инварианты, на которых держится корректность (нарушение любого = баг):

- **I1. Стабильность ключа.** Один и тот же логический source-record при реплее получает
  **тот же** `(partition, offset)`. Только **серверные** офсеты. Синтетических (счётчик на
  стороне читателя) офсетов не бывает — если источник не может дать стабильный offset, он
  не выставляет ключ и работает в at-least-once.
- **I2. Waterline растёт только на подтверждённом успехе.** `mark_committed` вызывается
  строго после успешного INSERT. Никакого продвижения «наперёд».
- **I3. Commit источника — только после полного успеха флеша.** Офсет в источнике коммитится
  строго после того, как **все** таблицы флеша (main + DLQ) записаны. **Механизм:**
  writer-таск для каждого батча формирует `TableWrite` для main и (если есть) DLQ,
  вызывает `sink.write(main)` затем `sink.write(dlq)`, и **только после успеха обоих**
  дёргает `source.commit()`. Отдельный коммит после каждого `TableWrite` — ошибка
  (при падении между ними потеряются строки незакоммиченной таблицы, см. §7).
- **I4. Сериализация и монотонность записи в пределах партиции.** Записи одной партиции
  сериализованы (следующий флеш не стартует до завершения предыдущего) и коммитятся строго
  по возрастанию offset — никакой старший offset не поднимается в waterline раньше, чем
  закоммичен младший. Waterline — скаляр (`max`), и опирается на это: конкурентная или
  out-of-order запись одной партиции = **потеря данных** (младшие offset'ы отфильтруются как
  «дубликаты», хотя не записаны). Сегодня обеспечивается моделью «одна партиция = один
  writer-таск = свой синк» (§4.1) и серийным writer'ом. Синки с out-of-order fan-out
  (напр. `ParallelChInsertSink`) с exactly-once **несовместимы** (§6.2).
- **I5. Атомарность группы `(partition, offset)`.** Все строки одной группы
  `(partition, offset)` пишутся в пределах **одного атомарного блока**; атомарная единица
  вставки никогда не дробит группу. Обеспечивается: offset пер-Message (P1) + весь Message
  в одном `RecordBatch` (P2) + блок = батч, сервер только склеивает блоки, не дробит (P3).
  Без I5 частичный сбой `insert_many` мог бы записать часть строк одного offset, поднять
  waterline до него и потерять остаток на реплее (§6.1).

  **Почему P3 выполняется:** Arrow IPC stream фреймит каждый `RecordBatch` как отдельное
  length-prefixed IPC-сообщение — на проводе это одна атомарная единица. ClickHouse при
  `FORMAT ArrowStream` принимает одно IPC-сообщение как один INSERT-блок и **не дробит**
  его: настройка `max_insert_block_size` применяется только к строковым форматам
  (TSV/CSV/JSONEachRow), для Arrow она неактивна. Клиент `clickhouse-arrow` внутри
  `insert_many` также не чанкует `RecordBatch` — каждый батч уходит ровно одним IPC-
  сообщением. Таким образом граница `RecordBatch` ≡ граница INSERT-блока ≡ граница
  атомарности. Проверка в реализации: **не включать**
  `input_format_arrow_allow_multiple_batches_in_one_block` (при появлении в CH) и
  **не разбивать** `RecordBatch` явно перед `insert_many`.

Из I2+I3: **любой неопределённый исход записи откатывается через рестарт** — waterline
пересоздаётся из ClickHouse, источник переотдаёт всё незакоммиченное, оно примиряется с
фактически записанным.

**Дополнительный инвариант плумбинга:** пустые батчи (0 rows) с `exactly_once_key = Some`
не доходят до синка — аккумулятор отсеивает их до flush.

**Предположение о владении партицией:** waterline — состояние **per-process**, не разделяемое
между воркерами. Корректность требует, чтобы в каждый момент партицию писал **ровно один
процесс**. Для YDS это обеспечивает **эксклюзивная аренда партиции ридером** (один консюмер на
партицию). Перекрытие двух процессов на одной партиции (rolling update без drain, смена
`total_workers` на лету) — за рамками гарантий, см. §13.

---

## 1. Суть подхода

Дедупликация на стороне Rust. Источник добавляет в каждую строку колонки составного ключа.
Синк держит in-memory `Waterline` и перед INSERT фильтрует строки с
`offset ≤ waterline(partition)`. Waterline для партиции подгружается из ClickHouse **лениво**
при первой встрече партиции и кешируется.

```
                    первая встреча партиции P
Каждый батч ──►  ensure_loaded(P): SELECT max(offset) WHERE partition=P  (разово, кешируется)
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
    pub offset:    ExactlyOnceColumn,  // Int64 → монотонный offset (YDS); Utf8 → low-cardinality filename (S3)
}

/// Значение ключа партиции — ключ HashMap'а waterline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    Int(i64),
    Str(String),  // low-cardinality implied (S3 filenames)
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

**Умолчания по источникам** (имена задаёт источник; переопределение имён — на будущее):

| Источник   | partition            | offset            | Тип partition        |
|------------|----------------------|-------------------|----------------------|
| YDS topic  | `__system_partition` | `__system_offset` | Int64                |
| YDS pqv1   | `__system_partition` | `__system_offset` | Int64                |
| S3         | `__system_filename`  | `__system_offset` | Utf8                 |
| CH-source  | (если есть стабильный ключ) | …          | по ситуации (TODO: не специфицирован в v4 — out of scope) |

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
партиция).

В `JsonParser::parse_into` (все 4 ветки `AllRootField`/`Mixed` × `NewLine`/`NoSplit`):
для **каждой успешно распарсенной** строки в лок-степ с data-колонками аппендить `partition`
и `msg.offset` в два дополнительных builder'а. Строки, уходящие в DLQ, в offset-колонку
**main** не аппендят (у DLQ свои колонки ключа — §7). Схема парсера (`arrow_schema`) при
`exactly_once` расширяется двумя полями ключа в конце.

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
    /// LRU-порядок для эвикта при переполнении cap (bounded память для S3-filenames).
    lru: /* bounded LRU */,
    cap: usize,
}

impl Waterline {
    /// Гарантирует, что waterline (таблицы, партиции) загружен в кеш. Разово ходит в CH.
    /// Double-checked: сначала быстрая проверка кеша (снаружи, до вызова), при промахе —
    /// этот async путь.
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
        if self.committed.contains_key(&wk) { return Ok(()); }
        let mut q = format!(
            "SELECT max({o}) FROM `{t}` WHERE {p} = {val} \
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
        // строку со значением 0 (проверено на MergeTree 25.4) → query_one_scalar дал бы Some(0),
        // а не None → первое сообщение партиции (offset 0) отфильтровалось бы. С `HAVING`:
        // пустая партиция → 0 строк → None. См. пояснение «Почему Option» ниже.
        let max: Option<i64> = client.query_one_scalar(&q).await?; // None ⟺ строк нет (HAVING отсёк)
        if let Some(m) = max {
            self.insert_lru(wk, m);
        }
        Ok(())
    }

    /// Максимальный записанный offset. `None` = не видели.
    #[inline]
    pub fn committed(&self, table: &Arc<str>, pid: &PartitionKey) -> Option<i64> {
        self.committed.get(&(table.clone(), pid.clone())).copied()
    }

    /// Обновление после успешного INSERT. Монотонно (max) — дешёвая страховка.
    #[inline]
    pub fn mark_committed(&mut self, table: &Arc<str>, pid: PartitionKey, offset: i64) {
        self.committed.entry((table.clone(), pid))
            .and_modify(|v| *v = (*v).max(offset))
            .or_insert(offset);
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

**`__system_offset = NULL` в данных:** если в таблице есть строки с `NULL` в колонке offset
(ручная вставка, миграция с незаполненной колонкой), `max(offset)` возвращает `NULL` (все
ненулевые значения > NULL, но функция `max` игнорирует NULL'ы и возвращает максимум среди
non-NULL; если все строки NULL — `max` возвращает NULL). `query_one_scalar` → `None` →
`ensure_loaded` не кеширует → партиция «не виденная» → все строки при реплее проходят —
безопасная деградация до at-least-once.

**Почему lazy, а не eager-скан на старте:** синк не знает списка партиций заранее в общем
случае (S3-файлы раскрываются в рантайме), и не должен зависеть от ключа/партиций до первого
батча. Ленивая загрузка единообразна для всех источников. Плата: на рестарте YDS — по одному
`SELECT max WHERE partition=P` на партицию (вместо одного `GROUP BY`); при типичном небольшом
числе партиций на воркер это дёшево (первый скан холодный, остальные тёплые из page-cache CH).
Если когда-нибудь упрёмся в «много партиций на воркер × огромная таблица» — добавим опциональный
bulk-preload одним `GROUP BY`.

**Эвикт (bounded-LRU) всегда корректен:** повторная встреча эвикнутой партиции → `ensure_loaded`
перечитает из CH; значение монотонно не меньше записанного (мы — единственный writer партиции,
ходим в конкретный хост → staleness нет) → ни дублей, ни потерь. Для YDS кеш мал и до
cap не доходит; эвикт реально работает только для потока S3-filenames.

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

**Лока нет.** Каждый синк принадлежит ровно одному writer-таску, а `write()` вызывается
строго последовательно (writer ждёт завершения каждого флеша перед следующим). Значит
`Waterline` — single-owner: конкурентного доступа к нему не бывает, никакой `Mutex`/`RwLock`
не нужен. `&mut self` достаточно.

**Cross-partition гонок нет by construction.** Раз waterline у каждой партиции свой,
исчезает гонка «эвикт одной партиции ломает чтение другой»: LRU-эвикт вообще возможен
только внутри одного S3-синка, где writer единственный и серийный, т.е. и там конкуренции
нет.

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
        ensure_loaded(w.table, pid)                 # разово для (таблица, партиция)
        wl = waterline.committed(w.table, pid)          # Option<i64> (без лока — §4.1)

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
            insert_rows(w.batches, rows)            # собираем RecordBatch из отобранных row_idx
            waterline.mark_committed(w.table, pid, max_off)
            continue

        # 5.b.3: частичное перекрытие — фильтрация
        # Формируем mask по колонке offset: offset > waterline
        keep_mask = gt(offset_col, wl.unwrap())
        filtered_rows = [r for r in rows if keep_mask[r.row_idx]]
        if filtered_rows not empty:
            insert_rows(w.batches, filtered_rows)
        # mark_committed от исходного max_off вызывается ВСЕГДА — даже если
        # filtered_rows пуст (все строки ≤ waterline, как в 5.b.1). Это поднимает
        # waterline → на следующем реплее батч уйдёт в 5.b.1 (continue без лишней
        # фильтрации). Асимметрия с 5.b.1 (где mark_committed не вызывается)
        # объясняется тем, что в 5.b.1 waterline УЖЕ ≥ max_off, а здесь — нет.
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
- **5.b.3**: дорогой путь (`compute::gt` + `filter_record_batch` + сборка новых батчей),
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
```

Аналогично в `ensure_loaded` для SQL: `Int` подставляется как число, `Str` — как
`unhex('<hex>')` (hex-кодирование байтов, без экранирования — см. §2 и заметку про драйвер
в §13). Exposed как `PartitionKey::to_sql_literal(&self) -> String`.

---

## 6. Fail-fast при ошибке INSERT или `ensure_loaded` + poisoning-обёртка

Waterline — скаляр на партицию; он **не умеет** выразить «дыру» в диапазоне (записаны
[40..45] и [48..50], но не [46..47]). `insert_many` пишет несколько блоков **неатомарно** —
частичный сбой оставил бы дыру. **Решение: любая ошибка `ensure_loaded` или `INSERT` инвалидирует синк и роняет процесс**

```
INSERT error
   → poison(sink)                 # синк больше не примет ни одной записи (§6.1)
   → fatal error пайплайна
   → процесс выходит с ненулевым кодом
   → супервизор (k8s/systemd) перезапускает
   → waterline пуст; ensure_loaded перечитает актуальный max из CH (source of truth)
   → источник переотдаёт незакоммиченное (I3) → примиряется с waterline
```

Разбор «[40..45] записан, [46..50] упал»: маркер не закоммичен (I3) → источник переотдаёт с
offset < 40 → `ensure_loaded` даёт waterline=45 → [40..45] фильтруются, [46..50] вставляются.
Ни потери, ни дубля.

**Разбор корректен в силу I5:** граница блока всегда совпадает с границей группы
`(partition, offset)` — весь Message в одном `RecordBatch` = одном Arrow IPC-сообщении =
одном INSERT-блоке (см. обоснование P3 в §0). Частичный сбой `insert_many` теряет только
**целые** блоки, не фрагменты offset'а. Поэтому «поднять waterline до частично записанного
offset» невозможно: либо весь блок записан → `mark_committed` вызывается, либо нет → ошибка
→ waterline не сдвинут.

**Удаляется из текущего кода:** in-process ретрай партиции с переиспользованием того же синка
(`main.rs:53-82`, до 5 попыток) — на нём дыра выживала бы. Ошибка синка = сразу fatal.

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

In-process enforcement инварианта «после ошибки записи синк не зовётся»; выход процесса — уже
восстановление.

### 6.3. Graceful drain in-flight операций при exit

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

Вывод: graceful drain — best-effort (уменьшает холостую работу на рестарте), но
корректность гарантирована в любом случае.

**Поведение при превышении таймаута drain:** если in-flight flush'ы не завершились за
отведённое время (например, 30 сек) → форсированный выход: `WARN` в лог
(«graceful drain timed out with N pending flushes — forcing exit, correctness unaffected»)
→ `exit(1)`. Незавершённые INSERT'ы либо закоммичены в CH (тогда `max(offset)` покроет
их при рестарте), либо отброшены сервером (тогда источник переотдаст). Оба варианта
корректны — см. выше.

### 6.2. `ParallelChInsertSink` несовместим с exactly-once

`ParallelChInsertSink` (`middleware/parallel_ch_insert.rs`) раскидывает записи round-robin по
N пулам и в докстринге прямо декларирует *«assumes all keys unique, parallel **out-of-order**
inserts»*. Под exactly-once это ломается дважды:
- **нарушает I4**: out-of-order вставки одной партиции ⇒ waterline (скаляр `max`) занизит/
  переставит порядок ⇒ потеря;
- допущение «все ключи уникальны» **ложно**: newline-splitter даёт несколько строк с одним
  offset, а реплей повторяет offset'ы.

Кроме того, его единственный механизм exactly-once — `SET insert_deduplication_token`, который
Шаг 1 чеклиста **удаляет**. Поэтому: при `add_exactly_once_key: true` **и** sink =
`ParallelChInsertSink` → **fatal на старте** («parallel insert sink несовместим с exactly-once;
используйте clickhouse sink или отключите exactly_once»). Waterline-дедуп предполагает серийный
пер-партиционный синк (I4/§4.1).

---

## 7. DLQ и exactly-once

DLQ дедуплицируется **тем же ключом**, что и main. Синк не различает `events` и `events.dlq` —
обе получают `TableWrite` с `exactly_once_key` и проходят waterline-проверку по своему
(независимому) состоянию.

- В `DLQ_SCHEMA`/`DLQ_CH_COLUMNS` (`json_parser.rs:526-542`) добавляются колонки ключа
  (`__system_partition`, `__system_offset`). Существующий `partition_id` **убирается**
  (заменяется на `__system_partition`).
- `dlq_payloads` (`json_parser.rs:692`) расширяется до
  `Vec<(Bytes, DlqReason, i64 /*offset*/, PartitionKey)>` — offset берётся из текущего Message.

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
миграции), но требует явной проверки существующих таблиц.

При `add_exactly_once_key: true`:

1. **`CREATE TABLE IF NOT EXISTS`** — колонки ключа включены **в определение** таблицы
   (`__system_partition Int64` / `__system_offset Int64`; для S3
   `__system_filename LowCardinality(String)`). Свежая таблица создаётся корректной.
2. **verify после create** (`DESCRIBE`): убедиться, что таблица (возможно, уже существовавшая)
   реально содержит колонки ключа нужного типа. Если нет → **fatal**:
   ```
   table 'events' exists without exactly-once key columns (__system_partition, __system_offset).
   Recreate the table with these columns or disable exactly_once. Auto-migration is not performed.
   ```
   Миграцию существующей таблицы делает пользователь осознанно; молча не чиним.
3. **Никаких `ALTER TABLE ADD COLUMN`.** Даже если пользователь делает это вручную —
   **опасность DEFAULT:** `ALTER TABLE ADD COLUMN __system_offset Int64 DEFAULT 0` заполнит
   все легаси-строки значением `0` → `ensure_loaded` вернёт `Some(0)` → первое настоящее
   сообщение с offset 0 будет отфильтровано и **потеряно**. Безопасный путь миграции:
   создать новую таблицу с колонками ключа в `CREATE TABLE` и перезалить данные, либо
   использовать `DEFAULT -1` (offset'ы неотрицательны, waterline просядет до -1 → все строки
   пройдут). Проверка при старте **не покрывает** этот случай (колонки есть, типы верны), поэтому
   документируем.
4. То же для DLQ-таблицы (её всегда создаём мы → колонки будут).
5. **Проверка движка:**
   - `MergeTree` → ок
   - `ReplicatedMergeTree` → проверить `insert_quorum` (из `system.tables`): если `< 2` **и** пользователь не установил `replicated_insert_quorum_override` в конфиге синка → **`FATAL`** («ReplicatedMergeTree requires insert_quorum ≥ 2 for exactly-once. Set it in the table definition or set sink.replicated_insert_quorum_override: 1 to degrade to at-least-once (duplicates possible on replica failover — see §13).»). Если `replicated_insert_quorum_override: 1` → `WARN` + гарантия деградирует до `AT_LEAST_ONCE`
   - `Distributed` → **деградация до `AT_LEAST_ONCE`** + `WARN` («Distributed table detected: async block forwarding breaks waterline consistency → exactly-once disabled. Write directly to the underlying MergeTree table instead.»)
   - **Любой другой движок** (`ReplacingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `Buffer`, etc.) → **`FATAL`** («engine '{}' is not supported for exactly-once. Only MergeTree and ReplicatedMergeTree are supported.»). Схлопывающие/суммирующие движки арифметически изменяют или схлопывают `__system_offset` → waterline необратимо сломан. Никаких корнеркейсов — просто запрещены
   - В `engine_full` найдена `TTL`-клауза → `INFO` («TTL will delete key columns → waterline may be lowered for affected offsets; at-least-once on replayed tail»)
6. **Проверка имени таблицы:**
   - Backtick (`` ` ``) в имени → **fatal** («table name contains backtick — incompatible with exactly-once SQL queries»).
   - Точка (`.`) в имени → **fatal** («table name contains '.': use the ClickHouse connection default database instead of `db.table` syntax»). CH-клиент уже подключён к целевой БД через DSN — квалифицировать таблицу базой в имени не нужно и вредно: `` FROM `db.table` `` — это одно имя с точкой, а не `db`.`table`.
   
   Имена таблиц из конфига, не из пользовательских данных → риск низкий, но валидация дёшева и страхует от SQL-ошибок в `ensure_loaded`.

Итог: **отсутствие колонок ключа → fatal**; **неподдерживаемый движок → fatal**; **backtick/точка в имени таблицы → fatal**.

---

## 9. Поддерживаемые движки ClickHouse и retention

`ensure_loaded` (`SELECT max(offset) WHERE partition=P`) корректен только на движках, где строки
**не мутируют и не схлопывают числовые колонки**. Поддерживаются ровно два движка:

| Движок | Статус | Комментарий |
|--------|--------|-------------|
| `MergeTree` | ✅ | Строки не схлопываются → `max(offset)` = истинный максимум. `FINAL` не нужен. |
| `ReplicatedMergeTree` | ⚠️ | Строки не схлопываются, **но** при `insert_quorum=1` (дефолт) и отказе реплики между INSERT и рестартом — записанные строки могут не успеть асинхронно реплицироваться на другие реплики. При рестарте на другой реплике `ensure_loaded` вернёт заниженный `max(offset)` → дубли. **Требуется `insert_quorum ≥ 2`** — стартовый `FATAL` при меньшем значении; пользователь может явно понизить через `replicated_insert_quorum_override` → деградация до `AT_LEAST_ONCE` (см. §8, §11). `select_sequential_consistency = 1` в `ensure_loaded` даёт дополнительную страховку при чтении с другой реплики после рестарта (для не-Replicated таблиц SETTINGS игнорируется сервером → накладных расходов нет). |
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

Если exactly-once поднимут на мульти-реплике с чтением через **балансировщик** (вместо конкретного
хоста) — `select_sequential_consistency` не поможет, т.к. балансировщик может увести запрос на
отставшую реплику; это **вне гарантий** и требует кворумной записи (`insert_quorum ≥ 2`) как
основной защиты. Экзотика с `Distributed`-шардированием не по партиции — тоже §13.

**Про retention/TTL:** waterline хранит состояние дедупа **в тех же данных, что чистятся** —
TTL/`TRUNCATE`/`DROP PARTITION` в ClickHouse не влияют на корректность. Источник переотдаёт
только незакоммиченный хвост (от last committed offset), и если retention источника настроен
корректно, TTL-чистка ClickHouse не пересекается с окном реплея. Пересечение возможно только
при агрессивном TTL (короче retention источника) — тогда waterline занизится, дубли пройдут →
at-least-once на затронутых офсетах. Это ожидаемое поведение, специальной защиты не требуется.
| CH-мутации по системным колонкам | `ALTER TABLE ... UPDATE __system_offset = ...` или `DELETE WHERE __system_partition = ...` искажают `max(offset)` → waterline необратимо сломан → дубли или потеря | Не выполнять мутации, затрагивающие `__system_offset`/`__system_partition`, на exactly-once таблицах |

---

## 10. Стабильные офсеты по источникам (инвариант I1)

| Источник | Источник offset | Стабилен? |
|----------|-----------------|-----------|
| YDS pqv1 | `message_data.offset` (`pq_v1.rs:331`) — серверный офсет Logbroker | ✅ |
| YDS topic | `TopicReaderMessage.offset` + `get_partition_id()` (ydb 0.13.5, `topicreader/messages.rs:99,129`) | ✅ |
| S3 | `__system_filename` + row number (начиная с 0 в каждом файле) | ✅ при неизменном файле (см. §10.1) |
| CH-source | стабильный ключ если есть; иначе ключ не выставляется | — |

**Проброс offset (YDS):**
- pqv1: добавить `offset` в `DecodedMessage` (сейчас `data`+`cookie`, `pq_v1.rs:128`) →
  `Message.offset`; `partition = Int(partition_id)`.
- topic: читать `msg.offset` и `msg.get_partition_id()` в `ydb_topic.rs:84-88` (сейчас
  отбрасываются) → `Message`.

**Синтетических офсетов нет.** Источник без стабильного серверного offset ключ не выставляет →
at-least-once (§11).

### 10.1. Доработка S3-источника (для exactly-once)

Сейчас (`s3/source.rs`) один `S3Source` листит все файлы под префиксом и идёт по ним
последовательно с единым `partition_id`; `Message` несёт только `value`, имя файла и офсет не
прокидываются. Чтобы S3 получил exactly-once:

- **`__system_filename`** = `self.files[current_idx].location` — имя текущего файла, кладётся в
  `Message.partition = Str(filename)`.
- **`__system_offset`** = номер строки от начала файла (row number, начиная с 0). Инкрементится
  для каждой **полной** строки после сборки из чанков. Разделитель строк — newline (`\n`).
  `safe_split_at` на границе чанка переносит остаток разорванной строки в следующий чанк;
  счётчик строк инкрементится только после того, как все части строки склеены, — поэтому
  row number **не зависит** от границ чанков и размера буфера, в отличие от байтовой позиции.
  Счётчик сбрасывается в 0 при переходе к следующему файлу.
- **Чтение всегда с начала файла.** При exactly-once S3-источник **не может** возобновлять
  чтение с середины файла (байтовая позиция после рестарта) — это сломало бы row number и
  нарушило I1. Вместо этого файл перечитывается с начала, а waterline отфильтровывает уже
  записанные строки (5.b.1/5.b.3). Цена — повторное скачивание файла при рестарте; это
  корректно, пока retention источника ≥ времени обработки файла.
- **Пустые файлы пропускаются.** S3-источник при обнаружении файла нулевого размера не создаёт
  `Message`-ов, не выставляет ключ и переходит к следующему файлу. Waterline для пустого файла не
  загружается — это корректно (нечего дедуплицировать).
- Файл под тем же именем не должен переписываться другим содержимым (иначе row number
  перестают соответствовать записям) — I1.

Пока эта доработка не сделана, S3 работает в at-least-once (ключ не выставлен, §11). Архитектура
(ключ, кеш, флаги) под неё уже заложена.

---

## 11. Вывод режима гарантий на старте

Режим выводится из `(источник выставляет ключ?) && (синк умеет waterline-dedup?) && (ReplicatedMergeTree quorum OK?) && (не Distributed?)` и **логируется**:

- `EXACTLY_ONCE`: источник выставил ключ И синк = clickhouse И (движок ≠ ReplicatedMergeTree ИЛИ `insert_quorum ≥ 2`)
- `AT_LEAST_ONCE`: иначе — с actionable-подсказкой, что включить / исправить

```
Guarantee mode: EXACTLY_ONCE  (key: __system_partition + __system_offset, sink: clickhouse, engine: MergeTree)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: ReplicatedMergeTree with insert_quorum < 2 and no replicated_insert_quorum_override
  → to enable EXACTLY_ONCE: ALTER TABLE ... MODIFY SETTING insert_quorum = 2
    OR set sink.replicated_insert_quorum_override: 1 to accept risk (duplicates on replica failover, §13)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: source 'topic' has exactly_once disabled
  → to enable EXACTLY_ONCE set source.<...>.add_exactly_once_key: true
    (adds __system_partition/__system_offset; table must be created with these columns)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: sink 'yds' does not support offset-based dedup
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: user set replicated_insert_quorum_override: 1 (risk accepted)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: table 'events_dist' has engine Distributed — async block forwarding breaks waterline consistency
  → to enable EXACTLY_ONCE: write directly to the underlying MergeTree table instead
```

At-least-once — легитимный режим: ключ не выставлен → синк делает обычный INSERT.

---

## 12. Что происходит при рестарте (сводно)

```
1. Падение / выход процесса (в т.ч. fail-fast по ошибке INSERT, §6)
2. Источник не получил commit для незакоммиченных офсетов (I3)
3. Рестарт процесса:
   a. create_tables: CREATE ... с колонками ключа; verify-или-fatal при их отсутствии (§8)
   b. Проверка движка/TTL → WARN при необходимости (§8/§9)
   c. Спавн партиционных задач; waterline ПУСТ
4. Источник переотдаёт с last_committed+1
5. Первый батч каждой партиции → ensure_loaded(P) читает max(offset) из CH
   (чтение с конкретного хоста), кладёт в кеш
6. Дубликаты фильтруются (5.b.1/5.b.3), новое пишется (5.b.2)
```

Стоимость: по одному `SELECT max WHERE partition=P` на партицию при её первой встрече
(§4). Bulk-preload одним `GROUP BY` — опциональная будущая оптимизация, если понадобится.

---

## 13. Известные ограничения

| Ограничение | Пояснение | Митигизация |
|-------------|-----------|-------------|
| CH-retention ≥ source-retention | TTL/TRUNCATE/DROP PARTITION занижают waterline, если чистка затронула офсеты в окне реплея → at-least-once на затронутых офсетах. Источник переотдаёт только незакоммиченный хвост, поэтому при стандартных настройках retention пересечения нет | Не настраивать TTL короче retention источника; при нормальной эксплуатации проблема не возникает |
| Существующая таблица без колонок ключа | `ADD COLUMN` не делаем; ручное `ALTER TABLE ADD COLUMN ... DEFAULT 0` создаст фантомные offset=0 → потеря первого настоящего сообщения | Fatal на старте (§8); миграция — только новым CREATE TABLE или `DEFAULT -1` |
| Схлопывающие/суммирующие движки | Summing/Aggregating арифметически изменяют `__system_offset` | Стартовый `FATAL` |
| Distributed-шардирование не по партиции | Партиция размазана по шардам → `max(offset)` неконсистентен | Модель «партиция→один воркер→одна таблица»; экзотику не поддерживаем |
| Мульти-реплика: отказ реплики между INSERT и рестартом | INSERT на реплику A с `insert_quorum=1` → OK, `mark_committed(P, 42)`, реплика A падает до асинхронной репликации на B/C. Процесс рестартует на реплике B (A недоступна) → `ensure_loaded` → `max` = 35 (не 42!) → waterline занижен → источник переотдаёт [36..42] → **дубли**. Сценарий: (1) INSERT на A, (2) `mark_committed`, (3) A падает до репликации, (4) рестарт → B, (5) `max` занижен → дубли | **По дефолту FATAL при `insert_quorum < 2`** (§8). Пользователь может явно понизить через `replicated_insert_quorum_override: 1` → деградация до `AT_LEAST_ONCE`. `select_sequential_consistency = 1` в `ensure_loaded` страхует от staleness при чтении с другой реплики. Надёжнее всего: не-Replicated таблицы (данные и так реплицируются на уровне Kafka/YDS) |
| Мульти-реплика: перманентный отказ **кворумных** реплик | `insert_quorum = 2`, 3 реплики A,B,C. INSERT подтверждён A+B → `mark_committed`. A и B **оба** перманентно падают до репликации на C → рестарт процесса на C → `ensure_loaded` + `select_sequential_consistency = 1` на C — данные с A/B никогда не придут → `max(offset)` занижен → источник переотдаёт → **дубли**. Сценарий экстремальный: потеря 2 из 3 реплик одновременно — проблема уровня ClickHouse, не exactly-once | `select_sequential_consistency` бессилен при перманентном отказе кворума; деградация до `AT_LEAST_ONCE` на затронутых офсетах. Восстановление: ручной `UNDROP` реплики, либо принять дубли и положиться на ReplacingMergeTree/`FINAL` для зачистки |
| Перекрытие записи одной партиции двумя процессами | Waterline — per-process, не общий. Rolling update без drain или смена `total_workers` на лету могут дать окно, когда старый и новый процесс пишут одну партицию → дубли (CH их не отсекает, dedup_token удалён) | Штатно закрыто **эксклюзивной арендой партиции YDS-ридером** (один консюмер на партицию, §0). Остаточный риск — только в окне перекрытия при деплое: drain старого процесса перед стартом нового (k8s: preStop/`maxUnavailable`), не менять `total_workers` без полной остановки |
| S3: переписывание файла под тем же именем | Row number перестаёт соответствовать записям | I1: файлы иммутабельны под своим именем |
| Много партиций на воркер × огромная таблица | N ленивых сканов на рестарте | Обычно N мало; при необходимости — bulk-preload одним `GROUP BY` |
| Crash-loop при флапающем CH | Любая ошибка `ensure_loaded` или `INSERT` → poison → fatal → рестарт → снова ошибка → цикл. Каждый рестарт = новые ленивые запросы + повторная попытка вставки | Безопасно (данные не портятся); backoff супервизора (k8s: `restartPolicy: OnFailure` + `backoffLimit`). CH-данные — source of truth; каждый цикл читает актуальное состояние |
| Две системные колонки в `SELECT *` | ~доли % overhead | `SELECT * EXCEPT(__system_*)` / документирование |
| `clickhouse-arrow 0.2.1`: binding параметров экранирует только `'`, не `\` | `encode_field_dump` (`query.rs:161`) + сам `{p:String}` разэкранирует значение → backslash в S3-filename терялся бы | Не используем binding для ключа; подставляем `unhex('<hex>')` (§2) — экранирование не нужно вовсе. Проверено на CH 25.4 |
| `Distributed`-движок | Distributed принимает INSERT и асинхронно пересылает блоки на шарды. `ensure_loaded` читает из Distributed и не видит блоки, ещё не доставленные на целевые шарды → waterline занижен → дубли | Стартовая деградация до `AT_LEAST_ONCE` + `WARN` (§8). Exactly-once с Distributed невозможен: пишите напрямую в целевую MergeTree |
| Неподдерживаемый движок (ReplacingMergeTree, CollapsingMergeTree, SummingMergeTree, Buffer, etc.) | Схлопывают/суммируют/модифицируют `__system_offset` → waterline необратимо сломан | Стартовый `FATAL` (§8). Поддерживаются только MergeTree и ReplicatedMergeTree (§9) |
| `ON CLUSTER` DDL | `CREATE TABLE IF NOT EXISTS` без `ON CLUSTER` создаст таблицу только на одной реплике. На кластерных инсталляциях пользователь должен создать таблицу на всех репликах самостоятельно | TODO: поддержка `ON CLUSTER` в конфиге — будет добавлена отдельно. Пока что DDL через `ON CLUSTER` — зона ответственности пользователя; verify-проверка колонок ключа поймает отсутствие таблицы на реплике с читаемым fatal |

---

## 14. Чеклист реализации

### Шаг 1 — Типы ключа
- [ ] `ExactlyOnceColumn { name }`, `ExactlyOnceKey { partition, offset }`, `PartitionKey { Int, Str }` в `types/`
- [ ] Зависимость `hex` (для `PartitionKey::Str::to_sql_literal` → `unhex('<hex>')`, §2); без ручного экранирования
- [ ] `Message`: `offset: Option<i64>`, `partition: Option<PartitionKey>`
- [ ] `TableData`/`TableWrite`: `exactly_once_key: Option<ExactlyOnceKey>`. Никаких `min_offset`/`max_offset`/`single_partition`/`partition`. Поле `dedup_token` **удалить**.
- [ ] Удалить старый механизм: `compute_dedup_token`, `SET insert_deduplication_token`, `non_replicated_deduplication_window` из DDL

### Шаг 2 — Источники (стабильный offset, I1)
- [ ] pqv1: `offset` в `DecodedMessage` → `Message.offset`; `partition = Int(partition_id)`
- [ ] topic: `msg.offset` + `get_partition_id()` → `Message`
- [ ] Флаг `add_exactly_once_key: bool` в конфиге источника; при `false` ключ не выставляется
- [ ] Валидация: запрет пользовательских колонок с префиксом `__`
- [ ] (S3, отдельная задача §10.1) `__system_filename` + row number → `Message`; выставить ключ

### Шаг 3 — Парсер
- [ ] `JsonParser::new`: при exactly_once расширить `arrow_schema` колонками ключа
- [ ] `parse_into`: per-row `offset` + const `partition` во все 4 ветки
- [ ] **Защита от коллизии имён (§2):** перед созданием батча проверить, что `__system_partition`/`__system_offset`/`__system_filename` отсутствуют среди имён data-колонок; при конфликте → fatal с читаемым сообщением
- [ ] DLQ: колонки ключа в `DLQ_SCHEMA`/`DLQ_CH_COLUMNS`; `dlq_payloads` тащит offset+partition

### Шаг 4 — Аккумулятор
- [ ] `BatchAccumulator`: **только копит батчи**, без агрегации метаданных. `TableWrite` содержит только `batches` и `exactly_once_key`
- [ ] **Граница флеша = граница Message** (§3.1): flush только после полного `RecordBatch` одного Message; при лимите размера — текущий Message завершается и флешится целиком; слишком большой Message → fatal
- [ ] Пустые батчи (0 rows) с ключом отсеиваются, не доходят до синка

### Шаг 5 — Waterline (lazy)
- [ ] `Waterline { HashMap<(Arc<str>, PartitionKey), i64>, bounded-LRU, cap }`: `committed(table, pid)->Option`, `mark_committed(table, pid, offset)`, `ensure_loaded(table, pid)`
- [ ] `ensure_loaded`: `SELECT max(offset) WHERE partition=P HAVING count()>0 SETTINGS select_sequential_consistency = 1` (SETTINGS игнорируется не-Replicated таблицами → без накладных расходов; для Replicated страхует от staleness при чтении с другой реплики после рестарта, §4/§13); отдельный loaded-set. **`HAVING count()>0` обязателен** — иначе CH вернёт `0` (не пусто) на несуществующей партиции → `Some(0)` вместо `None` → потеря offset 0 (§4)
- [ ] Waterline — **приватное поле пер-партиционного синка**, single-owner, **без `RwLock`/`Mutex`**. `&mut self` достаточно — writer серийный (§4.1)
- [ ] Синк НЕ инициализирует waterline на старте

### Шаг 6 — Синк: запись
- [ ] Нет ключа → обычный INSERT (at-least-once)
- [ ] `group_by_partition`: сгруппировать строки по значению partition-колонки (читать колонку из RecordBatch)
- [ ] Для каждой группы: `ensure_loaded` → `min_off`/`max_off` из колонки offset → 5.b.1/5.b.2/5.b.3
- [ ] `mark_committed` от исходного `max_off` группы (не отфильтрованного)
- [ ] `insert_rows`: собрать `RecordBatch` из подмножества отобранных row_idx
- [ ] **I5**: не дробить `RecordBatch` на под-блоки мельче Message (клиент не делает, сервер не делает для Arrow — см. обоснование P3 в §0); не включать `input_format_arrow_allow_multiple_batches_in_one_block`

### Шаг 7 — Fail-fast + poisoning
- [ ] **Синк строится на партицию**: убрать единый `build_sink()` из `main.rs` до цикла; строить синк внутри спавна на каждую `pid` (YDS → N синков; S3 → один синк на пайплайн). `deps` больше не несёт готовый `snk` — вместо этого `main.rs` создаёт CancellationToken для глобального выключения и передаёт его в партиционные таски; каждый таск сам создаёт свой синк (коннект + Waterline) при старте
- [ ] `PoisoningSink` (`AtomicBool`): флаг **per-sink** (приватный для каждого экземпляра); глобальное выключение при отказе партиции — task supervisor (CancellationToken / abort JoinHandle) (§6.1)
- [ ] Ошибка INSERT → poison → fatal → **процесс выходит с ненулевым кодом** (сейчас `main` возвращает `Ok(())` даже при фатале — исправить: фатал таска → `Err`/`exit(1)`, иначе супервизор не рестартует)
- [ ] Убрать in-process ретрай синка в `main.rs` (`spawn_partition_task`, retry до 5); commit источника только после полного успеха флеша (I3)

### Шаг 8 — DDL и стартовые проверки
- [ ] `CREATE TABLE` с колонками ключа (main + DLQ), только при exactly_once; **без `ADD COLUMN`**
- [ ] verify колонок ключа → **fatal** при отсутствии
- [ ] Проверка движка/TTL через `system.tables` → `WARN`
- [ ] `ReplicatedMergeTree` + `insert_quorum < 2` и нет `replicated_insert_quorum_override` → **`FATAL`**; с override → `WARN` + деградация до `AT_LEAST_ONCE` (§8, §11, §13)
- [ ] Конфиг синка: поле `replicated_insert_quorum_override: Option<u8>` — явное понижение кворума с принятием риска (§8). Допустимое значение: только `1` (любое другое → ошибка конфига)
- [ ] `Buffer`-обёртка → `WARN` (§13)
- [ ] `add_exactly_once_key: true` + sink = `ParallelChInsertSink` → **fatal** (несовместим, §6.2)
- [ ] Вывод и лог режима гарантий (§11)

### Шаг 9 — Тесты
- [ ] Юнит: `committed`/`mark_committed`; `None` vs `Some(0)` (offset 0 не теряется)
- [ ] Интеграция: **`ensure_loaded` на несуществующей партиции возвращает `None`, а не `Some(0)`** (проверка `HAVING count()>0`; регрессия на реальном CH — §4/#1)
- [ ] Юнит: `ensure_loaded` — загрузка/кеш/отсутствие партиции/эвикт+reload
- [ ] Юнит: фильтрация 5.b.1/5.b.2/5.b.3, single vs multi-partition
- [ ] Юнит: `PoisoningSink` — после Err не зовёт inner (per-sink poison); интеграция: отказ одной партиции → глобальное выключение через task supervisor → процесс выходит с ненулевым кодом
- [ ] Интеграция: запись → фильтрация дублей; рестарт → ленивая перезагрузка → нет дублей/потерь
- [ ] Интеграция: частичный/сбойный INSERT → fatal → **ненулевой код выхода** → рестарт → нет дублей/потерь
- [ ] Интеграция: newline-splitter — N строк с одним offset; DLQ с ключом
- [ ] Интеграция (**I5**): newline-Message из N строк (один offset) + сбой после части блоков → рестарт → ровно N строк, без потерь и дублей
- [ ] Интеграция: существующая таблица без колонок ключа → fatal на старте
- [ ] Интеграция: CH недоступен при `ensure_loaded` → fatal → процесс выходит с ненулевым кодом
- [ ] Старт: `ParallelChInsertSink` + exactly_once → fatal (§6.2)
- [ ] Юнит+интеграция (**#7**): `to_sql_literal` = `unhex(hex(bytes))` для S3-filename с `\` и `'` → `ensure_loaded` находит реальную строку (регрессия на CH: имя с backslash матчится; без экранирования)
- [ ] Старт: неподдерживаемый движок (ReplacingMergeTree, CollapsingMergeTree, SummingMergeTree, Buffer, etc.) → `FATAL` (§8/§9)
- [ ] Старт: Distributed-движок → деградация до `AT_LEAST_ONCE` + `WARN` (§8)

### Шаг 10 — Observability (TODO: отдельным PR)
- [ ] Метрики: `waterline_cache_hit` (counter), `waterline_rows_filtered` (counter), `waterline_rows_inserted` (counter), `waterline_guarantee_mode` (gauge: 1 = EXACTLY_ONCE, 0 = AT_LEAST_ONCE)
- [ ] Лог режима гарантий на старте (§11)
```
