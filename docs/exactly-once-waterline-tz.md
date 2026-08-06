# Exactly-Once → CH: In-Memory Waterline + Composite Exactly-Once Key

## Техническое задание для реализации

> Версия 3. Переработана после двух раундов ревью. Механизм exactly-once обобщён с
> «YDS partition+offset» до **произвольного составного ключа уникальности**, задаваемого
> источником. Waterline загружается **лениво** из ClickHouse (source of truth); ошибка
> вставки инвалидирует синк и роняет процесс (восстановление — через рестарт).

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
  строго после того, как **все** таблицы флеша (main + DLQ) записаны.
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
| CH-source  | (если есть стабильный ключ) | …          | по ситуации          |

**Именование:** префикс `__` зарезервирован под системные колонки; пользовательские колонки
с `__` запрещаются валидацией конфига.

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
    /// Double-checked: сначала read-lock проверка (снаружи), при промахе — этот async путь.
    async fn ensure_loaded(
        &mut self,                     // single-owner; interior lock (если есть) не держим через await — §4.1
        client: &Client,
        table: &Arc<str>,
        key: &ExactlyOnceKey,
        pid: &PartitionKey,
    ) -> anyhow::Result<()> {
        let wk: WaterlineKey = (table.clone(), pid.clone());
        if self.committed.contains_key(&wk) { return Ok(()); }
        let q = format!(
            "SELECT max({o}) FROM `{t}` WHERE {p} = {val} \
             HAVING count() > 0",
            o = key.offset.name, t = table, p = key.partition.name,
            val = pid.to_sql_literal(),
        );
        // Ходим в конкретный хост (не Distributed, не мульти-реплика) → staleness между
        // репликами невозможен, поэтому select_sequential_consistency не нужен. Мульти-хост
        // Replicated — вне гарантий (§13).
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

### 4.1. Владение синком и waterline (по партиции, без общего лока)

**Один синк на партицию.** Для YDS каждая партиция получает **собственный** экземпляр
синка (свой коннект + свой `Waterline`). Синк строится **внутри спавна партиционного
таска** (по одному на `pid`), а НЕ один на воркер. Для S3 понятия партиции на уровне
пайплайна нет — один `S3Source` = один пайплайн = один синк, внутри которого `Waterline`
держит много ключей (по `filename`) с bounded-LRU.

**Общий на весь воркер — только poison-флаг** (`Arc<AtomicBool>`, §6.1), чтобы падение
одной партиции роняло весь процесс. Коннект и waterline — приватные для синка. (Пул
коннектов как общий ресурс — опциональная будущая оптимизация; в базовом дизайне один
коннект на партиционный синк, что согласовано с fail-fast: обрыв = фатал = рестарт.)

**Лока нет.** Каждый синк принадлежит ровно одному writer-таску, а `write()` вызывается
строго последовательно (writer ждёт завершения каждого флеша перед следующим). Значит
`Waterline` — single-owner: конкурентного доступа к нему не бывает, `RwLock` не нужен.
Достаточно interior mutability (напр. `tokio::Mutex`, фактически без контенции) с одним
правилом: **лок не удерживается через `.await`** в `ensure_loaded`
(lock → проверка → unlock → async-запрос в CH → lock → вставка).

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
`(partition, offset)` (весь Message — в одном `RecordBatch` = одном блоке), поэтому частичный
сбой `insert_many` теряет только **целые** группы, а не половину offset'а. Отсюда требование
к синку: **не дробить `RecordBatch` на под-блоки мельче Message** — не выставлять серверный
сплит блока и не чанковать батч по строкам; если `insert_many` чанкует, граница чанка обязана
совпадать с границей батча. Тогда «поднять waterline до частично записанного offset»
невозможно.

**Удаляется из текущего кода:** in-process ретрай партиции с переиспользованием того же синка
(`main.rs:53-82`, до 5 попыток) — на нём дыра выживала бы. Ошибка синка = сразу fatal.

**Компромисс (документируется):** при флапающем ClickHouse — crash-loop с повторными ленивыми
запросами. Безопасно (данные не портятся), шумно; лечится backoff супервизора.

### 6.1. `PoisoningSink`

Синк и waterline — на партицию (§4.1), но **poison-флаг общий на весь воркер**
(`Arc<AtomicBool>`, шарится во все партиционные синки). Если у партиции A запись упала,
партиции B/C **не должны** продолжать писать/двигать waterline/коммитить источник, пока
процесс умирает. Общий флаг `poisoned` мгновенно роняет записи всех партиций:

```rust
// poisoned — общий Arc на весь воркер (один и тот же клонируется во все партиционные синки).
pub struct PoisoningSink { inner: Arc<dyn Sink>, poisoned: Arc<AtomicBool> }

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

In-process enforcement инварианта «после ошибки записи синк не зовётся»; выход процесса — уже
восстановление.

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
переотдаёт незакоммиченное (I3); строки допишутся ровно туда, где их нет. При `exactly_once:
false` DLQ — как сейчас, без ключа.

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
3. **Никаких `ALTER TABLE ADD COLUMN`.**
4. То же для DLQ-таблицы (её всегда создаём мы → колонки будут).
5. **Проверка движка (fatal или warn):**
   - `SummingMergeTree` / `AggregatingMergeTree` → **fatal** (арифметически ломают `__system_offset`)
   - `ReplacingMergeTree` → проверить `ORDER BY` (из `system.tables`): если он **не содержит** колонок ключа `(__system_partition, __system_offset)` → **`WARN`** («Replacing схлопывает по ORDER BY; без ключа в ORDER BY возможна потеря разных логических строк — §9»); если содержит → `INFO` (waterline — оптимизация, Replacing — финальная защита)
   - `MergeTree` / `ReplicatedMergeTree` → ок
   - В `engine_full` найдена `TTL`-клауза → `WARN`

Итог: **отсутствие колонок ключа → fatal**; **движок/TTL → WARN**.

---

## 9. Поддерживаемые движки ClickHouse и retention

`ensure_loaded` (`SELECT max(offset) WHERE partition=P`) корректен на движках, где строки
**не мутируют и не схлопывают числовые колонки**.

| Движок | Статус | Комментарий |
|--------|--------|-------------|
| `MergeTree` / `ReplicatedMergeTree` | ✅ | Строки не схлопываются → `max(offset)` = истинный максимум. `FINAL` не нужен. Чтение с конкретного хоста (см. §13 про мульти-реплику). |
| `ReplacingMergeTree` (+Replicated) | ⚠️ | После фонового мержа `max(__system_offset)` может **занизиться** (Replacing схлопнул строки и оставил одну версию на каждый ORDER BY-ключ; у оставшейся — её оригинальный offset, не максимальный из схлопнутых). Waterline просядет → часть дубликатов просочится мимо фильтра. Replacing добивает их **только если `ORDER BY` уникально идентифицирует логическую запись** — тогда результирующий набор строк корректен. **Если ORDER BY грубее ключа — Replacing схлопнет РАЗНЫЕ логические строки → потеря данных.** Для exactly-once обязательно включать `(__system_partition, __system_offset)` (или иной уникальный на запись ключ) в `ORDER BY`. Waterline здесь — оптимизация (отсекает 99.9% реплеев); Replacing добивает остаток при корректном ORDER BY. |
| `SummingMergeTree` / `AggregatingMergeTree` | ❌ | Суммирующие/агрегирующие движки арифметически изменяют ВСЕ числовые колонки, включая `__system_offset` → waterline необратимо сломан. Стартовый `FATAL`. |

**Про `FINAL`:** не нужен. На plain MergeTree схлопывать нечего. На ReplacingMergeTree waterline — оптимизация, а Replacing даёт итоговую консистентность без участия `FINAL` в SELECT-ах.

**Про Replicated и staleness:** `ensure_loaded` ходит в **конкретный хост** (не Distributed,
не балансировщик реплик), поэтому чтение-своих-записей выполняется тривиально и staleness не
возникает — `select_sequential_consistency` **не используется**. Если exactly-once поднимут на
мульти-реплике с чтением через балансировщик, INSERT на реплику A + чтение с отставшей реплики B
может занизить `max(offset)` → дубли; это **вне гарантий** и требует кворумной записи
(`insert_quorum`) + `select_sequential_consistency` на чтении — см. §13. Экзотика с
`Distributed`-шардированием не по партиции — тоже §13.

**Про retention/TTL:** waterline хранит состояние дедупа **в тех же данных, что чистятся**.
Инвариант: **CH-retention ≥ retention источника** для exactly-once таблиц. Если TTL/`TRUNCATE`/
`DROP PARTITION` вытеснят строки, которые источник ещё может переотдать, waterline занизится →
деградация до at-least-once на затронутых офсетах. TTL на exactly-once таблице — не рекомендуется
(стартовый `WARN`).

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
  для каждой записи внутри файла. Детерминирован при неизменном файле и **не зависит** от
  границ чанков (в отличие от байтовой позиции, которая сдвигается при переменном размере чанка
  и `safe_split_at` с переносом остатка). Источник должен вести счётчик строк на файл и
  сбрасывать при переходе к следующему файлу.
- Файл под тем же именем не должен переписываться другим содержимым (иначе row number
  перестают соответствовать записям) — I1.

Пока эта доработка не сделана, S3 работает в at-least-once (ключ не выставлен, §11). Архитектура
(ключ, кеш, флаги) под неё уже заложена.

---

## 11. Вывод режима гарантий на старте

Режим выводится из `(источник выставляет ключ?) && (синк умеет waterline-dedup?)` и **логируется**:

```
EXACTLY_ONCE, если источник выставил exactly_once_key И синк = clickhouse.
AT_LEAST_ONCE иначе — с actionable-подсказкой, что включить.
```

Примеры:
```
Guarantee mode: EXACTLY_ONCE  (key: __system_partition + __system_offset, sink: clickhouse)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: source 'topic' has exactly_once disabled
  → to enable EXACTLY_ONCE set source.<...>.add_exactly_once_key: true
    (adds __system_partition/__system_offset; table must be created with these columns;
     engine MergeTree/ReplacingMergeTree)
```
```
Guarantee mode: AT_LEAST_ONCE
  reason: sink 'yds' does not support offset-based dedup
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
| CH-retention ≥ source-retention | TTL/TRUNCATE/DROP PARTITION занижают waterline → at-least-once на затронутых офсетах | Не использовать TTL на exactly-once таблицах; стартовый `WARN` |
| Существующая таблица без колонок ключа | `ADD COLUMN` не делаем | Fatal на старте (§8); миграцию делает пользователь |
| Схлопывающие/суммирующие движки | Summing/Aggregating арифметически изменяют `__system_offset` | Стартовый `FATAL` |
| Distributed-шардирование не по партиции | Партиция размазана по шардам → `max(offset)` неконсистентен | Модель «партиция→один воркер→одна таблица»; экзотику не поддерживаем |
| Мульти-реплика с чтением через балансировщик | INSERT на реплику A + чтение с отставшей реплики B → `max(offset)` занижен → дубли | **Вне гарантий.** Штатно: `ensure_loaded` ходит в **конкретный хост** → staleness нет, `select_sequential_consistency` не используется. Для мульти-реплики нужен `insert_quorum≥2` + `select_sequential_consistency=1` (конфликтует с «один коннект + fail-fast») |
| Перекрытие записи одной партиции двумя процессами | Waterline — per-process, не общий. Rolling update без drain или смена `total_workers` на лету могут дать окно, когда старый и новый процесс пишут одну партицию → дубли (CH их не отсекает, dedup_token удалён) | Штатно закрыто **эксклюзивной арендой партиции YDS-ридером** (один консюмер на партицию, §0). Остаточный риск — только в окне перекрытия при деплое: drain старого процесса перед стартом нового (k8s: preStop/`maxUnavailable`), не менять `total_workers` без полной остановки |
| S3: переписывание файла под тем же именем | Row number перестаёт соответствовать записям | I1: файлы иммутабельны под своим именем |
| Много партиций на воркер × огромная таблица | N ленивых сканов на рестарте | Обычно N мало; при необходимости — bulk-preload одним `GROUP BY` |
| Crash-loop при флапающем CH | Каждый рестарт = новые ленивые запросы | Безопасно; backoff супервизора |
| Две системные колонки в `SELECT *` | ~доли % overhead | `SELECT * EXCEPT(__system_*)` / документирование |
| `clickhouse-arrow 0.2.1`: binding параметров экранирует только `'`, не `\` | `encode_field_dump` (`query.rs:161`) + сам `{p:String}` разэкранирует значение → backslash в S3-filename терялся бы | Не используем binding для ключа; подставляем `unhex('<hex>')` (§2) — экранирование не нужно вовсе. Проверено на CH 25.4 |

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
- [ ] DLQ: колонки ключа в `DLQ_SCHEMA`/`DLQ_CH_COLUMNS`; `dlq_payloads` тащит offset+partition

### Шаг 4 — Аккумулятор
- [ ] `BatchAccumulator`: **только копит батчи**, без агрегации метаданных. `TableWrite` содержит только `batches` и `exactly_once_key`
- [ ] Пустые батчи (0 rows) с ключом отсеиваются, не доходят до синка

### Шаг 5 — Waterline (lazy)
- [ ] `Waterline { HashMap<(Arc<str>, PartitionKey), i64>, bounded-LRU, cap }`: `committed(table, pid)->Option`, `mark_committed(table, pid, offset)`, `ensure_loaded(table, pid)`
- [ ] `ensure_loaded`: `SELECT max(offset) WHERE partition=P HAVING count()>0` (ходим в конкретный хост → `select_sequential_consistency` не нужен, §9/§13); отдельный loaded-set. **`HAVING count()>0` обязателен** — иначе CH вернёт `0` (не пусто) на несуществующей партиции → `Some(0)` вместо `None` → потеря offset 0 (§4)
- [ ] Waterline — **приватное поле пер-партиционного синка**, single-owner, **без `RwLock`**. Interior mutability (`tokio::Mutex`) без контенции; лок не держать через `.await` (lock→проверка→unlock→запрос→lock→вставка)
- [ ] Синк НЕ инициализирует waterline на старте

### Шаг 6 — Синк: запись
- [ ] Нет ключа → обычный INSERT (at-least-once)
- [ ] `group_by_partition`: сгруппировать строки по значению partition-колонки (читать колонку из RecordBatch)
- [ ] Для каждой группы: `ensure_loaded` → `min_off`/`max_off` из колонки offset → 5.b.1/5.b.2/5.b.3
- [ ] `mark_committed` от исходного `max_off` группы (не отфильтрованного)
- [ ] `insert_rows`: собрать `RecordBatch` из подмножества отобранных row_idx
- [ ] **I5**: не дробить `RecordBatch` на под-блоки мельче Message (не включать серверный сплит блока; если `insert_many` чанкует — граница чанка = граница батча). Иначе частичный сбой на multi-row offset → потеря

### Шаг 7 — Fail-fast + poisoning
- [ ] **Синк строится на партицию**: убрать единый `build_sink()` из `main.rs` до цикла; строить синк внутри спавна на каждую `pid` (YDS → N синков; S3 → один синк на пайплайн). `deps` больше не несёт готовый `snk` — держит фабрику (`Arc<dyn SinkProvider>`) + общий `poisoned`
- [ ] `PoisoningSink` (`AtomicBool`): флаг `Arc<AtomicBool>` **один на воркер**, шарится во все партиционные синки (waterline и коннект — приватные)
- [ ] Ошибка INSERT → poison → fatal → **процесс выходит с ненулевым кодом** (сейчас `main` возвращает `Ok(())` даже при фатале — исправить: фатал таска → `Err`/`exit(1)`, иначе супервизор не рестартует)
- [ ] Убрать in-process ретрай синка в `main.rs` (`spawn_partition_task`, retry до 5); commit источника только после полного успеха флеша (I3)

### Шаг 8 — DDL и стартовые проверки
- [ ] `CREATE TABLE` с колонками ключа (main + DLQ), только при exactly_once; **без `ADD COLUMN`**
- [ ] verify колонок ключа → **fatal** при отсутствии
- [ ] Проверка движка/TTL через `system.tables` → `WARN`
- [ ] `add_exactly_once_key: true` + sink = `ParallelChInsertSink` → **fatal** (несовместим, §6.2)
- [ ] Вывод и лог режима гарантий (§11)

### Шаг 9 — Тесты
- [ ] Юнит: `committed`/`mark_committed`; `None` vs `Some(0)` (offset 0 не теряется)
- [ ] Интеграция: **`ensure_loaded` на несуществующей партиции возвращает `None`, а не `Some(0)`** (проверка `HAVING count()>0`; регрессия на реальном CH — §4/#1)
- [ ] Юнит: `ensure_loaded` — загрузка/кеш/отсутствие партиции/эвикт+reload
- [ ] Юнит: фильтрация 5.b.1/5.b.2/5.b.3, single vs multi-partition
- [ ] Юнит: `PoisoningSink` — после Err не зовёт inner; общий `Arc<AtomicBool>` роняет **все** партиционные синки
- [ ] Интеграция: запись → фильтрация дублей; рестарт → ленивая перезагрузка → нет дублей/потерь
- [ ] Интеграция: частичный/сбойный INSERT → fatal → **ненулевой код выхода** → рестарт → нет дублей/потерь
- [ ] Интеграция: newline-splitter — N строк с одним offset; DLQ с ключом
- [ ] Интеграция (**I5**): newline-Message из N строк (один offset) + сбой после части блоков → рестарт → ровно N строк, без потерь и дублей
- [ ] Интеграция: существующая таблица без колонок ключа → fatal на старте
- [ ] Старт: `ParallelChInsertSink` + exactly_once → fatal (§6.2)
- [ ] Юнит+интеграция (**#7**): `to_sql_literal` = `unhex(hex(bytes))` для S3-filename с `\` и `'` → `ensure_loaded` находит реальную строку (регрессия на CH: имя с backslash матчится; без экранирования)
- [ ] Интеграция: ReplacingMergeTree с ключом в ORDER BY — занижение waterline самокорректируется (дубли схлопываются)
- [ ] Старт (**#8**): ReplacingMergeTree с ORDER BY **без** колонок ключа → `WARN` (§8)
```
