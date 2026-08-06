use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, Client};

use crate::types::exactly_once::{ExactlyOnceKey, PartitionKey};

/// Ключ waterline: (таблица, партиция). Разные таблицы (main и DLQ) имеют
/// независимые waterline в пределах одного синка.
type WaterlineKey = (Arc<str>, PartitionKey);

/// Кеш максимального уже записанного offset на (таблицу, партицию).
///
/// `None` для ключа = «ещё не видели / нет в CH» (НЕ то же, что offset 0).
/// `None` vs `Some(0)` различаются благодаря `HAVING count() > 0` в SQL-запросе
/// и negative caching (множество `loaded_also_empty`).
pub struct Waterline {
    /// Кеш: максимальный записанный offset. Только ключи с реальными данными в CH.
    committed: HashMap<WaterlineKey, i64>,
    /// Порядок для LRU-эвикта (bounded-память для S3-ключей).
    lru: VecDeque<WaterlineKey>,
    /// Максимальный размер кеша.
    cap: usize,
    /// Ключи, которые были загружены и оказались пусты (партиция без данных).
    /// Защищает от повторных SELECT max на пустую партицию каждый батч.
    loaded_also_empty: HashSet<WaterlineKey>,
}

impl Waterline {
    pub fn new(cap: usize) -> Self {
        Self {
            committed: HashMap::new(),
            lru: VecDeque::new(),
            cap,
            loaded_also_empty: HashSet::new(),
        }
    }

    /// Гарантирует, что waterline для (таблицы, партиции) загружен в кеш.
    /// Разово ходит в ClickHouse. Double-checked: сначала быстрая проверка кеша,
    /// при промахе — async путь.
    ///
    /// **API note:** `&mut self` — Waterline single-owner, конкурентного доступа нет.
    pub async fn ensure_loaded(
        &mut self,
        client: &Client<ArrowFormat>,
        table: &Arc<str>,
        key: &ExactlyOnceKey,
        pid: &PartitionKey,
    ) -> anyhow::Result<()> {
        let wk: WaterlineKey = (table.clone(), pid.clone());

        // Проверка кеша (включая negative caching — уже знаем что партиция пуста).
        if self.committed.contains_key(&wk) || self.loaded_also_empty.contains(&wk) {
            return Ok(());
        }

        let q = format!(
            "SELECT max(`{o}`) FROM `{t}` WHERE `{p}` = {val} \
             HAVING count() > 0 \
             SETTINGS select_sequential_consistency = 1",
            o = key.offset.name,
            t = table,
            p = key.partition.name,
            val = pid.to_sql_literal(),
        );

        // clickhouse-arrow 0.2.1: query_one возвращает Result<Option<RecordBatch>>
        // Извлекаем единственное значение (max offset) из первой колонки первой строки
        let batch: Option<RecordBatch> = client.query_one(&q, None).await?;
        let max: Option<i64> = batch.and_then(|b| {
            if b.num_rows() == 0 {
                return None;
            }
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|arr| {
                    if arr.is_null(0) { None } else { Some(arr.value(0)) }
                })
        });

        if let Some(m) = max {
            self.insert_lru(wk, m);
        } else {
            // Negative caching: запоминаем, что партиция пуста
            self.loaded_also_empty.insert(wk);
        }
        Ok(())
    }

    /// Максимальный записанный offset. `None` = не видели (или партиция пуста).
    #[inline]
    pub fn committed(&self, table: &Arc<str>, pid: &PartitionKey) -> Option<i64> {
        self.committed.get(&(table.clone(), pid.clone())).copied()
    }

    /// Обновление после успешного INSERT. Монотонно (max) — дешёвая страховка.
    /// При первом появлении данных удаляет ключ из `loaded_also_empty`.
    #[inline]
    pub fn mark_committed(&mut self, table: &Arc<str>, pid: PartitionKey, offset: i64) {
        let wk = (table.clone(), pid);
        self.loaded_also_empty.remove(&wk);
        // Используем insert_lru для унифицированного управления LRU и эвиктом
        let current = self.committed.get(&wk).copied().unwrap_or(i64::MIN);
        self.insert_lru(wk, current.max(offset));
    }

    // ── LRU internals ──

    fn insert_lru(&mut self, wk: WaterlineKey, offset: i64) {
        // Если ключ уже есть — обновить значение и переместить в конец LRU
        if let Some(v) = self.committed.get_mut(&wk) {
            *v = (*v).max(offset);
            // Переместить в конец LRU
            if let Some(pos) = self.lru.iter().position(|k| k == &wk) {
                self.lru.remove(pos);
            }
            self.lru.push_back(wk);
            return;
        }
        // Эвикт при переполнении: удаляем старейший
        while self.committed.len() >= self.cap {
            if let Some(old) = self.lru.pop_front() {
                self.committed.remove(&old);
                self.loaded_also_empty.remove(&old);
            } else {
                break;
            }
        }
        self.committed.insert(wk.clone(), offset);
        self.lru.push_back(wk);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk_int(v: i64) -> PartitionKey {
        PartitionKey::Int(v)
    }

    fn pk_str(s: &str) -> PartitionKey {
        PartitionKey::Str(s.to_string())
    }

    #[test]
    fn test_committed_none_for_unknown_key() {
        let wl = Waterline::new(100);
        let table: Arc<str> = "events".into();
        assert_eq!(wl.committed(&table, &pk_int(0)), None);
    }

    #[test]
    fn test_mark_committed_and_committed() {
        let mut wl = Waterline::new(100);
        let table: Arc<str> = "events".into();

        wl.mark_committed(&table, pk_int(0), 5);
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(5));

        // Монотонность: меньший offset не понижает
        wl.mark_committed(&table, pk_int(0), 3);
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(5));

        // Больший offset повышает
        wl.mark_committed(&table, pk_int(0), 10);
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(10));
    }

    #[test]
    fn test_mark_committed_removes_from_loaded_also_empty() {
        let mut wl = Waterline::new(100);
        let table: Arc<str> = "events".into();
        let wk: WaterlineKey = (table.clone(), pk_int(0));

        // simulate negative cache
        wl.loaded_also_empty.insert(wk.clone());
        assert!(wl.loaded_also_empty.contains(&wk));

        wl.mark_committed(&table, pk_int(0), 42);
        assert!(!wl.loaded_also_empty.contains(&wk));
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(42));
    }

    #[test]
    fn test_lru_eviction() {
        let mut wl = Waterline::new(2); // cap = 2
        let table: Arc<str> = "events".into();

        wl.mark_committed(&table, pk_int(0), 10);
        wl.mark_committed(&table, pk_int(1), 20);
        assert_eq!(wl.committed.len(), 2);

        // Третий ключ должен вытеснить старейший (pk_int(0))
        wl.mark_committed(&table, pk_int(2), 30);
        assert_eq!(wl.committed.len(), 2);
        assert_eq!(wl.committed(&table, &pk_int(0)), None); // вытеснен
        assert_eq!(wl.committed(&table, &pk_int(1)), Some(20));
        assert_eq!(wl.committed(&table, &pk_int(2)), Some(30));
    }

    #[test]
    fn test_different_tables_independent_waterlines() {
        let mut wl = Waterline::new(100);
        let main: Arc<str> = "events".into();
        let dlq: Arc<str> = "events.dlq".into();

        wl.mark_committed(&main, pk_int(0), 42);
        assert_eq!(wl.committed(&main, &pk_int(0)), Some(42));
        assert_eq!(wl.committed(&dlq, &pk_int(0)), None);
    }

    #[test]
    fn test_str_partition_key() {
        let mut wl = Waterline::new(100);
        let table: Arc<str> = "events".into();

        wl.mark_committed(&table, pk_str("dir/file.json"), 5);
        assert_eq!(wl.committed(&table, &pk_str("dir/file.json")), Some(5));
        // Другой файл — независимый waterline
        assert_eq!(wl.committed(&table, &pk_str("dir/other.json")), None);
    }

    #[test]
    fn test_to_sql_literal_int() {
        assert_eq!(pk_int(42).to_sql_literal(), "42");
        assert_eq!(pk_int(-1).to_sql_literal(), "-1");
        assert_eq!(pk_int(0).to_sql_literal(), "0");
    }

    #[test]
    fn test_to_sql_literal_str_is_hex_encoded() {
        let lit = pk_str("hello world").to_sql_literal();
        assert!(lit.starts_with("unhex('"));
        assert!(lit.ends_with("')"));
        // hex("hello world") = "68656c6c6f20776f726c64"
        assert_eq!(lit, "unhex('68656c6c6f20776f726c64')");
    }

    #[test]
    fn test_to_sql_literal_str_special_chars() {
        // backslash and single quote — must survive roundtrip via hex
        let lit = pk_str("dir\\batch").to_sql_literal();
        assert!(lit.starts_with("unhex('"));
        // hex("dir\\batch") = "6469725c6261746368"
        assert_eq!(lit, "unhex('6469725c6261746368')");

        let lit2 = pk_str("it's").to_sql_literal();
        // hex("it's") = "69742773"
        assert_eq!(lit2, "unhex('69742773')");
    }
}
