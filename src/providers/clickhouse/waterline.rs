use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow::array::{Array as _, Int64Array};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, Client};

use crate::types::exactly_once::{ExactlyOnceKey, PartitionKey};

/// Waterline key: (table, partition). Different tables (main and DLQ) have
/// independent waterlines within a single sink.
type WaterlineKey = (Arc<str>, PartitionKey);

/// Cache of the maximum already-written offset per (table, partition).
///
/// `None` for a key = "haven't seen yet / not in CH" (NOT the same as offset 0).
/// `None` vs `Some(0)` are distinguished via `HAVING count() > 0` in the SQL query
/// and negative caching (the `loaded_also_empty` set).
pub struct Waterline {
    /// Cache: maximum written offset. Only keys with actual data in CH.
    committed: HashMap<WaterlineKey, i64>,
    /// Order for LRU eviction (bounded memory for S3 keys).
    lru: VecDeque<WaterlineKey>,
    /// Maximum cache size.
    cap: usize,
    /// Keys that were loaded and found empty (partition with no data).
    /// Prevents repeated SELECT max on an empty partition every batch.
    loaded_also_empty: HashSet<WaterlineKey>,
}

impl Waterline {
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            committed: HashMap::new(),
            lru: VecDeque::new(),
            cap,
            loaded_also_empty: HashSet::new(),
        }
    }

    /// Ensures the waterline for (table, partition) is loaded into the cache.
    /// Makes a single round-trip to `ClickHouse`. Double-checked: fast cache check
    /// first, then async path on miss.
    ///
    /// **API note:** `&mut self` — Waterline is single-owner, no concurrent access.
    pub async fn ensure_loaded(
        &mut self,
        client: &Client<ArrowFormat>,
        table: &Arc<str>,
        key: &ExactlyOnceKey,
        pid: &PartitionKey,
    ) -> anyhow::Result<()> {
        let wk: WaterlineKey = (Arc::clone(table), pid.clone());

        // Check cache (including negative caching — already know the partition is empty).
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

        // clickhouse-arrow 0.2.1: query_one returns Result<Option<RecordBatch>>
        // Extract the single value (max offset) from the first column of the first row
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
            // Negative caching: remember that the partition is empty
            self.loaded_also_empty.insert(wk);
        }
        Ok(())
    }

    /// Maximum written offset. `None` = haven't seen (or partition is empty).
    #[inline]
    #[must_use]
    pub fn committed(&self, table: &Arc<str>, pid: &PartitionKey) -> Option<i64> {
        self.committed.get(&(Arc::clone(table), pid.clone())).copied()
    }

    /// Update after a successful INSERT. Monotonic (max) — cheap safeguard.
    /// On first data appearance, removes the key from `loaded_also_empty`.
    #[inline]
    pub fn mark_committed(&mut self, table: &Arc<str>, pid: PartitionKey, offset: i64) {
        let wk = (Arc::clone(table), pid);
        self.loaded_also_empty.remove(&wk);
        // Use insert_lru for unified LRU management and eviction
        let current = self.committed.get(&wk).copied().unwrap_or(i64::MIN);
        self.insert_lru(wk, current.max(offset));
    }

    // ── LRU internals ──

    fn insert_lru(&mut self, wk: WaterlineKey, offset: i64) {
        // If key already exists — update value and move to the end of LRU
        if let Some(v) = self.committed.get_mut(&wk) {
            *v = (*v).max(offset);
            // Move to the end of LRU
            if let Some(pos) = self.lru.iter().position(|k| k == &wk) {
                self.lru.remove(pos);
            }
            self.lru.push_back(wk);
            return;
        }
        // Evict on overflow: remove the oldest
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
    fn committed_none_for_unknown_key() {
        let wl = Waterline::new(100);
        let table: Arc<str> = "events".into();
        assert_eq!(wl.committed(&table, &pk_int(0)), None);
    }

    #[test]
    fn mark_committed_and_committed() {
        let mut wl = Waterline::new(100);
        let table: Arc<str> = "events".into();

        wl.mark_committed(&table, pk_int(0), 5);
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(5));

        // Monotonic: a smaller offset must not decrease
        wl.mark_committed(&table, pk_int(0), 3);
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(5));

        // A larger offset must increase
        wl.mark_committed(&table, pk_int(0), 10);
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(10));
    }

    #[test]
    fn mark_committed_removes_from_loaded_also_empty() {
        let mut wl = Waterline::new(100);
        let table: Arc<str> = "events".into();
        let wk: WaterlineKey = (Arc::clone(&table), pk_int(0));

        // simulate negative cache
        wl.loaded_also_empty.insert(wk.clone());
        assert!(wl.loaded_also_empty.contains(&wk));

        wl.mark_committed(&table, pk_int(0), 42);
        assert!(!wl.loaded_also_empty.contains(&wk));
        assert_eq!(wl.committed(&table, &pk_int(0)), Some(42));
    }

    #[test]
    fn lru_eviction() {
        let mut wl = Waterline::new(2); // cap = 2
        let table: Arc<str> = "events".into();

        wl.mark_committed(&table, pk_int(0), 10);
        wl.mark_committed(&table, pk_int(1), 20);
        assert_eq!(wl.committed.len(), 2);

        // Third key should evict the oldest (pk_int(0))
        wl.mark_committed(&table, pk_int(2), 30);
        assert_eq!(wl.committed.len(), 2);
        assert_eq!(wl.committed(&table, &pk_int(0)), None); // evicted
        assert_eq!(wl.committed(&table, &pk_int(1)), Some(20));
        assert_eq!(wl.committed(&table, &pk_int(2)), Some(30));
    }

    #[test]
    fn different_tables_independent_waterlines() {
        let mut wl = Waterline::new(100);
        let main: Arc<str> = "events".into();
        let dlq: Arc<str> = "events.dlq".into();

        wl.mark_committed(&main, pk_int(0), 42);
        assert_eq!(wl.committed(&main, &pk_int(0)), Some(42));
        assert_eq!(wl.committed(&dlq, &pk_int(0)), None);
    }

    #[test]
    fn str_partition_key() {
        let mut wl = Waterline::new(100);
        let table: Arc<str> = "events".into();

        wl.mark_committed(&table, pk_str("dir/file.json"), 5);
        assert_eq!(wl.committed(&table, &pk_str("dir/file.json")), Some(5));
        // Different file — independent waterline
        assert_eq!(wl.committed(&table, &pk_str("dir/other.json")), None);
    }

    #[test]
    fn to_sql_literal_int() {
        assert_eq!(pk_int(42).to_sql_literal(), "42");
        assert_eq!(pk_int(-1).to_sql_literal(), "-1");
        assert_eq!(pk_int(0).to_sql_literal(), "0");
    }

    #[test]
    fn to_sql_literal_str_is_hex_encoded() {
        let lit = pk_str("hello world").to_sql_literal();
        assert!(lit.starts_with("unhex('"));
        assert!(lit.ends_with("')"));
        // hex("hello world") = "68656c6c6f20776f726c64"
        assert_eq!(lit, "unhex('68656c6c6f20776f726c64')");
    }

    #[test]
    fn to_sql_literal_str_special_chars() {
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
