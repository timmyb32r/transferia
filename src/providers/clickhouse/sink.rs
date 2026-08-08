use std::collections::HashMap;
use alloc::sync::Arc;

use arrow::array::{Array as _, BooleanArray, BooleanBufferBuilder, Int64Array, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::config::yaml::parse_arrow_type;
use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::waterline::Waterline;
use crate::types::exactly_once::{ExactlyOnceKey, PartitionKey};
use crate::types::table_data::TableWrite;

/// `ClickHouse` sink config.
#[derive(Debug, Clone, Deserialize)]
pub struct ClickhouseSinkConfig {
    pub connection_string: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_batch")]
    pub batch_size: usize,
    #[serde(default = "default_linger")]
    pub max_linger_ms: u64,
    #[serde(default = "default_connections")]
    pub max_connections: usize,
    #[serde(default = "default_tls")]
    pub use_tls: bool,
    #[serde(default)]
    pub tls_domain: Option<String>,
}

fn default_database() -> String { "default".into() }
fn default_username() -> String { "default".into() }
const fn default_batch() -> usize { 10000 }
const fn default_linger() -> u64 { 500 }
const fn default_connections() -> usize { 4 }
const fn default_tls() -> bool { true }

/// Map an Arrow `DataType` to the equivalent `ClickHouse` column type.
fn arrow_to_clickhouse(dt: &DataType) -> anyhow::Result<String> {
    Ok(match dt {
        &DataType::Utf8 | &DataType::LargeUtf8 => "String".into(),
        &DataType::Int8 => "Int8".into(),
        &DataType::Int16 => "Int16".into(),
        &DataType::Int32 => "Int32".into(),
        &DataType::Int64 => "Int64".into(),
        &DataType::UInt8 => "UInt8".into(),
        &DataType::UInt16 => "UInt16".into(),
        &DataType::UInt32 => "UInt32".into(),
        &DataType::UInt64 => "UInt64".into(),
        &DataType::Float32 => "Float32".into(),
        &DataType::Float64 => "Float64".into(),
        &DataType::Boolean => "Bool".into(),
        &DataType::Date32 => "Date32".into(),
        &DataType::Date64 => "DateTime64(3)".into(),
        &DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => "DateTime".into(),
            TimeUnit::Millisecond => "DateTime64(3)".into(),
            TimeUnit::Microsecond => "DateTime64(6)".into(),
            TimeUnit::Nanosecond => "DateTime64(9)".into(),
        },
        other @ (
            &DataType::Null
            | &DataType::Float16
            | &DataType::Time32(_)
            | &DataType::Time64(_)
            | &DataType::Duration(_)
            | &DataType::Interval(_)
            | &DataType::Binary
            | &DataType::FixedSizeBinary(_)
            | &DataType::LargeBinary
            | &DataType::BinaryView
            | &DataType::Utf8View
            | &DataType::List(_)
            | &DataType::ListView(_)
            | &DataType::FixedSizeList(..)
            | &DataType::LargeList(_)
            | &DataType::LargeListView(_)
            | &DataType::Struct(_)
            | &DataType::Union(..)
            | &DataType::Dictionary(..)
            | &DataType::Decimal32(..)
            | &DataType::Decimal64(..)
            | &DataType::Decimal128(..)
            | &DataType::Decimal256(..)
            | &DataType::Map(..)
            | &DataType::RunEndEncoded(..)
        ) => {
            anyhow::bail!("No ClickHouse type mapping for Arrow type {other:?}")
        }
    })
}

// ── ClickHouseSink ─────────────────────────────────────────────────────

pub struct ClickHouseSink {
    pool: ConnectionPool<ArrowFormat>,
    /// Exactly-once waterline (per-partition for YDS, multi-key LRU for S3).
    /// Arc<Mutex<>> for interior mutability — `Sink::write` takes &self.
    waterline: Arc<Mutex<Waterline>>,
    _config: ClickhouseSinkConfig,
}

impl ClickHouseSink {
    pub async fn new(
        config: ClickhouseSinkConfig,
        waterline_cap: usize,
    ) -> anyhow::Result<Self> {
        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(config.connection_string.as_str())
            .configure_pool(|p| p.max_size(config.max_connections as u32))
            .configure_client(|b| {
                let mut builder = b.with_database(config.database.as_str())
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_tls(config.use_tls);
                if let Some(ref domain) = config.tls_domain {
                    builder = builder.with_domain(domain.as_str());
                }
                builder
            })
            .build().await
            .map_err(|e| anyhow::anyhow!("Failed to build ClickHouse pool: {e}"))?;
        {
            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool connection failed: {e}"))?;
            client.execute("SELECT 1", None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {e}"))?;
        };
        tracing::info!("Connected to ClickHouse at {} (pool: {})", config.connection_string, config.max_connections);
        Ok(Self {
            pool,
            waterline: Arc::new(Mutex::new(Waterline::new(waterline_cap))),
            _config: config,
        })
    }

    /// Build `(column_name, clickhouse_type)` pairs from column definitions.
    pub fn schema_columns(cols: &[crate::config::yaml::ColumnDef]) -> anyhow::Result<Vec<(String, String)>> {
        cols.iter().map(|c| {
            let dt = parse_arrow_type(&c.arrow_type)?;
            let mut ty = arrow_to_clickhouse(&dt)?;
            if c.nullable {
                ty = format!("Nullable({ty})");
            }
            Ok((c.column_name.clone(), ty))
        }).collect()
    }

    pub async fn create_table(
        &self, name: &str, columns: &[(String, String)],
        order_by: &[String], recreate: bool,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for create_table: {e}"))?;
        if recreate {
            tracing::warn!("RECREATE_TABLES set \u{2014} dropping table '{}'", name);
            client.execute(&format!("DROP TABLE IF EXISTS `{name}`"), None).await
                .map_err(|e| anyhow::anyhow!("Failed to drop table '{name}': {e}"))?;
        }
        let cols = columns.iter()
            .map(|c| format!("`{}` {}", c.0, c.1))
            .collect::<Vec<_>>()
            .join(", ");
        let order_clause = if order_by.is_empty() {
            "tuple()".to_string()
        } else {
            order_by.iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{name}` ({cols}) ENGINE = MergeTree ORDER BY ({order_clause})",
        );
        client.execute(&ddl, None).await
            .map_err(|e| anyhow::anyhow!("Failed to create table '{name}': {e}"))?;
        tracing::info!("Ensured table '{}'", name);
        Ok(())
    }

    /// Check `ClickHouse` version ≥ 22.8 (`select_sequential_consistency`).
    pub async fn check_ch_version(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for version check: {e}"))?;
        let batch = client.query_one("SELECT version()", None).await
            .map_err(|e| anyhow::anyhow!("ClickHouse version query failed: {e}"))?;
        let ver_str = batch.and_then(|b| {
            b.column(0).as_any().downcast_ref::<StringArray>()
                .map(|a| a.value(0).to_string())
        }).unwrap_or_default();
        tracing::info!("ClickHouse version: {}", ver_str);
        // Parse major.minor from "25.4.1.123" format
        let parts: Vec<&str> = ver_str.split('.').collect();
        if parts.len() < 2 {
            anyhow::bail!("Cannot parse ClickHouse version: {ver_str}");
        }
        let major: u32 = parts[0].parse().unwrap_or(0);
        let minor: u32 = parts[1].parse().unwrap_or(0);
        if major < 22 || (major == 22 && minor < 8) {
            anyhow::bail!(
                "ClickHouse {ver_str} is too old. Version 22.8+ required for exactly-once \
                 (select_sequential_consistency setting). Upgrade ClickHouse or disable exactly_once."
            );
        }
        Ok(())
    }

    /// Check table engine and replica count.
    /// Returns `(engine, insert_quorum, replica_count)`.
    pub async fn check_table_engine(&self, table: &str) -> anyhow::Result<(String, u64, u64)> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for engine check: {e}"))?;
        // Query engine and insert_quorum from system.tables
        let q = format!(
            "SELECT engine_full, cast(extract(settings, 'insert_quorum') AS Nullable(UInt64)) \
             FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
        );
        let batch = client.query_one(&q, None).await
            .map_err(|e| anyhow::anyhow!("ClickHouse engine query failed: {e}"))?;
        let (engine, quorum) = match batch {
            Some(b) if b.num_rows() > 0 => {
                let eng = b.column(0).as_any().downcast_ref::<StringArray>()
                    .map(|a| a.value(0).to_string()).unwrap_or_default();
                let iq = b.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>()
                    .map_or(1, |a| a.value(0));
                (eng, iq)
            }
            _ => (String::new(), 1),
        };
        // Query replica count (only for Replicated engines)
        let replica_count = if engine.contains("Replicated") {
            let rq = format!(
                "SELECT count() FROM system.replicas \
                 WHERE database = currentDatabase() AND table = '{table}'"
            );
            let rb = client.query_one(&rq, None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse replica query failed: {e}"))?;
            rb.and_then(|b| {
                b.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>()
                    .map(|a| a.value(0))
            }).unwrap_or(1)
        } else {
            1
        };
        Ok((engine, quorum, replica_count))
    }

}

// ── Sink trait ─────────────────────────────────────────────────────────

/// WARNING: `ClickHouseSink::write` requires `&mut self` for waterline.
/// The current Sink trait accepts `&self`. We use interior mutability
/// via `Arc<tokio::sync::Mutex<Waterline>>`.
impl Sink for ClickHouseSink {
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if write.batches.is_empty() {
                return Ok(());
            }

            let key = match write.exactly_once_key {
                Some(ref k) => k.clone(),
                None => {
                    // Plain INSERT — at-least-once
                    let client = self.pool.get().await
                        .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {e}"))?;
                    let query = format!("INSERT INTO `{}` VALUES", write.table);
                    let total: usize = write.batches.iter().map(RecordBatch::num_rows).sum();
                    let n = write.batches.len();
                    let mut stream = client.insert_many(&query, write.batches, None).await
                        .map_err(|e| anyhow::anyhow!("ClickHouse insert_many failed: {e}"))?;
                    while let Some(item) = stream.next().await {
                        item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {e}"))?;
                    }
                    tracing::info!("Inserted {} rows ({} blocks) into '{}'", total, n, write.table);
                    return Ok(());
                }
            };

            // Exactly-once path.
            //
            // One pass collects every row with its pid, offset, and location
            // (`collect_rows`). Rows are then grouped by pid in-memory — no
            // second scan of the batch columns. When exactly one group exists
            // (`is_single`), every row in every batch shares that pid, so the
            // whole `write.batches` can be inserted without masking/copying
            // (fast path F). Otherwise each group's rows are a subset of the
            // batches and must be masked per-batch (`insert_selected_rows`).
            let client = self.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {e}"))?;

            let mut wl = self.waterline.lock().await;

            let rows = collect_rows(&write.batches, &key)?;
            let mut groups: HashMap<PartitionKey, Vec<RowRef>> = HashMap::with_capacity(4);
            for r in rows {
                groups.entry(r.pid.clone()).or_default().push(r);
            }
            let is_single = groups.len() == 1;
            #[expect(clippy::iter_over_hash_type, reason = "insert order across partitions is independent; HashMap grouping is the natural data structure")]
            for (pid, grp) in groups {
                insert_with_waterline(
                    &client, &write, &key, &pid, grp, is_single, &mut wl,
                ).await?;
            }
            Ok(())
        })
    }

    fn as_any(&self) -> &dyn core::any::Any { self }

    fn max_linger_ms(&self) -> Option<u64> {
        Some(self._config.max_linger_ms)
    }
}

// ── Exactly-once helpers ───────────────────────────────────────────────

struct RowRef {
    batch_idx: usize,
    row_idx: usize,
    offset: i64,
    /// Partition value of this row. Carried so rows can be grouped by partition
    /// in-memory after a single collect pass — no second batch scan. `Copy` for
    /// YDS Int64; a cloned `String` for S3 Utf8 (clone count ≤ the old path,
    /// which cloned every row's string in `single_partition` to compare).
    pid: PartitionKey,
}

/// One pass over the partition **and** offset columns of all batches. Builds
/// `RowRef`s carrying pid, offset, and location. The caller groups by `pid`.
///
/// Replaces the old two-function `single_partition` + `collect_rows` (two
/// passes) and the batch-re-scanning `group_by_partition` (a third pass on the
/// multi-partition path). Partition nulls are mapped to a default key — both
/// `__system_partition` (YDS) and `__system_filename` (S3) are non-nullable in
/// the parser schema, so this is defensive only.
fn collect_rows(
    batches: &[RecordBatch],
    key: &ExactlyOnceKey,
) -> anyhow::Result<Vec<RowRef>> {
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut rows = Vec::with_capacity(total_rows);
    for (batch_idx, batch) in batches.iter().enumerate() {
        let part_col = batch.column_by_name(&key.partition.name)
            .ok_or_else(|| anyhow::anyhow!(
                "ExactlyOnceKey partition column '{}' not found in batch",
                key.partition.name
            ))?;
        let off_col = batch.column_by_name(&key.offset.name)
            .ok_or_else(|| anyhow::anyhow!(
                "ExactlyOnceKey offset column '{}' not found in batch",
                key.offset.name
            ))?;
        let offsets = off_col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("offset column is not Int64"))?;
        #[expect(clippy::wildcard_enum_match_arm, reason = "unsupported column types are rejected with an error; future arrow types will be rejected too")]
        match part_col.data_type() {
            DataType::Int64 => {
                let arr = part_col.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("partition column is not Int64"))?;
                for row_idx in 0..batch.num_rows() {
                    let pid = if arr.is_null(row_idx) {
                        PartitionKey::Int(0)
                    } else {
                        PartitionKey::Int(arr.value(row_idx))
                    };
                    let offset = if offsets.is_null(row_idx) { 0 } else { offsets.value(row_idx) };
                    rows.push(RowRef { batch_idx, row_idx, offset, pid });
                }
            }
            DataType::Utf8 => {
                let arr = part_col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("partition column is not Utf8"))?;
                for row_idx in 0..batch.num_rows() {
                    let pid = if arr.is_null(row_idx) {
                        PartitionKey::Str(String::new())
                    } else {
                        PartitionKey::Str(arr.value(row_idx).to_string())
                    };
                    let offset = if offsets.is_null(row_idx) { 0 } else { offsets.value(row_idx) };
                    rows.push(RowRef { batch_idx, row_idx, offset, pid });
                }
            }
            other => anyhow::bail!("Unsupported partition column type: {other:?}"),
        }
    }
    Ok(rows)
}

/// Pure (no I/O): build filtered `RecordBatch`es for the given rows across all
/// batches. The keep-mask is built per batch in **`O(rows_in_batch)`** via
/// `BooleanBufferBuilder::set_bit` — replacing the old `O(batch_rows × rows)`
/// `rows.iter().any(...)` — and rows from **every** batch are emitted. The old
/// `insert_rows_inner` only touched `batches[rows[0].batch_idx]` and silently
/// dropped rows from other batches (latent data-loss on multi-batch flushes).
fn build_filtered_blocks(
    batches: &[RecordBatch],
    rows: &[&RowRef],
) -> anyhow::Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    // Partition by batch_idx → O(rows + total_rows) mask build.
    let mut by_batch: Vec<Vec<usize>> = vec![Vec::new(); batches.len()];
    for r in rows {
        by_batch[r.batch_idx].push(r.row_idx);
    }
    let mut blocks: Vec<RecordBatch> = Vec::new();
    for (batch_idx, row_idxs) in by_batch.iter().enumerate() {
        if row_idxs.is_empty() {
            continue;
        }
        let batch = &batches[batch_idx];
        let n = batch.num_rows();
        let mut mask = BooleanBufferBuilder::new(n);
        mask.append_n(n, false);
        for &ri in row_idxs {
            mask.set_bit(ri, true);
        }
        let keep = BooleanArray::new(mask.finish(), None);
        let filtered = compute::filter_record_batch(batch, &keep)
            .map_err(|e| anyhow::anyhow!("filter_record_batch: {e}"))?;
        if filtered.num_rows() > 0 {
            blocks.push(filtered);
        }
    }
    Ok(blocks)
}

/// Insert only the selected rows (across all batches), masked per batch.
/// See [`build_filtered_blocks`] for the masking semantics.
async fn insert_selected_rows(
    client: &clickhouse_arrow::Client<ArrowFormat>,
    write: &TableWrite,
    rows: &[&RowRef],
) -> anyhow::Result<()> {
    let blocks = build_filtered_blocks(&write.batches, rows)?;
    if blocks.is_empty() {
        return Ok(());
    }
    let query = format!("INSERT INTO `{}` VALUES", write.table);
    let n_rows: usize = blocks.iter().map(RecordBatch::num_rows).sum();
    let mut stream = client.insert_many(&query, blocks, None).await
        .map_err(|e| anyhow::anyhow!("ClickHouse insert_many (exactly-once partial) failed: {e}"))?;
    while let Some(item) = stream.next().await {
        item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {e}"))?;
    }
    tracing::info!("Exactly-once partial: inserted {} filtered rows into '{}'", n_rows, write.table);
    Ok(())
}

/// Fast path (F): insert every batch whole — no boolean mask, no
/// `filter_record_batch` copy. Valid only when `is_single` (every row in every
/// batch is this pid) and all rows are above the waterline. Mirrors the
/// at-least-once INSERT shape; `RecordBatch` clone is an Arc refcount bump.
async fn insert_all_batches(
    client: &clickhouse_arrow::Client<ArrowFormat>,
    table: &str,
    batches: &[RecordBatch],
) -> anyhow::Result<()> {
    let query = format!("INSERT INTO `{table}` VALUES");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let n = batches.len();
    let mut stream = client.insert_many(&query, batches.to_vec(), None).await
        .map_err(|e| anyhow::anyhow!("ClickHouse insert_many (exactly-once fast-path) failed: {e}"))?;
    while let Some(item) = stream.next().await {
        item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {e}"))?;
    }
    tracing::info!("Exactly-once fast-path: inserted {total} rows ({n} blocks) into '{table}'");
    Ok(())
}

/// Exactly-once insert decision for one partition's rows, given the cached
/// waterline. Pure (no I/O) so it is unit-testable without a live `ClickHouse`.
///
/// - `Skip` when every offset is already at or below the waterline.
/// - `InsertAllBatches` when the partition is the only one in the flush
///   (`is_single`) and every row is above the waterline — whole batches can be
///   inserted with no masking/copying (fast path F).
/// - `InsertRows { above }` otherwise — mask to rows above `above` (`None` ⇒
///   all of this partition's rows; a multi-partition subset scattered across
///   batches).
enum EoDecision {
    Skip,
    InsertAllBatches,
    InsertRows { above: Option<i64> },
}

fn classify_eo(wl_val: Option<i64>, min_off: i64, max_off: i64, is_single: bool) -> EoDecision {
    if let Some(v) = wl_val {
        if max_off <= v {
            return EoDecision::Skip;
        }
    }
    let all_above = wl_val.is_none_or(|v| min_off > v);
    if is_single && all_above {
        EoDecision::InsertAllBatches
    } else if all_above {
        EoDecision::InsertRows { above: None }
    } else {
        // !all_above ⇒ wl_val is Some and min_off <= v.
        EoDecision::InsertRows { above: wl_val }
    }
}

/// Insert rows for a single partition, respecting the waterline (exactly-once
/// dedup). State machine unchanged: `ensure_loaded` → `committed` →
/// `classify_eo` → `mark_committed(max_off)`. The per-partition writer is
/// single (one writer per partition in `run_partition_pipeline`), so waterline
/// monotonicity holds.
async fn insert_with_waterline(
    client: &clickhouse_arrow::Client<ArrowFormat>,
    write: &TableWrite,
    key: &ExactlyOnceKey,
    pid: &PartitionKey,
    rows: Vec<RowRef>,
    is_single: bool,
    wl: &mut Waterline,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    wl.ensure_loaded(client, &write.table, key, pid).await?;
    let wl_val = wl.committed(&write.table, pid);

    let max_off = rows.iter().map(|r| r.offset).max().unwrap_or(0);
    let min_off = rows.iter().map(|r| r.offset).min().unwrap_or(0);

    match classify_eo(wl_val, min_off, max_off, is_single) {
        EoDecision::Skip => return Ok(()),
        EoDecision::InsertAllBatches => {
            // Fast path F: every row in every batch is this pid and above the
            // waterline — insert whole batches, no mask, no copy.
            insert_all_batches(client, &write.table, &write.batches).await?;
        }
        EoDecision::InsertRows { above } => {
            // Partial overlap, or a multi-partition subset: mask to the rows
            // above the waterline (or all of this pid's rows when `above` is
            // None), per batch — O(rows), multi-batch safe.
            let selected: Vec<&RowRef> = rows.iter()
                .filter(|r| above.is_none_or(|v| r.offset > v))
                .collect();
            insert_selected_rows(client, write, &selected).await?;
        }
    }
    wl.mark_committed(&write.table, pid.clone(), max_off);
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn poisoning_sink_blocks_after_error() {
        // Placeholder: full test requires a mock Sink
    }

    #[test]
    fn collect_rows_int64_single_and_multi() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__system_partition", DataType::Int64, false),
            Field::new("__system_offset", DataType::Int64, false),
        ]));
        let part = Int64Array::from(vec![0, 0, 1, 1]);
        let off = Int64Array::from(vec![10, 11, 20, 21]);
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(part), Arc::new(off),
        ])?;

        let key = ExactlyOnceKey {
            partition: crate::types::exactly_once::ExactlyOnceColumn { name: "__system_partition".into() },
            offset: crate::types::exactly_once::ExactlyOnceColumn { name: "__system_offset".into() },
        };

        // One pass collects every row with pid + offset + location.
        let rows = collect_rows(&[batch], &key)?;
        anyhow::ensure!(rows.len() == 4, "expected 4 RowRefs, got {}", rows.len());
        anyhow::ensure!(rows[0].pid == PartitionKey::Int(0) && rows[0].offset == 10, "row0");
        anyhow::ensure!(rows[2].pid == PartitionKey::Int(1) && rows[2].offset == 20, "row2");

        // Group by pid in-memory (mirrors the Sink::write EO branch).
        let mut groups: HashMap<PartitionKey, Vec<RowRef>> = HashMap::new();
        for r in rows {
            groups.entry(r.pid.clone()).or_default().push(r);
        }
        anyhow::ensure!(groups.len() == 2, "expected 2 groups, got {}", groups.len());
        anyhow::ensure!(
            groups[&PartitionKey::Int(0)].len() == 2,
            "partition 0 should have 2 rows",
        );
        anyhow::ensure!(
            groups[&PartitionKey::Int(1)].len() == 2,
            "partition 1 should have 2 rows",
        );
        Ok(())
    }

    /// Regression for the multi-batch data-loss bug: the old `insert_rows_inner`
    /// only touched `batches[rows[0].batch_idx]` and silently dropped rows from
    /// any other batch. `build_filtered_blocks` must emit a block per batch that
    /// has selected rows, with exactly those rows.
    #[test]
    fn build_filtered_blocks_multi_batch_no_drops() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("__system_partition", DataType::Int64, false),
            Field::new("__system_offset", DataType::Int64, false),
        ]));
        let b0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(Int64Array::from(vec![5, 5, 5])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )?;
        let b1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![200, 201, 202])),
                Arc::new(Int64Array::from(vec![5, 5, 5])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
            ],
        )?;
        let batches = vec![b0, b1];

        // Select rows from BOTH batches: batch0 {0,2}, batch1 {1}.
        let pid = PartitionKey::Int(5);
        let selected: Vec<RowRef> = vec![
            RowRef { batch_idx: 0, row_idx: 0, offset: 1, pid: pid.clone() },
            RowRef { batch_idx: 0, row_idx: 2, offset: 3, pid: pid.clone() },
            RowRef { batch_idx: 1, row_idx: 1, offset: 5, pid: pid.clone() },
        ];
        let refs: Vec<&RowRef> = selected.iter().collect();

        let blocks = build_filtered_blocks(&batches, &refs)?;
        anyhow::ensure!(blocks.len() == 2, "expected 2 blocks (one per batch), got {}", blocks.len());
        let total: usize = blocks.iter().map(RecordBatch::num_rows).sum();
        anyhow::ensure!(total == 3, "expected 3 total rows, got {total}");

        // batch0 → v=[100,102]; batch1 → v=[201].
        let mut vs: Vec<i64> = Vec::with_capacity(3);
        for b in &blocks {
            let a = b.column(0).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("v column not Int64"))?;
            for i in 0..a.len() {
                vs.push(a.value(i));
            }
        }
        vs.sort_unstable();
        anyhow::ensure!(vs == vec![100, 102, 201], "selected v values must be {{100,102,201}}, got {vs:?}");
        Ok(())
    }

    /// Verifies the F fast-path decision: single-partition + all-above-waterline
    /// ⇒ `InsertAllBatches` (no mask, no copy); otherwise mask. Pure — no CH.
    #[test]
    fn classify_eo_fast_path_and_partial() {
        // Single partition, no waterline → fast path.
        assert!(matches!(classify_eo(None, 5, 10, true), EoDecision::InsertAllBatches));
        // Single partition, all rows above waterline → fast path.
        assert!(matches!(classify_eo(Some(3), 5, 10, true), EoDecision::InsertAllBatches));
        // Single partition, partial overlap → mask rows above waterline (5).
        assert!(matches!(classify_eo(Some(5), 3, 10, true), EoDecision::InsertRows { above: Some(5) }));
        // All already committed → skip.
        assert!(matches!(classify_eo(Some(10), 5, 10, true), EoDecision::Skip));
        assert!(matches!(classify_eo(Some(12), 5, 10, true), EoDecision::Skip));
        // Multi-partition, all above / no waterline → mask all of this pid's rows.
        assert!(matches!(classify_eo(Some(3), 5, 10, false), EoDecision::InsertRows { above: None }));
        assert!(matches!(classify_eo(None, 5, 10, false), EoDecision::InsertRows { above: None }));
        // Multi-partition, partial → mask rows above waterline.
        assert!(matches!(classify_eo(Some(7), 5, 10, false), EoDecision::InsertRows { above: Some(7) }));
        // Never the no-mask fast path when more than one partition is present.
        assert!(!matches!(classify_eo(Some(3), 5, 10, false), EoDecision::InsertAllBatches));
    }
}
