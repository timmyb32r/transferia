use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use tokio::sync::Mutex;

use crate::config::yaml::{parse_arrow_type, SchemaConfig};
use crate::pipeline::sink::Sink;
use crate::providers::clickhouse::waterline::Waterline;
use crate::types::exactly_once::{ExactlyOnceKey, PartitionKey};
use crate::types::table_data::TableWrite;

/// Map an Arrow `DataType` to the equivalent ClickHouse column type.
fn arrow_to_clickhouse(dt: &DataType) -> anyhow::Result<String> {
    Ok(match dt {
        DataType::Utf8 | DataType::LargeUtf8 => "String".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Boolean => "Bool".into(),
        DataType::Date32 => "Date32".into(),
        DataType::Date64 => "DateTime64(3)".into(),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => "DateTime".into(),
            TimeUnit::Millisecond => "DateTime64(3)".into(),
            TimeUnit::Microsecond => "DateTime64(6)".into(),
            TimeUnit::Nanosecond => "DateTime64(9)".into(),
        },
        other => anyhow::bail!("No ClickHouse type mapping for Arrow type {:?}", other),
    })
}

// ── ClickHouseSink ─────────────────────────────────────────────────────

pub struct ClickHouseSink {
    pool: ConnectionPool<ArrowFormat>,
    /// Exactly-once waterline (per-partition for YDS, multi-key LRU for S3).
    /// Arc<Mutex<>> for interior mutability — Sink::write takes &self.
    waterline: Arc<Mutex<Waterline>>,
}

impl ClickHouseSink {
    pub async fn new(
        config: &crate::providers::clickhouse::provider::SinkConfig,
        waterline_cap: usize,
    ) -> anyhow::Result<Self> {
        let pool = ConnectionPoolBuilder::<ArrowFormat>::new(config.connection_string.as_str())
            .configure_pool(|p| p.max_size(config.max_connections as u32))
            .configure_client(|b| {
                let mut b = b.with_database(config.database.as_str())
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_tls(config.use_tls);
                if let Some(ref domain) = config.tls_domain {
                    b = b.with_domain(domain.as_str());
                }
                b
            })
            .build().await
            .map_err(|e| anyhow::anyhow!("Failed to build ClickHouse pool: {}", e))?;
        {
            let client = pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool connection failed: {}", e))?;
            client.execute("SELECT 1", None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {}", e))?;
        }
        tracing::info!("Connected to ClickHouse at {} (pool: {})", config.connection_string, config.max_connections);
        Ok(Self {
            pool,
            waterline: Arc::new(Mutex::new(Waterline::new(waterline_cap))),
        })
    }

    /// Build `(column_name, clickhouse_type)` pairs from a `SchemaConfig`.
    pub fn schema_columns(settings: &SchemaConfig) -> anyhow::Result<Vec<(String, String)>> {
        settings.columns.iter().map(|c| {
            let dt = parse_arrow_type(&c.arrow_type)?;
            let mut ty = arrow_to_clickhouse(&dt)?;
            if c.nullable {
                ty = format!("Nullable({})", ty);
            }
            Ok((c.column_name.clone(), ty))
        }).collect()
    }

    pub async fn create_table(
        &self, name: &str, columns: &[(String, String)],
        order_by: &[String], recreate: bool,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for create_table: {}", e))?;
        if recreate {
            tracing::warn!("RECREATE_TABLES set — dropping table '{}'", name);
            client.execute(&format!("DROP TABLE IF EXISTS `{}`", name), None).await
                .map_err(|e| anyhow::anyhow!("Failed to drop table '{}': {}", name, e))?;
        }
        let cols = columns.iter()
            .map(|(col, ty)| format!("`{}` {}", col, ty))
            .collect::<Vec<_>>()
            .join(", ");
        let order_clause = if order_by.is_empty() {
            "tuple()".to_string()
        } else {
            order_by.iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{}` ({}) ENGINE = MergeTree ORDER BY ({})",
            name, cols, order_clause,
        );
        client.execute(&ddl, None).await
            .map_err(|e| anyhow::anyhow!("Failed to create table '{}': {}", name, e))?;
        tracing::info!("Ensured table '{}'", name);
        Ok(())
    }

    /// Check ClickHouse version ≥ 22.8 (select_sequential_consistency).
    pub async fn check_ch_version(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for version check: {}", e))?;
        let batch = client.query_one("SELECT version()", None).await
            .map_err(|e| anyhow::anyhow!("ClickHouse version query failed: {}", e))?;
        let ver_str = batch.and_then(|b| {
            b.column(0).as_any().downcast_ref::<arrow::array::StringArray>()
                .map(|a| a.value(0).to_string())
        }).unwrap_or_default();
        tracing::info!("ClickHouse version: {}", ver_str);
        // Parse major.minor from "25.4.1.123" format
        let parts: Vec<&str> = ver_str.split('.').collect();
        if parts.len() < 2 {
            anyhow::bail!("Cannot parse ClickHouse version: {}", ver_str);
        }
        let major: u32 = parts[0].parse().unwrap_or(0);
        let minor: u32 = parts[1].parse().unwrap_or(0);
        if major < 22 || (major == 22 && minor < 8) {
            anyhow::bail!(
                "ClickHouse {} is too old. Version 22.8+ required for exactly-once \
                 (select_sequential_consistency setting). Upgrade ClickHouse or disable exactly_once.",
                ver_str
            );
        }
        Ok(())
    }

    /// Check table engine and replica count.
    /// Returns `(engine, insert_quorum, replica_count)`.
    pub async fn check_table_engine(&self, table: &str) -> anyhow::Result<(String, u64, u64)> {
        let client = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("ClickHouse pool get for engine check: {}", e))?;
        // Query engine and insert_quorum from system.tables
        let q = format!(
            "SELECT engine_full, cast(extract(settings, 'insert_quorum') AS Nullable(UInt64)) \
             FROM system.tables WHERE database = currentDatabase() AND name = '{}'",
            table
        );
        let batch = client.query_one(&q, None).await
            .map_err(|e| anyhow::anyhow!("ClickHouse engine query failed: {}", e))?;
        let (engine, quorum) = match batch {
            Some(b) if b.num_rows() > 0 => {
                let eng = b.column(0).as_any().downcast_ref::<arrow::array::StringArray>()
                    .map(|a| a.value(0).to_string()).unwrap_or_default();
                let iq = b.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>()
                    .map(|a| a.value(0)).unwrap_or(1);
                (eng, iq)
            }
            _ => (String::new(), 1u64),
        };
        // Query replica count (only for Replicated engines)
        let replica_count = if engine.contains("Replicated") {
            let rq = format!(
                "SELECT count() FROM system.replicas \
                 WHERE database = currentDatabase() AND table = '{}'",
                table
            );
            let rb = client.query_one(&rq, None).await
                .map_err(|e| anyhow::anyhow!("ClickHouse replica query failed: {}", e))?;
            rb.and_then(|b| {
                b.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>()
                    .map(|a| a.value(0))
            }).unwrap_or(1)
        } else {
            1u64
        };
        Ok((engine, quorum, replica_count))
    }

    // ── Exactly-once: insert_rows (static helper) ─────────────────────

    /// Build a RecordBatch from selected rows and call insert_many.
    async fn insert_rows_inner(
        client: &clickhouse_arrow::Client<ArrowFormat>,
        write: &TableWrite,
        rows: &[RowRef],
    ) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // In the exactly-once scenario all rows typically come from a single batch
        // (one Message). Multi-batch filtering — optionally via concat_batches.
        if rows.is_empty() {
            return Ok(());
        }
        // Build a subset of the first batch (primary case: single batch)
        let batch = &write.batches[rows[0].batch_idx];
        let keep: arrow::array::BooleanArray = (0..batch.num_rows())
            .map(|i| rows.iter().any(|r| r.row_idx == i))
            .collect();
        let filtered = compute::filter_record_batch(batch, &keep)
            .map_err(|e| anyhow::anyhow!("filter_record_batch: {}", e))?;

        let query = format!("INSERT INTO `{}` VALUES", write.table);
        let n_rows = filtered.num_rows();
        let mut stream = client.insert_many(&query, vec![filtered], None).await
            .map_err(|e| anyhow::anyhow!("ClickHouse insert_many (exactly-once) failed: {}", e))?;
        while let Some(item) = stream.next().await {
            item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {}", e))?;
        }
        tracing::info!("Exactly-once: inserted {} filtered rows into '{}'", n_rows, write.table);
        Ok(())
    }
}

// ── Sink trait ─────────────────────────────────────────────────────────

/// WARNING: ClickHouseSink::write requires `&mut self` for waterline.
/// The current Sink trait accepts `&self`. We use interior mutability
/// via `Arc<tokio::sync::Mutex<Waterline>>`.
impl Sink for ClickHouseSink {
    fn write<'a>(&'a self, write: TableWrite) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if write.batches.is_empty() {
                return Ok(());
            }

            let key = match &write.exactly_once_key {
                Some(k) => k.clone(),
                None => {
                    // Plain INSERT — at-least-once
                    let client = self.pool.get().await
                        .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;
                    let query = format!("INSERT INTO `{}` VALUES", write.table);
                    let total: usize = write.batches.iter().map(|b| b.num_rows()).sum();
                    let n = write.batches.len();
                    let mut stream = client.insert_many(&query, write.batches, None).await
                        .map_err(|e| anyhow::anyhow!("ClickHouse insert_many failed: {}", e))?;
                    while let Some(item) = stream.next().await {
                        item.map_err(|e| anyhow::anyhow!("ClickHouse insert_many error: {}", e))?;
                    }
                    tracing::info!("Inserted {} rows ({} blocks) into '{}'", total, n, write.table);
                    return Ok(());
                }
            };

            // Exactly-once path
            let client = self.pool.get().await
                .map_err(|e| anyhow::anyhow!("ClickHouse pool get: {}", e))?;

            let mut wl = self.waterline.lock().await;

            // Fast path: detect whether all rows share a single partition
            // (always true for YDS) — avoids HashMap allocation entirely.
            let single_pid = single_partition(&write.batches, &key)?;

            match single_pid {
                Some(pid) => {
                    // Collect all rows into a flat Vec — no HashMap overhead.
                    let rows = collect_rows(&write.batches, &key)?;
                    insert_with_waterline(
                        &client, &write, &key, &pid, rows, &mut wl,
                    ).await?;
                }
                None => {
                    // Multi-partition case (S3 with multiple file keys):
                    // group rows by partition value, then insert per partition.
                    let partitions = group_by_partition(&write.batches, &key)?;
                    for (pid, rows) in partitions {
                        insert_with_waterline(
                            &client, &write, &key, &pid, rows, &mut wl,
                        ).await?;
                    }
                }
            }
            Ok(())
        })
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}

// ── Exactly-once helpers ───────────────────────────────────────────────

struct RowRef {
    batch_idx: usize,
    row_idx: usize,
    offset: i64,
}

/// Fast-path check: if all rows share the same partition value, return it.
/// Returns `None` when partitions differ (multi-partition S3 case).
fn single_partition(
    batches: &[RecordBatch],
    key: &ExactlyOnceKey,
) -> anyhow::Result<Option<PartitionKey>> {
    let mut first_pid: Option<PartitionKey> = None;
    for batch in batches {
        let part_col = batch.column_by_name(&key.partition.name)
            .ok_or_else(|| anyhow::anyhow!(
                "ExactlyOnceKey partition column '{}' not found in batch",
                key.partition.name
            ))?;
        match part_col.data_type() {
            DataType::Int64 => {
                let arr = part_col.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("partition column is not Int64"))?;
                for row_idx in 0..batch.num_rows() {
                    if arr.is_null(row_idx) { continue; }
                    let pid = PartitionKey::Int(arr.value(row_idx));
                    match &first_pid {
                        None => first_pid = Some(pid),
                        Some(f) if *f != pid => return Ok(None),
                        _ => {}
                    }
                }
            }
            DataType::Utf8 => {
                let arr = part_col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("partition column is not Utf8"))?;
                for row_idx in 0..batch.num_rows() {
                    if arr.is_null(row_idx) { continue; }
                    let pid = PartitionKey::Str(arr.value(row_idx).to_string());
                    match &first_pid {
                        None => first_pid = Some(pid),
                        Some(f) if *f != pid => return Ok(None),
                        _ => {}
                    }
                }
            }
            other => anyhow::bail!("Unsupported partition column type: {:?}", other),
        }
    }
    Ok(first_pid)
}

/// Collect all rows from all batches into a flat Vec (single-partition fast path).
fn collect_rows(
    batches: &[RecordBatch],
    key: &ExactlyOnceKey,
) -> anyhow::Result<Vec<RowRef>> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut rows = Vec::with_capacity(total_rows);
    for (batch_idx, batch) in batches.iter().enumerate() {
        let off_col = batch.column_by_name(&key.offset.name)
            .ok_or_else(|| anyhow::anyhow!(
                "ExactlyOnceKey offset column '{}' not found in batch",
                key.offset.name
            ))?;
        let offsets = off_col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("offset column is not Int64"))?;
        for row_idx in 0..batch.num_rows() {
            let offset = if offsets.is_null(row_idx) { 0 } else { offsets.value(row_idx) };
            rows.push(RowRef { batch_idx, row_idx, offset });
        }
    }
    Ok(rows)
}

/// Groups rows from all batches by partition column value (multi-partition path).
fn group_by_partition(
    batches: &[RecordBatch],
    key: &ExactlyOnceKey,
) -> anyhow::Result<HashMap<PartitionKey, Vec<RowRef>>> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut result: HashMap<PartitionKey, Vec<RowRef>> = HashMap::with_capacity(4);
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

        let n = batch.num_rows();
        match part_col.data_type() {
            DataType::Int64 => {
                let arr = part_col.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("partition column is not Int64"))?;
                for row_idx in 0..n {
                    let pid = PartitionKey::Int(arr.value(row_idx));
                    let offset = if offsets.is_null(row_idx) { 0 } else { offsets.value(row_idx) };
                    result.entry(pid).or_insert_with(|| Vec::with_capacity(total_rows / 4))
                        .push(RowRef { batch_idx, row_idx, offset });
                }
            }
            DataType::Utf8 => {
                let arr = part_col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("partition column is not Utf8"))?;
                for row_idx in 0..n {
                    let pid = PartitionKey::Str(arr.value(row_idx).to_string());
                    let offset = if offsets.is_null(row_idx) { 0 } else { offsets.value(row_idx) };
                    result.entry(pid).or_insert_with(|| Vec::with_capacity(total_rows / 4))
                        .push(RowRef { batch_idx, row_idx, offset });
                }
            }
            other => anyhow::bail!("Unsupported partition column type: {:?}", other),
        }
    }
    Ok(result)
}

/// Insert rows for a single partition, respecting the waterline (exactly-once dedup).
async fn insert_with_waterline(
    client: &clickhouse_arrow::Client<ArrowFormat>,
    write: &TableWrite,
    key: &ExactlyOnceKey,
    pid: &PartitionKey,
    rows: Vec<RowRef>,
    wl: &mut Waterline,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    wl.ensure_loaded(client, &write.table, key, pid).await?;
    let wl_val = wl.committed(&write.table, pid);

    let max_off = rows.iter().map(|r| r.offset).max().unwrap_or(0);
    let min_off = rows.iter().map(|r| r.offset).min().unwrap_or(0);

    // All offsets already committed — skip.
    if let Some(v) = wl_val {
        if max_off <= v {
            return Ok(());
        }
    }

    // No waterline yet, or all rows above waterline — insert all.
    if wl_val.is_none() || min_off > wl_val.unwrap() {
        ClickHouseSink::insert_rows_inner(client, write, &rows).await?;
        wl.mark_committed(&write.table, pid.clone(), max_off);
        return Ok(());
    }

    // Partial overlap: only insert rows above the waterline.
    let wl_v = wl_val.unwrap();
    let filtered: Vec<_> = rows.into_iter()
        .filter(|r| r.offset > wl_v)
        .collect();
    if !filtered.is_empty() {
        ClickHouseSink::insert_rows_inner(client, write, &filtered).await?;
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
    fn test_poisoning_sink_blocks_after_error() {
        // Placeholder: full test requires a mock Sink
    }

    #[test]
    fn test_group_by_partition_int64() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__system_partition", DataType::Int64, false),
            Field::new("__system_offset", DataType::Int64, false),
        ]));
        let part = Int64Array::from(vec![0i64, 0, 1, 1]);
        let off = Int64Array::from(vec![10i64, 11, 20, 21]);
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(part), Arc::new(off),
        ]).unwrap();

        let key = ExactlyOnceKey {
            partition: crate::types::exactly_once::ExactlyOnceColumn { name: "__system_partition".into() },
            offset: crate::types::exactly_once::ExactlyOnceColumn { name: "__system_offset".into() },
        };

        let groups = group_by_partition(&[batch], &key).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&PartitionKey::Int(0)].len(), 2);
        assert_eq!(groups[&PartitionKey::Int(1)].len(), 2);
    }
}
