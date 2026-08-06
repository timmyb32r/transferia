use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::future::BoxFuture;

use crate::pipeline::sink::Sink;
use crate::types::table_data::TableWrite;

/// A sink that counts and discards all data — for measuring pipeline throughput
/// without being bottlenecked by the destination.
///
/// Exposes row/byte counters via [`EmptySink::rows_written`] and
/// [`EmptySink::bytes_processed`] for benchmarking.
pub struct EmptySink {
    rows: AtomicU64,
    bytes: AtomicU64,
}

impl EmptySink {
    pub fn new() -> Self {
        Self { rows: AtomicU64::new(0), bytes: AtomicU64::new(0) }
    }

    /// Total rows written since creation.
    pub fn rows_written(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// Total bytes (estimated from Arrow buffer sizes) processed since creation.
    pub fn bytes_processed(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Reset counters to zero.
    pub fn reset(&self) {
        self.rows.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for EmptySink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for EmptySink {
    fn write<'a>(&'a self, write: TableWrite) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let total_rows: u64 = write
                .batches
                .iter()
                .map(|b| b.num_rows() as u64)
                .sum();
            let total_bytes: u64 = write
                .batches
                .iter()
                .flat_map(|b| b.columns().iter())
                .map(|arr| arr.get_buffer_memory_size() as u64)
                .sum();

            self.rows.fetch_add(total_rows, Ordering::Relaxed);
            self.bytes.fetch_add(total_bytes, Ordering::Relaxed);

            tracing::debug!(
                "empty-sink: wrote {} rows (~{} bytes) into '{}'",
                total_rows,
                total_bytes,
                write.table,
            );
            Ok(())
        })
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[tokio::test]
    async fn empty_sink_counts_rows() {
        let sink = EmptySink::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![1i64, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let write = TableWrite {
            table: "test".into(),
            batches: vec![batch],
            exactly_once_key: None,
        };

        sink.write(write).await.unwrap();
        assert_eq!(sink.rows_written(), 3);
    }

    #[tokio::test]
    async fn empty_sink_accumulates_across_writes() {
        let sink = EmptySink::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

        for n in 1..=5 {
            let arr = Int64Array::from(vec![1i64; n]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
            let write = TableWrite { table: "t".into(), batches: vec![batch], exactly_once_key: None };
            sink.write(write).await.unwrap();
        }
        // 1+2+3+4+5 = 15
        assert_eq!(sink.rows_written(), 15);
    }

    #[tokio::test]
    async fn empty_sink_reset_zeros_counters() {
        let sink = EmptySink::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![1i64; 10]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        sink.write(TableWrite { table: "t".into(), batches: vec![batch], exactly_once_key: None }).await.unwrap();
        assert_eq!(sink.rows_written(), 10);
        sink.reset();
        assert_eq!(sink.rows_written(), 0);
    }
}
