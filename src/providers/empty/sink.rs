use core::sync::atomic::{AtomicU64, Ordering};

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
    #[must_use]
    pub const fn new() -> Self {
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
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
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

    fn as_any(&self) -> &dyn core::any::Any { self }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[tokio::test]
    async fn empty_sink_counts_rows() -> anyhow::Result<()> {
        let sink = EmptySink::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])?;

        let write = TableWrite {
            table: "test".into(),
            batches: vec![batch],
            exactly_once_key: None,
            message_count: 0,
        };

        sink.write(write).await?;
        anyhow::ensure!(
            sink.rows_written() == 3,
            "expected 3 rows, got {}",
            sink.rows_written(),
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_sink_accumulates_across_writes() -> anyhow::Result<()> {
        let sink = EmptySink::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

        for n in 1..=5 {
            let arr = Int64Array::from(vec![1; n]);
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])?;
            let write = TableWrite { table: "t".into(), batches: vec![batch], exactly_once_key: None, message_count: 0 };
            sink.write(write).await?;
        }
        // 1+2+3+4+5 = 15
        anyhow::ensure!(
            sink.rows_written() == 15,
            "expected 15 rows, got {}",
            sink.rows_written(),
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_sink_reset_zeros_counters() -> anyhow::Result<()> {
        let sink = EmptySink::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![1; 10]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])?;
        sink.write(TableWrite { table: "t".into(), batches: vec![batch], exactly_once_key: None, message_count: 0 }).await?;
        anyhow::ensure!(
            sink.rows_written() == 10,
            "expected 10 rows, got {}",
            sink.rows_written(),
        );
        sink.reset();
        anyhow::ensure!(
            sink.rows_written() == 0,
            "expected 0 rows after reset, got {}",
            sink.rows_written(),
        );
        Ok(())
    }
}
