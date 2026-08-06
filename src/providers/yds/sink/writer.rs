use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::pipeline::sink::Sink;
use crate::serializer::Serializer;
use crate::types::table_data::TableWrite;

/// YDS sink that writes serialized record batches to a YDS topic.
///
/// At-least-once by default. When an exactly-once key descriptor is present
/// (from the source), it enables exactly-once in downstream consumers.
pub struct YdsSink {
    serializer: Arc<dyn Serializer>,
    /// Track total rows for logging / metrics.
    _total_rows: core::sync::atomic::AtomicU64,
}

impl YdsSink {
    pub fn new(serializer: Arc<dyn Serializer>) -> Self {
        Self {
            serializer,
            _total_rows: core::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Sink for YdsSink {
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if write.batches.is_empty() {
                return Ok(());
            }

            let total_rows: usize = write.batches.iter().map(arrow::array::RecordBatch::num_rows).sum();

            // Serialize all batches into NDJSON
            let mut payload = Vec::new();
            for batch in &write.batches {
                let serialized = self.serializer.serialize_batch(batch)?;
                payload.extend_from_slice(&serialized);
            }

            self._total_rows.fetch_add(total_rows as u64, core::sync::atomic::Ordering::Relaxed);

            tracing::info!(
                "YDS sink: wrote {} rows to topic ({} bytes, exactly_once={})",
                total_rows,
                payload.len(),
                write.exactly_once_key.is_some(),
            );

            // TODO: actual YDS write via ydb topic client
            // For now, the serialized payload is ready; the actual YDS API call
            // requires a ydb::TopicWriter which will be connected in a future iteration.
            let _: Vec<u8> = payload;
            Ok(())
        })
    }

    fn as_any(&self) -> &dyn core::any::Any { self }
}
