use std::sync::Arc;

use futures_util::future::BoxFuture;
use object_store::ObjectStore;
use object_store::path::Path;

use crate::pipeline::sink::Sink;
use crate::serializer::Serializer;
use crate::types::table_data::TableWrite;

/// S3 sink that writes serialized record batches to S3-compatible storage.
///
/// **Snapshot mode** (ch→s3): one object per flush, key includes offset range
/// for exactly-once idempotency.
///
/// **Stream mode** (yds→s3): rolling files keyed by partition and offset range.
pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    serializer: Arc<dyn Serializer>,
}

impl S3Sink {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, serializer: Arc<dyn Serializer>) -> Self {
        Self { store, prefix, serializer }
    }
}

impl Sink for S3Sink {
    fn write(&self, write: TableWrite) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if write.batches.is_empty() {
                return Ok(());
            }

            // Build the serialized payload from all batches
            let mut payload = Vec::new();
            for batch in &write.batches {
                let serialized = self.serializer.serialize_batch(batch)?;
                payload.extend_from_slice(&serialized);
            }

            // Exactly-once idempotent object keys (derived from the key descriptor
            // + the batch's offset range) land in a later stage. Until then, use
            // a timestamp-based key (snapshot mode).
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let key = format!("{}/{}_{:016x}.jsonl", self.prefix, write.table, ts);

            let path = Path::from(key.clone());
            self.store.put(&path, payload.into()).await
                .map_err(|e| anyhow::anyhow!("S3 put '{key}' failed: {e}"))?;

            tracing::info!(
                "S3: wrote {} rows to '{}' (key: {})",
                write.batches.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>(),
                write.table,
                key,
            );
            Ok(())
        })
    }

    fn as_any(&self) -> &dyn core::any::Any { self }
}
