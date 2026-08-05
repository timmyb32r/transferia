pub mod json_serializer;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;

/// Converts Arrow [`RecordBatch`]es into serialized output format.
///
/// The inverse of the parser: takes columnar data and produces byte sequences
/// suitable for writing to sinks (S3 objects, YDS messages).
pub trait Serializer: Send + Sync {
    /// Serialize a single RecordBatch into a `Vec<u8>`.
    /// Each implementation defines its own output format (NDJSON, Parquet, etc.).
    fn serialize_batch(&self, batch: &RecordBatch) -> anyhow::Result<Bytes>;
}
