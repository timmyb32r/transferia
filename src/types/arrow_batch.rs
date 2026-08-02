use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Metadata carried alongside Arrow record batches through the pipeline.
#[derive(Debug, Clone)]
pub struct BatchMeta {
    /// Target ClickHouse table name (Arc<str> — cheap ref-counted clone)
    pub table_name: Arc<str>,
    /// Source YDB partition ID
    pub partition_id: i64,
    /// When true, route to DLQ table instead of main table
    pub dlq_flag: bool,
    /// Monotonic batch identifier for tracing (`u64` — no heap)
    pub batch_id: u64,
    /// Batch creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// The universal transport object flowing source -> middleware -> sink.
/// RecordBatch uses Arc internally - cloning is cheap (ref-count bump).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArrowBatch {
    pub batch: RecordBatch,
    pub meta: BatchMeta,
}
