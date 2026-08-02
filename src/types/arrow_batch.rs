use arrow::record_batch::RecordBatch;

/// Minimal metadata carried alongside Arrow record batches through the pipeline.
///
/// Only `dlq_flag` is load-bearing (routes to DLQ vs main table).
/// `batch_id` is used in a `tracing::debug!` for traceability.
/// `table_name`, `partition_id`, `created_at` were never read — removed.
#[derive(Debug, Clone)]
pub struct BatchMeta {
    /// When true, route to DLQ table instead of main table
    pub dlq_flag: bool,
    /// Monotonic batch identifier for tracing (`u64` — no heap)
    pub batch_id: u64,
}

/// The universal transport object flowing source -> middleware -> sink.
/// RecordBatch uses Arc internally - cloning is cheap (ref-count bump).
#[derive(Debug, Clone)]
pub struct ArrowBatch {
    pub batch: RecordBatch,
    pub meta: BatchMeta,
}
