use std::sync::Arc;

use arrow::record_batch::RecordBatch;

/// Minimal metadata carried alongside Arrow record batches through the pipeline.
///
/// `dlq_flag` routes to the DLQ vs main table; `table` is the base destination
/// table name (the DLQ target is `<table>.dlq`). `batch_id` is used in a
/// `tracing::debug!` for traceability.
#[derive(Debug, Clone)]
pub struct BatchMeta {
    /// When true, route to DLQ table (`<table>.dlq`) instead of the main table
    pub dlq_flag: bool,
    /// Monotonic batch identifier for tracing (`u64` — no heap)
    pub batch_id: u64,
    /// Base destination table name (resolved by the parser). Cheap to clone (`Arc`).
    pub table: Arc<str>,
}

/// The universal transport object flowing source -> middleware -> sink.
/// RecordBatch uses Arc internally - cloning is cheap (ref-count bump).
#[derive(Debug, Clone)]
pub struct ArrowBatch {
    pub batch: RecordBatch,
    pub meta: BatchMeta,
}
