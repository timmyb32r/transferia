use arrow::record_batch::RecordBatch;

/// Metadata carried alongside Arrow record batches through the pipeline.
#[derive(Debug, Clone)]
pub struct BatchMeta {
    /// Target ClickHouse table name (set by sink based on dlq_flag)
    pub table_name: String,
    /// Source YDB partition ID
    pub partition_id: i64,
    /// When true, route to DLQ table instead of main table
    pub dlq_flag: bool,
    /// UUID for exactly-once deduplication
    pub batch_id: String,
    /// (partition_id, offset) pairs for offset commit tracking
    pub offsets: Vec<(i64, i64)>,
    /// Batch creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// The universal transport object flowing source -> middleware -> sink.
/// RecordBatch uses Arc internally - cloning is cheap (ref-count bump).
#[derive(Debug, Clone)]
pub struct ArrowBatch {
    pub batch: RecordBatch,
    pub meta: BatchMeta,
}
