use std::future::Future;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::types::arrow_batch::ArrowBatch;

pub trait Sink: Send + Sync {
    /// Write a single batch. The destination table is taken from `batch.meta`
    /// (`table` + `dlq_flag`).
    fn write_batch(
        &self,
        batch: &ArrowBatch,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Write many batches into `table` (or `table.dlq` when `dlq_flag`).
    fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        table: Arc<str>,
        dlq_flag: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
