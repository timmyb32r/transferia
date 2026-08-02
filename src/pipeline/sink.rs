use std::future::Future;

use arrow::record_batch::RecordBatch;

use crate::types::arrow_batch::ArrowBatch;

pub trait Sink: Send + Sync {
    fn write_batch(
        &self,
        batch: &ArrowBatch,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        dlq_flag: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
