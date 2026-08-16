use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use object_store::path::Path;
use tokio_util::sync::CancellationToken;

use crate::core::data::message::{Message, MessageMeta, SourceBatch};
use crate::core::failure::DataPlaneFailure;
use crate::core::memory::PipelineMemory;
use crate::core::source::{CommitMarker, Source};
use crate::metrics::SourceCounters;

pub(super) struct S3Source {
    store: Arc<dyn object_store::ObjectStore>,
    keys: Arc<Vec<Path>>,
    timeout: Duration,
    cancellation: CancellationToken,
    memory: PipelineMemory,
    next: usize,
    counters: Arc<SourceCounters>,
}

impl S3Source {
    pub(super) const fn new(
        store: Arc<dyn object_store::ObjectStore>,
        keys: Arc<Vec<Path>>,
        timeout: Duration,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
    ) -> Self {
        Self {
            store,
            keys,
            timeout,
            cancellation,
            memory,
            next: 0,
            counters,
        }
    }
}

impl Source for S3Source {
    fn read_batch(&mut self) -> BoxFuture<'_, crate::core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let Some(key) = self.keys.get(self.next) else {
                return Ok(SourceBatch::Finished);
            };
            let result = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => {
                    return Err(DataPlaneFailure::retryable(anyhow::anyhow!("S3 read cancelled")));
                }
                result = tokio::time::timeout(self.timeout, self.store.get(key)) => {
                    result
                        .map_err(|_| DataPlaneFailure::retryable(anyhow::anyhow!("S3 GET '{key}' timed out")))?
                        .map_err(|error| DataPlaneFailure::retryable(error.into()))?
                }
            };
            let payload = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => {
                    return Err(DataPlaneFailure::retryable(anyhow::anyhow!("S3 read cancelled")));
                }
                result = tokio::time::timeout(self.timeout, result.bytes()) => {
                    result
                        .map_err(|_| DataPlaneFailure::retryable(anyhow::anyhow!("S3 body read '{key}' timed out")))?
                        .map_err(|error| DataPlaneFailure::retryable(error.into()))?
                }
            };
            let lease = self.memory.reserve_progress_source(payload.len()).await;
            let offset =
                i64::try_from(self.next).map_err(|error| DataPlaneFailure::fatal(error.into()))?;
            self.next += 1;
            self.counters.add_messages(1);
            self.counters.add_compressed_bytes(payload.len() as u64);
            self.counters.add_decompressed_bytes(payload.len() as u64);
            Ok(SourceBatch::Raw {
                messages: vec![Message {
                    value: payload,
                    meta: MessageMeta {
                        topic: Some(Arc::from(key.as_ref())),
                        partition: Some(0),
                        offset: Some(offset),
                        write_timestamp_ms: None,
                    },
                }],
                commit_marker: Some(CommitMarker::new(
                    i64::try_from(self.next)
                        .map_err(|error| DataPlaneFailure::fatal(error.into()))?,
                )),
                memory: vec![lease],
            })
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, crate::core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}
