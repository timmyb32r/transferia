use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use object_store::path::Path;
use tokio_util::sync::CancellationToken;

use crate::metrics::SourceCounters;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::{CommitMarker, Source};
use crate::types::message::{Message, MessageMeta, SourceBatch};

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
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        Box::pin(async move {
            let Some(key) = self.keys.get(self.next) else {
                return Ok(SourceBatch::Finished);
            };
            let result = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => anyhow::bail!("S3 read cancelled"),
                result = tokio::time::timeout(self.timeout, self.store.get(key)) => {
                    result.map_err(|_| anyhow::anyhow!("S3 GET '{key}' timed out"))??
                }
            };
            let payload = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => anyhow::bail!("S3 read cancelled"),
                result = tokio::time::timeout(self.timeout, result.bytes()) => {
                    result.map_err(|_| anyhow::anyhow!("S3 body read '{key}' timed out"))??
                }
            };
            let lease = self.memory.reserve_progress_source(payload.len()).await;
            let offset = i64::try_from(self.next)?;
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
                commit_marker: Some(CommitMarker::new(i64::try_from(self.next)?)),
                memory: vec![lease],
            })
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
