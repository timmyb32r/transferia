use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use object_store::path::Path;
use tokio_util::sync::CancellationToken;

use crate::metrics::SourceCounters;
use transferia_core::data::message::{Message, MessageMeta, SourceBatch};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};

pub(super) struct S3Source {
    store: Arc<dyn object_store::ObjectStore>,
    keys: Arc<Vec<Path>>,
    timeout: Duration,
    cancellation: CancellationToken,
    memory: PipelineMemory,
    next: usize,
    counters: Arc<SourceCounters>,
    parquet: Option<(Arc<str>, usize, arrow::datatypes::SchemaRef)>,
    parquet_key: Option<Path>,
    parquet_stream: Option<
        parquet::arrow::async_reader::ParquetRecordBatchStream<
            parquet::arrow::async_reader::ParquetObjectReader,
        >,
    >,
}

impl S3Source {
    pub(super) fn new(
        store: Arc<dyn object_store::ObjectStore>,
        keys: Arc<Vec<Path>>,
        timeout: Duration,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
        parquet: Option<(Arc<str>, usize, arrow::datatypes::SchemaRef)>,
    ) -> Self {
        Self {
            store,
            keys,
            timeout,
            cancellation,
            memory,
            next: 0,
            counters,
            parquet,
            parquet_key: None,
            parquet_stream: None,
        }
    }

    async fn parquet_batch(
        &mut self,
    ) -> transferia_core::failure::DataPlaneResult<Option<SourceBatch>> {
        let Some((table, batch_rows, expected_schema)) = self.parquet.clone() else {
            return Ok(None);
        };
        let batch = loop {
            if let Some(stream) = self.parquet_stream.as_mut() {
                let next = tokio::select! {
                    biased;
                    () = self.cancellation.cancelled() => {
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "S3 Parquet read cancelled"
                        )));
                    }
                    result = tokio::time::timeout(self.timeout, stream.next()) => result,
                };
                match next {
                    Ok(Some(Ok(batch))) => break batch,
                    Ok(Some(Err(error))) => {
                        let key = self
                            .parquet_key
                            .as_ref()
                            .map_or("<unknown>", Path::as_ref);
                        return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                            "S3 Parquet decoding failed for '{key}': {error}"
                        )));
                    }
                    Ok(None) => {
                        self.parquet_stream = None;
                        self.parquet_key = None;
                    }
                    Err(_) => {
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "S3 Parquet batch read timed out"
                        )))
                    }
                }
            } else {
                let Some(key) = self.keys.get(self.next) else {
                    return Ok(Some(SourceBatch::Finished));
                };
                let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
                    Arc::clone(&self.store),
                    key.clone(),
                );
                let builder = tokio::select! {
                    biased;
                    () = self.cancellation.cancelled() => {
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "S3 Parquet read cancelled"
                        )));
                    }
                    result = tokio::time::timeout(
                        self.timeout,
                        parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader),
                    ) => result,
                }
                .map_err(|_| {
                    DataPlaneFailure::retryable(anyhow::anyhow!(
                        "S3 Parquet metadata read '{key}' timed out"
                    ))
                })?
                .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                self.parquet_stream = Some(
                    builder
                        .with_batch_size(batch_rows)
                        .build()
                        .map_err(|error| DataPlaneFailure::fatal(error.into()))?,
                );
                self.parquet_key = Some(key.clone());
                self.next += 1;
            }
        };
        if batch.schema() != expected_schema {
            let key = self
                .parquet_key
                .as_ref()
                .map_or("<unknown>", Path::as_ref);
            return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "S3 Parquet object '{key}' has schema {:?}, expected {:?}",
                batch.schema(),
                expected_schema
            )));
        }
        let bytes = batch.get_array_memory_size();
        let rows = batch.num_rows() as u64;
        let memory = self.memory.reserve_progress_source(bytes).await;
        self.counters.add_messages(rows);
        self.counters.add_decompressed_bytes(bytes as u64);
        Ok(Some(SourceBatch::Typed {
            tables: vec![TableData::new(
                table,
                false,
                batch,
                SystemColumns::default(),
            )],
            source_rows: rows,
            commit_marker: Some(CommitMarker::new(self.next as i64)),
            memory: vec![memory],
        }))
    }
}

impl Source for S3Source {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.parquet.is_some() {
                return self.parquet_batch().await?.ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "S3 Parquet reader mode disappeared"
                    ))
                });
            }
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
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}
