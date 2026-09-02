use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt as _};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

use super::config::{ParquetCompression, ParquetRowGroupConfig, S3SinkConfig};
use super::upload::{ObjectUploader, S3Uploader, UploadError};
use crate::metrics::SinkCounters;
use transferia_core::delivery::DeliveryDiscovery;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_core::{project_sink_batch, ProjectedSinkBatch};

pub(super) struct S3ParquetSink {
    config: S3SinkConfig,
    uploader: Arc<S3Uploader>,
    counters: Arc<SinkCounters>,
    partition_id: i64,
    keep_system_columns: bool,

    discovery: Arc<DeliveryDiscovery>,
}

impl S3ParquetSink {
    pub(super) const fn new(
        config: S3SinkConfig,
        uploader: Arc<S3Uploader>,
        counters: Arc<SinkCounters>,
        partition_id: i64,
        keep_system_columns: bool,
        discovery: Arc<DeliveryDiscovery>,
    ) -> Self {
        Self {
            config,
            uploader,
            counters,
            partition_id,
            keep_system_columns,
            discovery,
        }
    }

    async fn write(&self, delivery: Delivery, io: &SinkIo) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.keep_system_columns == self.discovery.keep_system_columns,
            "S3 Parquet projection does not match delivery discovery"
        );
        let (compression, row_group) = self.config.parquet_settings()?;
        let source_messages = delivery.meta.source_messages;
        let concurrency = self.config.upload.max_in_flight_objects;
        let mut jobs = Vec::new();
        for output in delivery.outputs {
            let ProjectedSinkBatch::AppendOnly(batch) =
                project_sink_batch(&self.discovery, &output).map_err(|error| {
                    anyhow::anyhow!(
                        "S3 Parquet delivery validation failed for dataset '{}': {error}",
                        output.table
                    )
                })?
            else {
                anyhow::bail!("S3 cannot serialize a changelog dataset")
            };
            for batch in split_for_object_limits(
                batch,
                self.config.rotation.max_rows,
                self.config.rotation.max_bytes.0,
            ) {
                jobs.push((Arc::clone(&output.table), batch));
            }
        }
        let uploads = jobs.into_iter().enumerate().map(|(index, (table, batch))| {
            let output_rows = batch.num_rows() as u64;
            let key = object_key(
                &self.config.path_prefix,
                &table,
                self.partition_id,
                delivery.id.get(),
                index,
            );
            let row_group = row_group.clone();
            let uploader = Arc::clone(&self.uploader);
            let cancellation = io.cancellation.clone();
            async move {
                let key = key?;
                let payload = tokio::task::spawn_blocking(move || {
                    encode_parquet(&batch, compression, &row_group)
                })
                .await
                .map_err(|error| anyhow::anyhow!("Parquet encoder task failed: {error}"))??;
                let bytes = payload.len() as u64;
                uploader
                    .upload(&key, Bytes::from(payload), &cancellation)
                    .await
                    .map_err(upload_error)?;
                Ok::<_, anyhow::Error>((output_rows, bytes))
            }
        });
        let mut uploads = stream::iter(uploads).buffer_unordered(concurrency);
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        while let Some(result) = uploads.next().await {
            let (uploaded_rows, uploaded_bytes) = result?;
            rows = rows.saturating_add(uploaded_rows);
            bytes = bytes.saturating_add(uploaded_bytes);
        }
        self.counters.add_rows(rows);
        self.counters.add_bytes(bytes);
        self.counters.add_flush();
        self.counters.add_source_messages(source_messages);
        io.events
            .send(SinkEvent::CommittedThrough(delivery.id))
            .await
            .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
        Ok(())
    }
}

fn split_for_object_limits(
    batch: arrow::record_batch::RecordBatch,
    max_rows: usize,
    max_bytes: usize,
) -> Vec<arrow::record_batch::RecordBatch> {
    if batch.num_rows() == 0 {
        return vec![batch];
    }
    let rows_by_bytes = max_bytes
        .saturating_mul(batch.num_rows())
        .checked_div(batch.get_array_memory_size().max(1))
        .unwrap_or(1)
        .max(1);
    let rows_per_object = max_rows.min(rows_by_bytes).max(1);
    (0..batch.num_rows())
        .step_by(rows_per_object)
        .map(|offset| batch.slice(offset, rows_per_object.min(batch.num_rows() - offset)))
        .collect()
}

impl Sink for S3ParquetSink {
    fn run(
        self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            loop {
                let delivery = tokio::select! {
                    biased;
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv() => delivery,
                };
                let Some(delivery) = delivery else {
                    return Ok(());
                };
                self.write(delivery, &io)
                    .await
                    .map_err(DataPlaneFailure::retryable_or_passthrough)?;
            }
        })
    }
}

fn encode_parquet(
    batch: &arrow::record_batch::RecordBatch,
    compression: Compression,
    row_group: &ParquetRowGroupConfig,
) -> anyhow::Result<Vec<u8>> {
    let estimated_rows_by_bytes = if batch.num_rows() == 0 {
        row_group.max_rows
    } else {
        row_group
            .max_bytes
            .0
            .saturating_mul(batch.num_rows())
            .checked_div(batch.get_array_memory_size().max(1))
            .unwrap_or(1)
            .max(1)
    };
    let rows_per_group = row_group.max_rows.min(estimated_rows_by_bytes).max(1);
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(rows_per_group)
        .build();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(output)
}

fn object_key(
    prefix: &str,
    table: &str,
    partition_id: i64,
    delivery_id: u64,
    output_index: usize,
) -> anyhow::Result<String> {
    let relative =
        format!("{table}/partition={partition_id}/delivery={delivery_id}+{output_index}.parquet");
    let key = if prefix.is_empty() {
        relative
    } else {
        format!("{prefix}/{relative}")
    };
    object_store::path::Path::parse(&key)?;
    Ok(key)
}

fn upload_error(error: UploadError) -> anyhow::Error {
    match error {
        UploadError::Retryable(error) | UploadError::Permanent(error) => error,
        UploadError::Cancelled => anyhow::anyhow!("S3 Parquet upload cancelled"),
    }
}

pub(super) fn compression(codec: ParquetCompression) -> Compression {
    match codec {
        ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
        ParquetCompression::Snappy => Compression::SNAPPY,
        ParquetCompression::Gzip => Compression::GZIP(GzipLevel::default()),
        ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
    }
}

#[cfg(test)]
#[path = "tests/parquet.rs"]
mod tests;
