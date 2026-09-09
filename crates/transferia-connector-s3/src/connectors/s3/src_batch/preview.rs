use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
use tokio_util::sync::CancellationToken;
use transferia_registry::{SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem};

use super::config::S3SourceConfig;

pub(super) async fn sample_data(
    config: &S3SourceConfig,
    store: Arc<dyn ObjectStore>,
    parser: Arc<dyn transferia_delivery_contracts::parser::ParserFactory>,
    limits: transferia_registry::TableSampleLimits,
    cancellation: CancellationToken,
) -> anyhow::Result<Vec<transferia_core::TableData>> {
    limits.validate()?;
    let prefix = if config.path_prefix.is_empty() { None } else { Some(Path::parse(&config.path_prefix)?) };
    let object = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("S3 sample cancelled"),
        object = async {
            let mut listed = store.list(prefix.as_ref());
            let mut first: Option<object_store::ObjectMeta> = None;
            while let Some(object) = listed.try_next().await? {
                if object.size > 0 && first.as_ref().is_none_or(|current| object.location < current.location) {
                    first = Some(object);
                }
            }
            first.ok_or_else(|| anyhow::anyhow!("S3 path contains no non-empty objects"))
        } => object?,
    };
    let size = usize::try_from(object.size)?;
    limits.check_bytes(size)?;
    // Parse only a complete, version-checked object, never Scan's byte prefix.
    let payload = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("S3 sample cancelled"),
        payload = async {
            let result = store.get_opts(&object.location, GetOptions {
                if_match: object.e_tag.clone(), version: object.version.clone(),
                range: Some(GetRange::Bounded(0..object.size)), ..GetOptions::default()
            }).await?;
            let mut stream = result.into_stream();
            let mut bytes = bytes::BytesMut::with_capacity(size);
            while let Some(chunk) = stream.try_next().await? {
                let next = bytes.len().checked_add(chunk.len()).ok_or_else(|| anyhow::anyhow!("S3 sample byte count overflow"))?;
                limits.check_bytes(next)?;
                anyhow::ensure!(next <= size, "S3 sampled object grew while reading");
                bytes.extend_from_slice(&chunk);
            }
            anyhow::ensure!(bytes.len() == size, "S3 sampled object is incomplete");
            Ok::<_, anyhow::Error>(bytes.freeze())
        } => payload?,
    };
    if config.parser.parquet_batch_rows().is_some() {
        let name: Arc<str> = Arc::from(config.table_name.as_str());
        return tokio::task::spawn_blocking(move || {
            anyhow::ensure!(!cancellation.is_cancelled(), "S3 sample cancelled");
            let input_bytes = payload.len();
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(payload)?;
            admit_parquet_sample(builder.metadata(), input_bytes, limits)?;
            let schema = builder.schema().clone();
            let mut reader = builder.with_batch_size(limits.row_limit).with_limit(limits.row_limit).build()?;
            let batch = reader.next().transpose()?.unwrap_or_else(|| arrow::record_batch::RecordBatch::new_empty(schema));
            limits.check_bytes(input_bytes.checked_add(batch.get_array_memory_size())
                .ok_or_else(|| anyhow::anyhow!("Parquet sample allocation overflow"))?)?;
            anyhow::ensure!(!cancellation.is_cancelled(), "S3 sample cancelled");
            Ok(vec![transferia_core::TableData::new(name, false, batch, Default::default())])
        }).await?;
    }
    let message = transferia_core::data::message::Message {
        value: payload, tombstone: false, key: None, headers: Arc::from([]),
        meta: transferia_core::data::message::MessageMeta {
            topic: Some(Arc::from(object.location.as_ref())), partition: Some(0), offset: Some(0), write_timestamp_ms: None,
        },
    };
    transferia_connector_support::source_sample::parse_sample(parser, vec![message], limits, cancellation).await
}

pub(super) fn admit_parquet_sample(
    metadata: &parquet::file::metadata::ParquetMetaData,
    input_bytes: usize,
    limits: transferia_registry::TableSampleLimits,
) -> anyhow::Result<()> {
    let mut remaining = limits.row_limit;
    let mut bound = input_bytes;
    for group in metadata.row_groups() {
        if remaining == 0 { break; }
        let rows = usize::try_from(group.num_rows())?;
        let selected = rows.min(remaining);
        for column in group.columns() {
            let decoded = usize::try_from(column.uncompressed_size())?;
            let values = usize::try_from(column.num_values())?;
            // A dictionary/byte-array value may occupy the entire decoded
            // column. Repeated nested values can all belong to a single row.
            // Account for page/dictionary storage, output expansion, and the
            // definition/repetition/offset buffers before starting decoding.
            let output_values = if column.column_descr().max_rep_level() == 0 { selected.min(values) } else { values };
            bound = decoded.checked_mul(output_values.checked_add(1).ok_or_else(|| anyhow::anyhow!("Parquet sample allocation overflow"))?)
                .and_then(|size| values.checked_mul(4 * std::mem::size_of::<u64>()).and_then(|indices| size.checked_add(indices)))
                .and_then(|size| bound.checked_add(size))
                .ok_or_else(|| anyhow::anyhow!("Parquet sample allocation overflow"))?;
            limits.check_bytes(bound)?;
        }
        remaining -= selected;
    }
    Ok(())
}

pub async fn preview_message(
    config: &S3SourceConfig,
    max_bytes: usize,
    cancellation: CancellationToken,
) -> anyhow::Result<SourcePreview> {
    anyhow::ensure!(
        max_bytes > 0,
        "S3 message preview max_bytes must be positive"
    );
    anyhow::ensure!(!config.bucket.is_empty(), "s3.bucket must not be empty");
    anyhow::ensure!(config.timeout_ms > 0, "s3.timeout_ms must be positive");
    let prefix = if config.path_prefix.is_empty() {
        None
    } else {
        Some(Path::parse(&config.path_prefix)?)
    };
    preview_first_object(
        config.build_store()?,
        prefix.as_ref(),
        config.timeout(),
        max_bytes,
        cancellation,
    )
    .await
}

pub(super) async fn preview_first_object(
    store: Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
    timeout: Duration,
    max_bytes: usize,
    cancellation: CancellationToken,
) -> anyhow::Result<SourcePreview> {
    let object = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("S3 message preview cancelled"),
        result = tokio::time::timeout(timeout, async {
            let mut listed = store.list(prefix);
            while let Some(object) = listed.try_next().await? {
                if object.size > 0 {
                    return Ok::<_, object_store::Error>(Some(object));
                }
            }
            Ok(None)
        }) => result.map_err(|_| anyhow::anyhow!("S3 message preview listing timed out"))??
            .ok_or_else(|| anyhow::anyhow!("S3 path contains no non-empty objects"))?,
    };
    let preview_bytes = object.size.min(u64::try_from(max_bytes)?);
    let result = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("S3 message preview cancelled"),
        result = tokio::time::timeout(
            timeout,
            store.get_opts(
                &object.location,
                GetOptions {
                    if_match: object.e_tag.clone(),
                    range: Some(GetRange::Bounded(0..preview_bytes)),
                    version: object.version.clone(),
                    ..GetOptions::default()
                },
            ),
        ) => result.map_err(|_| anyhow::anyhow!(
            "S3 message preview GET '{}' timed out",
            object.location,
        ))??,
    };
    let payload = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("S3 message preview cancelled"),
        result = tokio::time::timeout(timeout, result.bytes()) => result.map_err(|_| {
            anyhow::anyhow!("S3 message preview body '{}' timed out", object.location)
        })??,
    };
    anyhow::ensure!(
        payload.len() <= max_bytes,
        "S3 message preview returned {} bytes, exceeding max_bytes={max_bytes}",
        payload.len()
    );
    let object_size = usize::try_from(object.size).ok();
    Ok(SourcePreview {
        payload: payload.to_vec(),
        detection_payloads: vec![payload.to_vec()],
        metadata: SourcePreviewMetadata {
            topic: object.location.to_string(),
            partition: 0,
            partition_session_id: 0,
            offset: 0,
            sequence_number: 0,
            created_at_ms: None,
            written_at_ms: Some(object.last_modified.timestamp_millis()),
            producer_id: String::new(),
            message_group_id: None,
            codec: "s3-object-range".to_owned(),
            compressed_size: payload.len(),
            declared_uncompressed_size: object_size,
            message_metadata: vec![SourcePreviewMetadataItem {
                key: "s3.object_size".to_owned(),
                value: object.size.to_string().into_bytes(),
            }],
            write_session_metadata: BTreeMap::new(),
        },
    })
}
