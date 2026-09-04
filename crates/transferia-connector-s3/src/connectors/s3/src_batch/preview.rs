use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
use tokio_util::sync::CancellationToken;
use transferia_registry::{
    SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem,
};

use super::config::S3SourceConfig;

pub async fn preview_message(
    config: &S3SourceConfig,
    max_bytes: usize,
    cancellation: CancellationToken,
) -> anyhow::Result<SourcePreview> {
    anyhow::ensure!(max_bytes > 0, "S3 message preview max_bytes must be positive");
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
