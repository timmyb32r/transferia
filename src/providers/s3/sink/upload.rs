use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};

use super::config::{RetryConfig, UploadConfig};

#[derive(Debug)]
pub enum UploadError {
    Retryable(anyhow::Error),
    Permanent(anyhow::Error),
}

pub trait ObjectUploader: Send + Sync {
    fn upload<'a>(&'a self, key: &'a str, payload: Bytes)
        -> BoxFuture<'a, Result<(), UploadError>>;
}

pub struct S3Uploader {
    store: Arc<dyn ObjectStore>,
    config: UploadConfig,
}

impl S3Uploader {
    pub fn new(store: Arc<dyn ObjectStore>, config: UploadConfig) -> Self {
        Self { store, config }
    }

    async fn upload_once(&self, key: &str, payload: Bytes) -> object_store::Result<()> {
        let path = Path::parse(key)?;
        if payload.len() < self.config.multipart_threshold.0 {
            self.store.put(&path, payload.into()).await?;
            return Ok(());
        }

        let mut upload = self.store.put_multipart(&path).await?;
        let mut parts = FuturesUnordered::new();
        for start in (0..payload.len()).step_by(self.config.part_size.0) {
            while parts.len() >= self.config.parallel_parts {
                if let Some(Err(error)) = parts.next().await {
                    let _ = upload.abort().await;
                    return Err(error);
                }
            }
            let end = start
                .saturating_add(self.config.part_size.0)
                .min(payload.len());
            parts.push(upload.put_part(payload.slice(start..end).into()));
        }
        while let Some(result) = parts.next().await {
            if let Err(error) = result {
                let _ = upload.abort().await;
                return Err(error);
            }
        }
        upload.complete().await?;
        Ok(())
    }
}

impl ObjectUploader for S3Uploader {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
    ) -> BoxFuture<'a, Result<(), UploadError>> {
        Box::pin(async move {
            self.upload_once(key, payload)
                .await
                .map_err(classify_object_store_error)
        })
    }
}

fn classify_object_store_error(error: ObjectStoreError) -> UploadError {
    let permanent = matches!(
        error,
        ObjectStoreError::PermissionDenied { .. }
            | ObjectStoreError::Unauthenticated { .. }
            | ObjectStoreError::InvalidPath { .. }
            | ObjectStoreError::NotSupported { .. }
            | ObjectStoreError::NotImplemented
            | ObjectStoreError::UnknownConfigurationKey { .. }
    );
    if permanent {
        UploadError::Permanent(error.into())
    } else {
        UploadError::Retryable(error.into())
    }
}

#[derive(Debug)]
pub struct UploadStats {
    pub retries: u64,
    pub busy: Duration,
}

pub async fn upload_with_retry(
    uploader: Arc<dyn ObjectUploader>,
    retry: RetryConfig,
    key: String,
    payload: Bytes,
) -> anyhow::Result<UploadStats> {
    let mut attempt = 0_u32;
    let mut retries = 0_u64;
    let mut busy = Duration::ZERO;
    loop {
        let started = Instant::now();
        let result = uploader.upload(&key, payload.clone()).await;
        busy += started.elapsed();
        match result {
            Ok(()) => return Ok(UploadStats { retries, busy }),
            Err(UploadError::Permanent(error)) => {
                return Err(error.context(format!("permanent S3 upload failure for '{key}'")));
            }
            Err(UploadError::Retryable(error)) => {
                retries = retries.saturating_add(1);
                let delay = retry_delay(&retry, attempt, &key);
                tracing::warn!(
                    object_key = key,
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis(),
                    "retryable S3 upload failure: {error}"
                );
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

fn retry_delay(config: &RetryConfig, attempt: u32, key: &str) -> Duration {
    let shift = attempt.min(20);
    let base_ms = config
        .initial_backoff
        .0
        .as_millis()
        .saturating_mul(1_u128 << shift);
    let capped_ms = base_ms.min(config.max_backoff.0.as_millis());
    let hash = key.bytes().fold(u64::from(attempt) + 1, |state, byte| {
        state
            .wrapping_mul(1_099_511_628_211)
            .wrapping_add(u64::from(byte))
    });
    let jitter_percent = 80 + hash % 41;
    let jittered = capped_ms.saturating_mul(u128::from(jitter_percent)) / 100;
    Duration::from_millis(u64::try_from(jittered).unwrap_or(u64::MAX))
}
