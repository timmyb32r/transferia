use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, MultipartUpload, ObjectStore};
use tokio_util::sync::CancellationToken;

use super::config::{RetryConfig, UploadConfig};
use crate::metrics::SinkCounters;
use crate::pipeline::retry::{jittered_retry_delay, stable_retry_seed};
use crate::pipeline::PipelineFailure;

// Keep abort cleanup below the actor's five-second upload-drain grace period.
const MULTIPART_ABORT_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_MULTIPART_PARTS: usize = 10_000;

#[derive(Clone, Copy)]
enum OperationPhase {
    Put,
    Multipart,
}

#[derive(Debug)]
pub enum UploadError {
    Retryable(anyhow::Error),
    Permanent(anyhow::Error),
    Cancelled,
}

pub trait ObjectUploader: Send + Sync {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), UploadError>>;
}

pub struct S3Uploader {
    store: Arc<dyn ObjectStore>,
    config: UploadConfig,
}

impl S3Uploader {
    pub fn new(store: Arc<dyn ObjectStore>, config: UploadConfig) -> Self {
        Self { store, config }
    }

    async fn upload_once(
        &self,
        key: &str,
        payload: Bytes,
        cancellation: &CancellationToken,
    ) -> Result<(), UploadError> {
        let path = Path::parse(key).map_err(|source| {
            classify_object_store_error(
                ObjectStoreError::InvalidPath { source },
                OperationPhase::Put,
            )
        })?;
        if payload.len() < self.config.multipart_threshold.0 {
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(UploadError::Cancelled),
                result = object_store_operation(
                    self.config.operation_timeout.0,
                    "PUT",
                    key,
                    OperationPhase::Put,
                    self.store.put(&path, payload.into()),
                ) => result,
            };
            result?;
            return Ok(());
        }

        validate_multipart_layout(key, payload.len(), self.config.part_size.0)?;

        let mut upload = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(UploadError::Cancelled),
            result = object_store_operation(
                self.config.operation_timeout.0,
                "multipart initiation",
                key,
                OperationPhase::Put,
                self.store.put_multipart(&path),
            ) => result?,
        };
        upload_multipart(upload.as_mut(), &self.config, key, payload, cancellation).await
    }
}

fn validate_multipart_layout(
    key: &str,
    payload_len: usize,
    part_size: usize,
) -> Result<(), UploadError> {
    if part_size == 0 {
        return Err(UploadError::Permanent(anyhow::anyhow!(
            "S3 upload.part_size must be positive"
        )));
    }
    let part_count = payload_len.div_ceil(part_size);
    if part_count > MAX_MULTIPART_PARTS {
        return Err(UploadError::Permanent(anyhow::anyhow!(
            "S3 object '{key}' requires {part_count} multipart parts, exceeding the S3 limit of {MAX_MULTIPART_PARTS}; increase upload.part_size"
        )));
    }
    Ok(())
}

async fn upload_multipart(
    upload: &mut dyn MultipartUpload,
    config: &UploadConfig,
    key: &str,
    payload: Bytes,
    cancellation: &CancellationToken,
) -> Result<(), UploadError> {
    let mut parts = FuturesUnordered::new();
    for start in (0..payload.len()).step_by(config.part_size.0) {
        while parts.len() >= config.parallel_parts {
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    drop(parts);
                    abort_multipart(upload, key).await;
                    return Err(UploadError::Cancelled);
                }
                result = parts.next() => result,
            };
            match result {
                Some(Ok(())) => {}
                None => break,
                Some(Err(error)) => {
                    drop(parts);
                    abort_multipart(upload, key).await;
                    return Err(error);
                }
            }
        }
        let end = start.saturating_add(config.part_size.0).min(payload.len());
        parts.push(object_store_operation(
            config.operation_timeout.0,
            "multipart part upload",
            key,
            OperationPhase::Multipart,
            upload.put_part(payload.slice(start..end).into()),
        ));
    }
    loop {
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                drop(parts);
                abort_multipart(upload, key).await;
                return Err(UploadError::Cancelled);
            }
            result = parts.next() => result,
        };
        match result {
            Some(Ok(())) => {}
            None => break,
            Some(Err(error)) => {
                drop(parts);
                abort_multipart(upload, key).await;
                return Err(error);
            }
        }
    }
    let completed = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            abort_multipart(upload, key).await;
            return Err(UploadError::Cancelled);
        }
        result = object_store_operation(
            config.operation_timeout.0,
            "multipart completion",
            key,
            OperationPhase::Multipart,
            upload.complete(),
        ) => result,
    };
    if let Err(error) = completed {
        abort_multipart(upload, key).await;
        return Err(error);
    }
    Ok(())
}

async fn abort_multipart(upload: &mut dyn MultipartUpload, key: &str) {
    match tokio::time::timeout(MULTIPART_ABORT_TIMEOUT, upload.abort()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(
                object_key = key,
                "failed to abort S3 multipart upload: {error}"
            );
        }
        Err(_) => {
            tracing::warn!(
                object_key = key,
                timeout_ms = MULTIPART_ABORT_TIMEOUT.as_millis(),
                "timed out aborting S3 multipart upload"
            );
        }
    }
}

async fn object_store_operation<T>(
    timeout: Duration,
    operation: &'static str,
    key: &str,
    phase: OperationPhase,
    future: impl Future<Output = object_store::Result<T>>,
) -> Result<T, UploadError> {
    tokio::time::timeout(timeout, future).await.map_or_else(
        |_| {
            Err(UploadError::Retryable(anyhow::anyhow!(
                "S3 {operation} for '{key}' timed out after {}ms",
                timeout.as_millis()
            )))
        },
        |result| result.map_err(|error| classify_object_store_error(error, phase)),
    )
}

impl ObjectUploader for S3Uploader {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), UploadError>> {
        Box::pin(async move { self.upload_once(key, payload, cancellation).await })
    }
}

fn classify_object_store_error(error: ObjectStoreError, phase: OperationPhase) -> UploadError {
    let permanent = matches!(
        &error,
        ObjectStoreError::PermissionDenied { .. }
            | ObjectStoreError::Unauthenticated { .. }
            | ObjectStoreError::InvalidPath { .. }
            | ObjectStoreError::NotSupported { .. }
            | ObjectStoreError::NotImplemented
            | ObjectStoreError::UnknownConfigurationKey { .. }
            | ObjectStoreError::AlreadyExists { .. }
            | ObjectStoreError::Precondition { .. }
            | ObjectStoreError::NotModified { .. }
    ) || matches!(
        (&error, phase),
        (ObjectStoreError::NotFound { .. }, OperationPhase::Put)
    );
    if permanent {
        UploadError::Permanent(error.into())
    } else {
        UploadError::Retryable(error.into())
    }
}

pub async fn upload_with_retry(
    uploader: Arc<dyn ObjectUploader>,
    retry: RetryConfig,
    key: &str,
    payload: &Bytes,
    cancellation: &CancellationToken,
    counters: &SinkCounters,
) -> Result<(), PipelineFailure> {
    let mut attempt = 1_usize;
    loop {
        if cancellation.is_cancelled() {
            return Err(PipelineFailure::retryable(anyhow::anyhow!(
                "S3 upload cancelled"
            )));
        }
        let started = Instant::now();
        let result = uploader.upload(key, payload.clone(), cancellation).await;
        counters.add_busy(started.elapsed());
        match result {
            Ok(()) => return Ok(()),
            Err(UploadError::Cancelled) => {
                return Err(PipelineFailure::retryable(anyhow::anyhow!(
                    "S3 upload cancelled"
                )));
            }
            Err(UploadError::Permanent(error)) => {
                return Err(PipelineFailure::fatal(
                    error.context(format!("permanent S3 upload failure for '{key}'")),
                ));
            }
            Err(UploadError::Retryable(error)) => {
                if attempt >= retry.max_attempts {
                    return Err(PipelineFailure::retryable(error.context(format!(
                        "S3 upload for '{key}' exhausted {} attempts",
                        retry.max_attempts
                    ))));
                }
                counters.add_retries(1);
                let delay =
                    retry_delay(&retry, u32::try_from(attempt - 1).unwrap_or(u32::MAX), key);
                tracing::warn!(
                    object_key = key,
                    attempt,
                    delay_ms = delay.as_millis(),
                    "retryable S3 upload failure: {error}"
                );
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(PipelineFailure::retryable(anyhow::anyhow!(
                            "S3 upload cancelled"
                        )));
                    }
                    () = tokio::time::sleep(delay) => {}
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

fn retry_delay(config: &RetryConfig, attempt: u32, key: &str) -> Duration {
    let shift = attempt.min(20);
    let base = config
        .initial_backoff
        .0
        .saturating_mul(1_u32 << shift)
        .min(config.max_backoff.0);
    jittered_retry_delay(base, attempt, stable_retry_seed(key.as_bytes()))
}

#[cfg(test)]
#[path = "tests/upload.rs"]
mod tests;
