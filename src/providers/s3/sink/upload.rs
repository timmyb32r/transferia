use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, MultipartUpload, ObjectStore};
use tokio_util::sync::CancellationToken;

use super::config::{RetryConfig, UploadConfig};
use crate::pipeline::PipelineFailure;

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
            classify_object_store_error(ObjectStoreError::InvalidPath { source })
        })?;
        if payload.len() < self.config.multipart_threshold.0 {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(UploadError::Cancelled),
                result = self.store.put(&path, payload.into()) => {
                    result.map_err(classify_object_store_error)?;
                }
            }
            return Ok(());
        }

        let mut upload = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(UploadError::Cancelled),
            result = self.store.put_multipart(&path) => {
                result.map_err(classify_object_store_error)?
            }
        };
        upload_multipart(upload.as_mut(), &self.config, key, payload, cancellation).await
    }
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
            if let Some(Err(error)) = result {
                drop(parts);
                abort_multipart(upload, key).await;
                return Err(classify_object_store_error(error));
            }
        }
        let end = start.saturating_add(config.part_size.0).min(payload.len());
        parts.push(upload.put_part(payload.slice(start..end).into()));
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
        let Some(result) = result else {
            break;
        };
        if let Err(error) = result {
            drop(parts);
            abort_multipart(upload, key).await;
            return Err(classify_object_store_error(error));
        }
    }
    let completed = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            abort_multipart(upload, key).await;
            return Err(UploadError::Cancelled);
        }
        result = upload.complete() => result,
    };
    if let Err(error) = completed {
        abort_multipart(upload, key).await;
        return Err(classify_object_store_error(error));
    }
    Ok(())
}

async fn abort_multipart(upload: &mut dyn MultipartUpload, key: &str) {
    if let Err(error) = upload.abort().await {
        tracing::warn!(
            object_key = key,
            "failed to abort S3 multipart upload: {error}"
        );
    }
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

fn classify_object_store_error(error: ObjectStoreError) -> UploadError {
    let permanent = matches!(
        error,
        ObjectStoreError::PermissionDenied { .. }
            | ObjectStoreError::Unauthenticated { .. }
            | ObjectStoreError::InvalidPath { .. }
            | ObjectStoreError::NotSupported { .. }
            | ObjectStoreError::NotImplemented
            | ObjectStoreError::UnknownConfigurationKey { .. }
            | ObjectStoreError::AlreadyExists { .. }
            | ObjectStoreError::Precondition { .. }
            | ObjectStoreError::NotModified { .. }
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
    key: &str,
    payload: &Bytes,
    cancellation: &CancellationToken,
) -> Result<UploadStats, PipelineFailure> {
    let mut attempt = 1_usize;
    let mut retries = 0_u64;
    let mut busy = Duration::ZERO;
    loop {
        if cancellation.is_cancelled() {
            return Err(PipelineFailure::retryable(anyhow::anyhow!(
                "S3 upload cancelled"
            )));
        }
        let started = Instant::now();
        let result = uploader.upload(key, payload.clone(), cancellation).await;
        busy += started.elapsed();
        match result {
            Ok(()) => return Ok(UploadStats { retries, busy }),
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
                retries = retries.saturating_add(1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use object_store::{PutPayload, PutResult, UploadPart};

    #[derive(Debug)]
    struct FakeMultipart {
        aborts: Arc<AtomicUsize>,
        fail_complete: bool,
    }

    impl MultipartUpload for FakeMultipart {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(async { Ok(()) })
        }

        fn complete<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> BoxFuture<'async_trait, object_store::Result<PutResult>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                if self.fail_complete {
                    Err(ObjectStoreError::NotImplemented)
                } else {
                    Ok(PutResult {
                        e_tag: None,
                        version: None,
                    })
                }
            })
        }

        fn abort<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> BoxFuture<'async_trait, object_store::Result<()>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.aborts.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(()) })
        }
    }

    fn multipart_config() -> UploadConfig {
        UploadConfig {
            multipart_threshold: super::super::config::ByteSize(1),
            part_size: super::super::config::ByteSize(5),
            parallel_parts: 1,
            max_in_flight_objects: 1,
        }
    }

    #[test]
    fn classifies_credentials_and_request_errors_as_permanent() {
        let unauthenticated = ObjectStoreError::Unauthenticated {
            path: "object".into(),
            source: Box::new(std::io::Error::other("invalid credentials")),
        };
        assert!(matches!(
            classify_object_store_error(unauthenticated),
            UploadError::Permanent(_)
        ));

        let transport = ObjectStoreError::Generic {
            store: "S3",
            source: Box::new(std::io::Error::other("connection reset")),
        };
        assert!(matches!(
            classify_object_store_error(transport),
            UploadError::Retryable(_)
        ));
    }

    #[tokio::test]
    async fn aborts_multipart_when_complete_fails() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut upload = FakeMultipart {
            aborts: Arc::clone(&aborts),
            fail_complete: true,
        };
        let result = upload_multipart(
            &mut upload,
            &multipart_config(),
            "object",
            Bytes::from_static(b"12345"),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, Err(UploadError::Permanent(_))));
        assert_eq!(aborts.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn cancellation_aborts_started_multipart() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut upload = FakeMultipart {
            aborts: Arc::clone(&aborts),
            fail_complete: false,
        };
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = upload_multipart(
            &mut upload,
            &multipart_config(),
            "object",
            Bytes::from_static(b"1234567890"),
            &cancellation,
        )
        .await;

        assert!(matches!(result, Err(UploadError::Cancelled)));
        assert_eq!(aborts.load(Ordering::Acquire), 1);
    }
}
