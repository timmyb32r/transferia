use super::*;
use core::sync::atomic::{AtomicUsize, Ordering};
use object_store::{PutPayload, PutResult, UploadPart};

#[derive(Debug)]
struct FakeMultipart {
    aborts: Arc<AtomicUsize>,
    fail_complete: bool,
    hanging_part: Option<usize>,
    slow_parts: bool,
    part_calls: usize,
    hang_abort: bool,
}

impl MultipartUpload for FakeMultipart {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        let part = self.part_calls;
        self.part_calls = self.part_calls.saturating_add(1);
        if self.hanging_part == Some(part) {
            Box::pin(std::future::pending())
        } else if self.slow_parts {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(900)).await;
                Ok(())
            })
        } else {
            Box::pin(async { Ok(()) })
        }
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
        if self.hang_abort {
            Box::pin(std::future::pending())
        } else {
            Box::pin(async { Ok(()) })
        }
    }
}

fn multipart_config() -> UploadConfig {
    UploadConfig {
        multipart_threshold: super::super::config::ByteSize(1),
        part_size: super::super::config::ByteSize(5),
        parallel_parts: 1,
        max_in_flight_objects: 1,
        operation_timeout: super::super::config::DurationValue(Duration::from_secs(1)),
    }
}

#[test]
fn retry_delay_is_deterministic_keyed_and_never_exceeds_the_cap() {
    let config = RetryConfig {
        initial_backoff: super::super::config::DurationValue(Duration::from_millis(100)),
        max_backoff: super::super::config::DurationValue(Duration::from_millis(200)),
        max_attempts: 3,
    };
    let delay = retry_delay(&config, 8, "partition/7/object");

    assert_eq!(delay, retry_delay(&config, 8, "partition/7/object"));
    assert!(delay >= Duration::from_millis(160));
    assert!(delay <= Duration::from_millis(200));
    assert!(
        (0..8)
            .map(|partition| retry_delay(&config, 8, &format!("partition/{partition}/object")))
            .any(|candidate| candidate != delay),
        "different object keys should desynchronize at least one retry"
    );
}

#[test]
fn classifies_credentials_and_request_errors_as_permanent() {
    let unauthenticated = ObjectStoreError::Unauthenticated {
        path: "object".into(),
        source: Box::new(std::io::Error::other("invalid credentials")),
    };
    assert!(matches!(
        classify_object_store_error(unauthenticated, OperationPhase::Put),
        UploadError::Permanent(_)
    ));

    let transport = ObjectStoreError::Generic {
        store: "S3",
        source: Box::new(std::io::Error::other("connection reset")),
    };
    assert!(matches!(
        classify_object_store_error(transport, OperationPhase::Put),
        UploadError::Retryable(_)
    ));

    let missing_bucket = ObjectStoreError::NotFound {
        path: "object".into(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "bucket does not exist",
        )),
    };
    assert!(matches!(
        classify_object_store_error(missing_bucket, OperationPhase::Put),
        UploadError::Permanent(_)
    ));

    let missing_upload = ObjectStoreError::NotFound {
        path: "object".into(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "multipart upload does not exist",
        )),
    };
    assert!(matches!(
        classify_object_store_error(missing_upload, OperationPhase::Multipart),
        UploadError::Retryable(_)
    ));
}

#[test]
fn rejects_objects_that_exceed_the_multipart_part_limit() {
    assert!(validate_multipart_layout("object", 50_000, 5).is_ok());
    let error = validate_multipart_layout("object", 50_001, 5)
        .expect_err("10,001 parts must be rejected before starting an upload");
    assert!(matches!(error, UploadError::Permanent(_)));
}

#[tokio::test]
async fn aborts_multipart_when_complete_fails() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let mut upload = FakeMultipart {
        aborts: Arc::clone(&aborts),
        fail_complete: true,
        hanging_part: None,
        slow_parts: false,
        part_calls: 0,
        hang_abort: false,
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
        hanging_part: None,
        slow_parts: false,
        part_calls: 0,
        hang_abort: false,
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

#[tokio::test(start_paused = true)]
async fn part_timeout_aborts_multipart() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let mut upload = FakeMultipart {
        aborts: Arc::clone(&aborts),
        fail_complete: false,
        hanging_part: Some(0),
        slow_parts: false,
        part_calls: 0,
        hang_abort: false,
    };
    let result = upload_multipart(
        &mut upload,
        &multipart_config(),
        "object",
        Bytes::from_static(b"12345"),
        &CancellationToken::new(),
    )
    .await;

    assert!(matches!(result, Err(UploadError::Retryable(_))));
    assert_eq!(aborts.load(Ordering::Acquire), 1);
}

#[tokio::test(start_paused = true)]
async fn each_parallel_part_has_its_own_deadline() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let mut upload = FakeMultipart {
        aborts: Arc::clone(&aborts),
        fail_complete: false,
        hanging_part: Some(0),
        slow_parts: true,
        part_calls: 0,
        hang_abort: false,
    };
    let mut config = multipart_config();
    config.parallel_parts = 2;
    let started = tokio::time::Instant::now();
    let result = upload_multipart(
        &mut upload,
        &config,
        "object",
        Bytes::from_static(b"12345678901234567890"),
        &CancellationToken::new(),
    )
    .await;

    assert!(matches!(result, Err(UploadError::Retryable(_))));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(aborts.load(Ordering::Acquire), 1);
}

#[tokio::test(start_paused = true)]
async fn cancellation_does_not_wait_forever_for_abort() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let mut upload = FakeMultipart {
        aborts: Arc::clone(&aborts),
        fail_complete: false,
        hanging_part: None,
        slow_parts: false,
        part_calls: 0,
        hang_abort: true,
    };
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = upload_multipart(
        &mut upload,
        &multipart_config(),
        "object",
        Bytes::from_static(b"12345"),
        &cancellation,
    )
    .await;

    assert!(matches!(result, Err(UploadError::Cancelled)));
    assert_eq!(aborts.load(Ordering::Acquire), 1);
}
