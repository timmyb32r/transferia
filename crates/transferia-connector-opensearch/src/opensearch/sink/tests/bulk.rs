use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use transferia_core::memory::PipelineMemory;
use transferia_delivery_contracts::metrics::SinkCounters;

use super::super::bulk::{write_bulk_with_retry, BulkFailure, BulkTransport};
use super::super::document::BulkAction;
use super::super::{initial_config, OpenSearchSinkConfig};

struct FakeTransport {
    responses: Mutex<VecDeque<Result<Vec<u16>, BulkFailure>>>,

    payloads: Mutex<Vec<Vec<u8>>>,
}

struct BlockingTransport {
    started: Notify,

    release: Notify,
}

impl BulkTransport for BlockingTransport {
    fn send<'a>(&'a self, _payload: Vec<u8>) -> BoxFuture<'a, Result<Vec<u16>, BulkFailure>> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            Ok(vec![201])
        })
    }
}

impl BulkTransport for FakeTransport {
    fn send<'a>(&'a self, payload: Vec<u8>) -> BoxFuture<'a, Result<Vec<u16>, BulkFailure>> {
        Box::pin(async move {
            self.payloads.lock().unwrap().push(payload);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted bulk response")
        })
    }
}

fn config() -> OpenSearchSinkConfig {
    let mut value = initial_config();
    value["hosts"] = serde_json::json!(["example.test"]);
    value["trusted_plaintext"] = true.into();
    value["retry_initial_ms"] = 1.into();
    value["retry_max_ms"] = 1.into();
    serde_json::from_value(value).unwrap()
}

fn action(id: &str) -> BulkAction {
    let ndjson: Arc<[u8]> = Arc::from(
        format!("{{\"index\":{{\"_id\":\"{id}\"}}}}\n{{\"v\":1}}\n").into_bytes(),
    );
    BulkAction {
        id: Arc::from(id),
        bytes: ndjson.len(),
        ndjson,
    }
}

#[tokio::test]
async fn retries_only_individually_transient_items_and_keeps_ndjson_terminated() {
    let transport = Arc::new(FakeTransport {
        responses: Mutex::new(VecDeque::from([
            Ok(vec![201, 429, 200]),
            Ok(vec![201]),
        ])),
        payloads: Mutex::new(Vec::new()),
    });
    let result = write_bulk_with_retry(
        transport.clone(),
        &config(),
        &SinkCounters::new(),
        &PipelineMemory::new(1024 * 1024),
        &CancellationToken::new(),
        vec![action("a"), action("b"), action("c")],
        7,
    )
    .await
    .unwrap();
    assert_eq!(result.0, 3);
    let payloads = transport.payloads.lock().unwrap();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0].last(), Some(&b'\n'));
    assert_eq!(payloads[1].last(), Some(&b'\n'));
    let retried = std::str::from_utf8(&payloads[1]).unwrap();
    assert!(retried.contains("\"b\""));
    assert!(!retried.contains("\"a\""));
    assert!(!retried.contains("\"c\""));
}

#[tokio::test]
async fn retries_item_level_internal_server_error() {
    let transport = Arc::new(FakeTransport {
        responses: Mutex::new(VecDeque::from([Ok(vec![500]), Ok(vec![201])])),
        payloads: Mutex::new(Vec::new()),
    });
    let result = write_bulk_with_retry(
        transport.clone(),
        &config(),
        &SinkCounters::new(),
        &PipelineMemory::new(1024 * 1024),
        &CancellationToken::new(),
        vec![action("a")],
        7,
    )
    .await
    .unwrap();
    assert_eq!(result.0, 1);
    assert_eq!(transport.payloads.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn response_cardinality_mismatch_is_fatal_and_never_acknowledged() {
    let transport = Arc::new(FakeTransport {
        responses: Mutex::new(VecDeque::from([Ok(vec![201])])),
        payloads: Mutex::new(Vec::new()),
    });
    let failure = write_bulk_with_retry(
        transport,
        &config(),
        &SinkCounters::new(),
        &PipelineMemory::new(1024 * 1024),
        &CancellationToken::new(),
        vec![action("a"), action("b")],
        7,
    )
    .await
    .unwrap_err();
    assert!(!failure.is_retryable());
}

#[tokio::test]
async fn permanent_item_failure_is_not_retried() {
    let transport = Arc::new(FakeTransport {
        responses: Mutex::new(VecDeque::from([Ok(vec![400])])),
        payloads: Mutex::new(Vec::new()),
    });
    let failure = write_bulk_with_retry(
        transport.clone(),
        &config(),
        &SinkCounters::new(),
        &PipelineMemory::new(1024 * 1024),
        &CancellationToken::new(),
        vec![action("a")],
        7,
    )
    .await
    .unwrap_err();
    assert!(!failure.is_retryable());
    assert_eq!(transport.payloads.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn transient_transport_failure_exhaustion_remains_retryable() {
    let mut config = config();
    config.retry_max_attempts = 2;
    let transport = Arc::new(FakeTransport {
        responses: Mutex::new(VecDeque::from([
            Err(BulkFailure::Retryable(anyhow::anyhow!("temporary"))),
            Err(BulkFailure::Retryable(anyhow::anyhow!("temporary"))),
        ])),
        payloads: Mutex::new(Vec::new()),
    });
    let failure = write_bulk_with_retry(
        transport.clone(),
        &config,
        &SinkCounters::new(),
        &PipelineMemory::new(1024 * 1024),
        &CancellationToken::new(),
        vec![action("a")],
        7,
    )
    .await
    .unwrap_err();
    assert!(failure.is_retryable());
    assert_eq!(transport.payloads.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn contiguous_request_payload_is_accounted_only_while_in_flight() {
    let transport = Arc::new(BlockingTransport {
        started: Notify::new(),
        release: Notify::new(),
    });
    let action = action("a");
    let encoded_bytes = action.bytes;
    let memory = PipelineMemory::new(1);
    let cached_memory = memory.reserve_transform(encoded_bytes);
    let task = {
        let transport = transport.clone();
        let memory = memory.clone();
        tokio::spawn(async move {
            write_bulk_with_retry(
                transport,
                &config(),
                &SinkCounters::new(),
                &memory,
                &CancellationToken::new(),
                vec![action],
                7,
            )
            .await
        })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        transport.started.notified(),
    )
    .await
    .expect("bulk request did not start");
    assert_eq!(memory.transform_used(), encoded_bytes * 2);

    transport.release.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(memory.transform_used(), encoded_bytes);
    drop(cached_memory);
    assert_eq!(memory.transform_used(), 0);
}
