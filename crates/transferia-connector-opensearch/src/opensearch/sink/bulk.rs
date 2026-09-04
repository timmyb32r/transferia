use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::PipelineMemory;
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_delivery_contracts::retry::{jittered_retry_delay, stable_retry_seed};

use super::super::{OpenSearchClient, OpenSearchHttpError};
use super::config::OpenSearchSinkConfig;
use super::document::BulkAction;

pub(super) trait BulkTransport: Send + Sync {
    fn send(&self, payload: Vec<u8>) -> BoxFuture<'_, Result<Vec<u16>, BulkFailure>>;
}

#[derive(Debug)]
pub(super) enum BulkFailure {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

pub(super) struct OpenSearchBulkTransport {
    client: OpenSearchClient,

    timeout_ms: u64,
}

impl OpenSearchBulkTransport {
    pub(super) const fn new(client: OpenSearchClient, timeout_ms: u64) -> Self {
        Self { client, timeout_ms }
    }
}

impl BulkTransport for OpenSearchBulkTransport {
    fn send(&self, payload: Vec<u8>) -> BoxFuture<'_, Result<Vec<u16>, BulkFailure>> {
        Box::pin(async move {
            let response = self
                .client
                .request(
                    Method::POST,
                    &["_bulk"],
                    &[("timeout", format!("{}ms", self.timeout_ms))],
                    "application/x-ndjson",
                    Some(payload),
                )
                .await
                .map_err(classify_http_error)?;
            parse_bulk_response(&response.body)
        })
    }
}

fn classify_http_error(error: OpenSearchHttpError) -> BulkFailure {
    if error.retryable() {
        BulkFailure::Retryable(anyhow::Error::new(error))
    } else {
        BulkFailure::Fatal(anyhow::Error::new(error))
    }
}

fn parse_bulk_response(body: &[u8]) -> Result<Vec<u16>, BulkFailure> {
    let response: Value = serde_json::from_slice(body).map_err(|_| {
        BulkFailure::Fatal(anyhow::anyhow!("OpenSearch bulk response is invalid JSON"))
    })?;
    let items = response
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            BulkFailure::Fatal(anyhow::anyhow!("OpenSearch bulk response omitted items"))
        })?;
    let mut statuses = Vec::with_capacity(items.len());
    for (position, item) in items.iter().enumerate() {
        let item = item.as_object().ok_or_else(|| {
            BulkFailure::Fatal(anyhow::anyhow!(
                "OpenSearch bulk item {position} is not an object"
            ))
        })?;
        if item.len() != 1 || !item.contains_key("index") {
            return Err(BulkFailure::Fatal(anyhow::anyhow!(
                "OpenSearch bulk item {position} does not describe exactly one index operation"
            )));
        }
        let status = item["index"]
            .get("status")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| {
                BulkFailure::Fatal(anyhow::anyhow!(
                    "OpenSearch bulk item {position} omitted a valid status"
                ))
            })?;
        statuses.push(status);
    }
    Ok(statuses)
}

pub(super) async fn write_bulk_with_retry(
    transport: Arc<dyn BulkTransport>,
    config: &OpenSearchSinkConfig,
    counters: &SinkCounters,
    memory: &PipelineMemory,
    cancellation: &CancellationToken,
    actions: Vec<BulkAction>,
    retry_seed: u64,
) -> Result<(usize, usize), DataPlaneFailure> {
    let mut pending = actions;
    let mut attempts = 0_u32;
    let mut backoff = Duration::from_millis(config.retry_initial_ms);
    let total_rows = pending.len();
    let total_bytes = pending.iter().map(|action| action.bytes).sum();
    loop {
        attempts = attempts.saturating_add(1);
        let payload = payload(&pending);
        let payload_memory = memory.reserve_transform(payload.len());
        let started = Instant::now();
        let response = tokio::select! {
            () = cancellation.cancelled() => {
                return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                    "OpenSearch bulk request cancelled before acknowledgement"
                )));
            }
            response = transport.send(payload) => response,
        };
        drop(payload_memory);
        counters.add_busy(started.elapsed());
        match response {
            Ok(statuses) => {
                if statuses.len() != pending.len() {
                    return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                        "OpenSearch bulk response contains {} items for {} requested operations",
                        statuses.len(),
                        pending.len()
                    )));
                }
                let mut retry = Vec::new();
                for (position, (action, status)) in pending.into_iter().zip(statuses).enumerate() {
                    if (200..300).contains(&status) {
                        continue;
                    }
                    if is_retryable_status(status) {
                        retry.push(action);
                    } else {
                        return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                            "OpenSearch rejected bulk item {position} with HTTP {status}; no delivery progress was acknowledged"
                        )));
                    }
                }
                if retry.is_empty() {
                    return Ok((total_rows, total_bytes));
                }
                pending = retry;
            }
            Err(BulkFailure::Fatal(error)) => return Err(DataPlaneFailure::fatal(error)),
            Err(BulkFailure::Retryable(error)) => {
                if attempts >= config.retry_max_attempts {
                    return Err(DataPlaneFailure::retryable(
                        error.context("OpenSearch bulk retry limit exhausted"),
                    ));
                }
            }
        }
        if attempts >= config.retry_max_attempts {
            return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                "OpenSearch bulk retry limit exhausted with {} unacknowledged items",
                pending.len()
            )));
        }
        counters.add_retries(1);
        let delay = jittered_retry_delay(backoff, attempts.saturating_sub(1), retry_seed);
        tokio::select! {
            () = cancellation.cancelled() => {
                return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                    "OpenSearch bulk retry cancelled before acknowledgement"
                )));
            }
            () = tokio::time::sleep(delay) => {}
        }
        backoff = backoff
            .saturating_mul(2)
            .min(Duration::from_millis(config.retry_max_ms));
    }
}

fn payload(actions: &[BulkAction]) -> Vec<u8> {
    let length = actions.iter().map(|action| action.ndjson.len()).sum();
    let mut payload = Vec::with_capacity(length);
    for action in actions {
        payload.extend_from_slice(&action.ndjson);
    }
    debug_assert_eq!(payload.last(), Some(&b'\n'));
    payload
}

const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

pub(super) fn retry_seed(partition_id: i64, index: &str, ordinal: usize) -> u64 {
    stable_retry_seed(&partition_id.to_le_bytes())
        ^ stable_retry_seed(index.as_bytes()).rotate_left(17)
        ^ stable_retry_seed(&ordinal.to_le_bytes()).rotate_left(33)
}
