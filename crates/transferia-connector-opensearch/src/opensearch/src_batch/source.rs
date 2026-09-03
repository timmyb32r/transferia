use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, StringBuilder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use transferia_connector_support::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    ARROW_JSON_EXTENSION_NAME, META_ARROW_EXTENSION_NAME, META_MAX_LENGTH, META_PRIMARY_KEY,
};
use transferia_core::data::system_columns::{
    SystemColumn, SystemColumnKind, SystemColumns,
};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};

use super::super::{OpenSearchClient, OpenSearchHttpError, OpenSearchResponse};

type SearchFuture = BoxFuture<'static, SlicePage>;

struct SliceState {
    emitted: u64,

    expected_total: Option<u64>,

    last_sort: Option<u64>,

    complete: bool,
}

pub(super) struct SlicePage {
    pub(super) slice: usize,

    pub(super) result: Result<SearchResponse, SourceRequestError>,
}

#[derive(Deserialize)]
struct OpenPitResponse {
    pit_id: String,

    #[serde(rename = "_shards")]
    shards: Shards,
}

#[derive(Deserialize)]
struct ClosePitResponse {
    pits: Vec<ClosedPit>,
}

#[derive(Deserialize)]
struct ClosedPit {
    successful: bool,

    pit_id: String,
}

#[derive(Clone, Copy)]
pub(super) struct RetryPolicy {
    initial: Duration,

    maximum: Duration,

    max_attempts: usize,
}

impl RetryPolicy {
    pub(super) const fn new(initial_ms: u64, maximum_ms: u64, max_attempts: usize) -> Self {
        Self {
            initial: Duration::from_millis(initial_ms),
            maximum: Duration::from_millis(maximum_ms),
            max_attempts,
        }
    }

    fn next_delay(self, current: Duration) -> Duration {
        current.saturating_mul(2).min(self.maximum)
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SourceRequestError {
    #[error(transparent)]
    Http(#[from] OpenSearchHttpError),

    #[error("OpenSearch returned an incomplete {operation} response")]
    Incomplete { operation: &'static str },

    #[error("OpenSearch returned an invalid {operation} response")]
    Protocol { operation: &'static str },
}

impl SourceRequestError {
    const fn retryable(&self) -> bool {
        match self {
            Self::Http(error) => error.retryable(),
            Self::Incomplete { .. } => true,
            Self::Protocol { .. } => false,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct SearchResponse {
    pub(super) timed_out: bool,

    #[serde(rename = "_shards")]
    pub(super) shards: Shards,

    pub(super) hits: SearchHits,
}

#[derive(Deserialize)]
pub(super) struct Shards {
    pub(super) total: u64,

    pub(super) successful: u64,

    #[serde(default)]
    pub(super) skipped: u64,

    pub(super) failed: u64,

    #[serde(default)]
    pub(super) failures: Vec<Value>,
}

#[derive(Deserialize)]
pub(super) struct SearchHits {
    pub(super) total: SearchTotal,

    pub(super) hits: Vec<SearchHit>,
}

#[derive(Deserialize)]
pub(super) struct SearchTotal {
    pub(super) value: u64,

    pub(super) relation: String,
}

#[derive(Deserialize)]
pub(super) struct SearchHit {
    #[serde(rename = "_index")]
    pub(super) index: String,

    #[serde(rename = "_id")]
    pub(super) id: String,

    #[serde(rename = "_routing", default)]
    pub(super) routing: Option<String>,

    #[serde(default)]
    pub(super) fields: HitFields,

    #[serde(rename = "_source")]
    pub(super) source: Box<RawValue>,

    pub(super) sort: Vec<Value>,
}

#[derive(Default, Deserialize)]
pub(super) struct HitFields {
    #[serde(rename = "_routing", default)]
    pub(super) routing: Vec<String>,
}

impl SearchHit {
    fn exact_routing(&self) -> anyhow::Result<Option<&str>> {
        anyhow::ensure!(
            self.fields.routing.len() <= 1,
            "OpenSearch hit '{}' returned multiple _routing values",
            self.id
        );
        let field = self.fields.routing.first().map(String::as_str);
        if let (Some(metadata), Some(field)) = (self.routing.as_deref(), field) {
            anyhow::ensure!(
                metadata == field,
                "OpenSearch hit '{}' returned conflicting _routing values",
                self.id
            );
        }
        Ok(self.routing.as_deref().or(field))
    }
}

pub(super) struct OpenSearchSource {
    client: OpenSearchClient,

    index: Arc<str>,

    partition: i64,

    page_rows: usize,

    read_concurrency: usize,

    pit_keep_alive: Arc<str>,

    retry: RetryPolicy,

    pit_id: Arc<str>,

    slices: Vec<SliceState>,

    pending_pages: VecDeque<(usize, Option<u64>)>,

    open_pit_ids: Vec<Arc<str>>,

    in_flight: FuturesUnordered<SearchFuture>,

    cancellation: CancellationToken,

    memory: PipelineMemory,

    counters: Arc<SourceCounters>,

    next_offset: i64,

    emitted_rows: u64,
}

impl OpenSearchSource {
    #[allow(
        clippy::too_many_arguments,
        reason = "source construction keeps its complete bounded-read contract explicit"
    )]
    pub(super) async fn open(
        client: OpenSearchClient,
        index: Arc<str>,
        partition: i64,
        page_rows: usize,
        read_concurrency: usize,
        shard_count: usize,
        pit_keep_alive: String,
        retry_initial_ms: u64,
        retry_max_ms: u64,
        retry_max_attempts: usize,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        // OpenSearch 2.x has no Elasticsearch `_shard_doc` sort. Keeping the logical slice count
        // exactly equal to the primary-shard count makes `_doc` a shard-local stable cursor;
        // `read_concurrency` must only bound in-flight page requests, never change this identity.
        let slice_count = shard_count;
        let retry = RetryPolicy::new(retry_initial_ms, retry_max_ms, retry_max_attempts);
        let pit_id = open_index_pit(
            &client,
            index.as_ref(),
            slice_count,
            &pit_keep_alive,
            retry,
            &cancellation,
            &counters,
        )
        .await?;
        let pit_keep_alive: Arc<str> = Arc::from(pit_keep_alive);
        let mut source = Self {
            client,
            index,
            partition,
            page_rows,
            read_concurrency,
            pit_keep_alive,
            retry,
            pit_id: Arc::clone(&pit_id),
            slices: (0..slice_count)
                .map(|_| SliceState {
                    emitted: 0,
                    expected_total: None,
                    last_sort: None,
                    complete: false,
                })
                .collect(),
            pending_pages: (0..slice_count).map(|slice| (slice, None)).collect(),
            open_pit_ids: vec![pit_id],
            in_flight: FuturesUnordered::new(),
            cancellation,
            memory,
            counters,
            next_offset: 0,
            emitted_rows: 0,
        };
        source.schedule_available()?;
        Ok(source)
    }

    fn schedule(&mut self, slice: usize, search_after: Option<u64>) -> anyhow::Result<()> {
        anyhow::ensure!(
            slice < self.slices.len(),
            "OpenSearch source has no slice {slice}"
        );
        self.in_flight.push(search_page(
            self.client.clone(),
            Arc::clone(&self.pit_keep_alive),
            Arc::clone(&self.pit_id),
            self.page_rows,
            slice,
            self.slices.len(),
            search_after,
            self.retry,
            Arc::clone(&self.counters),
        ));
        Ok(())
    }

    fn schedule_available(&mut self) -> anyhow::Result<()> {
        while self.in_flight.len() < self.read_concurrency {
            let Some((slice, search_after)) = self.pending_pages.pop_front() else {
                break;
            };
            self.schedule(slice, search_after)?;
        }
        Ok(())
    }

    fn process_page(&mut self, page: SlicePage) -> anyhow::Result<Option<RecordBatch>> {
        let response = page.result.map_err(anyhow::Error::from)?;
        anyhow::ensure!(
            response.hits.hits.len() <= self.page_rows,
            "OpenSearch slice {} returned {} hits above configured page_rows={}",
            page.slice,
            response.hits.hits.len(),
            self.page_rows
        );
        if response.hits.hits.is_empty() {
            let state = self
                .slices
                .get_mut(page.slice)
                .ok_or_else(|| anyhow::anyhow!("OpenSearch returned an unknown slice"))?;
            anyhow::ensure!(
                !state.complete,
                "OpenSearch returned a page for a completed slice"
            );
            match state.expected_total {
                Some(expected) => anyhow::ensure!(
                    expected == response.hits.total.value,
                    "OpenSearch PIT total changed for slice {}: expected {expected}, got {}",
                    page.slice,
                    response.hits.total.value
                ),
                None => state.expected_total = Some(response.hits.total.value),
            }

            anyhow::ensure!(
                state.emitted == response.hits.total.value,
                "OpenSearch slice {} ended after {} of {} exact hits",
                page.slice,
                state.emitted,
                response.hits.total.value
            );
            state.complete = true;
            self.schedule_available()?;
            return Ok(None);
        }

        let (last_sort, should_schedule) = {
            let state = self
                .slices
                .get_mut(page.slice)
                .ok_or_else(|| anyhow::anyhow!("OpenSearch returned an unknown slice"))?;
            anyhow::ensure!(
                !state.complete,
                "OpenSearch returned a page for a completed slice"
            );
            match state.expected_total {
                Some(expected) => anyhow::ensure!(
                    expected == response.hits.total.value,
                    "OpenSearch PIT total changed for slice {}: expected {expected}, got {}",
                    page.slice,
                    response.hits.total.value
                ),
                None => state.expected_total = Some(response.hits.total.value),
            }
            let last_sort = validate_hits(
                &self.index,
                page.slice,
                state.last_sort,
                &response.hits.hits,
            )?;
            let rows = u64::try_from(response.hits.hits.len())?;
            let new_emitted = state
                .emitted
                .checked_add(rows)
                .ok_or_else(|| anyhow::anyhow!("OpenSearch slice row count overflow"))?;
            anyhow::ensure!(
                new_emitted <= response.hits.total.value,
                "OpenSearch slice {} emitted {new_emitted} hits above exact total {}",
                page.slice,
                response.hits.total.value
            );
            state.emitted = new_emitted;
            state.last_sort = Some(last_sort);
            let should_schedule = new_emitted != response.hits.total.value;
            state.complete = !should_schedule;
            (last_sort, should_schedule)
        };
        if should_schedule {
            self.pending_pages.push_back((page.slice, Some(last_sort)));
        }
        self.schedule_available()?;
        let batch = hits_to_batch(
            &self.index,
            self.partition,
            self.next_offset,
            response.hits.hits,
        )?;
        self.next_offset = self
            .next_offset
            .checked_add(i64::try_from(batch.num_rows())?)
            .ok_or_else(|| anyhow::anyhow!("OpenSearch source offset overflow"))?;
        Ok(Some(batch))
    }

    async fn close_pits(&mut self) -> Result<(), SourceRequestError> {
        if self.open_pit_ids.is_empty() {
            return Ok(());
        }
        close_pits(
            &self.client,
            &mut self.open_pit_ids,
            self.retry,
            &self.counters,
        )
        .await
    }
}

impl Source for OpenSearchSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            loop {
                if self.in_flight.is_empty() {
                    if !self.slices.iter().all(|slice| slice.complete) {
                        return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                            "OpenSearch snapshot stopped with unfinished slices"
                        )));
                    }
                    return Ok(SourceBatch::Finished);
                }
                let page = tokio::select! {
                    biased;
                    () = self.cancellation.cancelled() => {
                        return Err(classify_failure(
                            self.emitted_rows,
                            anyhow::anyhow!("OpenSearch snapshot cancelled"),
                            true,
                        ));
                    }
                    page = self.in_flight.next() => match page {
                        Some(page) => page,
                        None => return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                            "non-empty OpenSearch request set yielded no page"
                        ))),
                    },
                };
                let batch = self.process_page(page).map_err(|error| {
                    let retryable = error
                        .downcast_ref::<SourceRequestError>()
                        .is_some_and(SourceRequestError::retryable);
                    classify_failure(self.emitted_rows, error, retryable)
                })?;
                let Some(batch) = batch else {
                    continue;
                };
                let rows = batch.num_rows() as u64;
                let bytes = batch.get_array_memory_size();
                let lease = tokio::select! {
                    biased;
                    () = self.cancellation.cancelled() => {
                        return Err(classify_failure(
                            self.emitted_rows,
                            anyhow::anyhow!("OpenSearch snapshot cancelled while reserving pipeline memory"),
                            true,
                        ));
                    }
                    lease = self.memory.reserve_progress_source(bytes) => lease,
                };
                self.counters.add_records(rows);
                self.emitted_rows = self.emitted_rows.saturating_add(rows);
                return Ok(SourceBatch::Typed {
                    tables: vec![TableData::new(
                        Arc::clone(&self.index),
                        false,
                        batch,
                        routing_system_columns(),
                    )],
                    source_rows: rows,
                    commit_marker: None,
                    memory: vec![lease],
                });
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, DataPlaneResult<()>> {
        Box::pin(async move {
            self.in_flight.clear();
            self.close_pits().await.map_err(|error| {
                if error.retryable() {
                    DataPlaneFailure::retryable(error.into())
                } else {
                    DataPlaneFailure::fatal(error.into())
                }
            })
        })
    }
}

pub(super) async fn open_index_pit(
    client: &OpenSearchClient,
    index: &str,
    shard_count: usize,
    keep_alive: &str,
    retry: RetryPolicy,
    cancellation: &CancellationToken,
    counters: &Arc<SourceCounters>,
) -> anyhow::Result<Arc<str>> {
    let query = pit_creation_query(keep_alive);
    let opened = open_pit(client, index, &query, retry, counters).await?;
    let mut known_pits = (!opened.pit_id.is_empty())
        .then(|| Arc::from(opened.pit_id.as_str()))
        .into_iter()
        .collect::<Vec<_>>();
    let valid = !opened.pit_id.is_empty()
        && opened.shards.total == shard_count as u64
        && opened.shards.successful == shard_count as u64
        && opened.shards.skipped == 0
        && opened.shards.failed == 0
        && opened.shards.failures.is_empty();
    let failure = if cancellation.is_cancelled() {
        Some(SourceRequestError::Incomplete {
            operation: "PIT creation cancelled",
        })
    } else if !valid {
        Some(SourceRequestError::Incomplete {
            operation: "PIT creation",
        })
    } else {
        None
    };
    if let Some(error) = failure {
        if let Err(cleanup) = close_pits(client, &mut known_pits, retry, counters).await {
            return Err(anyhow::Error::from(error).context(format!(
                "OpenSearch PIT creation failed and cleanup also failed: {cleanup}"
            )));
        }
        return Err(error.into());
    }
    Ok(Arc::from(opened.pit_id))
}

async fn open_pit(
    client: &OpenSearchClient,
    index: &str,
    query: &[(&str, String)],
    retry: RetryPolicy,
    counters: &SourceCounters,
) -> Result<OpenPitResponse, SourceRequestError> {
    let path = [index, "_search", "point_in_time"];
    let mut delay = retry.initial;
    for attempt in 1..=retry.max_attempts {
        let result = request_attempt(
            client,
            Method::POST,
            &path,
            query,
            "application/json",
            None,
            counters,
        )
        .await;
        match result {
            Ok(response) => {
                let opened = decode_json(&response.body, counters)?;
                return Ok(opened);
            }
            Err(error) if error.retryable() && attempt < retry.max_attempts => {
                tokio::time::sleep(delay).await;
                delay = retry.next_delay(delay);
            }
            Err(error) => return Err(SourceRequestError::Http(error)),
        }
    }
    Err(SourceRequestError::Incomplete {
        operation: "PIT creation",
    })
}

pub(super) async fn close_pits(
    client: &OpenSearchClient,
    pit_ids: &mut Vec<Arc<str>>,
    retry: RetryPolicy,
    counters: &SourceCounters,
) -> Result<(), SourceRequestError> {
    if pit_ids.is_empty() {
        return Ok(());
    }
    let mut delay = retry.initial;
    for attempt in 1..=retry.max_attempts {
        let body = serde_json::to_vec(&json!({
            "pit_id": pit_ids.iter().map(|pit| pit.as_ref()).collect::<Vec<&str>>()
        }))
        .map_err(|_| OpenSearchHttpError::InvalidJson)?;
        let result = request_attempt(
            client,
            Method::DELETE,
            &["_search", "point_in_time"],
            &[],
            "application/json",
            Some(&body),
            counters,
        )
        .await;
        let outcome = match result {
            Ok(response) => {
                let started = Instant::now();
                let result = decode_close_pits(&response.body, pit_ids);
                counters.add_network_decode_busy(started.elapsed());
                result
            }
            Err(OpenSearchHttpError::Status { status: StatusCode::NOT_FOUND }) => {
                pit_ids.clear();
                return Ok(());
            }
            Err(error) => Err(SourceRequestError::Http(error)),
        };
        match outcome {
            Ok(closed) => {
                pit_ids.retain(|pit| !closed.contains(pit.as_ref()));
                if pit_ids.is_empty() {
                    return Ok(());
                }
                if attempt == retry.max_attempts {
                    return Err(SourceRequestError::Incomplete {
                        operation: "PIT close",
                    });
                }
                tokio::time::sleep(delay).await;
                delay = retry.next_delay(delay);
            }
            Err(error) if error.retryable() && attempt < retry.max_attempts => {
                tokio::time::sleep(delay).await;
                delay = retry.next_delay(delay);
            }
            Err(error) => return Err(error),
        }
    }
    Err(SourceRequestError::Incomplete {
        operation: "PIT close",
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the request future owns the complete immutable slice cursor"
)]
pub(super) fn search_page(
    client: OpenSearchClient,
    keep_alive: Arc<str>,
    pit_id: Arc<str>,
    page_rows: usize,
    slice: usize,
    slice_count: usize,
    search_after: Option<u64>,
    retry: RetryPolicy,
    counters: Arc<SourceCounters>,
) -> SearchFuture {
    Box::pin(async move {
        let body = match build_search_body(
            &pit_id,
            &keep_alive,
            page_rows,
            slice,
            slice_count,
            search_after,
        ) {
            Ok(body) => body,
            Err(error) => {
                return SlicePage {
                    slice,
                    result: Err(error),
                };
            }
        };
        let mut delay = retry.initial;
        for attempt in 1..=retry.max_attempts {
            let result = request_attempt(
                &client,
                Method::POST,
                &["_search"],
                &[],
                "application/json",
                Some(&body),
                &counters,
            )
            .await;
            let outcome = match result {
                Ok(response) => match decode_json(&response.body, &counters) {
                    Ok(response) => match validate_complete_response(&response) {
                        Ok(()) => Ok(response),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                },
                Err(error) => Err(SourceRequestError::Http(error)),
            };
            match outcome {
                Ok(response) => {
                    return SlicePage {
                        slice,
                        result: Ok(response),
                    };
                }
                Err(error) if error.retryable() && attempt < retry.max_attempts => {
                    tokio::time::sleep(delay).await;
                    delay = retry.next_delay(delay);
                }
                Err(error) => {
                    return SlicePage {
                        slice,
                        result: Err(error),
                    };
                }
            }
        }
        SlicePage {
            slice,
            result: Err(SourceRequestError::Incomplete {
                operation: "search page",
            }),
        }
    })
}

async fn request_attempt(
    client: &OpenSearchClient,
    method: Method,
    path: &[&str],
    query: &[(&str, String)],
    content_type: &'static str,
    body: Option<&[u8]>,
    counters: &SourceCounters,
) -> Result<OpenSearchResponse, OpenSearchHttpError> {
    let started = Instant::now();
    let response = client
        .request(
            method,
            path,
            query,
            content_type,
            body.map(<[u8]>::to_vec),
        )
        .await;
    counters.add_response_wait(started.elapsed());
    if let Ok(response) = &response {
        counters.add_network_decoded_bytes(response.body.len() as u64);
    }
    response
}

fn decode_json<T: DeserializeOwned>(
    body: &[u8],
    counters: &SourceCounters,
) -> Result<T, SourceRequestError> {
    let started = Instant::now();
    let parsed = serde_json::from_slice(body)
        .map_err(|_| SourceRequestError::Http(OpenSearchHttpError::InvalidJson));
    counters.add_network_decode_busy(started.elapsed());
    parsed
}

pub(super) fn validate_complete_response(
    response: &SearchResponse,
) -> Result<(), SourceRequestError> {
    if response.timed_out
        || response.hits.total.relation != "eq"
        || validate_shards(&response.shards).is_err()
    {
        return Err(SourceRequestError::Incomplete {
            operation: "search page",
        });
    }
    Ok(())
}

fn validate_shards(shards: &Shards) -> anyhow::Result<()> {
    anyhow::ensure!(
        shards.failed == 0 && shards.failures.is_empty(),
        "OpenSearch response has failed shards"
    );
    anyhow::ensure!(
        shards.successful.checked_add(shards.skipped) == Some(shards.total),
        "OpenSearch response is partial: total={}, successful={}, skipped={}",
        shards.total,
        shards.successful,
        shards.skipped
    );
    Ok(())
}

pub(super) fn decode_close_pits(
    body: &[u8],
    expected_pits: &[Arc<str>],
) -> Result<HashSet<String>, SourceRequestError> {
    let response: ClosePitResponse = serde_json::from_slice(body)
        .map_err(|_| SourceRequestError::Http(OpenSearchHttpError::InvalidJson))?;
    if response.pits.len() != expected_pits.len() {
        return Err(SourceRequestError::Protocol {
            operation: "PIT close",
        });
    }
    let expected = expected_pits
        .iter()
        .map(|pit| pit.as_ref())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::with_capacity(response.pits.len());
    let mut successful = HashSet::new();
    for closed in response.pits {
        if !expected.contains(closed.pit_id.as_str()) || !seen.insert(closed.pit_id.clone()) {
            return Err(SourceRequestError::Protocol {
                operation: "PIT close",
            });
        }
        if closed.successful {
            successful.insert(closed.pit_id);
        }
    }
    Ok(successful)
}

pub(super) fn pit_creation_query(
    keep_alive: &str,
) -> Vec<(&'static str, String)> {
    vec![
        ("keep_alive", keep_alive.to_owned()),
        ("allow_partial_pit_creation", "false".to_owned()),
    ]
}

pub(super) fn build_search_body(
    pit_id: &str,
    keep_alive: &str,
    page_rows: usize,
    slice: usize,
    slice_count: usize,
    search_after: Option<u64>,
) -> Result<Vec<u8>, SourceRequestError> {
    let mut body = json!({
        "size": page_rows,
        "query": { "match_all": {} },
        "pit": { "id": pit_id, "keep_alive": keep_alive },
        "sort": [{ "_doc": "asc" }],
        "track_total_hits": true,
        "_source": true,
        "stored_fields": ["_routing"]
    });
    if let Some(object) = body.as_object_mut() {
        if slice_count > 1 {
            object.insert("slice".to_owned(), json!({ "id": slice, "max": slice_count }));
        }
        if let Some(sort) = search_after {
            object.insert("search_after".to_owned(), json!([sort]));
        }
    }
    serde_json::to_vec(&body)
        .map_err(|_| SourceRequestError::Http(OpenSearchHttpError::InvalidJson))
}

pub(super) fn validate_hits(
    index: &str,
    slice: usize,
    previous_sort: Option<u64>,
    hits: &[SearchHit],
) -> anyhow::Result<u64> {
    let mut last = previous_sort;
    for hit in hits {
        anyhow::ensure!(
            hit.index == index,
            "OpenSearch PIT for exact index '{index}' returned hit from '{}'",
            hit.index
        );
        anyhow::ensure!(!hit.id.is_empty(), "OpenSearch returned an empty _id");
        anyhow::ensure!(
            hit.id.len() <= 512,
            "OpenSearch returned an _id exceeding its 512-byte contract"
        );
        let source = hit.source.get().trim();
        anyhow::ensure!(
            source.starts_with('{') && source.ends_with('}'),
            "OpenSearch hit '{}' has no object _source",
            hit.id
        );
        hit.exact_routing()?;
        anyhow::ensure!(
            hit.sort.len() == 1,
            "OpenSearch hit '{}' has an invalid _doc cursor",
            hit.id
        );
        let sort = hit.sort[0].as_u64().ok_or_else(|| {
            anyhow::anyhow!("OpenSearch hit '{}' has a non-u64 _doc cursor", hit.id)
        })?;
        if let Some(previous) = last {
            anyhow::ensure!(
                sort > previous,
                "OpenSearch slice {slice} returned a non-increasing _doc cursor"
            );
        }
        last = Some(sort);
    }
    last.ok_or_else(|| anyhow::anyhow!("OpenSearch non-empty page has no cursor"))
}

pub(super) fn hits_to_batch(
    index: &str,
    partition: i64,
    start_offset: i64,
    hits: Vec<SearchHit>,
) -> anyhow::Result<RecordBatch> {
    let rows = hits.len();
    let mut ids = StringBuilder::new();
    let mut routings = StringBuilder::new();
    let mut sources = StringBuilder::new();
    let mut routing_keys = StringBuilder::new();
    for hit in hits {
        let routing = hit.exact_routing()?;
        ids.append_value(&hit.id);
        match routing {
            Some(routing) => routings.append_value(routing),
            None => routings.append_null(),
        }
        sources.append_value(hit.source.get());
        routing_keys.append_value(routing.unwrap_or(&hit.id));
    }
    let mut id_metadata = std::collections::HashMap::new();
    id_metadata.insert(META_PRIMARY_KEY.to_owned(), "true".to_owned());
    id_metadata.insert(META_MAX_LENGTH.to_owned(), "512".to_owned());
    let mut source_metadata = std::collections::HashMap::new();
    source_metadata.insert(
        META_ARROW_EXTENSION_NAME.to_owned(),
        ARROW_JSON_EXTENSION_NAME.to_owned(),
    );
    let end_offset = start_offset
        .checked_add(i64::try_from(rows)?)
        .ok_or_else(|| anyhow::anyhow!("OpenSearch source offset overflow"))?;
    let fields = vec![
        Field::new("_id", DataType::Utf8, false).with_metadata(id_metadata),
        Field::new("_routing", DataType::Utf8, true),
        Field::new("_source", DataType::Utf8, false).with_metadata(source_metadata),
        Field::new("_routing_key", DataType::Utf8, false)
            .with_metadata([(META_PRIMARY_KEY.to_owned(), "true".to_owned())].into()),
        Field::new(
            SystemColumnKind::Topic.default_name(),
            DataType::Utf8,
            false,
        ),
        Field::new(
            SystemColumnKind::Partition.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::MessageIndex.default_name(),
            DataType::UInt64,
            false,
        ),
    ];
    let arrays = vec![
        Arc::new(ids.finish()) as ArrayRef,
        Arc::new(routings.finish()) as ArrayRef,
        Arc::new(sources.finish()) as ArrayRef,
        Arc::new(routing_keys.finish()) as ArrayRef,
        Arc::new(arrow::array::StringArray::from(vec![index; rows])) as ArrayRef,
        Arc::new(Int64Array::from(vec![partition; rows])) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(start_offset..end_offset)) as ArrayRef,
        Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef,
    ];
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn routing_system_columns() -> SystemColumns {
    SystemColumns::new(vec![
        SystemColumn {
            kind: SystemColumnKind::Topic,
            name: Arc::from(SystemColumnKind::Topic.default_name()),
            index: 4,
        },
        SystemColumn {
            kind: SystemColumnKind::Partition,
            name: Arc::from(SystemColumnKind::Partition.default_name()),
            index: 5,
        },
        SystemColumn {
            kind: SystemColumnKind::Offset,
            name: Arc::from(SystemColumnKind::Offset.default_name()),
            index: 6,
        },
        SystemColumn {
            kind: SystemColumnKind::MessageIndex,
            name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
            index: 7,
        },
    ])
}

fn classify_failure(
    emitted_rows: u64,
    error: anyhow::Error,
    retryable_before_output: bool,
) -> DataPlaneFailure {
    if emitted_rows > 0 {
        return DataPlaneFailure::fatal(error.context(format!(
            "OpenSearch PIT snapshot failed after emitting {emitted_rows} rows; reopening the snapshot would duplicate already emitted data"
        )));
    }
    if retryable_before_output {
        DataPlaneFailure::retryable(error)
    } else {
        DataPlaneFailure::fatal(error)
    }
}
