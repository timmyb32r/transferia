//! `PQv1` (`PersQueue` V1) gRPC client for Logbroker.
//!
//! Flow: `ListEndpoints` (discover proxy) → `MigrationStreamingRead` bidi stream on the
//! proxy → `InitResponse` → Assigned → `StartRead` → `DataBatch`. Transport is HTTP/2 with
//! prior knowledge (Go-compatible), bridged into tonic via a small `tower::Service`.

use alloc::sync::Arc;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::Stream;

use crate::metrics::SourceCounters;
use crate::pipeline::memory::{MemoryReservation, PipelineMemory};
use crate::providers::yds::config::YdsSourceConfig;
use hyper::client::conn::http2;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::Uri;
use tonic::Request;

/// YDB cluster database used for discovery/routing metadata (`x-ydb-database`).
/// Always `/Root` in our deployment — hardcoded rather than configured.
const YDB_DATABASE: &str = "/Root";
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::message::SourcePartition;
use crate::types::message::{Message, MessageBatch, MessageMeta};
use crate::Ydb::pers_queue::v1::{
    migration_streaming_read_client_message::{self, InitRequest, TopicReadSettings},
    migration_streaming_read_server_message, CommitCookie, MigrationStreamingReadClientMessage,
    MigrationStreamingReadServerMessage, ReadParams,
};

/// `Ydb.StatusIds.SUCCESS`. Status codes live in the reserved range [400000, 400999];
/// SUCCESS is 400000 (NOT 0 — 0 is `STATUS_CODE_UNSPECIFIED`, sent on streaming data msgs).
const YDB_STATUS_SUCCESS: i32 = 400_000;
/// `Ydb.StatusIds.STATUS_CODE_UNSPECIFIED`. Streaming data messages carry it on every
/// batch; only real error codes abort the stream.
const YDB_STATUS_UNSPECIFIED: i32 = 0;

/// tonic's default decode cap is 4 MiB; Logbroker `DataBatch` messages can exceed it.
const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;
/// Capacity of the decompressed-batch channel (bg task → merge task). Bounded
/// so that if decompress ever falls behind download, memory is capped; with
/// parallel decompress keeping up, it stays near-empty.
const DECODED_CHANNEL_CAP: usize = 128;
const PARTITION_CHANNEL_CAP: usize = 1024;
const DECOMPRESS_CONCURRENCY: usize = 4;

// ---------------------------------------------------------------------------
// HTTP/2 prior-knowledge transport (Go-compatible)
// ---------------------------------------------------------------------------

/// Bridges Hyper 1.x `SendRequest` (which doesn't impl `tower::Service`) to tonic.
struct H2Service {
    inner: http2::SendRequest<tonic::body::Body>,
}

impl tower::Service<http::Request<tonic::body::Body>> for H2Service {
    type Response = http::Response<hyper::body::Incoming>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn core::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        Box::pin(self.inner.send_request(req))
    }
}

/// Establish an HTTP/2 prior-knowledge connection (sends the HTTP/2 preface directly,
/// like grpc-go — no HTTP/1.1 upgrade).
async fn connect_http2_prior_knowledge(uri: &Uri) -> anyhow::Result<H2Service> {
    let host = uri.host().unwrap_or("localhost");
    let port = uri.port_u16().unwrap_or(2135);
    let addr = format!("{host}:{port}");

    let stream = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| anyhow!("TCP connect to {addr}: {e}"))?;
    stream.set_nodelay(true)?;

    let io = hyper_util::rt::TokioIo::new(stream);
    let (send_request, conn) = http2::handshake(hyper_util::rt::TokioExecutor::new(), io)
        .await
        .map_err(|e| anyhow!("HTTP/2 handshake failed: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("HTTP/2 connection error: {}", e);
        }
    });

    tracing::debug!("HTTP/2 prior-knowledge connection to {}", addr);
    Ok(H2Service {
        inner: send_request,
    })
}

/// Attach the YDB auth/routing headers that Logbroker expects on every call.
fn set_ydb_headers(md: &mut MetadataMap, token: &str) {
    if let Ok(v) = AsciiMetadataValue::try_from(token) {
        md.insert("x-ydb-auth-ticket", v);
    }
    md.insert(
        "x-ydb-database",
        AsciiMetadataValue::from_static(YDB_DATABASE),
    );
    md.insert(
        "x-ydb-sdk-build-info",
        AsciiMetadataValue::from_static("go-sdk-2021.04.1"),
    );
    md.insert(
        "user-agent",
        AsciiMetadataValue::from_static("grpc-go/1.80.0"),
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[must_use]
pub const fn group_to_partition(group: i64) -> i64 {
    group
}
#[must_use]
pub const fn partition_to_group(partition: i64) -> i64 {
    partition
}

/// Parse a connection string into `(scheme, host, database)`. `database` is derived from
/// the path/query for compatibility but is not authoritative — the cluster DB is `YDB_DATABASE`.
pub fn parse_endpoint(conn_str: &str) -> anyhow::Result<(String, String, String)> {
    let uri: Uri = conn_str
        .parse()
        .map_err(|e| anyhow!("Invalid connection string '{conn_str}': {e}"))?;
    let scheme = uri.scheme_str().unwrap_or("grpc").to_string();
    let host = uri
        .authority()
        .map_or("localhost:2135", |a| a.as_str())
        .to_string();
    let database = {
        let path = uri.path().trim_start_matches('/').to_string();
        if path.is_empty() {
            YDB_DATABASE.to_string()
        } else {
            format!("/{path}")
        }
    };
    Ok((scheme, host, database))
}

fn http_uri(scheme: &str, host: &str) -> anyhow::Result<Uri> {
    let s = if scheme == "grpcs" { "https" } else { "http" };
    format!("{s}://{host}")
        .parse()
        .map_err(|e| anyhow!("bad uri {s}://{host}: {e}"))
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A decompressed message handed to the pipeline. `CommitCookie` is `Copy` (a couple of
/// integers), so carrying it by value per message is cheap.
pub struct DecodedMessage {
    pub data: Bytes,
    pub cookie: Option<CommitCookie>,
    /// Offset within the `PQv1` partition (for exactly-once dedup).
    pub offset: u64,
    pub write_timestamp_ms: u64,
    pub memory: MemoryReservation,
}

pub struct PqV1CommitMarker {
    pub partition_id: i64,
    pub cookie: CommitCookie,
}

/// Raw (still-compressed) message handed off to the decompress pool.
struct RawMsg {
    data: Vec<u8>,
    codec: i32,
    uncompressed_size: u64,
    offset: u64,
    write_timestamp_ms: u64,
}

/// One partition's worth of raw messages within a `DataBatch`.
struct RawPart {
    pid: i64,
    cookie: Option<CommitCookie>,
    msgs: Vec<RawMsg>,
}

/// One partition's decompressed messages.
struct DecodedPart {
    pid: i64,
    msgs: Vec<DecodedMessage>,
}

/// A decompressed `DataBatch`, re-ordered by `seq` before dispatch.
struct DecodedBatch {
    seq: u64,
    parts: Vec<DecodedPart>,
}

struct RequestStream {
    rx: mpsc::UnboundedReceiver<MigrationStreamingReadClientMessage>,
}

impl Stream for RequestStream {
    type Item = MigrationStreamingReadClientMessage;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rx.len(), None)
    }
}

/// A `Read` request — asks the server for the next batch.
#[inline]
const fn read_request() -> MigrationStreamingReadClientMessage {
    MigrationStreamingReadClientMessage {
        request: Some(migration_streaming_read_client_message::Request::Read(
            migration_streaming_read_client_message::Read {},
        )),
        token: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// PqV1Client
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PqV1Client {
    inner: Arc<PqV1ClientInner>,
}

struct PqV1ClientInner {
    request_tx: mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
    partition_queues: Mutex<HashMap<i64, mpsc::Sender<DecodedMessage>>>,
}

/// Discover a proxy endpoint via `ListEndpoints` over HTTP/2 prior knowledge.
/// The gRPC response type is `GetOperationResponse` (matching Go's `conn.Invoke`).
async fn discover_proxy(main_uri: &Uri, token: &str) -> anyhow::Result<String> {
    use crate::Ydb::discovery::{ListEndpointsRequest, ListEndpointsResult};
    use crate::Ydb::operations::GetOperationResponse;
    use prost::Message as _;

    let h2 = connect_http2_prior_knowledge(main_uri).await?;
    let mut grpc = tonic::client::Grpc::<H2Service>::with_origin(h2, main_uri.clone());

    let mut req = Request::new(ListEndpointsRequest {
        database: YDB_DATABASE.to_string(),
        service: vec![],
    });
    set_ydb_headers(req.metadata_mut(), token);

    grpc.ready()
        .await
        .map_err(|e| anyhow!("ListEndpoints ready: {e}"))?;
    let path =
        http::uri::PathAndQuery::from_static("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints");
    let resp: GetOperationResponse = grpc
        .unary(
            req,
            path,
            tonic_prost::ProstCodec::<ListEndpointsRequest, GetOperationResponse>::default(),
        )
        .await
        .map_err(|e| anyhow!("ListEndpoints failed: {e}"))?
        .into_inner();

    let op = resp.operation.ok_or_else(|| anyhow!("no operation"))?;
    if !op.ready {
        anyhow::bail!("ListEndpoints not ready");
    }
    // SUCCESS is 400000, not 0 (0 == UNSPECIFIED also acceptable for forward-compat).
    if op.status != YDB_STATUS_UNSPECIFIED && op.status != YDB_STATUS_SUCCESS {
        anyhow::bail!("ListEndpoints status={}", op.status);
    }
    let result = op.result.ok_or_else(|| anyhow!("no result"))?;
    let eps = ListEndpointsResult::decode(result.value.as_slice())?;
    eps.endpoints
        .first()
        .map(|e| format!("{}:{}", e.address, e.port))
        .ok_or_else(|| anyhow!("no endpoints"))
}

impl PqV1Client {
    pub async fn connect(
        endpoint: &str,
        topic_path: &str,
        consumer: &str,
        token: &str,
        partition_group_ids: &[i64],
        source_counters: Arc<SourceCounters>,
        cancel_token: CancellationToken,
        drop_before_decompress: bool,
        memory: PipelineMemory,
    ) -> anyhow::Result<(Self, HashMap<i64, mpsc::Receiver<DecodedMessage>>)> {
        let (scheme, main_host, _) = parse_endpoint(endpoint)?;
        let main_uri = http_uri(&scheme, &main_host)?;

        // Step 1: discover the proxy that actually serves the topic.
        let proxy = match discover_proxy(&main_uri, token).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Proxy discovery failed: {}. Using main endpoint.", e);
                main_host
            }
        };
        let target_uri = http_uri(&scheme, &proxy)?;
        tracing::info!(
            "PQv1 connecting: proxy={} topic={} consumer={}",
            proxy,
            topic_path,
            consumer
        );

        // Step 2: open the bidi stream on the proxy.
        let h2_service = connect_http2_prior_knowledge(&target_uri).await?;

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(
                migration_streaming_read_client_message::Request::InitRequest(InitRequest {
                    topics_read_settings: vec![TopicReadSettings {
                        topic: topic_path.to_string(),
                        partition_group_ids: vec![],
                        start_from_written_at_ms: 0,
                    }],
                    consumer: consumer.to_string(),
                    read_only_original: false,
                    max_lag_duration_ms: 0,
                    start_from_written_at_ms: 0,
                    max_supported_block_format_version: 0,
                    max_meta_cache_size: 0,
                    read_params: Some(ReadParams {
                        max_read_messages_count: 0,
                        max_read_size: 1_048_576,
                    }),
                    session_id: String::new(),
                    connection_attempt: 0,
                    state: None,
                    idle_timeout_ms: 0,
                    ranges_mode: false,
                }),
            ),
            token: token.as_bytes().to_vec(),
        })?;

        let mut req = Request::new(RequestStream { rx: request_rx });
        set_ydb_headers(req.metadata_mut(), token);

        let mut grpc = tonic::client::Grpc::with_origin(h2_service, target_uri)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        grpc.ready()
            .await
            .map_err(|e| anyhow!("grpc not ready: {e}"))?;
        let path = http::uri::PathAndQuery::from_static(
            "/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead",
        );
        let codec = tonic_prost::ProstCodec::<
            MigrationStreamingReadClientMessage,
            MigrationStreamingReadServerMessage,
        >::default();
        let response_stream = grpc
            .streaming(req, path, codec)
            .await
            .map_err(|e| anyhow!("MigrationStreamingRead failed: {e}"))?
            .into_inner();

        // Per-partition queues for the partitions we own.
        let assigned: HashSet<i64> = partition_group_ids
            .iter()
            .map(|&g| group_to_partition(g))
            .collect();
        let mut pqs = HashMap::with_capacity(assigned.len());
        let mut prs = HashMap::with_capacity(assigned.len());
        #[expect(
            clippy::iter_over_hash_type,
            reason = "partition ids come from a set built from the static config; order is irrelevant"
        )]
        for &pid in &assigned {
            let (tx, rx) = mpsc::channel(PARTITION_CHANNEL_CAP);
            pqs.insert(pid, tx);
            prs.insert(pid, rx);
        }

        let inner = Arc::new(PqV1ClientInner {
            request_tx: request_tx.clone(),
            partition_queues: Mutex::new(pqs),
        });

        // Pipelined decompress: the read loop hands each DataBatch to a blocking
        // task for decompression and immediately sends the next Read, so
        // `decompress()` never blocks the download. A merge task re-orders the
        // decompressed batches by `seq` to restore per-partition offset order
        // (the exactly-once waterline assumes offsets arrive in order per
        // partition).
        let (decoded_tx, decoded_rx) = mpsc::channel::<DecodedBatch>(DECODED_CHANNEL_CAP);
        let decompress_slots = Arc::new(tokio::sync::Semaphore::new(DECOMPRESS_CONCURRENCY));
        let merge_inner = Arc::clone(&inner);
        let merge_token = cancel_token.clone();
        tokio::spawn(async move {
            let mut next_seq: u64 = 0;
            let mut buffer: HashMap<u64, DecodedBatch> = HashMap::new();
            let mut rx = decoded_rx;
            loop {
                if merge_token.is_cancelled() {
                    break;
                }
                let Some(batch) = rx.recv().await else {
                    break;
                };
                buffer.insert(batch.seq, batch);
                while let Some(b) = buffer.remove(&next_seq) {
                    for DecodedPart { pid, msgs } in b.parts {
                        let tx = merge_inner.partition_queues.lock().await.get(&pid).cloned();
                        let Some(tx) = tx else {
                            continue;
                        };
                        let mut closed = false;
                        for msg in msgs {
                            if tx.send(msg).await.is_err() {
                                closed = true;
                                break;
                            }
                        }
                        if closed {
                            // Receiver dropped (shutdown) — remove the dead
                            // sender so future batches skip this partition.
                            merge_inner.partition_queues.lock().await.remove(&pid);
                            tracing::info!("PQv1 partition {} queue closed; stopped dispatch", pid);
                        }
                    }
                    next_seq += 1;
                }
            }
            tracing::info!("PQv1 merge task exited");
        });

        tokio::spawn(async move {
            let mut stream = response_stream;
            let mut init_done = false;
            let start = std::time::Instant::now();
            let mut seq_counter: u64 = 0;
            loop {
                if !init_done && start.elapsed() > core::time::Duration::from_secs(30) {
                    tracing::error!("PQv1 InitResponse timeout");
                    break;
                }
                let await_start = std::time::Instant::now();
                let msg = tokio::select! {
                    m = stream.message() => match m {
                        Ok(Some(msg)) => msg,
                        Ok(None) => { tracing::warn!("PQv1 stream closed"); break; }
                        Err(e) => { tracing::error!("PQv1 stream error: {}", e); break; }
                    },
                    // Ctrl+C / shutdown — stop reading promptly instead of
                    // waiting for the next server message (which could be never
                    // if the topic is idle).
                    () = cancel_token.cancelled() => {
                        tracing::info!("PQv1 background task cancelled (shutdown)");
                        break;
                    }
                };
                // Downloader busy = time a Read request is in-flight (awaiting
                // the next server message). idle = the processing below.
                source_counters.add_download_busy(await_start.elapsed());
                // Only a real error code (not SUCCESS, not UNSPECIFIED) aborts the stream.
                if msg.status != YDB_STATUS_UNSPECIFIED && msg.status != YDB_STATUS_SUCCESS {
                    tracing::error!("PQv1 status: {}, issues: {:?}", msg.status, msg.issues);
                    break;
                }
                match msg.response {
                    Some(migration_streaming_read_server_message::Response::InitResponse(r)) => {
                        init_done = true;
                        tracing::info!("PQv1 session: {}", r.session_id);
                        if request_tx.send(read_request()).is_err() {
                            tracing::warn!("PQv1 request channel closed; stopping stream");
                            break;
                        }
                    }
                    Some(migration_streaming_read_server_message::Response::Assigned(a)) => {
                        #[expect(
                            clippy::cast_possible_wrap,
                            reason = "partition ids from YDB always fit in i64"
                        )]
                        let pid = a.partition as i64;
                        if !assigned.contains(&pid) {
                            tracing::debug!("PQv1 skip unassigned partition={}", pid);
                            continue;
                        }
                        tracing::debug!(
                            "PQv1 lock partition={} read_offset={} end_offset={}",
                            pid,
                            a.read_offset,
                            a.end_offset
                        );
                        if request_tx
                            .send(MigrationStreamingReadClientMessage {
                                request: Some(
                                    migration_streaming_read_client_message::Request::StartRead(
                                        migration_streaming_read_client_message::StartRead {
                                            topic: a.topic,
                                            cluster: a.cluster,
                                            partition: a.partition,
                                            assign_id: a.assign_id,
                                            read_offset: a.read_offset,
                                            commit_offset: a.read_offset,
                                            verify_read_offset: true,
                                        },
                                    ),
                                ),
                                token: Vec::new(),
                            })
                            .is_err()
                        {
                            tracing::warn!("PQv1 request channel closed; stopping stream");
                            break;
                        }
                    }
                    Some(migration_streaming_read_server_message::Response::DataBatch(db)) => {
                        if drop_before_decompress {
                            // Bench: count + discard before decompression.
                            for pd in db.partition_data {
                                for batch in pd.batches {
                                    for md in batch.message_data {
                                        source_counters.add_compressed_bytes(md.data.len() as u64);
                                        source_counters.add_messages(1);
                                    }
                                }
                            }
                            if request_tx.send(read_request()).is_err() {
                                break;
                            }
                            continue;
                        }
                        let seq = seq_counter;
                        seq_counter += 1;
                        let mut parts: Vec<RawPart> = Vec::with_capacity(db.partition_data.len());
                        for pd in db.partition_data {
                            #[expect(
                                clippy::cast_possible_wrap,
                                reason = "partition ids from YDB always fit in i64"
                            )]
                            let pid = pd.partition as i64;
                            let cookie = pd.cookie;
                            let mut msgs = Vec::new();
                            for batch in pd.batches {
                                let write_timestamp_ms = batch.write_timestamp_ms;
                                for md in batch.message_data {
                                    source_counters.add_compressed_bytes(md.data.len() as u64);
                                    source_counters.add_messages(1);
                                    msgs.push(RawMsg {
                                        data: md.data,
                                        codec: md.codec,
                                        uncompressed_size: md.uncompressed_size,
                                        offset: md.offset,
                                        write_timestamp_ms,
                                    });
                                }
                            }
                            if !msgs.is_empty() {
                                parts.push(RawPart { pid, cookie, msgs });
                            }
                        }
                        if parts.is_empty() {
                            if request_tx.send(read_request()).is_err() {
                                break;
                            }
                            continue;
                        }
                        let peak_bytes = parts
                            .iter()
                            .flat_map(|part| &part.msgs)
                            .map(|message| {
                                message
                                    .data
                                    .len()
                                    .saturating_add(message.uncompressed_size as usize)
                            })
                            .fold(0_usize, usize::saturating_add);
                        let reservation = memory.reserve(peak_bytes).await;
                        let Ok(slot) = Arc::clone(&decompress_slots).acquire_owned().await else {
                            break;
                        };
                        let sc = Arc::clone(&source_counters);
                        let decoded_tx_w = decoded_tx.clone();
                        tokio::task::spawn_blocking(move || {
                            let _slot = slot;
                            let mut dec_parts: Vec<DecodedPart> = Vec::with_capacity(parts.len());
                            for RawPart { pid, cookie, msgs } in parts {
                                let mut decoded: Vec<DecodedMessage> =
                                    Vec::with_capacity(msgs.len());
                                for rm in msgs {
                                    let decomp_start = std::time::Instant::now();
                                    match decompress(rm.data, rm.codec, rm.uncompressed_size) {
                                        Ok(data) => {
                                            sc.add_decomp_busy(decomp_start.elapsed());
                                            sc.add_decompressed_bytes(data.len() as u64);
                                            decoded.push(DecodedMessage {
                                                data,
                                                cookie,
                                                offset: rm.offset,
                                                write_timestamp_ms: rm.write_timestamp_ms,
                                                memory: reservation.clone(),
                                            });
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "PQv1 decompress failed: codec={} offset={}: {}",
                                                rm.codec,
                                                rm.offset,
                                                e
                                            );
                                        }
                                    }
                                }
                                dec_parts.push(DecodedPart { pid, msgs: decoded });
                            }
                            // Always send (even if some parts are empty) so the
                            // merge task's `next_seq` advances — skipping a seq
                            // would stall every later batch in the reorder buffer.
                            // A send error means the merge channel closed (shutdown);
                            // the result is intentionally ignored.
                            let _send = decoded_tx_w.blocking_send(DecodedBatch {
                                seq,
                                parts: dec_parts,
                            });
                        });
                        if request_tx.send(read_request()).is_err() {
                            tracing::warn!("PQv1 request channel closed; stopping stream");
                            break;
                        }
                    }
                    _ => {}
                }
            }
            tracing::info!("PQv1 background task exited");
        });

        Ok((Self { inner }, prs))
    }

    /// `PQv1` (Logbroker) does not expose a `DescribeTopic` gRPC method.
    ///
    /// This always returns `Err` with guidance to configure `partition_ids` in the
    /// source config. The caller in `main` treats this as a signal to try the static
    /// `partition_ids` fallback path.
    pub fn describe_topic(_endpoint: &str, _topic_path: &str, _token: &str) -> anyhow::Result<i32> {
        Err(anyhow::anyhow!(
            "PQv1 DescribeTopic is not supported; configure partition_ids in source config"
        ))
    }

    /// Discover available partition IDs by doing a short-lived handshake with the
    /// `PQv1` proxy. Opens a bidi stream, sends `InitRequest`, collects partition IDs
    /// from `Assigned` server messages, then closes the connection.
    ///
    /// Used when `partition_ids` is omitted from the source config — the caller gets
    /// the full partition list and then distributes them across workers via modulo.
    /// Read one server message during partition discovery, validating the status code
    /// and the overall deadline. Returns `None` when the stream ended (caller should stop).
    async fn read_discovery_message(
        stream: &mut tonic::Streaming<MigrationStreamingReadServerMessage>,
        deadline: std::time::Instant,
    ) -> anyhow::Result<Option<MigrationStreamingReadServerMessage>> {
        if std::time::Instant::now() > deadline {
            anyhow::bail!("discover_partitions: timed out waiting for Assigned messages");
        }
        match stream.message().await {
            Ok(Some(m)) => {
                if m.status != YDB_STATUS_UNSPECIFIED && m.status != YDB_STATUS_SUCCESS {
                    anyhow::bail!(
                        "discover_partitions: server status={}, issues={:?}",
                        m.status,
                        m.issues
                    );
                }
                Ok(Some(m))
            }
            Ok(None) => Ok(None),
            Err(e) => anyhow::bail!("discover_partitions stream error: {e}"),
        }
    }

    /// Handle one validated server message during partition discovery. Returns `false`
    /// once we start getting `DataBatch`: by then all `Assigned` messages have been seen.
    fn handle_discovery_message(
        msg: MigrationStreamingReadServerMessage,
        request_tx: &mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
        partition_ids: &mut Vec<i64>,
    ) -> bool {
        match msg.response {
            Some(migration_streaming_read_server_message::Response::InitResponse(r)) => {
                tracing::info!("discover_partitions: session={}", r.session_id);
                if request_tx.send(read_request()).is_err() {
                    tracing::warn!("PQv1 request channel closed; stopping stream");
                    return false;
                }
                true
            }
            Some(migration_streaming_read_server_message::Response::Assigned(a)) => {
                #[expect(
                    clippy::cast_possible_wrap,
                    reason = "partition ids from YDB always fit in i64"
                )]
                let pid = a.partition as i64;
                tracing::debug!("discover_partitions: found partition={}", pid);
                partition_ids.push(pid);
                true
            }
            Some(migration_streaming_read_server_message::Response::DataBatch(_)) => false,
            _ => true,
        }
    }

    pub async fn discover_partitions(
        endpoint: &str,
        topic_path: &str,
        consumer: &str,
        token: &str,
    ) -> anyhow::Result<Vec<i64>> {
        let (scheme, main_host, _) = parse_endpoint(endpoint)?;
        let main_uri = http_uri(&scheme, &main_host)?;

        // Step 1: proxy discovery
        let proxy = match discover_proxy(&main_uri, token).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Proxy discovery failed: {}. Using main endpoint.", e);
                main_host
            }
        };
        // Steps 2-3: open the stream, send InitRequest, collect Assigned partitions.
        let (mut stream, request_tx) =
            Self::open_discovery_stream(&scheme, &proxy, topic_path, consumer, token).await?;
        let partition_ids = Self::collect_assigned_partitions(&mut stream, &request_tx).await?;

        Ok(partition_ids)
    }

    /// Open the `MigrationStreamingRead` bidi stream on the proxy and send the
    /// `InitRequest`. Returns the response stream and the request channel used to
    /// send `Read`/`StartRead` messages.
    async fn open_discovery_stream(
        scheme: &str,
        proxy: &str,
        topic_path: &str,
        consumer: &str,
        token: &str,
    ) -> anyhow::Result<(
        tonic::Streaming<MigrationStreamingReadServerMessage>,
        mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
    )> {
        tracing::info!(
            "PQv1 discover_partitions: proxy={} topic={}",
            proxy,
            topic_path
        );
        let target_uri = http_uri(scheme, proxy)?;
        let h2_service = connect_http2_prior_knowledge(&target_uri).await?;
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(
                migration_streaming_read_client_message::Request::InitRequest(InitRequest {
                    topics_read_settings: vec![TopicReadSettings {
                        topic: topic_path.to_string(),
                        partition_group_ids: vec![],
                        start_from_written_at_ms: 0,
                    }],
                    consumer: consumer.to_string(),
                    read_only_original: false,
                    max_lag_duration_ms: 0,
                    start_from_written_at_ms: 0,
                    max_supported_block_format_version: 0,
                    max_meta_cache_size: 0,
                    read_params: Some(ReadParams {
                        max_read_messages_count: 0,
                        max_read_size: 1_048_576,
                    }),
                    session_id: String::new(),
                    connection_attempt: 0,
                    state: None,
                    idle_timeout_ms: 0,
                    ranges_mode: false,
                }),
            ),
            token: token.as_bytes().to_vec(),
        })?;

        let mut req = Request::new(RequestStream { rx: request_rx });
        set_ydb_headers(req.metadata_mut(), token);

        let mut grpc = tonic::client::Grpc::with_origin(h2_service, target_uri)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        grpc.ready()
            .await
            .map_err(|e| anyhow!("grpc not ready: {e}"))?;
        let path = http::uri::PathAndQuery::from_static(
            "/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead",
        );
        let codec = tonic_prost::ProstCodec::<
            MigrationStreamingReadClientMessage,
            MigrationStreamingReadServerMessage,
        >::default();
        let stream = grpc
            .streaming(req, path, codec)
            .await
            .map_err(|e| anyhow!("MigrationStreamingRead failed: {e}"))?
            .into_inner();
        Ok((stream, request_tx))
    }

    /// Read server messages until all `Assigned` partitions have been reported
    /// (signaled by the first `DataBatch`) or the deadline passes.
    async fn collect_assigned_partitions(
        stream: &mut tonic::Streaming<MigrationStreamingReadServerMessage>,
        request_tx: &mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
    ) -> anyhow::Result<Vec<i64>> {
        let deadline = std::time::Instant::now() + core::time::Duration::from_secs(30);
        let mut partition_ids: Vec<i64> = Vec::new();
        loop {
            let Some(msg) = Self::read_discovery_message(stream, deadline).await? else {
                break;
            };
            if !Self::handle_discovery_message(msg, request_tx, &mut partition_ids) {
                break;
            }
        }
        if partition_ids.is_empty() {
            anyhow::bail!("discover_partitions: no partitions discovered");
        }
        partition_ids.sort_unstable();
        tracing::info!(
            "discover_partitions: found {} partitions: {:?}",
            partition_ids.len(),
            partition_ids,
        );
        Ok(partition_ids)
    }

    pub fn commit(&self, _partition_id: i64, cookie: CommitCookie) -> anyhow::Result<()> {
        self.inner
            .request_tx
            .send(MigrationStreamingReadClientMessage {
                request: Some(migration_streaming_read_client_message::Request::Commit(
                    migration_streaming_read_client_message::Commit {
                        cookies: vec![cookie],
                        offset_ranges: vec![],
                    },
                )),
                token: Vec::new(),
            })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Decompression
// ---------------------------------------------------------------------------

/// Decompress a message body. RAW (codec 1) reuses the input buffer (zero-copy).
fn decompress(data: Vec<u8>, codec: i32, uncompressed_size: u64) -> anyhow::Result<Bytes> {
    match codec {
        1 => Ok(Bytes::from(data)), // RAW — move, no copy
        2 => {
            use std::io::Read as _;
            let mut d = flate2::read::GzDecoder::new(&*data);
            let mut buf = Vec::with_capacity(uncompressed_size as usize);
            d.read_to_end(&mut buf)?;
            Ok(Bytes::from(buf))
        }
        4 => Ok(Bytes::from(zstd::decode_all(&*data)?)),
        _ => Err(anyhow!("Unsupported codec: {codec}")),
    }
}

// ---------------------------------------------------------------------------
// PqV1Source
// ---------------------------------------------------------------------------

pub struct PqV1Source {
    client: PqV1Client,
    rx: mpsc::Receiver<DecodedMessage>,
    partition_id: i64,
    topic_name: Arc<str>,
    last_write_timestamp_ms: Option<i64>,
    _config: YdsSourceConfig,
}

impl PqV1Source {
    #[must_use]
    pub fn new(
        client: PqV1Client,
        rx: mpsc::Receiver<DecodedMessage>,
        partition_id: i64,
        config: YdsSourceConfig,
    ) -> Self {
        let topic_name = Arc::from(config.topic_path.as_str());
        Self {
            client,
            rx,
            partition_id,
            topic_name,
            last_write_timestamp_ms: None,
            _config: config,
        }
    }
}

impl Source for PqV1Source {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            let Some(first) = self.rx.recv().await else {
                return Ok(ReadResult::Batch(MessageBatch {
                    messages: Vec::new(),
                    partition_id: self.partition_id,
                    commit_marker: None,
                    memory: Vec::new(),
                }));
            };
            let mut last_cookie: Option<CommitCookie> = first.cookie;
            let mut memory = vec![first.memory];
            let first_write_timestamp_ms = i64::try_from(first.write_timestamp_ms)?;
            self.observe_write_timestamp(first.offset, first_write_timestamp_ms);
            let mut messages = vec![Message {
                value: first.data,
                meta: MessageMeta {
                    topic_name: Some(Arc::clone(&self.topic_name)),
                    partition: Some(SourcePartition::Int(self.partition_id)),
                    offset: Some(i64::try_from(first.offset)?),
                    write_timestamp_ms: Some(first_write_timestamp_ms),
                },
            }];
            while let Ok(msg) = self.rx.try_recv() {
                last_cookie = msg.cookie;
                memory.push(msg.memory);
                let write_timestamp_ms = i64::try_from(msg.write_timestamp_ms)?;
                self.observe_write_timestamp(msg.offset, write_timestamp_ms);
                messages.push(Message {
                    value: msg.data,
                    meta: MessageMeta {
                        topic_name: Some(Arc::clone(&self.topic_name)),
                        partition: Some(SourcePartition::Int(self.partition_id)),
                        offset: Some(i64::try_from(msg.offset)?),
                        write_timestamp_ms: Some(write_timestamp_ms),
                    },
                });
            }
            let commit_marker = last_cookie.map(|cookie| {
                CommitMarker::new(PqV1CommitMarker {
                    partition_id: self.partition_id,
                    cookie,
                })
            });
            Ok(ReadResult::Batch(MessageBatch {
                messages,
                partition_id: self.partition_id,
                commit_marker,
                memory,
            }))
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        marker: &'ctx CommitMarker,
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move {
            let m = marker
                .downcast_ref::<PqV1CommitMarker>()
                .ok_or_else(|| anyhow!("Invalid commit marker"))?;
            self.client.commit(m.partition_id, m.cookie)
        })
    }
}

impl PqV1Source {
    fn observe_write_timestamp(&mut self, offset: u64, current: i64) {
        if self
            .last_write_timestamp_ms
            .is_some_and(|previous| current < previous)
        {
            tracing::warn!(
                partition = self.partition_id,
                offset,
                previous_write_timestamp_ms = self.last_write_timestamp_ms,
                write_timestamp_ms = current,
                "PQv1 write timestamp moved backwards; record-time sink semantics may be unsafe"
            );
        }
        self.last_write_timestamp_ms = Some(current);
    }
}
