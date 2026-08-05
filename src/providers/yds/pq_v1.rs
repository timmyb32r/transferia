//! PQv1 (PersQueue V1) gRPC client for Logbroker.
//!
//! Flow: ListEndpoints (discover proxy) → MigrationStreamingRead bidi stream on the
//! proxy → InitResponse → Assigned → StartRead → DataBatch. Transport is HTTP/2 with
//! prior knowledge (Go-compatible), bridged into tonic via a small `tower::Service`.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::Stream;
use hyper::client::conn::http2;
use tokio::sync::{mpsc, Mutex};
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::Uri;
use tonic::Request;

use crate::config::yaml::YDB_DATABASE;
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::message::{Message, MessageBatch};
use crate::Ydb::pers_queue::v1::{
    migration_streaming_read_client_message::{self, InitRequest, TopicReadSettings},
    migration_streaming_read_server_message,
    CommitCookie, MigrationStreamingReadClientMessage, MigrationStreamingReadServerMessage,
    ReadParams,
};

/// `Ydb.StatusIds.SUCCESS`. Status codes live in the reserved range [400000, 400999];
/// SUCCESS is 400000 (NOT 0 — 0 is STATUS_CODE_UNSPECIFIED, sent on streaming data msgs).
const YDB_STATUS_SUCCESS: i32 = 400000;

/// tonic's default decode cap is 4 MiB; Logbroker `DataBatch` messages can exceed it.
const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

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
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

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
    let addr = format!("{}:{}", host, port);

    let stream = tokio::net::TcpStream::connect(&addr).await
        .map_err(|e| anyhow!("TCP connect to {}: {}", addr, e))?;
    stream.set_nodelay(true)?;

    let io = hyper_util::rt::TokioIo::new(stream);
    let (send_request, conn) = http2::handshake(hyper_util::rt::TokioExecutor::new(), io).await
        .map_err(|e| anyhow!("HTTP/2 handshake failed: {}", e))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("HTTP/2 connection error: {}", e);
        }
    });

    tracing::debug!("HTTP/2 prior-knowledge connection to {}", addr);
    Ok(H2Service { inner: send_request })
}

/// Attach the YDB auth/routing headers that Logbroker expects on every call.
fn set_ydb_headers(md: &mut MetadataMap, token: &str) {
    if let Ok(v) = AsciiMetadataValue::try_from(token) {
        md.insert("x-ydb-auth-ticket", v);
    }
    md.insert("x-ydb-database", AsciiMetadataValue::from_static(YDB_DATABASE));
    md.insert("x-ydb-sdk-build-info", AsciiMetadataValue::from_static("go-sdk-2021.04.1"));
    md.insert("user-agent", AsciiMetadataValue::from_static("grpc-go/1.80.0"));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn group_to_partition(group: i64) -> i64 { group }
pub fn partition_to_group(partition: i64) -> i64 { partition }

/// Parse a connection string into `(scheme, host, database)`. `database` is derived from
/// the path/query for compatibility but is not authoritative — the cluster DB is `YDB_DATABASE`.
pub fn parse_endpoint(conn_str: &str) -> anyhow::Result<(String, String, String)> {
    let uri: Uri = conn_str.parse()
        .map_err(|e| anyhow!("Invalid connection string '{}': {}", conn_str, e))?;
    let scheme = uri.scheme_str().unwrap_or("grpc").to_string();
    let host = uri.authority().map(|a| a.as_str()).unwrap_or("localhost:2135").to_string();
    let database = {
        let path = uri.path().trim_start_matches('/').to_string();
        if !path.is_empty() { format!("/{}", path) } else { YDB_DATABASE.to_string() }
    };
    Ok((scheme, host, database))
}

fn http_uri(scheme: &str, host: &str) -> anyhow::Result<Uri> {
    let s = if scheme == "grpcs" { "https" } else { "http" };
    format!("{}://{}", s, host).parse().map_err(|e| anyhow!("bad uri {}://{}: {}", s, host, e))
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A decompressed message handed to the pipeline. `CommitCookie` is `Copy` (a couple of
/// integers), so carrying it by value per message is cheap.
pub struct DecodedMessage {
    pub data: Bytes,
    pub cookie: Option<CommitCookie>,
}

pub struct PqV1CommitMarker {
    pub partition_id: i64,
    pub cookie: CommitCookie,
}

struct RequestStream {
    rx: mpsc::UnboundedReceiver<MigrationStreamingReadClientMessage>,
}

impl Stream for RequestStream {
    type Item = MigrationStreamingReadClientMessage;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// A `Read` request — asks the server for the next batch.
#[inline]
fn read_request() -> MigrationStreamingReadClientMessage {
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
    partition_queues: Mutex<HashMap<i64, mpsc::UnboundedSender<DecodedMessage>>>,
}

/// Discover a proxy endpoint via `ListEndpoints` over HTTP/2 prior knowledge.
/// The gRPC response type is `GetOperationResponse` (matching Go's `conn.Invoke`).
async fn discover_proxy(main_uri: &Uri, token: &str) -> anyhow::Result<String> {
    use crate::Ydb::discovery::{ListEndpointsRequest, ListEndpointsResult};
    use crate::Ydb::operations::GetOperationResponse;
    use prost::Message;

    let h2 = connect_http2_prior_knowledge(main_uri).await?;
    let mut grpc = tonic::client::Grpc::<H2Service>::with_origin(h2, main_uri.clone());

    let mut req = Request::new(ListEndpointsRequest { database: YDB_DATABASE.to_string(), service: vec![] });
    set_ydb_headers(req.metadata_mut(), token);

    grpc.ready().await.map_err(|e| anyhow!("ListEndpoints ready: {}", e))?;
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints");
    let resp: GetOperationResponse = grpc
        .unary(req, path, tonic_prost::ProstCodec::<ListEndpointsRequest, GetOperationResponse>::default())
        .await
        .map_err(|e| anyhow!("ListEndpoints failed: {}", e))?
        .into_inner();

    let op = resp.operation.ok_or_else(|| anyhow!("no operation"))?;
    if !op.ready {
        anyhow::bail!("ListEndpoints not ready");
    }
    // SUCCESS is 400000, not 0 (0 == UNSPECIFIED also acceptable for forward-compat).
    if op.status != 0 && op.status != YDB_STATUS_SUCCESS {
        anyhow::bail!("ListEndpoints status={}", op.status);
    }
    let result = op.result.ok_or_else(|| anyhow!("no result"))?;
    let eps = ListEndpointsResult::decode(result.value.as_slice())?;
    eps.endpoints.first()
        .map(|e| format!("{}:{}", e.address, e.port))
        .ok_or_else(|| anyhow!("no endpoints"))
}

impl PqV1Client {
    pub async fn connect(
        endpoint: &str, topic_path: &str, consumer: &str,
        token: &str, partition_group_ids: &[i64],
    ) -> anyhow::Result<(Self, HashMap<i64, mpsc::UnboundedReceiver<DecodedMessage>>)> {
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
        tracing::info!("PQv1 connecting: proxy={} topic={} consumer={}", proxy, topic_path, consumer);

        // Step 2: open the bidi stream on the proxy.
        let h2_service = connect_http2_prior_knowledge(&target_uri).await?;

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(migration_streaming_read_client_message::Request::InitRequest(InitRequest {
                topics_read_settings: vec![TopicReadSettings {
                    topic: topic_path.to_string(), partition_group_ids: vec![], start_from_written_at_ms: 0,
                }],
                consumer: consumer.to_string(), read_only_original: false, max_lag_duration_ms: 0,
                start_from_written_at_ms: 0, max_supported_block_format_version: 0, max_meta_cache_size: 0,
                read_params: Some(ReadParams { max_read_messages_count: 0, max_read_size: 1048576 }),
                session_id: String::new(), connection_attempt: 0, state: None, idle_timeout_ms: 0, ranges_mode: false,
            })),
            token: token.as_bytes().to_vec(),
        })?;

        let mut req = Request::new(RequestStream { rx: request_rx });
        set_ydb_headers(req.metadata_mut(), token);

        let mut grpc = tonic::client::Grpc::with_origin(h2_service, target_uri)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        grpc.ready().await.map_err(|e| anyhow!("grpc not ready: {}", e))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static("/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead");
        let codec = tonic_prost::ProstCodec::<MigrationStreamingReadClientMessage, MigrationStreamingReadServerMessage>::default();
        let response_stream = grpc.streaming(req, path, codec).await
            .map_err(|e| anyhow!("MigrationStreamingRead failed: {}", e))?
            .into_inner();

        // Per-partition queues for the partitions we own.
        let assigned: HashSet<i64> = partition_group_ids.iter().map(|&g| group_to_partition(g)).collect();
        let mut pqs = HashMap::with_capacity(assigned.len());
        let mut prs = HashMap::with_capacity(assigned.len());
        for &pid in &assigned {
            let (tx, rx) = mpsc::unbounded_channel();
            pqs.insert(pid, tx);
            prs.insert(pid, rx);
        }

        let inner = Arc::new(PqV1ClientInner {
            request_tx: request_tx.clone(),
            partition_queues: Mutex::new(pqs),
        });
        let inner_bg = inner.clone();

        tokio::spawn(async move {
            let mut stream = response_stream;
            let mut init_done = false;
            let start = std::time::Instant::now();
            loop {
                if !init_done && start.elapsed() > std::time::Duration::from_secs(30) {
                    tracing::error!("PQv1 InitResponse timeout");
                    break;
                }
                let msg = match stream.message().await {
                    Ok(Some(m)) => m,
                    Ok(None) => { tracing::warn!("PQv1 stream closed"); break; }
                    Err(e) => { tracing::error!("PQv1 stream error: {}", e); break; }
                };
                // Only a real error code (not SUCCESS, not UNSPECIFIED) aborts the stream.
                if msg.status != 0 && msg.status != YDB_STATUS_SUCCESS {
                    tracing::error!("PQv1 status: {}, issues: {:?}", msg.status, msg.issues);
                    break;
                }
                match msg.response {
                    Some(migration_streaming_read_server_message::Response::InitResponse(r)) => {
                        init_done = true;
                        tracing::info!("PQv1 session: {}", r.session_id);
                        let _ = request_tx.send(read_request());
                    }
                    Some(migration_streaming_read_server_message::Response::Assigned(a)) => {
                        let pid = a.partition as i64;
                        if !assigned.contains(&pid) {
                            tracing::debug!("PQv1 skip unassigned partition={}", pid);
                            continue;
                        }
                        tracing::debug!("PQv1 lock partition={} read_offset={} end_offset={}", pid, a.read_offset, a.end_offset);
                        let _ = request_tx.send(MigrationStreamingReadClientMessage {
                            request: Some(migration_streaming_read_client_message::Request::StartRead(
                                migration_streaming_read_client_message::StartRead {
                                    topic: a.topic, cluster: a.cluster, partition: a.partition,
                                    assign_id: a.assign_id, read_offset: a.read_offset,
                                    commit_offset: a.read_offset, verify_read_offset: true,
                                },
                            )),
                            token: Vec::new(),
                        });
                    }
                    Some(migration_streaming_read_server_message::Response::DataBatch(db)) => {
                        let queues = inner_bg.partition_queues.lock().await;
                        // Iterate by value: for RAW payloads this moves the `Vec<u8>` into
                        // `Bytes` with no copy.
                        for pd in db.partition_data {
                            let pid = pd.partition as i64;
                            let Some(tx) = queues.get(&pid) else { continue };
                            let cookie = pd.cookie; // Option<CommitCookie>, Copy
                            for batch in pd.batches {
                                for md in batch.message_data {
                                    let data = match decompress(md.data, md.codec, md.uncompressed_size) {
                                        Ok(d) => d,
                                        Err(e) => {
                                            tracing::error!("PQv1 decompress failed: codec={} offset={}: {}", md.codec, md.offset, e);
                                            continue;
                                        }
                                    };
                                    let _ = tx.send(DecodedMessage { data, cookie });
                                }
                            }
                        }
                        drop(queues);
                        let _ = request_tx.send(read_request());
                    }
                    _ => {}
                }
            }
            tracing::info!("PQv1 background task exited");
        });

        Ok((Self { inner }, prs))
    }

    /// PQv1 (Logbroker) does not expose a DescribeTopic gRPC method.
    ///
    /// This always returns `Err` with guidance to configure `partition_ids` in the
    /// source config. The caller in `main` treats this as a signal to try the static
    /// `partition_ids` fallback path.
    pub async fn describe_topic(_endpoint: &str, _topic_path: &str, _token: &str) -> anyhow::Result<i32> {
        Err(anyhow::anyhow!(
            "PQv1 DescribeTopic is not supported; configure partition_ids in source config"
        ))
    }

    /// Discover available partition IDs by doing a short-lived handshake with the
    /// PQv1 proxy. Opens a bidi stream, sends `InitRequest`, collects partition IDs
    /// from `Assigned` server messages, then closes the connection.
    ///
    /// Used when `partition_ids` is omitted from the source config — the caller gets
    /// the full partition list and then distributes them across workers via modulo.
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
        let target_uri = http_uri(&scheme, &proxy)?;
        tracing::info!("PQv1 discover_partitions: proxy={} topic={}", proxy, topic_path);

        // Step 2: open bidi stream, send InitRequest
        let h2_service = connect_http2_prior_knowledge(&target_uri).await?;
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(migration_streaming_read_client_message::Request::InitRequest(InitRequest {
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
                    max_read_size: 1048576,
                }),
                session_id: String::new(),
                connection_attempt: 0,
                state: None,
                idle_timeout_ms: 0,
                ranges_mode: false,
            })),
            token: token.as_bytes().to_vec(),
        })?;

        let mut req = Request::new(RequestStream { rx: request_rx });
        set_ydb_headers(req.metadata_mut(), token);

        let mut grpc = tonic::client::Grpc::with_origin(h2_service, target_uri)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        grpc.ready().await.map_err(|e| anyhow!("grpc not ready: {}", e))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead",
        );
        let codec = tonic_prost::ProstCodec::<
            MigrationStreamingReadClientMessage,
            MigrationStreamingReadServerMessage,
        >::default();
        let mut stream = grpc
            .streaming(req, path, codec)
            .await
            .map_err(|e| anyhow!("MigrationStreamingRead failed: {}", e))?
            .into_inner();

        // Step 3: collect Assigned partition IDs
        let mut partition_ids: Vec<i64> = Vec::new();
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);
        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("discover_partitions: timed out waiting for Assigned messages");
            }
            let msg = match stream.message().await {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => anyhow::bail!("discover_partitions stream error: {}", e),
            };
            if msg.status != 0 && msg.status != YDB_STATUS_SUCCESS {
                anyhow::bail!(
                    "discover_partitions: server status={}, issues={:?}",
                    msg.status,
                    msg.issues
                );
            }
            match msg.response {
                Some(migration_streaming_read_server_message::Response::InitResponse(r)) => {
                    tracing::info!("discover_partitions: session={}", r.session_id);
                    let _ = request_tx.send(read_request());
                }
                Some(migration_streaming_read_server_message::Response::Assigned(a)) => {
                    let pid = a.partition as i64;
                    tracing::debug!("discover_partitions: found partition={}", pid);
                    partition_ids.push(pid);
                }
                // Once we start getting DataBatch we've seen all Assigned messages
                Some(migration_streaming_read_server_message::Response::DataBatch(_)) => {
                    break;
                }
                _ => {}
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

    pub async fn commit(&self, _partition_id: i64, cookie: CommitCookie) -> anyhow::Result<()> {
        let _ = self.inner.request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(migration_streaming_read_client_message::Request::Commit(
                migration_streaming_read_client_message::Commit {
                    cookies: vec![cookie], offset_ranges: vec![],
                },
            )),
            token: Vec::new(),
        });
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
            use std::io::Read;
            let mut d = flate2::read::GzDecoder::new(&data[..]);
            let mut buf = Vec::with_capacity(uncompressed_size as usize);
            d.read_to_end(&mut buf)?;
            Ok(Bytes::from(buf))
        }
        4 => Ok(Bytes::from(zstd::decode_all(&data[..])?)),
        _ => Err(anyhow!("Unsupported codec: {}", codec)),
    }
}

// ---------------------------------------------------------------------------
// PqV1Source
// ---------------------------------------------------------------------------

pub struct PqV1Source {
    client: PqV1Client,
    rx: mpsc::UnboundedReceiver<DecodedMessage>,
    partition_id: i64,
}

impl PqV1Source {
    pub fn new(client: PqV1Client, rx: mpsc::UnboundedReceiver<DecodedMessage>, partition_id: i64) -> Self {
        Self { client, rx, partition_id }
    }
}

impl Source for PqV1Source {
    fn read_batch<'a>(&'a mut self) -> BoxFuture<'a, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            let first = match self.rx.recv().await {
                Some(msg) => msg,
                None => return Ok(ReadResult::Batch(MessageBatch { messages: Vec::new(), partition_id: self.partition_id, commit_marker: None })),
            };
            let mut last_cookie: Option<CommitCookie> = first.cookie;
            let mut messages = vec![Message { value: first.data }];
            while let Ok(msg) = self.rx.try_recv() {
                last_cookie = msg.cookie;
                messages.push(Message { value: msg.data });
            }
            let commit_marker = last_cookie.map(|cookie| {
                CommitMarker::new(PqV1CommitMarker { partition_id: self.partition_id, cookie })
            });
            Ok(ReadResult::Batch(MessageBatch { messages, partition_id: self.partition_id, commit_marker }))
        })
    }

    fn commit_offsets<'a>(&'a mut self, marker: &'a CommitMarker) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let m = marker.downcast_ref::<PqV1CommitMarker>().ok_or_else(|| anyhow!("Invalid commit marker"))?;
            self.client.commit(m.partition_id, m.cookie).await
        })
    }
}
