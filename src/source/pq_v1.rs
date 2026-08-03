//! PQv1 (PersQueue V1) gRPC client for Logbroker.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use bytes::{BufMut, Bytes};
use futures_util::Stream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Endpoint, Uri};
use tonic::Request;
use tonic::codec::{Codec, EncodeBuf, Encoder};

// ---------------------------------------------------------------------------
// HTTP/2 prior-knowledge transport (Go-compatible)
// ---------------------------------------------------------------------------

use hyper::client::conn::http2;

/// A tower Service wrapper around hyper's HTTP/2 SendRequest.
/// This bridges Hyper 1.x (which doesn't impl tower::Service) to tonic (which needs it).
struct H2Service {
    inner: http2::SendRequest<tonic::body::Body>,
}

impl tower::Service<http::Request<tonic::body::Body>> for H2Service {
    type Response = http::Response<hyper::body::Incoming>;
    type Error = hyper::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<tonic::body::Body>) -> Self::Future {
        tracing::info!("[PQV1-H2] Outgoing: uri={:?} method={:?} headers={:?}",
            req.uri(), req.method(), req.headers());
        let fut = self.inner.send_request(req);
        Box::pin(fut)
    }
}

/// Establish an HTTP/2 prior-knowledge connection to the given URI.
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

    // Spawn the connection driver in background
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("HTTP/2 connection error: {}", e);
        }
    });

    // Wait for HTTP/2 SETTINGS exchange to complete before sending requests.
    // The Logbroker server requires full SETTINGS exchange (client→server→ack)
    // before accepting any gRPC requests. Without this, the server returns 400000.
    // Go gRPC waits for this implicitly; Hyper's HTTP/2 handshake does not.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tracing::info!("[PQV1-DIAG] HTTP/2 prior-knowledge connection established to {}", addr);
    Ok(H2Service { inner: send_request })
}

use crate::pipeline::source::{CommitMarker, Source};
use crate::types::message::{Message, MessageBatch};

/// YDB `Ydb.StatusIds.SUCCESS`. Status codes live in the reserved range
/// [400000, 400999]; SUCCESS is 400000 (NOT 0 — 0 is STATUS_CODE_UNSPECIFIED).
const YDB_STATUS_SUCCESS: i32 = 400000;

use crate::Ydb::pers_queue::v1::{
    migration_streaming_read_client_message::{self, InitRequest, TopicReadSettings},
    migration_streaming_read_server_message::{self},
    CommitCookie, MigrationStreamingReadClientMessage, MigrationStreamingReadServerMessage,
    ReadParams,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn group_to_partition(group: i64) -> i64 { group }
pub fn partition_to_group(partition: i64) -> i64 { partition }

pub fn parse_endpoint(conn_str: &str) -> anyhow::Result<(String, String, String)> {
    let uri: Uri = conn_str.parse()
        .map_err(|e| anyhow!("Invalid connection string '{}': {}", conn_str, e))?;
    let scheme = uri.scheme_str().unwrap_or("grpc").to_string();
    let host = uri.authority().map(|a| a.as_str()).unwrap_or("localhost:2135").to_string();
    let database = {
        let path = uri.path().trim_start_matches('/').to_string();
        if !path.is_empty() { format!("/{}", path) }
        else { uri.query().and_then(|q| q.split('&').find(|p| p.starts_with("database=")).map(|p| p.trim_start_matches("database=").to_string())).unwrap_or_else(|| "/Root".to_string()) }
    };
    Ok((scheme, host, database))
}

// ---------------------------------------------------------------------------
// Wire-format reorder: server expects field 20 (token) before field 1 (request)
// ---------------------------------------------------------------------------

fn reorder_token_first(raw: &[u8]) -> Vec<u8> {
    if raw.len() < 3 || raw[0] != 0x0a { return raw.to_vec(); }
    let mut pos = 1;
    let mut len1: usize = 0; let mut shift = 0;
    while pos < raw.len() { let b = raw[pos]; pos += 1; len1 |= ((b & 0x7F) as usize) << shift; if b & 0x80 == 0 { break; } shift += 7; }
    let req_end = pos + len1;
    if req_end >= raw.len() { return raw.to_vec(); }
    let mut out = Vec::with_capacity(raw.len());
    out.extend_from_slice(&raw[req_end..]);
    out.extend_from_slice(&raw[..req_end]);
    out
}

#[derive(Debug, Clone)]
struct ReorderCodec;

impl Codec for ReorderCodec {
    type Encode = MigrationStreamingReadClientMessage;
    type Decode = MigrationStreamingReadServerMessage;
    type Decoder = tonic_prost::ProstDecoder<MigrationStreamingReadServerMessage>;
    type Encoder = ReorderEncoder;
    fn encoder(&mut self) -> Self::Encoder { ReorderEncoder }
    fn decoder(&mut self) -> Self::Decoder { tonic_prost::ProstDecoder::new(Default::default()) }
}

#[derive(Debug, Clone)]
struct ReorderEncoder;

impl Encoder for ReorderEncoder {
    type Item = MigrationStreamingReadClientMessage;
    type Error = tonic::Status;
    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let raw = prost::Message::encode_to_vec(&item);
        let orig_hex: String = raw.iter().map(|b| format!("{:02x}", b)).collect();
        let reordered = reorder_token_first(&raw);
        let reord_hex: String = reordered.iter().map(|b| format!("{:02x}", b)).collect();
        if orig_hex != reord_hex {
            tracing::info!("[PQV1-DIAG] Reordered FULL: {}", reord_hex);
        }
        buf.put_slice(&reordered);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct DecodedMessage {
    pub offset: u64, pub seq_no: u64, pub create_timestamp_ms: u64,
    pub data: Bytes, pub partition_id: i64, pub cookie: Option<CommitCookie>,
}

pub struct PqV1CommitMarker { pub partition_id: i64, pub cookie: CommitCookie }

struct RequestStream { rx: mpsc::UnboundedReceiver<MigrationStreamingReadClientMessage> }

impl Stream for RequestStream {
    type Item = MigrationStreamingReadClientMessage;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> { self.rx.poll_recv(cx) }
}

// ---------------------------------------------------------------------------
// PqV1Client
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PqV1Client { inner: Arc<PqV1ClientInner> }

struct PqV1ClientInner {
    request_tx: mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
    partition_queues: Mutex<HashMap<i64, mpsc::UnboundedSender<DecodedMessage>>>,
    session_id: String,
}

/// Query ListEndpoints using HTTP/2 prior knowledge to discover a proxy endpoint.
/// Uses GetOperationResponse as the gRPC response type (matching Go's conn.Invoke behavior).
async fn discover_proxy(main_uri: &Uri, database: &str, token: &str) -> anyhow::Result<String> {
    use crate::Ydb::discovery::ListEndpointsRequest;
    use crate::Ydb::operations::GetOperationResponse;
    use prost::Message;

    tracing::info!("[PQV1-DIAG] discover_proxy: connecting to {}", main_uri);
    let h2 = connect_http2_prior_knowledge(main_uri).await?;
    let mut grpc = tonic::client::Grpc::<H2Service>::with_origin(h2, main_uri.clone());

    let req_body = ListEndpointsRequest { database: database.to_string(), service: vec![] };
    let mut req = Request::new(req_body);
    if let Ok(v) = AsciiMetadataValue::try_from(token) { req.metadata_mut().insert("x-ydb-auth-ticket", v); }
    if let Ok(v) = AsciiMetadataValue::try_from(database) { req.metadata_mut().insert("x-ydb-database", v); }
    req.metadata_mut().insert("x-ydb-sdk-build-info", AsciiMetadataValue::from_static("go-sdk-2021.04.1"));
    req.metadata_mut().insert("user-agent", AsciiMetadataValue::from_static("grpc-go/1.80.0"));

    grpc.ready().await.map_err(|e| anyhow!("ListEndpoints ready: {}", e))?;
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints");

    // CRITICAL: Go SDK uses GetOperationResponse (not ListEndpointsResponse) as the gRPC response type.
    // The server validates the response type in the method descriptor.
    let resp: GetOperationResponse = grpc.unary(req, path, tonic_prost::ProstCodec::<ListEndpointsRequest, GetOperationResponse>::default()).await
        .map_err(|e| anyhow!("ListEndpoints failed: {}", e))?.into_inner();

    let op = resp.operation.ok_or_else(|| anyhow!("no operation"))?;
    tracing::info!("[PQV1-DIAG] ListEndpoints: ready={} status={}", op.ready, op.status);
    if !op.ready { anyhow::bail!("not ready"); }
    // SUCCESS is 400000, not 0. 0 (UNSPECIFIED) also passes for forward-compat.
    if op.status != 0 && op.status != YDB_STATUS_SUCCESS { anyhow::bail!("status={}", op.status); }

    let result = op.result.ok_or_else(|| anyhow!("no result"))?;
    let eps: crate::Ydb::discovery::ListEndpointsResult = Message::decode(result.value.as_slice())?;
    let proxy = eps.endpoints.first().map(|e| format!("{}:{}", e.address, e.port))
        .ok_or_else(|| anyhow!("no endpoints"))?;
    Ok(proxy)
}

impl PqV1Client {
    pub async fn connect(
        endpoint: &str, database: &str, topic_path: &str, consumer: &str,
        token: &str, partition_group_ids: &[i64],
    ) -> anyhow::Result<(Self, HashMap<i64, mpsc::UnboundedReceiver<DecodedMessage>>)> {
        // Step 1: Discover proxy via ListEndpoints on main endpoint (HTTP/2 prior knowledge)
        let (scheme, main_host, _) = parse_endpoint(endpoint)?;
        let main_uri: Uri = format!("{}://{}", if scheme == "grpcs" { "https" } else { "http" }, main_host).parse()?;

        let proxy = match discover_proxy(&main_uri, database, token).await {
            Ok(p) => {
                tracing::info!("[PQV1-DIAG] Using discovered proxy: {}", p);
                p
            }
            Err(e) => {
                tracing::warn!("[PQV1-DIAG] Proxy discovery failed: {}. Using main endpoint.", e);
                main_host
            }
        };

        let target_uri: Uri = format!("{}://{}", if scheme == "grpcs" { "https" } else { "http" }, proxy).parse()?;
        tracing::info!("[PQV1-DIAG] connect: uri={} database={} topic={} consumer={} tls=false", target_uri, database, topic_path, consumer);

        // Step 2: Connect to proxy via HTTP/2 prior knowledge
        let h2_service = connect_http2_prior_knowledge(&target_uri).await?;

        // Build InitRequest
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let init_msg = MigrationStreamingReadClientMessage {
            request: Some(migration_streaming_read_client_message::Request::InitRequest(InitRequest {
                topics_read_settings: vec![TopicReadSettings { topic: topic_path.to_string(), partition_group_ids: vec![], start_from_written_at_ms: 0 }],
                consumer: consumer.to_string(), read_only_original: false, max_lag_duration_ms: 0,
                start_from_written_at_ms: 0, max_supported_block_format_version: 0, max_meta_cache_size: 0,
                read_params: Some(ReadParams { max_read_messages_count: 0, max_read_size: 1048576 }),
                session_id: String::new(), connection_attempt: 0, state: None, idle_timeout_ms: 0, ranges_mode: false,
            })),
            token: token.as_bytes().to_vec(),
        };
        request_tx.send(init_msg)?;

        // Open bidi stream with custom codec (reorders token before request to match Go)
        let request_stream = RequestStream { rx: request_rx };
        let mut req = Request::new(request_stream);
        if let Ok(v) = AsciiMetadataValue::try_from(token) { req.metadata_mut().insert("x-ydb-auth-ticket", v); }
        if let Ok(v) = AsciiMetadataValue::try_from(database) { req.metadata_mut().insert("x-ydb-database", v); }
        req.metadata_mut().insert("x-ydb-sdk-build-info", AsciiMetadataValue::from_static("go-sdk-2021.04.1"));
        req.metadata_mut().insert("user-agent", AsciiMetadataValue::from_static("grpc-go/1.80.0"));

        tracing::info!("[PQV1-DIAG] Calling MigrationStreamingRead (with reorder codec)...");
        // Logbroker DataBatch messages can exceed tonic's 4 MiB default decode cap.
        let mut grpc = tonic::client::Grpc::with_origin(h2_service, target_uri)
            .max_decoding_message_size(128 * 1024 * 1024)
            .max_encoding_message_size(128 * 1024 * 1024);
        grpc.ready().await.map_err(|e| anyhow!("grpc not ready: {}", e))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static("/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead");
        grpc.ready().await.map_err(|e| anyhow!("grpc not ready: {}", e))?;
        let response_stream = grpc.streaming(req, path, ReorderCodec).await
            .map_err(|e| anyhow!("MigrationStreamingRead failed: {}", e))?.into_inner();
        tracing::info!("[PQV1-DIAG] MigrationStreamingRead stream opened");

        // Per-partition queues
        let mut pqs: HashMap<i64, mpsc::UnboundedSender<DecodedMessage>> = HashMap::new();
        let mut prs: HashMap<i64, mpsc::UnboundedReceiver<DecodedMessage>> = HashMap::new();
        for &pg in partition_group_ids {
            let pid = group_to_partition(pg);
            let (tx, rx) = mpsc::unbounded_channel();
            pqs.insert(pid, tx); prs.insert(pid, rx);
        }

        let inner = Arc::new(PqV1ClientInner { request_tx: request_tx.clone(), partition_queues: Mutex::new(pqs), session_id: String::new() });
        let inner_bg = inner.clone();
        let tx_bg = request_tx.clone();

        tokio::spawn(async move {
            let mut stream = response_stream;
            let mut session = String::new();
            let mut init_done = false;
            let start = std::time::Instant::now();
            loop {
                if !init_done && start.elapsed() > std::time::Duration::from_secs(30) { tracing::error!("PQv1 InitResponse timeout"); break; }
                let msg = match stream.message().await {
                    Ok(Some(m)) => m, Ok(None) => { tracing::warn!("PQv1 stream closed"); break; }
                    Err(e) => { tracing::error!("PQv1 stream error: {}", e); break; }
                };
                // SUCCESS is 400000, not 0 (0 == UNSPECIFIED, sent on streaming data msgs).
                // Only a real error code (not SUCCESS, not UNSPECIFIED) aborts the stream.
                if msg.status != 0 && msg.status != YDB_STATUS_SUCCESS {
                    tracing::error!("PQv1 status: {}, issues: {:?}", msg.status, msg.issues); break;
                }
                match msg.response {
                    Some(migration_streaming_read_server_message::Response::InitResponse(r)) => {
                        session = r.session_id.clone(); init_done = true;
                        tracing::info!("PQv1 session: {}", session);
                        let _ = tx_bg.send(MigrationStreamingReadClientMessage {
                            request: Some(migration_streaming_read_client_message::Request::Read(migration_streaming_read_client_message::Read {})), token: vec![],
                        });
                    }
                    Some(migration_streaming_read_server_message::Response::Assigned(a)) => {
                        if a.partition != 0 { tracing::info!("PQv1 skip partition={}", a.partition); continue; }
                        tracing::info!("PQv1 lock partition={} read_offset={} end_offset={}", a.partition, a.read_offset, a.end_offset);
                        let _ = tx_bg.send(MigrationStreamingReadClientMessage {
                            request: Some(migration_streaming_read_client_message::Request::StartRead(
                                migration_streaming_read_client_message::StartRead {
                                    topic: a.topic, cluster: a.cluster, partition: a.partition,
                                    assign_id: a.assign_id, read_offset: a.read_offset,
                                    commit_offset: a.read_offset, verify_read_offset: true,
                                })), token: vec![],
                        });
                    }
                    Some(migration_streaming_read_server_message::Response::DataBatch(db)) => {
                        let queues = inner_bg.partition_queues.lock().await;
                        for pd in &db.partition_data {
                            let pid = group_to_partition(pd.partition as i64);
                            let cookie = pd.cookie.clone();
                            for batch in &pd.batches {
                                for md in &batch.message_data {
                                    let data = match decompress(&md.data, md.codec, md.uncompressed_size) { Ok(d) => d, Err(e) => { tracing::error!("Decompress: {}", e); continue; } };
                                    if let Some(tx) = queues.get(&pid) {
                                        let _ = tx.send(DecodedMessage { offset: md.offset, seq_no: md.seq_no, create_timestamp_ms: md.create_timestamp_ms, data, partition_id: pid, cookie: cookie.clone() });
                                    }
                                }
                            }
                        }
                        let _ = tx_bg.send(MigrationStreamingReadClientMessage {
                            request: Some(migration_streaming_read_client_message::Request::Read(migration_streaming_read_client_message::Read {})), token: vec![],
                        });
                    }
                    _ => {}
                }
            }
            tracing::info!("PQv1 background task exited (session={})", session);
        });

        Ok((Self { inner }, prs))
    }

    pub async fn describe_topic(_endpoint: &str, _database: &str, _topic_path: &str, _token: &str) -> anyhow::Result<i32> {
        anyhow::bail!("describe_topic: use partition_ids in config")
    }

    pub async fn commit(&self, _partition_id: i64, cookie: CommitCookie) -> anyhow::Result<()> {
        let _ = self.inner.request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(migration_streaming_read_client_message::Request::Commit(
                migration_streaming_read_client_message::Commit { cookies: vec![cookie], offset_ranges: vec![] }
            )), token: vec![],
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Decompression
// ---------------------------------------------------------------------------

fn decompress(data: &[u8], codec: i32, _uncompressed_size: u64) -> anyhow::Result<Bytes> {
    match codec {
        1 => Ok(Bytes::copy_from_slice(data)),
        2 => { use std::io::Read; let mut d = flate2::read::GzDecoder::new(data); let mut buf = Vec::new(); d.read_to_end(&mut buf)?; Ok(Bytes::from(buf)) }
        4 => Ok(Bytes::from(zstd::decode_all(data)?)),
        _ => Err(anyhow!("Unsupported codec: {}", codec)),
    }
}

// ---------------------------------------------------------------------------
// PqV1Source
// ---------------------------------------------------------------------------

pub struct PqV1Source { client: PqV1Client, rx: mpsc::UnboundedReceiver<DecodedMessage>, partition_id: i64 }

impl PqV1Source {
    pub fn new(client: PqV1Client, rx: mpsc::UnboundedReceiver<DecodedMessage>, partition_id: i64) -> Self { Self { client, rx, partition_id } }
}

impl Source for PqV1Source {
    async fn read_batch(&mut self) -> anyhow::Result<MessageBatch> {
        let mut messages = Vec::new();
        let mut last_cookie: Option<CommitCookie> = None;
        match self.rx.recv().await {
            Some(msg) => { last_cookie = msg.cookie.clone(); messages.push(Message { value: msg.data }); }
            None => return Ok(MessageBatch { messages: vec![], partition_id: self.partition_id, commit_marker: None }),
        }
        while let Ok(msg) = self.rx.try_recv() { last_cookie = msg.cookie.clone(); messages.push(Message { value: msg.data }); }
        let commit_marker = last_cookie.map(|c| CommitMarker::new(PqV1CommitMarker { partition_id: self.partition_id, cookie: c }));
        Ok(MessageBatch { messages, partition_id: self.partition_id, commit_marker })
    }

    async fn commit_offsets(&mut self, marker: &CommitMarker) -> anyhow::Result<()> {
        let m = marker.downcast_ref::<PqV1CommitMarker>().ok_or_else(|| anyhow!("Invalid commit marker"))?;
        self.client.commit(m.partition_id, m.cookie.clone()).await
    }
}
