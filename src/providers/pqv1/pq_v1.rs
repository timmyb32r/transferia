//! `PQv1` (`PersQueue` V1) gRPC client for Logbroker.
//!
//! Flow: `ListEndpoints` (discover proxy) → `MigrationStreamingRead` bidi stream on the
//! proxy → `InitResponse` → Assigned → `StartRead` → `DataBatch`. Transport is HTTP/2 with
//! prior knowledge (Go-compatible), bridged into tonic via a small `tower::Service`.

use alloc::sync::Arc;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex as StdMutex;

use anyhow::anyhow;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::Stream;

use crate::metrics::SourceCounters;
use crate::pipeline::memory::{MemoryReservation, PipelineMemory};
use crate::pipeline::PipelineFailure;
use http::Uri;
use hyper::client::conn::http2;
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::Request;

/// YDB cluster database used for discovery/routing metadata (`x-ydb-database`).
/// Always `/Root` in our deployment — hardcoded rather than configured.
const YDB_DATABASE: &str = "/Root";
use crate::pipeline::source::{CommitMarker, Source};
use crate::types::message::SourcePartition;
use crate::types::message::{Message, MessageBatch, MessageMeta};
use crate::Ydb::pers_queue::v1::{
    migration_streaming_read_client_message::{self, InitRequest, TopicReadSettings},
    migration_streaming_read_server_message, CommitCookie, MigrationStreamingReadClientMessage,
    MigrationStreamingReadServerMessage, ReadParams,
};
use crate::Ydb::status_ids::StatusCode;

/// `Ydb.StatusIds.SUCCESS`. Status codes live in the reserved range [400000, 400999];
/// SUCCESS is 400000 (NOT 0 — 0 is `STATUS_CODE_UNSPECIFIED`, sent on streaming data msgs).
const YDB_STATUS_SUCCESS: i32 = 400_000;
/// `Ydb.StatusIds.STATUS_CODE_UNSPECIFIED`. Streaming data messages carry it on every
/// batch; only real error codes abort the stream.
const YDB_STATUS_UNSPECIFIED: i32 = 0;

/// tonic's default decode cap is 4 MiB; Logbroker `DataBatch` messages can exceed it.
const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;
/// A corrupt or malicious declared size must not turn a small compressed message into an
/// unbounded allocation. This deliberately matches the transport cap; normal reads are much
/// smaller (`ReadParams.max_read_size` is 1 MiB).
const MAX_DECOMPRESSED_MESSAGE_SIZE: usize = MAX_MESSAGE_SIZE;
/// Bound the sum as well as each individual message: `PipelineMemory` deliberately admits one
/// oversized reservation, so it is not a substitute for a decompression safety limit.
const MAX_DECOMPRESSED_BATCH_SIZE: usize = MAX_MESSAGE_SIZE;
const MAX_ZSTD_WINDOW_LOG: u32 = 27; // log2(128 MiB)
const SESSION_INIT_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(30);
const COMMIT_ACK_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(30);
const RELEASE_HANDOFF_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(1);
const RELEASE_TRANSPORT_GRACE: core::time::Duration = core::time::Duration::from_millis(100);
/// Capacity of the decompressed-batch channel (bg task → merge task). Bounded
/// so that if decompress ever falls behind download, memory is capped; with
/// parallel decompress keeping up, it stays near-empty.
const DECODED_CHANNEL_CAP: usize = 128;
const PARTITION_CHANNEL_CAP: usize = 1024;
const MAX_PARTS_PER_SOURCE_BATCH: usize = 128;

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
async fn connect_http2_prior_knowledge(
    uri: &Uri,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<H2Service> {
    let host = uri.host().unwrap_or("localhost");
    let port = uri.port_u16().unwrap_or(2135);
    let addr = format!("{host}:{port}");

    let (send_request, conn) = network_stage("HTTP/2 connection", timeout, cancellation, async {
        let stream = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(|e| anyhow!("TCP connect to {addr}: {e}"))?;
        stream.set_nodelay(true)?;
        let io = hyper_util::rt::TokioIo::new(stream);
        http2::handshake(hyper_util::rt::TokioExecutor::new(), io)
            .await
            .map_err(|e| anyhow!("HTTP/2 handshake failed: {e}"))
    })
    .await?;
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

async fn network_stage<T>(
    name: &str,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
    operation: impl core::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("{name} cancelled"),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| anyhow!("{name} timed out after {} ms", timeout.as_millis()))?
        }
    }
}

/// Attach the YDB auth/routing headers that Logbroker expects on every call.
fn auth_metadata_value(token: &str) -> anyhow::Result<AsciiMetadataValue> {
    anyhow::ensure!(!token.is_empty(), "PQv1 access token must not be empty");
    AsciiMetadataValue::try_from(token)
        .map_err(|_| anyhow!("PQv1 access token is not valid ASCII metadata"))
}

fn set_ydb_headers(md: &mut MetadataMap, token: &str) -> anyhow::Result<()> {
    md.insert("x-ydb-auth-ticket", auth_metadata_value(token)?);
    md.insert(
        "x-ydb-database",
        AsciiMetadataValue::from_static(YDB_DATABASE),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a plaintext `PQv1` connection string into its authority. This client currently
/// targets the fixed [`YDB_DATABASE`], so accepting a database path would be misleading.
pub fn parse_endpoint(conn_str: &str) -> anyhow::Result<String> {
    let uri: Uri = conn_str
        .parse()
        .map_err(|e| anyhow!("Invalid connection string '{conn_str}': {e}"))?;
    let scheme = uri.scheme_str().unwrap_or("grpc").to_string();
    anyhow::ensure!(
        scheme == "grpc",
        "PQv1 scheme '{scheme}' is not supported: the custom transport requires grpc:// and uses a raw HTTP/2 TCP stream without TLS"
    );
    anyhow::ensure!(
        (uri.path().is_empty() || uri.path() == "/") && uri.query().is_none(),
        "PQv1 connection string must not contain a database path or query; the database is fixed to {YDB_DATABASE}"
    );
    let host = uri
        .authority()
        .map_or("localhost:2135", |a| a.as_str())
        .to_string();
    Ok(host)
}

fn http_uri(host: &str) -> anyhow::Result<Uri> {
    format!("http://{host}")
        .parse()
        .map_err(|e| anyhow!("bad uri http://{host}: {e}"))
}

fn init_request(topic_path: &str, consumer: &str, partition_group_ids: &[i64]) -> InitRequest {
    InitRequest {
        topics_read_settings: vec![TopicReadSettings {
            topic: topic_path.to_string(),
            partition_group_ids: partition_group_ids.to_vec(),
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
    }
}

fn start_read_request(
    assigned: migration_streaming_read_server_message::Assigned,
) -> MigrationStreamingReadClientMessage {
    MigrationStreamingReadClientMessage {
        request: Some(migration_streaming_read_client_message::Request::StartRead(
            migration_streaming_read_client_message::StartRead {
                topic: assigned.topic,
                cluster: assigned.cluster,
                partition: assigned.partition,
                assign_id: assigned.assign_id,
                read_offset: assigned.read_offset,
                commit_offset: assigned.read_offset,
                verify_read_offset: true,
            },
        )),
        token: Vec::new(),
    }
}

fn released_request(
    release: migration_streaming_read_server_message::Release,
) -> MigrationStreamingReadClientMessage {
    MigrationStreamingReadClientMessage {
        request: Some(migration_streaming_read_client_message::Request::Released(
            migration_streaming_read_client_message::Released {
                topic: release.topic,
                cluster: release.cluster,
                partition: release.partition,
                assign_id: release.assign_id,
            },
        )),
        token: Vec::new(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveAssignment {
    topic: String,
    cluster: String,
    assign_id: u64,
}

fn required_topic_path<'a>(
    event: &str,
    partition: i64,
    topic: Option<&'a crate::Ydb::pers_queue::v1::Path>,
) -> anyhow::Result<&'a str> {
    topic
        .map(|topic| topic.path.as_str())
        .ok_or_else(|| anyhow!("PQv1 {event} has no topic for partition {partition}"))
}

fn validate_event_identity(
    event: &str,
    partition: i64,
    topic: Option<&crate::Ydb::pers_queue::v1::Path>,
    cluster: &str,
    active: &ActiveAssignment,
) -> anyhow::Result<()> {
    let topic = required_topic_path(event, partition, topic)?;
    anyhow::ensure!(
        topic == active.topic,
        "PQv1 {event} topic mismatch for partition {partition}: active={:?}, received={topic:?}",
        active.topic
    );
    anyhow::ensure!(
        cluster == active.cluster,
        "PQv1 {event} cluster mismatch for partition {partition}: active={:?}, received={cluster:?}",
        active.cluster
    );
    Ok(())
}

fn register_assignment(
    active_assignments: &mut HashMap<i64, ActiveAssignment>,
    requested_partitions: &HashSet<i64>,
    configured_topic: &str,
    assigned: &migration_streaming_read_server_message::Assigned,
) -> Result<i64, SessionFailure> {
    let result = (|| {
        let pid = i64::try_from(assigned.partition).map_err(|_| {
            anyhow!(
                "PQv1 assignment partition id {} does not fit in i64",
                assigned.partition
            )
        })?;
        anyhow::ensure!(
            requested_partitions.contains(&pid),
            "PQv1 assigned unrequested partition {pid}"
        );
        anyhow::ensure!(
            !active_assignments.contains_key(&pid),
            "PQv1 reassigned active partition {pid}: active assign_id={}, new assign_id={}",
            active_assignments
                .get(&pid)
                .map_or(0, |active| active.assign_id),
            assigned.assign_id
        );
        let topic = required_topic_path("assignment", pid, assigned.topic.as_ref())?;
        anyhow::ensure!(
            topic == configured_topic,
            "PQv1 assignment topic mismatch for partition {pid}: configured={configured_topic:?}, assigned={topic:?}"
        );
        Ok((
            pid,
            ActiveAssignment {
                topic: topic.to_string(),
                cluster: assigned.cluster.clone(),
                assign_id: assigned.assign_id,
            },
        ))
    })()
    .map_err(SessionFailure::fatal)?;

    active_assignments.insert(result.0, result.1);
    Ok(result.0)
}

fn validate_release_assignment(
    active_assignments: &mut HashMap<i64, ActiveAssignment>,
    release: &migration_streaming_read_server_message::Release,
) -> Result<i64, SessionFailure> {
    let pid = (|| {
        let pid = i64::try_from(release.partition).map_err(|_| {
            anyhow!(
                "PQv1 release partition id {} does not fit in i64",
                release.partition
            )
        })?;
        let active = active_assignments
            .get(&pid)
            .ok_or_else(|| anyhow!("PQv1 released inactive partition {pid}"))?;
        validate_event_identity(
            "release",
            pid,
            release.topic.as_ref(),
            &release.cluster,
            active,
        )?;
        anyhow::ensure!(
            active.assign_id == release.assign_id,
            "PQv1 release assign_id mismatch for partition {pid}: active={}, released={}",
            active.assign_id,
            release.assign_id
        );
        Ok(pid)
    })()
    .map_err(SessionFailure::fatal)?;

    active_assignments.remove(&pid);
    Ok(pid)
}

fn validate_data_partition(
    partition: &migration_streaming_read_server_message::data_batch::PartitionData,
    active_assignments: &HashMap<i64, ActiveAssignment>,
) -> Result<(i64, CommitCookie), SessionFailure> {
    (|| {
        let pid = i64::try_from(partition.partition).map_err(|_| {
            anyhow!(
                "PQv1 data partition id {} does not fit in i64",
                partition.partition
            )
        })?;
        let active = active_assignments
            .get(&pid)
            .ok_or_else(|| anyhow!("PQv1 returned data for inactive partition {pid}"))?;
        validate_event_identity(
            "data",
            pid,
            partition.topic.as_ref(),
            &partition.cluster,
            active,
        )?;
        let cookie = partition.cookie.ok_or_else(|| {
            anyhow!("PQv1 returned data without a commit cookie for partition {pid}")
        })?;
        anyhow::ensure!(
            cookie.assign_id == active.assign_id,
            "PQv1 data cookie assign_id mismatch for partition {pid}: active={}, received={}",
            active.assign_id,
            cookie.assign_id
        );
        Ok((pid, cookie))
    })()
    .map_err(SessionFailure::fatal)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One decompressed message within a partition part.
pub struct DecodedMessage {
    pub data: Bytes,
    /// Stable source offset within the `PQv1` partition.
    pub offset: u64,
    pub write_timestamp_ms: u64,
}

pub struct PqV1CommitMarker {
    pub partition_id: i64,
    pub cookies: Vec<CommitCookie>,
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
pub struct DecodedPart {
    pid: i64,
    cookie: Option<CommitCookie>,
    msgs: Vec<DecodedMessage>,
    memory: MemoryReservation,
}

pub enum PartitionEvent {
    Data(DecodedPart),
}

/// A decompressed `DataBatch`, re-ordered by `seq` before dispatch.
struct DecodedBatch {
    seq: u64,
    parts: anyhow::Result<Vec<DecodedPart>>,
    failure_kind: TerminalFailureKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalFailureKind {
    Retryable,
    Fatal,
}

#[derive(Debug)]
struct SessionFailure {
    error: anyhow::Error,
    kind: TerminalFailureKind,
}

impl SessionFailure {
    const fn retryable(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: TerminalFailureKind::Retryable,
        }
    }

    const fn fatal(error: anyhow::Error) -> Self {
        Self {
            error,
            kind: TerminalFailureKind::Fatal,
        }
    }
}

fn release_failure(pid: i64, assign_id: u64, forceful: bool) -> SessionFailure {
    let mode = if forceful { "forcefully" } else { "gracefully" };
    SessionFailure::retryable(anyhow!(
        "PQv1 {mode} released partition {pid} assign_id={assign_id}; restarting session"
    ))
}

#[derive(Clone)]
struct TerminalFailure {
    message: Arc<str>,
    kind: TerminalFailureKind,
}

struct RequestStream {
    rx: mpsc::UnboundedReceiver<MigrationStreamingReadClientMessage>,
    release_handed_off: Arc<Notify>,
}

impl Stream for RequestStream {
    type Item = MigrationStreamingReadClientMessage;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.rx.poll_recv(cx);
        if matches!(
            &item,
            Poll::Ready(Some(MigrationStreamingReadClientMessage {
                request: Some(migration_streaming_read_client_message::Request::Released(
                    _
                )),
                ..
            }))
        ) {
            self.release_handed_off.notify_one();
        }
        item
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

fn status_failure_kind(status: i32) -> TerminalFailureKind {
    use StatusCode::{
        Aborted, BadRequest, BadSession, Cancelled, ExternalError, GenericError, InternalError,
        Overloaded, SessionBusy, SessionExpired, Timeout, Unauthorized, Unavailable, Undetermined,
    };

    match StatusCode::try_from(status) {
        Ok(
            InternalError | Aborted | Unavailable | Overloaded | GenericError | Timeout
            | BadSession | SessionExpired | Cancelled | Undetermined | SessionBusy | ExternalError,
        ) => TerminalFailureKind::Retryable,
        Ok(
            StatusCode::Unspecified
            | StatusCode::Success
            | BadRequest
            | Unauthorized
            | StatusCode::SchemeError
            | StatusCode::PreconditionFailed
            | StatusCode::AlreadyExists
            | StatusCode::NotFound
            | StatusCode::Unsupported,
        )
        | Err(_) => TerminalFailureKind::Fatal,
    }
}

fn validate_server_message(
    message: &MigrationStreamingReadServerMessage,
) -> Result<(), SessionFailure> {
    if message.status != YDB_STATUS_UNSPECIFIED && message.status != YDB_STATUS_SUCCESS {
        let status_name =
            StatusCode::try_from(message.status).map_or("UNKNOWN", |status| status.as_str_name());
        return Err(SessionFailure {
            error: anyhow!(
                "PQv1 status: {} ({status_name}), issues: {:?}",
                message.status,
                message.issues
            ),
            kind: status_failure_kind(message.status),
        });
    }
    if !message.issues.is_empty() {
        return Err(SessionFailure::fatal(anyhow!(
            "PQv1 returned issues with a successful status: {:?}",
            message.issues
        )));
    }
    if message.response.is_none() {
        return Err(SessionFailure::fatal(anyhow!(
            "PQv1 protocol violation: server message is missing response"
        )));
    }
    Ok(())
}

fn record_init_response(init_done: &mut bool) -> Result<(), SessionFailure> {
    if *init_done {
        return Err(SessionFailure::fatal(anyhow!(
            "PQv1 protocol violation: duplicate InitResponse"
        )));
    }
    *init_done = true;
    Ok(())
}

fn validate_response_phase(
    init_done: &mut bool,
    response: &migration_streaming_read_server_message::Response,
) -> Result<(), SessionFailure> {
    match response {
        migration_streaming_read_server_message::Response::InitResponse(_) => {
            record_init_response(init_done)
        }
        _ if !*init_done => Err(SessionFailure::fatal(anyhow!(
            "PQv1 protocol violation: non-init response received before InitResponse"
        ))),
        migration_streaming_read_server_message::Response::PartitionStatus(_) => {
            Err(SessionFailure::fatal(anyhow!(
                "PQv1 protocol violation: unsolicited PartitionStatus"
            )))
        }
        _ => Ok(()),
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
    partition_queues: Mutex<HashMap<i64, mpsc::Sender<PartitionEvent>>>,
    pending_commit_cookies: StdMutex<PendingCommitQueues>,
    terminal_failure: watch::Sender<Option<Arc<TerminalFailure>>>,
    session_token: CancellationToken,
}

struct PendingCommit {
    remaining: AtomicUsize,
    completed: Notify,
}

type CommitCookieKey = (u64, u64);
type PendingCommitQueues = HashMap<CommitCookieKey, VecDeque<Arc<PendingCommit>>>;

impl PendingCommit {
    fn new(cookie_count: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(cookie_count),
            completed: Notify::new(),
        }
    }

    async fn wait(&self) {
        loop {
            let completed = self.completed.notified();
            if self.remaining.load(Ordering::Acquire) == 0 {
                return;
            }
            completed.await;
        }
    }
}

fn acknowledge_committed(
    inner: &PqV1ClientInner,
    committed: &migration_streaming_read_server_message::Committed,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        committed.offset_ranges.is_empty(),
        "PQv1 acknowledged offset ranges that this client never committed"
    );
    anyhow::ensure!(
        !committed.cookies.is_empty(),
        "PQv1 returned an empty Committed response"
    );

    let keys: Vec<_> = committed
        .cookies
        .iter()
        .map(|cookie| (cookie.assign_id, cookie.partition_cookie))
        .collect();
    let mut acknowledged_counts = HashMap::new();
    for key in &keys {
        *acknowledged_counts.entry(*key).or_insert(0_usize) += 1;
    }

    let mut pending = inner
        .pending_commit_cookies
        .lock()
        .map_err(|_| anyhow!("PQv1 pending commit state is poisoned"))?;
    for (key, count) in &acknowledged_counts {
        anyhow::ensure!(
            pending.get(key).is_some_and(|queue| queue.len() >= *count),
            "PQv1 acknowledged unknown commit cookie assign_id={} partition_cookie={}",
            key.0,
            key.1
        );
    }
    let mut completed = Vec::with_capacity(keys.len());
    for key in &keys {
        let queue = pending
            .get_mut(key)
            .ok_or_else(|| anyhow!("PQv1 pending commit state disappeared"))?;
        let waiter = queue
            .pop_front()
            .ok_or_else(|| anyhow!("PQv1 pending commit queue is empty"))?;
        let remove_key = queue.is_empty();
        if remove_key {
            pending.remove(key);
        }
        completed.push(waiter);
    }
    drop(pending);
    for waiter in completed {
        let previous = waiter.remaining.fetch_sub(1, Ordering::AcqRel);
        anyhow::ensure!(previous > 0, "PQv1 commit acknowledgement underflow");
        if previous == 1 {
            waiter.completed.notify_waiters();
        }
    }
    Ok(())
}

fn remove_pending_commit(
    inner: &PqV1ClientInner,
    keys: &[CommitCookieKey],
    waiter: &Arc<PendingCommit>,
) -> anyhow::Result<()> {
    let mut pending = inner
        .pending_commit_cookies
        .lock()
        .map_err(|_| anyhow!("PQv1 pending commit state is poisoned"))?;
    for key in keys {
        let remove_key = pending.get_mut(key).is_some_and(|queue| {
            queue.retain(|candidate| !Arc::ptr_eq(candidate, waiter));
            queue.is_empty()
        });
        if remove_key {
            pending.remove(key);
        }
    }
    drop(pending);
    Ok(())
}

fn broadcast_failure(inner: &PqV1ClientInner, error: &anyhow::Error, kind: TerminalFailureKind) {
    let replacement = Arc::new(TerminalFailure {
        message: Arc::from(error.to_string()),
        kind,
    });
    inner.terminal_failure.send_if_modified(|current| {
        if current
            .as_ref()
            .is_some_and(|failure| failure.kind == TerminalFailureKind::Fatal)
        {
            return false;
        }
        *current = Some(replacement);
        true
    });
    inner.session_token.cancel();
}

fn spawn_session_task<F>(inner: Arc<PqV1ClientInner>, task_name: &'static str, task: F)
where
    F: core::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let result = tokio::spawn(task).await;
        if inner.session_token.is_cancelled() {
            return;
        }
        let error = match result {
            Ok(()) => anyhow!("PQv1 {task_name} task exited unexpectedly"),
            Err(error) => anyhow!("PQv1 {task_name} task failed: {error}"),
        };
        tracing::error!("{error}");
        broadcast_failure(&inner, &error, TerminalFailureKind::Retryable);
    });
}

async fn reserve_decoded_slot(
    sender: &mpsc::Sender<DecodedBatch>,
    cancellation: &CancellationToken,
) -> anyhow::Result<mpsc::OwnedPermit<DecodedBatch>> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("PQv1 session cancelled"),
        permit = sender.clone().reserve_owned() => {
            permit.map_err(|_| anyhow!("PQv1 decoded-batch queue closed"))
        }
    }
}

fn surface_terminal_failure(failure: &TerminalFailure) -> anyhow::Result<MessageBatch> {
    let error = anyhow!(failure.message.to_string());
    match failure.kind {
        TerminalFailureKind::Retryable => Err(error),
        TerminalFailureKind::Fatal => Err(PipelineFailure::fatal(error).into()),
    }
}

fn commit_session_stopped_error(inner: &PqV1ClientInner, partition_id: i64) -> anyhow::Error {
    let terminal_failure = inner.terminal_failure.borrow().clone();
    if let Some(failure) =
        terminal_failure.filter(|failure| failure.kind == TerminalFailureKind::Fatal)
    {
        let error = anyhow!(
            "PQv1 fatal session failure while acknowledging commit for partition {partition_id}: {}",
            failure.message
        );
        return PipelineFailure::fatal(error).into();
    }
    anyhow!("PQv1 session stopped before acknowledging commit for partition {partition_id}")
}

/// Discover a proxy endpoint via `ListEndpoints` over HTTP/2 prior knowledge.
/// The gRPC response type is `GetOperationResponse` (matching Go's `conn.Invoke`).
async fn discover_proxy(
    main_uri: &Uri,
    token: &str,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<String> {
    use crate::Ydb::discovery::{ListEndpointsRequest, ListEndpointsResult};
    use crate::Ydb::operations::GetOperationResponse;
    use prost::Message as _;

    let h2 = connect_http2_prior_knowledge(main_uri, timeout, cancellation).await?;
    let mut grpc = tonic::client::Grpc::<H2Service>::with_origin(h2, main_uri.clone());

    let mut req = Request::new(ListEndpointsRequest {
        database: YDB_DATABASE.to_string(),
        service: vec![],
    });
    set_ydb_headers(req.metadata_mut(), token)?;

    let path =
        http::uri::PathAndQuery::from_static("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints");
    let resp: GetOperationResponse =
        network_stage("PQv1 proxy discovery", timeout, cancellation, async {
            grpc.ready()
                .await
                .map_err(|e| anyhow!("ListEndpoints ready: {e}"))?;
            grpc.unary(
                req,
                path,
                tonic_prost::ProstCodec::<ListEndpointsRequest, GetOperationResponse>::default(),
            )
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| anyhow!("ListEndpoints failed: {e}"))
        })
        .await?;

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
    pub async fn resolve_proxy(
        endpoint: &str,
        token: &str,
        network_timeout: core::time::Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<String> {
        auth_metadata_value(token)?;
        let main_host = parse_endpoint(endpoint)?;
        let main_uri = http_uri(&main_host)?;
        match discover_proxy(&main_uri, token, network_timeout, cancellation).await {
            Ok(proxy) => Ok(proxy),
            Err(error) => {
                tracing::warn!("Proxy discovery failed: {error}. Using main endpoint.");
                Ok(main_host)
            }
        }
    }

    pub async fn connect(
        proxy: &str,
        topic_path: &str,
        consumer: &str,
        token: &str,
        partition_group_ids: &[i64],
        source_counters: Arc<SourceCounters>,
        cancel_token: CancellationToken,
        benchmark_discard_before_decompression: bool,
        memory: PipelineMemory,
        network_timeout: core::time::Duration,
        decompress_slots: Arc<tokio::sync::Semaphore>,
    ) -> anyhow::Result<(Self, HashMap<i64, mpsc::Receiver<PartitionEvent>>)> {
        auth_metadata_value(token)?;
        let target_uri = http_uri(proxy)?;
        tracing::info!(
            "PQv1 connecting: proxy={} topic={} consumer={}",
            proxy,
            topic_path,
            consumer
        );

        // Step 2: open the bidi stream on the proxy.
        let h2_service =
            connect_http2_prior_knowledge(&target_uri, network_timeout, &cancel_token).await?;

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        request_tx.send(MigrationStreamingReadClientMessage {
            request: Some(
                migration_streaming_read_client_message::Request::InitRequest(init_request(
                    topic_path,
                    consumer,
                    partition_group_ids,
                )),
            ),
            token: token.as_bytes().to_vec(),
        })?;

        let release_handed_off = Arc::new(Notify::new());
        let mut req = Request::new(RequestStream {
            rx: request_rx,
            release_handed_off: Arc::clone(&release_handed_off),
        });
        set_ydb_headers(req.metadata_mut(), token)?;

        let mut grpc = tonic::client::Grpc::with_origin(h2_service, target_uri)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        let path = http::uri::PathAndQuery::from_static(
            "/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead",
        );
        let codec = tonic_prost::ProstCodec::<
            MigrationStreamingReadClientMessage,
            MigrationStreamingReadServerMessage,
        >::default();
        let response_stream = network_stage(
            "PQv1 migration stream open",
            network_timeout,
            &cancel_token,
            async {
                grpc.ready()
                    .await
                    .map_err(|e| anyhow!("grpc not ready: {e}"))?;
                grpc.streaming(req, path, codec)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|e| anyhow!("MigrationStreamingRead failed: {e}"))
            },
        )
        .await?;

        // Per-partition queues for the partitions we own.
        let assigned: HashSet<i64> = partition_group_ids.iter().copied().collect();
        let configured_topic: Arc<str> = Arc::from(topic_path);
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

        let session_token = cancel_token.child_token();
        let (terminal_failure, _terminal_receiver) = watch::channel(None);
        let inner = Arc::new(PqV1ClientInner {
            request_tx: request_tx.clone(),
            partition_queues: Mutex::new(pqs),
            pending_commit_cookies: StdMutex::new(HashMap::new()),
            terminal_failure,
            session_token: session_token.clone(),
        });

        // Pipelined decompress: the read loop hands each DataBatch to a blocking
        // task for decompression and immediately sends the next Read, so
        // `decompress()` never blocks the download. A merge task re-orders the
        // decompressed batches by `seq` to preserve server order and make
        // contiguous commit-cookie acknowledgement possible.
        let (decoded_tx, decoded_rx) = mpsc::channel::<DecodedBatch>(DECODED_CHANNEL_CAP);
        let merge_inner = Arc::clone(&inner);
        let merge_token = session_token.clone();
        spawn_session_task(Arc::clone(&inner), "merge", async move {
            let mut next_seq: u64 = 0;
            let mut buffer: HashMap<u64, DecodedBatch> = HashMap::new();
            let mut rx = decoded_rx;
            loop {
                let batch = tokio::select! {
                    () = merge_token.cancelled() => break,
                    batch = rx.recv() => batch,
                };
                let Some(batch) = batch else { break };
                buffer.insert(batch.seq, batch);
                while let Some(b) = buffer.remove(&next_seq) {
                    let parts = match b.parts {
                        Ok(parts) => parts,
                        Err(error) => {
                            tracing::error!("PQv1 decompression failed: {error}");
                            broadcast_failure(&merge_inner, &error, b.failure_kind);
                            return;
                        }
                    };
                    for part in parts {
                        let pid = part.pid;
                        let tx = merge_inner.partition_queues.lock().await.get(&pid).cloned();
                        let Some(tx) = tx else {
                            continue;
                        };
                        if tx.send(PartitionEvent::Data(part)).await.is_err() {
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

        let stream_inner = Arc::clone(&inner);
        let stream_token = session_token;
        spawn_session_task(Arc::clone(&inner), "response stream", async move {
            let mut stream = response_stream;
            let mut init_done = false;
            let init_deadline = tokio::time::Instant::now() + SESSION_INIT_TIMEOUT;
            let mut seq_counter: u64 = 0;
            let mut terminal_error = None;
            let mut active_assignments: HashMap<i64, ActiveAssignment> = HashMap::new();
            'stream: loop {
                let await_start = std::time::Instant::now();
                let msg = tokio::select! {
                    m = stream.message() => match m {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 stream closed unexpectedly"
                            )));
                            break;
                        }
                        Err(error) => {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 stream error: {error}"
                            )));
                            break;
                        }
                    },
                    // Ctrl+C / shutdown — stop reading promptly instead of
                    // waiting for the next server message (which could be never
                    // if the topic is idle).
                    () = stream_token.cancelled() => {
                        tracing::info!("PQv1 background task cancelled (shutdown)");
                        break;
                    }
                    () = tokio::time::sleep_until(init_deadline), if !init_done => {
                        terminal_error = Some(SessionFailure::retryable(anyhow!(
                            "PQv1 InitResponse timeout"
                        )));
                        break;
                    }
                };
                // Downloader busy = time a Read request is in-flight (awaiting
                // the next server message). idle = the processing below.
                source_counters.add_download_busy(await_start.elapsed());
                if let Err(failure) = validate_server_message(&msg) {
                    terminal_error = Some(failure);
                    break;
                }
                let Some(response) = msg.response.as_ref() else {
                    terminal_error = Some(SessionFailure::fatal(anyhow!(
                        "PQv1 protocol violation: server message is missing response"
                    )));
                    break;
                };
                if let Err(failure) = validate_response_phase(&mut init_done, response) {
                    terminal_error = Some(failure);
                    break;
                }
                match msg.response {
                    Some(migration_streaming_read_server_message::Response::InitResponse(r)) => {
                        tracing::info!("PQv1 session: {}", r.session_id);
                        if request_tx.send(read_request()).is_err() {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 request channel closed"
                            )));
                            break;
                        }
                    }
                    Some(migration_streaming_read_server_message::Response::Assigned(a)) => {
                        let pid = match register_assignment(
                            &mut active_assignments,
                            &assigned,
                            configured_topic.as_ref(),
                            &a,
                        ) {
                            Ok(pid) => pid,
                            Err(failure) => {
                                terminal_error = Some(failure);
                                break;
                            }
                        };
                        tracing::debug!(
                            "PQv1 lock partition={} read_offset={} end_offset={}",
                            pid,
                            a.read_offset,
                            a.end_offset
                        );
                        if request_tx.send(start_read_request(a)).is_err() {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 request channel closed"
                            )));
                            break;
                        }
                    }
                    Some(migration_streaming_read_server_message::Response::DataBatch(db)) => {
                        let seq = seq_counter;
                        if benchmark_discard_before_decompression {
                            // Bench: discard payload before decompression, but retain
                            // commit cookies so the benchmark consumer still advances.
                            let mut discarded = Vec::with_capacity(db.partition_data.len());
                            let mut compressed_bytes = 0_u64;
                            let mut message_count = 0_u64;
                            for pd in db.partition_data {
                                let (pid, cookie) =
                                    match validate_data_partition(&pd, &active_assignments) {
                                        Ok(validated) => validated,
                                        Err(failure) => {
                                            terminal_error = Some(failure);
                                            break;
                                        }
                                    };
                                for batch in pd.batches {
                                    for md in batch.message_data {
                                        compressed_bytes =
                                            compressed_bytes.saturating_add(md.data.len() as u64);
                                        message_count = message_count.saturating_add(1);
                                    }
                                }
                                let reservation = tokio::select! {
                                    reservation = memory.reserve(1) => reservation,
                                    () = stream_token.cancelled() => break 'stream,
                                };
                                discarded.push(DecodedPart {
                                    pid,
                                    cookie: Some(cookie),
                                    msgs: vec![],
                                    memory: reservation,
                                });
                            }
                            if terminal_error.is_some() {
                                break;
                            }
                            source_counters.add_compressed_bytes(compressed_bytes);
                            source_counters.add_messages(message_count);
                            let decoded_slot =
                                match reserve_decoded_slot(&decoded_tx, &stream_token).await {
                                    Ok(slot) => slot,
                                    Err(error) => {
                                        terminal_error = Some(SessionFailure::retryable(error));
                                        break;
                                    }
                                };
                            let batch = DecodedBatch {
                                seq,
                                parts: Ok(discarded),
                                failure_kind: TerminalFailureKind::Fatal,
                            };
                            decoded_slot.send(batch);
                            seq_counter += 1;
                            if request_tx.send(read_request()).is_err() {
                                terminal_error = Some(SessionFailure::retryable(anyhow!(
                                    "PQv1 request channel closed"
                                )));
                                break;
                            }
                            continue;
                        }
                        let mut parts: Vec<RawPart> = Vec::with_capacity(db.partition_data.len());
                        let mut compressed_bytes = 0_u64;
                        let mut message_count = 0_u64;
                        for pd in db.partition_data {
                            let (pid, cookie) =
                                match validate_data_partition(&pd, &active_assignments) {
                                    Ok(validated) => validated,
                                    Err(failure) => {
                                        terminal_error = Some(failure);
                                        break;
                                    }
                                };
                            let mut msgs = Vec::new();
                            for batch in pd.batches {
                                let write_timestamp_ms = batch.write_timestamp_ms;
                                for md in batch.message_data {
                                    compressed_bytes =
                                        compressed_bytes.saturating_add(md.data.len() as u64);
                                    message_count = message_count.saturating_add(1);
                                    msgs.push(RawMsg {
                                        data: md.data,
                                        codec: md.codec,
                                        uncompressed_size: md.uncompressed_size,
                                        offset: md.offset,
                                        write_timestamp_ms,
                                    });
                                }
                            }
                            parts.push(RawPart {
                                pid,
                                cookie: Some(cookie),
                                msgs,
                            });
                        }
                        if terminal_error.is_some() {
                            break;
                        }
                        source_counters.add_compressed_bytes(compressed_bytes);
                        source_counters.add_messages(message_count);
                        if parts.is_empty() {
                            if request_tx.send(read_request()).is_err() {
                                terminal_error = Some(SessionFailure::retryable(anyhow!(
                                    "PQv1 request channel closed"
                                )));
                                break;
                            }
                            continue;
                        }
                        let decoded_slot =
                            match reserve_decoded_slot(&decoded_tx, &stream_token).await {
                                Ok(slot) => slot,
                                Err(error) => {
                                    terminal_error = Some(SessionFailure::retryable(error));
                                    break;
                                }
                            };
                        let peak_bytes = match peak_decode_bytes(&parts) {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                terminal_error = Some(SessionFailure::fatal(error));
                                break;
                            }
                        };
                        let reservation = tokio::select! {
                            reservation = memory.reserve(peak_bytes) => reservation,
                            () = stream_token.cancelled() => break,
                        };
                        let slot = tokio::select! {
                            slot = Arc::clone(&decompress_slots).acquire_owned() => match slot {
                                Ok(slot) => slot,
                                Err(_) => {
                                    terminal_error = Some(SessionFailure::retryable(anyhow!(
                                        "PQv1 decompression pool closed"
                                    )));
                                    break;
                                }
                            },
                            () = stream_token.cancelled() => break,
                        };
                        let sc = Arc::clone(&source_counters);
                        seq_counter += 1;
                        let decode_task = tokio::task::spawn_blocking(move || {
                            let _slot = slot;
                            decode_parts(parts, &reservation, sc.as_ref())
                        });
                        tokio::spawn(async move {
                            let (parts, failure_kind) = match decode_task.await {
                                Ok(parts) => (parts, TerminalFailureKind::Fatal),
                                Err(error) => (
                                    Err(anyhow!(
                                        "PQv1 decompression worker failed for sequence {seq}: {error}"
                                    )),
                                    TerminalFailureKind::Retryable,
                                ),
                            };
                            // Always send the sequence result. An error must advance
                            // through the reorder point before it terminates the
                            // partition streams, otherwise a later batch could pass it.
                            decoded_slot.send(DecodedBatch {
                                seq,
                                parts,
                                failure_kind,
                            });
                        });
                        if request_tx.send(read_request()).is_err() {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 request channel closed"
                            )));
                            break;
                        }
                    }
                    Some(migration_streaming_read_server_message::Response::Committed(
                        committed,
                    )) => {
                        if let Err(error) = acknowledge_committed(&stream_inner, &committed) {
                            terminal_error = Some(SessionFailure::fatal(error));
                            break;
                        }
                    }
                    Some(migration_streaming_read_server_message::Response::Release(release)) => {
                        let pid =
                            match validate_release_assignment(&mut active_assignments, &release) {
                                Ok(pid) => pid,
                                Err(failure) => {
                                    terminal_error = Some(failure);
                                    break;
                                }
                            };
                        let released_assign_id = release.assign_id;
                        if release.forceful_release {
                            terminal_error = Some(release_failure(pid, released_assign_id, true));
                            break;
                        }
                        if request_tx.send(released_request(release)).is_err() {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 request channel closed"
                            )));
                            break;
                        }
                        if tokio::time::timeout(
                            RELEASE_HANDOFF_TIMEOUT.min(network_timeout),
                            release_handed_off.notified(),
                        )
                        .await
                        .is_err()
                        {
                            terminal_error = Some(SessionFailure::retryable(anyhow!(
                                "PQv1 Released request was not consumed by the transport"
                            )));
                            break;
                        }
                        // `Released` has no protocol acknowledgement. Keep the bidi
                        // stream alive briefly after tonic consumes the body item so
                        // dropping the response task cannot immediately race its H2 send.
                        tokio::time::sleep(RELEASE_TRANSPORT_GRACE).await;
                        // A graceful release can race with data already queued for the
                        // pipeline. Restarting the source after acknowledging the release
                        // preserves at-least-once delivery instead of committing against a
                        // partition assignment we no longer own.
                        terminal_error = Some(release_failure(pid, released_assign_id, false));
                        break;
                    }
                    Some(migration_streaming_read_server_message::Response::PartitionStatus(_))
                    | None => {
                        terminal_error = Some(SessionFailure::fatal(anyhow!(
                            "PQv1 protocol response escaped validation"
                        )));
                        break;
                    }
                }
            }
            if let Some(failure) = terminal_error {
                tracing::error!("{}", failure.error);
                broadcast_failure(&stream_inner, &failure.error, failure.kind);
            }
            tracing::info!("PQv1 background task exited");
        });

        Ok((Self { inner }, prs))
    }

    pub async fn commit(
        &self,
        partition_id: i64,
        cookies: Vec<CommitCookie>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !cookies.is_empty(),
            "PQv1 commit for partition {partition_id} has no cookies"
        );
        if self.inner.session_token.is_cancelled() {
            return Err(commit_session_stopped_error(&self.inner, partition_id));
        }
        let keys: Vec<_> = cookies
            .iter()
            .map(|cookie| (cookie.assign_id, cookie.partition_cookie))
            .collect();
        let unique_keys: HashSet<_> = keys.iter().copied().collect();
        anyhow::ensure!(
            unique_keys.len() == keys.len(),
            "PQv1 commit for partition {partition_id} contains duplicate cookies"
        );
        let waiter = Arc::new(PendingCommit::new(keys.len()));
        {
            let mut pending = self
                .inner
                .pending_commit_cookies
                .lock()
                .map_err(|_| anyhow!("PQv1 pending commit state is poisoned"))?;
            for key in &keys {
                pending.entry(*key).or_default().push_back(waiter.clone());
            }
        }

        if self
            .inner
            .request_tx
            .send(MigrationStreamingReadClientMessage {
                request: Some(migration_streaming_read_client_message::Request::Commit(
                    migration_streaming_read_client_message::Commit {
                        cookies,
                        offset_ranges: vec![],
                    },
                )),
                token: Vec::new(),
            })
            .is_err()
        {
            remove_pending_commit(&self.inner, &keys, &waiter)?;
            anyhow::bail!("PQv1 request channel closed while committing partition {partition_id}");
        }

        tokio::select! {
            biased;
            () = waiter.wait() => Ok(()),
            () = self.inner.session_token.cancelled() => {
                remove_pending_commit(&self.inner, &keys, &waiter)?;
                Err(commit_session_stopped_error(&self.inner, partition_id))
            }
            () = tokio::time::sleep(COMMIT_ACK_TIMEOUT) => {
                remove_pending_commit(&self.inner, &keys, &waiter)?;
                let error = anyhow!(
                    "PQv1 timed out waiting for commit acknowledgement for partition {partition_id}"
                );
                broadcast_failure(&self.inner, &error, TerminalFailureKind::Retryable);
                Err(error)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Decompression
// ---------------------------------------------------------------------------

fn declared_uncompressed_size(uncompressed_size: u64) -> anyhow::Result<usize> {
    let size = usize::try_from(uncompressed_size)
        .map_err(|_| anyhow!("declared uncompressed size does not fit in usize"))?;
    anyhow::ensure!(
        size <= MAX_DECOMPRESSED_MESSAGE_SIZE,
        "declared uncompressed size {size} exceeds limit {MAX_DECOMPRESSED_MESSAGE_SIZE}"
    );
    Ok(size)
}

fn peak_decode_bytes(parts: &[RawPart]) -> anyhow::Result<usize> {
    parts
        .iter()
        .flat_map(|part| &part.msgs)
        .try_fold((0_usize, 0_usize), |(peak, decoded_total), message| {
            let decoded = declared_uncompressed_size(message.uncompressed_size)?;
            let decoded_total = decoded_total
                .checked_add(decoded)
                .ok_or_else(|| anyhow!("PQv1 decoded batch size overflow"))?;
            anyhow::ensure!(
                decoded_total <= MAX_DECOMPRESSED_BATCH_SIZE,
                "declared uncompressed batch size {decoded_total} exceeds limit {MAX_DECOMPRESSED_BATCH_SIZE}"
            );
            let message_peak = if message.codec == 1 {
                anyhow::ensure!(
                    message.data.len() == decoded,
                    "RAW decoded size mismatch: declared={decoded}, actual={}",
                    message.data.len()
                );
                message.data.len()
            } else {
                message
                    .data
                    .len()
                    .checked_add(decoded)
                    .ok_or_else(|| anyhow!("PQv1 message memory estimate overflow"))?
            };
            let peak = peak
                .checked_add(message_peak)
                .ok_or_else(|| anyhow!("PQv1 batch memory estimate overflow"))?;
            Ok((peak, decoded_total))
        })
        .map(|(peak, _decoded_total)| peak)
}

fn decode_parts(
    parts: Vec<RawPart>,
    reservation: &MemoryReservation,
    counters: &SourceCounters,
) -> anyhow::Result<Vec<DecodedPart>> {
    let mut decoded_parts = Vec::with_capacity(parts.len());
    let mut retained_bytes = 0_usize;
    for RawPart { pid, cookie, msgs } in parts {
        let mut decoded = Vec::with_capacity(msgs.len());
        let mut decomp_busy = core::time::Duration::ZERO;
        let mut decompressed_bytes = 0_u64;
        for message in msgs {
            let codec = message.codec;
            let offset = message.offset;
            let started = std::time::Instant::now();
            let data = match decompress(message.data, codec, message.uncompressed_size) {
                Ok(data) => data,
                Err(error) => {
                    decomp_busy += started.elapsed();
                    counters.add_decomp_busy(decomp_busy);
                    counters.add_decompressed_bytes(decompressed_bytes);
                    return Err(anyhow!(
                        "PQv1 decompress failed: codec={codec} offset={offset}: {error}"
                    ));
                }
            };
            decomp_busy += started.elapsed();
            decompressed_bytes = decompressed_bytes.saturating_add(data.len() as u64);
            retained_bytes = retained_bytes
                .checked_add(data.len())
                .ok_or_else(|| anyhow!("PQv1 decoded batch size overflow"))?;
            decoded.push(DecodedMessage {
                data,
                offset,
                write_timestamp_ms: message.write_timestamp_ms,
            });
        }
        counters.add_decomp_busy(decomp_busy);
        counters.add_decompressed_bytes(decompressed_bytes);
        decoded_parts.push(DecodedPart {
            pid,
            cookie,
            msgs: decoded,
            memory: reservation.clone(),
        });
    }
    // Compressed inputs have been dropped. Keep accounting only for the decoded
    // buffers retained by `DecodedPart` clones of this reservation.
    let _shrunk = reservation.shrink_to(retained_bytes);
    Ok(decoded_parts)
}

/// Decompress a message body. RAW (codec 1) reuses the input buffer (zero-copy).
fn decompress(data: Vec<u8>, codec: i32, uncompressed_size: u64) -> anyhow::Result<Bytes> {
    let expected_size = declared_uncompressed_size(uncompressed_size)?;
    let decoded = match codec {
        1 => Bytes::from(data), // RAW — move, no copy
        2 => {
            let decoder = flate2::read::GzDecoder::new(&*data);
            read_exact_decoded(decoder, expected_size)?
        }
        4 => {
            let mut decoder = zstd::stream::read::Decoder::new(&*data)?;
            decoder.window_log_max(MAX_ZSTD_WINDOW_LOG)?;
            read_exact_decoded(decoder, expected_size)?
        }
        _ => return Err(anyhow!("Unsupported codec: {codec}")),
    };
    anyhow::ensure!(
        decoded.len() == expected_size,
        "decoded size mismatch: declared={expected_size}, actual={}",
        decoded.len()
    );
    Ok(decoded)
}

/// Decode into an exactly-sized buffer, then probe one extra byte without growing it.
/// This keeps the actual allocation within the size reserved by `peak_decode_bytes` even
/// when a malformed stream expands beyond its declared size.
fn read_exact_decoded(
    mut decoder: impl std::io::Read,
    expected_size: usize,
) -> anyhow::Result<Bytes> {
    let mut decoded = vec![0_u8; expected_size];
    let mut actual_size = 0_usize;
    while actual_size < expected_size {
        match decoder.read(&mut decoded[actual_size..]) {
            Ok(0) => break,
            Ok(read) => actual_size += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::ensure!(
        actual_size == expected_size,
        "decoded size mismatch: declared={expected_size}, actual={actual_size}"
    );
    let mut extra = [0_u8; 1];
    let extra_size = decoder.read(&mut extra)?;
    anyhow::ensure!(
        extra_size == 0,
        "decoded size mismatch: declared={expected_size}, actual_at_least={}",
        expected_size.saturating_add(extra_size)
    );
    Ok(Bytes::from(decoded))
}

// ---------------------------------------------------------------------------
// PqV1Source
// ---------------------------------------------------------------------------

pub struct PqV1Source {
    client: PqV1Client,
    rx: mpsc::Receiver<PartitionEvent>,
    terminal_failure: watch::Receiver<Option<Arc<TerminalFailure>>>,
    partition_id: i64,
    topic_name: Arc<str>,
}

impl PqV1Source {
    #[must_use]
    pub fn new(
        client: PqV1Client,
        rx: mpsc::Receiver<PartitionEvent>,
        partition_id: i64,
        topic_name: Arc<str>,
    ) -> Self {
        let terminal_failure = client.inner.terminal_failure.subscribe();
        Self {
            client,
            rx,
            terminal_failure,
            partition_id,
            topic_name,
        }
    }
}

impl Source for PqV1Source {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<MessageBatch>> {
        Box::pin(async move {
            let current_failure = self.terminal_failure.borrow().clone();
            if let Some(error) = current_failure {
                return surface_terminal_failure(error.as_ref());
            }
            let first_event = loop {
                let event = tokio::select! {
                    biased;
                    changed = self.terminal_failure.changed() => {
                        if changed.is_err() {
                            anyhow::bail!("PQv1 terminal failure channel closed unexpectedly");
                        }
                        let current_failure = self.terminal_failure.borrow().clone();
                        if let Some(error) = current_failure {
                            return surface_terminal_failure(error.as_ref());
                        }
                        continue;
                    }
                    event = self.rx.recv() => event,
                };
                break event;
            };
            let Some(PartitionEvent::Data(first)) = first_event else {
                anyhow::bail!("PQv1 partition event stream closed unexpectedly");
            };
            let mut messages = Vec::new();
            let mut memory = Vec::new();
            let mut cookies = Vec::new();
            if let Err(error) = self.append_part(first, &mut messages, &mut memory, &mut cookies) {
                return Err(PipelineFailure::fatal(error).into());
            }
            for _ in 1..MAX_PARTS_PER_SOURCE_BATCH {
                let Ok(event) = self.rx.try_recv() else {
                    break;
                };
                match event {
                    PartitionEvent::Data(part) => {
                        if let Err(error) =
                            self.append_part(part, &mut messages, &mut memory, &mut cookies)
                        {
                            return Err(PipelineFailure::fatal(error).into());
                        }
                    }
                }
            }
            let current_failure = self.terminal_failure.borrow().clone();
            if let Some(error) = current_failure {
                return surface_terminal_failure(error.as_ref());
            }
            let commit_marker = (!cookies.is_empty()).then(|| {
                CommitMarker::new(PqV1CommitMarker {
                    partition_id: self.partition_id,
                    cookies,
                })
            });
            Ok(MessageBatch {
                messages,
                partition_id: self.partition_id,
                commit_marker,
                memory,
            })
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move {
            let mut cookies = Vec::new();
            for marker in markers {
                let Some(marker) = marker.downcast_ref::<PqV1CommitMarker>() else {
                    return Err(
                        PipelineFailure::fatal(anyhow!("Invalid PQv1 commit marker")).into(),
                    );
                };
                if marker.partition_id != self.partition_id {
                    return Err(PipelineFailure::fatal(anyhow!(
                        "PQv1 commit marker partition mismatch: source={}, marker={}",
                        self.partition_id,
                        marker.partition_id
                    ))
                    .into());
                }
                cookies.extend(marker.cookies.iter().copied());
            }
            self.client.commit(self.partition_id, cookies).await
        })
    }
}

impl PqV1Source {
    fn append_part(
        &self,
        part: DecodedPart,
        messages: &mut Vec<Message>,
        memory: &mut Vec<MemoryReservation>,
        cookies: &mut Vec<CommitCookie>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            part.pid == self.partition_id,
            "PQv1 partition mismatch: source={}, batch={}",
            self.partition_id,
            part.pid
        );
        memory.push(part.memory);
        if let Some(cookie) = part.cookie {
            cookies.push(cookie);
        }
        for message in part.msgs {
            let write_timestamp_ms = i64::try_from(message.write_timestamp_ms)?;
            messages.push(Message {
                value: message.data,
                meta: MessageMeta {
                    topic_name: Some(Arc::clone(&self.topic_name)),
                    partition: Some(SourcePartition::Int(self.partition_id)),
                    offset: Some(i64::try_from(message.offset)?),
                    write_timestamp_ms: Some(write_timestamp_ms),
                },
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> PqV1Client {
        test_client_with_requests().0
    }

    fn test_client_with_requests() -> (
        PqV1Client,
        mpsc::UnboundedReceiver<MigrationStreamingReadClientMessage>,
    ) {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (terminal_failure, _terminal_receiver) = watch::channel(None);
        let client = PqV1Client {
            inner: Arc::new(PqV1ClientInner {
                request_tx,
                partition_queues: Mutex::new(HashMap::new()),
                pending_commit_cookies: StdMutex::new(HashMap::new()),
                terminal_failure,
                session_token: CancellationToken::new(),
            }),
        };
        (client, request_rx)
    }

    fn test_topic() -> Arc<str> {
        Arc::from("topic")
    }

    fn cookie(partition_cookie: u64) -> CommitCookie {
        CommitCookie {
            assign_id: 11,
            partition_cookie,
        }
    }

    fn decoded_batch(seq: u64) -> DecodedBatch {
        DecodedBatch {
            seq,
            parts: Ok(Vec::new()),
            failure_kind: TerminalFailureKind::Fatal,
        }
    }

    fn protocol_path(path: &str) -> crate::Ydb::pers_queue::v1::Path {
        crate::Ydb::pers_queue::v1::Path {
            path: path.to_string(),
        }
    }

    fn assignment(topic: &str, cluster: &str) -> migration_streaming_read_server_message::Assigned {
        migration_streaming_read_server_message::Assigned {
            topic: Some(protocol_path(topic)),
            cluster: cluster.to_string(),
            partition: 7,
            assign_id: 11,
            read_offset: 3,
            end_offset: 5,
        }
    }

    fn active_assignment() -> HashMap<i64, ActiveAssignment> {
        let mut active = HashMap::new();
        register_assignment(
            &mut active,
            &HashSet::from([7]),
            "topic",
            &assignment("topic", "cluster"),
        )
        .unwrap();
        active
    }

    fn partition_data(
        topic: &str,
        cluster: &str,
    ) -> migration_streaming_read_server_message::data_batch::PartitionData {
        migration_streaming_read_server_message::data_batch::PartitionData {
            topic: Some(protocol_path(topic)),
            cluster: cluster.to_string(),
            partition: 7,
            cookie: Some(cookie(1)),
            ..Default::default()
        }
    }

    fn release(
        topic: &str,
        cluster: &str,
        forceful_release: bool,
    ) -> migration_streaming_read_server_message::Release {
        migration_streaming_read_server_message::Release {
            topic: Some(protocol_path(topic)),
            cluster: cluster.to_string(),
            partition: 7,
            assign_id: 11,
            forceful_release,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn decoded_slot_waits_while_the_channel_is_full() {
        use core::future::Future as _;

        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(decoded_batch(0)).await.unwrap();
        let cancellation = CancellationToken::new();
        let mut waiting = Box::pin(reserve_decoded_slot(&sender, &cancellation));

        let was_pending = futures_util::future::poll_fn(|context| {
            Poll::Ready(waiting.as_mut().poll(context).is_pending())
        })
        .await;
        assert!(was_pending, "a full decoded queue must apply backpressure");

        assert_eq!(receiver.recv().await.unwrap().seq, 0);
        let permit = waiting.await.unwrap();
        permit.send(decoded_batch(1));
        assert_eq!(receiver.recv().await.unwrap().seq, 1);
    }

    #[tokio::test]
    async fn decoded_slot_wait_is_cancelled_while_the_channel_is_full() {
        let (sender, _receiver) = mpsc::channel(1);
        sender.send(decoded_batch(0)).await.unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = reserve_decoded_slot(&sender, &cancellation)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("session cancelled"));
    }

    #[tokio::test(start_paused = true)]
    async fn network_stage_reports_its_timeout() {
        let cancellation = CancellationToken::new();
        let timeout = core::time::Duration::from_millis(25);

        let error = network_stage(
            "test network stage",
            timeout,
            &cancellation,
            core::future::pending::<anyhow::Result<()>>(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("test network stage timed out after 25 ms"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn network_stage_honors_cancellation() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = network_stage(
            "test network stage",
            core::time::Duration::from_secs(1),
            &cancellation,
            core::future::pending::<anyhow::Result<()>>(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "test network stage cancelled");
    }

    #[test]
    fn runtime_init_scopes_the_session_to_requested_partition_groups() {
        let runtime = init_request("topic", "consumer", &[3, 7]);
        assert_eq!(
            runtime.topics_read_settings[0].partition_group_ids,
            vec![3, 7]
        );
    }

    #[test]
    fn non_grpc_schemes_are_rejected_instead_of_using_the_cleartext_transport() {
        for scheme in ["grpcs", "https"] {
            let error = parse_endpoint(&format!("{scheme}://example.test:2135")).unwrap_err();
            assert!(error.to_string().contains("without TLS"));
            assert!(error.to_string().contains(scheme));
        }
        let error = parse_endpoint("grpc://example.test:2135/ignored-db").unwrap_err();
        assert!(error
            .to_string()
            .contains("must not contain a database path"));
    }

    #[test]
    fn invalid_auth_metadata_is_rejected_without_exposing_the_token() {
        for token in ["", "secret\nvalue"] {
            let error = set_ydb_headers(&mut MetadataMap::new(), token).unwrap_err();
            assert!(error.to_string().contains("access token"));
            if !token.is_empty() {
                assert!(!error.to_string().contains(token));
            }
        }
    }

    #[test]
    fn release_paths_are_retryable_and_graceful_release_is_acknowledged() {
        let forceful = release("topic", "cluster", true);
        let mut active = active_assignment();
        assert_eq!(
            validate_release_assignment(&mut active, &forceful).unwrap(),
            7
        );
        assert_eq!(
            release_failure(7, 11, true).kind,
            TerminalFailureKind::Retryable
        );

        let graceful = migration_streaming_read_server_message::Release {
            forceful_release: false,
            ..forceful
        };
        let mut active = active_assignment();
        assert_eq!(
            validate_release_assignment(&mut active, &graceful).unwrap(),
            7
        );
        assert!(!active.contains_key(&7));
        assert_eq!(
            release_failure(7, 11, false).kind,
            TerminalFailureKind::Retryable
        );
        let request = released_request(graceful);
        let Some(migration_streaming_read_client_message::Request::Released(released)) =
            request.request
        else {
            panic!("graceful Release must be answered with Released")
        };
        assert_eq!(released.partition, 7);
        assert_eq!(released.assign_id, 11);
    }

    #[test]
    fn assignment_topic_must_match_the_configured_topic() {
        let mut active = HashMap::new();

        let failure = register_assignment(
            &mut active,
            &HashSet::from([7]),
            "topic",
            &assignment("other-topic", "cluster"),
        )
        .unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure
            .error
            .to_string()
            .contains("assignment topic mismatch"));
        assert!(active.is_empty());
    }

    #[test]
    fn reassignment_of_an_active_partition_is_fatal() {
        let mut active = active_assignment();
        let mut reassigned = assignment("topic", "cluster");
        reassigned.assign_id = 12;

        let failure = register_assignment(&mut active, &HashSet::from([7]), "topic", &reassigned)
            .unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure
            .error
            .to_string()
            .contains("reassigned active partition"));
        assert_eq!(active.get(&7).unwrap().assign_id, 11);
    }

    #[test]
    fn data_identity_must_match_the_active_assignment() {
        for (topic, cluster, expected) in [
            ("other-topic", "cluster", "data topic mismatch"),
            ("topic", "other-cluster", "data cluster mismatch"),
        ] {
            let failure =
                validate_data_partition(&partition_data(topic, cluster), &active_assignment())
                    .unwrap_err();

            assert_eq!(failure.kind, TerminalFailureKind::Fatal);
            assert!(failure.error.to_string().contains(expected));
        }
    }

    #[test]
    fn release_identity_must_match_the_active_assignment() {
        for (topic, cluster, expected) in [
            ("other-topic", "cluster", "release topic mismatch"),
            ("topic", "other-cluster", "release cluster mismatch"),
        ] {
            let mut active = active_assignment();
            let failure = validate_release_assignment(&mut active, &release(topic, cluster, false))
                .unwrap_err();

            assert_eq!(failure.kind, TerminalFailureKind::Fatal);
            assert!(failure.error.to_string().contains(expected));
            assert!(
                active.contains_key(&7),
                "failed validation must retain ownership state"
            );
        }
    }

    #[test]
    fn data_cookie_assign_id_must_match_the_active_assignment() {
        let mut data = partition_data("topic", "cluster");
        data.cookie.as_mut().unwrap().assign_id = 12;

        let failure = validate_data_partition(&data, &active_assignment()).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure
            .error
            .to_string()
            .contains("cookie assign_id mismatch"));
    }

    #[test]
    fn release_assign_id_must_match_the_active_assignment() {
        let mut active = active_assignment();
        let mut released = release("topic", "cluster", false);
        released.assign_id = 12;

        let failure = validate_release_assignment(&mut active, &released).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure
            .error
            .to_string()
            .contains("release assign_id mismatch"));
        assert!(active.contains_key(&7));
    }

    #[test]
    fn data_partition_must_have_an_active_assignment() {
        let mut data = partition_data("topic", "cluster");
        data.partition = 8;

        let failure = validate_data_partition(&data, &active_assignment()).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("inactive partition 8"));
    }

    #[test]
    fn release_partition_must_have_an_active_assignment() {
        let mut active = active_assignment();
        let mut released = release("topic", "cluster", false);
        released.partition = 8;

        let failure = validate_release_assignment(&mut active, &released).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("inactive partition 8"));
        assert!(active.contains_key(&7));
    }

    #[tokio::test]
    async fn graceful_release_is_consumed_by_the_request_body_before_teardown() {
        use futures_util::StreamExt as _;

        let (sender, receiver) = mpsc::unbounded_channel();
        let handed_off = Arc::new(Notify::new());
        let mut stream = RequestStream {
            rx: receiver,
            release_handed_off: Arc::clone(&handed_off),
        };
        sender
            .send(released_request(
                migration_streaming_read_server_message::Release {
                    partition: 7,
                    assign_id: 11,
                    ..Default::default()
                },
            ))
            .unwrap();

        let notification = handed_off.notified();
        tokio::pin!(notification);
        let request = stream.next().await.expect("queued Released request");
        assert!(matches!(
            request.request,
            Some(migration_streaming_read_client_message::Request::Released(
                _
            ))
        ));
        tokio::time::timeout(core::time::Duration::from_millis(100), notification)
            .await
            .expect("request body must signal Released handoff");
    }

    #[test]
    fn decompression_rejects_oversized_and_inexact_output() {
        use std::io::Write as _;

        let oversized = u64::try_from(MAX_DECOMPRESSED_MESSAGE_SIZE)
            .unwrap()
            .saturating_add(1);
        let error = decompress(Vec::new(), 1, oversized).unwrap_err();
        assert!(error.to_string().contains("exceeds limit"));

        let error = decompress(vec![1, 2, 3], 1, 2).unwrap_err();
        assert!(error.to_string().contains("decoded size mismatch"));

        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gzip.write_all(b"too long").unwrap();
        let gzip = gzip.finish().unwrap();
        let zstd = zstd::encode_all(&b"too long"[..], 1).unwrap();
        for (codec, compressed) in [(2, gzip), (4, zstd)] {
            let error = decompress(compressed, codec, 3).unwrap_err();
            assert!(error.to_string().contains("decoded size mismatch"));
        }

        let gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast())
            .finish()
            .unwrap();
        let error = decompress(gzip, 2, 3).unwrap_err();
        assert!(error.to_string().contains("declared=3, actual=0"));
    }

    #[test]
    fn peak_accounting_is_zero_copy_for_raw_and_caps_the_decoded_batch() {
        let raw = vec![RawPart {
            pid: 7,
            cookie: None,
            msgs: vec![RawMsg {
                data: vec![1, 2, 3],
                codec: 1,
                uncompressed_size: 3,
                offset: 1,
                write_timestamp_ms: 1,
            }],
        }];
        assert_eq!(peak_decode_bytes(&raw).unwrap(), 3);

        let half_plus_one = u64::try_from(MAX_DECOMPRESSED_BATCH_SIZE / 2 + 1).unwrap();
        let compressed = vec![RawPart {
            pid: 7,
            cookie: None,
            msgs: vec![
                RawMsg {
                    data: vec![],
                    codec: 2,
                    uncompressed_size: half_plus_one,
                    offset: 1,
                    write_timestamp_ms: 1,
                },
                RawMsg {
                    data: vec![],
                    codec: 2,
                    uncompressed_size: half_plus_one,
                    offset: 2,
                    write_timestamp_ms: 1,
                },
            ],
        }];
        let error = peak_decode_bytes(&compressed).unwrap_err();
        assert!(error.to_string().contains("batch size"));
    }

    #[test]
    fn successful_status_with_issues_is_terminal() {
        let message = MigrationStreamingReadServerMessage {
            status: YDB_STATUS_SUCCESS,
            issues: vec![crate::Ydb::issue::IssueMessage {
                message: "protocol warning".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let failure = validate_server_message(&message).unwrap_err();
        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("protocol warning"));
    }

    #[test]
    fn successful_message_without_a_response_is_fatal() {
        let message = MigrationStreamingReadServerMessage {
            status: YDB_STATUS_SUCCESS,
            ..Default::default()
        };

        let failure = validate_server_message(&message).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("missing response"));
    }

    #[test]
    fn duplicate_init_response_is_fatal() {
        let mut init_done = false;
        record_init_response(&mut init_done).unwrap();

        let failure = record_init_response(&mut init_done).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("duplicate InitResponse"));
    }

    #[test]
    fn non_init_response_before_init_is_fatal() {
        let mut init_done = false;
        let response = migration_streaming_read_server_message::Response::DataBatch(
            migration_streaming_read_server_message::DataBatch::default(),
        );

        let failure = validate_response_phase(&mut init_done, &response).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("before InitResponse"));
    }

    #[test]
    fn unsolicited_partition_status_is_fatal() {
        let mut init_done = true;
        let response = migration_streaming_read_server_message::Response::PartitionStatus(
            migration_streaming_read_server_message::PartitionStatus::default(),
        );

        let failure = validate_response_phase(&mut init_done, &response).unwrap_err();

        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure
            .error
            .to_string()
            .contains("unsolicited PartitionStatus"));
    }

    #[test]
    fn server_statuses_have_explicit_retry_disposition() {
        use StatusCode::{
            Aborted, AlreadyExists, BadRequest, BadSession, Cancelled, ExternalError, GenericError,
            InternalError, NotFound, Overloaded, PreconditionFailed, SchemeError, SessionBusy,
            SessionExpired, Timeout, Unauthorized, Unavailable, Undetermined, Unsupported,
        };

        for status in [
            InternalError,
            Aborted,
            Unavailable,
            Overloaded,
            GenericError,
            Timeout,
            BadSession,
            SessionExpired,
            Cancelled,
            Undetermined,
            SessionBusy,
            ExternalError,
        ] {
            let message = MigrationStreamingReadServerMessage {
                status: status as i32,
                ..Default::default()
            };
            let failure = validate_server_message(&message).unwrap_err();
            assert_eq!(failure.kind, TerminalFailureKind::Retryable, "{status:?}");
        }

        for status in [
            BadRequest,
            Unauthorized,
            SchemeError,
            PreconditionFailed,
            AlreadyExists,
            NotFound,
            Unsupported,
        ] {
            let message = MigrationStreamingReadServerMessage {
                status: status as i32,
                ..Default::default()
            };
            let failure = validate_server_message(&message).unwrap_err();
            assert_eq!(failure.kind, TerminalFailureKind::Fatal, "{status:?}");
        }

        let unknown = MigrationStreamingReadServerMessage {
            status: 400_999,
            ..Default::default()
        };
        assert_eq!(
            validate_server_message(&unknown).unwrap_err().kind,
            TerminalFailureKind::Fatal
        );
    }

    #[tokio::test]
    async fn decompression_error_fails_the_whole_server_batch() {
        let memory = PipelineMemory::new(1024);
        let reservation = memory.reserve(10).await;
        let parts = vec![RawPart {
            pid: 7,
            cookie: None,
            msgs: vec![RawMsg {
                data: vec![1, 2, 3],
                codec: 99,
                uncompressed_size: 3,
                offset: 42,
                write_timestamp_ms: 100,
            }],
        }];

        let Err(error) = decode_parts(parts, &reservation, &SourceCounters::new()) else {
            panic!("unsupported codec must fail the batch");
        };
        assert!(error.to_string().contains("codec=99 offset=42"));
    }

    #[tokio::test]
    async fn source_treats_an_unexpected_partition_stream_close_as_retryable() {
        let (tx, rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        drop(tx);

        let error = source.read_batch().await.unwrap_err();
        assert!(error.to_string().contains("stream closed unexpectedly"));
        assert!(error.downcast_ref::<PipelineFailure>().is_none());
    }

    #[tokio::test]
    async fn source_treats_partition_mismatch_as_fatal() {
        let memory = PipelineMemory::new(16);
        let (tx, rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        tx.send(PartitionEvent::Data(DecodedPart {
            pid: 8,
            cookie: Some(cookie(1)),
            msgs: vec![],
            memory: memory.reserve(1).await,
        }))
        .await
        .unwrap();

        let error = source.read_batch().await.unwrap_err();
        assert!(error.to_string().contains("partition mismatch"));
        assert!(error
            .downcast_ref::<PipelineFailure>()
            .is_some_and(|failure| !failure.is_retryable()));
    }

    #[tokio::test]
    async fn terminal_failure_disposition_retries_transport_but_not_decompression() {
        let (client, _request_rx) = test_client_with_requests();
        let (_partition_tx, partition_rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(client.clone(), partition_rx, 7, test_topic());

        broadcast_failure(
            client.inner.as_ref(),
            &anyhow!("transport stopped"),
            TerminalFailureKind::Retryable,
        );
        let Err(error) = source.read_batch().await else {
            panic!("transport failure must request a retry")
        };
        assert!(error.to_string().contains("transport stopped"));

        let (client, _request_rx) = test_client_with_requests();
        let (_partition_tx, partition_rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(client.clone(), partition_rx, 7, test_topic());
        broadcast_failure(
            client.inner.as_ref(),
            &anyhow!("decompression contract violated"),
            TerminalFailureKind::Fatal,
        );

        let error = source.read_batch().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("decompression contract violated"));
        assert!(error
            .downcast_ref::<PipelineFailure>()
            .is_some_and(|failure| !failure.is_retryable()));
    }

    #[test]
    fn fatal_failure_cannot_be_overwritten_by_a_retryable_failure() {
        let client = test_client();
        broadcast_failure(
            client.inner.as_ref(),
            &anyhow!("fatal decompression contract violation"),
            TerminalFailureKind::Fatal,
        );
        broadcast_failure(
            client.inner.as_ref(),
            &anyhow!("later transport failure"),
            TerminalFailureKind::Retryable,
        );

        let failure = client
            .inner
            .terminal_failure
            .borrow()
            .clone()
            .expect("terminal failure");
        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.message.contains("fatal decompression"));
    }

    #[tokio::test]
    async fn session_task_panic_is_surfaced_as_retryable_failure() {
        let client = test_client();
        let (_partition_tx, partition_rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(client.clone(), partition_rx, 7, test_topic());
        spawn_session_task(Arc::clone(&client.inner), "test", async {
            panic!("test panic");
        });

        let result = tokio::time::timeout(core::time::Duration::from_secs(1), source.read_batch())
            .await
            .expect("supervisor must wake the source");
        let Err(error) = result else {
            panic!("task panic must be a retryable source error")
        };
        assert!(error.to_string().contains("test task failed"));
        assert!(error.to_string().contains("panicked"));
    }

    #[tokio::test]
    async fn source_keeps_one_memory_reservation_per_decoded_part() {
        let memory = PipelineMemory::new(1024);
        let reservation = memory.reserve(20).await;
        let (tx, rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        tx.send(PartitionEvent::Data(DecodedPart {
            pid: 7,
            cookie: None,
            msgs: vec![
                DecodedMessage {
                    data: Bytes::from_static(b"one"),
                    offset: 1,
                    write_timestamp_ms: 10,
                },
                DecodedMessage {
                    data: Bytes::from_static(b"two"),
                    offset: 2,
                    write_timestamp_ms: 11,
                },
            ],
            memory: reservation,
        }))
        .await
        .unwrap();

        let batch = source.read_batch().await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.memory.len(), 1);
    }

    #[tokio::test]
    async fn discarded_batch_emits_a_marker_without_messages() {
        let memory = PipelineMemory::new(16);
        let (tx, rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        tx.send(PartitionEvent::Data(DecodedPart {
            pid: 7,
            cookie: Some(cookie(3)),
            msgs: vec![],
            memory: memory.reserve(1).await,
        }))
        .await
        .unwrap();

        let batch = source.read_batch().await.unwrap();
        assert!(batch.messages.is_empty());
        let marker = batch.commit_marker.expect("discarded batch commit marker");
        assert_eq!(
            marker.downcast_ref::<PqV1CommitMarker>().unwrap().cookies[0].partition_cookie,
            3
        );
    }

    #[tokio::test]
    async fn decode_shrinks_peak_reservation_to_retained_bytes() {
        let memory = PipelineMemory::new(1024);
        let reservation = memory.reserve(20).await;
        let parts = vec![RawPart {
            pid: 7,
            cookie: Some(cookie(1)),
            msgs: vec![RawMsg {
                data: vec![1, 2, 3],
                codec: 1,
                uncompressed_size: 3,
                offset: 42,
                write_timestamp_ms: 100,
            }],
        }];

        let decoded = decode_parts(parts, &reservation, &SourceCounters::new()).unwrap();
        assert_eq!(decoded[0].memory.bytes(), 3);
        assert_eq!(memory.used(), 3);
    }

    #[tokio::test]
    async fn decode_preserves_cookie_for_an_empty_partition_part() {
        let memory = PipelineMemory::new(16);
        let reservation = memory.reserve(1).await;
        let decoded = decode_parts(
            vec![RawPart {
                pid: 7,
                cookie: Some(cookie(4)),
                msgs: vec![],
            }],
            &reservation,
            &SourceCounters::new(),
        )
        .unwrap();

        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].msgs.is_empty());
        assert_eq!(decoded[0].cookie.as_ref().unwrap().partition_cookie, 4);
    }

    #[tokio::test]
    async fn source_preserves_and_commits_every_cookie_in_one_grouped_request() {
        let memory = PipelineMemory::new(1024);
        let (client, mut request_rx) = test_client_with_requests();
        let (tx, rx) = mpsc::channel(2);
        let mut source = PqV1Source::new(client.clone(), rx, 7, test_topic());
        for partition_cookie in [1, 2] {
            tx.send(PartitionEvent::Data(DecodedPart {
                pid: 7,
                cookie: Some(cookie(partition_cookie)),
                msgs: vec![DecodedMessage {
                    data: Bytes::from_static(b"message"),
                    offset: partition_cookie,
                    write_timestamp_ms: 10 + partition_cookie,
                }],
                memory: memory.reserve(7).await,
            }))
            .await
            .unwrap();
        }

        let mut batch = source.read_batch().await.unwrap();
        let marker = batch.commit_marker.take().expect("commit marker");
        let marker_value = marker.downcast_ref::<PqV1CommitMarker>().unwrap();
        assert_eq!(
            marker_value
                .cookies
                .iter()
                .map(|cookie| cookie.partition_cookie)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let markers = [
            marker,
            CommitMarker::new(PqV1CommitMarker {
                partition_id: 7,
                cookies: vec![cookie(3)],
            }),
        ];
        let mut commit_future = Box::pin(source.commit_offsets(&markers));
        assert!(tokio::time::timeout(
            core::time::Duration::from_millis(20),
            commit_future.as_mut()
        )
        .await
        .is_err());
        let request = request_rx.recv().await.unwrap();
        let Some(migration_streaming_read_client_message::Request::Commit(commit)) =
            request.request
        else {
            panic!("commit marker must produce a Commit request")
        };
        assert_eq!(
            commit
                .cookies
                .iter()
                .map(|cookie| cookie.partition_cookie)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        acknowledge_committed(
            client.inner.as_ref(),
            &migration_streaming_read_server_message::Committed {
                cookies: commit.cookies,
                offset_ranges: vec![],
            },
        )
        .unwrap();
        tokio::time::timeout(
            core::time::Duration::from_millis(100),
            commit_future.as_mut(),
        )
        .await
        .expect("commit must finish after Committed response")
        .unwrap();
        assert!(client
            .inner
            .pending_commit_cookies
            .lock()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn fatal_terminal_failure_remains_fatal_while_waiting_for_commit_ack() {
        let (client, mut request_rx) = test_client_with_requests();
        let mut commit = Box::pin(client.commit(7, vec![cookie(1)]));
        assert!(
            tokio::time::timeout(core::time::Duration::from_millis(20), commit.as_mut())
                .await
                .is_err()
        );
        let _request = request_rx.recv().await.expect("Commit request");

        broadcast_failure(
            client.inner.as_ref(),
            &anyhow!("decompression contract violated"),
            TerminalFailureKind::Fatal,
        );
        let error = tokio::time::timeout(core::time::Duration::from_millis(100), commit)
            .await
            .expect("session cancellation must wake commit")
            .unwrap_err();
        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("fatal terminal error must keep its pipeline disposition");
        assert!(!failure.is_retryable());

        let error = client.commit(7, vec![cookie(2)]).await.unwrap_err();
        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("an already-stopped fatal session must keep its disposition");
        assert!(!failure.is_retryable());
    }

    #[tokio::test]
    async fn source_bounds_the_number_of_parts_drained_into_one_batch() {
        let memory = PipelineMemory::new(MAX_PARTS_PER_SOURCE_BATCH + 1);
        let (tx, rx) = mpsc::channel(MAX_PARTS_PER_SOURCE_BATCH + 1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        for partition_cookie in 0..=MAX_PARTS_PER_SOURCE_BATCH {
            tx.send(PartitionEvent::Data(DecodedPart {
                pid: 7,
                cookie: Some(cookie(partition_cookie as u64)),
                msgs: vec![],
                memory: memory.reserve(1).await,
            }))
            .await
            .unwrap();
        }

        let first = source.read_batch().await.unwrap();
        assert_eq!(
            first
                .commit_marker
                .as_ref()
                .unwrap()
                .downcast_ref::<PqV1CommitMarker>()
                .unwrap()
                .cookies
                .len(),
            MAX_PARTS_PER_SOURCE_BATCH
        );
        drop(first);

        let second = source.read_batch().await.unwrap();
        assert_eq!(
            second
                .commit_marker
                .as_ref()
                .unwrap()
                .downcast_ref::<PqV1CommitMarker>()
                .unwrap()
                .cookies
                .len(),
            1
        );
    }
}
