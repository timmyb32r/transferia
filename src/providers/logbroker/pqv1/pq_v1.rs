//! `PQv1` (`PersQueue` V1) gRPC client for Logbroker.
//!
//! Startup discovery runs `ListEndpoints` and `DescribeTopic`; each worker then opens a
//! `MigrationStreamingRead` bidi stream on a proxy → `InitResponse` → Assigned → `StartRead`
//! → `DataBatch`. Transport is HTTP/2 with prior knowledge (Go-compatible), bridged into
//! tonic via a small `tower::Service`.

mod decode;
mod transport;

use decode::*;
pub use decode::{DecodedMessage, DecodedPart, PqV1CommitMarker};
use transport::*;
pub use transport::{
    connect_http2_prior_knowledge, http_uri, parse_endpoint, set_ydb_headers, H2Service,
};

use alloc::sync::Arc;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use core::task::{Context, Poll};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex as StdMutex;

use anyhow::anyhow;
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt as _};

use crate::core::data::message::{Message, MessageMeta, SourceBatch};
use crate::core::memory::{MemoryReservation, PipelineMemory};
use crate::core::source::{CommitMarker, Source};
use crate::delivery::execution::PipelineFailure;
use crate::metrics::SourceCounters;
use crate::providers::logbroker::proto::pers_queue::v1::{
    migration_streaming_read_client_message::{self, InitRequest, TopicReadSettings},
    migration_streaming_read_server_message, CommitCookie, MigrationStreamingReadClientMessage,
    MigrationStreamingReadServerMessage, ReadParams,
};
use crate::providers::logbroker::proto::status_ids::StatusCode;
use tokio::sync::{mpsc, watch, Notify};
use tokio_util::sync::CancellationToken;
use tonic::Request;

/// `Ydb.StatusIds.SUCCESS`. Status codes live in the reserved range [400000, 400999];
/// SUCCESS is 400000 (NOT 0 — 0 is `STATUS_CODE_UNSPECIFIED`, sent on streaming data msgs).
const YDB_STATUS_SUCCESS: i32 = 400_000;
/// `Ydb.StatusIds.STATUS_CODE_UNSPECIFIED`. Streaming data messages carry it on every
/// batch; only real error codes abort the stream.
const YDB_STATUS_UNSPECIFIED: i32 = 0;

/// Bound protobuf allocations before application-level validation. Normal reads request at most
/// 1 MiB of payload; the remaining headroom covers repeated-field and protocol metadata.
const MAX_GRPC_MESSAGE_SIZE: usize = 8 * 1024 * 1024;
/// A corrupt or malicious declared size must not turn a small compressed message into an
/// unbounded allocation. This deliberately matches the transport cap; normal reads are much
/// smaller (`ReadParams.max_read_size` is 1 MiB).
const MAX_DECOMPRESSED_MESSAGE_SIZE: usize = 128 * 1024 * 1024;
/// Bound the sum as well as each individual message: `PipelineMemory` deliberately admits one
/// oversized reservation, so it is not a substitute for a decompression safety limit.
const MAX_DECOMPRESSED_BATCH_SIZE: usize = MAX_DECOMPRESSED_MESSAGE_SIZE;
const MAX_ZSTD_WINDOW_LOG: u32 = 27; // log2(128 MiB)
/// A size-only read limit does not bound allocations for empty or tiny messages. Keep the
/// protocol's message-count credit finite as a second, independent admission limit.
const MAX_READ_MESSAGES_COUNT: u32 = 10_000;
const MAX_READ_SIZE: u32 = 1_048_576;
const MAX_READ_BATCH_COUNT: usize = MAX_READ_MESSAGES_COUNT as usize;
const MAX_READ_EXTRA_FIELD_COUNT: usize = MAX_READ_MESSAGES_COUNT as usize;
const MIN_VEC_ALLOCATION_CAPACITY: usize = 8;
const DECODE_READ_CHUNK_SIZE: usize = 64 * 1024;
const RELEASE_HANDOFF_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(1);
const RELEASE_TRANSPORT_GRACE: core::time::Duration = core::time::Duration::from_millis(100);
const PARTITION_CHANNEL_CAP: usize = 1;
const DECODED_MESSAGE_METADATA_BYTES: usize = core::mem::size_of::<DecodedMessage>();
const DECODED_PART_METADATA_BYTES: usize = core::mem::size_of::<DecodedPart>();
/// `read_batch` converts every decoded item into a provider-neutral `Message` while the
/// decoded vector is still alive. Account that short overlap before it is allocated.
const OUTPUT_MESSAGE_METADATA_BYTES: usize = core::mem::size_of::<Message>();

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
            max_read_messages_count: MAX_READ_MESSAGES_COUNT,
            max_read_size: MAX_READ_SIZE,
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
    topic: Option<&'a crate::providers::logbroker::proto::pers_queue::v1::Path>,
) -> anyhow::Result<&'a str> {
    topic
        .map(|topic| topic.path.as_str())
        .ok_or_else(|| anyhow!("PQv1 {event} has no topic for partition {partition}"))
}

fn validate_event_identity(
    event: &str,
    partition: i64,
    topic: Option<&crate::providers::logbroker::proto::pers_queue::v1::Path>,
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

fn tonic_failure(stage: &str, status: &tonic::Status) -> SessionFailure {
    use tonic::Code;

    let error = anyhow!("PQv1 {stage}: {status}");
    // Transport/server availability failures are safe to retry because all
    // callers are read-only or open a fresh consumer session. Deterministic
    // request, identity, and range errors require configuration repair.
    match status.code() {
        Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Internal
        | Code::Unavailable => SessionFailure::retryable(error),
        Code::Ok
        | Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::DataLoss
        | Code::Unauthenticated => SessionFailure::fatal(error),
    }
}

fn surface_session_failure(failure: SessionFailure) -> anyhow::Error {
    match failure.kind {
        TerminalFailureKind::Retryable => failure.error,
        TerminalFailureKind::Fatal => PipelineFailure::fatal(failure.error).into(),
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

fn send_read_request(
    sender: &mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
    outstanding: &AtomicBool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        outstanding
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok(),
        "PQv1 attempted to issue overlapping Read requests"
    );
    if sender.send(read_request()).is_err() {
        outstanding.store(false, Ordering::Release);
        anyhow::bail!("PQv1 request channel closed");
    }
    Ok(())
}

async fn send_read_request_with_credit(
    memory: &PipelineMemory,
    credit_bytes: usize,
    sender: &mpsc::UnboundedSender<MigrationStreamingReadClientMessage>,
    outstanding: &AtomicBool,
    pending_credit: &StdMutex<Option<MemoryReservation>>,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    let reservation = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("PQv1 read credit reservation cancelled"),
        reservation = memory.reserve_progress_source(credit_bytes) => reservation,
    };
    {
        let mut pending = pending_credit
            .lock()
            .map_err(|_| anyhow!("PQv1 read credit state is poisoned"))?;
        anyhow::ensure!(
            pending.is_none(),
            "PQv1 attempted to replace an outstanding raw read credit"
        );
        *pending = Some(reservation);
    }
    if let Err(error) = send_read_request(sender, outstanding) {
        pending_credit
            .lock()
            .map_err(|_| anyhow!("PQv1 read credit state is poisoned"))?
            .take();
        return Err(error);
    }
    Ok(())
}

fn request_next_read(sender: &mpsc::Sender<()>) -> anyhow::Result<()> {
    sender.try_send(()).map_err(|error| match error {
        mpsc::error::TrySendError::Full(()) => {
            anyhow!("PQv1 attempted to queue overlapping read credit")
        }
        mpsc::error::TrySendError::Closed(()) => anyhow!("PQv1 read credit task closed"),
    })
}

fn consume_read_credit(
    outstanding: &AtomicBool,
    pending_credit: &StdMutex<Option<MemoryReservation>>,
) -> anyhow::Result<MemoryReservation> {
    anyhow::ensure!(
        outstanding
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok(),
        "PQv1 protocol violation: received DataBatch without an outstanding Read request"
    );
    pending_credit
        .lock()
        .map_err(|_| anyhow!("PQv1 read credit state is poisoned"))?
        .take()
        .ok_or_else(|| anyhow!("PQv1 DataBatch has no pre-reserved raw read credit"))
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
    partition_id: i64,
    partition_tx: mpsc::Sender<DecodedPart>,
    pending_commit_cookies: StdMutex<PendingCommitQueues>,
    terminal_failure: watch::Sender<Option<Arc<TerminalFailure>>>,
    session_token: CancellationToken,
    network_timeout: core::time::Duration,
}

struct PendingCommit {
    remaining: AtomicUsize,
    state: AtomicU8,
    completed: Notify,
}

const COMMIT_WAITING: u8 = 0;
const COMMIT_ACKNOWLEDGED: u8 = 1;
const COMMIT_ABANDONED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AbandonCommitResult {
    Acknowledged,
    Abandoned,
}

type CommitCookieKey = (u64, u64);
type PendingCommitQueues = HashMap<CommitCookieKey, VecDeque<Arc<PendingCommit>>>;

impl PendingCommit {
    fn new(cookie_count: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(cookie_count),
            state: AtomicU8::new(COMMIT_WAITING),
            completed: Notify::new(),
        }
    }

    async fn wait(&self) {
        loop {
            let completed = self.completed.notified();
            if self.state.load(Ordering::Acquire) != COMMIT_WAITING {
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
    for waiter in completed {
        if waiter.state.load(Ordering::Acquire) == COMMIT_ABANDONED {
            continue;
        }
        let previous = waiter.remaining.fetch_sub(1, Ordering::AcqRel);
        anyhow::ensure!(previous > 0, "PQv1 commit acknowledgement underflow");
        if previous == 1 {
            let changed = waiter.state.compare_exchange(
                COMMIT_WAITING,
                COMMIT_ACKNOWLEDGED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            anyhow::ensure!(
                changed.is_ok() || changed == Err(COMMIT_ABANDONED),
                "PQv1 commit acknowledgement has invalid state"
            );
            waiter.completed.notify_waiters();
        }
    }
    drop(pending);
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

/// Atomically arbitrate acknowledgement against timeout/cancellation while holding the same
/// mutex that owns the cookie queues. Abandoned entries intentionally remain as tombstones until
/// a possible late server acknowledgement consumes them; otherwise that acknowledgement could
/// either fail the session as unknown or acknowledge a newer waiter for the same cookie.
fn abandon_pending_commit(
    inner: &PqV1ClientInner,
    waiter: &Arc<PendingCommit>,
) -> anyhow::Result<AbandonCommitResult> {
    let _pending = inner
        .pending_commit_cookies
        .lock()
        .map_err(|_| anyhow!("PQv1 pending commit state is poisoned"))?;
    match waiter.state.compare_exchange(
        COMMIT_WAITING,
        COMMIT_ABANDONED,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) | Err(COMMIT_ABANDONED) => Ok(AbandonCommitResult::Abandoned),
        Err(COMMIT_ACKNOWLEDGED) => Ok(AbandonCommitResult::Acknowledged),
        Err(state) => anyhow::bail!("PQv1 commit waiter has invalid state {state}"),
    }
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

async fn dispatch_parts(inner: &PqV1ClientInner, parts: Vec<DecodedPart>) {
    for part in parts {
        if part.pid != inner.partition_id {
            let error = anyhow!(
                "PQv1 decoded partition mismatch: session={}, batch={}",
                inner.partition_id,
                part.pid
            );
            broadcast_failure(inner, &error, TerminalFailureKind::Fatal);
            return;
        }
        let sent = tokio::select! {
            biased;
            () = inner.session_token.cancelled() => return,
            sent = inner.partition_tx.send(part) => sent,
        };
        if sent.is_err() {
            tracing::info!(
                "PQv1 partition {} queue closed; stopped dispatch",
                inner.partition_id
            );
            inner.session_token.cancel();
            return;
        }
    }
}

async fn join_decode_or_cancel<T: Send + 'static>(
    cancellation: &CancellationToken,
    mut task: tokio::task::JoinHandle<T>,
) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            task.abort();
            // `abort` prevents a queued blocking task from starting. Once started, Tokio cannot
            // preempt it, so the decoder also observes this token between bounded read chunks.
            // Awaiting it here guarantees its semaphore permit and memory lease are released.
            drop(task.await);
            None
        }
        result = &mut task => Some(result),
    }
}

struct ResponseLoopContext {
    inner: Arc<PqV1ClientInner>,
    pending_read_credit: Arc<StdMutex<Option<MemoryReservation>>>,
    read_outstanding: Arc<AtomicBool>,
    source_counters: Arc<SourceCounters>,
    assigned: HashSet<i64>,
    configured_topic: Arc<str>,
    read_credit_tx: mpsc::Sender<()>,
    data_tx: mpsc::Sender<PendingDataBatch>,
    benchmark_discard_before_decompression: bool,
    release_handed_off: Arc<Notify>,
    network_timeout: core::time::Duration,
}

async fn run_response_stream<S>(stream: S, context: ResponseLoopContext)
where
    S: Stream<Item = Result<MigrationStreamingReadServerMessage, tonic::Status>> + Send,
{
    let ResponseLoopContext {
        inner,
        pending_read_credit,
        read_outstanding,
        source_counters,
        assigned,
        configured_topic,
        read_credit_tx,
        data_tx,
        benchmark_discard_before_decompression,
        release_handed_off,
        network_timeout,
    } = context;
    let stream_token = inner.session_token.clone();
    tokio::pin!(stream);
    let mut init_done = false;
    let init_deadline = tokio::time::Instant::now() + network_timeout;
    let mut terminal_error = None;
    let mut active_assignments: HashMap<i64, ActiveAssignment> = HashMap::new();
    loop {
        let await_start = std::time::Instant::now();
        let msg = tokio::select! {
            message = stream.next() => match message {
                Some(Ok(message)) => message,
                None => {
                    terminal_error = Some(SessionFailure::retryable(anyhow!(
                        "PQv1 stream closed unexpectedly"
                    )));
                    break;
                }
                Some(Err(error)) => {
                    terminal_error = Some(tonic_failure("stream error", &error));
                    break;
                }
            },
            // Ctrl+C / shutdown — stop reading promptly instead of waiting for the next server
            // message, which could be never if the topic is idle.
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
        // This measures response latency, including control-plane responses;
        // it is deliberately not presented as downloader CPU utilization.
        source_counters.add_response_wait(await_start.elapsed());
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
            Some(migration_streaming_read_server_message::Response::InitResponse(response)) => {
                tracing::info!("PQv1 session: {}", response.session_id);
                if let Err(error) = request_next_read(&read_credit_tx) {
                    terminal_error = Some(SessionFailure::retryable(error));
                    break;
                }
            }
            Some(migration_streaming_read_server_message::Response::Assigned(assignment)) => {
                let pid = match register_assignment(
                    &mut active_assignments,
                    &assigned,
                    configured_topic.as_ref(),
                    &assignment,
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
                    assignment.read_offset,
                    assignment.end_offset
                );
                if inner
                    .request_tx
                    .send(start_read_request(assignment))
                    .is_err()
                {
                    terminal_error = Some(SessionFailure::retryable(anyhow!(
                        "PQv1 request channel closed"
                    )));
                    break;
                }
            }
            Some(migration_streaming_read_server_message::Response::DataBatch(batch)) => {
                let raw_memory = match consume_read_credit(
                    read_outstanding.as_ref(),
                    pending_read_credit.as_ref(),
                ) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        terminal_error = Some(SessionFailure::fatal(error));
                        break;
                    }
                };
                if let Err(error) =
                    validate_raw_data_batch(&batch, assigned.len(), raw_memory.bytes())
                {
                    terminal_error = Some(SessionFailure::fatal(error));
                    break;
                }
                let (kind, compressed_bytes, message_count) = match prepare_data_batch(
                    batch,
                    &active_assignments,
                    benchmark_discard_before_decompression,
                ) {
                    Ok(prepared) => prepared,
                    Err(failure) => {
                        terminal_error = Some(failure);
                        break;
                    }
                };
                let retained_bytes = match pending_raw_bytes(&kind) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        terminal_error = Some(SessionFailure::fatal(error));
                        break;
                    }
                };
                if retained_bytes > raw_memory.bytes() {
                    terminal_error = Some(SessionFailure::fatal(anyhow!(
                        "PQv1 pending raw batch size {retained_bytes} exceeds read credit {}",
                        raw_memory.bytes()
                    )));
                    break;
                }
                let _shrunk = raw_memory.shrink_to(retained_bytes);
                let pending_batch = PendingDataBatch { kind, raw_memory };
                source_counters.add_compressed_bytes(compressed_bytes);
                source_counters.add_messages(message_count);
                if let Err(failure) = enqueue_pending_data(&data_tx, pending_batch) {
                    terminal_error = Some(failure);
                    break;
                }
            }
            Some(migration_streaming_read_server_message::Response::Committed(committed)) => {
                if let Err(error) = acknowledge_committed(&inner, &committed) {
                    terminal_error = Some(SessionFailure::fatal(error));
                    break;
                }
            }
            Some(migration_streaming_read_server_message::Response::Release(release)) => {
                let pid = match validate_release_assignment(&mut active_assignments, &release) {
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
                if inner.request_tx.send(released_request(release)).is_err() {
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
                // `Released` has no protocol acknowledgement. Keep the bidi stream alive briefly
                // after tonic consumes the body item so dropping the response task cannot
                // immediately race its H2 send.
                tokio::time::sleep(RELEASE_TRANSPORT_GRACE).await;
                // A graceful release can race with data already queued for the pipeline.
                // Restarting the source after acknowledging the release preserves at-least-once
                // delivery instead of committing against a partition assignment we no longer own.
                terminal_error = Some(release_failure(pid, released_assign_id, false));
                break;
            }
            Some(migration_streaming_read_server_message::Response::PartitionStatus(_)) | None => {
                terminal_error = Some(SessionFailure::fatal(anyhow!(
                    "PQv1 protocol response escaped validation"
                )));
                break;
            }
        }
    }
    if let Some(failure) = terminal_error {
        tracing::error!("{}", failure.error);
        broadcast_failure(&inner, &failure.error, failure.kind);
    }
    tracing::info!("PQv1 background task exited");
}

fn surface_terminal_failure(failure: &TerminalFailure) -> anyhow::Result<SourceBatch> {
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

impl PqV1Client {
    pub async fn connect(
        proxy: &str,
        topic_path: &str,
        consumer: &str,
        token: &str,
        partition_id: i64,
        source_counters: Arc<SourceCounters>,
        cancel_token: CancellationToken,
        benchmark_discard_before_decompression: bool,
        memory: PipelineMemory,
        network_timeout: core::time::Duration,
        decompress_slots: Arc<tokio::sync::Semaphore>,
    ) -> anyhow::Result<(Self, mpsc::Receiver<DecodedPart>)> {
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
                    &[partition_id],
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
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
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
                    .map_err(|status| {
                        surface_session_failure(tonic_failure("stream open", &status))
                    })
            },
        )
        .await?;

        // Session ownership is intentionally one stream per partition.
        let assigned = HashSet::from([partition_id]);
        let raw_read_credit = raw_read_credit_bytes(1)?;
        let configured_topic: Arc<str> = Arc::from(topic_path);
        let (partition_tx, partition_rx) = mpsc::channel(PARTITION_CHANNEL_CAP);

        let session_token = cancel_token.child_token();
        let (terminal_failure, _terminal_receiver) = watch::channel(None);
        let inner = Arc::new(PqV1ClientInner {
            request_tx: request_tx.clone(),
            partition_id,
            partition_tx,
            pending_commit_cookies: StdMutex::new(HashMap::new()),
            terminal_failure,
            session_token: session_token.clone(),
            network_timeout,
        });

        // Capacity one is sufficient because each completed admission sends exactly one next
        // `Read`. The response task never waits for memory, decompression, or partition dispatch,
        // so control responses already in the stream remain observable while data is pressured.
        let (data_tx, mut data_rx) = mpsc::channel::<PendingDataBatch>(1);
        let (read_credit_tx, mut read_credit_rx) = mpsc::channel::<()>(1);
        let read_outstanding = Arc::new(AtomicBool::new(false));
        let pending_read_credit = Arc::new(StdMutex::new(None));

        let credit_inner = Arc::clone(&inner);
        let credit_token = session_token.clone();
        let credit_memory = memory.clone();
        let credit_request_tx = request_tx.clone();
        let credit_outstanding = Arc::clone(&read_outstanding);
        let credit_slot = Arc::clone(&pending_read_credit);
        spawn_session_task(Arc::clone(&inner), "read credit", async move {
            while let Some(()) = tokio::select! {
                biased;
                () = credit_token.cancelled() => None,
                signal = read_credit_rx.recv() => signal,
            } {
                if let Err(error) = send_read_request_with_credit(
                    &credit_memory,
                    raw_read_credit,
                    &credit_request_tx,
                    credit_outstanding.as_ref(),
                    credit_slot.as_ref(),
                    &credit_token,
                )
                .await
                {
                    if !credit_token.is_cancelled() {
                        broadcast_failure(&credit_inner, &error, TerminalFailureKind::Retryable);
                    }
                    return;
                }
            }
            tracing::info!("PQv1 read credit task exited");
        });

        let data_inner = Arc::clone(&inner);
        let data_token = session_token;
        let data_counters = Arc::clone(&source_counters);
        let data_read_credit_tx = read_credit_tx.clone();
        spawn_session_task(Arc::clone(&inner), "data admission", async move {
            while let Some(batch) = tokio::select! {
                biased;
                () = data_token.cancelled() => None,
                batch = data_rx.recv() => batch,
            } {
                let output_bytes = match &batch.kind {
                    PendingDataKind::Discard { parts } => parts
                        .len()
                        .checked_mul(DECODED_PART_METADATA_BYTES)
                        .ok_or_else(|| anyhow!("PQv1 discarded batch metadata size overflow")),
                    PendingDataKind::Decode { parts } => decoded_batch_retained_bytes(parts),
                };
                let output_bytes = match output_bytes {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        broadcast_failure(&data_inner, &error, TerminalFailureKind::Fatal);
                        return;
                    }
                };
                let additional_output_bytes = match &batch.kind {
                    PendingDataKind::Discard { .. } => Ok(output_bytes),
                    PendingDataKind::Decode { parts } => decoded_batch_additional_bytes(parts),
                };
                let additional_output_bytes = match additional_output_bytes {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        broadcast_failure(&data_inner, &error, TerminalFailureKind::Fatal);
                        return;
                    }
                };
                let raw_bytes = batch.raw_memory.bytes();
                let Some(overlap_bytes) = raw_bytes.checked_add(additional_output_bytes) else {
                    let error = anyhow!("PQv1 raw/decoded overlap memory estimate overflow");
                    broadcast_failure(&data_inner, &error, TerminalFailureKind::Fatal);
                    return;
                };
                if let Err(error) = batch.raw_memory.grow_progress_source_to(overlap_bytes) {
                    broadcast_failure(&data_inner, &error, TerminalFailureKind::Fatal);
                    return;
                }
                let PendingDataBatch { kind, raw_memory } = batch;
                let parts = match kind {
                    PendingDataKind::Discard { parts } => {
                        let _shrunk = raw_memory.shrink_to(output_bytes);
                        let mut discarded = Vec::with_capacity(parts.len());
                        for (pid, cookie) in parts {
                            discarded.push(DecodedPart {
                                pid,
                                cookie: Some(cookie),
                                msgs: Vec::new(),
                                memory: raw_memory.clone(),
                            });
                        }
                        discarded
                    }
                    PendingDataKind::Decode { parts } => {
                        if batch_uses_only_raw_codec(&parts) {
                            match decode_parts_with_cancellation(
                                parts,
                                &raw_memory,
                                data_counters.as_ref(),
                                &data_token,
                            ) {
                                Ok(parts) => parts,
                                Err(error) if error.downcast_ref::<DecodeCancelled>().is_some() => {
                                    return;
                                }
                                Err(error) => {
                                    tracing::error!("PQv1 RAW decode failed: {error}");
                                    broadcast_failure(
                                        &data_inner,
                                        &error,
                                        TerminalFailureKind::Fatal,
                                    );
                                    return;
                                }
                            }
                        } else {
                            let slot = tokio::select! {
                                biased;
                                () = data_token.cancelled() => return,
                                slot = Arc::clone(&decompress_slots).acquire_owned() => match slot {
                                    Ok(slot) => slot,
                                    Err(_) => {
                                        let error = anyhow!("PQv1 decompression pool closed");
                                        broadcast_failure(
                                            &data_inner,
                                            &error,
                                            TerminalFailureKind::Retryable,
                                        );
                                        return;
                                    }
                                },
                            };
                            let counters = Arc::clone(&data_counters);
                            let decode_token = data_token.clone();
                            let decode_task = tokio::task::spawn_blocking(move || {
                                let _slot = slot;
                                decode_parts_with_cancellation(
                                    parts,
                                    &raw_memory,
                                    counters.as_ref(),
                                    &decode_token,
                                )
                            });
                            let Some(decode_result) =
                                join_decode_or_cancel(&data_token, decode_task).await
                            else {
                                return;
                            };
                            match decode_result {
                                Ok(Ok(parts)) => parts,
                                Ok(Err(error))
                                    if error.downcast_ref::<DecodeCancelled>().is_some() =>
                                {
                                    return;
                                }
                                Ok(Err(error)) => {
                                    tracing::error!("PQv1 decompression failed: {error}");
                                    broadcast_failure(
                                        &data_inner,
                                        &error,
                                        TerminalFailureKind::Fatal,
                                    );
                                    return;
                                }
                                Err(error) => {
                                    let error =
                                        anyhow!("PQv1 decompression worker failed: {error}");
                                    broadcast_failure(
                                        &data_inner,
                                        &error,
                                        TerminalFailureKind::Retryable,
                                    );
                                    return;
                                }
                            }
                        }
                    }
                };
                dispatch_parts(&data_inner, parts).await;

                if let Err(error) = request_next_read(&data_read_credit_tx) {
                    broadcast_failure(&data_inner, &error, TerminalFailureKind::Retryable);
                    return;
                }
            }
            tracing::info!("PQv1 data admission task exited");
        });

        let response_context = ResponseLoopContext {
            inner: Arc::clone(&inner),
            pending_read_credit,
            read_outstanding,
            source_counters,
            assigned,
            configured_topic,
            read_credit_tx,
            data_tx,
            benchmark_discard_before_decompression,
            release_handed_off,
            network_timeout,
        };
        spawn_session_task(
            Arc::clone(&inner),
            "response stream",
            run_response_stream(response_stream, response_context),
        );

        Ok((Self { inner }, partition_rx))
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
            () = waiter.wait() => match waiter.state.load(Ordering::Acquire) {
                COMMIT_ACKNOWLEDGED => Ok(()),
                COMMIT_ABANDONED => Err(commit_session_stopped_error(&self.inner, partition_id)),
                state => anyhow::bail!("PQv1 commit waiter woke in invalid state {state}"),
            },
            () = self.inner.session_token.cancelled() => {
                match abandon_pending_commit(&self.inner, &waiter)? {
                    AbandonCommitResult::Acknowledged => Ok(()),
                    AbandonCommitResult::Abandoned => {
                        Err(commit_session_stopped_error(&self.inner, partition_id))
                    }
                }
            }
            () = tokio::time::sleep(self.inner.network_timeout) => {
                match abandon_pending_commit(&self.inner, &waiter)? {
                    AbandonCommitResult::Acknowledged => Ok(()),
                    AbandonCommitResult::Abandoned => {
                        let error = anyhow!(
                            "PQv1 timed out waiting for commit acknowledgement for partition {partition_id}"
                        );
                        broadcast_failure(&self.inner, &error, TerminalFailureKind::Retryable);
                        Err(error)
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PqV1Source
// ---------------------------------------------------------------------------

pub struct PqV1Source {
    client: PqV1Client,
    rx: mpsc::Receiver<DecodedPart>,
    terminal_failure: watch::Receiver<Option<Arc<TerminalFailure>>>,
    partition_id: i64,
    topic_path: Arc<str>,
}

impl PqV1Source {
    #[must_use]
    pub fn new(
        client: PqV1Client,
        rx: mpsc::Receiver<DecodedPart>,
        partition_id: i64,
        topic_path: Arc<str>,
    ) -> Self {
        let terminal_failure = client.inner.terminal_failure.subscribe();
        Self {
            client,
            rx,
            terminal_failure,
            partition_id,
            topic_path,
        }
    }
}

impl Source for PqV1Source {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        Box::pin(async move {
            let current_failure = self.terminal_failure.borrow().clone();
            if let Some(error) = current_failure {
                return surface_terminal_failure(error.as_ref());
            }
            let first_part = loop {
                let part = tokio::select! {
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
                    part = self.rx.recv() => part,
                };
                break part;
            };
            let Some(first) = first_part else {
                anyhow::bail!("PQv1 decoded-part stream closed unexpectedly");
            };
            let mut messages = Vec::with_capacity(first.msgs.len());
            let mut memory = Vec::new();
            let mut cookies = Vec::new();
            if let Err(error) = self.append_part(first, &mut messages, &mut memory, &mut cookies) {
                return Err(PipelineFailure::fatal(error).into());
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
            Ok(SourceBatch::Raw {
                messages,
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
        let DecodedPart {
            cookie,
            msgs,
            memory: part_memory,
            ..
        } = part;
        if let Some(cookie) = cookie {
            cookies.push(cookie);
        }
        let decoded_metadata = DECODED_PART_METADATA_BYTES
            .saturating_add(msgs.len().saturating_mul(DECODED_MESSAGE_METADATA_BYTES));
        for message in msgs {
            let write_timestamp_ms = i64::try_from(message.write_timestamp_ms)?;
            messages.push(Message {
                value: message.data,
                meta: MessageMeta {
                    topic: Some(Arc::clone(&self.topic_path)),
                    partition: Some(self.partition_id),
                    offset: Some(i64::try_from(message.offset)?),
                    write_timestamp_ms: Some(write_timestamp_ms),
                },
            });
        }
        let output_bytes = part_memory.bytes().saturating_sub(decoded_metadata).max(1);
        let _shrunk = part_memory.shrink_to(output_bytes);
        memory.push(part_memory);
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/pq_v1.rs"]
mod tests;
