//! `PQv1` (`PersQueue` V1) gRPC client for Logbroker.
//!
//! Flow: `ListEndpoints` (discover proxy) → `MigrationStreamingRead` bidi stream on the
//! proxy → `InitResponse` → Assigned → `StartRead` → `DataBatch`. Transport is HTTP/2 with
//! prior knowledge (Go-compatible), bridged into tonic via a small `tower::Service`.

use alloc::sync::Arc;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use core::task::{Context, Poll};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex as StdMutex;

use anyhow::anyhow;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt as _};

use crate::metrics::SourceCounters;
use crate::pipeline::memory::{MemoryReservation, PipelineMemory};
use crate::pipeline::retry::stable_retry_seed;
use crate::pipeline::PipelineFailure;
use http::Uri;
use hyper::client::conn::http2;
use tokio::sync::{mpsc, watch, Notify};
use tokio_util::sync::CancellationToken;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::Request;

/// YDB cluster database used for discovery/routing metadata (`x-ydb-database`).
/// Always `/Root` in our deployment — hardcoded rather than configured.
const YDB_DATABASE: &str = "/Root";
use crate::pipeline::source::{CommitMarker, Source};
use crate::types::message::{Message, MessageBatch, MessageMeta};
use crate::Ydb::pers_queue::v1::{
    migration_streaming_read_client_message::{self, InitRequest, TopicReadSettings},
    migration_streaming_read_server_message, Codec, CommitCookie,
    MigrationStreamingReadClientMessage, MigrationStreamingReadServerMessage, ReadParams,
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
/// A size-only read limit does not bound allocations for empty or tiny messages. Keep the
/// protocol's message-count credit finite as a second, independent admission limit.
const MAX_READ_MESSAGES_COUNT: u32 = 10_000;
const MAX_READ_SIZE: u32 = 1_048_576;
const MAX_READ_BATCH_COUNT: usize = MAX_READ_MESSAGES_COUNT as usize;
const MAX_READ_EXTRA_FIELD_COUNT: usize = MAX_READ_MESSAGES_COUNT as usize;
const MIN_VEC_ALLOCATION_CAPACITY: usize = 8;
const DECODE_READ_CHUNK_SIZE: usize = 64 * 1024;
const SESSION_INIT_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(30);
const COMMIT_ACK_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(30);
const RELEASE_HANDOFF_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(1);
const RELEASE_TRANSPORT_GRACE: core::time::Duration = core::time::Duration::from_millis(100);
const PARTITION_CHANNEL_CAP: usize = 1;
const DECODED_MESSAGE_METADATA_BYTES: usize = core::mem::size_of::<DecodedMessage>();
const DECODED_PART_METADATA_BYTES: usize = core::mem::size_of::<DecodedPart>();
/// `read_batch` converts every decoded item into a provider-neutral `Message` while the
/// decoded vector is still alive. Account that short overlap before it is allocated.
const OUTPUT_MESSAGE_METADATA_BYTES: usize = core::mem::size_of::<Message>();

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
    let addr = socket_address(uri);

    let (send_request, conn) = network_stage("HTTP/2 connection", timeout, cancellation, async {
        let stream = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(|e| anyhow!("TCP connect to {addr}: {e}"))?;
        stream.set_nodelay(true)?;
        let io = hyper_util::rt::TokioIo::new(stream);
        let mut builder = http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        builder
            .timer(hyper_util::rt::TokioTimer::new())
            .keep_alive_interval(timeout)
            .keep_alive_timeout(timeout)
            .keep_alive_while_idle(true);
        builder
            .handshake(io)
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

/// Parse a plaintext `PQv1` discovery endpoint into its authority. This client currently
/// targets the fixed [`YDB_DATABASE`], so accepting a database path would be misleading.
pub fn parse_endpoint(endpoint: &str) -> anyhow::Result<String> {
    let uri: Uri = endpoint
        .parse()
        .map_err(|e| anyhow!("Invalid PQv1 discovery endpoint '{endpoint}': {e}"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| anyhow!("PQv1 discovery endpoint must include the grpc:// scheme"))?;
    anyhow::ensure!(
        scheme == "grpc",
        "PQv1 scheme '{scheme}' is not supported: the custom transport requires grpc:// and uses a raw HTTP/2 TCP stream without TLS"
    );
    anyhow::ensure!(
        (uri.path().is_empty() || uri.path() == "/") && uri.query().is_none(),
        "PQv1 discovery endpoint must not contain a database path or query; the database is fixed to {YDB_DATABASE}"
    );
    let host = uri
        .authority()
        .map(http::uri::Authority::as_str)
        .ok_or_else(|| anyhow!("PQv1 discovery endpoint must include a host authority"))?
        .to_string();
    Ok(host)
}

fn http_uri(host: &str) -> anyhow::Result<Uri> {
    format!("http://{host}")
        .parse()
        .map_err(|e| anyhow!("bad uri http://{host}: {e}"))
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn socket_address(uri: &Uri) -> String {
    format_host_port(
        uri.host().unwrap_or("localhost"),
        uri.port_u16().unwrap_or(2135),
    )
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

/// A server data response after protocol validation, but before downstream admission.
/// The response task only enqueues this bounded work item; all waits happen in the data task so
/// commit acknowledgements and partition releases remain observable under backpressure.
enum PendingDataKind {
    Decode { parts: Vec<RawPart> },
    Discard { parts: Vec<(i64, CommitCookie)> },
}

struct PendingDataBatch {
    kind: PendingDataKind,
    /// Source-stage credit acquired before the corresponding `Read` request.
    /// It accounts for this raw batch while it waits for transform admission.
    raw_memory: MemoryReservation,
}

fn prepare_data_batch(
    batch: migration_streaming_read_server_message::DataBatch,
    active_assignments: &HashMap<i64, ActiveAssignment>,
    discard_payload: bool,
) -> Result<(PendingDataKind, u64, u64), SessionFailure> {
    let result = (|| {
        let mut compressed_bytes = 0_u64;
        let mut message_count = 0_u64;
        if discard_payload {
            let mut parts = Vec::with_capacity(batch.partition_data.len());
            for partition in batch.partition_data {
                let (pid, cookie) = validate_data_partition(&partition, active_assignments)
                    .map_err(|failure| failure.error)?;
                for message_batch in partition.batches {
                    for message in message_batch.message_data {
                        compressed_bytes = compressed_bytes
                            .checked_add(u64::try_from(message.data.len())?)
                            .ok_or_else(|| anyhow!("PQv1 compressed byte count overflow"))?;
                        message_count = message_count
                            .checked_add(1)
                            .ok_or_else(|| anyhow!("PQv1 message count overflow"))?;
                    }
                }
                parts.push((pid, cookie));
            }
            validate_message_count(message_count)?;
            return Ok((
                PendingDataKind::Discard { parts },
                compressed_bytes,
                message_count,
            ));
        }

        let mut parts = Vec::with_capacity(batch.partition_data.len());
        for partition in batch.partition_data {
            let (pid, cookie) = validate_data_partition(&partition, active_assignments)
                .map_err(|failure| failure.error)?;
            let mut messages = Vec::new();
            for message_batch in partition.batches {
                let write_timestamp_ms = message_batch.write_timestamp_ms;
                for message in message_batch.message_data {
                    compressed_bytes = compressed_bytes
                        .checked_add(u64::try_from(message.data.len())?)
                        .ok_or_else(|| anyhow!("PQv1 compressed byte count overflow"))?;
                    message_count = message_count
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("PQv1 message count overflow"))?;
                    messages.push(RawMsg {
                        data: message.data,
                        codec: message.codec,
                        uncompressed_size: message.uncompressed_size,
                        offset: message.offset,
                        write_timestamp_ms,
                    });
                }
            }
            parts.push(RawPart {
                pid,
                cookie: Some(cookie),
                msgs: messages,
            });
        }
        validate_message_count(message_count)?;
        Ok((
            PendingDataKind::Decode { parts },
            compressed_bytes,
            message_count,
        ))
    })();
    result.map_err(SessionFailure::fatal)
}

fn checked_raw_add(total: usize, value: usize) -> anyhow::Result<usize> {
    total
        .checked_add(value)
        .ok_or_else(|| anyhow!("PQv1 raw batch memory estimate overflow"))
}

fn checked_raw_capacity<T>(capacity: usize) -> anyhow::Result<usize> {
    capacity
        .checked_mul(core::mem::size_of::<T>())
        .ok_or_else(|| anyhow!("PQv1 raw batch memory estimate overflow"))
}

/// Maximum source credit for one advertised read. Repeated protobuf fields can retain nearly
/// twice their element count in `Vec` capacity, so fixed metadata is budgeted at 2x. Dynamic
/// strings/bytes are likewise budgeted above the wire-size limit; the received object is checked
/// against this credit before it can enter the admission queue.
fn raw_read_credit_bytes(max_partitions: usize) -> anyhow::Result<usize> {
    use migration_streaming_read_server_message::data_batch::{Batch, MessageData, PartitionData};

    let max_messages = usize::try_from(MAX_READ_MESSAGES_COUNT)?;
    let dynamic_container_count = max_partitions
        .checked_mul(3)
        .and_then(|count| count.checked_add(MAX_READ_BATCH_COUNT.checked_mul(2)?))
        .and_then(|count| count.checked_add(MAX_READ_EXTRA_FIELD_COUNT.checked_mul(2)?))
        .and_then(|count| count.checked_add(max_messages.checked_mul(3)?))
        .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))?;
    let dynamic_capacity_slack = dynamic_container_count
        .checked_mul(MIN_VEC_ALLOCATION_CAPACITY)
        .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))?;
    let max_dynamic = usize::try_from(MAX_READ_SIZE)?
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(dynamic_capacity_slack))
        .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))?;
    let repeated_capacity = |elements: usize, containers: usize| {
        elements
            .checked_mul(2)
            .and_then(|capacity| {
                capacity.checked_add(containers.checked_mul(MIN_VEC_ALLOCATION_CAPACITY)?)
            })
            .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))
    };
    let mut bytes = core::mem::size_of::<migration_streaming_read_server_message::DataBatch>();
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<PartitionData>(repeated_capacity(max_partitions, 1)?)?,
    )?;
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<Batch>(repeated_capacity(MAX_READ_BATCH_COUNT, max_partitions)?)?,
    )?;
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<MessageData>(repeated_capacity(
            max_messages,
            MAX_READ_BATCH_COUNT,
        )?)?,
    )?;
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<crate::Ydb::pers_queue::v1::KeyValue>(repeated_capacity(
            MAX_READ_EXTRA_FIELD_COUNT,
            MAX_READ_BATCH_COUNT,
        )?)?,
    )?;
    checked_raw_add(bytes, max_dynamic)
}

fn validate_raw_data_batch(
    batch: &migration_streaming_read_server_message::DataBatch,
    max_partitions: usize,
    reserved_bytes: usize,
) -> anyhow::Result<usize> {
    use migration_streaming_read_server_message::data_batch::{Batch, MessageData, PartitionData};

    anyhow::ensure!(
        batch.partition_data.len() <= max_partitions,
        "PQv1 DataBatch contains {} partition parts, exceeding active partition count {max_partitions}",
        batch.partition_data.len()
    );

    let mut seen_partitions = HashSet::with_capacity(batch.partition_data.len());
    let mut batch_count = 0_usize;
    let mut message_count = 0_usize;
    let mut extra_field_count = 0_usize;
    let mut raw_payload_bytes = 0_usize;
    let mut bytes = core::mem::size_of::<migration_streaming_read_server_message::DataBatch>();
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<PartitionData>(batch.partition_data.capacity())?,
    )?;
    for partition in &batch.partition_data {
        anyhow::ensure!(
            seen_partitions.insert(partition.partition),
            "PQv1 DataBatch repeats partition {}",
            partition.partition
        );
        bytes = checked_raw_add(bytes, partition.cluster.capacity())?;
        bytes = checked_raw_add(bytes, partition.deprecated_topic.capacity())?;
        if let Some(topic) = &partition.topic {
            bytes = checked_raw_add(bytes, topic.path.capacity())?;
        }
        batch_count = batch_count
            .checked_add(partition.batches.len())
            .ok_or_else(|| anyhow!("PQv1 DataBatch batch count overflow"))?;
        bytes = checked_raw_add(
            bytes,
            checked_raw_capacity::<Batch>(partition.batches.capacity())?,
        )?;
        for message_batch in &partition.batches {
            bytes = checked_raw_add(bytes, message_batch.source_id.capacity())?;
            bytes = checked_raw_add(bytes, message_batch.ip.capacity())?;
            extra_field_count = extra_field_count
                .checked_add(message_batch.extra_fields.len())
                .ok_or_else(|| anyhow!("PQv1 DataBatch extra-field count overflow"))?;
            bytes = checked_raw_add(
                bytes,
                checked_raw_capacity::<crate::Ydb::pers_queue::v1::KeyValue>(
                    message_batch.extra_fields.capacity(),
                )?,
            )?;
            for field in &message_batch.extra_fields {
                bytes = checked_raw_add(bytes, field.key.capacity())?;
                bytes = checked_raw_add(bytes, field.value.capacity())?;
            }
            message_count = message_count
                .checked_add(message_batch.message_data.len())
                .ok_or_else(|| anyhow!("PQv1 DataBatch message count overflow"))?;
            bytes = checked_raw_add(
                bytes,
                checked_raw_capacity::<MessageData>(message_batch.message_data.capacity())?,
            )?;
            for message in &message_batch.message_data {
                raw_payload_bytes = raw_payload_bytes
                    .checked_add(message.data.len())
                    .ok_or_else(|| anyhow!("PQv1 DataBatch raw payload size overflow"))?;
                bytes = checked_raw_add(bytes, message.data.capacity())?;
                bytes = checked_raw_add(bytes, message.partition_key.capacity())?;
                bytes = checked_raw_add(bytes, message.explicit_hash.capacity())?;
            }
        }
    }
    anyhow::ensure!(
        batch_count <= MAX_READ_BATCH_COUNT,
        "PQv1 DataBatch contains {batch_count} batches, exceeding limit {MAX_READ_BATCH_COUNT}"
    );
    anyhow::ensure!(
        extra_field_count <= MAX_READ_EXTRA_FIELD_COUNT,
        "PQv1 DataBatch contains {extra_field_count} extra fields, exceeding limit {MAX_READ_EXTRA_FIELD_COUNT}"
    );
    anyhow::ensure!(
        raw_payload_bytes <= usize::try_from(MAX_READ_SIZE)?,
        "PQv1 DataBatch raw payload size {raw_payload_bytes} exceeds requested limit {MAX_READ_SIZE}"
    );
    validate_message_count(u64::try_from(message_count)?)?;
    anyhow::ensure!(
        bytes <= reserved_bytes,
        "PQv1 DataBatch retained size {bytes} exceeds pre-reserved read credit {reserved_bytes}"
    );
    Ok(bytes.max(1))
}

fn pending_raw_bytes(kind: &PendingDataKind) -> anyhow::Result<usize> {
    let mut bytes = core::mem::size_of::<PendingDataKind>();
    match kind {
        PendingDataKind::Decode { parts } => {
            bytes = checked_raw_add(bytes, checked_raw_capacity::<RawPart>(parts.capacity())?)?;
            for part in parts {
                bytes =
                    checked_raw_add(bytes, checked_raw_capacity::<RawMsg>(part.msgs.capacity())?)?;
                for message in &part.msgs {
                    bytes = checked_raw_add(bytes, message.data.capacity())?;
                }
            }
        }
        PendingDataKind::Discard { parts } => {
            bytes = checked_raw_add(
                bytes,
                checked_raw_capacity::<(i64, CommitCookie)>(parts.capacity())?,
            )?;
        }
    }
    Ok(bytes.max(1))
}

fn validate_message_count(message_count: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        message_count <= u64::from(MAX_READ_MESSAGES_COUNT),
        "PQv1 DataBatch contains {message_count} messages, exceeding requested limit {MAX_READ_MESSAGES_COUNT}"
    );
    Ok(())
}

fn enqueue_pending_data(
    sender: &mpsc::Sender<PendingDataBatch>,
    batch: PendingDataBatch,
) -> Result<(), SessionFailure> {
    match sender.try_send(batch) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => Err(SessionFailure::fatal(anyhow!(
            "PQv1 protocol violation: received DataBatch without available read credit"
        ))),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(SessionFailure::retryable(anyhow!(
            "PQv1 data admission channel closed"
        ))),
    }
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
    match status.code() {
        Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::NotFound
        | Code::AlreadyExists
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unavailable => SessionFailure::retryable(error),
        Code::Ok
        | Code::InvalidArgument
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::Unimplemented
        | Code::Internal
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
            let _ = task.await;
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
    let init_deadline = tokio::time::Instant::now() + SESSION_INIT_TIMEOUT;
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
async fn discover_proxies(
    main_uri: &Uri,
    token: &str,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<Vec<crate::Ydb::discovery::EndpointInfo>> {
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
            .map_err(|status| surface_session_failure(tonic_failure("proxy discovery", &status)))
        })
        .await?;

    let op = resp.operation.ok_or_else(|| anyhow!("no operation"))?;
    if !op.ready {
        anyhow::bail!("ListEndpoints not ready");
    }
    // SUCCESS is 400000, not 0 (0 == UNSPECIFIED also acceptable for forward-compat).
    if op.status != YDB_STATUS_UNSPECIFIED && op.status != YDB_STATUS_SUCCESS {
        let status_name =
            StatusCode::try_from(op.status).map_or("UNKNOWN", |status| status.as_str_name());
        let error = anyhow!(
            "PQv1 ListEndpoints failed: status={} ({status_name}), issues={:?}",
            op.status,
            op.issues
        );
        return Err(surface_session_failure(SessionFailure {
            error,
            kind: status_failure_kind(op.status),
        }));
    }
    let result = op.result.ok_or_else(|| anyhow!("no result"))?;
    let eps = ListEndpointsResult::decode(result.value.as_slice())?;
    anyhow::ensure!(!eps.endpoints.is_empty(), "no endpoints");
    Ok(eps.endpoints)
}

fn ordered_plaintext_proxies(
    endpoints: Vec<crate::Ydb::discovery::EndpointInfo>,
    partition_id: i64,
) -> anyhow::Result<Vec<String>> {
    let mut proxies: Vec<_> = endpoints
        .into_iter()
        .filter(|endpoint| !endpoint.ssl)
        .filter_map(|endpoint| {
            let port = u16::try_from(endpoint.port).ok().filter(|port| *port > 0)?;
            let address = endpoint.address.trim();
            if address.is_empty() || !endpoint.load_factor.is_finite() || endpoint.load_factor < 0.0
            {
                return None;
            }
            Some((format_host_port(address, port), endpoint.load_factor))
        })
        .collect();
    proxies.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    proxies.dedup_by(|left, right| left.0 == right.0);
    anyhow::ensure!(
        !proxies.is_empty(),
        "discovery returned no usable plaintext endpoints"
    );

    // Weighted rendezvous ordering: every partition gets a stable primary and failover order,
    // while a larger discovery load factor makes an endpoint proportionally less likely to lead.
    let partition_bytes = partition_id.to_le_bytes();
    let mut scored: Vec<_> = proxies
        .into_iter()
        .map(|(address, load_factor)| {
            let mut key = Vec::with_capacity(partition_bytes.len() + address.len());
            key.extend_from_slice(&partition_bytes);
            key.extend_from_slice(address.as_bytes());
            let hash = stable_retry_seed(&key);
            let unit = (hash as f64 + 1.0) / (u64::MAX as f64 + 1.0);
            let score = -unit.ln() * (1.0 + f64::from(load_factor));
            (address, score)
        })
        .collect();
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(scored.into_iter().map(|(address, _)| address).collect())
}

impl PqV1Client {
    pub async fn discover_endpoints(
        endpoint: &str,
        token: &str,
        network_timeout: core::time::Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<(String, Vec<crate::Ydb::discovery::EndpointInfo>)> {
        auth_metadata_value(token)?;
        let main_host = parse_endpoint(endpoint)?;
        let main_uri = http_uri(&main_host)?;
        let endpoints = discover_proxies(&main_uri, token, network_timeout, cancellation).await?;
        Ok((main_host, endpoints))
    }

    pub fn order_proxies(
        main_host: String,
        endpoints: Vec<crate::Ydb::discovery::EndpointInfo>,
        partition_id: i64,
    ) -> Vec<String> {
        match ordered_plaintext_proxies(endpoints, partition_id) {
            Ok(mut proxies) => {
                if !proxies.iter().any(|proxy| proxy == &main_host) {
                    proxies.push(main_host);
                }
                proxies
            }
            Err(error) => {
                tracing::warn!(
                    "Proxy discovery returned no compatible endpoint: {error}. Using main endpoint."
                );
                vec![main_host]
            }
        }
    }

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
                    .map_err(|status| {
                        surface_session_failure(tonic_failure("stream open", &status))
                    })
            },
        )
        .await?;

        // Runtime ownership is intentionally one stream/session per partition.
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
                let Ok(output_bytes) = output_bytes else {
                    let error = output_bytes.unwrap_err();
                    broadcast_failure(&data_inner, &error, TerminalFailureKind::Fatal);
                    return;
                };
                let additional_output_bytes = match &batch.kind {
                    PendingDataKind::Discard { .. } => Ok(output_bytes),
                    PendingDataKind::Decode { parts } => decoded_batch_additional_bytes(parts),
                };
                let Ok(additional_output_bytes) = additional_output_bytes else {
                    let error = additional_output_bytes.unwrap_err();
                    broadcast_failure(&data_inner, &error, TerminalFailureKind::Fatal);
                    return;
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
            () = tokio::time::sleep(COMMIT_ACK_TIMEOUT) => {
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

const fn decoded_part_retained_bytes(message_count: usize) -> usize {
    DECODED_PART_METADATA_BYTES.saturating_add(message_count.saturating_mul(
        DECODED_MESSAGE_METADATA_BYTES.saturating_add(OUTPUT_MESSAGE_METADATA_BYTES),
    ))
}

fn decoded_batch_retained_bytes(parts: &[RawPart]) -> anyhow::Result<usize> {
    decoded_batch_bytes(parts, true)
}

/// Extra allocation required while the raw protobuf batch is still alive.
/// RAW payload buffers are moved into `Bytes`, so counting their capacity a
/// second time would manufacture pressure without representing real memory.
fn decoded_batch_additional_bytes(parts: &[RawPart]) -> anyhow::Result<usize> {
    decoded_batch_bytes(parts, false)
}

fn decoded_batch_bytes(parts: &[RawPart], include_raw_payload: bool) -> anyhow::Result<usize> {
    let mut retained = 0_usize;
    let mut decoded_total = 0_usize;
    for part in parts {
        retained = retained
            .checked_add(DECODED_PART_METADATA_BYTES)
            .and_then(|total| {
                total.checked_add(part.msgs.len().checked_mul(
                    DECODED_MESSAGE_METADATA_BYTES.checked_add(OUTPUT_MESSAGE_METADATA_BYTES)?,
                )?)
            })
            .ok_or_else(|| anyhow!("PQv1 decoded batch metadata estimate overflow"))?;
        for message in &part.msgs {
            let decoded = declared_uncompressed_size(message.uncompressed_size)?;
            let retained_payload = if message.codec == Codec::Raw as i32 {
                anyhow::ensure!(
                    message.data.len() == decoded,
                    "RAW decoded size mismatch: declared={decoded}, actual={}",
                    message.data.len()
                );
                if include_raw_payload {
                    message.data.capacity()
                } else {
                    0
                }
            } else {
                decoded
            };
            decoded_total = decoded_total
                .checked_add(decoded)
                .ok_or_else(|| anyhow!("PQv1 decoded batch size overflow"))?;
            anyhow::ensure!(
                decoded_total <= MAX_DECOMPRESSED_BATCH_SIZE,
                "declared uncompressed batch size {decoded_total} exceeds limit {MAX_DECOMPRESSED_BATCH_SIZE}"
            );
            retained = retained
                .checked_add(retained_payload)
                .ok_or_else(|| anyhow!("PQv1 decoded batch memory estimate overflow"))?;
        }
    }
    Ok(retained.max(1))
}

fn batch_uses_only_raw_codec(parts: &[RawPart]) -> bool {
    parts
        .iter()
        .flat_map(|part| &part.msgs)
        .all(|message| message.codec == Codec::Raw as i32)
}

#[derive(Debug)]
struct DecodeCancelled;

impl core::fmt::Display for DecodeCancelled {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("PQv1 decompression cancelled")
    }
}

impl std::error::Error for DecodeCancelled {}

fn ensure_decode_active(cancellation: &CancellationToken) -> anyhow::Result<()> {
    if cancellation.is_cancelled() {
        return Err(DecodeCancelled.into());
    }
    Ok(())
}

fn decode_parts_with_cancellation(
    parts: Vec<RawPart>,
    reservation: &MemoryReservation,
    counters: &SourceCounters,
    cancellation: &CancellationToken,
) -> anyhow::Result<Vec<DecodedPart>> {
    let mut decoded_parts = Vec::with_capacity(parts.len());
    let mut retained_bytes = 0_usize;
    for RawPart { pid, cookie, msgs } in parts {
        retained_bytes = retained_bytes
            .checked_add(decoded_part_retained_bytes(msgs.len()))
            .ok_or_else(|| anyhow!("PQv1 decoded batch metadata size overflow"))?;
        let mut decoded = Vec::with_capacity(msgs.len());
        let mut decomp_busy = core::time::Duration::ZERO;
        let mut decompressed_bytes = 0_u64;
        for message in msgs {
            ensure_decode_active(cancellation)?;
            let codec = message.codec;
            let offset = message.offset;
            let raw_capacity = message.data.capacity();
            let started = std::time::Instant::now();
            let data = match decompress_with_cancellation(
                message.data,
                codec,
                message.uncompressed_size,
                cancellation,
            ) {
                Ok(data) => data,
                Err(error) if error.downcast_ref::<DecodeCancelled>().is_some() => {
                    return Err(error);
                }
                Err(error) => {
                    decomp_busy += started.elapsed();
                    counters.add_decomp_busy(decomp_busy);
                    counters.add_decompressed_bytes(decompressed_bytes);
                    return Err(anyhow!(
                        "PQv1 decompress failed: codec={codec} offset={offset}: {error}"
                    ));
                }
            };
            let retained_payload = if codec == Codec::Raw as i32 {
                raw_capacity
            } else {
                data.len()
            };
            decomp_busy += started.elapsed();
            decompressed_bytes = decompressed_bytes.saturating_add(data.len() as u64);
            retained_bytes = retained_bytes
                .checked_add(retained_payload)
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

#[cfg(test)]
fn decode_parts(
    parts: Vec<RawPart>,
    reservation: &MemoryReservation,
    counters: &SourceCounters,
) -> anyhow::Result<Vec<DecodedPart>> {
    decode_parts_with_cancellation(parts, reservation, counters, &CancellationToken::new())
}

/// Decompress a message body. RAW (codec 1) reuses the input buffer (zero-copy).
fn decompress_with_cancellation(
    data: Vec<u8>,
    codec: i32,
    uncompressed_size: u64,
    cancellation: &CancellationToken,
) -> anyhow::Result<Bytes> {
    ensure_decode_active(cancellation)?;
    let expected_size = declared_uncompressed_size(uncompressed_size)?;
    let decoded = match Codec::try_from(codec).ok() {
        Some(Codec::Raw) => Bytes::from(data), // RAW — move, no copy
        Some(Codec::Gzip) => {
            let decoder = flate2::read::GzDecoder::new(&*data);
            read_exact_decoded(decoder, expected_size, cancellation)?
        }
        Some(Codec::Zstd) => {
            let mut decoder = zstd::stream::read::Decoder::new(&*data)?;
            decoder.window_log_max(MAX_ZSTD_WINDOW_LOG)?;
            read_exact_decoded(decoder, expected_size, cancellation)?
        }
        Some(Codec::Unspecified | Codec::Lzop) | None => {
            return Err(anyhow!("Unsupported codec: {codec}"));
        }
    };
    anyhow::ensure!(
        decoded.len() == expected_size,
        "decoded size mismatch: declared={expected_size}, actual={}",
        decoded.len()
    );
    Ok(decoded)
}

#[cfg(test)]
fn decompress(data: Vec<u8>, codec: i32, uncompressed_size: u64) -> anyhow::Result<Bytes> {
    decompress_with_cancellation(data, codec, uncompressed_size, &CancellationToken::new())
}

/// Decode into an exactly-sized buffer, then probe one extra byte without growing it.
/// This keeps the actual allocation within the pre-accounted decoded size even when a malformed
/// stream expands beyond its declaration.
fn read_exact_decoded(
    mut decoder: impl std::io::Read,
    expected_size: usize,
    cancellation: &CancellationToken,
) -> anyhow::Result<Bytes> {
    ensure_decode_active(cancellation)?;
    let mut decoded = vec![0_u8; expected_size];
    let mut actual_size = 0_usize;
    while actual_size < expected_size {
        ensure_decode_active(cancellation)?;
        let chunk_end = actual_size
            .saturating_add(DECODE_READ_CHUNK_SIZE)
            .min(expected_size);
        match decoder.read(&mut decoded[actual_size..chunk_end]) {
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
    ensure_decode_active(cancellation)?;
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
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<MessageBatch>> {
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
                    topic_path: Some(Arc::clone(&self.topic_path)),
                    partition_id: Some(self.partition_id),
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
        let (partition_tx, _partition_rx) = mpsc::channel(1);
        let (terminal_failure, _terminal_receiver) = watch::channel(None);
        let client = PqV1Client {
            inner: Arc::new(PqV1ClientInner {
                request_tx,
                partition_id: 7,
                partition_tx,
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

    fn protocol_path(path: &str) -> crate::Ydb::pers_queue::v1::Path {
        crate::Ydb::pers_queue::v1::Path {
            path: path.to_string(),
        }
    }

    fn discovery_endpoint(
        address: &str,
        port: u32,
        load_factor: f32,
        ssl: bool,
    ) -> crate::Ydb::discovery::EndpointInfo {
        crate::Ydb::discovery::EndpointInfo {
            address: address.to_string(),
            port,
            load_factor,
            ssl,
            ..Default::default()
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

    fn data_batch_with_empty_messages(
        message_count: usize,
    ) -> migration_streaming_read_server_message::DataBatch {
        let mut partition = partition_data("topic", "cluster");
        partition.batches = vec![migration_streaming_read_server_message::data_batch::Batch {
            message_data: vec![
                migration_streaming_read_server_message::data_batch::MessageData::default();
                message_count
            ],
            ..Default::default()
        }];
        migration_streaming_read_server_message::DataBatch {
            partition_data: vec![partition],
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

    fn server_response(
        response: migration_streaming_read_server_message::Response,
    ) -> MigrationStreamingReadServerMessage {
        MigrationStreamingReadServerMessage {
            status: YDB_STATUS_SUCCESS,
            response: Some(response),
            ..Default::default()
        }
    }

    fn mock_response_stream() -> (
        mpsc::UnboundedSender<Result<MigrationStreamingReadServerMessage, tonic::Status>>,
        impl Stream<Item = Result<MigrationStreamingReadServerMessage, tonic::Status>>,
    ) {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let stream = futures_util::stream::poll_fn(move |context| receiver.poll_recv(context));
        (sender, stream)
    }

    async fn blocked_partition_dispatch(
        client: &mut PqV1Client,
        memory: &PipelineMemory,
    ) -> (tokio::task::JoinHandle<()>, mpsc::Receiver<DecodedPart>) {
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(DecodedPart {
                pid: 7,
                cookie: Some(cookie(1)),
                msgs: Vec::new(),
                memory: memory.reserve(1).await,
            })
            .await
            .unwrap();
        Arc::get_mut(&mut client.inner)
            .expect("test client must be uniquely owned before dispatch")
            .partition_tx = sender;
        let inner = Arc::clone(&client.inner);
        let dispatch_memory = memory.clone();
        let mut dispatch = tokio::spawn(async move {
            dispatch_parts(
                inner.as_ref(),
                vec![DecodedPart {
                    pid: 7,
                    cookie: Some(cookie(2)),
                    msgs: Vec::new(),
                    memory: dispatch_memory.reserve(1).await,
                }],
            )
            .await;
        });
        assert!(
            tokio::time::timeout(core::time::Duration::from_millis(20), &mut dispatch)
                .await
                .is_err(),
            "partition dispatch must be blocked by the full queue"
        );
        (dispatch, receiver)
    }

    #[tokio::test]
    async fn committed_response_is_processed_while_partition_dispatch_is_blocked() {
        let memory = PipelineMemory::new(16);
        let (mut client, _request_rx) = test_client_with_requests();
        let (mut dispatch, _partition_rx) = blocked_partition_dispatch(&mut client, &memory).await;

        let waiter = Arc::new(PendingCommit::new(1));
        client
            .inner
            .pending_commit_cookies
            .lock()
            .unwrap()
            .entry((11, 1))
            .or_default()
            .push_back(Arc::clone(&waiter));
        let (data_tx, _data_rx) = mpsc::channel(1);
        let (read_credit_tx, _read_credit_rx) = mpsc::channel(1);
        let (server_tx, server_stream) = mock_response_stream();
        let response_task = tokio::spawn(run_response_stream(
            server_stream,
            ResponseLoopContext {
                inner: Arc::clone(&client.inner),
                pending_read_credit: Arc::new(StdMutex::new(None)),
                read_outstanding: Arc::new(AtomicBool::new(false)),
                source_counters: Arc::new(SourceCounters::new()),
                assigned: HashSet::from([7]),
                configured_topic: test_topic(),
                read_credit_tx,
                data_tx,
                benchmark_discard_before_decompression: false,
                release_handed_off: Arc::new(Notify::new()),
                network_timeout: core::time::Duration::from_secs(1),
            },
        ));
        server_tx
            .send(Ok(server_response(
                migration_streaming_read_server_message::Response::InitResponse(
                    migration_streaming_read_server_message::InitResponse::default(),
                ),
            )))
            .unwrap();
        server_tx
            .send(Ok(server_response(
                migration_streaming_read_server_message::Response::Committed(
                    migration_streaming_read_server_message::Committed {
                        cookies: vec![cookie(1)],
                        offset_ranges: Vec::new(),
                    },
                ),
            )))
            .unwrap();

        tokio::time::timeout(core::time::Duration::from_secs(1), waiter.wait())
            .await
            .expect("Committed must be handled while partition dispatch is blocked");
        assert_eq!(waiter.state.load(Ordering::Acquire), COMMIT_ACKNOWLEDGED);
        assert!(!dispatch.is_finished());

        client.inner.session_token.cancel();
        tokio::time::timeout(core::time::Duration::from_secs(1), response_task)
            .await
            .expect("response loop must stop after cancellation")
            .unwrap();
        tokio::time::timeout(core::time::Duration::from_secs(1), &mut dispatch)
            .await
            .expect("blocked dispatch must stop after cancellation")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_release_is_handed_off_while_partition_dispatch_is_blocked() {
        let memory = PipelineMemory::new(16);
        let (mut client, request_rx) = test_client_with_requests();
        let (mut dispatch, _partition_rx) = blocked_partition_dispatch(&mut client, &memory).await;
        let mut terminal_failure = client.inner.terminal_failure.subscribe();

        let release_handed_off = Arc::new(Notify::new());
        let mut request_stream = RequestStream {
            rx: request_rx,
            release_handed_off: Arc::clone(&release_handed_off),
        };
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let transport = tokio::spawn(async move {
            while let Some(request) = request_stream.next().await {
                if let Some(migration_streaming_read_client_message::Request::Released(released)) =
                    request.request
                {
                    released_tx.send(released).unwrap();
                    return;
                }
            }
            panic!("request stream closed before Released was handed off");
        });

        let (data_tx, _data_rx) = mpsc::channel(1);
        let (read_credit_tx, _read_credit_rx) = mpsc::channel(1);
        let (server_tx, server_stream) = mock_response_stream();
        let response_task = tokio::spawn(run_response_stream(
            server_stream,
            ResponseLoopContext {
                inner: Arc::clone(&client.inner),
                pending_read_credit: Arc::new(StdMutex::new(None)),
                read_outstanding: Arc::new(AtomicBool::new(false)),
                source_counters: Arc::new(SourceCounters::new()),
                assigned: HashSet::from([7]),
                configured_topic: test_topic(),
                read_credit_tx,
                data_tx,
                benchmark_discard_before_decompression: false,
                release_handed_off,
                network_timeout: core::time::Duration::from_secs(1),
            },
        ));
        for response in [
            migration_streaming_read_server_message::Response::InitResponse(
                migration_streaming_read_server_message::InitResponse::default(),
            ),
            migration_streaming_read_server_message::Response::Assigned(assignment(
                "topic", "cluster",
            )),
            migration_streaming_read_server_message::Response::Release(release(
                "topic", "cluster", false,
            )),
        ] {
            server_tx.send(Ok(server_response(response))).unwrap();
        }

        let released = tokio::time::timeout(core::time::Duration::from_secs(1), released_rx)
            .await
            .expect("graceful release must reach the request transport")
            .unwrap();
        assert_eq!(released.partition, 7);
        assert_eq!(released.assign_id, 11);
        terminal_failure.changed().await.unwrap();
        let failure = terminal_failure
            .borrow()
            .clone()
            .expect("graceful release must stop the current session");
        assert_eq!(failure.kind, TerminalFailureKind::Retryable);
        assert!(failure.message.contains("gracefully released partition 7"));

        response_task.await.unwrap();
        transport.await.unwrap();
        tokio::time::timeout(core::time::Duration::from_secs(1), &mut dispatch)
            .await
            .expect("graceful release must cancel blocked dispatch")
            .unwrap();
    }

    #[tokio::test]
    async fn saturated_data_plane_does_not_delay_commit_acknowledgement() {
        let memory = PipelineMemory::new(1024);
        let (data_tx, _data_rx) = mpsc::channel(1);
        enqueue_pending_data(
            &data_tx,
            PendingDataBatch {
                kind: PendingDataKind::Decode { parts: Vec::new() },
                raw_memory: memory.reserve(64).await,
            },
        )
        .unwrap();
        assert_eq!(memory.source_used(), 64);
        let failure = enqueue_pending_data(
            &data_tx,
            PendingDataBatch {
                kind: PendingDataKind::Decode { parts: Vec::new() },
                raw_memory: memory.reserve(64).await,
            },
        )
        .unwrap_err();
        assert_eq!(failure.kind, TerminalFailureKind::Fatal);
        assert!(failure.error.to_string().contains("read credit"));
        assert_eq!(
            memory.source_used(),
            64,
            "only the queued raw batch lease must remain accounted"
        );

        let client = test_client();
        let waiter = Arc::new(PendingCommit::new(1));
        client
            .inner
            .pending_commit_cookies
            .lock()
            .unwrap()
            .entry((11, 1))
            .or_default()
            .push_back(Arc::clone(&waiter));
        acknowledge_committed(
            client.inner.as_ref(),
            &migration_streaming_read_server_message::Committed {
                cookies: vec![cookie(1)],
                offset_ranges: Vec::new(),
            },
        )
        .unwrap();

        tokio::time::timeout(core::time::Duration::from_millis(50), waiter.wait())
            .await
            .expect("control-plane acknowledgement must not wait for data admission");
        assert_eq!(waiter.state.load(Ordering::Acquire), COMMIT_ACKNOWLEDGED);
    }

    #[tokio::test]
    async fn acknowledgement_wins_timeout_arbitration_atomically() {
        let client = test_client();
        let waiter = Arc::new(PendingCommit::new(1));
        client
            .inner
            .pending_commit_cookies
            .lock()
            .unwrap()
            .entry((11, 1))
            .or_default()
            .push_back(Arc::clone(&waiter));

        acknowledge_committed(
            client.inner.as_ref(),
            &migration_streaming_read_server_message::Committed {
                cookies: vec![cookie(1)],
                offset_ranges: Vec::new(),
            },
        )
        .unwrap();

        assert_eq!(
            abandon_pending_commit(client.inner.as_ref(), &waiter).unwrap(),
            AbandonCommitResult::Acknowledged
        );
        waiter.wait().await;
    }

    #[test]
    fn late_ack_consumes_an_abandoned_tombstone_not_a_newer_commit() {
        let client = test_client();
        let abandoned = Arc::new(PendingCommit::new(1));
        let newer = Arc::new(PendingCommit::new(1));
        {
            let mut pending = client.inner.pending_commit_cookies.lock().unwrap();
            let queue = pending.entry((11, 1)).or_default();
            queue.push_back(Arc::clone(&abandoned));
            queue.push_back(Arc::clone(&newer));
            drop(pending);
        }
        assert_eq!(
            abandon_pending_commit(client.inner.as_ref(), &abandoned).unwrap(),
            AbandonCommitResult::Abandoned
        );

        let committed = migration_streaming_read_server_message::Committed {
            cookies: vec![cookie(1)],
            offset_ranges: Vec::new(),
        };
        acknowledge_committed(client.inner.as_ref(), &committed).unwrap();
        assert_eq!(abandoned.state.load(Ordering::Acquire), COMMIT_ABANDONED);
        assert_eq!(newer.state.load(Ordering::Acquire), COMMIT_WAITING);

        acknowledge_committed(client.inner.as_ref(), &committed).unwrap();
        assert_eq!(newer.state.load(Ordering::Acquire), COMMIT_ACKNOWLEDGED);
        assert!(client
            .inner
            .pending_commit_cookies
            .lock()
            .unwrap()
            .is_empty());
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
        let read_params = runtime.read_params.expect("read parameters");
        assert_eq!(read_params.max_read_messages_count, MAX_READ_MESSAGES_COUNT);
        assert_ne!(read_params.max_read_messages_count, 0);
        assert_eq!(read_params.max_read_size, MAX_READ_SIZE);
    }

    #[tokio::test]
    async fn read_preaccounts_raw_credit_even_under_transform_pressure() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let outstanding = AtomicBool::new(false);
        let pending_credit = StdMutex::new(None);
        let cancellation = CancellationToken::new();
        let memory = PipelineMemory::new(100);
        let transform = memory.reserve_transform(100);

        send_read_request_with_credit(
            &memory,
            200,
            &sender,
            &outstanding,
            &pending_credit,
            &cancellation,
        )
        .await
        .unwrap();
        assert!(outstanding.load(Ordering::Acquire));
        assert_eq!(memory.source_used(), 200);
        assert_eq!(memory.transform_used(), 100);
        assert_eq!(
            pending_credit.lock().unwrap().as_ref().unwrap().bytes(),
            200
        );
        assert!(matches!(
            receiver.try_recv().unwrap().request,
            Some(migration_streaming_read_client_message::Request::Read(_))
        ));

        let lease = consume_read_credit(&outstanding, &pending_credit).unwrap();
        assert_eq!(lease.bytes(), 200);
        assert!(pending_credit.lock().unwrap().is_none());
        drop(lease);
        assert_eq!(memory.used(), 100);
        drop(transform);
        assert_eq!(memory.used(), 0);
        assert!(consume_read_credit(&outstanding, &pending_credit)
            .unwrap_err()
            .to_string()
            .contains("without an outstanding Read"));
    }

    #[tokio::test]
    async fn waiting_read_credit_is_cancelled_without_sending_read() {
        let memory = PipelineMemory::new(100);
        let active = memory.reserve_progress_source(10).await;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let outstanding = AtomicBool::new(false);
        let pending_credit = StdMutex::new(None);
        let cancellation = CancellationToken::new();
        let mut sending = Box::pin(send_read_request_with_credit(
            &memory,
            20,
            &sender,
            &outstanding,
            &pending_credit,
            &cancellation,
        ));
        assert!(
            tokio::time::timeout(core::time::Duration::from_millis(20), &mut sending)
                .await
                .is_err()
        );
        cancellation.cancel();

        let error = tokio::time::timeout(core::time::Duration::from_millis(50), sending)
            .await
            .expect("cancellation must stop progress-credit acquisition")
            .unwrap_err();

        assert!(error.to_string().contains("cancelled"));
        assert!(receiver.try_recv().is_err());
        assert!(!outstanding.load(Ordering::Acquire));
        assert!(pending_credit.lock().unwrap().is_none());
        drop(active);
    }

    #[tokio::test]
    async fn cancellation_stops_waiting_for_an_inflight_blocking_decoder() {
        let cancellation = CancellationToken::new();
        let memory = PipelineMemory::new(16);
        let reservation = memory.reserve(8).await;
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let slot = Arc::clone(&slots).acquire_owned().await.unwrap();
        let gate = Arc::new((StdMutex::new(false), std::sync::Condvar::new()));
        let worker_gate = Arc::clone(&gate);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _reservation = reservation;
            let _slot = slot;
            started_tx.send(()).unwrap();
            let (lock, changed) = worker_gate.as_ref();
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            drop(released);
            finished_tx.send(()).unwrap();
            42
        });
        started_rx.await.unwrap();

        let mut waiting = Box::pin(join_decode_or_cancel(&cancellation, worker));
        assert!(
            tokio::time::timeout(core::time::Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        cancellation.cancel();
        let (lock, changed) = gate.as_ref();
        *lock.lock().unwrap() = true;
        changed.notify_all();
        assert!(
            tokio::time::timeout(core::time::Duration::from_secs(1), waiting)
                .await
                .expect("cancellation must stop awaiting the blocking decoder")
                .is_none()
        );
        tokio::time::timeout(core::time::Duration::from_secs(1), finished_rx)
            .await
            .expect("decoder must finish after its bounded operation is released")
            .unwrap();
        assert_eq!(memory.used(), 0);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancellation_stops_decoding_after_a_bounded_read_chunk() {
        struct GatedReader {
            started: Option<tokio::sync::oneshot::Sender<()>>,
            gate: Arc<(StdMutex<bool>, std::sync::Condvar)>,
            calls: Arc<AtomicUsize>,
            max_read_size: Arc<AtomicUsize>,
        }

        impl std::io::Read for GatedReader {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                self.calls.fetch_add(1, Ordering::AcqRel);
                self.max_read_size.fetch_max(output.len(), Ordering::AcqRel);
                if let Some(started) = self.started.take() {
                    started.send(()).unwrap();
                    let (lock, changed) = self.gate.as_ref();
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = changed.wait(released).unwrap();
                    }
                    drop(released);
                }
                output.fill(0);
                Ok(output.len())
            }
        }

        let cancellation = CancellationToken::new();
        let worker_token = cancellation.clone();
        let gate = Arc::new((StdMutex::new(false), std::sync::Condvar::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let max_read_size = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking({
            let gate = Arc::clone(&gate);
            let calls = Arc::clone(&calls);
            let max_read_size = Arc::clone(&max_read_size);
            move || {
                read_exact_decoded(
                    GatedReader {
                        started: Some(started_tx),
                        gate,
                        calls,
                        max_read_size,
                    },
                    2 * 64 * 1024,
                    &worker_token,
                )
            }
        });
        started_rx.await.unwrap();
        cancellation.cancel();
        let (lock, changed) = gate.as_ref();
        *lock.lock().unwrap() = true;
        changed.notify_all();

        let error = tokio::time::timeout(core::time::Duration::from_secs(1), worker)
            .await
            .expect("decoder must observe cancellation after its current bounded read")
            .unwrap()
            .unwrap_err();
        assert!(error.downcast_ref::<DecodeCancelled>().is_some());
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(max_read_size.load(Ordering::Acquire) <= 64 * 1024);
    }

    #[test]
    fn read_credit_allows_exactly_one_outstanding_data_request() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let outstanding = AtomicBool::new(false);

        send_read_request(&sender, &outstanding).unwrap();
        assert!(send_read_request(&sender, &outstanding)
            .unwrap_err()
            .to_string()
            .contains("overlapping Read"));
        assert!(matches!(
            receiver.try_recv().unwrap().request,
            Some(migration_streaming_read_client_message::Request::Read(_))
        ));
    }

    #[test]
    fn server_message_count_cannot_exceed_the_advertised_credit() {
        validate_message_count(u64::from(MAX_READ_MESSAGES_COUNT)).unwrap();
        let error = validate_message_count(u64::from(MAX_READ_MESSAGES_COUNT) + 1).unwrap_err();
        assert!(error.to_string().contains("exceeding requested limit"));
    }

    #[test]
    fn raw_batch_validation_enforces_wire_and_repeated_field_limits() {
        let credit = raw_read_credit_bytes(1).unwrap();

        let mut oversized = data_batch_with_empty_messages(1);
        oversized.partition_data[0].batches[0].message_data[0].data =
            vec![0; usize::try_from(MAX_READ_SIZE).unwrap() + 1];
        let error = validate_raw_data_batch(&oversized, 1, credit).unwrap_err();
        assert!(error.to_string().contains("raw payload size"));

        let too_many_messages =
            data_batch_with_empty_messages(usize::try_from(MAX_READ_MESSAGES_COUNT).unwrap() + 1);
        let error = validate_raw_data_batch(&too_many_messages, 1, credit).unwrap_err();
        assert!(error.to_string().contains("messages"));

        let mut too_many_batches = data_batch_with_empty_messages(0);
        too_many_batches.partition_data[0].batches = vec![
            migration_streaming_read_server_message::data_batch::Batch::default();
            MAX_READ_BATCH_COUNT + 1
        ];
        let error = validate_raw_data_batch(&too_many_batches, 1, credit).unwrap_err();
        assert!(error.to_string().contains("batches"));
    }

    #[test]
    fn raw_batch_memory_includes_fixed_message_and_partition_metadata() {
        let message_count = 2_000;
        let batch = data_batch_with_empty_messages(message_count);
        let credit = raw_read_credit_bytes(1).unwrap();
        let retained = validate_raw_data_batch(&batch, 1, credit).unwrap();
        let fixed_messages = message_count
            * core::mem::size_of::<migration_streaming_read_server_message::data_batch::MessageData>(
            );
        assert!(retained >= fixed_messages);
        assert!(retained > prost::Message::encoded_len(&batch));

        let (kind, _, _) = prepare_data_batch(batch, &active_assignment(), false).unwrap();
        let pending = pending_raw_bytes(&kind).unwrap();
        assert!(pending >= message_count * core::mem::size_of::<RawMsg>());
        assert!(pending <= retained);
    }

    #[test]
    fn non_grpc_schemes_are_rejected_instead_of_using_the_cleartext_transport() {
        let missing_scheme = parse_endpoint("example.test:2135").unwrap_err();
        assert!(missing_scheme.to_string().contains("grpc:// scheme"));
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
    fn discovery_filters_tls_and_builds_a_stable_failover_order() {
        let endpoints = vec![
            discovery_endpoint("b.test", 2135, 0.5, false),
            discovery_endpoint("tls.test", 2135, 0.0, true),
            discovery_endpoint("a.test", 2135, 0.1, false),
            discovery_endpoint("a.test", 2135, 0.9, false),
            discovery_endpoint("", 2135, 0.0, false),
            discovery_endpoint("invalid-port.test", 70_000, 0.0, false),
        ];
        let forward = ordered_plaintext_proxies(endpoints.clone(), 7).unwrap();
        let reverse = ordered_plaintext_proxies(endpoints.into_iter().rev().collect(), 7).unwrap();

        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), 2);
        assert!(forward.iter().any(|endpoint| endpoint == "a.test:2135"));
        assert!(forward.iter().any(|endpoint| endpoint == "b.test:2135"));
    }

    #[test]
    fn discovery_brackets_ipv6_literals() {
        let endpoints =
            ordered_plaintext_proxies(vec![discovery_endpoint("2001:db8::1", 2135, 0.0, false)], 7)
                .unwrap();
        assert_eq!(endpoints, vec!["[2001:db8::1]:2135"]);
        let uri = http_uri(&endpoints[0]).unwrap();
        assert_eq!(socket_address(&uri), "[2001:db8::1]:2135");
    }

    #[test]
    fn discovery_load_factor_biases_stable_partition_selection() {
        let mut low_load_primary = 0;
        let mut high_load_primary = 0;
        for partition_id in 0..256 {
            let order = ordered_plaintext_proxies(
                vec![
                    discovery_endpoint("low.test", 2135, 0.0, false),
                    discovery_endpoint("high.test", 2135, 20.0, false),
                ],
                partition_id,
            )
            .unwrap();
            if order[0] == "low.test:2135" {
                low_load_primary += 1;
            } else {
                high_load_primary += 1;
            }
        }

        assert!(
            low_load_primary > high_load_primary * 5,
            "low={low_load_primary}, high={high_load_primary}"
        );
    }

    #[test]
    fn discovery_rejects_a_set_without_plaintext_endpoints() {
        let error =
            ordered_plaintext_proxies(vec![discovery_endpoint("tls.test", 2135, 0.0, true)], 7)
                .unwrap_err();
        assert!(error.to_string().contains("no usable plaintext"));
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
    fn decoded_accounting_includes_fixed_metadata_and_caps_the_batch() {
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
        assert_eq!(
            decoded_batch_retained_bytes(&raw).unwrap(),
            DECODED_PART_METADATA_BYTES
                + DECODED_MESSAGE_METADATA_BYTES
                + OUTPUT_MESSAGE_METADATA_BYTES
                + 3
        );

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
        let error = decoded_batch_retained_bytes(&compressed).unwrap_err();
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

    #[test]
    fn tonic_statuses_have_explicit_retry_disposition() {
        use tonic::Code;

        for code in [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
            Code::Aborted,
        ] {
            let failure = tonic_failure("test", &tonic::Status::new(code, "injected"));
            assert_eq!(failure.kind, TerminalFailureKind::Retryable, "{code:?}");
        }
        for code in [
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::InvalidArgument,
            Code::Unimplemented,
            Code::DataLoss,
        ] {
            let failure = tonic_failure("test", &tonic::Status::new(code, "injected"));
            assert_eq!(failure.kind, TerminalFailureKind::Fatal, "{code:?}");
        }
    }

    #[test]
    fn fatal_tonic_status_survives_pre_session_stages() {
        let error = surface_session_failure(tonic_failure(
            "stream open",
            &tonic::Status::unauthenticated("bad token"),
        ));
        assert!(error
            .downcast_ref::<PipelineFailure>()
            .is_some_and(|failure| !failure.is_retryable()));

        let retryable = surface_session_failure(tonic_failure(
            "stream open",
            &tonic::Status::unavailable("proxy down"),
        ));
        assert!(retryable.downcast_ref::<PipelineFailure>().is_none());
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

    #[test]
    fn raw_batches_are_recognized_without_using_the_decompression_pool() {
        let raw = RawPart {
            pid: 7,
            cookie: None,
            msgs: vec![RawMsg {
                data: vec![1],
                codec: 1,
                uncompressed_size: 1,
                offset: 1,
                write_timestamp_ms: 1,
            }],
        };
        assert!(batch_uses_only_raw_codec(&[raw]));

        let compressed = RawPart {
            pid: 7,
            cookie: None,
            msgs: vec![RawMsg {
                data: vec![1],
                codec: 2,
                uncompressed_size: 1,
                offset: 1,
                write_timestamp_ms: 1,
            }],
        };
        assert!(!batch_uses_only_raw_codec(&[compressed]));
    }

    #[test]
    fn raw_overlap_counts_payload_only_once() {
        let raw = RawPart {
            pid: 7,
            cookie: None,
            msgs: vec![RawMsg {
                data: vec![1, 2, 3],
                codec: 1,
                uncompressed_size: 3,
                offset: 1,
                write_timestamp_ms: 1,
            }],
        };
        let retained = decoded_batch_retained_bytes(core::slice::from_ref(&raw)).unwrap();
        let additional = decoded_batch_additional_bytes(&[raw]).unwrap();
        assert_eq!(retained - additional, 3);
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
        tx.send(DecodedPart {
            pid: 8,
            cookie: Some(cookie(1)),
            msgs: vec![],
            memory: memory.reserve(1).await,
        })
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
        let peak = decoded_part_retained_bytes(2) + 6;
        let retained = peak - DECODED_PART_METADATA_BYTES - 2 * DECODED_MESSAGE_METADATA_BYTES;
        let reservation = memory.reserve(peak).await;
        let (tx, rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        tx.send(DecodedPart {
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
        })
        .await
        .unwrap();

        let batch = source.read_batch().await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.memory.len(), 1);
        assert_eq!(batch.memory[0].bytes(), retained);
        assert_eq!(memory.source_used(), retained);
    }

    #[tokio::test]
    async fn discarded_batch_emits_a_marker_without_messages() {
        let memory = PipelineMemory::new(16);
        let (tx, rx) = mpsc::channel(1);
        let mut source = PqV1Source::new(test_client(), rx, 7, test_topic());
        tx.send(DecodedPart {
            pid: 7,
            cookie: Some(cookie(3)),
            msgs: vec![],
            memory: memory.reserve(1).await,
        })
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
    async fn decode_retains_the_preaccounted_source_reservation() {
        let memory = PipelineMemory::new(64);
        let transform = memory.reserve_transform(64);
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
        let reservation = memory.reserve_progress_source(32).await;
        let decoded_bytes = decoded_batch_retained_bytes(&parts).unwrap();
        reservation
            .grow_progress_source_to(reservation.bytes() + decoded_bytes)
            .unwrap();
        assert!(memory.used() > memory.limit());

        let decoded = decode_parts(parts, &reservation, &SourceCounters::new()).unwrap();
        let retained = decoded_part_retained_bytes(1) + 3;
        assert_eq!(decoded[0].memory.bytes(), retained);
        assert_eq!(memory.used(), 64 + retained);
        assert_eq!(memory.source_used(), retained);
        assert_eq!(memory.transform_used(), 64);

        let decoded_lease = decoded[0].memory.clone();
        drop(reservation);
        drop(decoded);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let outstanding = AtomicBool::new(false);
        let pending_credit = StdMutex::new(None);
        let cancellation = CancellationToken::new();
        let mut next = Box::pin(send_read_request_with_credit(
            &memory,
            16,
            &sender,
            &outstanding,
            &pending_credit,
            &cancellation,
        ));
        assert!(
            tokio::time::timeout(core::time::Duration::from_millis(20), &mut next)
                .await
                .is_err()
        );
        assert!(receiver.try_recv().is_err());
        drop(decoded_lease);
        next.await.unwrap();
        assert!(matches!(
            receiver.try_recv().unwrap().request,
            Some(migration_streaming_read_client_message::Request::Read(_))
        ));
        drop(consume_read_credit(&outstanding, &pending_credit).unwrap());
        drop(transform);
        assert_eq!(memory.used(), 0);
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
    async fn source_groups_commit_markers_into_one_request_without_draining_data_batches() {
        let memory = PipelineMemory::new(1024);
        let (client, mut request_rx) = test_client_with_requests();
        let (tx, rx) = mpsc::channel(2);
        let mut source = PqV1Source::new(client.clone(), rx, 7, test_topic());
        for partition_cookie in [1, 2] {
            tx.send(DecodedPart {
                pid: 7,
                cookie: Some(cookie(partition_cookie)),
                msgs: vec![DecodedMessage {
                    data: Bytes::from_static(b"message"),
                    offset: partition_cookie,
                    write_timestamp_ms: 10 + partition_cookie,
                }],
                memory: memory.reserve(7).await,
            })
            .await
            .unwrap();
        }

        let mut first_batch = source.read_batch().await.unwrap();
        let first_marker = first_batch
            .commit_marker
            .take()
            .expect("first commit marker");
        let mut second_batch = source.read_batch().await.unwrap();
        let second_marker = second_batch
            .commit_marker
            .take()
            .expect("second commit marker");

        let markers = [
            first_marker,
            second_marker,
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
}
