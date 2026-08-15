use alloc::sync::Arc;
use std::collections::VecDeque;
use std::io::Read as _;
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_read_message::{
    commit_offset_request::PartitionCommitOffset, init_request::TopicReadSettings,
    read_response::Batch, CommitOffsetRequest, FromClient, FromServer, InitRequest, ReadRequest,
    StartPartitionSessionResponse, StopPartitionSessionResponse,
};
use ydb_grpc::ydb_proto::topic::{Codec, OffsetsRange};

use super::{connect_client, set_ydb_headers, YdbTopicSourceConfig};
use crate::metrics::SourceCounters;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::{CommitMarker, Source};
use crate::pipeline::PipelineFailure;
use crate::types::message::{Message, MessageMeta, SourceBatch};

const OUTGOING_CHANNEL_CAPACITY: usize = 8;
const MAX_DECOMPRESSED_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const MAX_DECOMPRESSED_BATCH_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
struct YdbTopicCommitMarker {
    partition_id: i64,
    partition_session_id: i64,
    ranges: Vec<OffsetsRange>,
}

enum SessionEvent {
    Batch(SourceBatch),
    CommitAck {
        partition_session_id: i64,
        committed_offset: i64,
    },
    Continue,
}

pub(super) struct YdbTopicSource {
    outgoing: mpsc::Sender<FromClient>,
    incoming: tonic::Streaming<FromServer>,
    buffered_batches: VecDeque<SourceBatch>,
    partition_id: i64,
    partition_session_id: Option<i64>,
    topic_path: Arc<str>,
    cancellation: CancellationToken,
    memory: PipelineMemory,
    counters: Arc<SourceCounters>,
    pending_credit: i64,
}

impl YdbTopicSource {
    pub(super) async fn connect(
        config: &YdbTopicSourceConfig,
        token: Arc<str>,
        partition_id: i64,
        counters: Arc<SourceCounters>,
        cancellation: CancellationToken,
        memory: PipelineMemory,
    ) -> anyhow::Result<Self> {
        let timeout = super::NETWORK_TIMEOUT;
        let (mut client, _) =
            connect_client(&config.host, config.port, timeout, &cancellation).await?;
        client = client.max_decoding_message_size(MAX_DECOMPRESSED_BATCH_BYTES);
        let (outgoing, receiver) = mpsc::channel(OUTGOING_CHANNEL_CAPACITY);
        let mut request = Request::new(ReceiverStream::new(receiver));
        set_ydb_headers(request.metadata_mut(), token.as_ref())?;
        outgoing
            .send(init_message(config, partition_id))
            .await
            .map_err(|_| anyhow!("YDB Topic request stream closed before init"))?;
        let incoming = tokio::time::timeout(timeout, client.stream_read(request))
            .await
            .map_err(|_| {
                anyhow!(
                    "YDB Topic StreamRead timed out after {} ms",
                    super::NETWORK_TIMEOUT.as_millis()
                )
            })??
            .into_inner();
        let mut source = Self {
            outgoing,
            incoming,
            buffered_batches: VecDeque::new(),
            partition_id,
            partition_session_id: None,
            topic_path: Arc::from(config.topic_path.as_str()),
            cancellation,
            memory,
            counters,
            pending_credit: 0,
        };
        source.await_init(timeout).await?;
        source
            .send(ClientMessage::ReadRequest(ReadRequest {
                bytes_size: i64::try_from(config.read_buffer_bytes)
                    .map_err(|_| anyhow!("ydb_topic.read_buffer_bytes exceeds i64"))?,
            }))
            .await?;
        Ok(source)
    }

    async fn await_init(&mut self, timeout: core::time::Duration) -> anyhow::Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                let response = self.next_response().await?;
                validate_status(&response)?;
                match response.server_message {
                    Some(ServerMessage::InitResponse(response)) => {
                        anyhow::ensure!(
                            !response.session_id.is_empty(),
                            "YDB Topic returned an empty read session id"
                        );
                        return Ok(());
                    }
                    Some(message) => {
                        let event = self.process_message(message).await?;
                        anyhow::ensure!(
                            !matches!(event, SessionEvent::Batch(_)),
                            "YDB Topic sent data before InitResponse"
                        );
                    }
                    None => anyhow::bail!("YDB Topic init response has no server message"),
                }
            }
        })
        .await
        .map_err(|_| {
            anyhow!(
                "YDB Topic init response timed out after {} ms",
                timeout.as_millis()
            )
        })?
    }

    async fn send(&self, message: ClientMessage) -> anyhow::Result<()> {
        let message = FromClient {
            client_message: Some(message),
        };
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                Err(PipelineFailure::retryable(anyhow!("YDB Topic session cancelled")).into())
            }
            result = self.outgoing.send(message) => {
                result.map_err(|_| PipelineFailure::retryable(anyhow!("YDB Topic request stream closed")).into())
            }
        }
    }

    async fn next_response(&mut self) -> anyhow::Result<FromServer> {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                return Err(PipelineFailure::retryable(anyhow!("YDB Topic session cancelled")).into());
            }
            response = self.incoming.message() => response,
        };
        self.counters.add_response_wait(started.elapsed());
        match result {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(PipelineFailure::retryable(anyhow!(
                "YDB Topic StreamRead closed unexpectedly"
            ))
            .into()),
            Err(status) => Err(tonic_failure("StreamRead receive", &status)),
        }
    }

    async fn process_message(&mut self, message: ServerMessage) -> anyhow::Result<SessionEvent> {
        match message {
            ServerMessage::InitResponse(_) => {
                Err(fatal(anyhow!("YDB Topic sent duplicate InitResponse")))
            }
            ServerMessage::StartPartitionSessionRequest(request) => {
                let session = request.partition_session.ok_or_else(|| {
                    fatal(anyhow!("YDB Topic start request has no partition session"))
                })?;
                if session.partition_id != self.partition_id
                    || session.path != self.topic_path.as_ref()
                {
                    return Err(fatal(anyhow!(
                        "YDB Topic assigned unexpected partition: expected {}:{}, got {}:{}",
                        self.topic_path,
                        self.partition_id,
                        session.path,
                        session.partition_id
                    )));
                }
                if let Some(current) = self.partition_session_id {
                    if current != session.partition_session_id {
                        return Err(PipelineFailure::retryable(anyhow!(
                            "YDB Topic replaced active partition session {current} with {}",
                            session.partition_session_id
                        ))
                        .into());
                    }
                }
                self.partition_session_id = Some(session.partition_session_id);
                self.send(ClientMessage::StartPartitionSessionResponse(
                    StartPartitionSessionResponse {
                        partition_session_id: session.partition_session_id,
                        read_offset: None,
                        commit_offset: None,
                    },
                ))
                .await?;
                Ok(SessionEvent::Continue)
            }
            ServerMessage::ReadResponse(response) => self.decode_response(response).await,
            ServerMessage::CommitOffsetResponse(response) => {
                let Some(committed) =
                    response
                        .partitions_committed_offsets
                        .into_iter()
                        .find(|committed| {
                            Some(committed.partition_session_id) == self.partition_session_id
                        })
                else {
                    return Err(fatal(anyhow!(
                        "YDB Topic commit response does not contain the active partition session"
                    )));
                };
                Ok(SessionEvent::CommitAck {
                    partition_session_id: committed.partition_session_id,
                    committed_offset: committed.committed_offset,
                })
            }
            ServerMessage::StopPartitionSessionRequest(request) => {
                self.send(ClientMessage::StopPartitionSessionResponse(
                    StopPartitionSessionResponse {
                        partition_session_id: request.partition_session_id,
                        graceful: request.graceful,
                    },
                ))
                .await?;
                Err(PipelineFailure::retryable(anyhow!(
                    "YDB Topic server stopped partition session {} (graceful={})",
                    request.partition_session_id,
                    request.graceful
                ))
                .into())
            }
            ServerMessage::EndPartitionSession(request) => {
                Err(PipelineFailure::retryable(anyhow!(
                    "YDB Topic partition session {} ended; child partitions: {:?}",
                    request.partition_session_id,
                    request.child_partition_ids
                ))
                .into())
            }
            ServerMessage::PartitionSessionStatusResponse(_)
            | ServerMessage::UpdateTokenResponse(_)
            | ServerMessage::UpdatePartitionSession(_) => Ok(SessionEvent::Continue),
        }
    }

    async fn decode_response(
        &mut self,
        response: ydb_grpc::ydb_proto::topic::stream_read_message::ReadResponse,
    ) -> anyhow::Result<SessionEvent> {
        anyhow::ensure!(
            response.bytes_size > 0,
            "YDB Topic ReadResponse.bytes_size must be positive"
        );
        self.pending_credit = self
            .pending_credit
            .checked_add(response.bytes_size)
            .ok_or_else(|| fatal(anyhow!("YDB Topic read credit overflow")))?;
        let session_id = self.partition_session_id.ok_or_else(|| {
            fatal(anyhow!(
                "YDB Topic sent data before assigning a partition session"
            ))
        })?;
        let mut batches = Vec::new();
        for partition_data in response.partition_data {
            if partition_data.partition_session_id != session_id {
                return Err(fatal(anyhow!(
                    "YDB Topic sent data for unexpected partition session {}",
                    partition_data.partition_session_id
                )));
            }
            batches.extend(partition_data.batches);
        }
        if batches.is_empty() {
            return Ok(SessionEvent::Continue);
        }

        let compressed_bytes = batches
            .iter()
            .flat_map(|batch| &batch.message_data)
            .try_fold(0usize, |total, message| {
                total.checked_add(message.data.len())
            })
            .ok_or_else(|| fatal(anyhow!("YDB Topic compressed batch size overflow")))?;
        let declared_bytes = batches
            .iter()
            .flat_map(|batch| &batch.message_data)
            .try_fold(0usize, |total, message| {
                let declared = usize::try_from(message.uncompressed_size).ok()?;
                total.checked_add(declared.max(message.data.len()))
            })
            .ok_or_else(|| fatal(anyhow!("YDB Topic has an invalid uncompressed batch size")))?;
        anyhow::ensure!(
            declared_bytes <= MAX_DECOMPRESSED_BATCH_BYTES,
            "YDB Topic declared batch size {declared_bytes} exceeds limit {MAX_DECOMPRESSED_BATCH_BYTES}"
        );
        let reservation = self
            .memory
            .reserve_progress_source(compressed_bytes.saturating_add(declared_bytes))
            .await;
        let topic = Arc::clone(&self.topic_path);
        let partition_id = self.partition_id;
        let started = Instant::now();
        let decoded =
            tokio::task::spawn_blocking(move || decode_batches(batches, &topic, partition_id))
                .await
                .map_err(|error| fatal(anyhow!("YDB Topic decoder task failed: {error}")))??;
        self.counters.add_decomp_busy(started.elapsed());
        let decompressed_bytes = decoded
            .messages
            .iter()
            .try_fold(0usize, |total, message| {
                total.checked_add(message.value.len())
            })
            .ok_or_else(|| fatal(anyhow!("YDB Topic decoded batch size overflow")))?;
        reservation.grow_progress_source_to(
            compressed_bytes
                .checked_add(decompressed_bytes)
                .ok_or_else(|| fatal(anyhow!("YDB Topic batch accounting overflow")))?,
        )?;
        let _ = reservation.shrink_to(decompressed_bytes);
        self.counters
            .add_messages(u64::try_from(decoded.messages.len()).unwrap_or(u64::MAX));
        self.counters
            .add_compressed_bytes(u64::try_from(compressed_bytes).unwrap_or(u64::MAX));
        self.counters
            .add_decompressed_bytes(u64::try_from(decompressed_bytes).unwrap_or(u64::MAX));
        let commit_marker = (!decoded.ranges.is_empty()).then(|| {
            CommitMarker::new(YdbTopicCommitMarker {
                partition_id,
                partition_session_id: session_id,
                ranges: decoded.ranges,
            })
        });
        Ok(SessionEvent::Batch(SourceBatch::Raw {
            messages: decoded.messages,
            commit_marker,
            memory: vec![reservation],
        }))
    }

    async fn receive_event(&mut self) -> anyhow::Result<SessionEvent> {
        let response = self.next_response().await?;
        validate_status(&response)?;
        let message = response
            .server_message
            .ok_or_else(|| fatal(anyhow!("YDB Topic response has no server message")))?;
        self.process_message(message).await
    }

    async fn replenish_credit(&mut self) -> anyhow::Result<()> {
        if self.pending_credit <= 0 {
            return Ok(());
        }
        let credit = core::mem::take(&mut self.pending_credit);
        self.send(ClientMessage::ReadRequest(ReadRequest {
            bytes_size: credit,
        }))
        .await
    }
}

impl Source for YdbTopicSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        Box::pin(async move {
            if let Some(batch) = self.buffered_batches.pop_front() {
                return Ok(batch);
            }
            self.replenish_credit().await?;
            loop {
                match self.receive_event().await? {
                    SessionEvent::Batch(batch) => return Ok(batch),
                    SessionEvent::CommitAck { .. } | SessionEvent::Continue => {}
                }
            }
        })
    }

    fn commit_offsets<'context>(
        &'context mut self,
        markers: &'context [CommitMarker],
    ) -> BoxFuture<'context, anyhow::Result<()>> {
        Box::pin(async move {
            if markers.is_empty() {
                return Ok(());
            }
            let session_id = self.partition_session_id.ok_or_else(|| {
                fatal(anyhow!(
                    "YDB Topic has no active partition session for commit"
                ))
            })?;
            let mut ranges = Vec::new();
            for marker in markers {
                let marker = marker
                    .downcast_ref::<YdbTopicCommitMarker>()
                    .ok_or_else(|| fatal(anyhow!("Invalid YDB Topic commit marker")))?;
                if marker.partition_id != self.partition_id
                    || marker.partition_session_id != session_id
                {
                    return Err(fatal(anyhow!("YDB Topic commit marker session mismatch")));
                }
                ranges.extend(marker.ranges.iter().copied());
            }
            let ranges = coalesce_ranges(ranges)?;
            let target = ranges
                .iter()
                .map(|range| range.end)
                .max()
                .ok_or_else(|| fatal(anyhow!("YDB Topic commit marker has no offsets")))?;
            self.send(ClientMessage::CommitOffsetRequest(CommitOffsetRequest {
                commit_offsets: vec![PartitionCommitOffset {
                    partition_session_id: session_id,
                    offsets: ranges,
                }],
            }))
            .await?;
            loop {
                match self.receive_event().await? {
                    SessionEvent::CommitAck {
                        partition_session_id,
                        committed_offset,
                    } if partition_session_id == session_id && committed_offset >= target => {
                        return Ok(());
                    }
                    SessionEvent::Batch(batch) => self.buffered_batches.push_back(batch),
                    SessionEvent::CommitAck { .. } | SessionEvent::Continue => {}
                }
            }
        })
    }
}

fn init_message(config: &YdbTopicSourceConfig, partition_id: i64) -> FromClient {
    FromClient {
        client_message: Some(ClientMessage::InitRequest(InitRequest {
            topics_read_settings: vec![TopicReadSettings {
                path: config.topic_path.clone(),
                partition_ids: vec![partition_id],
                max_lag: None,
                read_from: None,
            }],
            consumer: config.consumer_name.clone(),
            reader_name: "transferia-rust".to_owned(),
            direct_read: false,
            auto_partitioning_support: false,
            partition_max_in_flight_bytes: config.read_buffer_bytes as u64,
        })),
    }
}

struct DecodedBatch {
    messages: Vec<Message>,
    ranges: Vec<OffsetsRange>,
}

fn decode_batches(
    batches: Vec<Batch>,
    topic: &Arc<str>,
    partition_id: i64,
) -> anyhow::Result<DecodedBatch> {
    let message_capacity = batches.iter().map(|batch| batch.message_data.len()).sum();
    let mut messages = Vec::with_capacity(message_capacity);
    let mut ranges = Vec::with_capacity(message_capacity);
    let mut total_bytes = 0usize;
    for batch in batches {
        let codec = Codec::try_from(batch.codec)
            .map_err(|_| fatal(anyhow!("YDB Topic returned unknown codec {}", batch.codec)))?;
        let written_at_ms = batch.written_at.map(timestamp_millis).transpose()?;
        for message in batch.message_data {
            anyhow::ensure!(message.offset >= 0, "YDB Topic returned negative offset");
            let value = decode_message(codec, message.data)?;
            total_bytes = total_bytes
                .checked_add(value.len())
                .ok_or_else(|| fatal(anyhow!("YDB Topic decoded batch size overflow")))?;
            if total_bytes > MAX_DECOMPRESSED_BATCH_BYTES {
                return Err(fatal(anyhow!(
                    "YDB Topic decoded batch exceeds {MAX_DECOMPRESSED_BATCH_BYTES} bytes"
                )));
            }
            ranges.push(OffsetsRange {
                start: message.offset,
                end: message
                    .offset
                    .checked_add(1)
                    .ok_or_else(|| fatal(anyhow!("YDB Topic offset overflow")))?,
            });
            messages.push(Message {
                value,
                meta: MessageMeta {
                    topic: Some(Arc::clone(topic)),
                    partition: Some(partition_id),
                    offset: Some(message.offset),
                    write_timestamp_ms: written_at_ms,
                },
            });
        }
    }
    Ok(DecodedBatch {
        messages,
        ranges: coalesce_ranges(ranges)?,
    })
}

pub(super) fn decode_message(codec: Codec, data: Vec<u8>) -> anyhow::Result<Bytes> {
    match codec {
        Codec::Raw => Ok(Bytes::from(data)),
        Codec::Gzip => read_bounded(flate2::read::GzDecoder::new(data.as_slice()), "gzip"),
        Codec::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(data.as_slice())
                .map_err(|error| fatal(anyhow!("Invalid YDB Topic zstd payload: {error}")))?;
            read_bounded(decoder, "zstd")
        }
        Codec::Unspecified | Codec::Lzop | Codec::Custom => Err(fatal(anyhow!(
            "YDB Topic codec {} is not supported",
            codec.as_str_name()
        ))),
    }
}

fn read_bounded(mut reader: impl std::io::Read, codec: &str) -> anyhow::Result<Bytes> {
    let mut decoded = Vec::new();
    reader
        .by_ref()
        .take((MAX_DECOMPRESSED_MESSAGE_BYTES + 1) as u64)
        .read_to_end(&mut decoded)
        .map_err(|error| fatal(anyhow!("Invalid YDB Topic {codec} payload: {error}")))?;
    if decoded.len() > MAX_DECOMPRESSED_MESSAGE_BYTES {
        return Err(fatal(anyhow!(
            "YDB Topic decoded message exceeds {MAX_DECOMPRESSED_MESSAGE_BYTES} bytes"
        )));
    }
    Ok(Bytes::from(decoded))
}

fn timestamp_millis(
    timestamp: ydb_grpc::google_proto_workaround::protobuf::Timestamp,
) -> anyhow::Result<i64> {
    let nanos = i64::from(timestamp.nanos);
    anyhow::ensure!(
        (0..1_000_000_000).contains(&nanos),
        "YDB Topic timestamp has invalid nanos {}",
        timestamp.nanos
    );
    timestamp
        .seconds
        .checked_mul(1_000)
        .and_then(|millis| millis.checked_add(nanos / 1_000_000))
        .ok_or_else(|| fatal(anyhow!("YDB Topic timestamp overflows milliseconds")))
}

pub(super) fn coalesce_ranges(mut ranges: Vec<OffsetsRange>) -> anyhow::Result<Vec<OffsetsRange>> {
    for range in &ranges {
        anyhow::ensure!(
            range.start >= 0 && range.start < range.end,
            "Invalid YDB Topic commit range [{}, {})",
            range.start,
            range.end
        );
    }
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut coalesced: Vec<OffsetsRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = coalesced.last_mut() {
            if range.start <= last.end {
                last.end = last.end.max(range.end);
                continue;
            }
        }
        coalesced.push(range);
    }
    Ok(coalesced)
}

fn validate_status(response: &FromServer) -> anyhow::Result<()> {
    if response.status == StatusCode::Unspecified as i32
        || response.status == StatusCode::Success as i32
    {
        return Ok(());
    }
    let status = StatusCode::try_from(response.status).ok();
    let name = status.map_or("UNKNOWN", |status| status.as_str_name());
    let issues = response
        .issues
        .iter()
        .map(|issue| issue.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    let error = anyhow!(
        "YDB Topic StreamRead failed: status={} ({name}), issues={issues}",
        response.status
    );
    if status.is_some_and(|status| {
        matches!(
            status,
            StatusCode::BadRequest
                | StatusCode::Unauthorized
                | StatusCode::SchemeError
                | StatusCode::PreconditionFailed
                | StatusCode::NotFound
                | StatusCode::Unsupported
        )
    }) {
        Err(fatal(error))
    } else {
        Err(PipelineFailure::retryable(error).into())
    }
}

fn tonic_failure(stage: &str, status: &tonic::Status) -> anyhow::Error {
    let fatal_code = matches!(
        status.code(),
        Code::InvalidArgument
            | Code::Unauthenticated
            | Code::PermissionDenied
            | Code::NotFound
            | Code::FailedPrecondition
            | Code::Unimplemented
    );
    let error = anyhow!("YDB Topic {stage} failed: {status}");
    if fatal_code {
        fatal(error)
    } else {
        PipelineFailure::retryable(error).into()
    }
}

fn fatal(error: anyhow::Error) -> anyhow::Error {
    PipelineFailure::fatal(error).into()
}
