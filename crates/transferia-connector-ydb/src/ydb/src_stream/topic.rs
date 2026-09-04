use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::codec::Streaming;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_read_message::{
    commit_offset_request::PartitionCommitOffset,
    commit_offset_response::PartitionCommittedOffset, init_request::TopicReadSettings,
    CommitOffsetRequest, FromClient, FromServer, InitRequest, ReadRequest,
    StartPartitionSessionResponse, StopPartitionSessionResponse,
};
use ydb_grpc::ydb_proto::topic::{Codec, OffsetsRange};

use super::super::transport::YdbClient;
use crate::metrics::SourceCounters;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::{MemoryReservation, PipelineMemory};
use transferia_core::source::CommitMarker;

const OUTGOING_CHANNEL_CAPACITY: usize = 8;
// One partition commit encodes two signed 64-bit varints plus their nested
// protobuf tags/lengths. Thirty-two bytes conservatively covers that wire copy.
const MAX_ENCODED_PARTITION_COMMIT_BYTES: usize = 32;
// `VecDeque` uses the standard non-byte-vector minimum allocation when it first
// grows; admitting four inline batches covers that first allocation.
const MIN_BUFFERED_BATCH_CAPACITY: usize = 4;
// Pinned to tonic 0.14's default prost codec settings. The response decoder
// grows in 8 KiB increments and the request encoder eagerly owns one 8 KiB
// buffer for the lifetime of this streaming RPC.
const TONIC_CODEC_BUFFER_CHUNK_BYTES: usize = 8 * 1024;
const TONIC_GRPC_HEADER_BYTES: usize = 5;
const TONIC_OUTGOING_CODEC_BUFFER_BYTES: usize = 8 * 1024;
/// Prost may turn a compact stream of nested empty/repeated fields into heap
/// nodes and minimum-capacity Vec/HashMap allocations. Every such node consumes
/// at least one encoded byte; 512 bytes covers four slots of the largest YDB
/// Topic response node plus payload/string storage. A unit test pins this proof
/// against the generated dependency layouts so an upgrade fails visibly.
pub(super) const MAX_DECODED_BYTES_PER_ENCODED_RESPONSE_BYTE: usize = 512;

#[derive(Debug)]
pub(super) struct TopicCommitMarker {
    partitions: Vec<PartitionCommitMarker>,
    retained_bytes: usize,
}

#[derive(Debug)]
struct PartitionCommitMarker {
    topic_path: Arc<str>,
    partition_id: i64,
    partition_session_id: i64,
    range: OffsetsRange,
}

#[derive(Clone, Debug)]
pub(super) struct TopicRecord {
    pub(super) topic_path: Arc<str>,
    pub(super) partition_id: i64,
    pub(super) offset: i64,
    pub(super) written_at_ms: i64,
    pub(super) payload: Vec<u8>,
}

pub(super) struct TopicBatch {
    pub(super) records: Vec<TopicRecord>,
    pub(super) commit_marker: CommitMarker,
    pub(super) memory: MemoryReservation,
}

struct PartitionSessionState {
    topic_path: Arc<str>,
    partition_id: i64,
    committed_offset: i64,
    commit_response_offset: i64,
    read_through: i64,
    pending_graceful_stop: Option<i64>,
    invalidated: bool,
}

struct ReadResponsePlan {
    record_count: usize,
    marker_partition_count: usize,
    retained_batch_bytes: usize,
}

enum SessionEvent {
    Batch(TopicBatch),
    CommitAck(Vec<PartitionCommittedOffset>),
    Continue,
}

pub(super) struct TopicSession {
    outgoing: mpsc::Sender<FromClient>,
    incoming: Streaming<FromServer>,
    configured_topics: HashMap<Arc<str>, i64>,
    partition_sessions: HashMap<i64, PartitionSessionState>,
    buffered_batches: VecDeque<TopicBatch>,
    cancellation: CancellationToken,
    counters: Arc<SourceCounters>,
    request_timeout: Duration,
    commit_timeout: Duration,
    max_message_bytes: usize,
    max_batch_bytes: usize,
    response_buffer_bytes: usize,
    transport_buffer_bytes: usize,
    credit_memory: MemoryReservation,
    retained_batch_bytes: usize,
    pending_release_batch_bytes: usize,
    pending_credit: i64,
    available_credit: i64,
    uncommitted_batches: usize,
}

impl TopicSession {
    pub(super) async fn connect(
        client: &YdbClient,
        topics: Vec<(String, i64)>,
        consumer: String,
        reader_name: String,
        read_buffer_bytes: usize,
        max_message_bytes: usize,
        max_batch_bytes: usize,
        max_response_bytes: usize,
        commit_timeout: Duration,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!topics.is_empty(), "YDB changefeed topic list must not be empty");
        let mut configured_topics = HashMap::with_capacity(topics.len());
        for (topic, partition_id) in &topics {
            anyhow::ensure!(
                *partition_id >= 0,
                "YDB replication configured a negative partition id for changefeed topic '{topic}'"
            );
            anyhow::ensure!(
                configured_topics
                    .insert(
                        Arc::<str>::from(canonical_topic_path(topic)),
                        *partition_id,
                    )
                    .is_none(),
                "YDB replication repeats changefeed topic '{topic}'"
            );
        }
        let configured_topic_bytes = configured_topics_heap_bytes(&configured_topics)?;
        let request_timeout = client.timeout();
        let (outgoing, receiver) = mpsc::channel(OUTGOING_CHANNEL_CAPACITY);
        let request = client.request(ReceiverStream::new(receiver));
        outgoing
            .send(FromClient {
                client_message: Some(ClientMessage::InitRequest(InitRequest {
                    topics_read_settings: topics
                        .iter()
                        .map(|(topic, partition_id)| TopicReadSettings {
                            path: topic.clone(),
                            partition_ids: vec![*partition_id],
                            max_lag: None,
                            read_from: None,
                        })
                        .collect(),
                    consumer,
                    reader_name,
                    direct_read: false,
                    auto_partitioning_support: false,
                    partition_max_in_flight_bytes: u64::try_from(read_buffer_bytes)?,
                })),
            })
            .await
            .map_err(|_| anyhow!("YDB Topic request stream closed before init"))?;
        let transport_buffer_bytes = tonic_codec_buffer_bytes(max_response_bytes)?;
        let initial_processing_bytes =
            response_processing_bytes(max_response_bytes, configured_topic_bytes)?;
        anyhow::ensure!(
            initial_processing_bytes <= memory.limit(),
            "YDB Topic response/decode admission requires {initial_processing_bytes} bytes, exceeding pipeline memory limit {}",
            memory.limit()
        );
        let credit_memory = memory
            .reserve_progress_source(initial_processing_bytes)
            .await;
        let mut topic_service = client
            .topic_service()
            .max_decoding_message_size(max_response_bytes);
        let open_stream = async {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    Err(DataPlaneFailure::retryable(anyhow!("YDB changefeed session cancelled")).into())
                }
                response = topic_service.stream_read(request) => {
                    response
                        .map(|response| response.into_inner())
                        .map_err(|status| tonic_failure("StreamRead open", &status))
                }
            }
        };
        let incoming = observe_external_request(
            "ydb",
            "open_changefeed_stream",
            tokio::time::timeout(request_timeout, open_stream),
        )
        .await
        .map_err(|_| {
            DataPlaneFailure::retryable(anyhow!(
                "YDB changefeed StreamRead timed out after {} ms",
                request_timeout.as_millis()
            ))
        })??;
        let mut session = Self {
            outgoing,
            incoming,
            configured_topics,
            partition_sessions: HashMap::new(),
            buffered_batches: VecDeque::new(),
            cancellation,
            counters,
            request_timeout,
            commit_timeout,
            max_message_bytes,
            max_batch_bytes,
            response_buffer_bytes: max_response_bytes,
            transport_buffer_bytes,
            credit_memory,
            retained_batch_bytes: 0,
            pending_release_batch_bytes: 0,
            pending_credit: 0,
            available_credit: 0,
            uncommitted_batches: 0,
        };
        session.await_init().await?;
        session.shrink_credit_memory_to_retained()?;
        session
            .send_read_credit(i64::try_from(read_buffer_bytes)?)
            .await?;
        Ok(session)
    }

    async fn await_init(&mut self) -> anyhow::Result<()> {
        tokio::time::timeout(self.request_timeout, async {
            loop {
                let response = self.next_response().await?;
                validate_status(&response)?;
                match response.server_message {
                    Some(ServerMessage::InitResponse(response)) => {
                        anyhow::ensure!(
                            !response.session_id.is_empty(),
                            "YDB Topic returned an empty changefeed read session id"
                        );
                        return Ok(());
                    }
                    Some(_) => anyhow::bail!(
                        "YDB Topic sent a non-init message before InitResponse"
                    ),
                    None => anyhow::bail!("YDB Topic init response has no server message"),
                }
            }
        })
        .await
        .map_err(|_| {
            DataPlaneFailure::retryable(anyhow!(
                "YDB changefeed init response timed out after {} ms",
                self.request_timeout.as_millis()
            ))
        })?
    }

    pub(super) async fn read_batch(&mut self) -> anyhow::Result<Option<TopicBatch>> {
        self.release_committed_batch_memory()?;
        self.remove_settled_invalidated_sessions();
        if let Some(batch) = self.buffered_batches.pop_front() {
            if self.buffered_batches.is_empty() {
                self.buffered_batches.shrink_to_fit();
                self.shrink_credit_memory_to_retained()?;
            }
            return Ok(Some(batch));
        }
        if self.uncommitted_batches > 0 {
            return Ok(None);
        }
        self.replenish_credit().await?;
        loop {
            match self.receive_event().await? {
                SessionEvent::Batch(batch) => return Ok(Some(batch)),
                SessionEvent::CommitAck(_) => {
                    return Err(fatal(anyhow!(
                        "YDB Topic sent an unsolicited commit acknowledgement"
                    )))
                }
                SessionEvent::Continue => {
                    self.remove_settled_invalidated_sessions();
                    self.replenish_credit().await?;
                }
            }
        }
    }

    pub(super) async fn commit_offsets(
        &mut self,
        markers: &[CommitMarker],
    ) -> anyhow::Result<()> {
        if markers.is_empty() {
            return Ok(());
        }
        anyhow::ensure!(
            markers.len() <= self.uncommitted_batches,
            "YDB Topic commit count exceeds read batches"
        );
        let commit_memory = self
            .credit_memory
            .reserve_source_companion(commit_request_admission_bytes(markers)?)
            .map_err(|error| fatal(error.context("admit YDB Topic commit request")))?;
        let (commit_offsets, targets, committed_bytes) =
            build_commit_request(markers, &self.partition_sessions)?;
        anyhow::ensure!(
            committed_bytes <= self.retained_batch_bytes,
            "YDB Topic commit memory exceeds retained batch bytes"
        );
        ensure_targets_live(&targets, &self.partition_sessions)?;
        let commit_timeout = self.commit_timeout;
        let operation = async {
            self.send(ClientMessage::CommitOffsetRequest(CommitOffsetRequest {
                commit_offsets,
            }))
            .await?;
            loop {
                if targets.iter().all(|(session_id, target)| {
                    self.partition_sessions
                        .get(session_id)
                        .is_some_and(|state| state.commit_response_offset >= *target)
                }) {
                    break;
                }
                ensure_targets_live(&targets, &self.partition_sessions)?;
                match self.receive_event().await? {
                    SessionEvent::Batch(batch) => self.push_buffered_batch(batch)?,
                    SessionEvent::CommitAck(offsets) => {
                        apply_commit_ack(&targets, &mut self.partition_sessions, offsets)?;
                        self.release_gracefully_stopped_sessions().await?;
                    }
                    SessionEvent::Continue => {}
                }
                ensure_ack_does_not_exceed_targets(&targets, &self.partition_sessions)?;
            }
            self.remove_settled_invalidated_sessions();
            self.uncommitted_batches = self
                .uncommitted_batches
                .checked_sub(markers.len())
                .ok_or_else(|| fatal(anyhow!("YDB Topic commit count exceeds read batches")))?;
            self.pending_release_batch_bytes = self
                .pending_release_batch_bytes
                .checked_add(committed_bytes)
                .ok_or_else(|| fatal(anyhow!("YDB Topic committed memory overflow")))?;
            Ok::<(), anyhow::Error>(())
        };
        observe_external_request(
            "ydb",
            "commit_changefeed_offsets",
            tokio::time::timeout(commit_timeout, operation),
        )
        .await
        .map_err(|_| {
            DataPlaneFailure::retryable(anyhow!(
                "YDB changefeed offset commit timed out after {} ms",
                commit_timeout.as_millis()
            ))
        })??;
        drop(commit_memory);
        Ok(())
    }

    async fn receive_event(&mut self) -> anyhow::Result<SessionEvent> {
        let retained_bytes = self
            .retained_batch_bytes
            .checked_add(self.session_state_heap_bytes()?)
            .ok_or_else(|| fatal(anyhow!("YDB Topic retained memory accounting overflow")))?;
        let processing_peak = response_processing_bytes(self.response_buffer_bytes, retained_bytes)?;
        self.credit_memory
            .grow_progress_source_to(processing_peak)
            .map_err(|error| fatal(error.context("admit YDB Topic response processing")))?;
        let response = self.next_response().await?;
        validate_status(&response)?;
        let message = response
            .server_message
            .ok_or_else(|| fatal(anyhow!("YDB Topic response has no server message")))?;
        let event = self.process_message(message).await?;
        self.shrink_credit_memory_to_retained()?;
        Ok(event)
    }

    async fn next_response(&mut self) -> anyhow::Result<FromServer> {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                return Err(DataPlaneFailure::retryable(anyhow!("YDB changefeed session cancelled")).into());
            }
            response = self.incoming.message() => response,
        };
        self.counters.add_response_wait(started.elapsed());
        match result {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(DataPlaneFailure::retryable(anyhow!(
                "YDB changefeed StreamRead closed unexpectedly"
            ))
            .into()),
            Err(status) => Err(tonic_failure("StreamRead receive", &status)),
        }
    }

    async fn send(&self, message: ClientMessage) -> anyhow::Result<()> {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                Err(DataPlaneFailure::retryable(anyhow!("YDB changefeed session cancelled")).into())
            }
            result = self.outgoing.send(FromClient { client_message: Some(message) }) => {
                result.map_err(|_| DataPlaneFailure::retryable(anyhow!("YDB Topic request stream closed")).into())
            }
        }
    }

    async fn send_read_credit(&mut self, bytes: i64) -> anyhow::Result<()> {
        anyhow::ensure!(bytes > 0, "YDB Topic read credit must be positive");
        self.send(ClientMessage::ReadRequest(ReadRequest { bytes_size: bytes }))
            .await?;
        self.available_credit = self
            .available_credit
            .checked_add(bytes)
            .ok_or_else(|| fatal(anyhow!("YDB Topic available read credit overflow")))?;
        Ok(())
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
                let canonical_path = canonical_topic_path(&session.path);
                validate_assigned_partition(
                    &self.configured_topics,
                    &self.partition_sessions,
                    canonical_path,
                    session.partition_id,
                )?;
                anyhow::ensure!(
                    request.committed_offset >= 0,
                    "YDB Topic returned negative committed offset"
                );
                let offsets = request.partition_offsets.as_ref().ok_or_else(|| {
                    fatal(anyhow!(
                        "YDB Topic omitted the retained offset range for {}:{}; expiration safety cannot be proven",
                        session.path,
                        session.partition_id
                    ))
                })?;
                validate_retained_offset(
                    request.committed_offset,
                    offsets,
                    &session.path,
                    session.partition_id,
                )?;
                let read_through = request.committed_offset.max(offsets.start);
                ensure_partition_session_id_is_new(
                    &self.partition_sessions,
                    session.partition_session_id,
                )?;
                self.partition_sessions.insert(
                    session.partition_session_id,
                    PartitionSessionState {
                        topic_path: Arc::from(canonical_path),
                        partition_id: session.partition_id,
                        committed_offset: request.committed_offset,
                        commit_response_offset: request.committed_offset,
                        read_through,
                        pending_graceful_stop: None,
                        invalidated: false,
                    },
                );
                self.send(ClientMessage::StartPartitionSessionResponse(
                    StartPartitionSessionResponse {
                        partition_session_id: session.partition_session_id,
                        read_offset: Some(read_through),
                        commit_offset: None,
                    },
                ))
                .await?;
                Ok(SessionEvent::Continue)
            }
            ServerMessage::ReadResponse(response) => self.decode_response(response).await,
            ServerMessage::CommitOffsetResponse(response) => {
                anyhow::ensure!(
                    !response.partitions_committed_offsets.is_empty(),
                    "YDB Topic commit response contains no partition offsets"
                );
                Ok(SessionEvent::CommitAck(
                    response.partitions_committed_offsets,
                ))
            }
            ServerMessage::StopPartitionSessionRequest(request) => {
                let Some(state) = self
                    .partition_sessions
                    .get_mut(&request.partition_session_id)
                else {
                    self.send(ClientMessage::StopPartitionSessionResponse(
                        StopPartitionSessionResponse {
                            partition_session_id: request.partition_session_id,
                            graceful: request.graceful,
                        },
                    ))
                    .await?;
                    return Ok(SessionEvent::Continue);
                };
                anyhow::ensure!(
                    request.committed_offset >= state.committed_offset
                        && request.committed_offset <= state.read_through,
                    "YDB Topic stop request committed offset {} is outside [{}, {}]",
                    request.committed_offset,
                    state.committed_offset,
                    state.read_through
                );
                if request.graceful {
                    anyhow::ensure!(
                        state.pending_graceful_stop.is_none(),
                        "YDB Topic repeated graceful stop for partition session {}",
                        request.partition_session_id
                    );
                    state.pending_graceful_stop = Some(request.committed_offset);
                    self.release_gracefully_stopped_sessions().await?;
                } else {
                    state.pending_graceful_stop = None;
                    state.invalidated = true;
                    self.send(ClientMessage::StopPartitionSessionResponse(
                        StopPartitionSessionResponse {
                            partition_session_id: request.partition_session_id,
                            graceful: false,
                        },
                    ))
                    .await?;
                }
                Ok(SessionEvent::Continue)
            }
            ServerMessage::EndPartitionSession(request) => {
                reject_fixed_partition_end(
                    &self.partition_sessions,
                    request.partition_session_id,
                )
            }
            ServerMessage::UpdatePartitionSession(update) => {
                anyhow::ensure!(
                    self.partition_sessions
                        .contains_key(&update.partition_session_id),
                    "YDB Topic updated unknown partition session {}",
                    update.partition_session_id
                );
                Ok(SessionEvent::Continue)
            }
            ServerMessage::PartitionSessionStatusResponse(_)
            | ServerMessage::UpdateTokenResponse(_) => Ok(SessionEvent::Continue),
        }
    }

    async fn release_gracefully_stopped_sessions(&mut self) -> anyhow::Result<()> {
        let session_ids = self
            .partition_sessions
            .iter()
            .filter_map(|(session_id, state)| {
                graceful_stop_ready(state).then_some(*session_id)
            })
            .collect::<Vec<_>>();
        for session_id in session_ids {
            self.send(ClientMessage::StopPartitionSessionResponse(
                StopPartitionSessionResponse {
                    partition_session_id: session_id,
                    graceful: true,
                },
            ))
            .await?;
            let state = self
                .partition_sessions
                .get_mut(&session_id)
                .ok_or_else(|| fatal(anyhow!("YDB Topic graceful-stop session disappeared")))?;
            state.pending_graceful_stop = None;
            state.invalidated = true;
        }
        Ok(())
    }

    async fn decode_response(
        &mut self,
        response: ydb_grpc::ydb_proto::topic::stream_read_message::ReadResponse,
    ) -> anyhow::Result<SessionEvent> {
        let (available_credit, pending_credit) = consume_read_response_credit(
            self.available_credit,
            self.pending_credit,
            response.bytes_size,
        )?;
        self.available_credit = available_credit;
        self.pending_credit = pending_credit;
        let decoded_response_bytes = read_response_heap_bytes(&response)?;
        let response_plan = read_response_plan(&response)?;
        self.grow_credit_memory_for_decoded_response(
            decoded_response_bytes,
            response_plan.retained_batch_bytes,
        )?;
        let mut records = Vec::with_capacity(response_plan.record_count);
        let mut commit_markers = Vec::with_capacity(response_plan.marker_partition_count);
        let mut raw_bytes = 0usize;
        for partition in response.partition_data {
            let state = self
                .partition_sessions
                .get_mut(&partition.partition_session_id)
                .ok_or_else(|| {
                    fatal(anyhow!(
                        "YDB Topic sent data for unknown partition session {}",
                        partition.partition_session_id
                    ))
                })?;
            anyhow::ensure!(
                !state.invalidated && state.pending_graceful_stop.is_none(),
                "YDB Topic sent data for a stopped partition session"
            );
            let range_start = state.read_through;
            for batch in partition.batches {
                let codec = Codec::try_from(batch.codec).map_err(|_| {
                    fatal(anyhow!("YDB Topic returned unknown codec {}", batch.codec))
                })?;
                anyhow::ensure!(
                    codec == Codec::Raw,
                    "YDB changefeed returned unsupported codec {}",
                    codec.as_str_name()
                );
                let written_at_ms = timestamp_millis(batch.written_at.as_ref())?;
                for message in batch.message_data {
                    anyhow::ensure!(message.offset >= 0, "YDB Topic returned negative offset");
                    anyhow::ensure!(
                        message.offset == state.read_through,
                        "YDB changefeed offset gap for {}:{}: expected {}, got {}",
                        state.topic_path,
                        state.partition_id,
                        state.read_through,
                        message.offset
                    );
                    anyhow::ensure!(
                        message.data.len() <= self.max_message_bytes,
                        "YDB changefeed message at {}:{}:{} is {} bytes, exceeding max_message_bytes={}",
                        state.topic_path,
                        state.partition_id,
                        message.offset,
                        message.data.len(),
                        self.max_message_bytes
                    );
                    raw_bytes = raw_bytes
                        .checked_add(message.data.len())
                        .ok_or_else(|| fatal(anyhow!("YDB changefeed batch size overflow")))?;
                    anyhow::ensure!(
                        raw_bytes <= self.max_batch_bytes,
                        "YDB changefeed batch exceeds max_batch_bytes={} before decoding",
                        self.max_batch_bytes
                    );
                    let end = message
                        .offset
                        .checked_add(1)
                        .ok_or_else(|| fatal(anyhow!("YDB changefeed offset overflow")))?;
                    records.push(TopicRecord {
                        topic_path: Arc::clone(&state.topic_path),
                        partition_id: state.partition_id,
                        offset: message.offset,
                        written_at_ms,
                        payload: message.data,
                    });
                    state.read_through = end;
                }
            }
            if state.read_through > range_start {
                commit_markers.push(PartitionCommitMarker {
                        topic_path: Arc::clone(&state.topic_path),
                        partition_id: state.partition_id,
                        partition_session_id: partition.partition_session_id,
                        range: OffsetsRange {
                            start: range_start,
                            end: state.read_through,
                        },
                    });
            }
        }
        if records.is_empty() {
            return Ok(SessionEvent::Continue);
        }
        let marker_partitions = commit_markers;
        let batch_retained_bytes = topic_batch_retained_bytes(
            &records,
            records.capacity(),
            marker_partitions.capacity(),
        )?;
        let retained_batch_bytes = self
            .retained_batch_bytes
            .checked_add(batch_retained_bytes)
            .ok_or_else(|| fatal(anyhow!("YDB Topic retained batch memory overflow")))?;
        let reservation = &self.credit_memory;
        self.retained_batch_bytes = retained_batch_bytes;
        self.counters
            .add_records(u64::try_from(records.len()).unwrap_or(u64::MAX));
        // Tonic exposes this response only after protobuf decoding, so no
        // transport-raw byte measurement is available at this boundary.
        self.counters
            .add_network_decoded_bytes(u64::try_from(raw_bytes).unwrap_or(u64::MAX));
        self.uncommitted_batches = self
            .uncommitted_batches
            .checked_add(1)
            .ok_or_else(|| fatal(anyhow!("YDB Topic uncommitted batch count overflow")))?;
        Ok(SessionEvent::Batch(TopicBatch {
            records,
            commit_marker: CommitMarker::new(TopicCommitMarker {
                partitions: marker_partitions,
                retained_bytes: batch_retained_bytes,
            }),
            memory: reservation.clone(),
        }))
    }

    fn release_committed_batch_memory(&mut self) -> anyhow::Result<()> {
        if self.pending_release_batch_bytes == 0 {
            return Ok(());
        }
        self.retained_batch_bytes = self
            .retained_batch_bytes
            .checked_sub(self.pending_release_batch_bytes)
            .ok_or_else(|| fatal(anyhow!("YDB Topic released more memory than it retained")))?;
        self.pending_release_batch_bytes = 0;
        self.shrink_credit_memory_to_retained()?;
        Ok(())
    }

    fn shrink_credit_memory_to_retained(&self) -> anyhow::Result<()> {
        let reservation = &self.credit_memory;
        let session_state_bytes = self.session_state_heap_bytes()?;
        let accounted_bytes = self
            .transport_buffer_bytes
            .checked_add(self.retained_batch_bytes)
            .and_then(|bytes| bytes.checked_add(session_state_bytes))
            .ok_or_else(|| fatal(anyhow!("YDB Topic response memory accounting overflow")))?;
        let _ = reservation.shrink_to(accounted_bytes);
        Ok(())
    }

    fn remove_settled_invalidated_sessions(&mut self) {
        self.partition_sessions
            .retain(|_, state| !settled_invalidated(state));
    }

    fn push_buffered_batch(&mut self, batch: TopicBatch) -> anyhow::Result<()> {
        let prospective_capacity = if self.buffered_batches.len() == self.buffered_batches.capacity()
        {
            if self.buffered_batches.capacity() == 0 {
                Some(MIN_BUFFERED_BATCH_CAPACITY)
            } else {
                self.buffered_batches.capacity().checked_mul(2)
            }
        } else {
            Some(self.buffered_batches.capacity())
        }
        .ok_or_else(|| fatal(anyhow!("YDB Topic buffered-batch capacity overflow")))?;
        let prospective_queue_bytes = prospective_capacity
            .checked_mul(size_of::<TopicBatch>())
            .ok_or_else(|| fatal(anyhow!("YDB Topic buffered-batch accounting overflow")))?;
        let session_state_bytes = self.session_state_heap_bytes_without_queue()?;
        let admitted_bytes = self
            .transport_buffer_bytes
            .checked_add(self.retained_batch_bytes)
            .and_then(|bytes| bytes.checked_add(session_state_bytes))
            .and_then(|bytes| bytes.checked_add(prospective_queue_bytes))
            .ok_or_else(|| fatal(anyhow!("YDB Topic buffered-batch accounting overflow")))?;
        self.credit_memory
            .grow_progress_source_to(admitted_bytes)
            .map_err(|error| fatal(error.context("admit buffered YDB Topic batch")))?;
        self.buffered_batches.push_back(batch);
        self.shrink_credit_memory_to_retained()
    }

    fn session_state_heap_bytes_without_queue(&self) -> anyhow::Result<usize> {
        configured_topics_heap_bytes(&self.configured_topics)?.checked_add(
            partition_sessions_heap_bytes(&self.partition_sessions)?,
        )
        .ok_or_else(|| fatal(anyhow!("YDB Topic session-state accounting overflow")))
    }

    fn session_state_heap_bytes(&self) -> anyhow::Result<usize> {
        self.session_state_heap_bytes_without_queue()?
            .checked_add(
                self.buffered_batches
                    .capacity()
                    .checked_mul(size_of::<TopicBatch>())
                    .ok_or_else(|| fatal(anyhow!("YDB Topic buffered-batch accounting overflow")))?,
            )
            .ok_or_else(|| fatal(anyhow!("YDB Topic session-state accounting overflow")))
    }

    async fn replenish_credit(&mut self) -> anyhow::Result<()> {
        if self.pending_credit <= 0 || self.uncommitted_batches > 0 {
            return Ok(());
        }
        let credit = self.pending_credit;
        self.send_read_credit(credit).await?;
        self.pending_credit = 0;
        Ok(())
    }

    fn grow_credit_memory_for_decoded_response(
        &self,
        decoded_bytes: usize,
        prospective_retained_bytes: usize,
    ) -> anyhow::Result<()> {
        let session_state_bytes = self.session_state_heap_bytes()?;
        let processing_peak = self
            .transport_buffer_bytes
            .checked_add(self.retained_batch_bytes)
            .and_then(|bytes| bytes.checked_add(session_state_bytes))
            .and_then(|bytes| bytes.checked_add(decoded_bytes))
            .and_then(|bytes| bytes.checked_add(prospective_retained_bytes))
            .ok_or_else(|| fatal(anyhow!("YDB Topic response memory accounting overflow")))?;
        self.credit_memory
            .grow_progress_source_to(processing_peak)
            .map_err(|error| fatal(error.context("account decoded YDB Topic response")))
    }
}

fn ensure_partition_session_id_is_new(
    sessions: &HashMap<i64, PartitionSessionState>,
    partition_session_id: i64,
) -> anyhow::Result<()> {
    if sessions.contains_key(&partition_session_id) {
        return Err(fatal(anyhow!(
            "YDB Topic reused active partition session id {partition_session_id}"
        )));
    }
    Ok(())
}

fn reject_fixed_partition_end(
    sessions: &HashMap<i64, PartitionSessionState>,
    partition_session_id: i64,
) -> anyhow::Result<SessionEvent> {
    let state = sessions.get(&partition_session_id).ok_or_else(|| {
        fatal(anyhow!(
            "YDB Topic ended unknown partition session {partition_session_id}"
        ))
    })?;
    Err(fatal(anyhow!(
        "YDB Topic ended fixed changefeed partition {}:{}; the validated single-partition topology changed",
        state.topic_path,
        state.partition_id
    )))
}

pub(super) fn response_processing_bytes(
    response_bytes: usize,
    retained_bytes: usize,
) -> anyhow::Result<usize> {
    let decoded_admission = response_bytes
        .checked_mul(MAX_DECODED_BYTES_PER_ENCODED_RESPONSE_BYTE)
        .ok_or_else(|| fatal(anyhow!("YDB Topic response decode admission overflow")))?;
    tonic_codec_buffer_bytes(response_bytes)?
        .checked_add(retained_bytes)
        .and_then(|bytes| bytes.checked_add(decoded_admission))
        .ok_or_else(|| fatal(anyhow!("YDB Topic response memory accounting overflow")))
}

fn tonic_codec_buffer_bytes(max_response_bytes: usize) -> anyhow::Result<usize> {
    let framed_response = max_response_bytes
        .checked_add(TONIC_GRPC_HEADER_BYTES)
        .ok_or_else(|| fatal(anyhow!("YDB Topic framed response size overflow")))?;
    let rounded_response = framed_response
        .checked_add(TONIC_CODEC_BUFFER_CHUNK_BYTES - 1)
        .map(|bytes| bytes / TONIC_CODEC_BUFFER_CHUNK_BYTES)
        .and_then(|chunks| chunks.checked_mul(TONIC_CODEC_BUFFER_CHUNK_BYTES))
        .ok_or_else(|| fatal(anyhow!("YDB Topic response codec buffer overflow")))?;
    rounded_response
        .checked_add(TONIC_OUTGOING_CODEC_BUFFER_BYTES)
        .ok_or_else(|| fatal(anyhow!("YDB Topic codec buffer accounting overflow")))
}

fn configured_topics_heap_bytes(topics: &HashMap<Arc<str>, i64>) -> anyhow::Result<usize> {
    let mut bytes = hash_table_allocation_bytes(
        topics.capacity(),
        size_of::<(Arc<str>, i64)>(),
    )?;
    for topic in topics.keys() {
        bytes = bytes
            .checked_add(ARC_STRONG_WEAK_COUNTER_BYTES)
            .and_then(|total| total.checked_add(topic.len()))
            .and_then(|total| total.checked_add(std::mem::align_of::<usize>()))
            .ok_or_else(|| fatal(anyhow!("YDB Topic configured-topic accounting overflow")))?;
    }
    Ok(bytes)
}

fn validate_assigned_partition(
    configured_topics: &HashMap<Arc<str>, i64>,
    sessions: &HashMap<i64, PartitionSessionState>,
    topic_path: &str,
    partition_id: i64,
) -> anyhow::Result<()> {
    let expected_partition_id = configured_topics.get(topic_path).ok_or_else(|| {
        fatal(anyhow!(
            "YDB Topic assigned unconfigured changefeed topic '{topic_path}'"
        ))
    })?;
    if *expected_partition_id != partition_id {
        return Err(fatal(anyhow!(
            "YDB Topic assigned changefeed topic '{topic_path}' partition {partition_id}, but discovery fixed the only safe partition at {expected_partition_id}"
        )));
    }
    if sessions
        .values()
        .any(|state| state.topic_path.as_ref() == topic_path)
    {
        return Err(fatal(anyhow!(
            "YDB Topic assigned more than one live session for fixed changefeed topic '{topic_path}'"
        )));
    }
    Ok(())
}

fn partition_sessions_heap_bytes(
    sessions: &HashMap<i64, PartitionSessionState>,
) -> anyhow::Result<usize> {
    let mut bytes = hash_table_allocation_bytes(
        sessions.capacity(),
        size_of::<(i64, PartitionSessionState)>(),
    )?;
    for state in sessions.values() {
        bytes = bytes
            .checked_add(ARC_STRONG_WEAK_COUNTER_BYTES)
            .and_then(|total| total.checked_add(state.topic_path.len()))
            .and_then(|total| total.checked_add(std::mem::align_of::<usize>()))
            .ok_or_else(|| fatal(anyhow!("YDB Topic partition-state accounting overflow")))?;
    }
    Ok(bytes)
}

fn hash_table_allocation_bytes(capacity: usize, entry_bytes: usize) -> anyhow::Result<usize> {
    if capacity == 0 {
        return Ok(0);
    }
    // std's hashbrown table reports usable capacity at a load factor below one.
    // Twice that capacity is a conservative upper bound for allocated buckets;
    // one extra 16-byte control group covers the sentinel/control tail.
    let buckets = capacity
        .checked_mul(2)
        .ok_or_else(|| fatal(anyhow!("YDB Topic hash-table bucket accounting overflow")))?;
    buckets
        .checked_mul(entry_bytes)
        .and_then(|bytes| bytes.checked_add(buckets))
        .and_then(|bytes| bytes.checked_add(16))
        .ok_or_else(|| fatal(anyhow!("YDB Topic hash-table memory accounting overflow")))
}

fn consume_read_response_credit(
    available_credit: i64,
    pending_credit: i64,
    reported_bytes: i64,
) -> anyhow::Result<(i64, i64)> {
    anyhow::ensure!(
        reported_bytes > 0,
        "YDB Topic ReadResponse.bytes_size must be positive"
    );
    anyhow::ensure!(
        available_credit > 0,
        "YDB Topic sent a ReadResponse while the available read credit was not positive"
    );
    let available_credit = available_credit
        .checked_sub(reported_bytes)
        .ok_or_else(|| fatal(anyhow!("YDB Topic available read credit overflow")))?;
    let pending_credit = pending_credit
        .checked_add(reported_bytes)
        .ok_or_else(|| fatal(anyhow!("YDB Topic pending read credit overflow")))?;
    Ok((available_credit, pending_credit))
}

fn topic_batch_retained_bytes(
    records: &[TopicRecord],
    records_capacity: usize,
    partitions_capacity: usize,
) -> anyhow::Result<usize> {
    let mut payload_bytes = 0usize;
    for record in records {
        payload_bytes = payload_bytes
            .checked_add(record.payload.capacity())
            .ok_or_else(|| fatal(anyhow!("YDB Topic retained payload accounting overflow")))?;
    }
    topic_batch_retained_layout_bytes(records_capacity, payload_bytes, partitions_capacity)
}

const ARC_STRONG_WEAK_COUNTER_BYTES: usize = 2 * size_of::<usize>();

fn topic_batch_retained_layout_bytes(
    records_capacity: usize,
    payload_capacity_bytes: usize,
    partitions_capacity: usize,
) -> anyhow::Result<usize> {
    records_capacity
        .checked_mul(size_of::<TopicRecord>())
        .and_then(|bytes| bytes.checked_add(payload_capacity_bytes))
        .and_then(|bytes| bytes.checked_add(size_of::<TopicCommitMarker>()))
        .and_then(|bytes| bytes.checked_add(ARC_STRONG_WEAK_COUNTER_BYTES))
        .and_then(|bytes| {
            bytes.checked_add(
                partitions_capacity
                    .checked_mul(size_of::<PartitionCommitMarker>())?,
            )
        })
        .map(|bytes| bytes.max(1))
        .ok_or_else(|| fatal(anyhow!("YDB Topic retained batch accounting overflow")))
}

fn read_response_plan(
    response: &ydb_grpc::ydb_proto::topic::stream_read_message::ReadResponse,
) -> anyhow::Result<ReadResponsePlan> {
    let mut record_count = 0usize;
    let mut marker_partition_count = 0usize;
    let mut payload_capacity_bytes = 0usize;
    for partition in &response.partition_data {
        let before = record_count;
        for batch in &partition.batches {
            record_count = record_count
                .checked_add(batch.message_data.len())
                .ok_or_else(|| fatal(anyhow!("YDB Topic record count overflow")))?;
            for message in &batch.message_data {
                payload_capacity_bytes = payload_capacity_bytes
                    .checked_add(message.data.capacity())
                    .ok_or_else(|| fatal(anyhow!("YDB Topic payload admission overflow")))?;
            }
        }
        if record_count > before {
            marker_partition_count = marker_partition_count
                .checked_add(1)
                .ok_or_else(|| fatal(anyhow!("YDB Topic marker count overflow")))?;
        }
    }
    Ok(ReadResponsePlan {
        record_count,
        marker_partition_count,
        retained_batch_bytes: topic_batch_retained_layout_bytes(
            record_count,
            payload_capacity_bytes,
            marker_partition_count,
        )?,
    })
}

fn read_response_heap_bytes(
    response: &ydb_grpc::ydb_proto::topic::stream_read_message::ReadResponse,
) -> anyhow::Result<usize> {
    use ydb_grpc::ydb_proto::topic::stream_read_message::read_response::{
        Batch, MessageData, PartitionData,
    };

    let mut bytes = size_of::<ydb_grpc::ydb_proto::topic::stream_read_message::ReadResponse>()
        .checked_add(
            response
                .partition_data
                .capacity()
                .checked_mul(size_of::<PartitionData>())
                .ok_or_else(|| fatal(anyhow!("YDB Topic partition accounting overflow")))?,
        )
        .ok_or_else(|| fatal(anyhow!("YDB Topic response accounting overflow")))?;
    for partition in &response.partition_data {
        bytes = bytes
            .checked_add(
                partition
                    .batches
                    .capacity()
                    .checked_mul(size_of::<Batch>())
                    .ok_or_else(|| fatal(anyhow!("YDB Topic batch accounting overflow")))?,
            )
            .ok_or_else(|| fatal(anyhow!("YDB Topic response accounting overflow")))?;
        for batch in &partition.batches {
            bytes = bytes
                .checked_add(batch.producer_id.capacity())
                .and_then(|value| {
                    value.checked_add(
                        batch
                            .message_data
                            .capacity()
                            .checked_mul(size_of::<MessageData>())?,
                    )
                })
                .and_then(|value| {
                    value.checked_add(
                        batch.write_session_meta.capacity().checked_mul(
                            size_of::<(String, String)>() + size_of::<usize>(),
                        )?,
                    )
                })
                .ok_or_else(|| fatal(anyhow!("YDB Topic batch heap accounting overflow")))?;
            for (key, value) in &batch.write_session_meta {
                bytes = bytes
                    .checked_add(key.capacity())
                    .and_then(|total| total.checked_add(value.capacity()))
                    .ok_or_else(|| {
                        fatal(anyhow!("YDB Topic session metadata accounting overflow"))
                    })?;
            }
            for message in &batch.message_data {
                bytes = bytes
                    .checked_add(message.data.capacity())
                    .and_then(|value| value.checked_add(message.message_group_id.capacity()))
                    .and_then(|value| {
                        value.checked_add(
                            message
                                .metadata_items
                                .capacity()
                                .checked_mul(size_of::<
                                    ydb_grpc::ydb_proto::topic::MetadataItem,
                                >())?,
                        )
                    })
                    .ok_or_else(|| {
                        fatal(anyhow!("YDB Topic message heap accounting overflow"))
                    })?;
                for metadata in &message.metadata_items {
                    bytes = bytes
                        .checked_add(metadata.key.capacity())
                        .and_then(|value| value.checked_add(metadata.value.capacity()))
                        .ok_or_else(|| {
                            fatal(anyhow!("YDB Topic message metadata accounting overflow"))
                        })?;
                }
            }
        }
    }
    Ok(bytes)
}

fn build_commit_request(
    markers: &[CommitMarker],
    sessions: &HashMap<i64, PartitionSessionState>,
) -> anyhow::Result<(Vec<PartitionCommitOffset>, Vec<(i64, i64)>, usize)> {
    let entry_count = commit_marker_partition_count(markers)?;
    let mut entries = Vec::<(i64, OffsetsRange)>::with_capacity(entry_count);
    let mut committed_bytes = 0usize;
    for marker in markers {
        let marker = marker
            .value::<TopicCommitMarker>()
            .map_err(|error| fatal(anyhow!(error)))?;
        committed_bytes = committed_bytes
            .checked_add(marker.retained_bytes)
            .ok_or_else(|| fatal(anyhow!("YDB Topic committed memory overflow")))?;
        for partition in &marker.partitions {
            let state = sessions
                .get(&partition.partition_session_id)
                .ok_or_else(|| {
                    DataPlaneFailure::retryable(anyhow!(
                        "YDB Topic partition session {} ended before commit",
                        partition.partition_session_id
                    ))
                })?;
            anyhow::ensure!(
                state.topic_path == partition.topic_path
                    && state.partition_id == partition.partition_id,
                "YDB Topic commit marker session mismatch"
            );
            entries.push((partition.partition_session_id, partition.range));
        }
    }
    entries.sort_unstable_by_key(|(session_id, range)| (*session_id, range.start, range.end));
    let mut grouped = Vec::<(i64, OffsetsRange)>::with_capacity(entries.len());
    for (session_id, range) in entries {
        if let Some((previous_session, previous_range)) = grouped.last_mut() {
            if *previous_session == session_id {
                anyhow::ensure!(
                    previous_range.end == range.start,
                    "non-contiguous YDB Topic commit ranges for partition session {session_id}"
                );
                previous_range.end = range.end;
                continue;
            }
        }
        grouped.push((session_id, range));
    }
    let mut targets = Vec::with_capacity(grouped.len());
    let mut commits = Vec::with_capacity(grouped.len());
    for (session_id, range) in grouped {
        anyhow::ensure!(
            range.start >= 0 && range.start < range.end,
            "invalid YDB Topic commit range [{}, {})",
            range.start,
            range.end
        );
        let target = range.end;
        targets.push((session_id, target));
        commits.push(PartitionCommitOffset {
            partition_session_id: session_id,
            offsets: vec![range],
        });
    }
    Ok((commits, targets, committed_bytes))
}

fn commit_marker_partition_count(markers: &[CommitMarker]) -> anyhow::Result<usize> {
    markers.iter().try_fold(0usize, |count, marker| {
        let marker = marker
            .value::<TopicCommitMarker>()
            .map_err(|error| fatal(anyhow!(error)))?;
        count
            .checked_add(marker.partitions.len())
            .ok_or_else(|| fatal(anyhow!("YDB Topic commit marker count overflow")))
    })
}

fn commit_request_admission_bytes(markers: &[CommitMarker]) -> anyhow::Result<usize> {
    let partitions = commit_marker_partition_count(markers)?;
    let entry_bytes = size_of::<(i64, OffsetsRange)>()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(size_of::<(i64, i64)>()))
        .and_then(|bytes| bytes.checked_add(size_of::<PartitionCommitOffset>()))
        .and_then(|bytes| bytes.checked_add(size_of::<OffsetsRange>()))
        .and_then(|bytes| bytes.checked_add(MAX_ENCODED_PARTITION_COMMIT_BYTES))
        .ok_or_else(|| fatal(anyhow!("YDB Topic commit admission overflow")))?;
    partitions
        .checked_mul(entry_bytes)
        .and_then(|bytes| bytes.checked_add(size_of::<CommitOffsetRequest>()))
        .and_then(|bytes| bytes.checked_add(size_of::<FromClient>()))
        .ok_or_else(|| fatal(anyhow!("YDB Topic commit admission overflow")))
}

#[cfg(test)]
fn coalesce_ranges(mut ranges: Vec<OffsetsRange>) -> anyhow::Result<Vec<OffsetsRange>> {
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut output: Vec<OffsetsRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        anyhow::ensure!(
            range.start >= 0 && range.start < range.end,
            "invalid YDB Topic commit range [{}, {})",
            range.start,
            range.end
        );
        if let Some(previous) = output.last_mut() {
            anyhow::ensure!(
                range.start >= previous.end,
                "overlapping YDB Topic commit ranges"
            );
            if range.start == previous.end {
                previous.end = range.end;
                continue;
            }
        }
        output.push(range);
    }
    Ok(output)
}

fn ensure_targets_live(
    targets: &[(i64, i64)],
    sessions: &HashMap<i64, PartitionSessionState>,
) -> anyhow::Result<()> {
    for (session_id, _) in targets {
        let state = sessions.get(session_id).ok_or_else(|| {
            DataPlaneFailure::retryable(anyhow!(
                "YDB Topic partition session {session_id} ended before commit acknowledgement"
            ))
        })?;
        if state.invalidated {
            return Err(DataPlaneFailure::retryable(anyhow!(
                "YDB Topic partition session {session_id} was revoked before commit acknowledgement"
            ))
            .into());
        }
    }
    Ok(())
}

fn apply_commit_ack(
    targets: &[(i64, i64)],
    sessions: &mut HashMap<i64, PartitionSessionState>,
    mut offsets: Vec<PartitionCommittedOffset>,
) -> anyhow::Result<()> {
    offsets.sort_unstable_by_key(|offset| offset.partition_session_id);
    anyhow::ensure!(
        offsets
            .windows(2)
            .all(|pair| pair[0].partition_session_id != pair[1].partition_session_id),
        "YDB Topic commit acknowledgement repeats a partition session"
    );
    for offset in &offsets {
        let target = targets
            .binary_search_by_key(&offset.partition_session_id, |(session_id, _)| *session_id)
            .ok()
            .map(|index| targets[index].1)
            .ok_or_else(|| {
                fatal(anyhow!(
                    "YDB Topic acknowledged partition session {} outside the in-flight commit",
                    offset.partition_session_id
                ))
            })?;
        let state = sessions
            .get(&offset.partition_session_id)
            .ok_or_else(|| {
                fatal(anyhow!(
                    "YDB Topic acknowledged unknown partition session {}",
                    offset.partition_session_id
                ))
            })?;
        anyhow::ensure!(
            offset.committed_offset >= state.committed_offset
                && offset.committed_offset <= state.read_through
                && offset.committed_offset <= target,
            "YDB Topic acknowledged invalid offset {} for partition session {} with in-flight target {}",
            offset.committed_offset,
            offset.partition_session_id,
            target
        );
    }
    for offset in offsets {
        let state = sessions
            .get_mut(&offset.partition_session_id)
            .ok_or_else(|| fatal(anyhow!("validated YDB Topic partition session disappeared")))?;
        state.committed_offset = offset.committed_offset;
        state.commit_response_offset = offset.committed_offset;
    }
    Ok(())
}

fn ensure_ack_does_not_exceed_targets(
    targets: &[(i64, i64)],
    sessions: &HashMap<i64, PartitionSessionState>,
) -> anyhow::Result<()> {
    for (session_id, target) in targets {
        let acknowledged = sessions
            .get(session_id)
            .ok_or_else(|| {
                DataPlaneFailure::retryable(anyhow!(
                    "YDB Topic partition session {session_id} ended before commit acknowledgement"
                ))
            })?
            .commit_response_offset;
        anyhow::ensure!(
            acknowledged <= *target,
            "YDB Topic acknowledged offset {acknowledged} beyond requested target {target} for partition session {session_id}"
        );
    }
    Ok(())
}

fn validate_retained_offset(
    committed_offset: i64,
    retained: &OffsetsRange,
    topic_path: &str,
    partition_id: i64,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        retained.start >= 0
            && retained.start <= retained.end
            && committed_offset >= retained.start
            && committed_offset <= retained.end,
        "YDB changefeed committed offset {committed_offset} for {topic_path}:{partition_id} is outside the retained range [{}, {}]",
        retained.start,
        retained.end
    );
    Ok(())
}

const fn settled_invalidated(state: &PartitionSessionState) -> bool {
    state.invalidated && state.commit_response_offset >= state.read_through
}

const fn graceful_stop_ready(state: &PartitionSessionState) -> bool {
    state.pending_graceful_stop.is_some()
        && state.commit_response_offset >= state.read_through
}

fn canonical_topic_path(path: &str) -> &str {
    path.strip_prefix('/').unwrap_or(path)
}

fn timestamp_millis(
    timestamp: Option<&ydb_grpc::google_proto_workaround::protobuf::Timestamp>,
) -> anyhow::Result<i64> {
    let timestamp = timestamp.ok_or_else(|| {
        fatal(anyhow!(
            "YDB changefeed message batch has no server written_at timestamp"
        ))
    })?;
    anyhow::ensure!(
        (0..1_000_000_000).contains(&timestamp.nanos),
        "YDB Topic timestamp has invalid nanoseconds"
    );
    timestamp
        .seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(i64::from(timestamp.nanos) / 1_000_000))
        .ok_or_else(|| fatal(anyhow!("YDB Topic timestamp exceeds milliseconds")))
}

fn validate_status(response: &FromServer) -> anyhow::Result<()> {
    let status = StatusCode::try_from(response.status).ok();
    if status == Some(StatusCode::Success) {
        anyhow::ensure!(
            response.issues.is_empty(),
            "YDB Topic successful response unexpectedly contains issues"
        );
        return Ok(());
    }
    let status_name = status.map_or("UNKNOWN", |status| status.as_str_name());
    let issues = response
        .issues
        .iter()
        .map(|issue| issue.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    let error = anyhow!(
        "YDB Topic request failed with status {} ({status_name}), issues={issues}",
        response.status
    );
    Err(if status.is_some_and(is_retryable_status) {
        DataPlaneFailure::retryable(error).into()
    } else {
        DataPlaneFailure::fatal(error).into()
    })
}

fn tonic_failure(operation: &str, status: &tonic::Status) -> anyhow::Error {
    let error = anyhow!("YDB Topic {operation} failed with code {:?}", status.code());
    if matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
    ) {
        DataPlaneFailure::retryable(error).into()
    } else {
        DataPlaneFailure::fatal(error).into()
    }
}

const fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::Aborted
            | StatusCode::Unavailable
            | StatusCode::Overloaded
            | StatusCode::Timeout
            | StatusCode::BadSession
            | StatusCode::SessionExpired
            | StatusCode::Cancelled
            | StatusCode::Undetermined
            | StatusCode::SessionBusy
            | StatusCode::ExternalError
    )
}

fn fatal(error: anyhow::Error) -> anyhow::Error {
    DataPlaneFailure::fatal(error).into()
}

#[cfg(test)]
#[path = "tests/topic.rs"]
mod tests;
