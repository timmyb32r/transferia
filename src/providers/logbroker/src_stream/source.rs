use alloc::sync::Arc;
use std::collections::{HashMap, HashSet, VecDeque};
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

use super::{connect_client, set_ydb_headers, LogbrokerSourceConfig};
use crate::core::data::message::{Message, MessageMeta, SourceBatch};
use crate::core::failure::DataPlaneFailure;
use crate::core::memory::PipelineMemory;
use crate::core::source::{CommitMarker, Source};
use crate::metrics::SourceCounters;

const OUTGOING_CHANNEL_CAPACITY: usize = 8;
const MAX_DECOMPRESSED_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const MAX_DECOMPRESSED_BATCH_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct YdbTopicCommitMarker {
    pub(super) partitions: Vec<PartitionCommitMarker>,
}

#[derive(Debug)]
pub(super) struct PartitionCommitMarker {
    pub(super) topic_path: Arc<str>,
    pub(super) partition_id: i64,
    pub(super) partition_session_id: i64,
    pub(super) ranges: Vec<OffsetsRange>,
}

#[derive(Debug)]
pub(super) struct PartitionSessionState {
    pub(super) topic_path: Arc<str>,
    pub(super) partition_id: i64,
    pub(super) committed_offset: i64,
    pub(super) read_through: i64,
    pub(super) pending_graceful_stop: bool,
    pub(super) invalidated: bool,
}

enum SessionEvent {
    Batch(SourceBatch),
    CommitAck(Vec<(i64, i64)>),
    Continue,
}

pub(super) struct YdbTopicSource {
    outgoing: mpsc::Sender<FromClient>,
    incoming: tonic::Streaming<FromServer>,
    buffered_batches: VecDeque<SourceBatch>,
    topic_filters: HashMap<Arc<str>, Option<HashSet<i64>>>,
    partition_sessions: HashMap<i64, PartitionSessionState>,
    allow_ttl_rewind: bool,
    cancellation: CancellationToken,
    memory: PipelineMemory,
    counters: Arc<SourceCounters>,
    pending_credit: i64,
}

impl YdbTopicSource {
    pub(super) async fn connect(
        config: &LogbrokerSourceConfig,
        token: Arc<str>,
        reader_lane: i64,
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
            .send(init_message(config, reader_lane))
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
            topic_filters: config
                .topics
                .iter()
                .map(|topic| {
                    let filter = (!topic.partitions.is_empty())
                        .then(|| topic.partitions.iter().copied().collect());
                    (Arc::from(topic.path.as_str()), filter)
                })
                .collect(),
            partition_sessions: HashMap::new(),
            allow_ttl_rewind: config.allow_ttl_rewind,
            cancellation,
            memory,
            counters,
            pending_credit: 0,
        };
        source.await_init(timeout).await?;
        source
            .send(ClientMessage::ReadRequest(ReadRequest {
                bytes_size: i64::try_from(config.read_buffer_bytes)
                    .map_err(|_| anyhow!("logbroker.read_buffer_bytes exceeds i64"))?,
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
                Err(DataPlaneFailure::retryable(anyhow!("YDB Topic session cancelled")).into())
            }
            result = self.outgoing.send(message) => {
                result.map_err(|_| DataPlaneFailure::retryable(anyhow!("YDB Topic request stream closed")).into())
            }
        }
    }

    async fn next_response(&mut self) -> anyhow::Result<FromServer> {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                return Err(DataPlaneFailure::retryable(anyhow!("YDB Topic session cancelled")).into());
            }
            response = self.incoming.message() => response,
        };
        self.counters.add_response_wait(started.elapsed());
        match result {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(DataPlaneFailure::retryable(anyhow!(
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
                let configured =
                    self.topic_filters
                        .get(session.path.as_str())
                        .ok_or_else(|| {
                            fatal(anyhow!(
                                "YDB Topic assigned unconfigured topic '{}'",
                                session.path
                            ))
                        })?;
                if configured
                    .as_ref()
                    .is_some_and(|partitions| !partitions.contains(&session.partition_id))
                {
                    return Err(fatal(anyhow!(
                        "YDB Topic assigned unconfigured partition {}:{}",
                        session.path,
                        session.partition_id
                    )));
                }
                if let Some(offsets) = request.partition_offsets.as_ref() {
                    anyhow::ensure!(
                        offsets.start >= 0 && offsets.start <= offsets.end,
                        "YDB Topic returned invalid partition offsets [{}, {})",
                        offsets.start,
                        offsets.end
                    );
                    if request.committed_offset < offsets.start {
                        if !self.allow_ttl_rewind {
                            return Err(fatal(anyhow!(
                                "YDB Topic committed offset {} for {}:{} expired; the oldest retained offset is {}. Set allow_ttl_rewind=true only if replaying from the retention boundary is acceptable",
                                request.committed_offset,
                                session.path,
                                session.partition_id,
                                offsets.start
                            )));
                        }
                        tracing::warn!(
                            topic = session.path,
                            partition = session.partition_id,
                            committed_offset = request.committed_offset,
                            oldest_retained_offset = offsets.start,
                            "YDB Topic committed offset expired; continuing from the retention boundary because allow_ttl_rewind=true"
                        );
                    }
                }
                if let Some(current) = self.partition_sessions.get(&session.partition_session_id) {
                    anyhow::ensure!(
                        current.topic_path.as_ref() == session.path
                            && current.partition_id == session.partition_id,
                        "YDB Topic reused partition session {} for a different partition",
                        session.partition_session_id
                    );
                } else {
                    anyhow::ensure!(
                        !self.partition_sessions.values().any(|current| {
                            !current.invalidated
                                && current.topic_path.as_ref() == session.path
                                && current.partition_id == session.partition_id
                        }),
                        "YDB Topic assigned partition {}:{} twice in one read session",
                        session.path,
                        session.partition_id
                    );
                    self.partition_sessions.insert(
                        session.partition_session_id,
                        PartitionSessionState {
                            topic_path: Arc::from(session.path.as_str()),
                            partition_id: session.partition_id,
                            committed_offset: request.committed_offset,
                            read_through: request.committed_offset,
                            pending_graceful_stop: false,
                            invalidated: false,
                        },
                    );
                }
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
                anyhow::ensure!(
                    !response.partitions_committed_offsets.is_empty(),
                    "YDB Topic commit response contains no partition offsets"
                );
                let mut committed = Vec::with_capacity(response.partitions_committed_offsets.len());
                for offset in response.partitions_committed_offsets {
                    let state = self
                        .partition_sessions
                        .get_mut(&offset.partition_session_id)
                        .ok_or_else(|| {
                            fatal(anyhow!(
                                "YDB Topic acknowledged unknown partition session {}",
                                offset.partition_session_id
                            ))
                        })?;
                    state.committed_offset = state.committed_offset.max(offset.committed_offset);
                    committed.push((offset.partition_session_id, offset.committed_offset));
                }
                Ok(SessionEvent::CommitAck(committed))
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
                if request.graceful {
                    state.committed_offset = state.committed_offset.max(request.committed_offset);
                    state.pending_graceful_stop = true;
                    self.release_gracefully_stopped_sessions().await?;
                } else {
                    state.invalidated = true;
                    let remove_after_response = state.committed_offset >= state.read_through;
                    self.send(ClientMessage::StopPartitionSessionResponse(
                        StopPartitionSessionResponse {
                            partition_session_id: request.partition_session_id,
                            graceful: false,
                        },
                    ))
                    .await?;
                    if remove_after_response {
                        self.partition_sessions
                            .remove(&request.partition_session_id);
                    }
                }
                Ok(SessionEvent::Continue)
            }
            ServerMessage::EndPartitionSession(request) => {
                anyhow::ensure!(
                    self.partition_sessions
                        .contains_key(&request.partition_session_id),
                    "YDB Topic ended unknown partition session {}",
                    request.partition_session_id
                );
                Ok(SessionEvent::Continue)
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
        let releasable = releasable_session_ids(&self.partition_sessions);
        for session_id in releasable {
            self.send(ClientMessage::StopPartitionSessionResponse(
                StopPartitionSessionResponse {
                    partition_session_id: session_id,
                    graceful: true,
                },
            ))
            .await?;
            self.partition_sessions.remove(&session_id);
        }
        Ok(())
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
        let mut partition_batches = Vec::with_capacity(response.partition_data.len());
        for partition_data in response.partition_data {
            let state = self
                .partition_sessions
                .get(&partition_data.partition_session_id)
                .ok_or_else(|| {
                    fatal(anyhow!(
                        "YDB Topic sent data for unknown partition session {}",
                        partition_data.partition_session_id
                    ))
                })?;
            anyhow::ensure!(
                !state.invalidated,
                "YDB Topic sent data for invalidated partition session {}",
                partition_data.partition_session_id
            );
            partition_batches.push((
                partition_data.partition_session_id,
                Arc::clone(&state.topic_path),
                state.partition_id,
                partition_data.batches,
            ));
        }
        if partition_batches
            .iter()
            .all(|(_, _, _, batches)| batches.is_empty())
        {
            return Ok(SessionEvent::Continue);
        }

        let compressed_bytes = partition_batches
            .iter()
            .flat_map(|(_, _, _, batches)| batches)
            .flat_map(|batch| &batch.message_data)
            .try_fold(0usize, |total, message| {
                total.checked_add(message.data.len())
            })
            .ok_or_else(|| fatal(anyhow!("YDB Topic compressed batch size overflow")))?;
        let declared_bytes = partition_batches
            .iter()
            .flat_map(|(_, _, _, batches)| batches)
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
        let started = Instant::now();
        let decoded = tokio::task::spawn_blocking(move || {
            partition_batches
                .into_iter()
                .map(|(session_id, topic, partition_id, batches)| {
                    decode_batches(batches, &topic, partition_id)
                        .map(|decoded| (session_id, topic, partition_id, decoded))
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .await
        .map_err(|error| fatal(anyhow!("YDB Topic decoder task failed: {error}")))??;
        self.counters.add_decomp_busy(started.elapsed());
        let decompressed_bytes = decoded
            .iter()
            .flat_map(|(_, _, _, decoded)| &decoded.messages)
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
        self.counters.add_messages(
            u64::try_from(
                decoded
                    .iter()
                    .map(|(_, _, _, decoded)| decoded.messages.len())
                    .sum::<usize>(),
            )
            .unwrap_or(u64::MAX),
        );
        self.counters
            .add_compressed_bytes(u64::try_from(compressed_bytes).unwrap_or(u64::MAX));
        self.counters
            .add_decompressed_bytes(u64::try_from(decompressed_bytes).unwrap_or(u64::MAX));
        let mut messages = Vec::new();
        let mut partitions = Vec::new();
        for (session_id, topic_path, partition_id, decoded) in decoded {
            if !decoded.ranges.is_empty() {
                let read_through = decoded
                    .ranges
                    .iter()
                    .map(|range| range.end)
                    .max()
                    .ok_or_else(|| {
                        fatal(anyhow!(
                            "YDB Topic decoded partition has no offset upper bound"
                        ))
                    })?;
                let state = self
                    .partition_sessions
                    .get_mut(&session_id)
                    .ok_or_else(|| {
                        fatal(anyhow!(
                            "YDB Topic partition session {session_id} disappeared while decoding"
                        ))
                    })?;
                state.read_through = state.read_through.max(read_through);
                partitions.push(PartitionCommitMarker {
                    topic_path,
                    partition_id,
                    partition_session_id: session_id,
                    ranges: decoded.ranges,
                });
            }
            messages.extend(decoded.messages);
        }
        let commit_marker = (!partitions.is_empty())
            .then(|| CommitMarker::new(YdbTopicCommitMarker { partitions }));
        Ok(SessionEvent::Batch(SourceBatch::Raw {
            messages,
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
    fn read_batch(&mut self) -> BoxFuture<'_, crate::core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let result: anyhow::Result<SourceBatch> = async {
                if let Some(batch) = self.buffered_batches.pop_front() {
                    return Ok(batch);
                }
                self.replenish_credit().await?;
                loop {
                    match self.receive_event().await? {
                        SessionEvent::Batch(batch) => return Ok(batch),
                        SessionEvent::CommitAck(_) | SessionEvent::Continue => {
                            self.replenish_credit().await?;
                        }
                    }
                }
            }
            .await;
            result.map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }

    fn commit_offsets<'context>(
        &'context mut self,
        markers: &'context [CommitMarker],
    ) -> BoxFuture<'context, crate::core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                if markers.is_empty() {
                    return Ok(());
                }
                let (commit_offsets, mut targets) =
                    build_commit_request(markers, &self.partition_sessions)?;
                self.send(ClientMessage::CommitOffsetRequest(CommitOffsetRequest {
                    commit_offsets,
                }))
                .await?;
                loop {
                    match self.receive_event().await? {
                        SessionEvent::CommitAck(committed) => {
                            for (session_id, committed_offset) in committed {
                                if targets
                                    .get(&session_id)
                                    .is_some_and(|target| committed_offset >= *target)
                                {
                                    targets.remove(&session_id);
                                }
                            }
                            if targets.is_empty() {
                                self.release_gracefully_stopped_sessions().await?;
                                return Ok(());
                            }
                        }
                        SessionEvent::Batch(batch) => self.buffered_batches.push_back(batch),
                        SessionEvent::Continue => {}
                    }
                }
            }
            .await;
            result.map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}

pub(super) fn releasable_session_ids(sessions: &HashMap<i64, PartitionSessionState>) -> Vec<i64> {
    sessions
        .iter()
        .filter_map(|(session_id, state)| {
            (state.pending_graceful_stop && state.committed_offset >= state.read_through)
                .then_some(*session_id)
        })
        .collect()
}

pub(super) fn build_commit_request(
    markers: &[CommitMarker],
    sessions: &HashMap<i64, PartitionSessionState>,
) -> anyhow::Result<(Vec<PartitionCommitOffset>, HashMap<i64, i64>)> {
    let mut grouped = HashMap::<i64, Vec<OffsetsRange>>::new();
    for marker in markers {
        let marker = marker
            .value::<YdbTopicCommitMarker>()
            .map_err(|error| fatal(anyhow!(error)))?;
        for partition in &marker.partitions {
            let state = sessions
                .get(&partition.partition_session_id)
                .ok_or_else(|| {
                    DataPlaneFailure::retryable(anyhow!(
                        "YDB Topic partition session {} ended before commit",
                        partition.partition_session_id
                    ))
                })?;
            if state.invalidated {
                return Err(DataPlaneFailure::retryable(anyhow!(
                    "YDB Topic partition session {} was revoked before commit",
                    partition.partition_session_id
                ))
                .into());
            }
            anyhow::ensure!(
                state.topic_path == partition.topic_path
                    && state.partition_id == partition.partition_id,
                "YDB Topic commit marker session mismatch"
            );
            grouped
                .entry(partition.partition_session_id)
                .or_default()
                .extend(partition.ranges.iter().copied());
        }
    }
    let mut targets = HashMap::with_capacity(grouped.len());
    let mut commit_offsets = Vec::with_capacity(grouped.len());
    for (session_id, ranges) in grouped {
        let ranges = coalesce_ranges(ranges)?;
        let target = ranges
            .iter()
            .map(|range| range.end)
            .max()
            .ok_or_else(|| fatal(anyhow!("YDB Topic commit marker has no offsets")))?;
        targets.insert(session_id, target);
        commit_offsets.push(PartitionCommitOffset {
            partition_session_id: session_id,
            offsets: ranges,
        });
    }
    commit_offsets.sort_unstable_by_key(|commit| commit.partition_session_id);
    Ok((commit_offsets, targets))
}

pub(super) fn init_message(config: &LogbrokerSourceConfig, reader_lane: i64) -> FromClient {
    FromClient {
        client_message: Some(ClientMessage::InitRequest(InitRequest {
            topics_read_settings: config
                .topics
                .iter()
                .map(|topic| TopicReadSettings {
                    path: topic.path.clone(),
                    partition_ids: topic.partitions.clone(),
                    max_lag: None,
                    read_from: None,
                })
                .collect(),
            consumer: config.consumer_name.clone(),
            reader_name: format!("transferia-rust-{reader_lane}"),
            direct_read: false,
            auto_partitioning_support: true,
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
        Err(DataPlaneFailure::retryable(error).into())
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
        DataPlaneFailure::retryable(error).into()
    }
}

fn fatal(error: anyhow::Error) -> anyhow::Error {
    DataPlaneFailure::fatal(error).into()
}
