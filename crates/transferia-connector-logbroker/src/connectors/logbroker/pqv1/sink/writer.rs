use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use futures_util::{Stream, TryStreamExt as _};
use tokio::sync::mpsc;
use tonic::Request;

use crate::connectors::logbroker::pqv1::config::PqV1SinkConfig;
use crate::connectors::logbroker::pqv1::pq_v1::{
    connect_http2_prior_knowledge, http_uri, parse_endpoint, set_ydb_headers,
};
use crate::connectors::logbroker::proto::pers_queue::v1::{
    streaming_write_client_message, streaming_write_server_message, Codec,
    StreamingWriteClientMessage, StreamingWriteServerMessage,
};
use crate::metrics::SinkCounters;
use crate::serializer::{DeliverySerializer, QueueMessageMode, SerializedDelivery};
use transferia_core::delivery::SinkLimits;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_registry::SinkBuildContext;

const SUCCESS: i32 = 400_000;
const UNSPECIFIED: i32 = 0;
pub const MAX_GRPC_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

pub(super) struct PqV1Sink {
    config: Arc<PqV1SinkConfig>,
    message_group_id: Arc<str>,
    token: Arc<str>,
    counters: Arc<SinkCounters>,
    delivery_name: Arc<str>,
    discovery: Arc<transferia_core::delivery::DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
}

struct RequestStream(mpsc::Receiver<StreamingWriteClientMessage>);

impl Stream for RequestStream {
    type Item = StreamingWriteClientMessage;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context)
    }
}

impl PqV1Sink {
    pub(super) fn new(
        config: Arc<PqV1SinkConfig>,
        message_group_id: Arc<str>,
        token: Arc<str>,
        context: SinkBuildContext,
    ) -> Self {
        let limits: Arc<dyn SinkLimits> = Arc::clone(&config) as Arc<dyn SinkLimits>;
        Self {
            config,
            message_group_id,
            token,
            counters: context.counters,
            delivery_name: context.delivery_name,
            discovery: context.discovery,
            limits,
        }
    }

    async fn run_session(&self, mut io: SinkIo) -> anyhow::Result<()> {
        let host = parse_endpoint(&self.config.endpoint())?;
        let uri = http_uri(&host)?;
        let transport =
            connect_http2_prior_knowledge(&uri, self.config.network_timeout(), &io.cancellation)
                .await?;
        let mut grpc = tonic::client::Grpc::with_origin(transport, uri)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE);
        let (request_tx, request_rx) = mpsc::channel(2);
        let mut request = Request::new(RequestStream(request_rx));
        set_ydb_headers(request.metadata_mut(), &self.token)?;
        let path = http::uri::PathAndQuery::from_static(
            "/Ydb.PersQueue.V1.PersQueueService/StreamingWrite",
        );
        let response = tokio::time::timeout(self.config.network_timeout(), async {
            grpc.ready()
                .await
                .map_err(|error| anyhow::anyhow!("PQv1 writer is not ready: {error}"))?;
            grpc
                .streaming(
                    request,
                    path,
                    tonic_prost::ProstCodec::<
                        StreamingWriteClientMessage,
                        StreamingWriteServerMessage,
                    >::default(),
                )
                .await
                .map(tonic::Response::into_inner)
                .map_err(anyhow::Error::from)
        })
        .await
        .map_err(|_| anyhow::anyhow!("PQv1 writer connect timed out"))??;
        request_tx
            .send(init_message(&self.config, &self.message_group_id))
            .await
            .map_err(|_| anyhow::anyhow!("PQv1 writer request stream closed before init"))?;
        let mut responses = response;
        let init = next_response(&mut responses, self.config.network_timeout()).await?;
        let Some(streaming_write_server_message::ServerMessage::InitResponse(init)) =
            init.server_message
        else {
            anyhow::bail!("PQv1 writer expected InitResponse");
        };
        anyhow::ensure!(
            init.topic == self.config.topic_path,
            "PQv1 writer server initialized unexpected topic '{}'",
            init.topic
        );
        anyhow::ensure!(
            init.supported_codecs.contains(&(Codec::Raw as i32)),
            "PQv1 writer server does not support RAW codec"
        );
        anyhow::ensure!(
            init.block_format_version == 0,
            "PQv1 writer supports only block format 0, server selected {}",
            init.block_format_version
        );
        let mut next_sequence = init
            .last_sequence_number
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PQv1 sequence overflow"))?;
        let mut serializer =
            DeliverySerializer::new(
                &self.config.serializer,
                QueueMessageMode::ValuesOnly,
                &self.delivery_name,
            )?;

        while let Some(delivery) = io.deliveries.recv().await {
            let started = std::time::Instant::now();
            let serialized = serialize_delivery(
                &mut serializer,
                &delivery,
                &self.discovery,
                self.limits.as_ref(),
            )
            .await
            .map_err(|error| anyhow::Error::from(DataPlaneFailure::fatal(error)))?;
            let payload_bytes = serialized.payload_bytes()?;
            let rows = serialized.source_rows;
            let payloads = serialized
                .batches
                .into_iter()
                .flat_map(|batch| batch.messages)
                .map(|message| {
                    anyhow::ensure!(
                        message.key.is_none(),
                        "PQv1 value-only serializer unexpectedly produced a key"
                    );
                    message.value.ok_or_else(|| {
                        anyhow::anyhow!("PQv1 value-only serializer produced a tombstone")
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            if payloads.is_empty() {
                io.events
                    .send(SinkEvent::CommittedThrough(delivery.id))
                    .await
                    .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
                continue;
            }
            let mut sequence_numbers = Vec::with_capacity(payloads.len());
            for _ in &payloads {
                sequence_numbers.push(next_sequence);
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("PQv1 sequence overflow"))?;
            }
            for (sequence, payload) in sequence_numbers.iter().copied().zip(payloads) {
                request_tx
                    .send(write_message(&[sequence], vec![payload])?)
                    .await
                    .map_err(|_| anyhow::anyhow!("PQv1 writer request stream closed"))?;
            }
            let response = next_response(&mut responses, self.config.network_timeout()).await?;
            let Some(streaming_write_server_message::ServerMessage::BatchWriteResponse(ack)) =
                response.server_message
            else {
                anyhow::bail!("PQv1 writer expected BatchWriteResponse");
            };
            let mut acknowledged = ack.sequence_numbers;
            while acknowledged.len() < sequence_numbers.len() {
                let response = next_response(&mut responses, self.config.network_timeout()).await?;
                let Some(streaming_write_server_message::ServerMessage::BatchWriteResponse(ack)) =
                    response.server_message
                else {
                    anyhow::bail!("PQv1 writer expected BatchWriteResponse");
                };
                acknowledged.extend(ack.sequence_numbers);
            }
            validate_ack(&acknowledged, &sequence_numbers)?;
            self.counters.add_rows(rows);
            self.counters.add_bytes(payload_bytes);
            self.counters.add_flush();
            self.counters
                .add_source_messages(delivery.meta.source_messages);
            self.counters.add_busy(started.elapsed());
            io.events
                .send(SinkEvent::CommittedThrough(delivery.id))
                .await
                .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
        }
        Ok(())
    }
}

impl Sink for PqV1Sink {
    fn run(
        self: Box<Self>,
        io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            self.run_session(io)
                .await
                .map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}

fn init_message(config: &PqV1SinkConfig, message_group_id: &str) -> StreamingWriteClientMessage {
    StreamingWriteClientMessage {
        client_message: Some(streaming_write_client_message::ClientMessage::InitRequest(
            streaming_write_client_message::InitRequest {
                topic: config.topic_path.clone(),
                message_group_id: message_group_id.to_owned(),
                partition_group_id: config.partition_group_id,
                max_supported_format_version: 0,
                idle_timeout_ms: i64::try_from(config.network_timeout_ms).unwrap_or(i64::MAX),
                ..Default::default()
            },
        )),
    }
}

fn write_message(
    sequence_numbers: &[i64],
    payloads: Vec<Vec<u8>>,
) -> anyhow::Result<StreamingWriteClientMessage> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let sizes = payloads
        .iter()
        .map(|payload| i64::try_from(payload.len()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StreamingWriteClientMessage {
        client_message: Some(streaming_write_client_message::ClientMessage::WriteRequest(
            streaming_write_client_message::WriteRequest {
                sequence_numbers: sequence_numbers.to_vec(),
                created_at_ms: vec![now; payloads.len()],
                sent_at_ms: vec![now; payloads.len()],
                message_sizes: sizes.clone(),
                blocks_offsets: vec![0; payloads.len()],
                blocks_part_numbers: vec![0; payloads.len()],
                blocks_message_counts: vec![1; payloads.len()],
                blocks_uncompressed_sizes: sizes,
                blocks_headers: vec![vec![Codec::Raw as u8]; payloads.len()],
                blocks_data: payloads,
            },
        )),
    })
}

pub async fn serialize_delivery(
    serializer: &mut DeliverySerializer,
    delivery: &Delivery,
    discovery: &transferia_core::delivery::DeliveryDiscovery,
    limits: &dyn SinkLimits,
) -> anyhow::Result<SerializedDelivery> {
    serializer
        .serialize(delivery, discovery, limits, MAX_GRPC_MESSAGE_SIZE / 2)
        .await
}

async fn next_response(
    responses: &mut tonic::Streaming<StreamingWriteServerMessage>,
    timeout: Duration,
) -> anyhow::Result<StreamingWriteServerMessage> {
    let response = tokio::time::timeout(timeout, responses.try_next())
        .await
        .map_err(|_| anyhow::anyhow!("PQv1 writer response timed out"))??
        .ok_or_else(|| anyhow::anyhow!("PQv1 writer response stream closed"))?;
    anyhow::ensure!(
        matches!(response.status, SUCCESS | UNSPECIFIED),
        "PQv1 writer failed with status {}: {:?}",
        response.status,
        response.issues
    );
    anyhow::ensure!(
        response.issues.is_empty(),
        "PQv1 writer returned issues: {:?}",
        response.issues
    );
    Ok(response)
}

pub(super) fn validate_ack(actual: &[i64], expected: &[i64]) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual.len() == expected.len(),
        "PQv1 writer acknowledged {} sequences, expected {}",
        actual.len(),
        expected.len()
    );
    let actual = actual.iter().copied().collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual == expected,
        "PQv1 writer acknowledgement sequence set differs from request"
    );
    Ok(())
}
