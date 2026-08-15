use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use futures_util::{Stream, TryStreamExt as _};
use tokio::sync::mpsc;
use tonic::Request;

use crate::delivery::SinkLimits;
use crate::metrics::SinkCounters;
use crate::pipeline::sink::{Delivery, Sink, SinkEvent, SinkIo};
use crate::pipeline::PipelineFailure;
use crate::providers::pqv1::config::PqV1SinkConfig;
use crate::providers::pqv1::pq_v1::{
    connect_http2_prior_knowledge, http_uri, parse_endpoint, set_ydb_headers,
};
use crate::providers::traits::SinkContext;
use crate::serializer::JsonBatchEncoder;
use crate::Ydb::pers_queue::v1::{
    streaming_write_client_message, streaming_write_server_message, Codec,
    StreamingWriteClientMessage, StreamingWriteServerMessage,
};

const SUCCESS: i32 = 400_000;
const UNSPECIFIED: i32 = 0;
pub const MAX_GRPC_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

pub(super) struct PqV1Sink {
    config: Arc<PqV1SinkConfig>,
    token: Arc<str>,
    counters: Arc<SinkCounters>,
    discovery: Arc<crate::delivery::DeliveryDiscovery>,
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
    pub(super) fn new(config: Arc<PqV1SinkConfig>, token: Arc<str>, context: SinkContext) -> Self {
        let limits: Arc<dyn SinkLimits> = Arc::clone(&config) as Arc<dyn SinkLimits>;
        Self {
            config,
            token,
            counters: context.counters,
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
            .send(init_message(&self.config))
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

        while let Some(delivery) = io.deliveries.recv().await {
            let started = std::time::Instant::now();
            let (payloads, rows) =
                serialize_delivery(&delivery, &self.discovery, self.limits.as_ref())
                    .map_err(|error| anyhow::Error::from(PipelineFailure::fatal(error)))?;
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
            self.counters.add_bytes(u64::try_from(
                delivery
                    .outputs
                    .iter()
                    .map(|batch| batch.byte_size)
                    .sum::<usize>(),
            )?);
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
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_session(io).await })
    }
}

fn init_message(config: &PqV1SinkConfig) -> StreamingWriteClientMessage {
    StreamingWriteClientMessage {
        client_message: Some(streaming_write_client_message::ClientMessage::InitRequest(
            streaming_write_client_message::InitRequest {
                topic: config.topic_path.clone(),
                message_group_id: config.message_group_id.clone(),
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

pub fn serialize_delivery(
    delivery: &Delivery,
    discovery: &crate::delivery::DeliveryDiscovery,
    limits: &dyn SinkLimits,
) -> anyhow::Result<(Vec<Vec<u8>>, u64)> {
    let mut payloads = Vec::new();
    let mut rows = 0_u64;
    for batch in &delivery.outputs {
        limits.validate_batch(discovery, batch)?;
        let encoder = JsonBatchEncoder::new(&batch.batch, |index| {
            !batch
                .system_columns
                .iter()
                .any(|column| column.index == index)
        })?;
        for row in 0..batch.rows() {
            let mut output = Vec::new();
            encoder.write_row(row, &mut output);
            anyhow::ensure!(
                output.len() <= MAX_GRPC_MESSAGE_SIZE / 2,
                "Logbroker serialized JSON message exceeds {} bytes",
                MAX_GRPC_MESSAGE_SIZE / 2
            );
            payloads.push(output);
        }
        rows = rows
            .checked_add(batch.rows() as u64)
            .ok_or_else(|| anyhow::anyhow!("Logbroker sink row counter overflow"))?;
    }
    Ok((payloads, rows))
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
