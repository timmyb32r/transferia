use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use futures_util::{Stream, TryStreamExt as _};
use tokio::sync::mpsc;
use tonic::Request;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::init_request::Partitioning;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_response::write_ack::{
    skipped, MessageWriteStatus,
};
use ydb_grpc::ydb_proto::topic::stream_write_message::{
    write_request, FromClient, FromServer, InitRequest, WriteRequest,
};
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use ydb_grpc::ydb_proto::topic::Codec;

use super::config::LogbrokerSinkConfig;
use crate::delivery::execution::sink::{Sink, SinkEvent, SinkIo};
use crate::delivery::execution::PipelineFailure;
use crate::delivery::SinkLimits;
use crate::metrics::SinkCounters;
use crate::providers::logbroker::pqv1::pq_v1::set_ydb_headers;
use crate::providers::logbroker::pqv1::sink::writer::{serialize_delivery, MAX_GRPC_MESSAGE_SIZE};
use crate::providers::traits::SinkContext;
use crate::providers::ydb_transport::connect_http2_prior_knowledge;

const NETWORK_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(10);
const WRITE_PAYLOAD_BUDGET: usize = MAX_GRPC_MESSAGE_SIZE / 2;

pub(super) struct YdbTopicSink {
    config: Arc<LogbrokerSinkConfig>,
    token: Arc<str>,
    counters: Arc<SinkCounters>,
    discovery: Arc<crate::delivery::DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
}

struct RequestStream(mpsc::Receiver<FromClient>);

impl Stream for RequestStream {
    type Item = FromClient;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context)
    }
}

impl YdbTopicSink {
    pub(super) fn new(
        config: Arc<LogbrokerSinkConfig>,
        token: Arc<str>,
        context: SinkContext,
    ) -> Self {
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
        let uri: http::Uri =
            crate::providers::address::url("http", &self.config.host, self.config.port)
                .parse()
                .map_err(|error| anyhow::anyhow!("Invalid YDB Topic sink endpoint: {error}"))?;
        let transport =
            connect_http2_prior_knowledge(&uri, NETWORK_TIMEOUT, &io.cancellation).await?;
        let mut client = TopicServiceClient::with_origin(transport, uri)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE);
        let (request_tx, request_rx) = mpsc::channel(2);
        let mut request = Request::new(RequestStream(request_rx));
        set_ydb_headers(request.metadata_mut(), &self.token)?;
        let mut responses = tokio::time::timeout(NETWORK_TIMEOUT, client.stream_write(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB Topic writer connect timed out"))??
            .into_inner();
        request_tx
            .send(init_message(&self.config))
            .await
            .map_err(|_| anyhow::anyhow!("YDB Topic writer request stream closed before init"))?;
        let init = next_response(&mut responses).await?;
        let Some(ServerMessage::InitResponse(init)) = init.server_message else {
            anyhow::bail!("YDB Topic writer expected InitResponse");
        };
        if let Some(codecs) = init.supported_codecs {
            anyhow::ensure!(
                codecs.codecs.contains(&(Codec::Raw as i32)),
                "YDB Topic writer server does not support RAW codec"
            );
        }
        let mut next_sequence = init
            .last_seq_no
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("YDB Topic sequence overflow"))?;

        while let Some(delivery) = io.deliveries.recv().await {
            let started = std::time::Instant::now();
            let (payloads, rows) =
                serialize_delivery(&delivery, &self.discovery, self.limits.as_ref())
                    .map_err(|error| anyhow::Error::from(PipelineFailure::fatal(error)))?;
            let mut payloads = payloads.into_iter().peekable();
            while payloads.peek().is_some() {
                let mut request_payload_bytes = 0_usize;
                let mut messages = Vec::new();
                let mut expected_sequences = Vec::new();
                while let Some(payload) = payloads.peek() {
                    if !messages.is_empty()
                        && request_payload_bytes.saturating_add(payload.len())
                            > WRITE_PAYLOAD_BUDGET
                    {
                        break;
                    }
                    let payload = payloads
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("YDB Topic payload stream ended early"))?;
                    request_payload_bytes = request_payload_bytes
                        .checked_add(payload.len())
                        .ok_or_else(|| anyhow::anyhow!("YDB Topic write size overflow"))?;
                    let sequence = next_sequence;
                    next_sequence = next_sequence
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("YDB Topic sequence overflow"))?;
                    expected_sequences.push(sequence);
                    messages.push((sequence, payload));
                }
                request_tx
                    .send(write_message(messages)?)
                    .await
                    .map_err(|_| anyhow::anyhow!("YDB Topic writer request stream closed"))?;
                let response = next_response(&mut responses).await?;
                let Some(ServerMessage::WriteResponse(response)) = response.server_message else {
                    anyhow::bail!("YDB Topic writer expected WriteResponse");
                };
                for ack in &response.acks {
                    match ack.message_write_status {
                        Some(MessageWriteStatus::Written(_)) => {}
                        Some(MessageWriteStatus::Skipped(skipped)) => anyhow::ensure!(
                            skipped.reason == skipped::Reason::AlreadyWritten as i32,
                            "YDB Topic writer skipped sequence {} for an unexpected reason",
                            ack.seq_no
                        ),
                        Some(MessageWriteStatus::WrittenInTx(_)) => anyhow::bail!(
                            "YDB Topic writer acknowledged sequence {} inside an unexpected transaction",
                            ack.seq_no
                        ),
                        None => anyhow::bail!(
                            "YDB Topic writer acknowledgement for sequence {} has no status",
                            ack.seq_no
                        ),
                    }
                }
                validate_ack(
                    &response
                        .acks
                        .iter()
                        .map(|ack| ack.seq_no)
                        .collect::<Vec<_>>(),
                    &expected_sequences,
                )?;
            }
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

impl Sink for YdbTopicSink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_session(io).await })
    }
}

fn init_message(config: &LogbrokerSinkConfig) -> FromClient {
    FromClient {
        client_message: Some(ClientMessage::InitRequest(InitRequest {
            path: config.topic_path.clone(),
            producer_id: config.producer_id.clone(),
            get_last_seq_no: true,
            partitioning: Some(config.partition_id.map_or_else(
                || Partitioning::MessageGroupId(config.producer_id.clone()),
                Partitioning::PartitionId,
            )),
            ..Default::default()
        })),
    }
}

fn write_message(messages: Vec<(i64, Vec<u8>)>) -> anyhow::Result<FromClient> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let created_at = ydb_grpc::google_proto_workaround::protobuf::Timestamp {
        seconds: i64::try_from(now.as_secs())?,
        nanos: i32::try_from(now.subsec_nanos())?,
    };
    Ok(FromClient {
        client_message: Some(ClientMessage::WriteRequest(WriteRequest {
            messages: messages
                .into_iter()
                .map(|(sequence, payload)| {
                    Ok(write_request::MessageData {
                        seq_no: sequence,
                        created_at: Some(created_at),
                        uncompressed_size: i64::try_from(payload.len())?,
                        data: payload,
                        ..Default::default()
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            codec: Codec::Raw as i32,
            tx: None,
        })),
    })
}

async fn next_response(responses: &mut tonic::Streaming<FromServer>) -> anyhow::Result<FromServer> {
    let response = tokio::time::timeout(NETWORK_TIMEOUT, responses.try_next())
        .await
        .map_err(|_| anyhow::anyhow!("YDB Topic writer response timed out"))??
        .ok_or_else(|| anyhow::anyhow!("YDB Topic writer response stream closed"))?;
    anyhow::ensure!(
        matches!(
            StatusCode::try_from(response.status),
            Ok(StatusCode::Success | StatusCode::Unspecified)
        ),
        "YDB Topic writer failed with status {}: {:?}",
        response.status,
        response.issues
    );
    anyhow::ensure!(
        response.issues.is_empty(),
        "YDB Topic writer returned issues: {:?}",
        response.issues
    );
    Ok(response)
}

fn validate_ack(actual: &[i64], expected: &[i64]) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual.len() == expected.len(),
        "YDB Topic writer acknowledged {} sequences, expected {}",
        actual.len(),
        expected.len()
    );
    let actual = actual.iter().copied().collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual == expected,
        "YDB Topic writer acknowledgement sequence set differs from request"
    );
    Ok(())
}
