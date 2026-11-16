//! One YDB Topic write session over raw messages.

use std::collections::BTreeSet;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{Stream, TryStreamExt as _};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
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

use crate::connectors::logbroker::pqv1::sink::writer::MAX_GRPC_MESSAGE_SIZE;
use crate::connectors::logbroker::transport::{
    connect_http2_prior_knowledge, set_ydb_request_metadata,
};

/// Payload and framing carried by one write request; a larger write is split
/// into several.
const WRITE_REQUEST_BUDGET: usize = MAX_GRPC_MESSAGE_SIZE / 2;

/// Upper bound of what one message adds to a request besides its payload.
const MESSAGE_OVERHEAD_BYTES: usize = 64;

/// Largest message that is certain to fit one write request.
pub const MAX_MESSAGE_BYTES: usize = WRITE_REQUEST_BUDGET;

/// Compression of the messages of one session.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicCodec {
    Raw,
    Gzip,
    Zstd,
}

impl TopicCodec {
    const fn wire(self) -> Codec {
        match self {
            Self::Raw => Codec::Raw,
            Self::Gzip => Codec::Gzip,
            Self::Zstd => Codec::Zstd,
        }
    }
}

/// Where and as whom one session writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicWriterConfig {
    pub host: String,

    pub port: u16,

    /// Database the session is routed to.
    pub database: String,

    pub topic: String,

    /// The server deduplicates by producer identity and sequence number, so
    /// concurrent sessions must not share an identity.
    pub producer_id: String,

    /// Explicit partition; `None` routes by producer identity.
    pub partition_id: Option<i64>,

    /// Applied to every message; the session fails to start when the topic
    /// does not support it.
    pub codec: TopicCodec,

    /// Bound of the connection, of the session start and of each acknowledgement.
    pub network_timeout: Duration,
}

struct RequestStream(mpsc::Receiver<FromClient>);

impl Stream for RequestStream {
    type Item = FromClient;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context)
    }
}

/// An initialized write session. A failed or interrupted write leaves the
/// session unusable; start a new one.
pub struct TopicWriter {
    request_tx: mpsc::Sender<FromClient>,
    responses: tonic::Streaming<FromServer>,
    next_sequence: i64,
    codec: TopicCodec,
    network_timeout: Duration,
}

impl TopicWriter {
    /// Opens a plaintext session; the caller owns the trust decision.
    pub async fn connect(
        config: &TopicWriterConfig,
        token: &str,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Self> {
        let uri: http::Uri = crate::connectors::address::url("http", &config.host, config.port)
            .parse()
            .map_err(|error| anyhow::anyhow!("Invalid YDB Topic sink endpoint: {error}"))?;
        let transport =
            connect_http2_prior_knowledge(&uri, config.network_timeout, cancellation).await?;
        let mut client = TopicServiceClient::with_origin(transport, uri)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE);
        let (request_tx, request_rx) = mpsc::channel(2);
        // Queued before the call: a server may answer only after it has read it.
        request_tx
            .send(init_message(
                &config.topic,
                config.partition_id,
                &config.producer_id,
            ))
            .await
            .map_err(|_| anyhow::anyhow!("YDB Topic writer request stream closed before init"))?;
        let mut request = Request::new(RequestStream(request_rx));
        set_ydb_request_metadata(request.metadata_mut(), token, &config.database)?;
        let mut responses =
            tokio::time::timeout(config.network_timeout, client.stream_write(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB Topic writer connect timed out"))??
                .into_inner();
        let init = next_response(&mut responses, config.network_timeout).await?;
        let Some(ServerMessage::InitResponse(init)) = init.server_message else {
            anyhow::bail!("YDB Topic writer expected InitResponse");
        };
        if let Some(codecs) = init.supported_codecs {
            let codec = config.codec.wire();
            anyhow::ensure!(
                codecs.codecs.contains(&(codec as i32)),
                "YDB Topic writer server does not support {} codec",
                codec.as_str_name().trim_start_matches("CODEC_")
            );
        }
        let next_sequence = init
            .last_seq_no
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("YDB Topic sequence overflow"))?;
        Ok(Self {
            request_tx,
            responses,
            next_sequence,
            codec: config.codec,
            network_timeout: config.network_timeout,
        })
    }

    /// Writes the messages in order and waits until all are acknowledged.
    pub async fn write(&mut self, messages: Vec<Vec<u8>>) -> anyhow::Result<()> {
        let mut messages = encode(self.codec, messages).await?.into_iter();
        while messages.len() > 0 {
            let count =
                request_message_count(messages.as_slice().iter().map(|message| message.data.len()));
            let mut request = Vec::with_capacity(count);
            let mut expected_sequences = Vec::with_capacity(count);
            for message in messages.by_ref().take(count) {
                let sequence = self.next_sequence;
                self.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("YDB Topic sequence overflow"))?;
                expected_sequences.push(sequence);
                request.push((sequence, message));
            }
            self.request_tx
                .send(write_message(self.codec, request)?)
                .await
                .map_err(|_| anyhow::anyhow!("YDB Topic writer request stream closed"))?;
            let response = next_response(&mut self.responses, self.network_timeout).await?;
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
        Ok(())
    }
}

struct EncodedMessage {
    data: Vec<u8>,
    uncompressed_size: usize,
}

/// How many of the leading messages one request takes: the first one always,
/// then as many as fit the request budget.
fn request_message_count(payload_sizes: impl Iterator<Item = usize>) -> usize {
    let mut bytes = 0_usize;
    let mut count = 0_usize;
    for size in payload_sizes {
        bytes = bytes
            .saturating_add(size)
            .saturating_add(MESSAGE_OVERHEAD_BYTES);
        if count > 0 && bytes > WRITE_REQUEST_BUDGET {
            break;
        }
        count = count.saturating_add(1);
    }
    count
}

fn compress(codec: TopicCodec, payload: Vec<u8>) -> anyhow::Result<EncodedMessage> {
    let uncompressed_size = payload.len();
    let data = match codec {
        TopicCodec::Raw => payload,
        TopicCodec::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&payload)?;
            encoder.finish()?
        }
        TopicCodec::Zstd => zstd::bulk::compress(&payload, zstd::DEFAULT_COMPRESSION_LEVEL)?,
    };
    Ok(EncodedMessage {
        data,
        uncompressed_size,
    })
}

type Payloads = Mutex<Option<Vec<Vec<u8>>>>;

fn take_payloads(payloads: &Payloads) -> Vec<Vec<u8>> {
    payloads
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .unwrap_or_default()
}

/// Takes the payloads of a compression job back when the write that waits for
/// it is cancelled, so a job still queued on the blocking pool holds nothing.
struct Reclaim(Arc<Payloads>);

impl Drop for Reclaim {
    fn drop(&mut self) {
        drop(take_payloads(&self.0));
    }
}

fn spawn_compression(
    codec: TopicCodec,
    messages: Vec<Vec<u8>>,
) -> (
    tokio::task::JoinHandle<anyhow::Result<Vec<EncodedMessage>>>,
    Reclaim,
) {
    let payloads = Arc::new(Mutex::new(Some(messages)));
    let reclaim = Reclaim(Arc::clone(&payloads));
    let job = tokio::task::spawn_blocking(move || {
        take_payloads(&payloads)
            .into_iter()
            .map(|payload| compress(codec, payload))
            .collect()
    });
    (job, reclaim)
}

/// Compression runs off the async executor; RAW passes the payloads through.
async fn encode(codec: TopicCodec, messages: Vec<Vec<u8>>) -> anyhow::Result<Vec<EncodedMessage>> {
    if codec == TopicCodec::Raw {
        return messages
            .into_iter()
            .map(|payload| compress(codec, payload))
            .collect();
    }
    let (job, _reclaim) = spawn_compression(codec, messages);
    job.await
        .map_err(|error| anyhow::anyhow!("YDB Topic message compression task failed: {error}"))?
}

fn init_message(topic: &str, partition_id: Option<i64>, producer_id: &str) -> FromClient {
    FromClient {
        client_message: Some(ClientMessage::InitRequest(InitRequest {
            path: topic.to_owned(),
            producer_id: producer_id.to_owned(),
            get_last_seq_no: true,
            partitioning: Some(partition_id.map_or_else(
                || Partitioning::MessageGroupId(producer_id.to_owned()),
                Partitioning::PartitionId,
            )),
            ..Default::default()
        })),
    }
}

fn write_message(
    codec: TopicCodec,
    messages: Vec<(i64, EncodedMessage)>,
) -> anyhow::Result<FromClient> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let created_at = ydb_grpc::google_proto_workaround::protobuf::Timestamp {
        seconds: i64::try_from(now.as_secs())?,
        nanos: i32::try_from(now.subsec_nanos())?,
    };
    Ok(FromClient {
        client_message: Some(ClientMessage::WriteRequest(WriteRequest {
            messages: messages
                .into_iter()
                .map(|(sequence, message)| {
                    Ok(write_request::MessageData {
                        seq_no: sequence,
                        created_at: Some(created_at),
                        uncompressed_size: i64::try_from(message.uncompressed_size)?,
                        data: message.data,
                        ..Default::default()
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            codec: codec.wire() as i32,
            tx: None,
        })),
    })
}

async fn next_response(
    responses: &mut tonic::Streaming<FromServer>,
    timeout: Duration,
) -> anyhow::Result<FromServer> {
    let response = tokio::time::timeout(timeout, responses.try_next())
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

#[cfg(test)]
#[path = "tests/topic_writer.rs"]
mod tests;
