#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use futures_util::stream;
use http_body_util::{BodyExt as _, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use transferia::core::delivery::DeliveryDiscoveryRequest;
use transferia::core::memory::PipelineMemory;
use transferia::metrics::MetricsRegistry;
use transferia::providers::logbroker::pqv1::src_stream::PqV1SourceProvider;
use transferia::providers::logbroker::proto::discovery::{EndpointInfo, ListEndpointsResult};
use transferia::providers::logbroker::proto::operations::{GetOperationResponse, Operation};
use transferia::providers::logbroker::proto::pers_queue::v1::{
    migration_streaming_read_client_message, migration_streaming_read_server_message,
    AutoPartitioningSettings, AutoPartitioningStrategy, Codec, CommitCookie, DescribeTopicResponse,
    DescribeTopicResult, MigrationStreamingReadClientMessage, MigrationStreamingReadServerMessage,
    Path, TopicSettings,
};
use transferia::registry::{SourceBuildContext, SourceDiscoveryContext, SourceProvider};

const TOPIC: &str = "/Root/e2e-topic";
const CONSUMER: &str = "e2e-consumer";
const TOKEN: &str = "e2e-token";
const YDB_STATUS_SUCCESS: i32 = 400_000;

type TestBody = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;

struct PqFixture {
    address: SocketAddr,
    committed: Arc<AtomicBool>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl PqFixture {
    async fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let committed = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_committed = Arc::clone(&committed);
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    biased;
                    () = server_shutdown.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, _peer)) = accepted else {
                    break;
                };
                let committed = Arc::clone(&server_committed);
                tokio::spawn(async move {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = service_fn(move |request| {
                        let committed = Arc::clone(&committed);
                        async move { Ok::<_, Infallible>(handle_request(request, address, committed)) }
                    });
                    drop(
                        http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                            .serve_connection(io, service)
                            .await,
                    );
                });
            }
        });
        Ok(Self {
            address,
            committed,
            shutdown,
            task,
        })
    }

    async fn stop(self) {
        self.shutdown.cancel();
        drop(self.task.await);
    }
}

fn grpc_frame(message: &impl prost::Message) -> Bytes {
    let payload = message.encode_to_vec();
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&payload);
    Bytes::from(frame)
}

fn grpc_body(messages: impl IntoIterator<Item = Bytes>) -> TestBody {
    let mut frames: Vec<Result<Frame<Bytes>, Infallible>> = messages
        .into_iter()
        .map(|message| Ok(Frame::data(message)))
        .collect();
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    frames.push(Ok(Frame::trailers(trailers)));
    StreamBody::new(stream::iter(frames)).boxed_unsync()
}

fn grpc_response(messages: impl IntoIterator<Item = Bytes>) -> Response<TestBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .body(grpc_body(messages))
        .unwrap()
}

fn operation(result: &impl prost::Message, type_url: &str) -> Operation {
    Operation {
        ready: true,
        status: YDB_STATUS_SUCCESS,
        result: Some(prost_types::Any {
            type_url: type_url.to_owned(),
            value: result.encode_to_vec(),
        }),
        ..Default::default()
    }
}

fn handle_request(
    request: Request<Incoming>,
    address: SocketAddr,
    committed: Arc<AtomicBool>,
) -> Response<TestBody> {
    assert_eq!(
        request
            .headers()
            .get("x-ydb-auth-ticket")
            .and_then(|value| value.to_str().ok()),
        Some(TOKEN)
    );
    assert_eq!(
        request
            .headers()
            .get("x-ydb-database")
            .and_then(|value| value.to_str().ok()),
        Some("/Root")
    );

    let response = match request.uri().path() {
        "/Ydb.Discovery.V1.DiscoveryService/ListEndpoints" => {
            let result = ListEndpointsResult {
                endpoints: vec![EndpointInfo {
                    address: address.ip().to_string(),
                    port: u32::from(address.port()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            grpc_response([grpc_frame(&GetOperationResponse {
                operation: Some(operation(
                    &result,
                    "type.googleapis.com/Ydb.Discovery.ListEndpointsResult",
                )),
            })])
        }
        "/Ydb.PersQueue.V1.PersQueueService/DescribeTopic" => {
            let result = DescribeTopicResult {
                settings: Some(TopicSettings {
                    partitions_count: 1,
                    auto_partitioning_settings: Some(AutoPartitioningSettings {
                        strategy: AutoPartitioningStrategy::Disabled as i32,
                        ..Default::default()
                    }),
                    read_rules: vec![topic_settings::ReadRule {
                        consumer_name: CONSUMER.to_owned(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            };
            grpc_response([grpc_frame(&DescribeTopicResponse {
                operation: Some(operation(
                    &result,
                    "type.googleapis.com/Ydb.PersQueue.V1.DescribeTopicResult",
                )),
            })])
        }
        "/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead" => {
            streaming_read_response(request.into_body(), committed)
        }
        path => panic!("unexpected PQv1 gRPC path: {path}"),
    };
    response
}

fn streaming_read_response(body: Incoming, committed: Arc<AtomicBool>) -> Response<TestBody> {
    let (response_tx, response_rx) = mpsc::unbounded_channel::<Result<Frame<Bytes>, Infallible>>();
    tokio::spawn(async move {
        process_stream_requests(body, &response_tx, committed).await;
    });
    let body = StreamBody::new(stream::unfold(response_rx, |mut receiver| async move {
        receiver.recv().await.map(|frame| (frame, receiver))
    }))
    .boxed_unsync();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .body(body)
        .unwrap()
}

async fn process_stream_requests(
    mut body: Incoming,
    response_tx: &mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>,
    committed: Arc<AtomicBool>,
) {
    let mut buffered = BytesMut::new();
    let mut sent_batch = false;
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            return;
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        buffered.extend_from_slice(&data);
        while let Some(message) =
            decode_grpc_message::<MigrationStreamingReadClientMessage>(&mut buffered)
        {
            match message.request {
                Some(migration_streaming_read_client_message::Request::InitRequest(init)) => {
                    assert_eq!(message.token, TOKEN.as_bytes());
                    assert_eq!(init.consumer, CONSUMER);
                    assert_eq!(init.topics_read_settings[0].topic, TOPIC);
                    send_server_message(
                        response_tx,
                        migration_streaming_read_server_message::Response::InitResponse(
                            migration_streaming_read_server_message::InitResponse {
                                session_id: "e2e-session".to_owned(),
                                ..Default::default()
                            },
                        ),
                    );
                    send_server_message(
                        response_tx,
                        migration_streaming_read_server_message::Response::Assigned(
                            migration_streaming_read_server_message::Assigned {
                                topic: Some(Path {
                                    path: TOPIC.to_owned(),
                                }),
                                cluster: "e2e-cluster".to_owned(),
                                partition: 0,
                                assign_id: 17,
                                read_offset: 0,
                                end_offset: 1,
                            },
                        ),
                    );
                }
                Some(migration_streaming_read_client_message::Request::Read(_)) if !sent_batch => {
                    sent_batch = true;
                    send_server_message(
                        response_tx,
                        migration_streaming_read_server_message::Response::DataBatch(data_batch()),
                    );
                }
                Some(migration_streaming_read_client_message::Request::Commit(commit)) => {
                    assert_eq!(commit.cookies.len(), 1);
                    committed.store(true, Ordering::SeqCst);
                    send_server_message(
                        response_tx,
                        migration_streaming_read_server_message::Response::Committed(
                            migration_streaming_read_server_message::Committed {
                                cookies: commit.cookies,
                                offset_ranges: Vec::new(),
                            },
                        ),
                    );
                }
                Some(
                    migration_streaming_read_client_message::Request::StartRead(_)
                    | migration_streaming_read_client_message::Request::Read(_),
                ) => {}
                other => panic!("unexpected PQv1 stream request: {other:?}"),
            }
        }
    }
}

fn decode_grpc_message<M: prost::Message + Default>(buffer: &mut BytesMut) -> Option<M> {
    if buffer.len() < 5 {
        return None;
    }
    assert_eq!(buffer[0], 0, "compressed test gRPC frames are unsupported");
    let length = u32::from_be_bytes(buffer[1..5].try_into().unwrap()) as usize;
    if buffer.len() < length + 5 {
        return None;
    }
    buffer.advance(5);
    let payload = buffer.split_to(length);
    Some(M::decode(payload).unwrap())
}

fn send_server_message(
    response_tx: &mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>,
    response: migration_streaming_read_server_message::Response,
) {
    response_tx
        .send(Ok(Frame::data(grpc_frame(
            &MigrationStreamingReadServerMessage {
                status: YDB_STATUS_SUCCESS,
                response: Some(response),
                ..Default::default()
            },
        ))))
        .unwrap();
}

fn data_batch() -> migration_streaming_read_server_message::DataBatch {
    migration_streaming_read_server_message::DataBatch {
        partition_data: vec![
            migration_streaming_read_server_message::data_batch::PartitionData {
                topic: Some(Path {
                    path: TOPIC.to_owned(),
                }),
                cluster: "e2e-cluster".to_owned(),
                partition: 0,
                batches: vec![migration_streaming_read_server_message::data_batch::Batch {
                    write_timestamp_ms: 1_700_000_000_000,
                    message_data: vec![
                        migration_streaming_read_server_message::data_batch::MessageData {
                            offset: 0,
                            codec: Codec::Raw as i32,
                            data: br#"{"id":42}"#.to_vec(),
                            uncompressed_size: 9,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                cookie: Some(CommitCookie {
                    assign_id: 17,
                    partition_cookie: 23,
                }),
                ..Default::default()
            },
        ],
    }
}

#[tokio::test]
async fn pqv1_source_discovers_reads_and_commits_over_real_grpc() -> anyhow::Result<()> {
    let fixture = PqFixture::start().await?;
    let config = serde_yaml::from_str(&format!(
        r"
host: {}
port: {}
topic_path: {TOPIC}
consumer_name: {CONSUMER}
partition_group_ids: [0]
auth:
  type: access_token
  token: {TOKEN}
network_timeout_ms: 3000
benchmark_discard_before_decompression: true
parser:
  common:
    table_naming: {{ type: from_config, name: events }}
  benchmark_discard: {{}}
",
        fixture.address.ip(),
        fixture.address.port()
    ))?;
    let provider = PqV1SourceProvider::from_config(config, Arc::new(MetricsRegistry::new()))?;
    let cancellation = CancellationToken::new();

    let discovery = provider
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: cancellation.child_token(),
        })
        .await?;
    assert_eq!(
        discovery.source_topology,
        transferia::core::delivery::SourceTopology::StaticPartitions(vec![0])
    );

    let mut source = provider
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: cancellation.child_token(),
            memory: PipelineMemory::new(16 * 1024 * 1024),
            durable: support::durable_context(),
        })
        .await?;
    let batch =
        tokio::time::timeout(core::time::Duration::from_secs(3), source.read_batch()).await??;
    let transferia::core::data::message::SourceBatch::Raw {
        messages,
        commit_marker,
        ..
    } = batch
    else {
        panic!("expected raw PQv1 batch");
    };
    assert!(messages.is_empty());
    let marker = commit_marker.expect("PQv1 DataBatch must be committable");
    source.commit_offsets(&[marker]).await?;
    assert!(fixture.committed.load(Ordering::SeqCst));

    cancellation.cancel();
    fixture.stop().await;
    Ok(())
}

mod topic_settings {
    pub use transferia::providers::logbroker::proto::pers_queue::v1::topic_settings::ReadRule;
}
