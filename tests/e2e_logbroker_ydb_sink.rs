#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::convert::Infallible;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
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
use transferia::core::data::schema::{DatasetSchema, SchemaColumn};
use transferia::core::data::system_columns::SystemColumns;
use transferia::core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin};
use transferia::core::memory::PipelineMemory;
use transferia::core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia::metrics::SinkCounters;
use transferia::providers::logbroker::build_sink_provider;
use transferia::providers::traits::SinkBuildContext;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_response::write_ack;
use ydb_grpc::ydb_proto::topic::stream_write_message::{
    write_response, FromClient, FromServer, InitResponse, WriteResponse,
};
use ydb_grpc::ydb_proto::topic::{Codec, SupportedCodecs};

const TOKEN: &str = "ydb-topic-sink-token";
const TOPIC: &str = "/Root/output-topic";
const SUCCESS: i32 = 400_000;
type TestBody = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;

#[tokio::test]
async fn logbroker_ydb_sink_commits_only_after_real_grpc_ack() -> anyhow::Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let server_observed = Arc::clone(&observed);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let io = hyper_util::rt::TokioIo::new(stream);
        let service = service_fn(move |request| {
            let observed = Arc::clone(&server_observed);
            async move { Ok::<_, Infallible>(handle_write(request, observed)) }
        });
        drop(
            http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(io, service)
                .await,
        );
    });

    let provider = build_sink_provider(serde_yaml::from_str(&format!(
        "host: '{}'\nport: {}\ntopic_path: '{TOPIC}'\nproducer_id: transferia-e2e\nauth: {{ type: token, token: {TOKEN} }}\ndriver: ydb\ntrusted_plaintext: true\n",
        address.ip(),
        address.port()
    ))?)?;
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("name".into(), DataType::Utf8, true),
    ]);
    let discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("typed-e2e"),
        source_topology: transferia::core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
    });
    provider.limits().validate_discovery(&discovery)?;
    let sink = provider
        .build_sink(SinkBuildContext {
            durable: support::durable_context(),
            partition_id: 0,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
        })
        .await?;
    let memory = PipelineMemory::new(1024 * 1024);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("one"), None])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let actor = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from("events"),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve_transform(bytes),
                system_columns: SystemColumns::default(),
            }],
            meta: DeliveryMeta { source_messages: 2 },
        })
        .await?;
    drop(delivery_tx);
    assert_eq!(
        event_rx.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    actor.await??;
    server.await?;
    assert_eq!(
        *observed.lock().unwrap(),
        [
            b"{\"id\":1,\"name\":\"one\"}\n".to_vec(),
            b"{\"id\":2,\"name\":null}\n".to_vec()
        ]
    );
    Ok(())
}

fn handle_write(
    request: Request<Incoming>,
    observed: Arc<Mutex<Vec<Vec<u8>>>>,
) -> Response<TestBody> {
    assert_eq!(
        request.uri().path(),
        "/Ydb.Topic.V1.TopicService/StreamWrite"
    );
    assert_eq!(request.headers().get("x-ydb-auth-ticket").unwrap(), TOKEN);
    let (response_tx, response_rx) = mpsc::unbounded_channel::<Result<Frame<Bytes>, Infallible>>();
    tokio::spawn(async move {
        process_requests(request.into_body(), response_tx, observed).await;
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .body(
            StreamBody::new(stream::unfold(response_rx, |mut receiver| async move {
                receiver.recv().await.map(|frame| (frame, receiver))
            }))
            .boxed_unsync(),
        )
        .unwrap()
}

async fn process_requests(
    mut body: Incoming,
    response_tx: mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>,
    observed: Arc<Mutex<Vec<Vec<u8>>>>,
) {
    let mut buffered = BytesMut::new();
    while let Some(Ok(frame)) = body.frame().await {
        let Ok(data) = frame.into_data() else {
            continue;
        };
        buffered.extend_from_slice(&data);
        while let Some(message) = decode_grpc::<FromClient>(&mut buffered) {
            match message.client_message {
                Some(ClientMessage::InitRequest(init)) => {
                    assert_eq!(init.path, TOPIC);
                    assert_eq!(init.producer_id, "transferia-e2e");
                    send(
                        &response_tx,
                        &FromServer {
                            status: SUCCESS,
                            issues: Vec::new(),
                            server_message: Some(ServerMessage::InitResponse(InitResponse {
                                last_seq_no: 0,
                                session_id: "test-session".into(),
                                partition_id: 0,
                                supported_codecs: Some(SupportedCodecs {
                                    codecs: vec![Codec::Raw as i32],
                                }),
                            })),
                        },
                    );
                }
                Some(ClientMessage::WriteRequest(write)) => {
                    let acks = write
                        .messages
                        .into_iter()
                        .map(|message| {
                            observed.lock().unwrap().push(message.data);
                            write_response::WriteAck {
                                seq_no: message.seq_no,
                                message_write_status: Some(write_ack::MessageWriteStatus::Written(
                                    write_ack::Written {
                                        offset: message.seq_no,
                                    },
                                )),
                            }
                        })
                        .collect();
                    send(
                        &response_tx,
                        &FromServer {
                            status: SUCCESS,
                            issues: Vec::new(),
                            server_message: Some(ServerMessage::WriteResponse(WriteResponse {
                                acks,
                                partition_id: 0,
                                write_statistics: None,
                            })),
                        },
                    );
                }
                _ => panic!("unexpected YDB Topic write message"),
            }
        }
    }
}

fn send(tx: &mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>, message: &FromServer) {
    tx.send(Ok(Frame::data(grpc_frame(message)))).unwrap();
}

fn grpc_frame(message: &impl prost::Message) -> Bytes {
    let payload = message.encode_to_vec();
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&payload);
    Bytes::from(frame)
}

fn decode_grpc<T: prost::Message + Default>(buffer: &mut BytesMut) -> Option<T> {
    if buffer.len() < 5 {
        return None;
    }
    let len = usize::try_from(u32::from_be_bytes(buffer[1..5].try_into().unwrap())).unwrap();
    if buffer.len() < 5 + len {
        return None;
    }
    buffer.advance(5);
    Some(T::decode(buffer.split_to(len)).unwrap())
}
