use bytes::Bytes;
use tonic::metadata::MetadataMap;

use super::*;

fn decode_parts(
    parts: Vec<RawPart>,
    reservation: &MemoryReservation,
    counters: &SourceCounters,
) -> anyhow::Result<Vec<DecodedPart>> {
    decode_parts_with_cancellation(parts, reservation, counters, &CancellationToken::new())
}

fn decompress(data: Vec<u8>, codec: i32, uncompressed_size: u64) -> anyhow::Result<Bytes> {
    decompress_with_cancellation(data, codec, uncompressed_size, &CancellationToken::new())
}

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
            network_timeout: core::time::Duration::from_secs(30),
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

fn protocol_path(path: &str) -> crate::providers::logbroker::proto::pers_queue::v1::Path {
    crate::providers::logbroker::proto::pers_queue::v1::Path {
        path: path.to_string(),
    }
}

fn discovery_endpoint(
    address: &str,
    port: u32,
    load_factor: f32,
    ssl: bool,
) -> crate::providers::logbroker::proto::discovery::EndpointInfo {
    crate::providers::logbroker::proto::discovery::EndpointInfo {
        address: address.to_string(),
        port,
        load_factor,
        ssl,
        ..Default::default()
    }
}

fn describe_topic_response(
    settings: Option<crate::providers::logbroker::proto::pers_queue::v1::TopicSettings>,
    status: i32,
) -> crate::providers::logbroker::proto::pers_queue::v1::DescribeTopicResponse {
    use prost::Message as _;

    let result = crate::providers::logbroker::proto::pers_queue::v1::DescribeTopicResult {
        self_: None,
        settings,
    };
    crate::providers::logbroker::proto::pers_queue::v1::DescribeTopicResponse {
        operation: Some(crate::providers::logbroker::proto::operations::Operation {
            ready: true,
            status,
            result: Some(prost_types::Any {
                type_url: "type.googleapis.com/Ydb.PersQueue.V1.DescribeTopicResult".to_string(),
                value: result.encode_to_vec(),
            }),
            ..Default::default()
        }),
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
            migration_streaming_read_server_message::data_batch::MessageData::default(
            );
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
        migration_streaming_read_server_message::Response::Assigned(assignment("topic", "cluster")),
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
fn init_scopes_the_session_to_requested_partition_groups() {
    let init = init_request("topic", "consumer", &[3, 7]);
    assert_eq!(init.topics_read_settings[0].partition_group_ids, vec![3, 7]);
    let read_params = init.read_params.expect("read parameters");
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
        * core::mem::size_of::<migration_streaming_read_server_message::data_batch::MessageData>();
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
fn describe_topic_decodes_successful_metadata() {
    let settings = crate::providers::logbroker::proto::pers_queue::v1::TopicSettings {
        partitions_count: 3,
        ..Default::default()
    };
    let decoded =
        decode_describe_topic_response(describe_topic_response(Some(settings), YDB_STATUS_SUCCESS))
            .unwrap();
    assert_eq!(decoded.partitions_count, 3);
}

#[test]
fn describe_topic_requests_synchronous_metadata() {
    let request = describe_topic_request("/Root/topic");
    assert_eq!(request.path, "/Root/topic");
    assert_eq!(
        request.operation_params.unwrap().operation_mode,
        crate::providers::logbroker::proto::operations::operation_params::OperationMode::Sync
            as i32
    );
}

#[test]
fn describe_topic_requires_settings_in_a_successful_result() {
    let error = decode_describe_topic_response(describe_topic_response(None, YDB_STATUS_SUCCESS))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("successful result is missing topic settings"),
        "{error:#}"
    );
    assert!(error
        .downcast_ref::<PipelineFailure>()
        .is_some_and(|failure| !failure.is_retryable()));
}

#[test]
fn describe_topic_requires_a_ready_sync_operation() {
    let mut response = describe_topic_response(
        Some(crate::providers::logbroker::proto::pers_queue::v1::TopicSettings::default()),
        YDB_STATUS_SUCCESS,
    );
    response.operation.as_mut().unwrap().ready = false;
    let error = decode_describe_topic_response(response).unwrap_err();
    assert!(error.to_string().contains("SYNC operation is not ready"));
    assert!(error
        .downcast_ref::<PipelineFailure>()
        .is_some_and(|failure| !failure.is_retryable()));
}

#[test]
fn describe_topic_rejects_a_non_success_operation() {
    let error = decode_describe_topic_response(describe_topic_response(
        Some(crate::providers::logbroker::proto::pers_queue::v1::TopicSettings::default()),
        StatusCode::SchemeError as i32,
    ))
    .unwrap_err();
    assert!(error.to_string().contains("SCHEME_ERROR"), "{error:#}");
    assert!(error
        .downcast_ref::<PipelineFailure>()
        .is_some_and(|failure| !failure.is_retryable()));
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
    let error = ordered_plaintext_proxies(vec![discovery_endpoint("tls.test", 2135, 0.0, true)], 7)
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

    let failure =
        register_assignment(&mut active, &HashSet::from([7]), "topic", &reassigned).unwrap_err();

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
        let failure =
            validate_release_assignment(&mut active, &release(topic, cluster, false)).unwrap_err();

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
        issues: vec![crate::providers::logbroker::proto::issue::IssueMessage {
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
        Code::Cancelled,
        Code::Unknown,
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::Internal,
    ] {
        let failure = tonic_failure("test", &tonic::Status::new(code, "injected"));
        assert_eq!(failure.kind, TerminalFailureKind::Retryable, "{code:?}");
    }
    for code in [
        Code::Ok,
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::OutOfRange,
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
    let SourceBatch::Raw {
        messages,
        memory: reservations,
        ..
    } = batch
    else {
        panic!("expected raw batch");
    };
    assert_eq!(messages.len(), 2);
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].bytes(), retained);
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
    let SourceBatch::Raw {
        messages,
        commit_marker,
        ..
    } = batch
    else {
        panic!("expected raw batch");
    };
    assert!(messages.is_empty());
    let marker = commit_marker.expect("discarded batch commit marker");
    assert_eq!(
        marker.value::<PqV1CommitMarker>().unwrap().cookies[0].partition_cookie,
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

    let SourceBatch::Raw {
        commit_marker: first_marker,
        ..
    } = source.read_batch().await.unwrap()
    else {
        panic!("expected raw batch");
    };
    let first_marker = first_marker.expect("first commit marker");
    let SourceBatch::Raw {
        commit_marker: second_marker,
        ..
    } = source.read_batch().await.unwrap()
    else {
        panic!("expected raw batch");
    };
    let second_marker = second_marker.expect("second commit marker");

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
    let Some(migration_streaming_read_client_message::Request::Commit(commit)) = request.request
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
