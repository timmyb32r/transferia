use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{Field, Schema};

struct RecordingSource {
    groups: Arc<std::sync::Mutex<Vec<Vec<i64>>>>,
    fail_commit: bool,
    fail_shutdown: bool,
    shutdowns: Arc<AtomicU64>,
}

struct OverestimatedSession;

struct OverestimatedFactory;

struct CancellationSensitiveSource {
    reads: u8,
    second_read_started: Arc<tokio::sync::Notify>,
    finish_second_read: Arc<tokio::sync::Notify>,
    cancelled_reads: Arc<AtomicU64>,
    shutdowns: Arc<AtomicU64>,
    fail_shutdown: bool,
}

struct FiniteShutdownFailureSource {
    reads: u8,
    commits: Arc<AtomicU64>,
    shutdowns: Arc<AtomicU64>,
}

struct QuiescingSink {
    write_started: Arc<tokio::sync::Notify>,

    release_write: Arc<tokio::sync::Notify>,

    quiesced: Arc<AtomicU64>,
}

impl Sink for QuiescingSink {
    fn run(
        self: Box<Self>,
        mut io: SinkIo,
    ) -> futures_util::future::BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(async move {
            let _events = io.events.clone();
            let delivery = io.deliveries.recv().await.ok_or_else(|| {
                DataPlaneFailure::fatal(anyhow::anyhow!("test sink received no delivery"))
            })?;
            self.write_started.notify_one();
            io.cancellation.cancelled().await;
            self.release_write.notified().await;
            drop(delivery);
            self.quiesced.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

struct ReadCancellationGuard {
    completed: bool,
    cancelled_reads: Arc<AtomicU64>,
}

impl Drop for ReadCancellationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled_reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ParserFactory for OverestimatedFactory {
    fn create_session(self: Arc<Self>, _memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(OverestimatedSession)
    }
}

struct StatefulFactory {
    created: Arc<AtomicU64>,
}

impl ParserFactory for StatefulFactory {
    fn create_session(self: Arc<Self>, _memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        self.created.fetch_add(1, Ordering::Relaxed);
        Box::new(StatefulSession { calls: 0 })
    }
}

struct StatefulSession {
    calls: i64,
}

impl ParserSession for StatefulSession {
    fn output_memory_bound(&self, _messages: &[Message]) -> usize {
        1024
    }

    fn parse_into(
        &mut self,
        _messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        self.calls += 1;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                arrow::datatypes::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![self.calls]))],
        )?;
        Ok((
            TableData::new(
                "events".into(),
                false,
                batch,
                transferia_core::data::system_columns::SystemColumns::default(),
            ),
            None,
        ))
    }
}

impl ParserSession for OverestimatedSession {
    fn output_memory_bound(&self, _messages: &[Message]) -> usize {
        2048
    }

    fn parse_into(
        &mut self,
        _messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                arrow::datatypes::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )?;
        Ok((
            TableData::new(
                "events".into(),
                false,
                batch,
                transferia_core::data::system_columns::SystemColumns::default(),
            ),
            None,
        ))
    }
}

impl Source for RecordingSource {
    fn read_batch(
        &mut self,
    ) -> futures_util::future::BoxFuture<
        '_,
        transferia_core::failure::DataPlaneResult<transferia_core::data::message::SourceBatch>,
    > {
        Box::pin(async {
            Err(transferia_core::failure::DataPlaneFailure::fatal(
                anyhow::anyhow!("recording source is commit-only"),
            ))
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> futures_util::future::BoxFuture<'ctx, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            if self.fail_commit {
                return Err(transferia_core::failure::DataPlaneFailure::retryable(
                    anyhow::anyhow!("injected grouped commit failure"),
                ));
            }
            let group = markers
                .iter()
                .map(|marker| marker.value::<i64>().copied().map_err(anyhow::Error::new))
                .collect::<anyhow::Result<Vec<_>>>()
                .map_err(transferia_core::failure::DataPlaneFailure::fatal)?;
            self.groups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(group);
            Ok(())
        })
    }

    fn shutdown(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        self.shutdowns.fetch_add(1, Ordering::Relaxed);
        let fail = self.fail_shutdown;
        Box::pin(async move {
            if fail {
                Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "injected source shutdown failure"
                )))
            } else {
                Ok(())
            }
        })
    }
}

impl Source for CancellationSensitiveSource {
    fn read_batch(
        &mut self,
    ) -> futures_util::future::BoxFuture<
        '_,
        transferia_core::failure::DataPlaneResult<transferia_core::data::message::SourceBatch>,
    > {
        match self.reads {
            0 => {
                self.reads = 1;
                Box::pin(async {
                    let batch = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new(
                            "value",
                            arrow::datatypes::DataType::Int64,
                            false,
                        )])),
                        vec![Arc::new(Int64Array::from(vec![1_i64]))],
                    )
                    .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                    Ok(SourceBatch::Typed {
                        tables: vec![TableData::new(
                            "events".into(),
                            false,
                            batch,
                            transferia_core::data::system_columns::SystemColumns::default(),
                        )],
                        source_rows: 1,
                        commit_marker: Some(CommitMarker::new(1_i64)),
                        memory: Vec::new(),
                    })
                })
            }
            1 => {
                self.reads = 2;
                let started = Arc::clone(&self.second_read_started);
                let finish = Arc::clone(&self.finish_second_read);
                let cancelled_reads = Arc::clone(&self.cancelled_reads);
                Box::pin(async move {
                    let mut guard = ReadCancellationGuard {
                        completed: false,
                        cancelled_reads,
                    };
                    started.notify_one();
                    finish.notified().await;
                    guard.completed = true;
                    Ok(SourceBatch::Finished)
                })
            }
            _ => Box::pin(async { Ok(SourceBatch::Finished) }),
        }
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _markers: &'ctx [CommitMarker],
    ) -> futures_util::future::BoxFuture<'ctx, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        self.shutdowns.fetch_add(1, Ordering::Relaxed);
        let fail = self.fail_shutdown;
        Box::pin(async move {
            if fail {
                Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                    "injected cancellation shutdown failure"
                )))
            } else {
                Ok(())
            }
        })
    }
}

impl Source for FiniteShutdownFailureSource {
    fn read_batch(
        &mut self,
    ) -> futures_util::future::BoxFuture<
        '_,
        transferia_core::failure::DataPlaneResult<transferia_core::data::message::SourceBatch>,
    > {
        if self.reads == 0 {
            self.reads = 1;
            Box::pin(async {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        arrow::datatypes::DataType::Int64,
                        false,
                    )])),
                    vec![Arc::new(Int64Array::from(vec![1_i64]))],
                )
                .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                Ok(SourceBatch::Typed {
                    tables: vec![TableData::new(
                        "events".into(),
                        false,
                        batch,
                        transferia_core::data::system_columns::SystemColumns::default(),
                    )],
                    source_rows: 1,
                    commit_marker: Some(CommitMarker::new(1_i64)),
                    memory: Vec::new(),
                })
            })
        } else {
            Box::pin(async { Ok(SourceBatch::Finished) })
        }
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> futures_util::future::BoxFuture<'ctx, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            if markers.len() != 1 {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "expected one finite-source commit marker"
                )));
            }
            self.commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }

    fn shutdown(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        self.shutdowns.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                "injected persistent source shutdown failure"
            )))
        })
    }
}

#[tokio::test]
async fn conservative_parser_estimate_is_not_a_correctness_rejection() {
    let memory = PipelineMemory::new(1024);
    let cancellation = CancellationToken::new();
    let (input_tx, input_rx) = mpsc::channel(1);
    let (output_tx, mut output_rx) = mpsc::channel(1);
    input_tx
        .send(ReadEnvelope {
            id: DeliveryId::new(1),
            payload: ReadPayload::Raw(vec![Message::new(bytes::Bytes::from_static(b"{}"))]),
            memory: Vec::new(),
            meta: DeliveryMeta { source_messages: 1 },
        })
        .await
        .unwrap();
    drop(input_tx);

    parser_loop(
        input_rx,
        output_tx,
        Arc::new(OverestimatedFactory),
        Arc::new(Vec::new()),
        memory,
        Arc::new(ParseCounters::new()),
        cancellation,
    )
    .await
    .unwrap();

    let delivery = output_rx.recv().await.expect("delivery must be produced");
    assert_eq!(delivery.outputs[0].rows(), 1);
}

#[tokio::test]
async fn idle_parser_task_does_not_construct_a_session_or_hold_a_worker() {
    let created = Arc::new(AtomicU64::new(0));
    let factory = Arc::new(StatefulFactory {
        created: Arc::clone(&created),
    });
    let cancellation = CancellationToken::new();
    let (input_tx, input_rx) = mpsc::channel(1);
    let (output_tx, _output_rx) = mpsc::channel(1);
    let task = tokio::spawn(parser_loop(
        input_rx,
        output_tx,
        factory,
        Arc::new(Vec::new()),
        PipelineMemory::new(1024),
        Arc::new(ParseCounters::new()),
        cancellation.clone(),
    ));

    tokio::task::yield_now().await;
    assert_eq!(created.load(Ordering::Relaxed), 0);

    cancellation.cancel();
    drop(input_tx);
    task.await.unwrap().unwrap();
    assert_eq!(created.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn parser_session_state_is_preserved_across_blocking_workers() {
    let created = Arc::new(AtomicU64::new(0));
    let factory = Arc::new(StatefulFactory {
        created: Arc::clone(&created),
    });
    let cancellation = CancellationToken::new();
    let (input_tx, input_rx) = mpsc::channel(2);
    let (output_tx, mut output_rx) = mpsc::channel(2);
    for id in 1..=2 {
        input_tx
            .send(ReadEnvelope {
                id: DeliveryId::new(id),
                payload: ReadPayload::Raw(vec![Message::new(bytes::Bytes::from_static(b"{}"))]),
                memory: Vec::new(),
                meta: DeliveryMeta { source_messages: 1 },
            })
            .await
            .unwrap();
    }
    drop(input_tx);

    parser_loop(
        input_rx,
        output_tx,
        factory,
        Arc::new(Vec::new()),
        PipelineMemory::new(1 << 20),
        Arc::new(ParseCounters::new()),
        cancellation,
    )
    .await
    .unwrap();

    assert_eq!(created.load(Ordering::Relaxed), 1);
    for expected in 1..=2 {
        let delivery = output_rx.recv().await.expect("delivery must be produced");
        let values = delivery.outputs[0].batch.column(0);
        let values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state column must be Int64");
        assert_eq!(values.value(0), expected);
    }
}

#[tokio::test]
async fn commit_through_submits_the_contiguous_prefix_as_one_source_group() {
    let groups = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut source: Box<dyn Source> = Box::new(RecordingSource {
        groups: Arc::clone(&groups),
        fail_commit: false,
        fail_shutdown: false,
        shutdowns: Arc::new(AtomicU64::new(0)),
    });
    let mut ledger = VecDeque::from([
        CommitEntry {
            id: DeliveryId::new(1),
            marker: Some(CommitMarker::new(11_i64)),
        },
        CommitEntry {
            id: DeliveryId::new(2),
            marker: None,
        },
        CommitEntry {
            id: DeliveryId::new(3),
            marker: Some(CommitMarker::new(33_i64)),
        },
    ]);

    let progress = PipelineProgress::new();
    commit_through(&mut source, &mut ledger, DeliveryId::new(3), &progress)
        .await
        .unwrap();

    assert!(ledger.is_empty());
    assert!(progress.advanced_since(0));
    assert_eq!(
        *groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![vec![11, 33]]
    );
}

#[tokio::test]
async fn commit_through_rejects_an_unknown_sink_delivery_as_fatal() {
    let mut source: Box<dyn Source> = Box::new(RecordingSource {
        groups: Arc::new(std::sync::Mutex::new(Vec::new())),
        fail_commit: false,
        fail_shutdown: false,
        shutdowns: Arc::new(AtomicU64::new(0)),
    });
    let mut ledger = VecDeque::from([CommitEntry {
        id: DeliveryId::new(1),
        marker: Some(CommitMarker::new(11_i64)),
    }]);

    let error = commit_through(
        &mut source,
        &mut ledger,
        DeliveryId::new(2),
        &PipelineProgress::new(),
    )
    .await
    .expect_err("sink cannot commit a delivery the source never issued");

    let failure = error
        .downcast_ref::<DataPlaneFailure>()
        .expect("delivery protocol violations must keep their fatal disposition");
    assert!(!failure.is_retryable());
    assert_eq!(ledger.len(), 1);
}

#[tokio::test]
async fn sink_commit_events_do_not_cancel_an_in_flight_source_read() {
    let second_read_started = Arc::new(tokio::sync::Notify::new());
    let finish_second_read = Arc::new(tokio::sync::Notify::new());
    let cancelled_reads = Arc::new(AtomicU64::new(0));
    let shutdowns = Arc::new(AtomicU64::new(0));
    let source: Box<dyn Source> = Box::new(CancellationSensitiveSource {
        reads: 0,
        second_read_started: Arc::clone(&second_read_started),
        finish_second_read: Arc::clone(&finish_second_read),
        cancelled_reads: Arc::clone(&cancelled_reads),
        shutdowns: Arc::clone(&shutdowns),
        fail_shutdown: false,
    });
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let (event_tx, event_rx) = mpsc::channel(1);
    let task = tokio::spawn(reader_loop(
        source,
        output_tx,
        event_rx,
        PipelineMemory::new(1 << 20),
        CancellationToken::new(),
        Arc::new(PipelineProgress::new()),
    ));

    let first = output_rx
        .recv()
        .await
        .expect("source batch must be emitted");
    assert_eq!(first.id, DeliveryId::new(1));
    second_read_started.notified().await;
    event_tx
        .send(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    finish_second_read.notify_one();

    task.await.unwrap().unwrap();
    assert_eq!(cancelled_reads.load(Ordering::Relaxed), 0);
    assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn pipeline_cancellation_awaits_source_shutdown_after_cancelling_a_read() {
    let second_read_started = Arc::new(tokio::sync::Notify::new());
    let finish_second_read = Arc::new(tokio::sync::Notify::new());
    let cancelled_reads = Arc::new(AtomicU64::new(0));
    let shutdowns = Arc::new(AtomicU64::new(0));
    let source: Box<dyn Source> = Box::new(CancellationSensitiveSource {
        reads: 0,
        second_read_started: Arc::clone(&second_read_started),
        finish_second_read,
        cancelled_reads: Arc::clone(&cancelled_reads),
        shutdowns: Arc::clone(&shutdowns),
        fail_shutdown: true,
    });
    let cancellation = CancellationToken::new();
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let (_event_tx, event_rx) = mpsc::channel(1);
    let task = tokio::spawn(reader_loop(
        source,
        output_tx,
        event_rx,
        PipelineMemory::new(1 << 20),
        cancellation.clone(),
        Arc::new(PipelineProgress::new()),
    ));

    output_rx
        .recv()
        .await
        .expect("first source batch must be emitted");
    second_read_started.notified().await;
    cancellation.cancel();

    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cancelled reader must stop after awaited shutdown")
        .unwrap()
        .unwrap();
    assert_eq!(cancelled_reads.load(Ordering::Relaxed), 1);
    assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn pipeline_cancellation_awaits_sink_external_io_quiescence() {
    let second_read_started = Arc::new(tokio::sync::Notify::new());
    let source: Box<dyn Source> = Box::new(CancellationSensitiveSource {
        reads: 0,
        second_read_started,
        finish_second_read: Arc::new(tokio::sync::Notify::new()),
        cancelled_reads: Arc::new(AtomicU64::new(0)),
        shutdowns: Arc::new(AtomicU64::new(0)),
        fail_shutdown: false,
    });
    let write_started = Arc::new(tokio::sync::Notify::new());
    let release_write = Arc::new(tokio::sync::Notify::new());
    let quiesced = Arc::new(AtomicU64::new(0));
    let sink: Box<dyn Sink> = Box::new(QuiescingSink {
        write_started: Arc::clone(&write_started),
        release_write: Arc::clone(&release_write),
        quiesced: Arc::clone(&quiesced),
    });
    let cancellation = CancellationToken::new();
    let mut task = tokio::spawn(run_partition_pipeline(
        source,
        Arc::new(OverestimatedFactory),
        Arc::new(Vec::new()),
        sink,
        PipelineMemory::new(1 << 20),
        cancellation.clone(),
        0,
        Arc::new(ParseCounters::new()),
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            () = write_started.notified() => {}
            result = &mut task => panic!("pipeline stopped before the sink write: {result:?}"),
        }
    })
    .await
    .expect("sink must receive the first delivery");
    cancellation.cancel();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
    assert_eq!(quiesced.load(Ordering::Relaxed), 0);

    release_write.notify_one();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("pipeline must finish after sink I/O quiesces")
        .expect("pipeline task must not panic")
        .expect("pipeline cancellation must be graceful");
    assert_eq!(quiesced.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn finite_source_shutdown_failure_does_not_replay_committed_snapshot() {
    let commits = Arc::new(AtomicU64::new(0));
    let shutdowns = Arc::new(AtomicU64::new(0));
    let source: Box<dyn Source> = Box::new(FiniteShutdownFailureSource {
        reads: 0,
        commits: Arc::clone(&commits),
        shutdowns: Arc::clone(&shutdowns),
    });
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let (event_tx, event_rx) = mpsc::channel(1);
    let task = tokio::spawn(reader_loop(
        source,
        output_tx,
        event_rx,
        PipelineMemory::new(1 << 20),
        CancellationToken::new(),
        Arc::new(PipelineProgress::new()),
    ));

    let delivery = output_rx
        .recv()
        .await
        .expect("finite source batch must be emitted");
    event_tx
        .send(SinkEvent::CommittedThrough(delivery.id))
        .await
        .expect("commit acknowledgement must reach the reader");
    drop(delivery);

    task.await
        .expect("reader task must not panic")
        .expect("post-commit shutdown failure must not fail the finite snapshot");
    assert_eq!(commits.load(Ordering::Relaxed), 1);
    assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn source_failure_remains_primary_when_awaited_shutdown_also_fails() {
    let shutdowns = Arc::new(AtomicU64::new(0));
    let source: Box<dyn Source> = Box::new(RecordingSource {
        groups: Arc::new(std::sync::Mutex::new(Vec::new())),
        fail_commit: false,
        fail_shutdown: true,
        shutdowns: Arc::clone(&shutdowns),
    });
    let (output_tx, _output_rx) = mpsc::channel(1);
    let (_event_tx, event_rx) = mpsc::channel(1);

    let error = reader_loop(
        source,
        output_tx,
        event_rx,
        PipelineMemory::new(1 << 20),
        CancellationToken::new(),
        Arc::new(PipelineProgress::new()),
    )
    .await
    .expect_err("source read must fail");

    assert!(error.to_string().contains("recording source is commit-only"));
    assert!(!error
        .to_string()
        .contains("injected source shutdown failure"));
    assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn failed_grouped_commit_keeps_the_ledger_for_pipeline_recovery() {
    let mut source: Box<dyn Source> = Box::new(RecordingSource {
        groups: Arc::new(std::sync::Mutex::new(Vec::new())),
        fail_commit: true,
        fail_shutdown: false,
        shutdowns: Arc::new(AtomicU64::new(0)),
    });
    let mut ledger = VecDeque::from([
        CommitEntry {
            id: DeliveryId::new(1),
            marker: Some(CommitMarker::new(11_i64)),
        },
        CommitEntry {
            id: DeliveryId::new(2),
            marker: Some(CommitMarker::new(22_i64)),
        },
    ]);

    let progress = PipelineProgress::new();
    let error = commit_through(&mut source, &mut ledger, DeliveryId::new(2), &progress)
        .await
        .expect_err("injected source commit failure must propagate");

    assert!(error
        .to_string()
        .contains("source commit failed through delivery 2"));
    assert_eq!(ledger.len(), 2);
    assert!(!progress.advanced_since(0));
}
