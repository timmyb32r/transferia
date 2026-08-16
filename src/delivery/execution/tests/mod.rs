use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{Field, Schema};

struct RecordingSource {
    groups: Arc<std::sync::Mutex<Vec<Vec<i64>>>>,
    fail_commit: bool,
}

struct OverestimatedSession;

struct OverestimatedFactory;

impl ParserFactory for OverestimatedFactory {
    fn create_session(self: Arc<Self>) -> Box<dyn ParserSession> {
        Box::new(OverestimatedSession)
    }
}

struct StatefulFactory {
    created: Arc<AtomicU64>,
}

impl ParserFactory for StatefulFactory {
    fn create_session(self: Arc<Self>) -> Box<dyn ParserSession> {
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

    fn hard_output_limit(&self) -> Option<usize> {
        Some(1024)
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
                crate::delivery::data::system_columns::SystemColumns::default(),
            ),
            None,
        ))
    }
}

impl ParserSession for OverestimatedSession {
    fn output_memory_bound(&self, _messages: &[Message]) -> usize {
        2048
    }

    fn hard_output_limit(&self) -> Option<usize> {
        Some(1024)
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
                crate::delivery::data::system_columns::SystemColumns::default(),
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
        anyhow::Result<crate::delivery::data::message::SourceBatch>,
    > {
        Box::pin(async { anyhow::bail!("recording source is commit-only") })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> futures_util::future::BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move {
            if self.fail_commit {
                anyhow::bail!("injected grouped commit failure");
            }
            let group = markers
                .iter()
                .map(|marker| {
                    marker
                        .downcast_ref::<i64>()
                        .copied()
                        .ok_or_else(|| anyhow::anyhow!("unexpected marker"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            self.groups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(group);
            Ok(())
        })
    }
}

#[test]
fn parser_shutdown_timeout_cannot_restart_over_live_blocking_work() {
    let error = ComponentOutcome::fatal_timeout("parser timeout")
        .result
        .expect_err("timeout must fail the pipeline");
    let failure = error
        .downcast_ref::<PipelineFailure>()
        .expect("parser timeout must preserve its restart contract");
    assert!(!failure.is_retryable());
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
        .downcast_ref::<PipelineFailure>()
        .expect("delivery protocol violations must keep their fatal disposition");
    assert!(!failure.is_retryable());
    assert_eq!(ledger.len(), 1);
}

#[tokio::test]
async fn failed_grouped_commit_keeps_the_ledger_for_pipeline_recovery() {
    let mut source: Box<dyn Source> = Box::new(RecordingSource {
        groups: Arc::new(std::sync::Mutex::new(Vec::new())),
        fail_commit: true,
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
