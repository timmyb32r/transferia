use core::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use transferia::metrics::{ParseCounters, SinkCounters};
use transferia::middleware::filter::FilterMiddleware;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::middleware::Middleware;
use transferia::pipeline::run_partition_pipeline;
use transferia::pipeline::source::{CommitMarker, ReadResult, Source};
use transferia::pipeline::PipelineFailure;
use transferia::providers::clickhouse::{
    ClickHouseSink, ClickHouseSinkConfig, InsertError, InsertTransport,
};
use transferia::types::message::{Message, MessageBatch, MessageMeta, SourcePartition};

struct FakeSource {
    batches: VecDeque<Vec<Message>>,
    repeat: bool,
    next_offset: i64,
    reads: Arc<AtomicUsize>,
    commits: mpsc::UnboundedSender<i64>,
}

struct FailedSource;

struct MarkerOnlySource {
    marker: Option<i64>,
    commits: mpsc::UnboundedSender<i64>,
}

impl Source for FailedSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async {
            Ok(ReadResult::Failed(anyhow::anyhow!(
                "corrupt compressed source batch"
            )))
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _marker: &'ctx CommitMarker,
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async { anyhow::bail!("failed source must never commit") })
    }
}

impl Source for MarkerOnlySource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            if let Some(marker) = self.marker.take() {
                return Ok(ReadResult::Batch(MessageBatch {
                    messages: Vec::new(),
                    partition_id: 0,
                    commit_marker: Some(CommitMarker::new(marker)),
                    memory: Vec::new(),
                }));
            }
            Ok(ReadResult::Batch(MessageBatch {
                messages: Vec::new(),
                partition_id: 0,
                commit_marker: None,
                memory: Vec::new(),
            }))
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        marker: &'ctx CommitMarker,
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move {
            let marker = marker
                .downcast_ref::<i64>()
                .copied()
                .ok_or_else(|| anyhow::anyhow!("invalid marker-only source marker"))?;
            self.commits
                .send(marker)
                .map_err(|_| anyhow::anyhow!("marker commit receiver closed"))?;
            Ok(())
        })
    }
}

impl FakeSource {
    fn message(offset: i64) -> Message {
        Message {
            value: Bytes::from(format!(r#"{{"id":"{offset}","kind":"keep"}}"#)),
            meta: MessageMeta {
                partition: Some(SourcePartition::Int(0)),
                offset: Some(offset),
                ..MessageMeta::default()
            },
        }
    }
}

impl Source for FakeSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::AcqRel);
            let messages = if let Some(messages) = self.batches.pop_front() {
                messages
            } else if self.repeat {
                let offset = self.next_offset;
                self.next_offset += 1;
                vec![Self::message(offset)]
            } else {
                return Ok(ReadResult::Batch(MessageBatch {
                    messages: Vec::new(),
                    partition_id: 0,
                    commit_marker: None,
                    memory: Vec::new(),
                }));
            };
            let marker = messages
                .last()
                .and_then(|message| message.meta.offset)
                .unwrap_or_default();
            Ok(ReadResult::Batch(MessageBatch {
                messages,
                partition_id: 0,
                commit_marker: Some(CommitMarker::new(marker)),
                memory: Vec::new(),
            }))
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        marker: &'ctx CommitMarker,
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move {
            let offset = marker
                .downcast_ref::<i64>()
                .copied()
                .ok_or_else(|| anyhow::anyhow!("invalid fake marker"))?;
            self.commits
                .send(offset)
                .map_err(|_| anyhow::anyhow!("fake commit receiver closed"))?;
            Ok(())
        })
    }
}

struct FakeClickHouse {
    inserts: AtomicUsize,
    rows: AtomicUsize,
    block: bool,
    gate: Arc<Semaphore>,
    started: Notify,
}

impl FakeClickHouse {
    fn new(block: bool) -> Arc<Self> {
        Arc::new(Self {
            inserts: AtomicUsize::new(0),
            rows: AtomicUsize::new(0),
            block,
            gate: Arc::new(Semaphore::new(0)),
            started: Notify::new(),
        })
    }
}

impl InsertTransport for FakeClickHouse {
    fn insert(
        &self,
        _table: Arc<str>,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>> {
        self.inserts.fetch_add(1, Ordering::AcqRel);
        self.rows.fetch_add(
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum(),
            Ordering::AcqRel,
        );
        self.started.notify_one();
        let gate = if self.block {
            Some(Arc::clone(&self.gate).acquire_owned())
        } else {
            None
        };
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.await
                    .map_err(|error| InsertError::Transient(anyhow::anyhow!(error)))?
                    .forget();
            }
            Ok(())
        })
    }
}

fn sink_config() -> ClickHouseSinkConfig {
    ClickHouseSinkConfig {
        connection_string: "fake".into(),
        database: "default".into(),
        username: "default".into(),
        password: String::new(),
        max_insert_rows: 1,
        max_insert_bytes: usize::MAX,
        flush_interval_ms: 1,
        retry_initial_ms: 1,
        retry_max_ms: 10,
        retry_max_attempts: None,
        use_tls: false,
        tls_domain: None,
        sorting_key: Vec::new(),
        recreate_tables: false,
    }
}

fn parser() -> Arc<dyn transferia::parsers::Parser> {
    let raw: serde_yaml::Value = serde_yaml::from_str(
        r#"
columns:
  - jsonpath: "$.id"
    column_name: "id"
    arrow_type: "Utf8"
    nullable: false
  - jsonpath: "$.kind"
    column_name: "kind"
    arrow_type: "Utf8"
    nullable: false
"#,
    )
    .unwrap();
    transferia::parsers::build_parser(
        "json_parser",
        raw,
        Arc::from("events"),
        transferia::parsers::CommonParserConfig {
            table_naming: transferia::parsers::TableNaming {
                kind: "from_config".into(),
                name: Some("events".into()),
            },
            system_columns: transferia::parsers::SystemColumnsConfig::default(),
        },
    )
    .unwrap()
}

async fn wait_for_insert(transport: &FakeClickHouse, count: usize) {
    tokio::time::timeout(core::time::Duration::from_secs(5), async {
        while transport.inserts.load(Ordering::Acquire) < count {
            transport.started.notified().await;
        }
    })
    .await
    .expect("fake ClickHouse INSERT did not start");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_parser_to_actor_sink_commits_only_after_fake_clickhouse() {
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let reads = Arc::new(AtomicUsize::new(0));
    let source = FakeSource {
        batches: VecDeque::from([vec![FakeSource::message(7)]]),
        repeat: false,
        next_offset: 8,
        reads,
        commits: commit_tx,
    };
    let transport = FakeClickHouse::new(true);
    let sink_counters = Arc::new(SinkCounters::new());
    let sink = ClickHouseSink::with_transport(
        sink_config(),
        Arc::clone(&sink_counters),
        Arc::clone(&transport) as Arc<dyn InsertTransport>,
    );
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(run_partition_pipeline(
        Box::new(source),
        parser(),
        Arc::new(vec![
            Box::new(FilterMiddleware::new("kind".into(), "keep".into()).unwrap())
                as Box<dyn Middleware>,
        ]),
        Box::new(sink),
        PipelineMemory::new(1024 * 1024),
        cancellation.clone(),
        0,
        Arc::new(ParseCounters::new()),
    ));

    wait_for_insert(&transport, 1).await;
    assert!(commit_rx.try_recv().is_err());
    transport.gate.add_permits(1);
    let committed = tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv())
        .await
        .expect("source commit timed out");
    assert_eq!(committed, Some(7));
    assert_eq!(transport.inserts.load(Ordering::Acquire), 1);
    assert_eq!(transport.rows.load(Ordering::Acquire), 1);
    assert_eq!(sink_counters.rows_total(), 1);
    assert_eq!(sink_counters.flushes_total(), 1);
    assert_eq!(sink_counters.source_messages_total(), 1);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_sink_propagates_memory_backpressure_to_source_reads() {
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let reads = Arc::new(AtomicUsize::new(0));
    let source = FakeSource {
        batches: VecDeque::new(),
        repeat: true,
        next_offset: 1,
        reads: Arc::clone(&reads),
        commits: commit_tx,
    };
    let transport = FakeClickHouse::new(true);
    let sink = ClickHouseSink::with_transport(
        sink_config(),
        Arc::new(SinkCounters::new()),
        Arc::clone(&transport) as Arc<dyn InsertTransport>,
    );
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(run_partition_pipeline(
        Box::new(source),
        parser(),
        Arc::new(Vec::new()),
        Box::new(sink),
        PipelineMemory::new(64),
        cancellation.clone(),
        0,
        Arc::new(ParseCounters::new()),
    ));

    wait_for_insert(&transport, 1).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let stalled_reads = reads.load(Ordering::Acquire);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        reads.load(Ordering::Acquire),
        stalled_reads,
        "source kept reading while sink held the budget"
    );
    assert!(
        stalled_reads <= 16,
        "source reads exceeded the pipeline's outstanding-delivery bound"
    );
    assert!(commit_rx.try_recv().is_err());

    transport.gate.add_permits(1);
    let committed = tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv())
        .await
        .expect("source commit timed out");
    assert!(committed.is_some());
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_failed_result_is_a_non_retryable_pipeline_failure() {
    for _ in 0..32 {
        let sink =
            transferia::providers::empty::sink::EmptySink::new(Arc::new(SinkCounters::new()));
        let error = run_partition_pipeline(
            Box::new(FailedSource),
            parser(),
            Arc::new(Vec::new()),
            Box::new(sink),
            PipelineMemory::new(1024),
            CancellationToken::new(),
            0,
            Arc::new(ParseCounters::new()),
        )
        .await
        .expect_err("terminal source corruption must fail the pipeline");
        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("failure must preserve explicit retryability");
        assert!(!failure.is_retryable());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn marker_only_delivery_is_acknowledged_and_committed() -> anyhow::Result<()> {
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let source = MarkerOnlySource {
        marker: Some(41),
        commits: commit_tx,
    };
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(run_partition_pipeline(
        Box::new(source),
        parser(),
        Arc::new(Vec::new()),
        Box::new(transferia::providers::empty::sink::EmptySink::new(
            Arc::new(SinkCounters::new()),
        )),
        PipelineMemory::new(1024),
        cancellation.clone(),
        0,
        Arc::new(ParseCounters::new()),
    ));

    assert_eq!(
        tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv()).await?,
        Some(41)
    );
    cancellation.cancel();
    task.await??;
    Ok(())
}
