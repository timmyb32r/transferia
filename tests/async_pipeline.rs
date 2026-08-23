#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use core::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use transferia::connectors::clickhouse::{
    ClickHouseSink, ClickHouseSinkConfig, InsertError, InsertTransport,
};
use transferia::core::data::message::{Message, MessageMeta, SourceBatch};
use transferia::core::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use transferia::core::failure::DataPlaneFailure;
use transferia::core::memory::PipelineMemory;
use transferia::core::source::{CommitMarker, Source};
use transferia::delivery::execution::middleware::Middleware;
use transferia::delivery::execution::run_partition_pipeline;
use transferia::metrics::{ParseCounters, SinkCounters};
use transferia::middleware::filter::FilterMiddleware;

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
    fn read_batch(&mut self) -> BoxFuture<'_, transferia::core::DataPlaneResult<SourceBatch>> {
        Box::pin(async {
            Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "corrupt compressed source batch"
            )))
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, transferia::core::DataPlaneResult<()>> {
        Box::pin(async {
            Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "failed source must never commit"
            )))
        })
    }
}

impl Source for MarkerOnlySource {
    fn read_batch(&mut self) -> BoxFuture<'_, transferia::core::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if let Some(marker) = self.marker.take() {
                return Ok(SourceBatch::Raw {
                    messages: Vec::new(),
                    commit_marker: Some(CommitMarker::new(marker)),
                    memory: Vec::new(),
                });
            }
            Ok(SourceBatch::Raw {
                messages: Vec::new(),
                commit_marker: None,
                memory: Vec::new(),
            })
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, transferia::core::DataPlaneResult<()>> {
        Box::pin(async move {
            for marker in markers {
                let marker = marker
                    .value::<i64>()
                    .copied()
                    .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                self.commits.send(marker).map_err(|_| {
                    DataPlaneFailure::retryable(anyhow::anyhow!("marker commit receiver closed"))
                })?;
            }
            Ok(())
        })
    }
}

impl FakeSource {
    fn message(offset: i64) -> Message {
        Message {
            value: Bytes::from(format!(r#"{{"id":"{offset}","kind":"keep"}}"#)),
            meta: MessageMeta {
                partition: Some(0),
                offset: Some(offset),
                ..MessageMeta::default()
            },
        }
    }
}

impl Source for FakeSource {
    fn read_batch(&mut self) -> BoxFuture<'_, transferia::core::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::AcqRel);
            let messages = if let Some(messages) = self.batches.pop_front() {
                messages
            } else if self.repeat {
                let offset = self.next_offset;
                self.next_offset += 1;
                vec![Self::message(offset)]
            } else {
                return Ok(SourceBatch::Raw {
                    messages: Vec::new(),
                    commit_marker: None,
                    memory: Vec::new(),
                });
            };
            let marker = messages
                .last()
                .and_then(|message| message.meta.offset)
                .unwrap_or_default();
            Ok(SourceBatch::Raw {
                messages,
                commit_marker: Some(CommitMarker::new(marker)),
                memory: Vec::new(),
            })
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, transferia::core::DataPlaneResult<()>> {
        Box::pin(async move {
            for marker in markers {
                let offset = marker
                    .value::<i64>()
                    .copied()
                    .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                self.commits.send(offset).map_err(|_| {
                    DataPlaneFailure::retryable(anyhow::anyhow!("fake commit receiver closed"))
                })?;
            }
            Ok(())
        })
    }
}

struct FakeClickHouse {
    inserts: AtomicUsize,
    rows: AtomicUsize,
    persisted_transient_failures: AtomicUsize,
    block: bool,
    gate: Arc<Semaphore>,
    started: Notify,
}

impl FakeClickHouse {
    fn new(block: bool) -> Arc<Self> {
        Arc::new(Self {
            inserts: AtomicUsize::new(0),
            rows: AtomicUsize::new(0),
            persisted_transient_failures: AtomicUsize::new(0),
            block,
            gate: Arc::new(Semaphore::new(0)),
            started: Notify::new(),
        })
    }

    fn persist_then_fail_once() -> Arc<Self> {
        let transport = Self::new(false);
        transport
            .persisted_transient_failures
            .store(1, Ordering::Release);
        transport
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
        let persisted_but_transient = self
            .persisted_transient_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok();
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
            if persisted_but_transient {
                return Err(InsertError::Transient(anyhow::anyhow!(
                    "response lost after ClickHouse persisted the INSERT"
                )));
            }
            Ok(())
        })
    }
}

fn sink_config() -> ClickHouseSinkConfig {
    ClickHouseSinkConfig {
        hosts: vec!["fake".into()],
        port: 9000,
        trusted_plaintext: true,
        tls_ca_file: None,
        data_host_count: None,
        database: "default".into(),
        username: "default".into(),
        password: String::new(),
        shard_group: String::new(),
        insert_target_rows: 1,
        insert_target_bytes: usize::MAX,
        flush_interval_ms: 1,
        retry_initial_ms: 1,
        retry_max_ms: 10,
        retry_max_attempts: None,
        connect_timeout_ms: 30_000,
        request_timeout_ms: 30_000,
    }
}

fn parser() -> Arc<dyn transferia::parsers::ParserFactory> {
    let config: transferia::parsers::ParserConfig = serde_yaml::from_str(
        r#"
common:
  table_naming: { type: from_config, name: events }
json_parser:
  conversion_error: dlq
  unknown_fields: { action: fail }
  columns:
    - { jsonpath: "$.id", column_name: "id", json_data_type: string, arrow_type: "Utf8", nullable: false }
    - { jsonpath: "$.kind", column_name: "kind", json_data_type: string, arrow_type: "Utf8", nullable: false }
"#,
    )
    .unwrap();
    transferia::parsers::ParserPlan::from_config(&config, "topic")
        .unwrap()
        .parser()
}

fn discovery() -> Arc<DeliveryDiscovery> {
    let config: transferia::parsers::ParserConfig = serde_yaml::from_str(
        r#"
common:
  table_naming: { type: from_config, name: events }
json_parser:
  conversion_error: dlq
  unknown_fields: { action: fail }
  columns:
    - { jsonpath: "$.id", column_name: "id", json_data_type: string, arrow_type: "Utf8", nullable: false }
    - { jsonpath: "$.kind", column_name: "kind", json_data_type: string, arrow_type: "Utf8", nullable: false }
"#,
    )
    .unwrap();
    let plan = transferia::parsers::ParserPlan::from_config(&config, "topic").unwrap();
    Arc::new(
        plan.delivery_discovery(
            Arc::from("topic"),
            transferia::core::delivery::SourceTopology::StaticPartitions(vec![0]),
            DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
        )
        .unwrap(),
    )
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
        discovery(),
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
async fn ambiguous_clickhouse_insert_replay_commits_without_loss_and_can_duplicate() {
    let transport = FakeClickHouse::persist_then_fail_once();
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let mut first_config = sink_config();
    first_config.retry_max_attempts = Some(1);
    let first_source = FakeSource {
        batches: VecDeque::from([vec![FakeSource::message(7)]]),
        repeat: false,
        next_offset: 8,
        reads: Arc::new(AtomicUsize::new(0)),
        commits: commit_tx.clone(),
    };
    let first_sink = ClickHouseSink::with_transport(
        first_config,
        Arc::new(SinkCounters::new()),
        Arc::clone(&transport) as Arc<dyn InsertTransport>,
        discovery(),
    );
    let first_task = tokio::spawn(run_partition_pipeline(
        Box::new(first_source),
        parser(),
        Arc::new(Vec::new()),
        Box::new(first_sink),
        PipelineMemory::new(1024 * 1024),
        CancellationToken::new(),
        0,
        Arc::new(ParseCounters::new()),
    ));

    let first_error = tokio::time::timeout(core::time::Duration::from_secs(5), first_task)
        .await
        .expect("ambiguous first pipeline attempt did not stop")
        .expect("ambiguous first pipeline task panicked")
        .expect_err("retry_max_attempts=1 must restart after the ambiguous INSERT");
    assert!(first_error.is_retryable(), "{first_error:#}");
    assert!(matches!(
        commit_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(transport.inserts.load(Ordering::Acquire), 1);
    assert_eq!(
        transport.rows.load(Ordering::Acquire),
        1,
        "the destination persisted the row even though the source was not committed"
    );

    let mut replay_config = sink_config();
    replay_config.retry_max_attempts = Some(1);
    let replay_source = FakeSource {
        batches: VecDeque::from([vec![FakeSource::message(7)]]),
        repeat: false,
        next_offset: 8,
        reads: Arc::new(AtomicUsize::new(0)),
        commits: commit_tx,
    };
    let replay_sink = ClickHouseSink::with_transport(
        replay_config,
        Arc::new(SinkCounters::new()),
        Arc::clone(&transport) as Arc<dyn InsertTransport>,
        discovery(),
    );
    let replay_cancellation = CancellationToken::new();
    let replay_task = tokio::spawn(run_partition_pipeline(
        Box::new(replay_source),
        parser(),
        Arc::new(Vec::new()),
        Box::new(replay_sink),
        PipelineMemory::new(1024 * 1024),
        replay_cancellation.clone(),
        0,
        Arc::new(ParseCounters::new()),
    ));

    assert_eq!(
        tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv())
            .await
            .expect("replayed source commit timed out"),
        Some(7)
    );
    assert_eq!(transport.inserts.load(Ordering::Acquire), 2);
    assert_eq!(
        transport.rows.load(Ordering::Acquire),
        2,
        "at-least-once replay must preserve the row even when it creates a duplicate"
    );
    replay_cancellation.cancel();
    replay_task.await.unwrap().unwrap();
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
        discovery(),
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
            transferia::connectors::discard::sink::DiscardSink::new(Arc::new(SinkCounters::new()));
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
        assert!(!error.is_retryable());
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
        Box::new(transferia::connectors::discard::sink::DiscardSink::new(
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
