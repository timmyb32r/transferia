use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::metrics::SinkCounters;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::sink::{
    Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo,
};
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::message::{Message, MessageBatch, MessageMeta, SourcePartition};
use crate::types::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};

use super::actor::S3Sink;
use super::config::S3SinkConfig;
use super::upload::{ObjectUploader, UploadError};

struct FakeUploader {
    attempts: AtomicUsize,
    failures_left: AtomicUsize,
    uploads: Mutex<Vec<(String, Bytes)>>,
    gate: Option<Arc<Semaphore>>,
    started: Notify,
}

impl FakeUploader {
    fn immediate(failures: usize) -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(failures),
            uploads: Mutex::new(Vec::new()),
            gate: None,
            started: Notify::new(),
        })
    }

    fn blocked() -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(0),
            uploads: Mutex::new(Vec::new()),
            gate: Some(Arc::new(Semaphore::new(0))),
            started: Notify::new(),
        })
    }

    async fn wait_for_attempts(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while self.attempts.load(Ordering::Acquire) < expected {
                self.started.notified().await;
            }
        })
        .await
        .expect("upload attempt did not start");
    }
}

impl ObjectUploader for FakeUploader {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
    ) -> BoxFuture<'a, Result<(), UploadError>> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        self.started.notify_waiters();
        Box::pin(async move {
            if self
                .failures_left
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(UploadError::Retryable(anyhow::anyhow!("temporary")));
            }
            if let Some(gate) = &self.gate {
                gate.acquire()
                    .await
                    .map_err(|error| UploadError::Permanent(error.into()))?
                    .forget();
            }
            self.uploads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((key.to_owned(), payload));
            Ok(())
        })
    }
}

struct FakeSource {
    message: Option<Message>,
    commits: mpsc::UnboundedSender<i64>,
}

impl Source for FakeSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            let Some(message) = self.message.take() else {
                return Ok(ReadResult::Batch(MessageBatch {
                    messages: Vec::new(),
                    partition_id: 3,
                    commit_marker: None,
                    memory: Vec::new(),
                }));
            };
            Ok(ReadResult::Batch(MessageBatch {
                messages: vec![message],
                partition_id: 3,
                commit_marker: Some(CommitMarker::new(77_i64)),
                memory: Vec::new(),
            }))
        })
    }

    fn commit_offsets<'context>(
        &'context mut self,
        marker: &'context CommitMarker,
    ) -> BoxFuture<'context, anyhow::Result<()>> {
        Box::pin(async move {
            let offset = marker
                .downcast_ref::<i64>()
                .copied()
                .ok_or_else(|| anyhow::anyhow!("unexpected commit marker"))?;
            self.commits
                .send(offset)
                .map_err(|_| anyhow::anyhow!("commit receiver closed"))
        })
    }
}

fn config(extra: &str) -> S3SinkConfig {
    config_with_rotation(1, "", extra)
}

fn config_with_rotation(max_rows: usize, rotation_extra: &str, extra: &str) -> S3SinkConfig {
    serde_yaml::from_str(&format!(
        "bucket: test\nrotation:\n  max_rows: {max_rows}\n  max_bytes: 1MiB\n{rotation_extra}buffering:\n  max_open_objects: 8\n  max_buffered_bytes: 8MiB\nupload:\n  multipart_threshold: 25MiB\n  part_size: 5MiB\n  parallel_parts: 4\nretry:\n  initial_backoff: 1ms\n  max_backoff: 2ms\n{extra}"
    ))
    .unwrap()
}

async fn delivery(
    memory: &PipelineMemory,
    id: u64,
    offset: i64,
    timestamp_ms: i64,
    is_dlq: bool,
) -> Delivery {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nullable", DataType::Utf8, true),
        Field::new(SystemColumnKind::TopicName.name(), DataType::Utf8, false),
        Field::new(
            SystemColumnKind::PartitionNum.name(),
            DataType::Int64,
            false,
        ),
        Field::new(SystemColumnKind::Offset.name(), DataType::Int64, false),
        Field::new(
            SystemColumnKind::MessageIndex.name(),
            DataType::UInt64,
            false,
        ),
        Field::new(
            SystemColumnKind::WriteTimestampMs.name(),
            DataType::Int64,
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![offset])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["topic/a"])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![offset])),
            Arc::new(UInt64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![timestamp_ms])),
        ],
    )
    .unwrap();
    let bytes = batch.get_array_memory_size();
    let kinds = [
        SystemColumnKind::TopicName,
        SystemColumnKind::PartitionNum,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::WriteTimestampMs,
    ];
    Delivery {
        id: DeliveryId::new(id),
        outputs: vec![SinkBatch {
            table: Arc::from(if is_dlq { "events_dlq" } else { "events" }),
            is_dlq,
            batch,
            byte_size: bytes,
            memory: memory.reserve(bytes).await,
            system_columns: SystemColumns::new(
                kinds
                    .into_iter()
                    .enumerate()
                    .map(|(position, kind)| SystemColumn {
                        kind,
                        index: position + 2,
                    })
                    .collect::<Vec<_>>(),
            ),
        }],
        meta: DeliveryMeta {
            source_messages: 1,
            first_offset: Some(offset),
            last_offset: Some(offset),
            ..DeliveryMeta::default()
        },
    }
}

fn spawn(
    config: S3SinkConfig,
    uploader: Arc<FakeUploader>,
    memory: PipelineMemory,
) -> (
    mpsc::Sender<Delivery>,
    mpsc::Receiver<SinkEvent>,
    CancellationToken,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    spawn_with_system_columns(config, uploader, memory, false)
}

fn spawn_with_system_columns(
    config: S3SinkConfig,
    uploader: Arc<FakeUploader>,
    memory: PipelineMemory,
    keep_system_columns: bool,
) -> (
    mpsc::Sender<Delivery>,
    mpsc::Receiver<SinkEvent>,
    CancellationToken,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    spawn_with_capacity(config, uploader, memory, keep_system_columns, 8)
}

fn spawn_with_capacity(
    config: S3SinkConfig,
    uploader: Arc<FakeUploader>,
    memory: PipelineMemory,
    keep_system_columns: bool,
    channel_capacity: usize,
) -> (
    mpsc::Sender<Delivery>,
    mpsc::Receiver<SinkEvent>,
    CancellationToken,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let (delivery_tx, delivery_rx) = mpsc::channel(channel_capacity);
    let (event_tx, event_rx) = mpsc::channel(8);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config,
        uploader as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        keep_system_columns,
    )
    .unwrap();
    let task = tokio::spawn(Box::new(sink).run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory,
        cancellation: cancellation.clone(),
    }));
    (delivery_tx, event_rx, cancellation, task)
}

#[tokio::test]
async fn buffers_next_epoch_while_one_upload_is_in_flight() {
    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 10, 1_000, false).await)
        .await
        .unwrap();
    uploader.wait_for_attempts(1).await;
    tx.send(delivery(&memory, 2, 11, 1_001, false).await)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(events.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    uploader.wait_for_attempts(2).await;
    uploader.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
    );
    {
        let uploads = uploader.uploads.lock().unwrap();
        assert_eq!(
            uploads[0].0,
            "events/topic=topic%2Fa/partition=3/topic%2Fa+3+10.json"
        );
        assert_eq!(
            uploads[0].1,
            Bytes::from_static(b"{\"id\":10,\"nullable\":null}\n")
        );
        drop(uploads);
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn retries_transient_failure_without_early_commit() {
    let uploader = FakeUploader::immediate(2);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 3);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dlq_uses_fixed_source_partition_route() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) = spawn(
        config("partitioning:\n  type: fields\n  columns: [id]\n"),
        Arc::clone(&uploader),
        memory.clone(),
    );
    tx.send(delivery(&memory, 1, 9, 1_000, true).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    {
        let uploads = uploader.uploads.lock().unwrap();
        assert!(uploads[0]
            .0
            .starts_with("events_dlq/topic=topic%2Fa/partition=3/"));
        drop(uploads);
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn time_partition_regression_is_fatal() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(
        config("partitioning:\n  type: time\n  window: 1h\n  timezone: UTC\n  path: 'hour=%H'\n"),
        uploader,
        memory.clone(),
    );
    tx.send(delivery(&memory, 1, 1, 7_200_000, false).await)
        .await
        .unwrap();
    tx.send(delivery(&memory, 2, 2, 3_600_000, false).await)
        .await
        .unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("time partition regression"));
}

#[tokio::test]
async fn commit_waits_for_main_and_dlq_objects_in_same_epoch() {
    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    let mut combined = delivery(&memory, 1, 12, 1_000, false).await;
    let dlq = delivery(&memory, 1, 12, 1_000, true).await;
    combined.outputs.extend(dlq.outputs);
    tx.send(combined).await.unwrap();
    uploader.wait_for_attempts(1).await;
    uploader.gate.as_ref().unwrap().add_permits(1);
    uploader.wait_for_attempts(2).await;
    assert!(events.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn can_explicitly_keep_system_columns_in_json() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) =
        spawn_with_system_columns(config(""), Arc::clone(&uploader), memory.clone(), true);
    tx.send(delivery(&memory, 1, 5, 1_234, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    {
        let uploads = uploader.uploads.lock().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&uploads[0].1).unwrap();
        drop(uploads);
        assert_eq!(json[SystemColumnKind::TopicName.name()], "topic/a");
        assert_eq!(json[SystemColumnKind::WriteTimestampMs.name()], 1_234);
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pqv1_parser_to_s3_commits_source_only_after_upload() {
    let uploader = FakeUploader::blocked();
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let source = FakeSource {
        message: Some(Message {
            value: Bytes::from_static(b"{\"id\":77,\"nullable\":null}"),
            meta: MessageMeta {
                topic_name: Some(Arc::from("topic/a")),
                partition: Some(SourcePartition::Int(3)),
                offset: Some(77),
                write_timestamp_ms: Some(1_234),
            },
        }),
        commits: commit_tx,
    };
    let parser_raw: serde_yaml::Value = serde_yaml::from_str(
        "columns:\n  - { jsonpath: $.id, column_name: id, arrow_type: Int64, nullable: false }\n  - { jsonpath: $.nullable, column_name: nullable, arrow_type: Utf8, nullable: true }\nchunk_splitter: one-message-one-row\n",
    )
    .unwrap();
    let parser = crate::parsers::build_parser(
        "json_parser",
        parser_raw,
        Arc::from("events"),
        crate::parsers::CommonParserConfig {
            table_naming: crate::parsers::TableNaming {
                kind: "from_config".into(),
                name: Some("events".into()),
            },
            system_columns: crate::parsers::SystemColumnsConfig {
                topic_name: true,
                partition_num: true,
                offset: true,
                message_index: true,
                write_timestamp_ms: true,
            },
        },
    )
    .unwrap();
    let memory = PipelineMemory::new(1 << 20);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config(""),
        Arc::clone(&uploader) as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        false,
    )
    .unwrap();
    let task = tokio::spawn(crate::pipeline::run_partition_pipeline(
        Box::new(source),
        parser,
        Arc::new(Vec::new()),
        Box::new(sink),
        memory,
        cancellation.clone(),
        3,
        Arc::new(crate::metrics::ParseCounters::new()),
    ));

    uploader.wait_for_attempts(1).await;
    assert!(commit_rx.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(commit_rx.recv().await, Some(77));
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn record_time_rotation_is_data_deterministic() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let config = config_with_rotation(100, "  record_time_interval: 100ms\n", "");
    let (tx, mut events, cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
    tx.send(delivery(&memory, 2, 2, 1_100, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    let key = uploader.uploads.lock().unwrap()[0].0.clone();
    assert!(key.ends_with("+3+1.json"));
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn partition_change_mode_selects_confluent_or_keep_open_behavior() {
    let rotation = "  on_partition_change: rotate\n";
    let partitioning = "partitioning:\n  type: fields\n  columns: [id]\n";
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) = spawn(
        config_with_rotation(100, rotation, partitioning),
        Arc::clone(&uploader),
        memory.clone(),
    );
    tx.send(delivery(&memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tx.send(delivery(&memory, 2, 2, 1_001, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancel.cancel();
    task.await.unwrap().unwrap();

    let keep_open_uploader = FakeUploader::immediate(0);
    let keep_open_memory = PipelineMemory::new(1 << 20);
    let (tx, _events, cancel, task) = spawn(
        config_with_rotation(100, "", partitioning),
        Arc::clone(&keep_open_uploader),
        keep_open_memory.clone(),
    );
    tx.send(delivery(&keep_open_memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tx.send(delivery(&keep_open_memory, 2, 2, 1_001, false).await)
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(keep_open_uploader.attempts.load(Ordering::Acquire), 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn wall_clock_rotation_flushes_an_idle_epoch() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let config = config_with_rotation(100, "  wall_clock_interval: 100ms\n", "");
    let (tx, mut events, cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn buffering_limit_stops_delivery_reception_during_an_outage() {
    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_buffered_bytes: 40 }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
    )
    .unwrap();
    let (tx, _events, cancel, task) =
        spawn_with_capacity(config, Arc::clone(&uploader), memory.clone(), false, 1);
    tx.send(delivery(&memory, 1, 10, 1_000, false).await)
        .await
        .unwrap();
    uploader.wait_for_attempts(1).await;
    tx.send(delivery(&memory, 2, 11, 1_001, false).await)
        .await
        .unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tx.try_send(delivery(&memory, 3, 12, 1_002, false).await)
        .expect("one delivery should fit in the bounded channel");
    let fourth = delivery(&memory, 4, 13, 1_003, false).await;
    assert!(matches!(
        tx.try_send(fourth),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    cancel.cancel();
    task.await.unwrap().unwrap();
}
