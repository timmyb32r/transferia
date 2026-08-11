use std::collections::VecDeque;
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
use crate::pipeline::source::{CommitMarker, Source};
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
    permanent: bool,
    started: Notify,
}

impl FakeUploader {
    fn immediate(failures: usize) -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(failures),
            uploads: Mutex::new(Vec::new()),
            gate: None,
            permanent: false,
            started: Notify::new(),
        })
    }

    fn blocked() -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(0),
            uploads: Mutex::new(Vec::new()),
            gate: Some(Arc::new(Semaphore::new(0))),
            permanent: false,
            started: Notify::new(),
        })
    }

    fn permanent() -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(0),
            uploads: Mutex::new(Vec::new()),
            gate: None,
            permanent: true,
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
        .unwrap_or_else(|_| panic!("upload attempt {expected} did not start"));
    }
}

impl ObjectUploader for FakeUploader {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), UploadError>> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        self.started.notify_waiters();
        Box::pin(async move {
            if self.permanent {
                return Err(UploadError::Permanent(anyhow::anyhow!("permanent")));
            }
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
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(UploadError::Cancelled),
                    permit = gate.acquire() => {
                        permit
                            .map_err(|error| UploadError::Permanent(error.into()))?
                            .forget();
                    }
                }
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
    batches: VecDeque<Vec<Message>>,
    commits: mpsc::UnboundedSender<i64>,
}

impl Source for FakeSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<MessageBatch>> {
        Box::pin(async move {
            let Some(messages) = self.batches.pop_front() else {
                return Ok(MessageBatch {
                    messages: Vec::new(),
                    partition_id: 3,
                    commit_marker: None,
                    memory: Vec::new(),
                });
            };
            let marker = messages
                .last()
                .and_then(|message| message.meta.offset)
                .ok_or_else(|| anyhow::anyhow!("fake source message is missing an offset"))?;
            Ok(MessageBatch {
                messages,
                partition_id: 3,
                commit_marker: Some(CommitMarker::new(marker)),
                memory: Vec::new(),
            })
        })
    }

    fn commit_offsets<'context>(
        &'context mut self,
        markers: &'context [CommitMarker],
    ) -> BoxFuture<'context, anyhow::Result<()>> {
        Box::pin(async move {
            for marker in markers {
                let offset = marker
                    .downcast_ref::<i64>()
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("unexpected commit marker"))?;
                self.commits
                    .send(offset)
                    .map_err(|_| anyhow::anyhow!("commit receiver closed"))?;
            }
            Ok(())
        })
    }
}

fn config(extra: &str) -> S3SinkConfig {
    config_with_rotation(1, "", extra)
}

fn config_with_rotation(max_rows: usize, rotation_extra: &str, extra: &str) -> S3SinkConfig {
    serde_yaml::from_str(&format!(
        "bucket: test\nrotation:\n  max_rows: {max_rows}\n  max_bytes: 1MiB\n{rotation_extra}buffering:\n  max_open_objects: 8\n  max_buffered_bytes: 8MiB\n  max_epoch_bytes: 8MiB\nupload:\n  multipart_threshold: 25MiB\n  part_size: 5MiB\n  parallel_parts: 4\nretry:\n  initial_backoff: 1ms\n  max_backoff: 2ms\n{extra}"
    ))
    .unwrap()
}

fn pipeline_parser() -> Arc<dyn crate::parsers::Parser> {
    let parser_raw: serde_yaml::Value = serde_yaml::from_str(
        "columns:\n  - { jsonpath: $.id, column_name: id, arrow_type: Int64, nullable: false }\n  - { jsonpath: $.nullable, column_name: nullable, arrow_type: Utf8, nullable: true }\nchunk_splitter: new-line\n",
    )
    .unwrap();
    crate::parsers::build_parser(
        "json_parser",
        parser_raw,
        Arc::from("events"),
        &crate::parsers::CommonParserConfig {
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

async fn replay_objects(uploader: Arc<FakeUploader>) -> Vec<(String, Bytes)> {
    let blocked = uploader.gate.is_some();
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 2, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_pending_upload_objects: 2, max_buffered_bytes: 8MiB, max_epoch_bytes: 1MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
    )
    .unwrap();
    let (tx, mut events, cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());

    tx.send(delivery(&memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tx.send(delivery(&memory, 2, 2, 1_001, false).await)
        .await
        .unwrap();
    if blocked {
        uploader.wait_for_attempts(1).await;
    } else {
        while events.recv().await != Some(SinkEvent::CommittedThrough(DeliveryId::new(2))) {}
    }

    tx.send(delivery(&memory, 3, 3, 1_002, false).await)
        .await
        .unwrap();
    tx.send(delivery(&memory, 4, 4, 1_003, false).await)
        .await
        .unwrap();
    if let Some(gate) = &uploader.gate {
        gate.add_permits(1);
        uploader.wait_for_attempts(2).await;
        gate.add_permits(4);
    }
    while events.recv().await != Some(SinkEvent::CommittedThrough(DeliveryId::new(4))) {}

    cancel.cancel();
    task.await.unwrap().unwrap();
    let mut objects = uploader
        .uploads
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    objects.sort_by(|left, right| left.0.cmp(&right.0));
    objects
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_boundaries_do_not_depend_on_upload_completion_timing() {
    let immediate = replay_objects(FakeUploader::immediate(0)).await;
    let blocked = replay_objects(FakeUploader::blocked()).await;

    assert_eq!(blocked, immediate);
    assert_eq!(blocked.len(), 2);
}

#[tokio::test]
async fn one_open_object_accumulates_rows_until_a_deterministic_rotation_limit() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 2, max_bytes: 1MiB }\nbuffering: { max_open_objects: 1, max_buffered_bytes: 8MiB, max_epoch_bytes: 1MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
    )
    .unwrap();
    let (tx, mut events, cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());

    tx.send(delivery(&memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
    tx.send(delivery(&memory, 2, 2, 1_001, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
    );
    {
        let uploads = uploader
            .uploads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(uploads.len(), 1);
        assert!(uploads[0]
            .1
            .windows(b"\n{".len())
            .any(|window| window == b"\n{"));
        drop(uploads);
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn uploads_multiple_closed_objects_in_parallel() {
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
    uploader.wait_for_attempts(2).await;
    assert!(events.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(2);
    loop {
        let Some(SinkEvent::CommittedThrough(id)) = events.recv().await else {
            panic!("sink event stream closed")
        };
        if id == DeliveryId::new(2) {
            break;
        }
    }
    {
        let uploads = uploader.uploads.lock().unwrap();
        assert!(uploads.iter().any(|(key, payload)| {
            key == "events/topic=topic%2Fa/partition=3/topic%2Fa+3+10.json"
                && payload == &Bytes::from_static(b"{\"id\":10,\"nullable\":null}\n")
        }));
        assert!(uploads
            .iter()
            .any(|(key, _)| { key == "events/topic=topic%2Fa/partition=3/topic%2Fa+3+11.json" }));
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
async fn late_time_slot_and_replay_are_routed_deterministically() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, mut events, cancel, task) = spawn(
        config("partitioning:\n  type: record_time\n  window: 1h\n  timezone: UTC\n  path: 'hour=%H'\n"),
        Arc::clone(&uploader),
        memory.clone(),
    );
    tx.send(delivery(&memory, 1, 1, 7_200_000, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    tx.send(delivery(&memory, 2, 2, 3_600_000, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
    );
    tx.send(delivery(&memory, 3, 2, 3_600_000, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(3)))
    );
    let keys = uploader
        .uploads
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 3);
    assert!(keys[0].contains("hour=02"));
    assert!(keys[1].contains("hour=01"));
    assert_eq!(
        keys[1], keys[2],
        "replaying an offset must overwrite its key"
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
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
    uploader.wait_for_attempts(2).await;
    assert!(events.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(1);
    tokio::task::yield_now().await;
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
async fn commit_waits_for_every_object_before_advancing_within_an_epoch() {
    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 100, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_pending_upload_objects: 8, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
    )
    .unwrap();
    let (tx, mut events, _cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());

    tx.send(delivery(&memory, 1, 12, 1_000, false).await)
        .await
        .unwrap();
    let mut second = delivery(&memory, 2, 13, 1_001, false).await;
    let dlq = delivery(&memory, 2, 13, 1_001, true).await;
    second.outputs.extend(dlq.outputs);
    tx.send(second).await.unwrap();
    drop(tx);

    uploader.wait_for_attempts(1).await;
    uploader.gate.as_ref().unwrap().add_permits(1);
    uploader.wait_for_attempts(2).await;
    tokio::task::yield_now().await;
    assert!(
        events.try_recv().is_err(),
        "a durable prefix inside an unfinished epoch must not be committed"
    );

    uploader.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
    );
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
async fn multirow_pqv1_message_with_field_partitioning_commits_after_every_object() {
    let uploader = FakeUploader::blocked();
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let source = FakeSource {
        batches: VecDeque::from([vec![Message {
            value: Bytes::from_static(
                b"{\"id\":77,\"nullable\":null}\n{\"id\":88,\"nullable\":null}",
            ),
            meta: MessageMeta {
                topic_name: Some(Arc::from("topic/a")),
                partition: Some(SourcePartition::Int(3)),
                offset: Some(77),
                write_timestamp_ms: Some(1_234),
            },
        }]]),
        commits: commit_tx,
    };
    let memory = PipelineMemory::new(1 << 20);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config("partitioning:\n  type: fields\n  columns: [id]\n"),
        Arc::clone(&uploader) as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        false,
    )
    .unwrap();
    let mut task = tokio::spawn(crate::pipeline::run_partition_pipeline(
        Box::new(source),
        pipeline_parser(),
        Arc::new(Vec::new()),
        Box::new(sink),
        memory,
        cancellation.clone(),
        3,
        Arc::new(crate::metrics::ParseCounters::new()),
    ));

    tokio::select! {
        () = uploader.wait_for_attempts(1) => {}
        result = &mut task => panic!("pipeline stopped before the first upload started: {result:?}"),
    }
    assert!(commit_rx.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(1);
    tokio::select! {
        () = uploader.wait_for_attempts(2) => {}
        commit = commit_rx.recv() => {
            panic!("committed {commit:?} after only one durable object");
        }
    }
    assert!(commit_rx.try_recv().is_err());
    uploader.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(commit_rx.recv().await, Some(77));
    let keys = uploader
        .uploads
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().any(|key| key.contains("id=77")));
    assert!(keys.iter().any(|key| key.contains("id=88")));
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deterministic_epoch_can_grow_beyond_pipeline_channel_capacity() {
    const DELIVERIES: i64 = 20;

    let uploader = FakeUploader::immediate(0);
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let batches = (1..=DELIVERIES)
        .map(|offset| Message {
            value: Bytes::from(format!("{{\"id\":{offset},\"nullable\":null}}")),
            meta: MessageMeta {
                topic_name: Some(Arc::from("topic/a")),
                partition: Some(SourcePartition::Int(3)),
                offset: Some(offset),
                write_timestamp_ms: Some(1_234 + offset),
            },
        })
        .map(|message| vec![message])
        .collect();
    let source = FakeSource {
        batches,
        commits: commit_tx,
    };
    let memory = PipelineMemory::new(1 << 20);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config_with_rotation(DELIVERIES as usize, "", "partitioning: { type: source }\n"),
        Arc::clone(&uploader) as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        false,
    )
    .unwrap();
    let task = tokio::spawn(crate::pipeline::run_partition_pipeline(
        Box::new(source),
        pipeline_parser(),
        Arc::new(Vec::new()),
        Box::new(sink),
        memory,
        cancellation.clone(),
        3,
        Arc::new(crate::metrics::ParseCounters::new()),
    ));

    for expected in 1..=DELIVERIES {
        let committed = tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv())
            .await
            .expect("deterministic S3 epoch stalled before reaching its row threshold");
        assert_eq!(committed, Some(expected));
    }
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 1);
    cancellation.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_epoch_releases_memory_before_a_delivery_tail_closes() {
    let uploader = FakeUploader::immediate(0);
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let message = |offset| Message {
        value: Bytes::from(format!("{{\"id\":{offset},\"nullable\":null}}")),
        meta: MessageMeta {
            topic_name: Some(Arc::from("topic/a")),
            partition: Some(SourcePartition::Int(3)),
            offset: Some(offset),
            write_timestamp_ms: Some(1_234 + offset),
        },
    };
    // The first timing-dependent read Delivery straddles two deterministic
    // epochs: offsets 1-2 close an epoch, while offset 3 remains in the next
    // one. Releasing the durable epoch's leases must let the parser accept the
    // second Delivery, whose offset 4 deterministically closes the tail.
    let source = FakeSource {
        batches: VecDeque::from([vec![message(1), message(2), message(3)], vec![message(4)]]),
        commits: commit_tx,
    };
    let memory = PipelineMemory::new(256);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config_with_rotation(2, "", "partitioning: { type: source }\n"),
        Arc::clone(&uploader) as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        false,
    )
    .unwrap();
    let task = tokio::spawn(crate::pipeline::run_partition_pipeline(
        Box::new(source),
        pipeline_parser(),
        Arc::new(Vec::new()),
        Box::new(sink),
        memory,
        cancellation.clone(),
        3,
        Arc::new(crate::metrics::ParseCounters::new()),
    ));

    for expected in [3, 4] {
        let committed = tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv())
            .await
            .expect("S3 epoch memory ownership stalled the next delivery");
        assert_eq!(committed, Some(expected));
    }
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 2);
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
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_buffered_bytes: 40, max_epoch_bytes: 40 }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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

#[tokio::test]
async fn pending_object_limit_bounds_metadata_during_an_outage() {
    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_pending_upload_objects: 2, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
        .expect("one delivery should fit in the bounded input channel");
    let fourth = delivery(&memory, 4, 13, 1_003, false).await;
    assert!(matches!(
        tx.try_send(fourth),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn retry_budget_turns_persistent_transient_failure_into_sink_error() {
    let uploader = FakeUploader::immediate(100);
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms, max_attempts: 2 }\n",
    )
    .unwrap();
    let (tx, _events, _cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();

    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("exhausted 2 attempts"));
    assert!(error
        .downcast_ref::<crate::pipeline::PipelineFailure>()
        .is_some_and(crate::pipeline::PipelineFailure::is_retryable));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn permanent_upload_failure_is_non_retryable() {
    let uploader = FakeUploader::permanent();
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(config(""), uploader, memory.clone());
    tx.send(delivery(&memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = error
        .downcast_ref::<crate::pipeline::PipelineFailure>()
        .expect("permanent S3 error must preserve its restart contract");
    assert!(!failure.is_retryable());
}

#[tokio::test]
async fn deterministic_routing_failure_is_non_retryable() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    let mut invalid = delivery(&memory, 1, 4, 1_000, false).await;
    invalid.outputs[0].system_columns = SystemColumns::default();
    tx.send(invalid).await.unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = error
        .downcast_ref::<crate::pipeline::PipelineFailure>()
        .expect("deterministic S3 routing errors must preserve their restart contract");
    assert!(!failure.is_retryable());
    assert!(error.to_string().contains("required system column"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn delivery_progress_violation_is_non_retryable() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 2, 4, 1_000, false).await)
        .await
        .unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = error
        .downcast_ref::<crate::pipeline::PipelineFailure>()
        .expect("S3 progress violations must preserve their restart contract");
    assert!(!failure.is_retryable());
    assert!(error.to_string().contains("delivery order violation"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn explicit_epoch_byte_limit_rotates_independently_of_pipeline_memory() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 100, max_bytes: 1MiB }\nbuffering: { max_open_objects: 8, max_pending_upload_objects: 8, max_buffered_bytes: 1MiB, max_epoch_bytes: 300 }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
    )
    .unwrap();
    let (tx, mut events, cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
    tx.send(delivery(&memory, 2, 2, 1_001, false).await)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
    );
    {
        let uploads = uploader
            .uploads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(uploads.len(), 1);
        assert!(uploads[0]
            .1
            .windows(b"\n{".len())
            .any(|window| window == b"\n{"));
        drop(uploads);
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
}
