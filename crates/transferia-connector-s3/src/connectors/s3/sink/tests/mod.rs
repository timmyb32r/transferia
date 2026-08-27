use std::collections::{BTreeMap, VecDeque};
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
use transferia_core::data::message::{Message, MessageMeta, SourceBatch};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{
    Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo,
};
use transferia_core::source::{CommitMarker, Source};

use super::actor::S3Sink;
use super::config::S3SinkConfig;
use super::upload::{ObjectUploader, UploadError};

fn durable_storage() -> Arc<dyn crate::durable::DurableStorage> {
    crate::durable::test_support::context().storage
}

struct FakeUploader {
    attempts: AtomicUsize,
    failures_left: AtomicUsize,
    uploads: Mutex<Vec<(String, Bytes)>>,
    started_uploads: Mutex<Vec<(usize, String)>>,
    gate: Option<Arc<Semaphore>>,
    attempt_gates: Option<Vec<Arc<Semaphore>>>,
    permanent: bool,
    started: Notify,
    completed: Notify,
}

impl FakeUploader {
    fn immediate(failures: usize) -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(failures),
            uploads: Mutex::new(Vec::new()),
            started_uploads: Mutex::new(Vec::new()),
            gate: None,
            attempt_gates: None,
            permanent: false,
            started: Notify::new(),
            completed: Notify::new(),
        })
    }

    fn blocked() -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(0),
            uploads: Mutex::new(Vec::new()),
            started_uploads: Mutex::new(Vec::new()),
            gate: Some(Arc::new(Semaphore::new(0))),
            attempt_gates: None,
            permanent: false,
            started: Notify::new(),
            completed: Notify::new(),
        })
    }

    fn controlled(attempts: usize) -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(0),
            uploads: Mutex::new(Vec::new()),
            started_uploads: Mutex::new(Vec::new()),
            gate: None,
            attempt_gates: Some((0..attempts).map(|_| Arc::new(Semaphore::new(0))).collect()),
            permanent: false,
            started: Notify::new(),
            completed: Notify::new(),
        })
    }

    fn permanent() -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            failures_left: AtomicUsize::new(0),
            uploads: Mutex::new(Vec::new()),
            started_uploads: Mutex::new(Vec::new()),
            gate: None,
            attempt_gates: None,
            permanent: true,
            started: Notify::new(),
            completed: Notify::new(),
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

    async fn wait_for_uploads(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let completed = self.completed.notified();
                if self
                    .uploads
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
                    >= expected
                {
                    return;
                }
                completed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("upload {expected} did not complete"));
    }

    async fn wait_for_started_uploads(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let started = self.started.notified();
                if self
                    .started_uploads
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
                    >= expected
                {
                    return;
                }
                started.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("upload start {expected} was not recorded"));
    }

    fn release_attempt(&self, attempt: usize) {
        self.attempt_gates
            .as_ref()
            .and_then(|gates| gates.get(attempt))
            .unwrap_or_else(|| panic!("missing gate for upload attempt {attempt}"))
            .add_permits(1);
    }

    fn release_key_suffix(&self, suffix: &str) {
        let attempt = self
            .started_uploads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find_map(|(attempt, key)| key.ends_with(suffix).then_some(*attempt))
            .unwrap_or_else(|| panic!("upload ending with {suffix:?} has not started"));
        self.release_attempt(attempt);
    }
}

impl ObjectUploader for FakeUploader {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), UploadError>> {
        let attempt = self.attempts.fetch_add(1, Ordering::AcqRel);
        self.started_uploads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((attempt, key.to_owned()));
        self.started.notify_waiters();
        let gate = self
            .attempt_gates
            .as_ref()
            .and_then(|gates| gates.get(attempt))
            .cloned()
            .or_else(|| self.gate.clone());
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
            if let Some(gate) = gate {
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
            self.completed.notify_waiters();
            Ok(())
        })
    }
}

enum ReplayUploadPlan {
    Success,
    Retryable,
    Gated(Arc<Semaphore>),
}

struct PersistentReplayUploader {
    objects: Arc<Mutex<BTreeMap<String, Bytes>>>,
    plans: Mutex<VecDeque<ReplayUploadPlan>>,
    attempts: AtomicUsize,
    started: Notify,
}

impl PersistentReplayUploader {
    fn new(
        objects: Arc<Mutex<BTreeMap<String, Bytes>>>,
        plans: impl IntoIterator<Item = ReplayUploadPlan>,
    ) -> Arc<Self> {
        Arc::new(Self {
            objects,
            plans: Mutex::new(plans.into_iter().collect()),
            attempts: AtomicUsize::new(0),
            started: Notify::new(),
        })
    }

    async fn wait_for_attempts(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let started = self.started.notified();
                if self.attempts.load(Ordering::Acquire) >= expected {
                    return;
                }
                started.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("persistent upload attempt {expected} did not start"));
    }
}

impl ObjectUploader for PersistentReplayUploader {
    fn upload<'a>(
        &'a self,
        key: &'a str,
        payload: Bytes,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), UploadError>> {
        let plan = self
            .plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(ReplayUploadPlan::Success);
        self.attempts.fetch_add(1, Ordering::AcqRel);
        self.started.notify_waiters();
        Box::pin(async move {
            match plan {
                ReplayUploadPlan::Success => {}
                ReplayUploadPlan::Retryable => {
                    return Err(UploadError::Retryable(anyhow::anyhow!(
                        "injected partial-epoch failure"
                    )));
                }
                ReplayUploadPlan::Gated(gate) => {
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
            }
            self.objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key.to_owned(), payload);
            Ok(())
        })
    }
}

struct FakeSource {
    batches: VecDeque<Vec<Message>>,
    commits: mpsc::UnboundedSender<i64>,
}

impl Source for FakeSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let Some(messages) = self.batches.pop_front() else {
                return Ok(SourceBatch::Raw {
                    messages: Vec::new(),
                    commit_marker: None,
                    memory: Vec::new(),
                });
            };
            let marker = messages
                .last()
                .and_then(|message| message.meta.offset)
                .ok_or_else(|| {
                    transferia_core::failure::DataPlaneFailure::fatal(anyhow::anyhow!(
                        "fake source message is missing an offset"
                    ))
                })?;
            Ok(SourceBatch::Raw {
                messages,
                commit_marker: Some(CommitMarker::new(marker)),
                memory: Vec::new(),
            })
        })
    }

    fn commit_offsets<'context>(
        &'context mut self,
        markers: &'context [CommitMarker],
    ) -> BoxFuture<'context, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            for marker in markers {
                let offset = marker.value::<i64>().copied().map_err(|error| {
                    transferia_core::failure::DataPlaneFailure::fatal(error.into())
                })?;
                self.commits.send(offset).map_err(|_| {
                    transferia_core::failure::DataPlaneFailure::retryable(anyhow::anyhow!(
                        "commit receiver closed"
                    ))
                })?;
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
        "bucket: test\nrotation:\n  max_rows: {max_rows}\n  max_bytes: 1MiB\n{rotation_extra}buffering:\n  max_epoch_buffers: 8\n  max_buffered_bytes: 8MiB\n  max_epoch_bytes: 8MiB\nupload:\n  multipart_threshold: 25MiB\n  part_size: 5MiB\n  parallel_parts: 4\nretry:\n  initial_backoff: 1ms\n  max_backoff: 2ms\n{extra}"
    ))
    .unwrap()
}

fn pipeline_parser() -> Arc<dyn crate::parsers::ParserFactory> {
    let config: crate::parsers::ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { topic: _system_topic, partition: _system_partition, offset: _system_offset, message_index: _system_message_index, write_timestamp_ms: _system_write_timestamp_ms }\njson_parser:\n  columns:\n    - { jsonpath: $.id, column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }\n    - { jsonpath: $.nullable, column_name: nullable, json_data_type: string, arrow_type: Utf8, nullable: true }\n  json_framing: json_lines\n  conversion_error: dlq\n  unknown_fields: { action: fail }\n",
    )
    .unwrap();
    crate::parsers::ParserPlan::from_config(&config, "topic")
        .unwrap()
        .parser()
}

fn test_discovery(keep_system_columns: bool) -> Arc<DeliveryDiscovery> {
    let system_columns = [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::WriteTimestampMs,
    ];
    let incoming_schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("nullable".into(), DataType::Utf8, true),
        SchemaColumn::new(
            SystemColumnKind::Topic.default_name().into(),
            DataType::Utf8,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::Partition.default_name().into(),
            DataType::Int64,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::Offset.default_name().into(),
            DataType::Int64,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::MessageIndex.default_name().into(),
            DataType::UInt64,
            false,
        ),
        SchemaColumn::new(
            SystemColumnKind::WriteTimestampMs.default_name().into(),
            DataType::Int64,
            false,
        ),
    ]);
    let stored_schema = if keep_system_columns {
        incoming_schema.clone()
    } else {
        DatasetSchema::new(incoming_schema.columns[..2].to_vec())
    };
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("topic-a"),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![3]),
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns,
        datasets: [
            (DatasetRole::Main, "events"),
            (DatasetRole::DeadLetterQueue, "events_dlq"),
        ]
        .into_iter()
        .map(|(role, name)| DiscoveredDataset {
            role,
            name: Arc::from(name),
            incoming_schema: incoming_schema.clone(),
            stored_schema: stored_schema.clone(),
            system_columns: system_columns.iter().copied().map(Into::into).collect(),
        })
        .collect(),
        performance_advice: Vec::new(),
    })
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
        Field::new(
            SystemColumnKind::Topic.default_name(),
            DataType::Utf8,
            false,
        ),
        Field::new(
            SystemColumnKind::Partition.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::MessageIndex.default_name(),
            DataType::UInt64,
            false,
        ),
        Field::new(
            SystemColumnKind::WriteTimestampMs.default_name(),
            DataType::Int64,
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![offset])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["topic-a"])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![offset])),
            Arc::new(UInt64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![timestamp_ms])),
        ],
    )
    .unwrap();
    let bytes = batch.get_array_memory_size();
    let kinds = [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
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
                        name: Arc::from(kind.default_name()),
                        index: position + 2,
                    })
                    .collect::<Vec<_>>(),
            ),
        }],
        meta: DeliveryMeta { source_messages: 1 },
    }
}

fn multi_message_delivery(memory: &PipelineMemory, id: u64, offsets: &[i64]) -> Delivery {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nullable", DataType::Utf8, true),
        Field::new(
            SystemColumnKind::Topic.default_name(),
            DataType::Utf8,
            false,
        ),
        Field::new(
            SystemColumnKind::Partition.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        ),
        Field::new(
            SystemColumnKind::MessageIndex.default_name(),
            DataType::UInt64,
            false,
        ),
        Field::new(
            SystemColumnKind::WriteTimestampMs.default_name(),
            DataType::Int64,
            false,
        ),
    ]));
    let rows = offsets.len();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(offsets.to_vec())),
            Arc::new(StringArray::from(vec![None::<&str>; rows])),
            Arc::new(StringArray::from(vec!["topic-a"; rows])),
            Arc::new(Int64Array::from(vec![3; rows])),
            Arc::new(Int64Array::from(offsets.to_vec())),
            Arc::new(UInt64Array::from(vec![0; rows])),
            Arc::new(Int64Array::from(
                offsets
                    .iter()
                    .map(|offset| 1_000 + offset)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let bytes = batch.get_array_memory_size();
    let kinds = [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::WriteTimestampMs,
    ];
    Delivery {
        id: DeliveryId::new(id),
        outputs: vec![SinkBatch {
            table: Arc::from("events"),
            is_dlq: false,
            batch,
            byte_size: bytes,
            memory: memory.reserve_transform(bytes),
            system_columns: SystemColumns::new(
                kinds
                    .into_iter()
                    .enumerate()
                    .map(|(position, kind)| SystemColumn {
                        kind,
                        name: Arc::from(kind.default_name()),
                        index: position + 2,
                    })
                    .collect::<Vec<_>>(),
            ),
        }],
        meta: DeliveryMeta {
            source_messages: rows as u64,
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
    tokio::task::JoinHandle<transferia_core::failure::DataPlaneResult<()>>,
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
    tokio::task::JoinHandle<transferia_core::failure::DataPlaneResult<()>>,
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
    tokio::task::JoinHandle<transferia_core::failure::DataPlaneResult<()>>,
) {
    spawn_with_storage(
        config,
        uploader,
        memory,
        keep_system_columns,
        channel_capacity,
        durable_storage(),
    )
}

fn spawn_with_storage(
    config: S3SinkConfig,
    uploader: Arc<FakeUploader>,
    memory: PipelineMemory,
    keep_system_columns: bool,
    channel_capacity: usize,
    storage: Arc<dyn crate::durable::DurableStorage>,
) -> (
    mpsc::Sender<Delivery>,
    mpsc::Receiver<SinkEvent>,
    CancellationToken,
    tokio::task::JoinHandle<transferia_core::failure::DataPlaneResult<()>>,
) {
    let (delivery_tx, delivery_rx) = mpsc::channel(channel_capacity);
    let (event_tx, event_rx) = mpsc::channel(8);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config,
        uploader as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        keep_system_columns,
        3,
        test_discovery(keep_system_columns),
        storage,
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
async fn closed_epoch_replay_after_actor_restart_skips_put_and_commits() {
    let durable = durable_storage();
    let first_uploader = FakeUploader::immediate(0);
    let first_memory = PipelineMemory::new(1 << 20);
    let (first_tx, mut first_events, first_cancel, first_task) = spawn_with_storage(
        config(""),
        Arc::clone(&first_uploader),
        first_memory.clone(),
        false,
        8,
        Arc::clone(&durable),
    );
    first_tx
        .send(delivery(&first_memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();
    assert_eq!(
        first_events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    assert_eq!(first_uploader.attempts.load(Ordering::Acquire), 1);
    first_cancel.cancel();
    first_task.await.unwrap().unwrap();

    let replay_uploader = FakeUploader::immediate(0);
    let replay_memory = PipelineMemory::new(1 << 20);
    let (replay_tx, mut replay_events, replay_cancel, replay_task) = spawn_with_storage(
        config(""),
        Arc::clone(&replay_uploader),
        replay_memory.clone(),
        false,
        8,
        durable,
    );
    replay_tx
        .send(delivery(&replay_memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();
    assert_eq!(
        replay_events.recv().await,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    assert_eq!(
        replay_uploader.attempts.load(Ordering::Acquire),
        0,
        "CLOSED durable epoch must recover commit without another PUT"
    );
    replay_cancel.cancel();
    replay_task.await.unwrap().unwrap();
}

async fn replay_objects(uploader: Arc<FakeUploader>) -> Vec<(String, Bytes)> {
    let blocked = uploader.gate.is_some();
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 2, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_pending_upload_objects: 2, max_buffered_bytes: 8MiB, max_epoch_bytes: 1MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
        "bucket: test\nrotation: { max_rows: 2, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 1, max_buffered_bytes: 8MiB, max_epoch_bytes: 1MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
            key == "events/topic=topic-a/partition=3/topic-a+3+10.json"
                && payload == &Bytes::from_static(b"{\"id\":10,\"nullable\":null}\n")
        }));
        assert!(uploads
            .iter()
            .any(|(key, _)| { key == "events/topic=topic-a/partition=3/topic-a+3+11.json" }));
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
            .starts_with("events_dlq/topic=topic-a/partition=3/"));
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
    assert_eq!(keys.len(), 2, "CLOSED replay must not issue another PUT");
    assert!(keys[0].contains("hour=02"));
    assert!(keys[1].contains("hour=01"));
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
        "bucket: test\nrotation: { max_rows: 100, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_pending_upload_objects: 8, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
        assert_eq!(json[SystemColumnKind::Topic.default_name()], "topic-a");
        assert_eq!(
            json[SystemColumnKind::WriteTimestampMs.default_name()],
            1_234
        );
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
                topic: Some(Arc::from("topic-a")),
                partition: Some(3),
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
        3,
        test_discovery(false),
        durable_storage(),
    )
    .unwrap();
    let mut task = tokio::spawn(transferia_pipeline::run_partition_pipeline(
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
async fn partial_epoch_failure_replays_to_the_uninterrupted_object_map() {
    let start_pipeline = |uploader: Arc<PersistentReplayUploader>,
                          commits: mpsc::UnboundedSender<i64>| {
        let source = FakeSource {
            batches: VecDeque::from([vec![Message {
                value: Bytes::from_static(
                    b"{\"id\":77,\"nullable\":null}\n{\"id\":88,\"nullable\":null}",
                ),
                meta: MessageMeta {
                    topic: Some(Arc::from("topic-a")),
                    partition: Some(3),
                    offset: Some(77),
                    write_timestamp_ms: Some(1_234),
                },
            }]]),
            commits,
        };
        let config: S3SinkConfig = serde_yaml::from_str(
                "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_pending_upload_objects: 8, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms, max_attempts: 1 }\npartitioning: { type: fields, columns: [id] }\n",
            )
            .unwrap();
        let memory = PipelineMemory::new(1 << 20);
        let cancellation = CancellationToken::new();
        let sink = S3Sink::new(
            config,
            uploader as Arc<dyn ObjectUploader>,
            Arc::new(SinkCounters::new()),
            false,
            3,
            test_discovery(false),
            durable_storage(),
        )
        .unwrap();
        let task = tokio::spawn(transferia_pipeline::run_partition_pipeline(
            Box::new(source),
            pipeline_parser(),
            Arc::new(Vec::new()),
            Box::new(sink),
            memory,
            cancellation.clone(),
            3,
            Arc::new(crate::metrics::ParseCounters::new()),
        ));
        (cancellation, task)
    };

    let objects = Arc::new(Mutex::new(BTreeMap::new()));
    let first_uploader = PersistentReplayUploader::new(
        Arc::clone(&objects),
        [ReplayUploadPlan::Success, ReplayUploadPlan::Retryable],
    );
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let (_first_cancellation, first_task) = start_pipeline(first_uploader, commit_tx.clone());
    let first_error = tokio::time::timeout(std::time::Duration::from_secs(5), first_task)
        .await
        .expect("partial-epoch pipeline attempt did not stop")
        .expect("partial-epoch pipeline task panicked")
        .expect_err("the injected second-object failure must restart the pipeline");
    assert!(first_error.is_retryable());
    assert!(matches!(
        commit_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1,
        "the failed attempt must leave exactly one durable object from the epoch"
    );

    let first_replay_gate = Arc::new(Semaphore::new(0));
    let second_replay_gate = Arc::new(Semaphore::new(0));
    let replay_uploader = PersistentReplayUploader::new(
        Arc::clone(&objects),
        [
            ReplayUploadPlan::Gated(Arc::clone(&first_replay_gate)),
            ReplayUploadPlan::Gated(Arc::clone(&second_replay_gate)),
        ],
    );
    let (replay_cancellation, replay_task) =
        start_pipeline(Arc::clone(&replay_uploader), commit_tx);
    replay_uploader.wait_for_attempts(1).await;
    assert!(matches!(
        commit_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    first_replay_gate.add_permits(1);
    replay_uploader.wait_for_attempts(2).await;
    assert!(matches!(
        commit_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    second_replay_gate.add_permits(1);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), commit_rx.recv())
            .await
            .expect("replayed epoch was not committed after both objects became durable"),
        Some(77)
    );
    replay_cancellation.cancel();
    replay_task.await.unwrap().unwrap();

    let reference_objects = Arc::new(Mutex::new(BTreeMap::new()));
    let reference_uploader = PersistentReplayUploader::new(
        Arc::clone(&reference_objects),
        std::iter::empty::<ReplayUploadPlan>(),
    );
    let (reference_commit_tx, mut reference_commit_rx) = mpsc::unbounded_channel();
    let (reference_cancellation, reference_task) =
        start_pipeline(reference_uploader, reference_commit_tx);
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reference_commit_rx.recv()
        )
        .await
        .expect("uninterrupted reference epoch was not committed"),
        Some(77)
    );
    reference_cancellation.cancel();
    reference_task.await.unwrap().unwrap();

    let replayed = objects
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let uninterrupted = reference_objects
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(replayed.len(), 2);
    assert_eq!(replayed, uninterrupted);
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
                topic: Some(Arc::from("topic-a")),
                partition: Some(3),
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
        3,
        test_discovery(false),
        durable_storage(),
    )
    .unwrap();
    let task = tokio::spawn(transferia_pipeline::run_partition_pipeline(
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
            topic: Some(Arc::from("topic-a")),
            partition: Some(3),
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
        3,
        test_discovery(false),
        durable_storage(),
    )
    .unwrap();
    let task = tokio::spawn(transferia_pipeline::run_partition_pipeline(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pressure_stops_at_the_next_deterministic_epoch_rotation() {
    const GROUPS: i64 = 20;
    const MEMORY_LIMIT: usize = 512;
    const EPOCH_LIMIT: usize = 512;

    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(MEMORY_LIMIT);
    let config: S3SinkConfig = serde_yaml::from_str(&format!(
        "bucket: test\nrotation: {{ max_rows: 100, max_bytes: 1MiB }}\nbuffering: {{ max_epoch_buffers: 8, max_pending_upload_objects: 32, max_buffered_bytes: 8MiB, max_epoch_bytes: {EPOCH_LIMIT} }}\nupload: {{ multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }}\nretry: {{ initial_backoff: 1ms, max_backoff: 2ms }}\n"
    ))
    .unwrap();
    let (tx, mut events, cancel, task) = spawn(config, Arc::clone(&uploader), memory.clone());
    let offsets = (1..=GROUPS).collect::<Vec<_>>();
    let delivery = multi_message_delivery(&memory, 1, &offsets);
    tx.send(delivery).await.unwrap();
    drop(tx);

    uploader.wait_for_attempts(1).await;
    tokio::task::yield_now().await;
    assert!(memory.transform_used() > MEMORY_LIMIT);
    assert_eq!(
        uploader.attempts.load(Ordering::Acquire),
        1,
        "pressure may include input, routes, and older epochs, but must not cascade past the next deterministic epoch rotation"
    );

    uploader
        .gate
        .as_ref()
        .unwrap()
        .add_permits(usize::try_from(GROUPS).unwrap());
    assert_eq!(
        tokio::time::timeout(core::time::Duration::from_secs(5), events.recv())
            .await
            .expect("resumable delivery did not finish after uploads were released"),
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_group_epoch_barrier_blocks_the_next_delivery_until_durable() {
    let uploader = FakeUploader::blocked();
    let memory = PipelineMemory::new(128);
    let (tx, mut events, cancel, task) = spawn(
        config_with_rotation(1, "", "partitioning: { type: source }\n"),
        Arc::clone(&uploader),
        memory.clone(),
    );

    tx.send(multi_message_delivery(&memory, 1, &[1]))
        .await
        .unwrap();
    uploader.wait_for_attempts(1).await;
    tx.send(multi_message_delivery(&memory, 2, &[2]))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            core::time::Duration::from_millis(50),
            uploader.wait_for_attempts(2),
        )
        .await
        .is_err(),
        "the second upload started before the first epoch became durable"
    );
    assert_eq!(
        uploader.attempts.load(Ordering::Acquire),
        1,
        "dropping a completed PendingDelivery must not drop its epoch memory barrier"
    );

    uploader.gate.as_ref().unwrap().add_permits(1);
    uploader.wait_for_attempts(2).await;
    uploader.gate.as_ref().unwrap().add_permits(1);
    loop {
        let event = tokio::time::timeout(core::time::Duration::from_secs(5), events.recv())
            .await
            .expect("deliveries did not finish after both exact epoch barriers completed");
        if event == Some(SinkEvent::CommittedThrough(DeliveryId::new(2))) {
            break;
        }
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_epoch_completion_does_not_resume_a_pressured_delivery() {
    const GROUPS: usize = 4;
    const MEMORY_LIMIT: usize = 1 << 20;
    const HEADROOM_FOR_FIRST_GROUP: usize = 250;
    const EXTRA_PRESSURE: usize = 512;
    const ROUTE_RETAINED_BYTES: usize =
        128 + "events".len() + "topic-a".len() + "topic=topic-a/partition=3".len();

    let uploader = FakeUploader::controlled(GROUPS);
    let memory = PipelineMemory::new(MEMORY_LIMIT);
    let (tx, mut events, cancel, task) = spawn(
        config_with_rotation(1, "", "partitioning: { type: source }\n"),
        Arc::clone(&uploader),
        memory.clone(),
    );
    let offsets = (1..=i64::try_from(GROUPS).unwrap()).collect::<Vec<_>>();
    let delivery = multi_message_delivery(&memory, 1, &offsets);
    let accounted_before_routing = memory.transform_used();
    let route_bytes = GROUPS * ROUTE_RETAINED_BYTES;
    let filler_bytes = MEMORY_LIMIT
        .checked_sub(accounted_before_routing + route_bytes + HEADROOM_FOR_FIRST_GROUP)
        .expect("test delivery must fit below the memory limit before its first group");
    let _filler = memory.reserve_transform(filler_bytes);
    tx.send(delivery).await.unwrap();

    uploader.wait_for_started_uploads(2).await;
    let _extra_pressure = memory.reserve_transform(EXTRA_PRESSURE);
    uploader.release_key_suffix("+3+1.json");
    uploader.wait_for_uploads(1).await;
    tokio::task::yield_now().await;
    assert_eq!(
        uploader.attempts.load(Ordering::Acquire),
        2,
        "completing an unrelated epoch must not admit the third group"
    );

    uploader.release_key_suffix("+3+2.json");
    uploader.wait_for_started_uploads(3).await;
    uploader.release_key_suffix("+3+3.json");
    uploader.wait_for_started_uploads(4).await;
    uploader.release_key_suffix("+3+4.json");
    assert_eq!(
        tokio::time::timeout(core::time::Duration::from_secs(5), events.recv())
            .await
            .expect("delivery did not finish after its exact epoch barriers completed"),
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    cancel.cancel();
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
async fn partition_path_change_selects_rotate_or_keep_epoch_behavior() {
    let rotation = "  on_partition_path_change: rotate\n";
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

    let keep_epoch_uploader = FakeUploader::immediate(0);
    let keep_epoch_memory = PipelineMemory::new(1 << 20);
    let (tx, _events, cancel, task) = spawn(
        config_with_rotation(100, "", partitioning),
        Arc::clone(&keep_epoch_uploader),
        keep_epoch_memory.clone(),
    );
    tx.send(delivery(&keep_epoch_memory, 1, 1, 1_000, false).await)
        .await
        .unwrap();
    tx.send(delivery(&keep_epoch_memory, 2, 2, 1_001, false).await)
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(keep_epoch_uploader.attempts.load(Ordering::Acquire), 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_change_tracks_the_last_row_of_a_multirow_source_message() {
    let uploader = FakeUploader::immediate(0);
    let (commit_tx, mut commit_rx) = mpsc::unbounded_channel();
    let message = |value: &'static [u8], offset| Message {
        value: Bytes::from_static(value),
        meta: MessageMeta {
            topic: Some(Arc::from("topic-a")),
            partition: Some(3),
            offset: Some(offset),
            write_timestamp_ms: Some(1_234 + offset),
        },
    };
    let source = FakeSource {
        batches: VecDeque::from([
            vec![message(
                b"{\"id\":10,\"nullable\":null}\n{\"id\":20,\"nullable\":null}",
                1,
            )],
            vec![message(b"{\"id\":10,\"nullable\":null}", 2)],
        ]),
        commits: commit_tx,
    };
    let memory = PipelineMemory::new(1 << 20);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config_with_rotation(
            100,
            "  on_partition_path_change: rotate\n",
            "partitioning:\n  type: fields\n  columns: [id]\n",
        ),
        Arc::clone(&uploader) as Arc<dyn ObjectUploader>,
        Arc::new(SinkCounters::new()),
        false,
        3,
        test_discovery(false),
        durable_storage(),
    )
    .unwrap();
    let task = tokio::spawn(transferia_pipeline::run_partition_pipeline(
        Box::new(source),
        pipeline_parser(),
        Arc::new(Vec::new()),
        Box::new(sink),
        memory,
        cancellation.clone(),
        3,
        Arc::new(crate::metrics::ParseCounters::new()),
    ));

    let committed = tokio::time::timeout(core::time::Duration::from_secs(5), commit_rx.recv())
        .await
        .expect("B -> A must close the epoch that contains the atomic A,B source message");
    assert_eq!(committed, Some(1));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 2);
    cancellation.cancel();
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
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_buffered_bytes: 40, max_epoch_bytes: 40 }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_pending_upload_objects: 2, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4, max_in_flight_objects: 1 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
        "bucket: test\nrotation: { max_rows: 1, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_buffered_bytes: 8MiB, max_epoch_bytes: 8MiB }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms, max_attempts: 2 }\n",
    )
    .unwrap();
    let counters = Arc::new(SinkCounters::new());
    let (delivery_tx, delivery_rx) = mpsc::channel(8);
    let (event_tx, _events) = mpsc::channel(8);
    let cancellation = CancellationToken::new();
    let sink = S3Sink::new(
        config,
        Arc::clone(&uploader) as Arc<dyn ObjectUploader>,
        Arc::clone(&counters),
        false,
        3,
        test_discovery(false),
        durable_storage(),
    )
    .unwrap();
    let task = tokio::spawn(Box::new(sink).run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation,
    }));
    delivery_tx
        .send(delivery(&memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();

    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("exhausted 2 attempts"));
    assert!(error.is_retryable());
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 2);
    assert_eq!(counters.retries_total(), 1);
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
    let failure = &error;
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
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(error.to_string().contains("S3 delivery validation failed"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn dataset_mismatch_is_fatal_before_routing_or_upload() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    let mut invalid = delivery(&memory, 1, 4, 1_000, false).await;
    invalid.outputs[0].table = Arc::from("renamed_after_discovery");
    tx.send(invalid).await.unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(error
        .to_string()
        .contains("has no Main dataset named 'renamed_after_discovery'"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn schema_metadata_drift_is_fatal_before_upload() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    let mut invalid = delivery(&memory, 1, 4, 1_000, false).await;
    let schema = invalid.outputs[0].batch.schema();
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect::<Vec<_>>();
    fields[0] = fields[0]
        .clone()
        .with_metadata(std::collections::HashMap::from([(
            transferia_core::data::schema::META_LOW_CARDINALITY.to_owned(),
            "true".to_owned(),
        )]));
    invalid.outputs[0].batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        invalid.outputs[0].batch.columns().to_vec(),
    )
    .expect("metadata-only schema drift remains an Arrow-valid batch");
    tx.send(invalid).await.unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(error
        .to_string()
        .contains("metadata does not match discovery"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn overlong_static_prefix_fails_before_upload_and_is_non_retryable() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let mut sink_config = config("");
    sink_config.path_prefix = "x".repeat(super::object_key::MAX_OBJECT_KEY_BYTES);
    let (tx, _events, _cancel, task) = spawn(sink_config, Arc::clone(&uploader), memory.clone());
    tx.send(delivery(&memory, 1, 4, 1_000, false).await)
        .await
        .unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(error.to_string().contains("1024-byte limit"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn source_partition_mismatch_is_non_retryable_and_never_uploads() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let (tx, _events, _cancel, task) = spawn(config(""), Arc::clone(&uploader), memory.clone());
    let mut invalid = delivery(&memory, 1, 4, 1_000, false).await;
    let partition_index = invalid.outputs[0]
        .system_columns
        .get(SystemColumnKind::Partition)
        .expect("partition system column")
        .index;
    let mut columns = invalid.outputs[0].batch.columns().to_vec();
    columns[partition_index] = Arc::new(Int64Array::from(vec![4]));
    invalid.outputs[0].batch = RecordBatch::try_new(invalid.outputs[0].batch.schema(), columns)
        .expect("valid mismatched batch");
    tx.send(invalid).await.unwrap();

    let error = task.await.unwrap().unwrap_err();
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(error.to_string().contains("source partition mismatch"));
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
    let failure = &error;
    assert!(!failure.is_retryable());
    assert!(error.to_string().contains("delivery order violation"));
    assert_eq!(uploader.attempts.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn explicit_epoch_byte_limit_rotates_independently_of_pipeline_memory() {
    let uploader = FakeUploader::immediate(0);
    let memory = PipelineMemory::new(1 << 20);
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nrotation: { max_rows: 100, max_bytes: 1MiB }\nbuffering: { max_epoch_buffers: 8, max_pending_upload_objects: 8, max_buffered_bytes: 1MiB, max_epoch_bytes: 300 }\nupload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 4 }\nretry: { initial_backoff: 1ms, max_backoff: 2ms }\n",
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
