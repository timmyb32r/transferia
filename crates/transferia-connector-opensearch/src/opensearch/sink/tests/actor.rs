use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{
    Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo,
};
use transferia_delivery_contracts::metrics::SinkCounters;

use super::super::actor::OpenSearchSink;
use super::super::bulk::{BulkFailure, BulkTransport};
use super::super::document::encode_batch;
use super::super::{initial_config, OpenSearchSinkConfig, RoutedIdentity};

struct FakeTransport {
    payloads: Mutex<Vec<Vec<u8>>>,

    responses: Mutex<VecDeque<Vec<u16>>>,
}

struct BlockingTransport {
    started: Notify,

    release: Notify,
}

impl BulkTransport for BlockingTransport {
    fn send(&self, _payload: Vec<u8>) -> BoxFuture<'_, Result<Vec<u16>, BulkFailure>> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            Ok(vec![201])
        })
    }
}

impl BulkTransport for FakeTransport {
    fn send(&self, payload: Vec<u8>) -> BoxFuture<'_, Result<Vec<u16>, BulkFailure>> {
        Box::pin(async move {
            self.payloads.lock().unwrap().push(payload);
            Ok(self.responses.lock().unwrap().pop_front().unwrap())
        })
    }
}

fn config() -> Arc<OpenSearchSinkConfig> {
    let mut value = initial_config();
    value["hosts"] = serde_json::json!(["example.test"]);
    value["trusted_plaintext"] = true.into();
    value["bulk_target_rows"] = 1.into();
    Arc::new(serde_json::from_value(value).unwrap())
}

fn discovery() -> Arc<DeliveryDiscovery> {
    let id = SchemaColumn::new("id".to_owned(), DataType::Int64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, false);
    let topic = SchemaColumn::new(
        SystemColumnKind::Topic.default_name().to_owned(),
        SystemColumnKind::Topic.data_type(),
        false,
    );
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("logs"),
            incoming_schema: DatasetSchema::new(vec![id.clone(), payload.clone(), topic]),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: vec![SystemColumnKind::Topic.into()],
        }],
        performance_advice: Vec::new(),
    })
}

fn sink_batch(memory: &PipelineMemory, ids: Vec<i64>) -> SinkBatch {
    let rows = ids.len();
    sink_batch_with_payloads(memory, ids, vec!["value"; rows])
}

fn sink_batch_with_payloads(
    memory: &PipelineMemory,
    ids: Vec<i64>,
    payloads: Vec<&str>,
) -> SinkBatch {
    let rows = ids.len();
    let id = SchemaColumn::new("id".to_owned(), DataType::Int64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, false);
    let topic_name = SystemColumnKind::Topic.default_name();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(id.arrow_metadata()),
            Field::new("payload", DataType::Utf8, false).with_metadata(payload.arrow_metadata()),
            Field::new(topic_name, DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
            Arc::new(StringArray::from(vec!["secret-topic"; rows])) as ArrayRef,
        ],
    )
    .unwrap();
    let bytes = batch.get_array_memory_size();
    SinkBatch {
        table: Arc::from("logs"),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: memory.reserve_transform(bytes),
        system_columns: SystemColumns::new(vec![SystemColumn {
            kind: SystemColumnKind::Topic,
            name: Arc::from(topic_name),
            index: 2,
        }]),
    }
}

#[tokio::test]
async fn duplicate_identity_across_buffered_deliveries_fails_before_request() {
    let transport = Arc::new(FakeTransport {
        payloads: Mutex::new(Vec::new()),
        responses: Mutex::new(VecDeque::new()),
    });
    let memory = PipelineMemory::new(1024 * 1024);
    let mut settings = (*config()).clone();
    settings.bulk_target_rows = 10;
    settings.bulk_concurrency = 1;
    settings.flush_interval_ms = 1_000;
    settings.retry_initial_ms = 1;
    settings.retry_max_ms = 1;
    let sink = OpenSearchSink::new(
        Arc::new(settings),
        transport.clone(),
        Arc::new(SinkCounters::new()),
        discovery(),
        0,
    );
    let (delivery_tx, delivery_rx) = mpsc::channel(2);
    let (event_tx, _event_rx) = mpsc::channel(2);
    let task = tokio::spawn(Box::new(sink).run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    for (delivery_id, payload) in [(1, "first"), (2, "second")] {
        delivery_tx
            .send(Delivery {
                id: DeliveryId::new(delivery_id),
                outputs: vec![sink_batch_with_payloads(&memory, vec![7], vec![payload])],
                meta: DeliveryMeta::default(),
            })
            .await
            .unwrap();
    }
    drop(delivery_tx);
    let failure = task.await.unwrap().unwrap_err();
    assert!(!failure.is_retryable());
    assert!(failure.to_string().contains("globally unique"));
    assert!(transport.payloads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn encoded_ndjson_is_accounted_until_bulk_completion() {
    let transport = Arc::new(BlockingTransport {
        started: Notify::new(),
        release: Notify::new(),
    });
    let memory = PipelineMemory::new(128);
    let discovered = discovery();
    let output =
        sink_batch_with_payloads(&memory, vec![1], vec![r#""\\"\\"\\"\\"\\"\\"\\"\\"\\"\\"#]);
    let input_bytes = output.memory.bytes();
    let encoded_bytes = encode_batch(
        "logs",
        &discovered.datasets[0].stored_schema,
        &output.batch,
        RoutedIdentity::Fail,
    )
    .unwrap()
    .iter()
    .map(|action| action.bytes)
    .sum::<usize>();
    assert!(
        input_bytes.saturating_add(encoded_bytes) > memory.limit(),
        "fixture must prove admission can cross a full input lease without deadlock"
    );
    let sink = OpenSearchSink::new(
        config(),
        transport.clone(),
        Arc::new(SinkCounters::new()),
        discovered,
        0,
    );
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, _event_rx) = mpsc::channel(1);
    let task = tokio::spawn(Box::new(sink).run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![output],
            meta: DeliveryMeta::default(),
        })
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        transport.started.notified(),
    )
    .await
    .expect("bulk write did not start");
    assert_eq!(
        memory.transform_used(),
        input_bytes + encoded_bytes.saturating_mul(2)
    );

    transport.release.notify_one();
    drop(delivery_tx);
    task.await.unwrap().unwrap();
    assert_eq!(memory.transform_used(), 0);
}

#[tokio::test]
async fn actor_projects_system_columns_and_commits_only_after_every_item_succeeds() {
    let transport = Arc::new(FakeTransport {
        payloads: Mutex::new(Vec::new()),
        responses: Mutex::new(VecDeque::from([vec![201], vec![201]])),
    });
    let memory = PipelineMemory::new(1024 * 1024);
    let sink = OpenSearchSink::new(
        config(),
        transport.clone(),
        Arc::new(SinkCounters::new()),
        discovery(),
        0,
    );
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let task = tokio::spawn(Box::new(sink).run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![sink_batch(&memory, vec![1, 2])],
            meta: DeliveryMeta { source_messages: 2 },
        })
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), event_rx.recv())
            .await
            .unwrap(),
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    drop(delivery_tx);
    task.await.unwrap().unwrap();
    let payloads = transport.payloads.lock().unwrap();
    let combined = payloads
        .iter()
        .flat_map(|payload| payload.iter().copied())
        .collect::<Vec<_>>();
    drop(payloads);
    let text = std::str::from_utf8(&combined).unwrap();
    assert!(!text.contains("secret-topic"));
    assert!(!text.contains(SystemColumnKind::Topic.default_name()));
    assert!(text.contains("\"payload\":\"value\""));
}

#[tokio::test]
async fn duplicate_primary_key_in_one_delivery_fails_closed_before_request() {
    let transport = Arc::new(FakeTransport {
        payloads: Mutex::new(Vec::new()),
        responses: Mutex::new(VecDeque::new()),
    });
    let memory = PipelineMemory::new(1024 * 1024);
    let sink = OpenSearchSink::new(
        config(),
        transport.clone(),
        Arc::new(SinkCounters::new()),
        discovery(),
        0,
    );
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, _event_rx) = mpsc::channel(1);
    let task = tokio::spawn(Box::new(sink).run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![sink_batch(&memory, vec![1, 1])],
            meta: DeliveryMeta::default(),
        })
        .await
        .unwrap();
    drop(delivery_tx);
    let failure = task.await.unwrap().unwrap_err();
    assert!(!failure.is_retryable());
    assert!(transport.payloads.lock().unwrap().is_empty());
}
