#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::net::{Ipv4Addr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::{MetricsRegistry, SinkCounters};
use transferia_connector_kafka::kafka::{
    KafkaSinkConfig, KafkaSinkConnector, KafkaSourceConfig, KafkaSourceConnector,
};
use transferia_registry::{
    SinkBuildContext, SinkConnector as _, SourceBuildContext, SourceConnector as _,
};

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str = "v24.3.18";

#[tokio::test]
async fn kafka_sink_source_and_offset_commit_use_a_real_broker() -> anyhow::Result<()> {
    let host_port = unused_local_port()?;
    let container = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
        .with_startup_timeout(Duration::from_mins(3))
        .with_mapped_port(host_port, 9092.tcp())
        .with_cmd([
            "redpanda".to_owned(),
            "start".to_owned(),
            "--mode".to_owned(),
            "dev-container".to_owned(),
            "--smp".to_owned(),
            "1".to_owned(),
            "--memory".to_owned(),
            "1G".to_owned(),
            "--reserve-memory".to_owned(),
            "0M".to_owned(),
            "--kafka-addr".to_owned(),
            "0.0.0.0:9092".to_owned(),
            "--advertise-kafka-addr".to_owned(),
            format!("127.0.0.1:{host_port}"),
        ])
        .start()
        .await?;
    let broker = format!("{}:{host_port}", container.get_host().await?);
    let topic = "transferia-kafka-e2e";
    let discovery = discovery();
    let sink_config: KafkaSinkConfig = serde_yaml::from_str(&format!(
        "brokers: ['{broker}']\ntopic: {topic}\nsecurity: {{ type: plaintext }}\nserializer: {{ type: json }}\npartition: 0\nrequest_timeout_ms: 30000\nmax_in_flight: 4\n"
    ))?;
    let sink_connector = KafkaSinkConnector::from_config(sink_config)?;
    sink_connector.limits().validate_discovery(&discovery)?;
    let sink = sink_connector
        .build_sink(SinkBuildContext {
            partition_id: 0,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    send_delivery(sink).await?;

    let source_connector = KafkaSourceConnector::from_config(
        source_config(&broker, topic, "transferia-e2e-group")?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let mut source = source_connector.build_source(source_context()).await?;
    let batch = read_after_transient_broker_startup(&mut source).await?;
    let transferia_core::data::message::SourceBatch::Raw {
        messages,
        commit_marker,
        memory: _,
    } = batch
    else {
        panic!("Kafka source returned a non-raw batch")
    };
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].value.as_ref(), b"{\"id\":1,\"name\":\"one\"}\n");
    assert_eq!(messages[1].value.as_ref(), b"{\"id\":2,\"name\":\"two\"}\n");
    source
        .commit_offsets(&[commit_marker.expect("Kafka batch commit marker")])
        .await?;
    drop(source);

    let restarted = KafkaSourceConnector::from_config(
        source_config(&broker, topic, "transferia-e2e-group")?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let mut restarted = restarted.build_source(source_context()).await?;
    assert_no_committed_replay(&mut restarted).await?;
    Ok(())
}

async fn assert_no_committed_replay(
    source: &mut Box<dyn transferia_core::source::Source>,
) -> anyhow::Result<()> {
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match source.read_batch().await {
                Ok(_) => anyhow::bail!("Kafka replayed a committed batch"),
                Err(error) if error.is_retryable() => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error.into_source()),
            }
        }
    })
    .await;
    result.unwrap_or_else(|_| Ok(()))
}

async fn read_after_transient_broker_startup(
    source: &mut Box<dyn transferia_core::source::Source>,
) -> anyhow::Result<transferia_core::data::message::SourceBatch> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match source.read_batch().await {
                Ok(batch) => return Ok(batch),
                Err(error) if error.is_retryable() => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error.into_source()),
            }
        }
    })
    .await?
}

fn source_context() -> SourceBuildContext {
    SourceBuildContext {
        partition_id: 0,
        cancellation: CancellationToken::new(),
        memory: PipelineMemory::new(16 * 1024 * 1024),
        durable: transferia_test_support::durable_context(),
    }
}

fn unused_local_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    Ok(listener.local_addr()?.port())
}

fn discovery() -> Arc<DeliveryDiscovery> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false),
        SchemaColumn::new("name".to_owned(), DataType::Utf8, false),
    ]);
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("kafka-e2e"),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
    })
}

async fn send_delivery(sink: Box<dyn transferia_core::sink::Sink>) -> anyhow::Result<()> {
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let task = tokio::spawn(sink.run(SinkIo {
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
        tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    task.await??;
    Ok(())
}

fn source_config(
    broker: &str,
    topic: &str,
    consumer_group: &str,
) -> anyhow::Result<KafkaSourceConfig> {
    Ok(serde_yaml::from_str(&format!(
        "brokers: ['{broker}']\ntopics: ['{topic}']\nconsumer_group: '{consumer_group}'\nsecurity: {{ type: plaintext }}\noffset_reset: earliest\nparser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    conversion_error: fail\n    unknown_fields: {{ action: fail }}\n    json_framing: single_document\n    columns:\n      - {{ jsonpath: '$.id', column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }}\n      - {{ jsonpath: '$.name', column_name: name, json_data_type: string, arrow_type: Utf8, nullable: false }}\nbatch_max_messages: 1000\nbatch_max_bytes: 16777216\nrequest_timeout_ms: 30000\n"
    ))?)
}
