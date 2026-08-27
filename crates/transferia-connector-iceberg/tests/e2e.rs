#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;

use anyhow::Context as _;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_connector_iceberg::iceberg::{
    IcebergSinkConfig, IcebergSinkConnector, IcebergSourceConfig, IcebergSourceConnector,
};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::{MetricsRegistry, SinkCounters};
use transferia_registry::{
    SinkBuildContext, SinkConnector as _, SinkPrepare, SourceBuildContext, SourceConnector as _,
    SourceDiscoveryContext,
};

const LOCALSTACK_IMAGE: &str = "localstack/localstack";
const LOCALSTACK_TAG: &str = "4.14.0";
const REST_IMAGE: &str = "tabulario/iceberg-rest";
const REST_TAG: &str = "1.6.0";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".into()
    } else {
        host
    }
}

#[tokio::test]
async fn iceberg_sink_and_source_round_trip_through_rest_catalog_and_s3() -> anyhow::Result<()> {
    let s3 = GenericImage::new(LOCALSTACK_IMAGE, LOCALSTACK_TAG)
        .with_exposed_port(4566.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/_localstack/health")
                .with_port(4566.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("SERVICES", "s3")
        .start()
        .await?;
    let s3_host = reachable_host(&s3.get_host().await?);
    let s3_port = s3.get_host_port_ipv4(4566.tcp()).await?;
    let mut create_bucket = s3
        .exec(ExecCommand::new([
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "iceberg-warehouse",
            "--region",
            "us-east-1",
        ]))
        .await?;
    let stderr = String::from_utf8(create_bucket.stderr_to_vec().await?)?;
    anyhow::ensure!(
        create_bucket.exit_code().await? == Some(0),
        "LocalStack bucket creation failed: {stderr}"
    );

    let catalog_s3_endpoint = format!("http://host.docker.internal:{s3_port}");
    let catalog = GenericImage::new(REST_IMAGE, REST_TAG)
        .with_exposed_port(8181.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/v1/config")
                .with_port(8181.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("AWS_ACCESS_KEY_ID", "test")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "test")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("CATALOG_WAREHOUSE", "s3://iceberg-warehouse/")
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", catalog_s3_endpoint)
        .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .start()
        .await?;
    let catalog_host = reachable_host(&catalog.get_host().await?);
    let catalog_port = catalog.get_host_port_ipv4(8181.tcp()).await?;
    let catalog_uri = format!("http://{catalog_host}:{catalog_port}");
    reqwest::Client::new()
        .post(format!("{catalog_uri}/v1/namespaces"))
        .json(&serde_json::json!({ "namespace": ["default"], "properties": {} }))
        .send()
        .await?
        .error_for_status()?;
    let storage_yaml = format!(
        "type: s3\nbucket: iceberg-warehouse\nregion: us-east-1\nendpoint: http://{s3_host}:{s3_port}\ncredentials:\n  access_key: test\n  secret_key: test\npath_style_access: true\n"
    );
    let sink_config: IcebergSinkConfig = serde_yaml::from_str(&format!(
        "catalog:\n  uri: {catalog_uri}\n  auth: {{ type: none }}\nstorage:\n{}namespace: default\ncreate_if_missing: true\ntarget_file_size_bytes: 1048576\n",
        indent(&storage_yaml, 2)
    ))?;
    let connector = IcebergSinkConnector::from_config(sink_config)?;
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("value".to_owned(), DataType::Utf8, false),
    ]);
    let discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("events"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    });
    connector.limits().validate_discovery(&discovery)?;
    connector
        .prepare(SinkPrepare::from_discovery(&discovery)?.expect("row discovery"))
        .await
        .context("prepare Iceberg table through REST catalog")?;
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            durable: transferia_test_support::durable_context(),
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
        })
        .await?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(
                std::iter::once(("transferia.primary_key".to_owned(), "true".to_owned())).collect(),
            ),
            Field::new("value", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let sink_task = tokio::spawn(sink.run(SinkIo {
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
    let event = event_rx.recv().await;
    let sink_result = sink_task.await?;
    sink_result.context("write and commit Iceberg delivery")?;
    assert_eq!(event, Some(SinkEvent::CommittedThrough(DeliveryId::new(1))));

    let source_config: IcebergSourceConfig = serde_yaml::from_str(&format!(
        "catalog:\n  uri: {catalog_uri}\n  auth: {{ type: none }}\nstorage:\n{}namespace: default\ntable_names: [events]\n",
        indent(&storage_yaml, 2)
    ))?;
    let source_connector =
        IcebergSourceConnector::from_config(source_config, Arc::new(MetricsRegistry::new()))?;
    source_connector
        .delivery_discovery(SourceDiscoveryContext {
            request: transferia_core::delivery::DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    let mut source = source_connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(16 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let SourceBatch::Typed { tables, .. } = source
        .read_batch()
        .await
        .context("read committed Iceberg snapshot")?
    else {
        anyhow::bail!("Iceberg source did not return the committed snapshot")
    };
    assert_eq!(tables.len(), 1);
    let ids = tables[0]
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(ids.values().as_ref(), [1, 2]);
    assert!(matches!(source.read_batch().await?, SourceBatch::Finished));
    Ok(())
}

fn indent(value: &str, spaces: usize) -> String {
    let prefix = " ".repeat(spaces);
    value.lines().fold(String::new(), |mut output, line| {
        output.push_str(&prefix);
        output.push_str(line);
        output.push('\n');
        output
    })
}
