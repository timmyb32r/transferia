#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, BinaryArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_delivery_contracts::metrics::{MetricsRegistry, SinkCounters};
use transferia_provider_ytsaurus::ytsaurus::{YTsaurusSinkProvider, YTsaurusSourceProvider};
use transferia_registry::{
    SinkBuildContext, SinkPrepare, SinkProvider as _, SourceBuildContext, SourceDiscoveryContext,
    SourceProvider as _,
};

const IMAGE: &str = "ghcr.io/ytsaurus/local";
const TAG: &str = "stable@sha256:6f0991f7c85b4824bebead742fa4d752c3508532c013ffcb778a1b14c0b50b22";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

async fn yt_request(
    client: &reqwest::Client,
    endpoint: &str,
    method: reqwest::Method,
    command: &str,
    parameters: serde_json::Value,
) -> anyhow::Result<reqwest::Response> {
    Ok(client
        .request(method, format!("{endpoint}/api/v3/{command}"))
        .header("X-YT-Parameters", serde_json::to_string(&parameters)?)
        .send()
        .await?
        .error_for_status()?)
}

async fn create_table(client: &reqwest::Client, endpoint: &str, path: &str) -> anyhow::Result<()> {
    yt_request(
        client,
        endpoint,
        reqwest::Method::POST,
        "create",
        serde_json::json!({
            "type": "table",
            "path": path,
            "attributes": {
                "schema": [
                    { "name": "id", "type": "int64", "required": true },
                    { "name": "payload", "type": "string", "required": false }
                ]
            }
        }),
    )
    .await?;
    Ok(())
}

fn batch(ids: Vec<i64>, payloads: Vec<Option<&[u8]>>) -> anyhow::Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Binary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(BinaryArray::from(payloads)) as ArrayRef,
        ],
    )?)
}

fn discovery() -> Arc<DeliveryDiscovery> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("payload".into(), DataType::Binary, true),
    ]);
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("typed-e2e"),
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

async fn write_delivery(
    sink: Box<dyn transferia_core::sink::Sink>,
    memory: PipelineMemory,
    batches: Vec<RecordBatch>,
) -> anyhow::Result<()> {
    let (delivery_tx, delivery_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    let outputs = batches
        .into_iter()
        .map(|batch| {
            let bytes = batch.get_array_memory_size();
            SinkBatch {
                table: Arc::from("events"),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve_transform(bytes),
                system_columns: SystemColumns::default(),
            }
        })
        .collect();
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs,
            meta: DeliveryMeta { source_messages: 3 },
        })
        .await?;
    drop(delivery_tx);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), event_rx.recv())
            .await?
            .expect("YTsaurus sink event"),
        SinkEvent::CommittedThrough(DeliveryId::new(1))
    );
    task.await??;
    Ok(())
}

async fn read_arrow(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let request = client
        .get(format!("{endpoint}/api/v3/read_table"))
        .header(
            "X-YT-Parameters",
            serde_json::to_string(&serde_json::json!({"path": path}))?,
        )
        .header("X-YT-Output-Format", "\"arrow\"")
        .send()
        .await?
        .error_for_status()?;
    let mut bytes = arrow::buffer::Buffer::from(request.bytes().await?);
    let mut decoder = StreamDecoder::new();
    let mut batches = Vec::new();
    while !bytes.is_empty() {
        match decoder.decode(&mut bytes) {
            Ok(Some(batch)) => batches.push(batch),
            Ok(None) => {}
            Err(arrow::error::ArrowError::IpcError(message)) if message == "Unexpected EOS" => {
                decoder = StreamDecoder::new();
            }
            Err(error) => return Err(error.into()),
        }
    }
    decoder.finish()?;
    Ok(batches)
}

#[tokio::test]
async fn ytsaurus_source_and_both_sink_formats_use_the_real_http_api() -> anyhow::Result<()> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(80.tcp())
        .with_wait_for(WaitFor::message_on_either_std("Local YT started"))
        .with_platform("linux/amd64")
        .with_startup_timeout(Duration::from_mins(3))
        .with_cmd([
            "--fqdn",
            "localhost",
            "--port-range-start",
            "24400",
            "--node-port-set-size",
            "100",
            "--proxy-config",
            "{coordinator={public_fqdn=\"localhost:80\"};}",
            "--rpc-proxy-count",
            "0",
            "--rpc-proxy-port",
            "8002",
            "--node-count",
            "1",
            "--queue-agent-count",
            "0",
            "--address-resolver-config",
            "{enable_ipv4=%true;enable_ipv6=%false;}",
            "--native-client-supported",
            "--id",
            "locasaurus",
        ])
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(80.tcp()).await?;
    let endpoint = format!("http://{host}:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    create_table(&client, &endpoint, "//tmp/input").await?;

    let arrow_config: transferia_provider_ytsaurus::ytsaurus::YTsaurusSinkConfig =
        serde_yaml::from_str(&format!(
        "host: {host}\nport: {port}\ntrusted_plaintext: true\nreplace_tables: true\nformat: arrow\npath: //tmp/arrow_output\n"
    ))?;
    transferia_provider_ytsaurus::ytsaurus::check_connection(&arrow_config.connection).await?;
    let arrow_provider = YTsaurusSinkProvider::from_config(arrow_config)?;
    let discovered = discovery();
    arrow_provider.limits().validate_discovery(&discovered)?;
    arrow_provider
        .prepare(SinkPrepare::from_discovery(&discovered)?.expect("dataset"))
        .await?;
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let arrow_sink = arrow_provider
        .build_sink(SinkBuildContext {
            durable: transferia_test_support::durable_context(),
            partition_id: 0,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovered),
        })
        .await?;
    write_delivery(
        arrow_sink,
        memory,
        vec![batch(vec![1, 2], vec![Some(b"a"), None])?],
    )
    .await?;

    let yson_provider = YTsaurusSinkProvider::from_config(serde_yaml::from_str(&format!(
        "host: {host}\nport: {port}\ntrusted_plaintext: true\nreplace_tables: true\nformat: yson\npath: //tmp/yson_output\n"
    ))?)?;
    yson_provider.limits().validate_discovery(&discovered)?;
    yson_provider
        .prepare(SinkPrepare::from_discovery(&discovered)?.expect("dataset"))
        .await?;
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let yson_sink = yson_provider
        .build_sink(SinkBuildContext {
            durable: transferia_test_support::durable_context(),
            partition_id: 0,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovered),
        })
        .await?;
    write_delivery(yson_sink, memory, vec![batch(vec![3], vec![Some(b"b")])?]).await?;

    let arrow_rows = read_arrow(&client, &endpoint, "//tmp/arrow_output/events").await?;
    assert_eq!(
        arrow_rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );
    let yson_rows = read_arrow(&client, &endpoint, "//tmp/yson_output/events").await?;
    assert_eq!(
        yson_rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1
    );

    let input_schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("payload".into(), DataType::Binary, true),
    ]);
    let input_payload = {
        let input = batch(vec![9, 10, 11], vec![Some(b"x"), None, Some(b"z")])?;
        let mut bytes = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &input.schema())?;
            writer.write(&input)?;
            writer.finish()?;
        }
        bytes
    };
    client
        .put(format!("{endpoint}/api/v3/write_table"))
        .header(
            "X-YT-Parameters",
            serde_json::to_string(&serde_json::json!({"path":"<append=%true>//tmp/input"}))?,
        )
        .header("X-YT-Input-Format", "\"arrow\"")
        .body(input_payload)
        .send()
        .await?
        .error_for_status()?;
    let source = YTsaurusSourceProvider::from_config(
        serde_yaml::from_str(&format!(
            "host: {host}\nport: {port}\ntrusted_plaintext: true\nbatch_rows: 2\ntables:\n  - path: //tmp/input\n    output_name: events\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let source_discovery = source
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    assert_eq!(
        source_discovery.datasets[0].stored_schema.columns.len(),
        input_schema.columns.len()
    );
    let mut actor = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(16 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let mut rows = 0;
    loop {
        match actor.read_batch().await? {
            SourceBatch::Typed { tables, .. } => {
                assert!(tables[0].batch.num_rows() <= 2);
                rows += tables[0].batch.num_rows();
            }
            SourceBatch::Finished => break,
            SourceBatch::Raw { .. } => anyhow::bail!("YTsaurus source returned raw bytes"),
        }
    }
    assert_eq!(rows, 3);
    Ok(())
}
