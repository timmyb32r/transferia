#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use testcontainers::core::{Healthcheck, IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use transferia_connector_ydb::metrics::SinkCounters;
use transferia_connector_ydb::ydb::{
    self, YdbAuth, YdbConnectionConfig, YdbSinkConfig, YdbSinkConnector, YdbSourceConfig,
    YdbSourceConnector, YdbTableConfig,
};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
    SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_registry::{
    SinkBuildContext, SinkConnector as _, SinkPrepare, SourceBuildContext, SourceConnector as _,
    SourceDiscoveryContext,
};

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

async fn wait_for_ydb(config: &YdbConnectionConfig) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            match ydb::check_connection(config).await {
                Ok(()) => return Ok(()),
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("YDB testcontainer did not become ready"))?
}

fn discovery() -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false),
    ]);
    DeliveryDiscovery {
        source_name: Arc::from("ydb-sink-e2e"),
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
    }
}

fn changelog_discovery() -> DeliveryDiscovery {
    let id = SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, false);
    DeliveryDiscovery {
        source_name: Arc::from("postgres-cdc-e2e"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(vec![
                id.clone(),
                SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
                SchemaColumn::new(
                    SystemColumnKind::ChangeOperation.default_name().into(),
                    SystemColumnKind::ChangeOperation.data_type(),
                    false,
                ),
                SchemaColumn::new(
                    SystemColumnKind::Offset.default_name().into(),
                    SystemColumnKind::Offset.data_type(),
                    false,
                ),
                SchemaColumn::new(
                    SystemColumnKind::ChangedColumns.default_name().into(),
                    SystemColumnKind::ChangedColumns.data_type(),
                    false,
                ),
            ]),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: vec![
                SystemColumnKind::ChangeOperation.into(),
                SystemColumnKind::Offset.into(),
                SystemColumnKind::ChangedColumns.into(),
            ],
        }],
        performance_advice: Vec::new(),
    }
}

fn sink_batch(
    memory: &PipelineMemory,
    ids: Vec<u64>,
    payloads: Vec<&str>,
) -> anyhow::Result<SinkBatch> {
    let id = SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false).with_metadata(id.arrow_metadata()),
            Field::new("payload", DataType::Utf8, false).with_metadata(payload.arrow_metadata()),
        ])),
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: memory.reserve_transform(bytes),
        system_columns: SystemColumns::default(),
    })
}

fn changelog_sink_batch(
    memory: &PipelineMemory,
    operations: Vec<&str>,
    ids: Vec<u64>,
    payloads: Vec<Option<&str>>,
    lsn: i64,
) -> anyhow::Result<SinkBatch> {
    let discovery = changelog_discovery();
    let fields = discovery.datasets[0]
        .incoming_schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let rows = ids.len();
    let changed = operations
        .iter()
        .zip(&payloads)
        .map(|(operation, payload)| {
            if *operation == "d" || (*operation == "u" && payload.is_none()) {
                &[0b01_u8][..]
            } else {
                &[0b11_u8][..]
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
            Arc::new(StringArray::from(operations)) as ArrayRef,
            Arc::new(Int64Array::from(vec![lsn; rows])) as ArrayRef,
            Arc::new(arrow::array::BinaryArray::from_iter_values(changed)) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: memory.reserve_transform(bytes),
        system_columns: SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
                index: 2,
            },
            SystemColumn {
                kind: SystemColumnKind::Offset,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
                index: 3,
            },
            SystemColumn {
                kind: SystemColumnKind::ChangedColumns,
                name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
                index: 4,
            },
        ]),
    })
}

#[tokio::test]
async fn ydb_sink_bulk_upserts_arrow_and_replay_replaces_the_same_key() -> anyhow::Result<()> {
    let container = GenericImage::new("ydbplatform/local-ydb", "25.4.1")
        .with_exposed_port(2136.tcp())
        .with_wait_for(WaitFor::healthcheck())
        .with_env_var("YDB_USE_IN_MEMORY_PDISKS", "true")
        .with_env_var("GRPC_PORT", "2136")
        .with_health_check(
            Healthcheck::cmd(["/health_check"])
                .with_interval(Duration::from_secs(1))
                .with_timeout(Duration::from_secs(3))
                .with_retries(90),
        )
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(2136.tcp()).await?;
    let connection = YdbConnectionConfig {
        endpoint: format!("grpc://{host}:{port}"),
        database: "/local".to_owned(),
        trusted_plaintext: true,
        auth: YdbAuth::Anonymous,
        request_timeout_ms: 30_000,
    };
    wait_for_ydb(&connection).await?;

    let table = YdbTableConfig {
        path: "/local/events".to_owned(),
    };
    let sink = YdbSinkConnector::from_config(YdbSinkConfig {
        connection: connection.clone(),
        tables: vec![table.clone()],
        create_tables: true,
        retry_max_ms: 30_000,
    })?;
    let discovery = Arc::new(discovery());
    sink.limits().validate_discovery(&discovery)?;
    sink.prepare(SinkPrepare::from_discovery(&discovery, true)?.expect("datasets"))
        .await?;

    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let built = sink
        .build_sink(SinkBuildContext {
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(2);
    let task = tokio::spawn(built.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![sink_batch(&memory, vec![1, 2], vec!["one", "two"])?],
            meta: DeliveryMeta { source_messages: 2 },
        })
        .await?;
    let first = tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?;
    if first != Some(SinkEvent::CommittedThrough(DeliveryId::new(1))) {
        drop(delivery_tx);
        return match task.await? {
            Ok(()) => anyhow::bail!("YDB sink exited without committing delivery 1"),
            Err(error) => Err(error.into()),
        };
    }
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(2),
            outputs: vec![sink_batch(&memory, vec![1], vec!["one-replayed"])?],
            meta: DeliveryMeta { source_messages: 1 },
        })
        .await?;
    let second = tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?;
    if second != Some(SinkEvent::CommittedThrough(DeliveryId::new(2))) {
        drop(delivery_tx);
        return match task.await? {
            Ok(()) => anyhow::bail!("YDB sink exited without committing delivery 2"),
            Err(error) => Err(error.into()),
        };
    }
    drop(delivery_tx);
    task.await??;

    let changelog_discovery = Arc::new(changelog_discovery());
    sink.limits().validate_discovery(&changelog_discovery)?;
    sink.prepare(
        SinkPrepare::from_discovery(&changelog_discovery, false)?.expect("changelog dataset"),
    )
    .await?;
    let changelog_sink = sink
        .build_sink(SinkBuildContext {
            partition_id: 0,
            finite_source: false,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: changelog_discovery,
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(2);
    let task = tokio::spawn(changelog_sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    for (delivery_id, operations, ids, payloads, lsn) in [
        (
            3,
            vec!["u", "d", "c"],
            vec![1, 2, 3],
            vec![Some("one-current"), None, Some("three")],
            42,
        ),
        (4, vec!["u"], vec![3], vec![None], 43),
    ] {
        delivery_tx
            .send(Delivery {
                id: DeliveryId::new(delivery_id),
                outputs: vec![changelog_sink_batch(
                    &memory, operations, ids, payloads, lsn,
                )?],
                meta: DeliveryMeta { source_messages: 3 },
            })
            .await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(delivery_id)))
        );
    }
    drop(delivery_tx);
    task.await??;

    let source = YdbSourceConnector::from_config(
        YdbSourceConfig {
            connection,
            tables: vec![table],
            batch_rows: 1024,
        },
        Arc::new(transferia_connector_ydb::metrics::MetricsRegistry::new()),
    )?;
    let source_discovery = source
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    assert_eq!(source_discovery.datasets[0].stored_schema.columns.len(), 2);
    let mut source = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(16 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let SourceBatch::Typed { tables, .. } = source.read_batch().await? else {
        anyhow::bail!("YDB source returned no typed rows after BulkUpsert")
    };
    let batch = &tables[0].batch;
    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let payloads = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = (0..batch.num_rows())
        .map(|row| (ids.value(row), payloads.value(row).to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.get(&1).map(String::as_str), Some("one-current"));
    assert_eq!(values.get(&2), None);
    assert_eq!(values.get(&3).map(String::as_str), Some("three"));
    assert!(matches!(source.read_batch().await?, SourceBatch::Finished));
    Ok(())
}
