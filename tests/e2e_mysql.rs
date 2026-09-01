#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mysql_async::prelude::Queryable as _;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use transferia::connectors::mysql::{
    MySqlConnectionConfig, MySqlSinkConnector, MySqlSourceConnector,
};
use transferia::core::data::message::SourceBatch;
use transferia::core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia::core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia::core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
    SourceTopology,
};
use transferia::core::memory::PipelineMemory;
use transferia::core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia::metrics::{MetricsRegistry, SinkCounters};
use transferia::registry::{
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

async fn wait_for_mysql(config: &MySqlConnectionConfig) -> anyhow::Result<mysql_async::Conn> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match transferia::connectors::mysql::connect(config).await {
                Ok(connection) => return connection,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("MySQL-compatible testcontainer did not become ready"))
}

async fn assert_mysql_family_source(image: GenericImage, password_env: &str) -> anyhow::Result<()> {
    let container = image
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var(password_env, "test")
        .with_env_var("MYSQL_DATABASE", "transferia")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(3306.tcp()).await?;
    let connection_config = MySqlConnectionConfig {
        host: host.clone(),
        port,
        database: "transferia".to_owned(),
        username: "root".to_owned(),
        password: "test".to_owned(),
        trusted_plaintext: true,
        tls_ca_file: None,
    };
    let mut connection = wait_for_mysql(&connection_config).await?;
    connection
        .query_drop(
            "CREATE TABLE all_types (\
                id BIGINT UNSIGNED PRIMARY KEY,\
                signed_tiny TINYINT, unsigned_tiny TINYINT UNSIGNED,\
                signed_small SMALLINT, unsigned_small SMALLINT UNSIGNED,\
                signed_medium MEDIUMINT, unsigned_medium MEDIUMINT UNSIGNED,\
                signed_int INT, unsigned_int INT UNSIGNED,\
                signed_big BIGINT, unsigned_big BIGINT UNSIGNED,\
                float_value FLOAT, double_value DOUBLE, decimal_value DECIMAL(65, 30),\
                bit_value BIT(64), binary_value VARBINARY(16), blob_value LONGBLOB,\
                char_value CHAR(8), varchar_value VARCHAR(255), text_value LONGTEXT,\
                enum_value ENUM('one', 'two'), set_value SET('red', 'green'), json_value JSON,\
                date_value DATE, time_value TIME(6), datetime_value DATETIME(6),\
                timestamp_value TIMESTAMP(6), year_value YEAR, point_value POINT\
            )",
        )
        .await?;
    connection
        .query_drop(
            "INSERT INTO all_types VALUES (\
                18446744073709551615, -128, 255, -32768, 65535, -8388608, 16777215,\
                -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,\
                1.5, 2.25, 99999999999999999999999999999999999.123456789012345678901234567890,\
                X'FEDCBA9876543210', X'00FF', X'000102FF', 'chars', 'utf8 text',\
                'long text', 'two', 'red,green', JSON_OBJECT('answer', 42),\
                '2024-02-29', '838:59:58.123456', '2024-02-29 12:34:56.123456',\
                '2024-02-29 12:34:56.123456', 2024, ST_GeomFromText('POINT(1 2)')\
            ), (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,\
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,\
                NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .await?;
    connection.disconnect().await?;

    let source = MySqlSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: root\npassword: test\ntrusted_plaintext: true\nbatch_rows: 1\ntables:\n  - name: all_types\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = source
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    assert!(matches!(
        discovery.source_topology,
        SourceTopology::StaticPartitions(ref partitions) if partitions == &[0]
    ));
    let dataset = &discovery.datasets[0];
    let id = dataset
        .stored_schema
        .columns
        .iter()
        .find(|column| column.name == "id")
        .unwrap();
    assert_eq!(id.data_type, DataType::UInt64);
    assert!(id.primary_key);
    assert_eq!(
        dataset
            .stored_schema
            .columns
            .iter()
            .find(|column| column.name == "decimal_value")
            .unwrap()
            .data_type,
        DataType::Utf8
    );
    assert_eq!(
        dataset
            .stored_schema
            .columns
            .iter()
            .find(|column| column.name == "point_value")
            .unwrap()
            .data_type,
        DataType::Binary
    );

    let mut actor = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(64 * 1024 * 1024),
            durable: support::durable_context(),
        })
        .await?;
    let mut batches = Vec::new();
    loop {
        match actor.read_batch().await? {
            SourceBatch::Typed { tables, .. } => batches.push(tables[0].batch.clone()),
            SourceBatch::Finished => break,
            other => anyhow::bail!("unexpected MySQL source batch: {other:?}"),
        }
    }
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    let populated = batches
        .iter()
        .find(|batch| {
            batch
                .column_by_name("id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .is_some_and(|column| column.value(0) == u64::MAX)
        })
        .unwrap();
    assert_eq!(
        populated
            .column_by_name("decimal_value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "99999999999999999999999999999999999.123456789012345678901234567890"
    );
    assert_eq!(
        populated
            .column_by_name("time_value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "838:59:58.123456"
    );
    assert_eq!(
        populated
            .column_by_name("binary_value")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 255]
    );
    let nullable = batches
        .iter()
        .find(|batch| {
            batch
                .column_by_name("id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .is_some_and(|column| column.value(0) == 1)
        })
        .unwrap();
    assert!(nullable.column_by_name("signed_tiny").unwrap().is_null(0));
    Ok(())
}

#[tokio::test]
async fn mysql_source_reads_all_type_families_losslessly() -> anyhow::Result<()> {
    assert_mysql_family_source(GenericImage::new("mysql", "8.4.6"), "MYSQL_ROOT_PASSWORD").await
}

#[tokio::test]
async fn mariadb_source_reads_all_type_families_losslessly() -> anyhow::Result<()> {
    assert_mysql_family_source(
        GenericImage::new("mariadb", "11.8.3"),
        "MARIADB_ROOT_PASSWORD",
    )
    .await
}

fn sink_dataset(name: &str, json_payload: bool) -> DiscoveredDataset {
    let payload = if json_payload {
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME)
    } else {
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false)
    };
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        payload,
    ]);
    DiscoveredDataset {
        role: DatasetRole::Main,
        name: Arc::from(name),
        incoming_schema: schema.clone(),
        stored_schema: schema,
        system_columns: Vec::new(),
    }
}

fn sink_batch(
    memory: &PipelineMemory,
    table: &'static str,
    ids: Vec<u64>,
    payloads: Vec<&str>,
    json_payload: bool,
) -> anyhow::Result<SinkBatch> {
    let payload_column = if json_payload {
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME)
    } else {
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false)
    };
    let id_column = SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
        .with_constraints(true, false, None);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false).with_metadata(id_column.arrow_metadata()),
        Field::new("payload", DataType::Utf8, false).with_metadata(payload_column.arrow_metadata()),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(SinkBatch {
        table: Arc::from(table),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: memory.reserve_transform(bytes),
        system_columns: SystemColumns::default(),
    })
}

fn mysql_changelog_discovery() -> DeliveryDiscovery {
    let id = SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, false);
    DeliveryDiscovery {
        source_name: Arc::from("postgres-cdc"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("cdc_rows"),
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
            ]),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: vec![
                SystemColumnKind::ChangeOperation.into(),
                SystemColumnKind::Offset.into(),
            ],
        }],
        performance_advice: Vec::new(),
    }
}

fn mysql_changelog_delivery(
    memory: &PipelineMemory,
    delivery_id: u64,
    operations: Vec<&str>,
    ids: Vec<u64>,
    payloads: Vec<Option<&str>>,
    lsn: i64,
) -> anyhow::Result<Delivery> {
    let discovery = mysql_changelog_discovery();
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
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
            Arc::new(StringArray::from(operations)) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(vec![lsn; rows])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    Ok(Delivery {
        id: DeliveryId::new(delivery_id),
        outputs: vec![SinkBatch {
            table: Arc::from("cdc_rows"),
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
            ]),
        }],
        meta: DeliveryMeta {
            source_messages: rows as u64,
        },
    })
}

#[tokio::test]
async fn mysql_sink_commits_atomically_and_rolls_back_failed_delivery() -> anyhow::Result<()> {
    let container = GenericImage::new("mysql", "8.4.6")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "test")
        .with_env_var("MYSQL_DATABASE", "transferia")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(3306.tcp()).await?;
    let connection_config = MySqlConnectionConfig {
        host: host.clone(),
        port,
        database: "transferia".to_owned(),
        username: "root".to_owned(),
        password: "test".to_owned(),
        trusted_plaintext: true,
        tls_ca_file: None,
    };
    wait_for_mysql(&connection_config)
        .await?
        .disconnect()
        .await?;
    let discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("mysql-sink-e2e"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            sink_dataset("committed_rows", false),
            sink_dataset("json_rows", true),
        ],
        performance_advice: Vec::new(),
    });
    let sink = MySqlSinkConnector::from_config(serde_yaml::from_str(&format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: root\npassword: test\ntrusted_plaintext: true\ncreate_tables: true\ninsert_rows: 2\n"
    ))?)?;
    let mut wrong_key_connection = wait_for_mysql(&connection_config).await?;
    wrong_key_connection
        .query_drop("CREATE TABLE cdc_wrong_key (id BIGINT UNSIGNED NOT NULL, payload TEXT NOT NULL) ENGINE=InnoDB")
        .await?;
    wrong_key_connection.disconnect().await?;
    let mut wrong_key = mysql_changelog_discovery();
    wrong_key.datasets[0].name = Arc::from("cdc_wrong_key");
    let error = sink
        .prepare(SinkPrepare::from_discovery(&wrong_key, false)?.expect("wrong-key dataset"))
        .await
        .expect_err("an existing changelog table without the declared key must fail at startup");
    assert!(error.to_string().contains("has primary key []"), "{error:#}");
    sink.limits().validate_discovery(&discovery)?;
    sink.prepare(SinkPrepare::from_discovery(&discovery, true)?.expect("datasets"))
        .await
        .context("prepare MySQL sink tables")?;

    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let built = sink
        .build_sink(SinkBuildContext {
            partition_id: 0,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery: Arc::clone(&discovery),
            durable: support::durable_context(),
        })
        .await
        .context("build MySQL sink")?;
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
            outputs: vec![sink_batch(
                &memory,
                "committed_rows",
                vec![1, 2, 3],
                vec!["one", "two", "three"],
                false,
            )?],
            meta: DeliveryMeta { source_messages: 3 },
        })
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
        Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
    );
    delivery_tx
        .send(Delivery {
            id: DeliveryId::new(2),
            outputs: vec![
                sink_batch(
                    &memory,
                    "committed_rows",
                    vec![4],
                    vec!["must roll back"],
                    false,
                )?,
                sink_batch(&memory, "json_rows", vec![1], vec!["not json"], true)?,
            ],
            meta: DeliveryMeta { source_messages: 2 },
        })
        .await?;
    drop(delivery_tx);
    assert!(task.await?.is_err());
    assert_eq!(
        event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    );

    let mut connection = wait_for_mysql(&connection_config).await?;
    let rows: Vec<(u64, String)> = connection
        .query("SELECT id, payload FROM committed_rows ORDER BY id")
        .await
        .context("query committed MySQL rows")?;
    assert_eq!(
        rows,
        vec![
            (1, "one".to_owned()),
            (2, "two".to_owned()),
            (3, "three".to_owned()),
        ]
    );
    let json_rows: Option<u64> = connection
        .query_first("SELECT COUNT(*) FROM json_rows")
        .await
        .context("query rolled-back MySQL rows")?;
    assert_eq!(json_rows, Some(0));

    let changelog_discovery = Arc::new(mysql_changelog_discovery());
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
            durable: support::durable_context(),
        })
        .await?;
    let (delivery_tx, delivery_rx) = mpsc::channel(3);
    let (event_tx, mut event_rx) = mpsc::channel(3);
    let task = tokio::spawn(changelog_sink.run(SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    for delivery in [
        mysql_changelog_delivery(
            &memory,
            1,
            vec!["c", "u", "c", "d"],
            vec![1, 1, 2, 2],
            vec![Some("old"), Some("current"), Some("deleted"), None],
            42,
        )?,
        mysql_changelog_delivery(
            &memory,
            2,
            vec!["c", "u", "c", "d"],
            vec![1, 1, 2, 2],
            vec![Some("old"), Some("current"), Some("deleted"), None],
            42,
        )?,
        mysql_changelog_delivery(
            &memory,
            3,
            vec!["d", "c"],
            vec![1, 3],
            vec![None, Some("three")],
            43,
        )?,
    ] {
        let id = delivery.id;
        delivery_tx.send(delivery).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await?,
            Some(SinkEvent::CommittedThrough(id))
        );
    }
    drop(delivery_tx);
    task.await??;
    let cdc_rows: Vec<(u64, String)> = connection
        .query("SELECT id, payload FROM cdc_rows ORDER BY id")
        .await?;
    assert_eq!(cdc_rows, vec![(3, "three".to_owned())]);
    connection.disconnect().await?;
    Ok(())
}
