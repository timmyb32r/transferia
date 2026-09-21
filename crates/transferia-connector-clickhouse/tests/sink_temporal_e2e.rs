#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, reason = "test assertions")]

use std::sync::Arc;

use anyhow::Context as _;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_connector_clickhouse::clickhouse::ClickHouseSinkConnector;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology, UpdatePolicy};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_registry::{SinkBuildContext, SinkConnector as _, SinkPrepare};

fn discovery(table: &str, schema: DatasetSchema) -> Arc<DeliveryDiscovery> {
    Arc::new(DeliveryDiscovery {
        source_name: Arc::from("temporal-regression"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            namespace: None,
            update_policy: UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name: Arc::from(table),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    })
}

async fn query(http: &reqwest::Client, url: &str, sql: &str) -> anyhow::Result<String> {
    let response = http.post(url).body(sql.to_owned()).send().await?;
    let status = response.status();
    let text = response.text().await?;
    anyhow::ensure!(status.is_success(), "ClickHouse fixture query failed: {status}: {text}");
    Ok(text)
}

async fn write(
    connector: &ClickHouseSinkConnector,
    discovery: Arc<DeliveryDiscovery>,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    let bytes = batch.get_array_memory_size();
    // Match the production-sized sink fixtures; Arrow/Parquet carry schema and
    // serialization overhead unrelated to the five input rows.
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let sink = connector.build_sink(SinkBuildContext {
        delivery_name: "temporal regression".into(),
        durable: transferia_test_support::durable_context(),
        partition_id: 0,
        replay_identity: None,
        finite_source: true,
        counters: Arc::new(SinkCounters::new()),
        keep_system_columns: false,
        discovery: Arc::clone(&discovery),
    }).await?;
    let (deliveries, receiver) = mpsc::channel(1);
    let (events, mut commits) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let mut task = tokio::spawn(sink.run(SinkIo {
        deliveries: receiver,
        events,
        memory: memory.clone(),
        cancellation: cancellation.clone(),
    }));
    deliveries.send(Delivery {
        id: DeliveryId::new(1),
        meta: DeliveryMeta { source_messages: batch.num_rows() as u64 },
        outputs: vec![SinkBatch {
            table: Arc::clone(&discovery.datasets[0].name),
            is_dlq: false,
            batch,
            byte_size: bytes,
            memory: memory.reserve_transform(bytes),
            system_columns: transferia_core::SystemColumns::default(),
        }],
    }).await?;
    drop(deliveries);
    let completion = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        // Surface an actor/transport error immediately, even if it never emits
        // a commit. Do not hide the cause behind the fixture's outer deadline.
        let commit = tokio::select! {
            biased;
            result = &mut task => { result??; commits.recv().await }
            commit = commits.recv() => { (&mut task).await??; commit }
        };
        anyhow::ensure!(commit == Some(SinkEvent::CommittedThrough(DeliveryId::new(1))),
            "ClickHouse actor finished without committing the fixture delivery");
        Ok::<_, anyhow::Error>(())
    }).await;
    if completion.is_err() {
        cancellation.cancel();
        task.abort();
        drop(task.await);
    }
    completion.context("ClickHouse fixture actor did not finish")??;
    Ok(())
}

#[tokio::test]
async fn explicit_timestamps_preserve_instants_on_a_non_utc_server() -> anyhow::Result<()> {
    let container = GenericImage::new("clickhouse/clickhouse-server", "25.8.28.1")
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::http(HttpWaitStrategy::new("/ping")
            .with_port(8123.tcp()).with_expected_status_code(200_u16)))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_env_var("TZ", "Europe/Moscow")
        .start().await?;
    let host = container.get_host().await?.to_string();
    let host = if host == "localhost" { "127.0.0.1".to_owned() } else { host };
    let native = container.get_host_port_ipv4(9000.tcp()).await?;
    let http_port = container.get_host_port_ipv4(8123.tcp()).await?;
    let url = format!("http://{host}:{http_port}/");
    let http = reqwest::Client::new();
    assert_eq!(query(&http, &url, "SELECT timezone()").await?.trim(), "Europe/Moscow");

    // The original comparison failed at Moscow's different historical offsets:
    // January 1971 (+3h) and July 2013 (+4h). Include fractions, pre-epoch and
    // post-u32 seconds, and NULL to exercise all three production wire formats.
    let micros = vec![
        Some(31_536_000_123_456),
        Some(1_372_636_800_654_321),
        Some(-1_000_001),
        Some(4_294_967_296_000_000),
        None,
    ];
    let seconds = vec![Some(31_536_000), Some(1_372_636_800), Some(-2), Some(4_294_967_296), None];
    // PostgreSQL's NUMERIC source now emits these exact Decimal128 values;
    // the historical projection failed because its schema was Utf8 instead.
    let integers = vec![Some(18_446_744_073_709_551_616_i128), Some(99_999_999_999_999_999_999), Some(-99_999_999_999_999_999_999), Some(0), None];
    let fractions = vec![Some(123_456_789_012_345_678_901_234_i128), Some(-999_999_999_999_999_999_999_999), Some(1), Some(0), None];
    for format in ["native", "parquet", "arrow_stream"] {
        let table = format!("temporal_{format}");
        let connector = ClickHouseSinkConnector::from_config(serde_yaml::from_str(&format!(
            "hosts: ['{host}']\nport: {native}\nhttp_port: {http_port}\ntrusted_plaintext: true\ndatabase: default\nusername: default\ninsert_format: {format}\nflush_interval_ms: 10\nrequest_timeout_ms: 5000\nretry_max_attempts: 1\n"
        ))?)?;
        let schema = DatasetSchema::new(vec![
            SchemaColumn::new("id".into(), DataType::Int64, false),
            SchemaColumn::new("at_utc".into(), DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            SchemaColumn::new("at_moscow".into(), DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Moscow"))), true),
            SchemaColumn::new("seconds".into(), DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))), true),
            SchemaColumn::new("numeric_integer".into(), DataType::Decimal128(20, 0), true),
            SchemaColumn::new("numeric_fraction".into(), DataType::Decimal128(24, 4), true),
        ]);
        let discovery = discovery(&table, schema);
        connector.limits().validate_discovery(&discovery)?;
        connector.prepare(SinkPrepare::from_discovery(&discovery, false, "temporal", None)?.unwrap()).await?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(discovery.datasets[0].incoming_schema.columns.iter().map(|column| {
                Field::new(&column.name, column.data_type.clone(), column.nullable).with_metadata(column.arrow_metadata())
            }).collect::<Vec<_>>())),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(micros.clone()).with_timezone("UTC")),
                Arc::new(TimestampMicrosecondArray::from(micros.clone()).with_timezone("Europe/Moscow")),
                Arc::new(TimestampSecondArray::from(seconds.clone()).with_timezone("UTC")),
                Arc::new(Decimal128Array::from(integers.clone()).with_precision_and_scale(20, 0)?),
                Arc::new(Decimal128Array::from(fractions.clone()).with_precision_and_scale(24, 4)?),
            ],
        )?;
        write(&connector, Arc::clone(&discovery), batch).await.with_context(|| format!("{format} temporal/decimal insert"))?;
        let actual = query(&http, &url, &format!(
            "SELECT toUnixTimestamp64Micro(at_utc), toUnixTimestamp64Micro(at_moscow), toUnixTimestamp64Second(seconds) FROM {table} ORDER BY id FORMAT TabSeparated"
        )).await?;
        let expected = micros.iter().zip(&seconds).map(|(micro, second)| match (micro, second) {
            (Some(micro), Some(second)) => format!("{micro}\t{micro}\t{second}\n"),
            _ => "\\N\t\\N\t\\N\n".to_owned(),
        }).collect::<String>();
        assert_eq!(actual, expected, "{format} must preserve instants exactly");
        let rendered = query(&http, &url, &format!(
            "SELECT toString(at_utc), toString(at_moscow) FROM {table} WHERE id IN (0, 1) ORDER BY id FORMAT TabSeparated"
        )).await?;
        assert_eq!(rendered,
            "1971-01-01 00:00:00.123456\t1971-01-01 03:00:00.123456\n2013-07-01 00:00:00.654321\t2013-07-01 04:00:00.654321\n");
        let numeric = query(&http, &url, &format!(
            "SELECT toString(numeric_integer), toString(numeric_fraction) FROM {table} ORDER BY id FORMAT TabSeparated"
        )).await?;
        assert_eq!(numeric,
            "18446744073709551616\t12345678901234567890.1234\n99999999999999999999\t-99999999999999999999.9999\n-99999999999999999999\t0.0001\n0\t0\n\\N\t\\N\n",
            "{format} must preserve NUMERIC precision, scale and NULL");
    }

    // An omitted zone must not be read as UTC merely because the client type
    // parser uses UTC as its fallback. Reject before any INSERT or acknowledgement.
    query(&http, &url, "CREATE TABLE implicit_zone (at DateTime64(6)) ENGINE=MergeTree ORDER BY tuple()").await?;
    let connector = ClickHouseSinkConnector::from_config(serde_yaml::from_str(&format!(
        "hosts: ['{host}']\nport: {native}\ntrusted_plaintext: true\ndatabase: default\nusername: default\n"
    ))?)?;
    let discovery = discovery("implicit_zone", DatasetSchema::new(vec![SchemaColumn::new(
        "at".into(), DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false,
    )]));
    connector.limits().validate_discovery(&discovery)?;
    let error = connector.prepare(SinkPrepare::from_discovery(&discovery, false, "temporal", None)?.unwrap()).await.unwrap_err();
    assert!(format!("{error:#}").contains("omits its timezone"));
    assert_eq!(query(&http, &url, "SELECT count() FROM implicit_zone").await?.trim(), "0");

    // Exercise the native driver below the sink's schema guard: a local codec
    // failure must retain its permanent type instead of looking like a broken
    // connection and retrying an input that can never be encoded.
    let client = clickhouse_arrow::ClientBuilder::new()
        .with_destination(format!("{host}:{native}"))
        .with_database("default")
        .with_username("default")
        .with_tls(false)
        .build_arrow().await?;
    let invalid = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["invalid-integer"]))],
    )?;
    let error = match client.insert_many("INSERT INTO temporal_native (id) VALUES", vec![invalid], None).await {
        Ok(_) => anyhow::bail!("mismatched native block unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(error, clickhouse_arrow::Error::ArrowSerialize(_)), "{error}");
    assert_eq!(query(&http, &url, "SELECT count() FROM temporal_native").await?.trim(), "5");

    // A server rejection before the column header must also retain its typed
    // cause, rather than hanging on the header or reporting a channel closure.
    let client = clickhouse_arrow::ClientBuilder::new()
        .with_destination(format!("{host}:{native}"))
        .with_database("default")
        .with_username("default")
        .with_tls(false)
        .build_arrow().await?;
    let valid = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )?;
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), client.insert_many(
        "INSERT INTO definitely_missing_header_destination (id) VALUES", vec![valid], None,
    )).await.context("missing-table INSERT did not report its pre-header rejection")?;
    let error = match result {
        Ok(_) => anyhow::bail!("INSERT into a missing table unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(&error, clickhouse_arrow::Error::ServerException(exception) if exception.code == 60),
        "expected the original UNKNOWN_TABLE server exception: {error}");
    Ok(())
}
