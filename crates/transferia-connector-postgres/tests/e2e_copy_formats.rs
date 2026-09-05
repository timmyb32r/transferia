#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array,
    Int8Array, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_connector_postgres::metrics::{MetricsRegistry, SinkCounters};
use transferia_connector_postgres::postgres::{PostgresSinkConnector, PostgresSourceConnector};
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
    SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::{Delivery, DeliveryId, DeliveryMeta, SinkBatch, SinkEvent, SinkIo};
use transferia_core::source::Source as _;
use transferia_registry::{
    SinkBuildContext, SinkConnector as _, SinkPrepare, SourceBuildContext, SourceConnector as _,
    SourceDiscoveryContext,
};

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_TAG: &str = "17.6-bookworm";

#[derive(Debug, PartialEq)]
struct SnapshotRow {
    id: i64,
    char_value: i8,
    oid_value: u32,
    flag: bool,
    ratio: f64,
    name: Option<String>,
    payload: Vec<u8>,
    day: i32,
    created_at: i64,
    observed_at: i64,
}

#[tokio::test]
async fn binary_and_text_copy_are_wire_and_value_equivalent() -> anyhow::Result<()> {
    let postgres = GenericImage::new(POSTGRES_IMAGE, POSTGRES_TAG)
        .with_exposed_port(5_432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start()
        .await?;
    let host = reachable_host(&postgres.get_host().await?);
    let port = postgres.get_host_port_ipv4(5_432.tcp()).await?;
    let connection =
        format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let client = connect_with_retry(&connection).await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE copy_source (
                id bigint NOT NULL,
                char_value "char" NOT NULL,
                oid_value oid NOT NULL,
                flag boolean NOT NULL,
                ratio double precision NOT NULL,
                name text,
                payload bytea NOT NULL,
                day date NOT NULL,
                created_at timestamp(6) without time zone NOT NULL,
                observed_at timestamp(6) with time zone NOT NULL
            );
            INSERT INTO copy_source VALUES
                (2, '\377'::"char", 4000000000::oid, false, -1.25,
                 E'literal\\N\ttab\nline\\tail', decode('00ff09', 'hex'),
                 DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00.123456',
                 TIMESTAMPTZ '2024-01-01 03:00:00.123456+03'),
                (1, ''::"char", 42::oid, true, 1.5, NULL, decode('', 'hex'),
                 DATE '1970-01-01', TIMESTAMP '1969-12-31 23:59:59.999999',
                 TIMESTAMPTZ '1969-12-31 23:59:59.999999+00');
            "#,
        )
        .await?;

    let binary = read_snapshot(&host, port, "binary").await?;
    let text = read_snapshot(&host, port, "text").await?;
    assert_eq!(binary, text);
    assert_eq!(
        binary,
        vec![
            SnapshotRow {
                id: 1,
                char_value: 0,
                oid_value: 42,
                flag: true,
                ratio: 1.5,
                name: None,
                payload: Vec::new(),
                day: 0,
                created_at: -1,
                observed_at: -1,
            },
            SnapshotRow {
                id: 2,
                char_value: -1,
                oid_value: 4_000_000_000,
                flag: false,
                ratio: -1.25,
                name: Some("literal\\N\ttab\nline\\tail".to_owned()),
                payload: vec![0, 255, 9],
                day: 19_723,
                created_at: 1_704_067_200_123_456,
                observed_at: 1_704_067_200_123_456,
            },
        ]
    );

    write_sink_batch(&host, port, "copy_binary_target", "binary").await?;
    write_sink_batch(&host, port, "copy_text_target", "text").await?;
    let binary = stored_sink_rows(&client, "copy_binary_target").await?;
    let text = stored_sink_rows(&client, "copy_text_target").await?;
    assert_eq!(binary, text);
    assert_eq!(
        binary,
        vec![
            (
                0,
                42,
                String::new(),
                None,
                "1970-01-01".to_owned(),
                "1969-12-31 23:59:59.999999".to_owned(),
                "1969-12-31 23:59:59.999999".to_owned(),
                1.5,
                true,
            ),
            (
                -1,
                4_000_000_000,
                "00ff09".to_owned(),
                Some("literal\\N\ttab\nline\\tail".to_owned()),
                "2024-01-01".to_owned(),
                "2024-01-01 00:00:00.123456".to_owned(),
                "2024-01-01 00:00:00.123456".to_owned(),
                -1.25,
                false,
            ),
        ]
    );
    Ok(())
}

async fn read_snapshot(host: &str, port: u16, format: &str) -> anyhow::Result<Vec<SnapshotRow>> {
    let connector = PostgresSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - {{ schema: public, name: copy_source }}\nbatch_rows: 1\ncopy_to_format: {format}\nreplication:\n  plugin: {{ type: pgoutput, publication: must_not_be_used_for_batch }}\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
        })
        .await?;
    let mut source = connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
            phase: transferia_registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(16 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let mut rows = Vec::new();
    loop {
        match source.read_batch().await? {
            SourceBatch::Typed { tables, .. } => {
                assert_eq!(tables.len(), 1);
                let batch = &tables[0].batch;
                let ids = array::<Int64Array>(batch, 0);
                let chars = array::<Int8Array>(batch, 1);
                let oids = array::<UInt32Array>(batch, 2);
                let flags = array::<BooleanArray>(batch, 3);
                let ratios = array::<Float64Array>(batch, 4);
                let names = array::<StringArray>(batch, 5);
                let payloads = array::<BinaryArray>(batch, 6);
                let days = array::<Date32Array>(batch, 7);
                let created_at = array::<TimestampMicrosecondArray>(batch, 8);
                let observed_at = array::<TimestampMicrosecondArray>(batch, 9);
                assert_eq!(batch.schema().field(7).data_type(), &DataType::Date32);
                assert_eq!(
                    batch.schema().field(8).data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
                assert_eq!(
                    batch.schema().field(9).data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                );
                for row in 0..batch.num_rows() {
                    rows.push(SnapshotRow {
                        id: ids.value(row),
                        char_value: chars.value(row),
                        oid_value: oids.value(row),
                        flag: flags.value(row),
                        ratio: ratios.value(row),
                        name: (!names.is_null(row)).then(|| names.value(row).to_owned()),
                        payload: payloads.value(row).to_vec(),
                        day: days.value(row),
                        created_at: created_at.value(row),
                        observed_at: observed_at.value(row),
                    });
                }
            }
            SourceBatch::Finished => break,
            SourceBatch::Raw { .. } => panic!("PostgreSQL snapshot must emit typed batches"),
        }
    }
    source.shutdown().await?;
    rows.sort_by_key(|row| row.id);
    Ok(rows)
}

async fn write_sink_batch(host: &str, port: u16, table: &str, format: &str) -> anyhow::Result<()> {
    let schema = sink_schema();
    let discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("copy-format-e2e"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from(table),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    });
    let connector = PostgresSinkConnector::from_config(serde_yaml::from_str(&format!(
        "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncreate_tables: true\ncopy_from_format: {format}\n"
    ))?)?;
    connector.limits().validate_discovery(&discovery)?;
    connector
        .prepare(
            SinkPrepare::from_discovery(&discovery, true, "copy-format-e2e", None)?
                .expect("one dataset"),
        )
        .await?;
    let memory = PipelineMemory::new(16 * 1024 * 1024);
    let sink = connector
        .build_sink(SinkBuildContext {
            partition_id: 0,
            delivery_name: "test delivery".into(),
            replay_identity: None,
            finite_source: true,
            counters: Arc::new(SinkCounters::new()),
            keep_system_columns: false,
            discovery,
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let batch = sink_batch()?;
    let bytes = batch.get_array_memory_size();
    let (deliveries, delivery_rx) = mpsc::channel(1);
    let (events, mut event_rx) = mpsc::channel(1);
    let task = tokio::spawn(sink.run(SinkIo {
        deliveries: delivery_rx,
        events,
        memory: memory.clone(),
        cancellation: CancellationToken::new(),
    }));
    deliveries
        .send(Delivery {
            id: DeliveryId::new(1),
            outputs: vec![SinkBatch {
                table: Arc::from(table),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve_transform(bytes),
                system_columns: SystemColumns::default(),
            }],
            meta: DeliveryMeta { source_messages: 2 },
        })
        .await?;
    drop(deliveries);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), event_rx.recv())
            .await?
            .expect("sink commit event"),
        SinkEvent::CommittedThrough(DeliveryId::new(1))
    );
    task.await??;
    Ok(())
}

fn sink_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("char_value".to_owned(), DataType::Int8, false),
        SchemaColumn::new("oid_value".to_owned(), DataType::UInt32, false),
        SchemaColumn::new("payload".to_owned(), DataType::Binary, false),
        SchemaColumn::new("name".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("day".to_owned(), DataType::Date32, false),
        SchemaColumn::new(
            "created_at".to_owned(),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        SchemaColumn::new(
            "observed_at".to_owned(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        SchemaColumn::new("ratio".to_owned(), DataType::Float64, false),
        SchemaColumn::new("flag".to_owned(), DataType::Boolean, false),
    ])
}

fn sink_batch() -> anyhow::Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("char_value", DataType::Int8, false),
            Field::new("oid_value", DataType::UInt32, false),
            Field::new("payload", DataType::Binary, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("day", DataType::Date32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("ratio", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, false),
        ])),
        vec![
            Arc::new(Int8Array::from(vec![0, -1])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![42, 4_000_000_000])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![
                b"".as_slice(),
                b"\0\xff\t".as_slice(),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                None,
                Some("literal\\N\ttab\nline\\tail"),
            ])) as ArrayRef,
            Arc::new(Date32Array::from(vec![0, 19_723])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![
                -1,
                1_704_067_200_123_456,
            ])) as ArrayRef,
            Arc::new(
                TimestampMicrosecondArray::from(vec![-1, 1_704_067_200_123_456])
                    .with_timezone("UTC"),
            ) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.5, -1.25])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ],
    )?)
}

type StoredRow = (
    i32,
    i64,
    String,
    Option<String>,
    String,
    String,
    String,
    f64,
    bool,
);

async fn stored_sink_rows(
    client: &tokio_postgres::Client,
    table: &str,
) -> anyhow::Result<Vec<StoredRow>> {
    let statement = format!(
        "SELECT char_value::integer, oid_value::bigint, encode(payload, 'hex'), name, day::text, created_at::text, (observed_at AT TIME ZONE 'UTC')::text, ratio, flag FROM \"{table}\" ORDER BY oid_value"
    );
    client
        .query(&statement, &[])
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get(0)?,
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
                row.try_get(4)?,
                row.try_get(5)?,
                row.try_get(6)?,
                row.try_get(7)?,
                row.try_get(8)?,
            ))
        })
        .collect()
}

fn array<T: arrow::array::Array + 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch.column(index).as_any().downcast_ref().unwrap()
}

fn reachable_host(host: &impl ToString) -> String {
    match host.to_string().as_str() {
        "localhost" => "127.0.0.1".to_owned(),
        host => host.to_owned(),
    }
}

async fn connect_with_retry(connection: &str) -> anyhow::Result<tokio_postgres::Client> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok((client, connection)) =
                tokio_postgres::connect(connection, tokio_postgres::NoTls).await
            {
                tokio::spawn(async move {
                    drop(connection.await);
                });
                return client;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("PostgreSQL testcontainer did not become ready"))
}
