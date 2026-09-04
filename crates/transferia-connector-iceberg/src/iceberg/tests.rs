use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array as _, Decimal128Array, Int64Array, TimestampMicrosecondArray, TimestampSecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use iceberg::TableIdent;
use schemars::schema_for;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, DiscoveredSystemColumn, SchemaOrigin,
    SourceTopology,
};
use transferia_registry::{SinkConnector, SnapshotRowCountStrategy};

use super::config::{
    IcebergParquetCompression, IcebergSinkConfig, IcebergSourceConfig, OpenDalStorageConfig,
};
use super::sink::IcebergCommitIdentity;
use super::source::{classify_scan_failure, restore_transferia_types};

fn speedtest_row_count_discovery() -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "id".to_owned(),
        DataType::Int64,
        false,
    )]);
    DeliveryDiscovery {
        source_name: Arc::from("snapshot-source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                role: DatasetRole::Main,
                name: Arc::from("events"),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from("events_dlq"),
                incoming_schema: schema.clone(),
                stored_schema: schema,
                system_columns: Vec::new(),
            },
        ],
        performance_advice: Vec::new(),
    }
}

fn replica_discovery(source: &str, complete_old_image: bool) -> DeliveryDiscovery {
    let system_columns = [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::ChangeOperation,
        SystemColumnKind::ChangedColumns,
    ]
    .into_iter()
    .map(DiscoveredSystemColumn::from)
    .collect::<Vec<_>>();
    let mut stored_columns = vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("value".to_owned(), DataType::Utf8, false),
    ];
    stored_columns.extend(
        system_columns
            .iter()
            .filter(|column| {
                !matches!(
                    column.kind,
                    SystemColumnKind::ChangeOperation | SystemColumnKind::ChangedColumns
                )
            })
            .map(|column| {
                SchemaColumn::new(column.name.to_string(), column.kind.data_type(), false)
            }),
    );
    let stored = DatasetSchema::new(stored_columns);
    let mut incoming = stored.columns.clone();
    incoming.push(
        SchemaColumn::new("_old_id".to_owned(), DataType::Int64, true)
            .with_old_value_of("id".to_owned()),
    );
    if complete_old_image {
        incoming.push(
            SchemaColumn::new("_old_value".to_owned(), DataType::Utf8, true)
                .with_old_value_of("value".to_owned()),
        );
    }
    incoming.push(
        SchemaColumn::new("_source_tx".to_owned(), DataType::UInt64, false)
            .with_system_role(SYSTEM_ROLE_SOURCE_TRANSACTION_ID),
    );
    incoming.extend(
        system_columns
            .iter()
            .filter(|column| {
                matches!(
                    column.kind,
                    SystemColumnKind::ChangeOperation | SystemColumnKind::ChangedColumns
                )
            })
            .map(|column| {
                SchemaColumn::new(column.name.to_string(), column.kind.data_type(), false)
            }),
    );
    DeliveryDiscovery {
        source_name: Arc::from(source),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: true,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(incoming),
            stored_schema: stored,
            system_columns,
        }],
        performance_advice: Vec::new(),
    }
}

struct FakeIcebergRowCountCatalog {
    rows: BTreeMap<String, Option<u64>>,
    calls: Mutex<Vec<String>>,
}

impl super::sink::IcebergRowCountCatalog for FakeIcebergRowCountCatalog {
    fn row_count<'a>(
        &'a self,
        table: &'a TableIdent,
    ) -> BoxFuture<'a, anyhow::Result<Option<u64>>> {
        Box::pin(async move {
            let target = table.to_string();
            self.calls.lock().expect("calls lock").push(target.clone());
            self.rows
                .get(&target)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("unexpected table '{target}'"))
        })
    }
}

#[test]
fn source_defaults_to_s3_storage() {
    let config: IcebergSourceConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "namespace": "analytics",
        "table_names": ["events"]
    }))
    .expect("valid source config");
    assert!(matches!(config.storage, OpenDalStorageConfig::S3(_)));
    assert_eq!(config.read_batch_rows, 65_536);
    assert_eq!(config.read_data_file_concurrency, 32);
    assert_eq!(config.read_manifest_concurrency, 32);
    assert_eq!(config.parquet_metadata_size_hint_bytes, 512 * 1024);
    assert_eq!(config.parquet_range_coalesce_bytes, 1024 * 1024);
    assert_eq!(config.parquet_range_fetch_concurrency, 10);
}

#[test]
fn iceberg_source_rejects_zero_read_parallelism() {
    let config: IcebergSourceConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics",
        "table_names": ["events"],
        "read_data_file_concurrency": 0
    }))
    .expect("syntactically valid config");
    let error = config.validate().expect_err("zero concurrency must fail");
    assert!(error.to_string().contains("read_data_file_concurrency"));
}

#[test]
fn iceberg_sink_groups_deliveries_until_target_or_end_of_input() {
    assert!(!super::sink::delivery_group_ready(64, 128, false));
    assert!(super::sink::delivery_group_ready(128, 128, false));
    assert!(super::sink::delivery_group_ready(64, 128, true));
}

#[test]
fn iceberg_sink_defaults_to_bounded_parallel_zstd_writes() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics"
    }))
    .expect("valid sink config");
    assert_eq!(config.parquet_compression, IcebergParquetCompression::Zstd);
    assert_eq!(config.parquet_row_group_rows, 250_000);
    assert_eq!(config.write_concurrency, 8);
    assert_eq!(config.commit_target_size_bytes, 512 * 1024 * 1024);
    assert_eq!(config.commit_target_size_bytes(), 512 * 1024 * 1024);
    config.validate().expect("default sink config validates");
}

#[test]
fn iceberg_sink_rejects_zero_write_concurrency() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics",
        "write_concurrency": 0
    }))
    .expect("syntactically valid config");
    let error = config.validate().expect_err("zero concurrency must fail");
    assert!(error.to_string().contains("write_concurrency"));
}

#[test]
fn iceberg_replica_requires_mysql_or_postgres_and_complete_old_images() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics"
    }))
    .expect("valid sink config");
    let connector = super::sink::IcebergSinkConnector::from_config(config).expect("connector");
    connector
        .limits()
        .validate_discovery(&replica_discovery("mysql", true))
        .expect("complete MySQL replica contract");
    connector
        .limits()
        .validate_discovery(&replica_discovery("postgres", true))
        .expect("complete PostgreSQL replica contract");
    let missing_old = connector
        .limits()
        .validate_discovery(&replica_discovery("postgres", false))
        .expect_err("partial old image must fail before table creation");
    assert!(
        missing_old.to_string().contains("old-value column"),
        "{missing_old}"
    );
    let unsupported = connector
        .limits()
        .validate_discovery(&replica_discovery("logbroker", true))
        .expect_err("unsupported changelog source must fail before table creation");
    assert!(unsupported.to_string().contains("PostgreSQL and MySQL"));
}

#[test]
fn iceberg_sink_declares_exact_additive_snapshot_row_counts() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics"
    }))
    .expect("valid sink config");
    let connector = super::sink::IcebergSinkConnector::from_config(config).expect("connector");

    assert_eq!(
        connector.snapshot_row_count_strategy(),
        Some(SnapshotRowCountStrategy::AdditiveBaseline)
    );
}

#[tokio::test]
async fn iceberg_snapshot_row_counts_cover_main_and_dlq_without_scanning_data() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics"
    }))
    .expect("valid sink config");
    let catalog = FakeIcebergRowCountCatalog {
        rows: BTreeMap::from([
            ("analytics.events".to_owned(), Some(41)),
            ("analytics.events_dlq".to_owned(), None),
        ]),
        calls: Mutex::new(Vec::new()),
    };

    let counts = super::sink::snapshot_iceberg_row_counts(
        &catalog,
        &config,
        &speedtest_row_count_discovery(),
    )
    .await
    .expect("exact catalog metadata counts");

    assert_eq!(counts.len(), 2);
    assert_eq!(counts[0].role, DatasetRole::Main);
    assert_eq!(counts[0].table.as_ref(), "events");
    assert_eq!(counts[0].target.as_ref(), "analytics.events");
    assert!(counts[0].exists);
    assert_eq!(counts[0].rows, 41);
    assert_eq!(counts[1].role, DatasetRole::DeadLetterQueue);
    assert_eq!(counts[1].table.as_ref(), "events_dlq");
    assert_eq!(counts[1].target.as_ref(), "analytics.events_dlq");
    assert!(!counts[1].exists);
    assert_eq!(counts[1].rows, 0);
    assert_eq!(
        catalog.calls.lock().expect("calls lock").as_slice(),
        ["analytics.events", "analytics.events_dlq"]
    );
}

#[tokio::test]
async fn iceberg_snapshot_row_count_probe_fails_on_an_unexpected_target() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": "analytics"
    }))
    .expect("valid sink config");
    let catalog = FakeIcebergRowCountCatalog {
        rows: BTreeMap::from([("analytics.events".to_owned(), Some(41))]),
        calls: Mutex::new(Vec::new()),
    };

    let error = super::sink::snapshot_iceberg_row_counts(
        &catalog,
        &config,
        &speedtest_row_count_discovery(),
    )
    .await
    .expect_err("missing exact metadata must fail closed");

    assert!(error.to_string().contains("analytics.events_dlq"));
}

#[test]
fn iceberg_snapshot_row_count_requires_an_exact_unsigned_total() {
    assert_eq!(
        super::sink::exact_iceberg_total_records(7, Some("0")).expect("zero total"),
        0
    );
    assert_eq!(
        super::sink::exact_iceberg_total_records(7, Some("18446744073709551615"))
            .expect("u64 total"),
        u64::MAX
    );
    for value in [None, Some("-1"), Some("1.0"), Some("not-a-count")] {
        let error = super::sink::exact_iceberg_total_records(91, value)
            .expect_err("inexact snapshot summary must fail closed");
        assert!(error.to_string().contains("snapshot 91"));
        assert!(error.to_string().contains("total-records"));
    }
}

#[test]
fn hdfs_is_an_explicit_storage_variant() {
    let config: IcebergSourceConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": {
            "type": "hdfs",
            "endpoint": "https://namenode.example:9871",
            "authority": "namenode.example",
            "root": "/warehouse",
            "user": "transferia"
        },
        "namespace": "analytics",
        "table_names": ["events"]
    }))
    .expect("valid HDFS config");
    config.validate().expect("HDFS config validates");
    assert!(matches!(config.storage, OpenDalStorageConfig::Hdfs(_)));
}

#[test]
fn config_rejects_silent_identifier_trimming() {
    let config: IcebergSourceConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": " analytics",
        "table_names": ["events"]
    }))
    .expect("syntactically valid config");
    let error = config.validate().expect_err("whitespace must be rejected");
    assert!(error.to_string().contains("leading or trailing whitespace"));
}

#[test]
fn sink_rejects_invalid_destination_namespace() {
    let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "storage": { "type": "s3", "bucket": "warehouse" },
        "namespace": " analytics",
        "target_file_size_bytes": 1_048_576
    }))
    .expect("syntactically valid config");
    let error = config.validate().expect_err("whitespace must fail");
    assert!(error.to_string().contains("leading or trailing whitespace"));
}

#[test]
fn source_and_sink_schemas_expose_storage_choice() {
    let source = serde_json::to_value(schema_for!(IcebergSourceConfig)).expect("source schema");
    let sink = serde_json::to_value(schema_for!(IcebergSinkConfig)).expect("sink schema");
    for schema in [source, sink] {
        let rendered = schema.to_string();
        assert!(rendered.contains("s3"));
        assert!(rendered.contains("hdfs"));
        assert!(rendered.contains("storage"));
    }
}

#[test]
fn storage_debug_output_redacts_every_s3_credential() {
    let storage: OpenDalStorageConfig = serde_json::from_value(serde_json::json!({
        "type": "s3",
        "bucket": "warehouse",
        "credentials": {
            "access_key": "access-secret",
            "secret_key": "key-secret"
        },
        "session_token": "session-secret"
    }))
    .expect("valid storage");
    let output = format!("{storage:?}");
    for secret in ["access-secret", "key-secret", "session-secret"] {
        assert!(!output.contains(secret));
    }
}

#[test]
fn iceberg_schema_preserves_primary_key_columns() {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), arrow::datatypes::DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("value".to_owned(), arrow::datatypes::DataType::Utf8, true),
    ]);
    let converted = super::sink::iceberg_schema(&schema).expect("Iceberg schema");
    let identifiers = converted
        .identifier_field_ids()
        .filter_map(|id| converted.name_by_field_id(id))
        .collect::<Vec<_>>();
    assert_eq!(identifiers, ["id"]);
}

#[test]
fn iceberg_sink_losslessly_widens_timestamp_seconds_to_microseconds() {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new(
            "event_time".to_owned(),
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        SchemaColumn::new("event_date".to_owned(), DataType::Date32, false),
    ]);
    let iceberg = super::sink::iceberg_schema(&schema).expect("Iceberg schema");
    let target =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg).expect("Iceberg Arrow schema"));
    assert_eq!(
        target.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(target.field(1).data_type(), &DataType::Date32);

    let source = Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        source,
        vec![Arc::new(TimestampSecondArray::from(vec![
            Some(-1),
            Some(0),
            Some(1),
            None,
        ]))],
    )
    .expect("source batch");
    let target = Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let converted = super::sink::with_schema(&batch, target).expect("converted batch");
    let values = converted
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("microsecond timestamp");
    assert_eq!(values.value(0), -1_000_000);
    assert_eq!(values.value(1), 0);
    assert_eq!(values.value(2), 1_000_000);
    assert!(values.is_null(3));
}

#[test]
fn iceberg_timestamp_validation_rejects_invalid_schema_and_overflow_before_write() {
    let invalid_timezone = DatasetSchema::new(vec![SchemaColumn::new(
        "event_time".to_owned(),
        DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        false,
    )]);
    assert!(super::sink::iceberg_schema(&invalid_timezone).is_err());

    let source = Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Second, None),
        false,
    )]));
    let batch = RecordBatch::try_new(
        source,
        vec![Arc::new(TimestampSecondArray::from(vec![i64::MAX]))],
    )
    .expect("source batch");
    assert!(super::sink::validate_timestamp_values(&batch).is_err());
    let target = Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]));
    assert!(super::sink::with_schema(&batch, target).is_err());
}

#[test]
fn iceberg_sink_losslessly_maps_full_uint64_range_to_decimal() {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "offset".to_owned(),
        DataType::UInt64,
        false,
    )]);
    let iceberg = super::sink::iceberg_schema(&schema).expect("Iceberg schema");
    let target =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg).expect("Iceberg Arrow schema"));
    assert_eq!(target.field(0).data_type(), &DataType::Decimal128(20, 0));

    let source = Arc::new(Schema::new(vec![Field::new(
        "offset",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(source, vec![Arc::new(UInt64Array::from(vec![0, u64::MAX]))])
        .expect("source batch");
    let converted = super::sink::with_schema(&batch, target).expect("converted batch");
    let values = converted
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal column");
    assert_eq!(values.values(), &[0, i128::from(u64::MAX)]);
}

#[test]
fn iceberg_required_fields_accept_nullable_changelog_schema_only_when_values_are_present() {
    let source = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let target = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let present = RecordBatch::try_new(
        Arc::clone(&source),
        vec![Arc::new(Int64Array::from(vec![Some(7)]))],
    )
    .expect("nullable changelog batch with present value");
    super::sink::with_schema(&present, Arc::clone(&target))
        .expect("present changelog value can populate a required Iceberg field");

    let missing = RecordBatch::try_new(source, vec![Arc::new(Int64Array::from(vec![None]))])
        .expect("nullable changelog batch with missing value");
    assert!(super::sink::with_schema(&missing, target).is_err());
}

#[test]
fn iceberg_source_restores_transferia_message_index_to_uint64() {
    let physical = Arc::new(Schema::new(vec![Field::new(
        "_system_message_index",
        DataType::Decimal128(20, 0),
        false,
    )]));
    let batch = RecordBatch::try_new(
        physical,
        vec![Arc::new(
            Decimal128Array::from(vec![0, i128::from(u64::MAX)])
                .with_precision_and_scale(20, 0)
                .expect("valid decimal metadata"),
        )],
    )
    .expect("physical Iceberg batch");

    let restored = restore_transferia_types(batch).expect("lossless UInt64 restoration");
    assert_eq!(restored.schema().field(0).data_type(), &DataType::UInt64);
    let values = restored
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("UInt64 column");
    assert_eq!(values.values(), &[0, u64::MAX]);
}

#[test]
fn iceberg_snapshot_scan_only_retries_before_emitting_rows() {
    let initial = classify_scan_failure(0, anyhow::anyhow!("temporary read failure"));
    assert!(initial.is_retryable());

    let progressed = classify_scan_failure(1, anyhow::anyhow!("temporary read failure"));
    assert!(!progressed.is_retryable());
    assert!(progressed
        .to_string()
        .contains("restarting from the beginning would duplicate data"));
}

#[test]
fn iceberg_commit_identity_is_stable_and_scoped() {
    let table = uuid::Uuid::from_u128(1);
    let first = IcebergCommitIdentity::new_finite_for_test(
        "delivery",
        Some("replay"),
        0,
        "events",
        table,
        vec![7],
    )
    .expect("commit identity");
    let replay = IcebergCommitIdentity::new_finite_for_test(
        "delivery",
        Some("replay"),
        0,
        "events",
        table,
        vec![7],
    )
    .expect("commit identity");
    assert_eq!(first.token, replay.token);
    assert_eq!(first.exact, replay.exact);
    assert_eq!(first.durable_key, replay.durable_key);
    assert_eq!(first.uuid, replay.uuid);

    for distinct in [
        IcebergCommitIdentity::new_finite_for_test(
            "other-delivery",
            Some("replay"),
            0,
            "events",
            table,
            vec![7],
        ),
        IcebergCommitIdentity::new_finite_for_test(
            "delivery",
            Some("replay"),
            1,
            "events",
            table,
            vec![7],
        ),
        IcebergCommitIdentity::new_finite_for_test(
            "delivery",
            Some("replay"),
            0,
            "other-events",
            table,
            vec![7],
        ),
        IcebergCommitIdentity::new_finite_for_test(
            "delivery",
            Some("replay"),
            0,
            "events",
            uuid::Uuid::from_u128(2),
            vec![7],
        ),
        IcebergCommitIdentity::new_finite_for_test(
            "delivery",
            Some("replay"),
            0,
            "events",
            table,
            vec![8],
        ),
    ] {
        let distinct = distinct.expect("commit identity");
        assert_ne!(first.token, distinct.token);
        assert_ne!(first.durable_key, distinct.durable_key);
        assert_ne!(first.uuid, distinct.uuid);
    }
}

#[tokio::test]
async fn writer_collection_drains_all_tasks_after_one_writer_fails() {
    struct DropGuard(Arc<AtomicBool>);

    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let entered = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let late_write = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let mut writers = JoinSet::new();
    let writer_entered = Arc::clone(&entered);
    let writer_dropped = Arc::clone(&dropped);
    let writer_late_write = Arc::clone(&late_write);
    let writer_release = Arc::clone(&release);
    writers.spawn(async move {
        let _guard = DropGuard(writer_dropped);
        writer_entered.store(true, Ordering::Release);
        writer_release.notified().await;
        writer_late_write.store(true, Ordering::Release);
        Ok::<Vec<u8>, anyhow::Error>(Vec::new())
    });
    writers.spawn(async { anyhow::bail!("simulated writer failure") });
    let collector = tokio::spawn(super::sink::collect_writer_results(writers));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    tokio::task::yield_now().await;
    assert!(
        !collector.is_finished(),
        "a writer error must not detach or abort another in-flight writer"
    );
    release.notify_one();
    let error = collector
        .await
        .expect("the collector task must not panic")
        .expect_err("the first writer failure must be reported after all writers quiesce");
    assert!(error.to_string().contains("Iceberg file writer failed"));
    assert!(dropped.load(Ordering::Acquire));
    assert!(late_write.load(Ordering::Acquire));
}

#[tokio::test]
async fn writer_collection_drains_all_tasks_after_one_writer_panics() {
    let entered = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let mut writers = JoinSet::new();
    let writer_entered = Arc::clone(&entered);
    let writer_completed = Arc::clone(&completed);
    let writer_release = Arc::clone(&release);
    writers.spawn(async move {
        writer_entered.store(true, Ordering::Release);
        writer_release.notified().await;
        writer_completed.store(true, Ordering::Release);
        Ok::<Vec<u8>, anyhow::Error>(Vec::new())
    });
    writers.spawn(async {
        panic!("simulated writer panic");
        #[allow(
            unreachable_code,
            reason = "the explicit result type keeps the spawned panic task type-compatible"
        )]
        Ok::<Vec<u8>, anyhow::Error>(Vec::new())
    });
    let collector = tokio::spawn(super::sink::collect_writer_results(writers));
    while !entered.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    tokio::task::yield_now().await;
    assert!(
        !collector.is_finished(),
        "a writer panic must not detach or abort another in-flight writer"
    );
    release.notify_one();
    let error = collector
        .await
        .expect("the collector task must not panic")
        .expect_err("the writer panic must be reported after all writers quiesce");
    assert!(error
        .to_string()
        .contains("Iceberg file writer task failed"));
    assert!(completed.load(Ordering::Acquire));
}

#[tokio::test]
async fn iceberg_sink_speedtest_is_rejected_before_external_io_for_every_storage() {
    let storage_configs = [
        serde_json::json!({ "type": "s3", "bucket": "production" }),
        serde_json::json!({
            "type": "hdfs",
            "endpoint": "https://unreachable.invalid:9871",
            "authority": "unreachable.invalid",
            "root": "/production"
        }),
    ];
    let discovery = Arc::new(DeliveryDiscovery {
        source_name: Arc::from("source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: Vec::new(),
        performance_advice: Vec::new(),
    });

    for storage in storage_configs {
        let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "uri": "https://unreachable.invalid",
                "auth": { "type": "none" }
            },
            "storage": storage,
            "namespace": "production"
        }))
        .expect("valid sink config");
        let connector =
            Arc::new(super::sink::IcebergSinkConnector::from_config(config).expect("connector"));
        let Err(error) = connector
            .isolate_speedtest(
                Arc::clone(&discovery),
                "0123456789abcdef0123456789abcdef".to_owned(),
            )
            .await
        else {
            panic!("Iceberg sink speedtest must fail before external I/O");
        };
        assert!(error
            .to_string()
            .contains("Iceberg sink speedtests are disabled"));
        assert!(error.to_string().contains("before external I/O"));
    }
}
