use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{
    Array as _, Decimal128Array, TimestampMicrosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use schemars::schema_for;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{DeliveryDiscovery, SchemaOrigin, SourceTopology};
use transferia_registry::SinkConnector;

use super::config::{
    IcebergParquetCompression, IcebergSinkConfig, IcebergSourceConfig, OpenDalStorageConfig,
};
use super::sink::IcebergCommitIdentity;
use super::source::{classify_scan_failure, restore_transferia_types};

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
    let first = IcebergCommitIdentity::new("delivery", 0, "events", table, 7);
    let replay = IcebergCommitIdentity::new("delivery", 0, "events", table, 7);
    assert_eq!(first.token, replay.token);
    assert_eq!(first.durable_key, replay.durable_key);
    assert_eq!(first.uuid, replay.uuid);

    for distinct in [
        IcebergCommitIdentity::new("other-delivery", 0, "events", table, 7),
        IcebergCommitIdentity::new("delivery", 1, "events", table, 7),
        IcebergCommitIdentity::new("delivery", 0, "other-events", table, 7),
        IcebergCommitIdentity::new("delivery", 0, "events", uuid::Uuid::from_u128(2), 7),
        IcebergCommitIdentity::new("delivery", 0, "events", table, 8),
    ] {
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
        #[allow(unreachable_code)]
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
    assert!(error.to_string().contains("Iceberg file writer task failed"));
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
        let error = match connector
            .isolate_speedtest(
                Arc::clone(&discovery),
                "0123456789abcdef0123456789abcdef".to_owned(),
            )
            .await
        {
            Ok(_) => panic!("Iceberg sink speedtest must fail before external I/O"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Iceberg sink speedtests are disabled"));
        assert!(error.to_string().contains("before external I/O"));
    }
}
