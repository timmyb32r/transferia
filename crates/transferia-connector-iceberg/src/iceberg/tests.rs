use std::sync::Arc;

use arrow::array::{Decimal128Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use schemars::schema_for;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

use super::config::{IcebergSinkConfig, IcebergSourceConfig, OpenDalStorageConfig};
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
}

#[test]
fn iceberg_sink_groups_deliveries_until_target_or_end_of_input() {
    assert!(!super::sink::delivery_group_ready(64, 128, false));
    assert!(super::sink::delivery_group_ready(128, 128, false));
    assert!(super::sink::delivery_group_ready(64, 128, true));
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
