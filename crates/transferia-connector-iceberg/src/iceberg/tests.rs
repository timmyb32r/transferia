use schemars::schema_for;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

use super::config::{IcebergSinkConfig, IcebergSourceConfig, OpenDalStorageConfig};

#[test]
fn source_defaults_to_s3_storage() {
    let config: IcebergSourceConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "uri": "https://catalog.example", "auth": { "type": "none" } },
        "namespace": ["analytics"],
        "table_names": ["events"]
    }))
    .expect("valid source config");
    assert!(matches!(config.storage, OpenDalStorageConfig::S3(_)));
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
        "namespace": ["analytics"],
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
        "namespace": [" analytics"],
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
        "namespace": [" analytics"],
        "target_file_size_bytes": 1_048_576
    })).expect("syntactically valid config");
    let error = config.validate().expect_err("whitespace must fail");
    assert!(error
        .to_string()
        .contains("leading or trailing whitespace"));
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
