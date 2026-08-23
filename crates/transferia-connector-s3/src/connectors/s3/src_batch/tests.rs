use super::*;
use crate::metrics::MetricsRegistry;
use std::sync::Arc;

#[test]
fn source_supports_only_json_parquet_and_discard_parsers() {
    let common = "bucket: test\ntable_name: events\nregion: us-east-1\nparser:\n  type: json\n  common: {}\n  json_parser:\n    conversion_error: dlq\n    unknown_fields: { action: fail }\n    columns:\n      - { jsonpath: '$.id', column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }\n";
    let wrong = common.replace("type: json", "type: schema_registry");
    assert!(serde_yaml::from_str::<S3SourceConfig>(&wrong).is_err());
    let bad_prefix = format!("path_prefix: /bad\n{common}");
    assert!(S3SourceConnector::from_config(
        serde_yaml::from_str(&bad_prefix).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_err());
}

#[test]
fn parquet_source_requires_an_explicit_table_and_positive_batch_size() {
    let empty_table: S3SourceConfig = serde_yaml::from_str(
        "bucket: test\ntable_name: ''\nparser: { type: parquet, batch_rows: 65536 }\n",
    )
    .unwrap();
    assert!(empty_table.validate().is_err());

    let zero_batch: S3SourceConfig = serde_yaml::from_str(
        "bucket: test\ntable_name: events\nparser: { type: parquet, batch_rows: 0 }\n",
    )
    .unwrap();
    assert!(zero_batch.validate().is_err());

    let valid: S3SourceConfig = serde_yaml::from_str(
        "bucket: test\ntable_name: events\nparser: { type: parquet, batch_rows: 65536 }\n",
    )
    .unwrap();
    valid.validate().unwrap();
}
