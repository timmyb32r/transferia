use super::*;
use crate::metrics::MetricsRegistry;
use std::sync::Arc;

#[test]
fn source_requires_json_parser_and_normalized_prefix() {
    let common = "bucket: test\nregion: us-east-1\nformat:\n  type: json\n  parser:\n    common:\n      table_naming: { type: from_config, name: events }\n    json_parser:\n      conversion_error: dlq\n      unknown_fields: { action: fail }\n      columns:\n        - { jsonpath: '$.id', column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }\n";
    let wrong = common.replace("json_parser", "benchmark_discard");
    assert!(S3SourceConnector::from_config(
        serde_yaml::from_str(&wrong).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_err());
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
        "bucket: test\nformat: { type: parquet, table_name: '', batch_rows: 65536 }\n",
    )
    .unwrap();
    assert!(empty_table.validate().is_err());

    let zero_batch: S3SourceConfig = serde_yaml::from_str(
        "bucket: test\nformat: { type: parquet, table_name: events, batch_rows: 0 }\n",
    )
    .unwrap();
    assert!(zero_batch.validate().is_err());

    let valid: S3SourceConfig = serde_yaml::from_str(
        "bucket: test\nformat: { type: parquet, table_name: events, batch_rows: 65536 }\n",
    )
    .unwrap();
    valid.validate().unwrap();
}
