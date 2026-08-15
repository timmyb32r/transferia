use super::*;
use crate::metrics::MetricsRegistry;
use std::sync::Arc;

#[test]
fn source_requires_json_parser_and_normalized_prefix() {
    let common = "bucket: test\nregion: us-east-1\nallow_http: false\nparser:\n  common:\n    table_naming: { type: from_config, name: events }\n  json_parser:\n    conversion_error: dlq\n    unknown_fields: { action: fail }\n    columns:\n      - { jsonpath: '$.id', column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }\n";
    let wrong = common.replace("json_parser", "benchmark_discard");
    assert!(S3SourceProvider::from_config(
        serde_yaml::from_str(&wrong).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_err());
    let bad_prefix = format!("prefix: /bad\n{common}");
    assert!(S3SourceProvider::from_config(
        serde_yaml::from_str(&bad_prefix).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_err());
}
