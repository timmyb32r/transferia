use super::config::S3InputParser;
use super::preview::preview_first_object;
use super::*;
use crate::metrics::MetricsRegistry;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use schemars::schema_for;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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

#[test]
fn parser_schema_declares_s3_matrix_capabilities() {
    let schema = serde_json::to_value(schema_for!(S3InputParser)).unwrap();
    let variants = schema["oneOf"].as_array().unwrap();

    for (title, key) in [("Parquet parser", "s3_parquet"), ("JSON parser", "s3_json")] {
        let variant = variants
            .iter()
            .find(|variant| variant["title"] == title)
            .unwrap();
        assert_eq!(variant["x-ui"]["capabilities"]["component"], "parser");
        assert_eq!(variant["x-ui"]["capabilities"]["key"], key);
        assert_eq!(
            variant["x-ui"]["capabilities"]["record_semantics"],
            serde_json::json!(["append_only"])
        );
    }
}

#[tokio::test]
async fn preview_reads_a_bounded_prefix_of_the_first_nonempty_object() {
    let store = Arc::new(InMemory::new());
    store
        .put(&Path::from("events/empty.json"), Bytes::new().into())
        .await
        .unwrap();
    store
        .put(
            &Path::from("events/rows.jsonl"),
            Bytes::from_static(b"{\"id\":1}\n{\"id\":2}\n").into(),
        )
        .await
        .unwrap();

    let prefix = Path::from("events");
    let preview = preview_first_object(
        store,
        Some(&prefix),
        Duration::from_secs(1),
        9,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(preview.payload, b"{\"id\":1}\n");
    assert_eq!(preview.detection_payloads, vec![b"{\"id\":1}\n".to_vec()]);
    assert_eq!(preview.metadata.topic, "events/rows.jsonl");
    assert_eq!(preview.metadata.declared_uncompressed_size, Some(18));
    assert_eq!(preview.metadata.compressed_size, 9);
}
