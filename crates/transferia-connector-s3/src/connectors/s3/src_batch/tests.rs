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

fn sample_limits() -> transferia_registry::TableSampleLimits {
    transferia_registry::TableSampleLimits { row_limit: 20, max_bytes: 16 * 1024 * 1024, timeout_ms: 30_000 }
}

fn sample_json_config() -> S3SourceConfig {
    serde_yaml::from_str("bucket: test\npath_prefix: events\ntable_name: configured_name\nparser:\n  type: json\n  common: {}\n  json_parser:\n    json_framing: json_lines\n    conversion_error: fail\n    unknown_fields: { action: fail }\n    columns:\n      - { jsonpath: '$.id', column_name: configured_id, json_data_type: number, arrow_type: Int64, nullable: false }\n").unwrap()
}

#[tokio::test]
async fn data_sample_applies_configured_s3_json_parser_and_preserves_integer_precision() -> anyhow::Result<()> {
    use transferia_registry::SourceConnector as _;
    let config = sample_json_config();
    let source = S3SourceConnector::from_config(config.clone(), Arc::new(MetricsRegistry::new()))?;
    let store = Arc::new(InMemory::new());
    store.put(&Path::from("events/a.json"), Bytes::from_static(b"{\"id\":9007199254740993}\n").into()).await?;
    let tables = super::preview::sample_data(&config, store, source.parser(), sample_limits(), CancellationToken::new()).await?;
    assert_eq!(&*tables[0].table, "configured_name");
    assert_eq!(tables[0].batch.schema().field(0).name(), "configured_id");
    assert_eq!(tables[0].batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 9_007_199_254_740_993);
    Ok(())
}

#[tokio::test]
async fn data_sample_rejects_an_oversized_object_instead_of_parsing_a_valid_prefix() -> anyhow::Result<()> {
    use transferia_registry::SourceConnector as _;
    let config = sample_json_config();
    let source = S3SourceConnector::from_config(config.clone(), Arc::new(MetricsRegistry::new()))?;
    let store = Arc::new(InMemory::new());
    store.put(&Path::from("events/a.json"), Bytes::from_static(b"{\"id\":1}\n{\"id\":2}\n").into()).await?;
    let result = super::preview::sample_data(&config, store, source.parser(), transferia_registry::TableSampleLimits {
        max_bytes: 9, ..sample_limits()
    }, CancellationToken::new()).await;
    assert!(result.unwrap_err().to_string().contains("max_sample_bytes"));
    Ok(())
}

#[tokio::test]
async fn data_sample_reads_native_parquet_with_the_requested_row_limit() -> anyhow::Result<()> {
    use transferia_registry::SourceConnector as _;
    let config: S3SourceConfig = serde_yaml::from_str("bucket: test\ntable_name: parquet_data\nparser: { type: parquet }\n")?;
    let source = S3SourceConnector::from_config(config.clone(), Arc::new(MetricsRegistry::new()))?;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))])?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    let bytes = writer.into_inner()?;
    let store = Arc::new(InMemory::new());
    store.put(&Path::from("first.parquet"), Bytes::from(bytes).into()).await?;
    let tables = super::preview::sample_data(&config, store, source.parser(), transferia_registry::TableSampleLimits {
        row_limit: 1, ..sample_limits()
    }, CancellationToken::new()).await?;
    assert_eq!(&*tables[0].table, "parquet_data");
    assert_eq!(tables[0].batch, batch.slice(0, 1));
    Ok(())
}

#[tokio::test]
async fn data_sample_rejects_parquet_expansion_before_decoding_rows() -> anyhow::Result<()> {
    use transferia_registry::SourceConnector as _;
    let config: S3SourceConfig = serde_yaml::from_str("bucket: test\ntable_name: parquet_data\nparser: { type: parquet }\n")?;
    let source = S3SourceConnector::from_config(config.clone(), Arc::new(MetricsRegistry::new()))?;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Utf8, false)]));
    let value = "x".repeat(512 * 1024);
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arrow::array::StringArray::from(vec![value.as_str()]))])?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::GZIP(Default::default())).build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
    writer.write(&batch)?;
    let bytes = writer.into_inner()?;
    let budget = 64 * 1024;
    assert!(bytes.len() < budget);
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone()))?;
    let limits = transferia_registry::TableSampleLimits { row_limit: 1, max_bytes: budget, ..sample_limits() };
    // The preflight itself rejects this footer before reader.next() can allocate
    // the expanded string; the public sample path must preserve that rejection.
    assert!(super::preview::admit_parquet_sample(builder.metadata(), bytes.len(), limits).is_err());
    let store = Arc::new(InMemory::new());
    store.put(&Path::from("first.parquet"), Bytes::from(bytes).into()).await?;
    assert!(super::preview::sample_data(&config, store, source.parser(), limits, CancellationToken::new()).await.is_err());
    Ok(())
}
