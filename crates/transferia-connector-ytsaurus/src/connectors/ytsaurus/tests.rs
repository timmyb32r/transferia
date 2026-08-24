use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use super::config::{
    YTsaurusReadFormat, YTsaurusSinkConfig, YTsaurusSourceConfig,
    YTsaurusTableReaderConfig, YTsaurusWriteFormat,
};
use super::client::rich_read_path;
use super::discard::{DiscardDecoder, output_format};
use super::schema::{parse_schema, schema_to_yt};
use super::sink::{encode_arrow, encode_yson, validate_row_weight};
use super::src_batch::validate_read_schema;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME,
};

#[test]
fn auth_uses_the_wide_credentials_control() {
    let schema = serde_json::to_value(schemars::schema_for!(YTsaurusSourceConfig))
        .expect("YTsaurus source schema must serialize");
    assert_eq!(
        schema
            .pointer("/properties/auth/x-ui/control_width")
            .and_then(serde_json::Value::as_str),
        Some("auth"),
    );
}

#[test]
fn source_tables_require_explicit_unique_logical_names() -> anyhow::Result<()> {
    let source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntables:\n  - name: events\n    path: //tmp/input\n",
    )?;
    source.validate()?;
    let tls_source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: false\ntables:\n  - name: events\n    path: //tmp/input\n"
    )?;
    tls_source.validate()?;
    assert_eq!(tls_source.connection.endpoint(), "https://localhost:8000");
    assert!(serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntables:\n  - path: //tmp/input\n"
    )
    .is_err());
    assert!(serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntables:\n  - { name: events, path: //tmp/a }\n  - { name: events, path: //tmp/b }\n"
    )?
    .validate()
    .is_err());
    assert!(serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: relative }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n"
    )?
    .validate()
    .is_err());
    Ok(())
}

#[test]
fn snapshot_recovery_materializes_an_exact_row_range() {
    assert_eq!(rich_read_path("//tmp/input", 0), "//tmp/input");
    assert_eq!(
        rich_read_path("//tmp/input", 42_971_400),
        "<ranges=[{lower_limit={row_index=42971400}}]>//tmp/input"
    );
}

#[test]
fn arrow_is_the_default_sink_format() -> anyhow::Result<()> {
    let config = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n",
    )?;
    assert_eq!(config.format(), YTsaurusWriteFormat::Arrow);
    assert_eq!(config.path_for_dataset("events")?, "//tmp/output/events");
    Ok(())
}

#[test]
fn schema_round_trip_and_writers_are_native() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("name".into(), DataType::Utf8, true),
    ]);
    let encoded = schema_to_yt(&schema)?;
    let response = serde_json::json!({
        "$attributes": { "strict": true },
        "$value": encoded
    });
    let parsed = parse_schema(response)?;
    assert_eq!(parsed.columns.len(), 2);
    assert_eq!(parsed.columns[0].data_type, DataType::Int64);
    assert_eq!(parsed.columns[1].data_type, DataType::Utf8);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alice"), None])) as ArrayRef,
        ],
    )?;
    validate_row_weight(&batch)?;
    assert!(!encode_arrow(&batch)?.is_empty());
    assert_eq!(
        encode_yson(&batch)?,
        b"{\"id\"=1;\"name\"=\"alice\";};{\"id\"=2;\"name\"=#;};"
    );
    Ok(())
}

#[test]
fn arrow_writer_strips_extension_annotations_from_the_ytsaurus_wire_schema() -> anyhow::Result<()> {
    let field = Field::new("payload", DataType::Utf8, false).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_owned(),
        ARROW_JSON_EXTENSION_NAME.to_owned(),
    )]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StringArray::from(vec!["{}"] as Vec<&str>)) as ArrayRef],
    )?;

    let encoded = encode_arrow(&batch)?;
    let mut reader = StreamReader::try_new(Cursor::new(encoded), None)?;
    let decoded = reader.next().expect("one Arrow batch")?;
    assert_eq!(decoded.column(0), batch.column(0));
    assert_eq!(decoded.schema().field(0).metadata().get("ARROW:extension:name"), None);
    Ok(())
}

#[test]
fn unsupported_types_and_invalid_names_fail_during_validation() {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "@internal".into(),
        DataType::Decimal128(20, 2),
        false,
    )]);
    assert!(schema_to_yt(&schema).is_err());
}

#[test]
fn source_rejects_read_type_or_nullability_drift_instead_of_casting() -> anyhow::Result<()> {
    let expected = DatasetSchema::new(vec![SchemaColumn::new("id".into(), DataType::Int64, false)]);
    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["1"])) as ArrayRef],
    )?;
    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
    )?;

    assert!(validate_read_schema(&wrong_type, &expected).is_err());
    assert!(validate_read_schema(&nullable, &expected).is_err());
    Ok(())
}

#[test]
fn benchmark_format_descriptors_are_valid_header_json() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("name".into(), DataType::Utf8, false),
        SchemaColumn::new("count".into(), DataType::Int64, true),
    ]);
    for format in [
        YTsaurusReadFormat::Arrow,
        YTsaurusReadFormat::Skiff,
        YTsaurusReadFormat::SchemafulDsv,
        YTsaurusReadFormat::YsonBinary,
        YTsaurusReadFormat::YsonText,
        YTsaurusReadFormat::Json,
    ] {
        let descriptor = output_format(format, &schema)?;
        let parsed = serde_json::from_str::<serde_json::Value>(&descriptor)?;
        assert!(parsed.is_string() || parsed.get("$value").is_some());
    }
    Ok(())
}

#[test]
fn benchmark_discard_counters_survive_arbitrary_chunk_boundaries() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("name".into(), DataType::Utf8, false),
        SchemaColumn::new("count".into(), DataType::Int64, true),
    ]);

    let arrow_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        ],
    )?;
    let arrow_wire = encode_arrow(&arrow_batch)?;
    let mut arrow = DiscardDecoder::new(YTsaurusReadFormat::Arrow, &schema)?;
    let mut arrow_rows = 0;
    for byte in arrow_wire {
        arrow_rows += arrow.decode(bytes::Bytes::from(vec![byte]))?;
    }
    arrow_rows += arrow.finish()?;
    assert_eq!(arrow_rows, 2);

    let mut yson = DiscardDecoder::new(YTsaurusReadFormat::YsonText, &schema)?;
    assert_eq!(yson.decode(bytes::Bytes::from_static(b"{\"name\"=\"a"))?, 0);
    assert_eq!(yson.decode(bytes::Bytes::from_static(b"\";};{\"name\"=\"b\";};"))?, 2);
    assert_eq!(yson.finish()?, 0);

    let mut skiff = DiscardDecoder::new(YTsaurusReadFormat::Skiff, &schema)?;
    let mut wire = Vec::new();
    wire.extend_from_slice(&0_u16.to_le_bytes());
    wire.extend_from_slice(&3_u32.to_le_bytes());
    wire.extend_from_slice(b"one");
    wire.push(1);
    wire.extend_from_slice(&42_i64.to_le_bytes());
    wire.extend_from_slice(&0_u16.to_le_bytes());
    wire.extend_from_slice(&3_u32.to_le_bytes());
    wire.extend_from_slice(b"two");
    wire.push(0);
    assert_eq!(skiff.decode(bytes::Bytes::copy_from_slice(&wire[..7]))?, 0);
    assert_eq!(skiff.decode(bytes::Bytes::copy_from_slice(&wire[7..]))?, 2);
    assert_eq!(skiff.finish()?, 0);
    Ok(())
}

#[test]
fn benchmark_table_reader_validates_effective_server_limits() {
    assert!(YTsaurusTableReaderConfig {
        window_size: Some(64 * 1024 * 1024),
        ..YTsaurusTableReaderConfig::default()
    }
    .validate()
    .is_err());
    assert!(YTsaurusTableReaderConfig {
        group_size: Some(32 * 1024 * 1024),
        ..YTsaurusTableReaderConfig::default()
    }
    .validate()
    .is_err());
}
