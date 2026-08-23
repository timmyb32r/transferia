use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::config::{YTsaurusSinkConfig, YTsaurusSourceConfig, YTsaurusWriteFormat};
use super::schema::{parse_schema, schema_to_yt};
use super::sink::{encode_arrow, encode_yson, validate_row_weight};
use super::src_batch::validate_read_schema;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

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
fn configs_derive_transport_and_use_paths_as_table_names() -> anyhow::Result<()> {
    let source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntables:\n  - path: //tmp/input\n",
    )?;
    source.validate()?;
    let tls_source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: false\ntables:\n  - path: //tmp/input\n"
    )?;
    tls_source.validate()?;
    assert_eq!(tls_source.connection.endpoint(), "https://localhost:8000");
    assert!(serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: relative }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n"
    )?
    .validate()
    .is_err());
    Ok(())
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
