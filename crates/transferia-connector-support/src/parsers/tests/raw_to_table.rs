use arrow::array::{BinaryArray, Int64Array, StringArray};
use bytes::Bytes;

use super::*;

fn message(value: &'static [u8]) -> Message {
    Message {
        value: Bytes::from_static(value),
        key: Some(Bytes::from_static(b"\xffkey")),
        headers: vec![
            MessageHeader {
                key: Arc::from("duplicate"),
                value: Some(Bytes::from_static(&[0, 255])),
            },
            MessageHeader {
                key: Arc::from("duplicate"),
                value: None,
            },
        ]
        .into(),
        meta: MessageMeta {
            topic: Some(Arc::from("account/topic")),
            partition: Some(7),
            offset: Some(i64::MAX),
            write_timestamp_ms: Some(1234),
        },
    }
}

#[test]
fn defaults_preserve_binary_key_headers_timestamp_and_i64_offset() -> anyhow::Result<()> {
    let parser = Arc::new(RawToTableParser::new(
        &RawToTableParserConfig::default(),
        Arc::from("events"),
    )?);
    let mut session = parser.create_session(1024 * 1024);
    let (main, dlq) = session.parse_into(vec![message(b"\0\xffvalue")])?;

    assert!(dlq.is_none());
    assert_eq!(main.batch.num_rows(), 1);
    assert_eq!(main.batch.num_columns(), 7);
    assert_eq!(
        main.batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "account/topic"
    );
    assert_eq!(
        main.batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MAX
    );
    assert_eq!(
        main.batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        r#"[{"key":"duplicate","value_base64":"AP8="},{"key":"duplicate","value_base64":null}]"#
    );
    assert_eq!(
        main.batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"\xffkey"
    );
    assert_eq!(
        main.batch
            .column(6)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"\0\xffvalue"
    );
    Ok(())
}

#[test]
fn invalid_typed_value_reaches_lossless_dlq() -> anyhow::Result<()> {
    let config = RawToTableParserConfig {
        value_type: RawValueType::Json,
        ..Default::default()
    };
    let parser = Arc::new(RawToTableParser::new(&config, Arc::from("events"))?);
    let mut session = parser.create_session(1024 * 1024);
    let (main, dlq) = session.parse_into(vec![message(b"not-json")])?;

    assert_eq!(main.batch.num_rows(), 0);
    let dlq = dlq.expect("invalid JSON must reach DLQ");
    assert_eq!(dlq.batch.num_rows(), 1);
    assert_eq!(dlq.batch.num_columns(), 8);
    assert_eq!(
        dlq.batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"\xffkey"
    );
    assert_eq!(
        dlq.batch
            .column(6)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"not-json"
    );
    assert!(dlq
        .batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .contains("not valid JSON"));
    Ok(())
}

#[test]
fn missing_address_metadata_fails_closed() -> anyhow::Result<()> {
    let parser = Arc::new(RawToTableParser::new(
        &RawToTableParserConfig::default(),
        Arc::from("events"),
    )?);
    let mut session = parser.create_session(1024 * 1024);
    let mut input = message(b"value");
    input.meta.offset = None;
    let error = session
        .parse_into(vec![input])
        .err()
        .ok_or_else(|| anyhow::anyhow!("missing offset must fail"))?;
    assert!(error.to_string().contains("offset metadata"), "{error:#}");
    Ok(())
}

#[test]
fn destructive_omissions_are_explicit_and_remove_only_selected_columns() {
    let config = RawToTableParserConfig {
        preserve_key: false,
        preserve_headers: false,
        preserve_write_timestamp: false,
        ..Default::default()
    };
    assert_eq!(
        config
            .dataset_schema()
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["topic", "partition", "offset", "value"]
    );
}
