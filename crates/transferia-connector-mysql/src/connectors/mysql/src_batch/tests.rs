use arrow::datatypes::{DataType, TimeUnit};
use mysql_async::Value;

use super::config::{MySqlReadProtocol, MySqlSourceConfig};
use super::connector::MySqlColumnKind;
use super::reader::{
    value_date32, value_f64, value_i64, value_timestamp_micros, value_u64,
};

const MINIMAL_SOURCE_CONFIG: &str = "\
host: db.example
port: 3306
database: transferia
username: reader
password: secret
trusted_plaintext: true
tables:
  - name: events
";

#[test]
fn read_protocol_defaults_to_binary_and_accepts_text_explicitly() -> anyhow::Result<()> {
    let default: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    assert_eq!(default.batch_rows, 16_384);
    assert_eq!(default.read_protocol, MySqlReadProtocol::Binary);

    let text: MySqlSourceConfig = serde_yaml::from_str(&format!(
        "{MINIMAL_SOURCE_CONFIG}read_protocol: text\n"
    ))?;
    assert_eq!(text.read_protocol, MySqlReadProtocol::Text);

    assert!(serde_yaml::from_str::<MySqlSourceConfig>(&format!(
        "{MINIMAL_SOURCE_CONFIG}read_protocol: native\n"
    ))
    .is_err());
    Ok(())
}

#[test]
fn read_protocol_is_a_user_visible_advanced_choice() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig))?;
    let read_protocol = &schema["properties"]["read_protocol"];

    assert_eq!(read_protocol["$ref"], "#/$defs/MySqlReadProtocol");
    assert_eq!(read_protocol["x-ui"]["section"], "advanced");
    assert_eq!(
        schema["$defs"]["MySqlReadProtocol"]["enum"],
        serde_json::json!(["text", "binary"])
    );
    Ok(())
}

#[test]
fn numeric_conversion_accepts_text_and_native_protocol_values() -> anyhow::Result<()> {
    assert_eq!(value_i64::<i8>(&Value::Int(-7))?, -7);
    assert_eq!(value_i64::<i32>(&Value::Bytes(b"42".to_vec()))?, 42);
    assert_eq!(value_u64::<u64>(&Value::UInt(u64::MAX))?, u64::MAX);
    assert_eq!(value_u64::<u16>(&Value::Bytes(b"65535".to_vec()))?, 65_535);
    assert!((value_f64::<f32>(&Value::Float(1.5))? - 1.5).abs() < f32::EPSILON);
    assert!((value_f64::<f64>(&Value::Bytes(b"2.25".to_vec()))? - 2.25).abs() < f64::EPSILON);
    Ok(())
}

#[test]
fn numeric_conversion_rejects_lossy_or_out_of_range_values() {
    assert!(value_i64::<i8>(&Value::Int(128)).is_err());
    assert!(value_u64::<u8>(&Value::Int(-1)).is_err());
    assert!(value_i64::<i64>(&Value::Bytes(b"1.5".to_vec())).is_err());
}

#[test]
fn temporal_discovery_uses_lossless_arrow_types_and_explicit_timestamp_timezone() {
    assert_eq!(MySqlColumnKind::Date.arrow_type(), DataType::Date32);
    assert_eq!(
        MySqlColumnKind::DateTime.arrow_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        MySqlColumnKind::TimestampUtc.arrow_type(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn date_conversion_has_text_binary_parity_across_the_mysql_range() -> anyhow::Result<()> {
    for (year, month, day) in [(1000, 1, 1), (1970, 1, 1), (2024, 2, 29), (9999, 12, 31)] {
        let text = format!("{year:04}-{month:02}-{day:02}");
        assert_eq!(
            value_date32(&Value::Bytes(text.into_bytes()))?,
            value_date32(&Value::Date(year, month, day, 0, 0, 0, 0))?
        );
    }
    assert_eq!(value_date32(&Value::Bytes(b"1970-01-01".to_vec()))?, 0);
    Ok(())
}

#[test]
fn datetime_conversion_has_text_binary_parity_without_precision_loss() -> anyhow::Result<()> {
    let cases = [
        ("1000-01-01 00:00:00", Value::Date(1000, 1, 1, 0, 0, 0, 0)),
        (
            "2024-02-29 12:34:56.1",
            Value::Date(2024, 2, 29, 12, 34, 56, 100_000),
        ),
        (
            "2024-02-29 12:34:56.1234",
            Value::Date(2024, 2, 29, 12, 34, 56, 123_400),
        ),
        (
            "2038-01-19 03:14:07.999999",
            Value::Date(2038, 1, 19, 3, 14, 7, 999_999),
        ),
        (
            "2106-02-07 06:28:15.999999",
            Value::Date(2106, 2, 7, 6, 28, 15, 999_999),
        ),
        (
            "9999-12-31 23:59:59.999999",
            Value::Date(9999, 12, 31, 23, 59, 59, 999_999),
        ),
    ];
    for (text, binary) in cases {
        assert_eq!(
            value_timestamp_micros(&Value::Bytes(text.as_bytes().to_vec()))?,
            value_timestamp_micros(&binary)?,
            "text/binary mismatch for {text}"
        );
    }
    Ok(())
}

#[test]
fn temporal_conversion_rejects_zero_partial_and_invalid_values() {
    for value in [
        Value::Bytes(b"0000-00-00".to_vec()),
        Value::Bytes(b"2024-00-01".to_vec()),
        Value::Bytes(b"2024-01-00".to_vec()),
        Value::Bytes(b"2023-02-29".to_vec()),
        Value::Bytes(b"0999-12-31".to_vec()),
        Value::Date(0, 0, 0, 0, 0, 0, 0),
        Value::Date(2024, 2, 29, 1, 0, 0, 0),
    ] {
        assert!(value_date32(&value).is_err(), "unexpected valid DATE: {value:?}");
    }

    for value in [
        Value::Bytes(b"0000-00-00 00:00:00".to_vec()),
        Value::Bytes(b"2024-02-29T12:34:56".to_vec()),
        Value::Bytes(b"2024-02-29 24:00:00".to_vec()),
        Value::Bytes(b"2024-02-29 12:60:00".to_vec()),
        Value::Bytes(b"2024-02-29 12:34:60".to_vec()),
        Value::Bytes(b"2024-02-29 12:34:56.".to_vec()),
        Value::Bytes(b"2024-02-29 12:34:56.1234567".to_vec()),
        Value::Date(2024, 2, 29, 12, 34, 56, 1_000_000),
    ] {
        assert!(
            value_timestamp_micros(&value).is_err(),
            "unexpected valid DATETIME/TIMESTAMP: {value:?}"
        );
    }
}
