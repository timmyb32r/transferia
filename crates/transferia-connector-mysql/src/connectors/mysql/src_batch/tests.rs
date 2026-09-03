use std::mem::size_of;

use arrow::array::Array;
use arrow::datatypes::{DataType, TimeUnit};
use mysql_async::{DriverError, Value};
use transferia_core::failure::FailureDisposition;

use super::config::{
    MySqlReadProtocol, MySqlSourceConfig, DEFAULT_MYSQL_BATCH_TARGET_BYTES,
    DEFAULT_MYSQL_MAX_ROW_BYTES, MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES,
};
use super::connector::{ColumnPlan, MySqlColumnKind};
use super::reader::{
    column_array, estimate_arrow_working_set_bytes, max_decoded_row_admission_bytes,
    next_snapshot_rows_capacity, optional_value_column_array, retained_row_value_heap_bytes,
    retained_rows_heap_bytes, should_read_snapshot_row, snapshot_row_error, value_date32,
    value_f64, value_i64, value_timestamp_micros, value_u64,
    validate_snapshot_batch_growth, validate_snapshot_memory_limits,
};
use crate::connectors::mysql::src_stream::validate_replication_column_plan;

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
    assert_eq!(default.batch_target_bytes, DEFAULT_MYSQL_BATCH_TARGET_BYTES);
    assert_eq!(default.max_row_bytes, DEFAULT_MYSQL_MAX_ROW_BYTES);
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
fn snapshot_memory_limits_are_visible_and_validated_before_execution() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig))?;
    let target = &schema["properties"]["batch_target_bytes"];
    assert_eq!(target["minimum"], 1);
    assert_eq!(target["maximum"], MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES);
    let row = &schema["properties"]["max_row_bytes"];
    assert_eq!(row["minimum"], 1_024);
    assert_eq!(row["maximum"], 1_073_741_824_u64);

    let mut config: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    config.batch_target_bytes = 0;
    assert!(config.validate().is_err());
    config.batch_target_bytes = MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES + 1;
    assert!(config.validate().is_err());
    config.batch_target_bytes = DEFAULT_MYSQL_BATCH_TARGET_BYTES;
    config.max_row_bytes = 1_023;
    assert!(config.validate().is_err());
    config.max_row_bytes = 1_073_741_825;
    assert!(config.validate().is_err());
    validate_snapshot_memory_limits(
        16_384,
        DEFAULT_MYSQL_BATCH_TARGET_BYTES,
        DEFAULT_MYSQL_MAX_ROW_BYTES,
    )?;
    assert!(validate_snapshot_memory_limits(0, 1, 1_024).is_err());
    assert!(validate_snapshot_memory_limits(1, 0, 1_024).is_err());
    assert!(validate_snapshot_memory_limits(1, 1, 1_023).is_err());
    Ok(())
}

#[test]
fn snapshot_batch_stops_after_one_indivisible_target_overshoot() -> anyhow::Result<()> {
    assert!(should_read_snapshot_row(0, 0, 10, 100));
    assert!(should_read_snapshot_row(1, 99, 10, 100));
    assert!(!should_read_snapshot_row(2, 100, 10, 100));
    assert!(!should_read_snapshot_row(10, 99, 10, 100));
    validate_snapshot_batch_growth(99, 180, 100)?;
    assert!(validate_snapshot_batch_growth(100, 180, 100).is_err());
    assert!(validate_snapshot_batch_growth(99, 99, 100).is_err());
    assert_eq!(next_snapshot_rows_capacity(0, 0)?, 4);
    assert_eq!(next_snapshot_rows_capacity(3, 4)?, 4);
    assert_eq!(next_snapshot_rows_capacity(4, 4)?, 8);
    Ok(())
}

#[test]
fn snapshot_row_heap_uses_retained_vector_and_payload_capacities() -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(b"payload");
    let mut row = Vec::with_capacity(8);
    row.push(Some(Value::Bytes(payload)));
    let row_bytes = retained_row_value_heap_bytes(&row)?;
    assert_eq!(row_bytes, 8 * size_of::<Option<Value>>() + 64);
    assert_eq!(retained_rows_heap_bytes(3, row_bytes)?, 3 * size_of::<Vec<Option<Value>>>() + row_bytes);
    Ok(())
}

#[test]
fn low_wire_limit_pre_admits_high_column_decoded_row_overhead() -> anyhow::Result<()> {
    let admission = max_decoded_row_admission_bytes(1_024, 1_000)?;
    assert_eq!(
        admission,
        1_024 + 1_000 * size_of::<Option<Value>>() + size_of::<mysql_async::Row>()
    );
    assert!(admission > 1_024);
    Ok(())
}

#[test]
fn arrow_working_set_is_derived_from_rows_schema_and_payload() -> anyhow::Result<()> {
    let columns = vec![
        test_column("id", MySqlColumnKind::UInt64, "bigint unsigned", None),
        test_column("body", MySqlColumnKind::Utf8, "varchar(255)", Some("utf8mb4")),
    ];
    let rows = vec![vec![
        Some(Value::UInt(7)),
        Some(Value::Bytes(vec![b'x'; 4_096])),
    ]];
    let estimate = estimate_arrow_working_set_bytes(&rows, &columns, None)?;
    assert!(estimate > 4_096);
    Ok(())
}

#[test]
fn optional_value_slice_conversion_does_not_require_row_reconstruction() -> anyhow::Result<()> {
    let column = test_column("value", MySqlColumnKind::UInt64, "bigint unsigned", None);
    let present = vec![Some(Value::UInt(7))];
    let rows = [Some(present.as_slice()), None];
    let array = optional_value_column_array(&rows, 0, &column)?;
    assert_eq!(array.len(), 2);
    assert_eq!(array.null_count(), 1);
    Ok(())
}

#[test]
fn snapshot_packet_limit_failure_is_fatal_but_transport_failure_retries() {
    let packet = snapshot_row_error(DriverError::PacketTooLarge.into(), 1_024);
    assert_eq!(packet.disposition(), FailureDisposition::Fatal);
    assert_eq!(
        packet.to_string(),
        "MySQL snapshot row exceeds configured max_row_bytes=1024"
    );

    let transport = snapshot_row_error(
        mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "fixture reset",
        ))),
        1_024,
    );
    assert_eq!(transport.disposition(), FailureDisposition::Retryable);
}

#[test]
fn replication_rejects_unsupported_physical_types_while_snapshot_conversion_accepts_them() {
    let cases = [
        (
            "json",
            MySqlColumnKind::Json,
            Some("utf8mb4"),
            Value::Bytes(br#"{"k":1}"#.to_vec()),
        ),
        (
            "timestamp(6)",
            MySqlColumnKind::TimestampUtc,
            None,
            Value::Date(2024, 1, 2, 3, 4, 5, 6),
        ),
        (
            "time(6)",
            MySqlColumnKind::Utf8,
            None,
            Value::Bytes(b"12:34:56.000006".to_vec()),
        ),
        (
            "enum('a','b')",
            MySqlColumnKind::Utf8,
            Some("utf8mb4"),
            Value::Bytes(b"a".to_vec()),
        ),
        (
            "set('a','b')",
            MySqlColumnKind::Utf8,
            Some("utf8mb4"),
            Value::Bytes(b"a,b".to_vec()),
        ),
        (
            "year",
            MySqlColumnKind::Utf8,
            None,
            Value::Bytes(b"2024".to_vec()),
        ),
    ];
    for (column_type, kind, character_set, value) in cases {
        let column = test_column("value", kind, column_type, character_set);
        assert!(
            validate_replication_column_plan(&column).is_err(),
            "replication unexpectedly accepted {column_type}"
        );
        let array = column_array(&[vec![Some(value)]], 0, &column).unwrap();
        assert_eq!(array.len(), 1, "snapshot rejected {column_type}");
    }
}

#[test]
fn replication_character_set_validation_is_exact_and_snapshot_remains_available() {
    for character_set in ["ascii", "utf8mb3", "utf8mb4"] {
        let column = test_column("value", MySqlColumnKind::Utf8, "varchar(8)", Some(character_set));
        validate_replication_column_plan(&column).unwrap();
    }
    let latin1 = test_column("value", MySqlColumnKind::Utf8, "varchar(8)", Some("latin1"));
    assert!(validate_replication_column_plan(&latin1).is_err());
    let array = column_array(&[vec![Some(Value::Bytes(b"text".to_vec()))]], 0, &latin1).unwrap();
    assert_eq!(array.len(), 1);

    let mut missing_numeric_collation =
        test_column("value", MySqlColumnKind::Utf8, "varchar(8)", Some("utf8mb4"));
    missing_numeric_collation.collation_id = None;
    assert!(validate_replication_column_plan(&missing_numeric_collation).is_err());
    let array = column_array(
        &[vec![Some(Value::Bytes(b"text".to_vec()))]],
        0,
        &missing_numeric_collation,
    )
    .unwrap();
    assert_eq!(array.len(), 1);
}

fn test_column(
    name: &str,
    kind: MySqlColumnKind,
    column_type: &str,
    character_set: Option<&str>,
) -> ColumnPlan {
    ColumnPlan {
        name: name.to_owned(),
        kind,
        nullable: true,
        primary_key: false,
        max_length: None,
        expression: format!("`{name}`"),
        column_type: column_type.to_owned(),
        character_set: character_set.map(str::to_owned),
        collation: character_set.map(|charset| format!("{charset}_fixture")),
        collation_id: character_set.map(|_| 255),
        extra: String::new(),
        generation_expression: Some(String::new()),
        primary_key_ordinal: None,
        primary_key_prefix_length: None,
        primary_key_direction: None,
    }
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
fn source_schema_declares_snapshot_and_replication_capability_overrides() {
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig)).unwrap();
    assert_eq!(
        schema.pointer("/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source",
            "key": "snapshot",
            "delivery_modes": ["batch"],
            "record_semantics": ["append_only"]
        }))
    );
    assert_eq!(
        schema.pointer("/$defs/MySqlReplicationConfig/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source",
            "key": "replication",
            "delivery_modes": ["stream", "batch_and_stream"],
            "record_semantics": ["changelog"]
        }))
    );
    for (property, minimum) in [
        ("server_id", 1),
        ("max_events", 1),
        ("max_transaction_bytes", 19),
        ("poll_interval_ms", 1),
        ("bootstrap_timeout_ms", 1),
    ] {
        assert_eq!(
            schema.pointer(&format!(
                "/$defs/MySqlReplicationConfig/properties/{property}/minimum"
            )),
            Some(&serde_json::json!(minimum)),
            "{property} did not expose its positive backend constraint"
        );
    }
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
