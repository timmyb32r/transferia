use arrow::datatypes::{DataType, TimeUnit};

use super::connector::{decimal_sql_type, mysql_sql_type};
use super::writer::{date_text, decimal_text, timestamp_text};
use transferia_core::data::schema::{SchemaColumn, ARROW_JSON_EXTENSION_NAME};

fn column(data_type: DataType) -> SchemaColumn {
    SchemaColumn::new("value".to_owned(), data_type, false)
}

#[test]
fn maps_lossless_mysql_column_types() {
    assert_eq!(mysql_sql_type(&column(DataType::UInt64)).unwrap(), "BIGINT UNSIGNED");
    assert_eq!(
        mysql_sql_type(&column(DataType::Decimal128(65, 30))).unwrap(),
        "DECIMAL(65,30)"
    );
    assert_eq!(
        mysql_sql_type(&column(DataType::Timestamp(TimeUnit::Nanosecond, None))).unwrap(),
        "DATETIME(6)"
    );
    assert_eq!(
        mysql_sql_type(
            &column(DataType::Utf8).with_arrow_extension(ARROW_JSON_EXTENSION_NAME)
        )
        .unwrap(),
        "JSON"
    );
}

#[test]
fn rejects_types_mysql_cannot_preserve() {
    assert!(decimal_sql_type(66, 0).is_err());
    assert!(decimal_sql_type(38, 31).is_err());
    assert!(mysql_sql_type(&column(DataType::Timestamp(
        TimeUnit::Microsecond,
        Some("Europe/Moscow".into()),
    )))
    .is_err());
    assert!(mysql_sql_type(
        &column(DataType::Utf8).with_constraints(true, false, None)
    )
    .is_err());
}

#[test]
fn renders_decimal_values_without_rounding() {
    assert_eq!(decimal_text("12345", 2), "123.45");
    assert_eq!(decimal_text("-12", 4), "-0.0012");
    assert_eq!(decimal_text("12", -3), "12000");
}

#[test]
fn temporal_conversion_is_exact_and_bounded() {
    assert_eq!(date_text(19_782).unwrap(), "2024-02-29");
    assert_eq!(
        timestamp_text(1_709_210_096_123_456, TimeUnit::Microsecond).unwrap(),
        "2024-02-29 12:34:56.123456"
    );
    assert!(timestamp_text(1, TimeUnit::Nanosecond).is_err());
    assert!(date_text(-400_000).is_err());
}
