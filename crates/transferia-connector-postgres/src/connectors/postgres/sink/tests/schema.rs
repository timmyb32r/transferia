use super::validate_type;
use arrow::datatypes::{DataType, TimeUnit};
use tokio_postgres::types::Type;

fn numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + 4
}

#[test]
fn copy_rejects_text_as_numeric_for_binary_and_text_formats() {
    assert!(validate_type(&DataType::Utf8, Type::NUMERIC.oid(), numeric_typmod(20, 0)).is_err());
    assert!(validate_type(&DataType::Utf8, Type::TEXT.oid(), -1).is_ok());
    assert!(validate_type(&DataType::Utf8, Type::VARCHAR.oid(), -1).is_ok());
    assert!(validate_type(&DataType::Utf8, Type::VARCHAR.oid(), 14).is_err());
    assert!(validate_type(&DataType::Utf8, Type::BPCHAR.oid(), 14).is_err());
    assert!(validate_type(&DataType::UInt64, Type::INT8.oid(), -1).is_err());
}

#[test]
fn numeric_destination_must_preserve_scale_and_entire_integer_domain() {
    let source = DataType::Decimal128(20, 4);
    for (precision, scale) in [(20, 4), (22, 6), (38, 4)] {
        assert!(validate_type(&source, Type::NUMERIC.oid(), numeric_typmod(precision, scale)).is_ok());
    }
    for (precision, scale) in [(20, 2), (20, 6), (19, 4)] {
        assert!(validate_type(&source, Type::NUMERIC.oid(), numeric_typmod(precision, scale)).is_err());
    }
    assert!(validate_type(&source, Type::NUMERIC.oid(), -1).is_ok());
    assert!(validate_type(&DataType::UInt64, Type::NUMERIC.oid(), numeric_typmod(20, 0)).is_ok());
    assert!(validate_type(&DataType::UInt64, Type::NUMERIC.oid(), numeric_typmod(19, 0)).is_err());
    assert!(validate_type(&DataType::Decimal128(4, -3), Type::NUMERIC.oid(), numeric_typmod(4, -3)).is_ok());
    assert!(validate_type(&DataType::Decimal128(4, -3), Type::NUMERIC.oid(), numeric_typmod(4, -4)).is_err());
}

#[test]
fn timestamp_destination_must_not_round_or_change_timezone_semantics() {
    let source = DataType::Timestamp(TimeUnit::Microsecond, None);
    assert!(validate_type(&source, Type::TIMESTAMP.oid(), 6).is_ok());
    assert!(validate_type(&source, Type::TIMESTAMP.oid(), -1).is_ok());
    assert!(validate_type(&source, Type::TIMESTAMP.oid(), 3).is_err());
    assert!(validate_type(&source, Type::TIMESTAMPTZ.oid(), 6).is_err());
}
