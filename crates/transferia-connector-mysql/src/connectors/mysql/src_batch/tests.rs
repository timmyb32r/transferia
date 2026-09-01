use mysql_async::Value;

use super::reader::{value_f64, value_i64, value_u64};

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
