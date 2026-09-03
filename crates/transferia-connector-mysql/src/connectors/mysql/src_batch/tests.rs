use mysql_async::Value;

use super::config::{MySqlReadProtocol, MySqlSourceConfig};
use super::reader::{value_f64, value_i64, value_u64};

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
