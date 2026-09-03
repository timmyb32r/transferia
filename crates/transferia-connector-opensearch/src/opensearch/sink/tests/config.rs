use super::super::{initial_config, OpenSearchSinkConfig, RoutedIdentity};

#[test]
fn initial_configuration_is_complete_and_lossless() {
    let mut value = initial_config();
    value["hosts"] = serde_json::json!(["example.test"]);
    value["auth"]["username"] = "writer".into();
    let config: OpenSearchSinkConfig = serde_json::from_value(value).unwrap();
    assert_eq!(config.routed_identity, RoutedIdentity::Fail);
    assert!(config.create_indices);
    assert!(config.bulk_target_rows > 0);
    assert!(config.bulk_target_bytes > 0);
    assert!(config.bulk_concurrency > 0);
    assert!(config.retry_max_attempts > 0);
    config.validate().unwrap();
}

#[test]
fn invalid_operational_limits_fail_before_io() {
    let mut value = initial_config();
    value["bulk_target_rows"] = 0.into();
    let config: OpenSearchSinkConfig = serde_json::from_value(value).unwrap();
    assert!(config.validate().is_err());

    let mut value = initial_config();
    value["bulk_concurrency"] = 33.into();
    let config: OpenSearchSinkConfig = serde_json::from_value(value).unwrap();
    assert!(config.validate().is_err());

    let mut value = initial_config();
    value["retry_initial_ms"] = 2_000.into();
    value["retry_max_ms"] = 1_000.into();
    let config: OpenSearchSinkConfig = serde_json::from_value(value).unwrap();
    assert!(config.validate().is_err());
}
