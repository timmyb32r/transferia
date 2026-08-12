use super::*;

#[test]
fn parses_configuration() -> anyhow::Result<()> {
    let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\nsorting_key: [id]\n",
    )?)?;
    anyhow::ensure!(config.sorting_key == ["id"]);
    Ok(())
}

#[test]
fn plaintext_transport_requires_an_explicit_trust_acknowledgement() -> anyhow::Result<()> {
    let missing: Value = serde_yaml::from_str("endpoint: localhost:9000\n")?;
    let denied: Value =
        serde_yaml::from_str("endpoint: localhost:9000\ntrusted_plaintext: false\n")?;

    assert!(ClickHouseSinkConfig::from_value(missing).is_err());
    assert!(ClickHouseSinkConfig::from_value(denied).is_err());
    Ok(())
}

#[test]
fn rejects_unknown_or_unsupported_options() {
    for yaml in [
        "endpoint: localhost:9000\ntrusted_plaintext: true\nuse_tls: false\n",
        "endpoint: localhost:9000\ntrusted_plaintext: true\nunexpected_option: true\n",
    ] {
        assert!(serde_yaml::from_str::<ClickHouseSinkConfig>(yaml).is_err());
    }
}

#[test]
fn rejects_duplicate_sorting_key_columns_during_config_validation() {
    let result = ClickHouseSinkConfig::from_value(
        serde_yaml::from_str(
            "endpoint: localhost:9000\ntrusted_plaintext: true\nsorting_key: [id, id]\n",
        )
        .unwrap(),
    );
    assert!(result.is_err());
}

#[test]
fn defaults_to_finite_retries() -> anyhow::Result<()> {
    let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\n",
    )?)?;
    assert_eq!(config.effective_retry_max_attempts(), 20);
    Ok(())
}

#[test]
fn validates_retry_policy() -> anyhow::Result<()> {
    let zero_attempts: Value = serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\nretry_max_attempts: 0\n",
    )?;
    let inverted_backoff: Value = serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\nretry_initial_ms: 20\nretry_max_ms: 10\n",
    )?;
    let zero_connect_timeout: Value = serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\nconnect_timeout_ms: 0\n",
    )?;
    let zero_request_timeout: Value = serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\nrequest_timeout_ms: 0\n",
    )?;

    assert!(ClickHouseSinkConfig::from_value(zero_attempts).is_err());
    assert!(ClickHouseSinkConfig::from_value(inverted_backoff).is_err());
    assert!(ClickHouseSinkConfig::from_value(zero_connect_timeout).is_err());
    assert!(ClickHouseSinkConfig::from_value(zero_request_timeout).is_err());
    Ok(())
}

#[test]
fn rejects_invalid_sorting_key_identifiers() -> anyhow::Result<()> {
    for column in ["", "2id", "nested.id", "id,ts", "ид"] {
        let value: Value = serde_yaml::from_str(&format!(
            "endpoint: localhost:9000\ntrusted_plaintext: true\nsorting_key: [{column:?}]\n"
        ))?;
        assert!(
            ClickHouseSinkConfig::from_value(value).is_err(),
            "sorting key {column:?} must be rejected"
        );
    }
    Ok(())
}

#[test]
fn debug_redacts_password() -> anyhow::Result<()> {
    let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
        "endpoint: localhost:9000\ntrusted_plaintext: true\npassword: super-secret\n",
    )?)?;
    let debug = format!("{config:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("super-secret"));
    Ok(())
}
