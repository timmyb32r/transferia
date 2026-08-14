use super::config::PostgresSourceConfig;

#[test]
fn source_config_requires_explicit_plaintext_trust_and_tables() {
    let config: PostgresSourceConfig =
        serde_yaml::from_str("host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: false\ntables: []\n").unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn source_rejects_the_old_connection_string() {
    assert!(serde_yaml::from_str::<PostgresSourceConfig>(
        "connection: host=localhost port=5432\ntrusted_plaintext: true\ntables: []\n"
    )
    .is_err());
}
