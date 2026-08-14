use super::config::PostgresSourceConfig;

#[test]
fn source_config_requires_explicit_plaintext_trust_and_tables() {
    let config: PostgresSourceConfig =
        serde_yaml::from_str("connection: host=localhost\ntrusted_plaintext: false\ntables: []\n")
            .unwrap();
    assert!(config.validate().is_err());
}
