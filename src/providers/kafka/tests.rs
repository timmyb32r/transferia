use super::{
    config::base_client_config, validate_sink_config, KafkaSaslMechanism, KafkaSecurityConfig,
    KafkaSinkConfig,
};
use crate::serializer::SerializerConfig;

#[test]
fn sink_rejects_whitespace_in_topic() {
    let config = KafkaSinkConfig {
        brokers: vec!["localhost:9092".to_owned()],
        topic: " topic".to_owned(),
        security: KafkaSecurityConfig::Plaintext,
        serializer: SerializerConfig::Json,
        partition: None,
        request_timeout_ms: 30_000,
        max_in_flight: 16,
    };
    assert!(validate_sink_config(&config).is_err());
}

#[test]
fn sasl_tls_security_configures_credentials_and_certificate_verification() -> anyhow::Result<()> {
    let config = base_client_config(
        &["broker.example:9091".to_owned()],
        &KafkaSecurityConfig::SaslTls {
            username: "transfer-user".to_owned(),
            password: "secret".to_owned(),
            mechanism: KafkaSaslMechanism::ScramSha512,
            ca_file: Some("/etc/ssl/internal-ca.crt".to_owned()),
        },
        30_000,
    )?;
    assert_eq!(config.get("security.protocol"), Some("sasl_ssl"));
    assert_eq!(config.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    assert_eq!(config.get("sasl.username"), Some("transfer-user"));
    assert_eq!(config.get("sasl.password"), Some("secret"));
    assert_eq!(
        config.get("ssl.ca.location"),
        Some("/etc/ssl/internal-ca.crt")
    );
    Ok(())
}

#[test]
fn source_and_sink_config_schemas_compile() {
    let source = schemars::schema_for!(super::KafkaSourceConfig);
    let sink = schemars::schema_for!(super::KafkaSinkConfig);
    assert!(serde_json::to_value(source).unwrap().is_object());
    assert!(serde_json::to_value(sink).unwrap().is_object());
}

#[test]
fn tls_security_configures_kafka_certificate_verification() -> anyhow::Result<()> {
    let config = base_client_config(
        &["broker.example:9091".to_owned()],
        &KafkaSecurityConfig::Tls {
            ca_file: Some("/etc/ssl/internal-ca.crt".to_owned()),
        },
        30_000,
    )?;
    assert_eq!(config.get("security.protocol"), Some("ssl"));
    assert_eq!(
        config.get("ssl.ca.location"),
        Some("/etc/ssl/internal-ca.crt")
    );
    Ok(())
}
