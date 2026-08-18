use super::{validate_sink_config, KafkaSecurityConfig, KafkaSinkConfig};
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
fn source_and_sink_config_schemas_compile() {
    let source = schemars::schema_for!(super::KafkaSourceConfig);
    let sink = schemars::schema_for!(super::KafkaSinkConfig);
    assert!(serde_json::to_value(source).unwrap().is_object());
    assert!(serde_json::to_value(sink).unwrap().is_object());
}
