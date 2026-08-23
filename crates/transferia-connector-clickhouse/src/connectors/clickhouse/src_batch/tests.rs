use std::sync::Arc;

use crate::metrics::MetricsRegistry;

use super::*;

#[test]
fn source_contract_has_no_shard_group() {
    let schema = serde_json::to_value(schemars::schema_for!(ClickHouseSourceConfig))
        .expect("ClickHouse source schema must serialize");
    assert!(schema.pointer("/properties/shard_group").is_none());

    let legacy = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\nshard_group: legacy\ntables: [{database: default, name: events}]\n";
    assert!(serde_yaml::from_str::<ClickHouseSourceConfig>(legacy).is_err());
}

#[test]
fn derives_output_name_from_the_source_table() {
    let yaml = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events}]\n";
    assert!(ClickHouseSourceConnector::from_config(
        serde_yaml::from_str(yaml).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_ok());

    let invalid = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: bad-name}]\n";
    assert!(ClickHouseSourceConnector::from_config(
        serde_yaml::from_str(invalid).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_err());
}

#[test]
fn supports_verified_tls() {
    let value = serde_yaml::from_str("hosts: [localhost]\nport: 9440\ntrusted_plaintext: false\ntls_ca_file: /tmp/ca.pem\nusername: default\ntables: [{database: default, name: events}]\n").unwrap();
    assert!(
        ClickHouseSourceConnector::from_config(value, Arc::new(MetricsRegistry::new())).is_ok()
    );
}
