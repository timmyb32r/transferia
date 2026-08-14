use std::sync::Arc;

use crate::metrics::MetricsRegistry;

use super::*;

#[test]
fn rejects_missing_order_and_ambiguous_names() {
    for yaml in [
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events, output_name: events, order_by: []}]\n",
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events, output_name: bad-name, order_by: [id]}]\n",
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events, output_name: events, order_by: [id, id]}]\n",
    ] {
        assert!(ClickHouseSourceProvider::from_config(serde_yaml::from_str(yaml).unwrap(), Arc::new(MetricsRegistry::new())).is_err());
    }
}

#[test]
fn requires_explicit_plaintext_trust() {
    let value = serde_yaml::from_str("hosts: [localhost]\nport: 9000\ntrusted_plaintext: false\nusername: default\ntables: [{database: default, name: events, output_name: events, order_by: [id]}]\n").unwrap();
    assert!(
        ClickHouseSourceProvider::from_config(value, Arc::new(MetricsRegistry::new())).is_err()
    );
}
