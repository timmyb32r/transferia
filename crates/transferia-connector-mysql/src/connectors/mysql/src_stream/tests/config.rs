use super::super::MySqlReplicationConfig;

#[test]
fn replication_defaults_are_explicit_but_server_id_is_required() {
    let config: MySqlReplicationConfig = serde_json::from_value(serde_json::json!({
        "server_id": 42
    }))
    .unwrap();
    assert_eq!(config.server_id, 42);
    assert_eq!(config.max_events, 4_096);
    assert_eq!(config.max_transaction_bytes, 64 * 1024 * 1024);
    assert_eq!(config.poll_interval_ms, 100);
    assert_eq!(config.bootstrap_timeout_ms, 30_000);
    config.validate().unwrap();

    assert!(serde_json::from_value::<MySqlReplicationConfig>(serde_json::json!({})).is_err());
}

#[test]
fn every_replication_limit_must_satisfy_its_protocol_minimum() {
    let valid = MySqlReplicationConfig {
        server_id: 1,
        max_events: 1,
        max_transaction_bytes: 19,
        poll_interval_ms: 1,
        bootstrap_timeout_ms: 2,
    };
    for invalid in [
        MySqlReplicationConfig {
            server_id: 0,
            ..valid.clone()
        },
        MySqlReplicationConfig {
            max_events: 0,
            ..valid.clone()
        },
        MySqlReplicationConfig {
            max_transaction_bytes: 18,
            ..valid.clone()
        },
        MySqlReplicationConfig {
            poll_interval_ms: 0,
            ..valid.clone()
        },
        MySqlReplicationConfig {
            bootstrap_timeout_ms: 0,
            ..valid.clone()
        },
        MySqlReplicationConfig {
            poll_interval_ms: 2,
            bootstrap_timeout_ms: 2,
            ..valid
        },
    ] {
        assert!(invalid.validate().is_err());
    }

    let maximum = MySqlReplicationConfig {
        max_transaction_bytes: super::super::super::MYSQL_CLIENT_PACKET_MAX_BYTES,
        ..valid
    };
    maximum.validate().unwrap();
    assert!(MySqlReplicationConfig {
        max_transaction_bytes: super::super::super::MYSQL_CLIENT_PACKET_MAX_BYTES + 1,
        ..valid
    }
    .validate()
    .is_err());

    let maximum_heartbeat_ms = u64::MAX / 1_000_000;
    MySqlReplicationConfig {
        poll_interval_ms: maximum_heartbeat_ms,
        bootstrap_timeout_ms: maximum_heartbeat_ms + 1,
        ..valid
    }
    .validate()
    .unwrap();
    assert!(MySqlReplicationConfig {
        poll_interval_ms: maximum_heartbeat_ms + 1,
        bootstrap_timeout_ms: maximum_heartbeat_ms + 2,
        ..valid
    }
    .validate()
    .is_err());
}
