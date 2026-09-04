#![allow(
    clippy::expect_used,
    reason = "test assertions intentionally fail fast"
)]

use super::super::super::config::{YdbAuth, YdbConnectionConfig};
use super::{validate_response_admission, YdbReplicationConfig};

fn valid_config() -> YdbReplicationConfig {
    YdbReplicationConfig {
        changefeed_name: "cdc".to_owned(),
        consumer_name: "transferia".to_owned(),
        coordination_node_path: "/production/transferia".to_owned(),
        read_buffer_bytes: 1024,
        max_message_bytes: 2048,
        max_batch_bytes: 4096,
        max_response_bytes: 8192,
        commit_timeout_ms: 30_000,
    }
}

#[test]
fn validates_relational_message_batch_and_response_limits() {
    valid_config().validate().expect("valid replication limits");

    let mut config = valid_config();
    config.max_message_bytes = config.max_batch_bytes + 1;
    assert!(config.validate().is_err());

    let mut config = valid_config();
    config.max_batch_bytes = config.max_response_bytes + 1;
    assert!(config.validate().is_err());
}

#[test]
fn default_response_decode_admission_fits_the_default_pipeline_budget() {
    let config: YdbReplicationConfig = serde_json::from_value(serde_json::json!({
        "changefeed_name": "cdc",
        "consumer_name": "transferia",
        "coordination_node_path": "/production/transferia"
    }))
    .expect("replication defaults");
    assert_eq!(config.max_message_bytes, 1024 * 1024);
    assert_eq!(config.max_batch_bytes, 1024 * 1024);
    assert_eq!(config.max_response_bytes, 1536 * 1024);
    assert!(
        config
            .minimum_pipeline_memory_bytes()
            .expect("bounded response admission")
            <= 1024 * 1024 * 1024
    );
}

#[test]
fn response_admission_rejects_platform_overflow() {
    assert!(validate_response_admission(usize::MAX).is_err());
    let retained_factor = super::super::topic::MAX_DECODED_BYTES_PER_ENCODED_RESPONSE_BYTE + 1;
    let codec_headroom = 3 * 8 * 1024;
    validate_response_admission((usize::MAX - codec_headroom) / retained_factor)
        .expect("largest representable admission");
}

#[test]
fn coordination_path_must_be_canonical_absolute_non_root() {
    for path in [
        "",
        "/",
        "relative",
        "/production/",
        " /production",
        "/production//transferia",
        "/production/./transferia",
        "/production/../transferia",
    ] {
        let mut config = valid_config();
        config.coordination_node_path = path.to_owned();
        assert!(config.validate().is_err(), "path {path:?} must be rejected");
    }
}

#[test]
fn persisted_endpoint_identity_accepts_only_a_bare_credential_free_authority() {
    let mut connection = YdbConnectionConfig {
        endpoint: "grpcs://ydb.example:2135".to_owned(),
        database: "/production".to_owned(),
        trusted_plaintext: false,
        auth: YdbAuth::Anonymous,
        request_timeout_ms: 30_000,
        max_rpc_message_bytes: 256 * 1024 * 1024,
    };
    assert_eq!(
        connection.tonic_endpoint().expect("valid TLS endpoint"),
        "https://ydb.example:2135"
    );

    for endpoint in [
        "grpcs://user@ydb.example:2135",
        "grpcs://ydb.example:2135/",
        "grpcs://ydb.example:2135/database",
        "grpcs://ydb.example:2135?token=secret",
        "grpcs://ydb.example:2135#fragment",
    ] {
        connection.endpoint = endpoint.to_owned();
        assert!(
            connection.tonic_endpoint().is_err(),
            "endpoint {endpoint:?} must be rejected before persistence"
        );
    }
}
