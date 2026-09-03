use super::super::{validate_index_name, OpenSearchAuth, OpenSearchConnectionConfig};

#[test]
fn index_names_are_rejected_instead_of_silently_rewritten() {
    for name in [
        "Upper",
        "has space",
        "with/slash",
        "with:colon",
        "line\nbreak",
        "*",
        "_reserved",
        ".",
        "..",
    ] {
        assert!(validate_index_name(name).is_err(), "{name}");
    }
    validate_index_name("logs-2026.09.03").unwrap();
}

#[test]
fn debug_output_redacts_basic_auth_password() {
    let config = OpenSearchConnectionConfig {
        hosts: vec!["example.test".to_owned()],
        port: 9200,
        trusted_plaintext: true,
        tls_ca_file: None,
        auth: OpenSearchAuth::Basic {
            username: "reader".to_owned(),
            password: "super-secret".to_owned(),
        },
        request_timeout_ms: 1_000,
        max_response_bytes: 1_024,
    };
    let output = format!("{config:?}");
    assert!(!output.contains("super-secret"));
    assert!(output.contains("<redacted>"));
}
