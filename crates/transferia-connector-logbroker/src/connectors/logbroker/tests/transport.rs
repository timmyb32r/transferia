use super::socket_address;

fn address(uri: &str) -> anyhow::Result<String> {
    socket_address(&uri.parse()?)
}

#[test]
fn endpoint_needs_the_http_scheme_and_a_host() {
    for uri in [
        "/Root",
        "https://topic.example.test:2135",
        "grpc://topic.example.test:2135",
    ] {
        assert!(address(uri).is_err(), "{uri} must be rejected");
    }
}

#[test]
fn missing_port_is_the_native_grpc_port() -> anyhow::Result<()> {
    assert_eq!(
        address("http://topic.example.test")?,
        "topic.example.test:2135"
    );
    assert_eq!(
        address("http://topic.example.test:2136")?,
        "topic.example.test:2136"
    );
    Ok(())
}
