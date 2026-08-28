use super::*;

const BASE: &str = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n";

fn parse_config(yaml: &str) -> anyhow::Result<ClickHouseSinkConfig> {
    let config: ClickHouseSinkConfig = serde_yaml::from_str(yaml)?;
    config.validate()?;
    Ok(config)
}

#[test]
fn parses_multiple_hosts_with_one_native_port() -> anyhow::Result<()> {
    let config = parse_config(
        "hosts: [ch-a, ch-b]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    )?;
    assert_eq!(config.hosts, ["ch-a", "ch-b"]);
    assert_eq!(config.port, 9000);
    assert_eq!(config.effective_data_host_count(), 2);
    Ok(())
}

#[test]
fn resolved_topology_count_is_independent_from_connectivity_hosts() -> anyhow::Result<()> {
    let config = parse_config(
        "hosts: [ch-a]\ndata_host_count: 3\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    )?;
    assert_eq!(config.effective_data_host_count(), 3);
    assert!(parse_config(
        "hosts: [ch-a]\ndata_host_count: 0\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n"
    )
    .is_err());
    Ok(())
}

#[test]
fn database_and_username_have_no_defaults() {
    for yaml in [
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: transferia\n",
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\n",
    ] {
        assert!(parse_config(yaml).is_err());
    }
}

#[test]
fn transport_choice_is_explicit_and_verified_tls_is_supported() -> anyhow::Result<()> {
    assert!(parse_config(
        "hosts: [localhost]\nport: 9000\ndatabase: analytics\nusername: transferia\n"
    )
    .is_err());
    let tls = parse_config(
        "hosts: [localhost]\nport: 9440\ntrusted_plaintext: false\ntls_ca_file: /tmp/ca.pem\ndatabase: analytics\nusername: transferia\n",
    )?;
    assert!(!tls.trusted_plaintext);
    assert_eq!(tls.tls_ca_file.as_deref(), Some("/tmp/ca.pem"));
    Ok(())
}

#[test]
fn rejects_old_endpoint_and_sorting_key_options() {
    for yaml in [
        "endpoint: localhost:9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\nsorting_key: [id]\n",
    ] {
        assert!(serde_yaml::from_str::<ClickHouseSinkConfig>(yaml).is_err());
    }
}

#[test]
fn validates_hosts_and_native_port() {
    for yaml in [
        "hosts: []\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
        "hosts: [localhost, localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
        "hosts: ['http://localhost']\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    ] {
        assert!(parse_config(yaml).is_err());
    }
    let error = parse_config(
        "hosts: [localhost]\nport: 8123\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    )
    .unwrap_err();
    assert!(error.to_string().contains("HTTP port"));
    assert!(error.to_string().contains("native protocol"));
}

#[test]
fn defaults_to_finite_retries() -> anyhow::Result<()> {
    let config = parse_config(BASE)?;
    assert_eq!(config.effective_retry_max_attempts(), 20);
    assert_eq!(config.insert_concurrency, 1);
    assert_eq!(config.compression, ClickHouseCompression::Lz4);
    assert!(!config.async_insert);
    Ok(())
}

#[test]
fn parses_explicit_native_insert_transport_tuning() -> anyhow::Result<()> {
    let config = parse_config(&format!("{BASE}compression: zstd\nasync_insert: true\n"))?;
    assert_eq!(config.compression, ClickHouseCompression::Zstd);
    assert!(config.async_insert);
    Ok(())
}

#[test]
fn validates_retry_policy() {
    for suffix in [
        "retry_max_attempts: 0\n",
        "retry_initial_ms: 20\nretry_max_ms: 10\n",
        "connect_timeout_ms: 0\n",
        "request_timeout_ms: 0\n",
        "insert_concurrency: 0\n",
        "insert_concurrency: 33\n",
    ] {
        assert!(parse_config(&format!("{BASE}{suffix}")).is_err());
    }
}

#[test]
fn debug_redacts_password() -> anyhow::Result<()> {
    let config = parse_config(&format!("{BASE}password: super-secret\n"))?;
    let debug = format!("{config:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("super-secret"));
    Ok(())
}
