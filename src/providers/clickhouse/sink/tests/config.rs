use super::*;

const BASE: &str = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n";

#[test]
fn parses_multiple_hosts_with_one_native_port() -> anyhow::Result<()> {
    let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
        "hosts: [ch-a, ch-b]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    )?)?;
    assert_eq!(config.hosts, ["ch-a", "ch-b"]);
    assert_eq!(config.port, 9000);
    Ok(())
}

#[test]
fn database_and_username_have_no_defaults() -> anyhow::Result<()> {
    for yaml in [
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: transferia\n",
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\n",
    ] {
        assert!(ClickHouseSinkConfig::from_value(serde_yaml::from_str(yaml)?).is_err());
    }
    Ok(())
}

#[test]
fn plaintext_transport_requires_an_explicit_trust_acknowledgement() -> anyhow::Result<()> {
    for yaml in [
        "hosts: [localhost]\nport: 9000\ndatabase: analytics\nusername: transferia\n",
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: false\ndatabase: analytics\nusername: transferia\n",
    ] {
        assert!(ClickHouseSinkConfig::from_value(serde_yaml::from_str(yaml)?).is_err());
    }
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
fn validates_hosts_and_native_port() -> anyhow::Result<()> {
    for yaml in [
        "hosts: []\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
        "hosts: [localhost, localhost]\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
        "hosts: ['http://localhost']\nport: 9000\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    ] {
        assert!(ClickHouseSinkConfig::from_value(serde_yaml::from_str(yaml)?).is_err());
    }
    let error = ClickHouseSinkConfig::from_value(serde_yaml::from_str(
        "hosts: [localhost]\nport: 8123\ntrusted_plaintext: true\ndatabase: analytics\nusername: transferia\n",
    )?)
    .unwrap_err();
    assert!(error.to_string().contains("HTTP port"));
    assert!(error.to_string().contains("native protocol"));
    Ok(())
}

#[test]
fn defaults_to_finite_retries() -> anyhow::Result<()> {
    let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(BASE)?)?;
    assert_eq!(config.effective_retry_max_attempts(), 20);
    Ok(())
}

#[test]
fn validates_retry_policy() -> anyhow::Result<()> {
    for suffix in [
        "retry_max_attempts: 0\n",
        "retry_initial_ms: 20\nretry_max_ms: 10\n",
        "connect_timeout_ms: 0\n",
        "request_timeout_ms: 0\n",
    ] {
        let value = serde_yaml::from_str(&format!("{BASE}{suffix}"))?;
        assert!(ClickHouseSinkConfig::from_value(value).is_err());
    }
    Ok(())
}

#[test]
fn debug_redacts_password() -> anyhow::Result<()> {
    let config = ClickHouseSinkConfig::from_value(serde_yaml::from_str(&format!(
        "{BASE}password: super-secret\n"
    ))?)?;
    let debug = format!("{config:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("super-secret"));
    Ok(())
}
