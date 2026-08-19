use std::net::Ipv6Addr;

pub fn validate_host(field: &str, host: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!host.is_empty(), "{field} must not be empty");
    anyhow::ensure!(
        host.trim() == host,
        "{field} must not contain surrounding whitespace"
    );
    anyhow::ensure!(
        !host.contains("://") && !host.contains(['/', '?', '#']),
        "{field} must contain only a hostname or IP address, without a scheme, path, query, or fragment"
    );
    anyhow::ensure!(
        host.parse::<Ipv6Addr>().is_ok() || !host.contains(':'),
        "{field} must not include a port; configure port separately"
    );
    Ok(())
}

pub fn validate_port(field: &str, port: u16) -> anyhow::Result<()> {
    anyhow::ensure!(port > 0, "{field} must be positive");
    Ok(())
}

#[must_use]
pub fn host_port(host: &str, port: u16) -> String {
    if host.parse::<Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[must_use]
pub fn url(scheme: &str, host: &str, port: u16) -> String {
    format!("{scheme}://{}", host_port(host, port))
}
