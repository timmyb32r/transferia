use crate::providers::logbroker::pqv1::config::PqV1AuthConfig;

/// Load the raw access token required by the legacy `PQv1` protocol.
#[expect(
    clippy::unreachable,
    reason = "PqV1AuthConfig::validate requires exactly one token source"
)]
pub fn load_access_token(auth: &PqV1AuthConfig) -> anyhow::Result<String> {
    auth.validate()?;
    if let Some(path) = auth.token_file.as_deref() {
        let expanded = shellexpand::full(path)
            .map_err(|e| anyhow::anyhow!("Failed to expand token_file path '{path}': {e}"))?;
        let token = std::fs::read_to_string(expanded.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to read token from '{expanded}': {e}"))?
            .trim()
            .to_string();
        anyhow::ensure!(!token.is_empty(), "PQv1 access token file is empty");
        Ok(token)
    } else if let Some(tok) = auth.token.as_deref() {
        Ok(tok.trim().to_string())
    } else {
        unreachable!("validated that one token source is configured")
    }
}

#[cfg(test)]
#[path = "tests/credentials.rs"]
mod tests;
