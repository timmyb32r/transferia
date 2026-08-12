/// Validate an unqualified `ClickHouse` table or column identifier.
///
/// The sink deliberately supports only a small ASCII subset so identifiers have
/// one canonical representation in DDL, INSERT, and metadata comparisons.
pub fn validate_identifier(identifier: &str) -> anyhow::Result<()> {
    let mut bytes = identifier.bytes();
    let first = bytes.next();
    let valid = first.is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric());
    anyhow::ensure!(
        valid,
        "invalid ClickHouse identifier '{identifier}'; expected ASCII [A-Za-z_][A-Za-z0-9_]*"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
