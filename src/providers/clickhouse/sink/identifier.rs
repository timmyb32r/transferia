/// Validate an unqualified `ClickHouse` table or column identifier.
///
/// The sink deliberately supports only a small ASCII subset so identifiers have
/// one canonical representation in DDL, INSERT, and metadata comparisons.
pub(super) fn validate_identifier(identifier: &str) -> anyhow::Result<()> {
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
mod tests {
    use super::*;

    #[test]
    fn accepts_the_supported_ascii_identifier_grammar() -> anyhow::Result<()> {
        for identifier in ["events", "_events", "Events_2026", "a0"] {
            validate_identifier(identifier)?;
        }
        Ok(())
    }

    #[test]
    fn rejects_ambiguous_or_qualified_identifiers() {
        for identifier in [
            "",
            "2026_events",
            "events-archive",
            "default.events",
            "events,archive",
            "`events`",
            "события",
        ] {
            assert!(
                validate_identifier(identifier).is_err(),
                "identifier {identifier:?} must be rejected"
            );
        }
    }
}
