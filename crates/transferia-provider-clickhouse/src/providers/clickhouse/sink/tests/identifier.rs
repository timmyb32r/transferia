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
