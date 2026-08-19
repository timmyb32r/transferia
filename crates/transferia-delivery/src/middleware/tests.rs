use super::*;

#[test]
fn decodes_an_opaque_middleware_entry_without_owning_its_implementation() -> anyhow::Result<()> {
    let entry: MiddlewareEntry =
        serde_yaml::from_str("filter:\n  field: event_name\n  value: page_view\n")?;
    anyhow::ensure!(entry.kind()? == "filter");
    anyhow::ensure!(entry.raw()?["field"] == "event_name");
    Ok(())
}

#[test]
fn rejects_multiple_middleware_kinds() -> anyhow::Result<()> {
    let entry: MiddlewareEntry = serde_yaml::from_str("first: {}\nsecond: {}\n")?;
    anyhow::ensure!(entry.kind().is_err());
    Ok(())
}
