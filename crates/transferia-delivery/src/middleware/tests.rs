use super::*;

#[test]
fn builds_middleware_from_opaque_entry() -> anyhow::Result<()> {
    let entry: MiddlewareEntry =
        serde_yaml::from_str("filter:\n  field: event_name\n  value: page_view\n")?;
    anyhow::ensure!(entry.kind()? == "filter");
    drop(build_middleware(entry.kind()?, entry.raw()?.clone())?);
    Ok(())
}

#[test]
fn rejects_unknown_middleware() -> anyhow::Result<()> {
    let entry: MiddlewareEntry = serde_yaml::from_str("unknown: {}\n")?;
    anyhow::ensure!(build_middleware(entry.kind()?, entry.raw()?.clone()).is_err());
    Ok(())
}
#[cfg(feature = "datafusion")]
mod datafusion;
