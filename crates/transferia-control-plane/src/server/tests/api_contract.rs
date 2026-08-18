use super::*;

#[test]
fn committed_schema_matches_rust_dtos() -> anyhow::Result<()> {
    let committed: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../transferia-server-contracts/contracts/server-api.schema.json"
    )))?;
    assert_eq!(committed, schema()?);
    Ok(())
}

#[test]
fn committed_fixture_is_generated_by_rust_dtos() -> anyhow::Result<()> {
    let committed: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../transferia-server-contracts/contracts/server-api.fixture.json"
    )))?;
    assert_eq!(committed, fixture()?);
    Ok(())
}
