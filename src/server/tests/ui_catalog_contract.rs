use super::super::ui_catalog::build_ui_catalog;

#[test]
fn rust_catalog_satisfies_the_typescript_schema_contract() -> anyhow::Result<()> {
    let catalog = serde_json::to_string(&build_ui_catalog()?)?;
    let status = std::process::Command::new("npm")
        .args(["test", "--", "--run", "tests/catalogContract.test.ts"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/web"))
        .env("TRANSFERIA_CATALOG_CONTRACT", catalog)
        .status()?;

    anyhow::ensure!(status.success(), "catalog contract test failed: {status}");
    Ok(())
}
