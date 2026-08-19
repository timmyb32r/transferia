use std::path::Path;

const SCHEMA_OUTPUT: &str =
    "crates/transferia-server-contracts/contracts/server-api.schema.json";
const FIXTURE_OUTPUT: &str =
    "crates/transferia-server-contracts/contracts/server-api.fixture.json";

fn main() -> anyhow::Result<()> {
    let check = std::env::args().any(|argument| argument == "--check");
    write_or_check(
        Path::new(SCHEMA_OUTPUT),
        transferia_server_contracts::api::schema()?,
        check,
    )?;
    write_or_check(
        Path::new(FIXTURE_OUTPUT),
        transferia_server_contracts::api::fixture()?,
        check,
    )?;
    Ok(())
}

fn write_or_check(path: &Path, value: serde_json::Value, check: bool) -> anyhow::Result<()> {
    let output = format!("{}\n", serde_json::to_string_pretty(&value)?);
    if check {
        let committed = std::fs::read_to_string(path)?;
        anyhow::ensure!(
            committed == output,
            "{} is stale; run `just api-contract`",
            path.display()
        );
    } else {
        std::fs::write(path, output)?;
    }
    Ok(())
}
