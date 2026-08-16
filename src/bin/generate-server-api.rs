use std::path::Path;

const SCHEMA_OUTPUT: &str = "contracts/server-api.schema.json";
const FIXTURE_OUTPUT: &str = "contracts/server-api.fixture.json";

fn main() -> anyhow::Result<()> {
    let artifacts = [
        (SCHEMA_OUTPUT, transferia::server::api_contract::schema()?),
        (FIXTURE_OUTPUT, transferia::server::api_contract::fixture()?),
    ];
    if std::env::args().any(|argument| argument == "--check") {
        for (output, generated) in artifacts {
            let committed: serde_json::Value = serde_json::from_slice(&std::fs::read(output)?)?;
            anyhow::ensure!(
                committed == generated,
                "{output} is stale; run `TRANSFERIA_SKIP_SERVER_UI=1 cargo run --bin generate-server-api`"
            );
        }
        return Ok(());
    }
    for (output, generated) in artifacts {
        let mut serialized = serde_json::to_string_pretty(&generated)?;
        serialized.push('\n');
        std::fs::write(Path::new(output), serialized)?;
    }
    Ok(())
}
