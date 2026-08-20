use std::path::Path;

const OUTPUT: &str = "crates/transferia-server-contracts/contracts/provider-catalog.fixture.json";

fn main() -> anyhow::Result<()> {
    let check = std::env::args().any(|argument| argument == "--check");
    let output = format!(
        "{}\n",
        serde_json::to_string_pretty(
            &transferia_control_plane::server::ui_catalog::build_ui_catalog()?
        )?
    );
    let path = Path::new(OUTPUT);
    if check {
        let committed = std::fs::read_to_string(path)?;
        anyhow::ensure!(
            committed == output,
            "{} is stale; run `just catalog-contract`",
            path.display()
        );
    } else {
        std::fs::write(path, output)?;
    }
    Ok(())
}
