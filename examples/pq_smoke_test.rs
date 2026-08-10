//! `PQv1` smoke test. Uses the provider registry directly.
//! Requires: `config_bench.yaml` with `source: { pqv1: { ... } }`

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("PQv1 smoke test — provider-based dispatch not yet wired for examples.");
    println!("Use `transferia --config config_bench.yaml` for e2e testing.");
    Ok(())
}
