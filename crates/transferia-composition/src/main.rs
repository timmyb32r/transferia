use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    transferia_composition::run(transferia_connectors::extension::Transferia::public()?).await
}
