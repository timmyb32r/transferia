use anyhow::Context as _;
use clap::Parser;
use tokio::signal;
use tokio_util::sync::CancellationToken;

use transferia_delivery::delivery::config::yaml::Config;
use transferia_delivery::delivery::execution::runner::start_delivery;
use transferia_delivery::delivery::preparation::{
    build_delivery_plan_with, build_resolved_delivery_document_with, ResolvedConfigDocument,
};
use transferia_connectors::extension::Transferia;
use transferia_runtime_local::LocalWorkerSupervisor;

use worker_control::WorkerControl;

mod worker_control;

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "Native data transfer pipeline")]
struct Cli {
    #[arg(long, env = "CONFIG_PATH")]
    config: Option<String>,

    #[arg(long)]
    server: bool,

    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: std::net::SocketAddr,

    #[arg(long, default_value = ".transferia-server")]
    state_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 1)]
    total_workers: u32,

    #[arg(long, default_value_t = 0)]
    worker_index: u32,

    #[arg(long, hide = true)]
    parent_control: Option<std::net::SocketAddr>,

    #[arg(long, env = "TRANSFERIA_PARENT_TOKEN", hide = true)]
    parent_token: Option<String>,

    #[arg(long, hide = true)]
    resolved_config: bool,

    #[arg(long, hide = true)]
    composition_fingerprint: Option<String>,
}

fn validate_worker_assignment(cli: &Cli) -> anyhow::Result<()> {
    anyhow::ensure!(cli.total_workers > 0, "total_workers must be positive");
    anyhow::ensure!(
        cli.worker_index < cli.total_workers,
        "worker_index must be less than total_workers"
    );
    anyhow::ensure!(
        cli.parent_control.is_some() == cli.parent_token.is_some(),
        "--parent-control and TRANSFERIA_PARENT_TOKEN must be provided together"
    );
    anyhow::ensure!(
        cli.resolved_config == cli.composition_fingerprint.is_some(),
        "--resolved-config and --composition-fingerprint must be provided together"
    );
    Ok(())
}

fn spawn_shutdown_listener(cancellation: CancellationToken) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            cancellation.cancel();
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            cancellation.cancel();
        }
    });
    Ok(())
}

pub async fn run(transferia: Transferia) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    if cli.server {
        let supervisor = std::sync::Arc::new(LocalWorkerSupervisor::new(
            std::env::current_exe()?,
            cli.state_dir.clone(),
        ));
        return transferia_control_plane::run_with(cli.bind, cli.state_dir, transferia, supervisor)
            .await;
    }
    validate_worker_assignment(&cli)?;
    let config_path = cli
        .config
        .as_deref()
        .context("--config is required unless --server is selected")?;
    let cancellation = CancellationToken::new();
    let parent_control = match (cli.parent_control, cli.parent_token.as_deref()) {
        (Some(address), Some(token)) => {
            Some(WorkerControl::connect(address, token, cancellation.clone()).await?)
        }
        (None, None) => None,
        _ => {
            anyhow::bail!("--parent-control and TRANSFERIA_PARENT_TOKEN must be provided together")
        }
    };
    spawn_shutdown_listener(cancellation.clone())?;
    let startup = async {
        if let Some(expected) = &cli.composition_fingerprint {
            anyhow::ensure!(
                expected == transferia.composition_fingerprint(),
                "worker composition does not match the composition that resolved its configuration"
            );
        }
        let plan = if cli.resolved_config {
            let document = ResolvedConfigDocument::from_file(config_path)?;
            build_resolved_delivery_document_with(document, cancellation.clone(), &transferia)
                .await?
        } else {
            let config = Config::from_file(config_path)?;
            build_delivery_plan_with(config, cancellation.clone(), &transferia).await?
        };
        start_delivery(plan, cli.total_workers, cli.worker_index, cancellation).await
    }
    .await;
    let Some(mut execution) = (match startup {
        Ok(execution) => execution,
        Err(error) => {
            if let Some(parent_control) = &parent_control {
                let _ignored = parent_control.startup_failed(&error).await;
            }
            return Err(error);
        }
    }) else {
        return Ok(());
    };
    if let Some(parent_control) = &parent_control {
        if let Err(error) = parent_control.ready().await {
            execution.shutdown().await;
            return Err(error).context("failed to report worker readiness");
        }
    }
    execution.wait().await
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
