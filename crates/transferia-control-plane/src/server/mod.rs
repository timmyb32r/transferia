use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use transferia_connectors::extension::Transferia;
use transferia_runtime::WorkerSupervisor;

pub mod api_contract;
mod assets;
mod http;
mod logs;
mod service;
mod store;
pub mod ui_catalog;

pub async fn run_with(
    bind: SocketAddr,
    allow_non_loopback: bool,
    state_dir: PathBuf,
    transferia: Transferia,
    supervisor: Arc<dyn WorkerSupervisor>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        bind.ip().is_loopback() || allow_non_loopback,
        "the local control plane may only bind to a non-loopback address after explicit opt-in"
    );
    if !bind.ip().is_loopback() {
        tracing::warn!(%bind, "control plane is exposed on a non-loopback interface");
    }
    let store = Arc::new(store::JsonDeliveryStore::open(state_dir.clone()).await?);
    let control_plane = Arc::new(
        service::ControlPlane::new(store, supervisor, transferia.clone())
            .with_worker_logs(logs::WorkerLogReader::new(&state_dir)),
    );
    control_plane.spawn_supervisor_monitor()?;
    let catalog = ui_catalog::build_ui_catalog_with(&transferia)?;
    let listener = TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    tracing::info!(%address, "local control plane is ready");

    let shutdown = CancellationToken::new();
    spawn_shutdown_listener(shutdown.clone())?;
    let serve = http::serve(
        listener,
        Arc::clone(&control_plane),
        catalog,
        shutdown.clone(),
    );
    tokio::pin!(serve);
    let (http_result, worker_result) = tokio::select! {
        result = &mut serve => {
            shutdown.cancel();
            (result, control_plane.shutdown().await)
        }
        () = shutdown.cancelled() => {
            let (worker_result, http_result) = tokio::join!(control_plane.shutdown(), &mut serve);
            (http_result, worker_result)
        }
    };
    match (http_result, worker_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(http), Ok(())) => Err(http),
        (Ok(()), Err(workers)) => Err(workers.into()),
        (Err(http), Err(workers)) => Err(anyhow::anyhow!(
            "HTTP server failed: {http:#}; worker shutdown failed: {workers}"
        )),
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

fn spawn_shutdown_listener(shutdown: CancellationToken) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "failed to listen for Ctrl-C");
                    }
                }
                _ = terminate.recv() => {}
            }
            shutdown.cancel();
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown.cancel();
        }
    });
    Ok(())
}
