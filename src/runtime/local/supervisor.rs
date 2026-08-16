use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_util::sync::CancellationToken;

use super::super::{
    RunId, SupervisorError, WorkerEvent, WorkerInfo, WorkerOutcome, WorkerSupervisor,
};
use crate::delivery::preparation::ResolvedDeliveryConfig;

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(90);
const STOP_GRACE: Duration = Duration::from_secs(10);
const PARENT_TOKEN_ENV: &str = "TRANSFERIA_PARENT_TOKEN";

#[derive(Clone)]
struct WorkerHandle {
    run_id: RunId,
    cancellation: CancellationToken,
    completion: watch::Receiver<Option<Result<(), String>>>,
}

pub struct LocalWorkerSupervisor {
    executable: PathBuf,
    state_dir: PathBuf,
    workers: Arc<Mutex<BTreeMap<String, WorkerHandle>>>,
    start_lock: Mutex<()>,
    shutting_down: AtomicBool,
    events: mpsc::UnboundedSender<WorkerEvent>,
    event_receiver: StdMutex<Option<mpsc::UnboundedReceiver<WorkerEvent>>>,
}

impl LocalWorkerSupervisor {
    pub fn new(executable: PathBuf, state_dir: PathBuf) -> Self {
        if let Err(error) = cleanup_stale_worker_configs(&state_dir) {
            tracing::warn!(%error, "failed to remove stale resolved worker configurations");
        }
        let (events, event_receiver) = mpsc::unbounded_channel();
        Self {
            executable,
            state_dir,
            workers: Arc::new(Mutex::new(BTreeMap::new())),
            start_lock: Mutex::new(()),
            shutting_down: AtomicBool::new(false),
            events,
            event_receiver: StdMutex::new(Some(event_receiver)),
        }
    }

    async fn spawn_worker(
        &self,
        delivery_id: &str,
        run_id: &RunId,
        config: &ResolvedDeliveryConfig,
    ) -> anyhow::Result<(WorkerInfo, StartupWait)> {
        let runs_dir = self.state_dir.join("runs");
        tokio::fs::create_dir_all(&runs_dir).await?;
        secure_directory(&runs_dir).await?;
        let config_path = runs_dir.join(format!("{delivery_id}-{}.yaml", run_id.0));
        let log_path = runs_dir.join(format!("{delivery_id}.log"));
        let temporary_config = TemporaryConfig::new(config_path.clone());
        secure_write(&config_path, config.yaml().as_bytes()).await?;
        let log = secure_log_file(&log_path)?;
        let error_log = log.try_clone()?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let control_address = listener.local_addr()?;
        let token = random_token()?;
        let child = match Command::new(&self.executable)
            .arg("--config")
            .arg(&config_path)
            .arg("--resolved-config")
            .arg("--composition-fingerprint")
            .arg(config.composition_fingerprint())
            .arg("--parent-control")
            .arg(control_address.to_string())
            .env(PARENT_TOKEN_ENV, &token)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                let _ignored = tokio::fs::remove_file(&config_path).await;
                return Err(error.into());
            }
        };
        let pid = child
            .id()
            .context("spawned worker did not expose a process id")?;

        let cancellation = CancellationToken::new();
        let startup_cancellation = cancellation.clone();
        let (completion, completion_rx) = watch::channel(None);
        let (startup, startup_rx) = oneshot::channel();
        self.workers.lock().await.insert(
            delivery_id.to_owned(),
            WorkerHandle {
                run_id: run_id.clone(),
                cancellation: cancellation.clone(),
                completion: completion_rx,
            },
        );
        let config_path = temporary_config.disarm();
        spawn_worker_actor(WorkerActor {
            delivery_id: delivery_id.to_owned(),
            run_id: run_id.clone(),
            child,
            listener,
            token,
            cancellation,
            startup,
            completion,
            config_path,
            workers: Arc::clone(&self.workers),
            events: self.events.clone(),
        });
        Ok((
            WorkerInfo { pid },
            StartupWait {
                receiver: startup_rx,
                cancellation: startup_cancellation,
                completed: false,
            },
        ))
    }

    async fn stop_handle(handle: WorkerHandle) -> Result<(), SupervisorError> {
        handle.cancellation.cancel();
        let mut completion = handle.completion;
        loop {
            let current = completion.borrow().clone();
            if let Some(result) = current {
                return result.map_err(SupervisorError::Stop);
            }
            completion.changed().await.map_err(|_| {
                SupervisorError::Stop("worker completion channel closed".to_owned())
            })?;
        }
    }
}

#[async_trait]
impl WorkerSupervisor for LocalWorkerSupervisor {
    async fn start(
        &self,
        delivery_id: &str,
        run_id: &RunId,
        config: &ResolvedDeliveryConfig,
    ) -> Result<WorkerInfo, SupervisorError> {
        let (worker, startup) = {
            let _start = self.start_lock.lock().await;
            if self.shutting_down.load(Ordering::Acquire) {
                return Err(SupervisorError::ShuttingDown);
            }
            if self.workers.lock().await.contains_key(delivery_id) {
                return Err(SupervisorError::AlreadyRunning(delivery_id.to_owned()));
            }
            self.spawn_worker(delivery_id, run_id, config)
                .await
                .map_err(|error| SupervisorError::Startup(error.to_string()))?
        };
        match startup.wait().await {
            Ok(StartupOutcome::Ready) => Ok(worker),
            Ok(StartupOutcome::Failed(message)) => Err(SupervisorError::Startup(message)),
            Ok(StartupOutcome::Cancelled) => Err(SupervisorError::StartupCancelled),
            Err(_) => Err(SupervisorError::Startup(
                "worker startup task ended without a result".to_owned(),
            )),
        }
    }

    async fn stop(&self, delivery_id: &str, run_id: &RunId) -> Result<(), SupervisorError> {
        let handle = self
            .workers
            .lock()
            .await
            .get(delivery_id)
            .cloned()
            .ok_or_else(|| SupervisorError::NotRunning(delivery_id.to_owned()))?;
        if &handle.run_id != run_id {
            return Err(SupervisorError::RunMismatch {
                delivery_id: delivery_id.to_owned(),
            });
        }
        Self::stop_handle(handle).await
    }

    async fn shutdown_all(&self) -> Result<(), SupervisorError> {
        let workers = {
            let _start = self.start_lock.lock().await;
            self.shutting_down.store(true, Ordering::Release);
            self.workers
                .lock()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut errors = Vec::new();
        for worker in workers {
            if let Err(error) = Self::stop_handle(worker.clone()).await {
                tracing::warn!(run_id = %worker.run_id.0, %error, "failed to stop worker during shutdown");
                errors.push(format!("{}: {error}", worker.run_id.0));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(SupervisorError::Stop(format!(
                "failed to stop workers: {}",
                errors.join("; ")
            )))
        }
    }

    fn take_events(&self) -> Result<mpsc::UnboundedReceiver<WorkerEvent>, SupervisorError> {
        self.event_receiver
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event receiver mutex was poisoned"))?
            .take()
            .ok_or(SupervisorError::EventsAlreadyTaken)
    }
}

fn cleanup_stale_worker_configs(state_dir: &Path) -> std::io::Result<()> {
    let runs_dir = state_dir.join("runs");
    let entries = match std::fs::read_dir(runs_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "yaml")
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

struct TemporaryConfig {
    path: PathBuf,
    armed: bool,
}

impl TemporaryConfig {
    const fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(mut self) -> PathBuf {
        self.armed = false;
        self.path.clone()
    }
}

impl Drop for TemporaryConfig {
    fn drop(&mut self) {
        if self.armed {
            let _ignored = std::fs::remove_file(&self.path);
        }
    }
}

struct WorkerActor {
    delivery_id: String,
    run_id: RunId,
    child: Child,
    listener: TcpListener,
    token: String,
    cancellation: CancellationToken,
    startup: oneshot::Sender<StartupOutcome>,
    completion: watch::Sender<Option<Result<(), String>>>,
    config_path: PathBuf,
    workers: Arc<Mutex<BTreeMap<String, WorkerHandle>>>,
    events: mpsc::UnboundedSender<WorkerEvent>,
}

enum StartupOutcome {
    Ready,
    Failed(String),
    Cancelled,
}

struct StartupWait {
    receiver: oneshot::Receiver<StartupOutcome>,
    cancellation: CancellationToken,
    completed: bool,
}

impl StartupWait {
    async fn wait(mut self) -> Result<StartupOutcome, oneshot::error::RecvError> {
        let result = (&mut self.receiver).await;
        self.completed = true;
        result
    }
}

impl Drop for StartupWait {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellation.cancel();
        }
    }
}

fn spawn_worker_actor(actor: WorkerActor) {
    tokio::spawn(async move {
        run_worker_actor(actor).await;
    });
}

async fn run_worker_actor(mut actor: WorkerActor) {
    let startup_result = {
        let startup = async {
            let stream =
                authenticate_worker(&actor.listener, &actor.token, &mut actor.child).await?;
            await_ready(stream, &mut actor.child).await
        };
        tokio::pin!(startup);
        tokio::select! {
            biased;
            () = actor.cancellation.cancelled() => None,
            result = &mut startup => Some(result),
        }
    };

    let (outcome, stop_result) = match startup_result {
        Some(Ok(mut control)) => {
            let _ignored = tokio::fs::remove_file(&actor.config_path).await;
            if actor.startup.send(StartupOutcome::Ready).is_err() {
                let result = stop_child(&mut actor.child, &mut control).await;
                outcome_from_stop(result)
            } else {
                tokio::select! {
                    biased;
                    () = actor.cancellation.cancelled() => {
                        outcome_from_stop(stop_child(&mut actor.child, &mut control).await)
                    },
                    status = actor.child.wait() => match status {
                        Ok(status) => {
                            let message = status.to_string();
                            let stop_result = if status.success() {
                                Ok(())
                            } else {
                                Err(message.clone())
                            };
                            (
                                WorkerOutcome::Exited {
                                    success: status.success(),
                                    message,
                                },
                                stop_result,
                            )
                        }
                        Err(error) => (
                            WorkerOutcome::Exited {
                                success: false,
                                message: error.to_string(),
                            },
                            Err(error.to_string()),
                        ),
                    }
                }
            }
        }
        Some(Err(error)) => {
            let mut message = error.to_string();
            if let Err(kill_error) = actor.child.kill().await {
                let _ = write!(
                    message,
                    "; terminating failed worker also failed: {kill_error}"
                );
            }
            let _ignored = actor.startup.send(StartupOutcome::Failed(message.clone()));
            (
                WorkerOutcome::Exited {
                    success: false,
                    message: message.clone(),
                },
                Err(message),
            )
        }
        None => {
            let _ignored = actor.startup.send(StartupOutcome::Cancelled);
            match actor.child.kill().await {
                Ok(()) => (WorkerOutcome::Stopped, Ok(())),
                Err(error) => {
                    let message = format!("failed to terminate cancelled worker: {error}");
                    (
                        WorkerOutcome::Exited {
                            success: false,
                            message: message.clone(),
                        },
                        Err(message),
                    )
                }
            }
        }
    };

    let _ignored = tokio::fs::remove_file(&actor.config_path).await;
    let mut workers = actor.workers.lock().await;
    if workers
        .get(&actor.delivery_id)
        .is_some_and(|worker| worker.run_id == actor.run_id)
    {
        workers.remove(&actor.delivery_id);
    }
    drop(workers);
    let _ignored = actor.events.send(WorkerEvent {
        delivery_id: actor.delivery_id,
        run_id: actor.run_id,
        outcome,
    });
    actor.completion.send_replace(Some(stop_result));
}

fn outcome_from_stop(result: anyhow::Result<()>) -> (WorkerOutcome, Result<(), String>) {
    match result {
        Ok(()) => (WorkerOutcome::Stopped, Ok(())),
        Err(error) => {
            let message = error.to_string();
            (
                WorkerOutcome::Exited {
                    success: false,
                    message: message.clone(),
                },
                Err(message),
            )
        }
    }
}

async fn stop_child(child: &mut Child, control: &mut TcpStream) -> anyhow::Result<()> {
    let _ignored = control.write_all(b"STOP\n").await;
    match tokio::time::timeout(STOP_GRACE, child.wait()).await {
        Ok(status) => {
            status?;
        }
        Err(_) => {
            child.kill().await?;
        }
    }
    Ok(())
}

async fn authenticate_worker(
    listener: &TcpListener,
    expected_token: &str,
    child: &mut Child,
) -> anyhow::Result<TcpStream> {
    tokio::time::timeout(AUTH_TIMEOUT, async {
        loop {
            tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    anyhow::bail!("worker exited before control authentication: {status}");
                }
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    if !peer.ip().is_loopback() {
                        continue;
                    }
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).await?;
                    if line.trim_end() == format!("AUTH {expected_token}") {
                        return Ok(reader.into_inner());
                    }
                }
            }
        }
    })
    .await
    .context("worker did not authenticate with the control plane")?
}

async fn await_ready(mut stream: TcpStream, child: &mut Child) -> anyhow::Result<TcpStream> {
    tokio::time::timeout(READY_TIMEOUT, async {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                anyhow::bail!("worker exited before readiness: {status}");
            }
            read = reader.read_line(&mut line) => {
                read?;
                anyhow::ensure!(line.trim_end() == "READY", "worker sent an invalid readiness message");
            }
        }
        Ok(stream)
    })
    .await
    .context("worker readiness timed out")?
}

fn random_token() -> anyhow::Result<String> {
    use std::fmt::Write as _;

    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)?;
    let mut token = String::with_capacity(64);
    for byte in bytes {
        write!(token, "{byte:02x}")?;
    }
    Ok(token)
}

async fn secure_write(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        let mut file = options.open(path).await?;
        file.write_all(contents).await?;
        file.sync_all().await?;
    }
    #[cfg(not(unix))]
    {
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(path).await?;
        file.write_all(contents).await?;
        file.sync_all().await?;
    }
    Ok(())
}

fn secure_log_file(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

async fn secure_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/supervisor.rs"]
mod tests;
