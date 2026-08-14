use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(90);
const STOP_GRACE: Duration = Duration::from_secs(10);
const PARENT_TOKEN_ENV: &str = "TRANSFERIA_PARENT_TOKEN";

#[derive(Clone, Debug)]
pub enum WorkerOutcome {
    Stopped,
    Exited { success: bool, message: String },
}

#[derive(Clone, Debug)]
pub struct WorkerEvent {
    pub delivery_id: String,
    pub outcome: WorkerOutcome,
}

#[derive(Clone, Copy, Debug)]
pub struct WorkerInfo {
    pub pid: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("delivery '{0}' is already running")]
    AlreadyRunning(String),
    #[error("delivery '{0}' is not running")]
    NotRunning(String),
    #[error("worker startup failed: {0}")]
    Startup(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
pub trait WorkerSupervisor: Send + Sync {
    async fn start(
        &self,
        delivery_id: &str,
        config_yaml: &str,
    ) -> Result<WorkerInfo, SupervisorError>;

    async fn stop(&self, delivery_id: &str) -> Result<(), SupervisorError>;

    async fn shutdown_all(&self) -> Result<(), SupervisorError>;

    fn subscribe(&self) -> broadcast::Receiver<WorkerEvent>;
}

struct WorkerHandle {
    commands: mpsc::Sender<WorkerCommand>,
}

enum WorkerCommand {
    Stop {
        completed: oneshot::Sender<anyhow::Result<()>>,
    },
}

pub struct LocalWorkerSupervisor {
    executable: PathBuf,
    state_dir: PathBuf,
    workers: Arc<Mutex<BTreeMap<String, WorkerHandle>>>,
    starting: Mutex<BTreeSet<String>>,
    events: broadcast::Sender<WorkerEvent>,
}

impl LocalWorkerSupervisor {
    pub fn new(executable: PathBuf, state_dir: PathBuf) -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            executable,
            state_dir,
            workers: Arc::new(Mutex::new(BTreeMap::new())),
            starting: Mutex::new(BTreeSet::new()),
            events,
        }
    }

    async fn start_worker(
        &self,
        delivery_id: &str,
        config_yaml: &str,
    ) -> anyhow::Result<WorkerInfo> {
        let runs_dir = self.state_dir.join("runs");
        tokio::fs::create_dir_all(&runs_dir).await?;
        secure_directory(&runs_dir).await?;
        let config_path = runs_dir.join(format!("{delivery_id}.yaml"));
        let log_path = runs_dir.join(format!("{delivery_id}.log"));
        secure_write(&config_path, config_yaml.as_bytes()).await?;
        let log = secure_log_file(&log_path)?;
        let error_log = log.try_clone()?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let control_address = listener.local_addr()?;
        let token = random_token()?;
        let mut child = Command::new(&self.executable)
            .arg("--config")
            .arg(&config_path)
            .arg("--parent-control")
            .arg(control_address.to_string())
            .env(PARENT_TOKEN_ENV, &token)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .kill_on_drop(true)
            .spawn()?;
        let pid = child
            .id()
            .context("spawned worker did not expose a process id")?;

        let stream = match authenticate_worker(&listener, &token, &mut child).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ignored = child.kill().await;
                let _ignored = tokio::fs::remove_file(&config_path).await;
                return Err(error);
            }
        };
        let stream = match await_ready(stream, &mut child).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ignored = child.kill().await;
                let _ignored = tokio::fs::remove_file(&config_path).await;
                return Err(error);
            }
        };

        let (commands, receiver) = mpsc::channel(4);
        self.workers.lock().await.insert(
            delivery_id.to_owned(),
            WorkerHandle {
                commands: commands.clone(),
            },
        );
        spawn_worker_actor(
            delivery_id.to_owned(),
            child,
            stream,
            receiver,
            config_path,
            Arc::clone(&self.workers),
            self.events.clone(),
        );
        Ok(WorkerInfo { pid })
    }
}

#[async_trait]
impl WorkerSupervisor for LocalWorkerSupervisor {
    async fn start(
        &self,
        delivery_id: &str,
        config_yaml: &str,
    ) -> Result<WorkerInfo, SupervisorError> {
        {
            let mut starting = self.starting.lock().await;
            if starting.contains(delivery_id) || self.workers.lock().await.contains_key(delivery_id)
            {
                return Err(SupervisorError::AlreadyRunning(delivery_id.to_owned()));
            }
            starting.insert(delivery_id.to_owned());
        }
        let result = self
            .start_worker(delivery_id, config_yaml)
            .await
            .map_err(|error| SupervisorError::Startup(error.to_string()));
        self.starting.lock().await.remove(delivery_id);
        result
    }

    async fn stop(&self, delivery_id: &str) -> Result<(), SupervisorError> {
        let commands = self
            .workers
            .lock()
            .await
            .get(delivery_id)
            .map(|worker| worker.commands.clone())
            .ok_or_else(|| SupervisorError::NotRunning(delivery_id.to_owned()))?;
        let (completed, result) = oneshot::channel();
        commands
            .send(WorkerCommand::Stop { completed })
            .await
            .map_err(|_| SupervisorError::NotRunning(delivery_id.to_owned()))?;
        result
            .await
            .map_err(|_| SupervisorError::NotRunning(delivery_id.to_owned()))??;
        Ok(())
    }

    async fn shutdown_all(&self) -> Result<(), SupervisorError> {
        let ids = self
            .workers
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in ids {
            if let Err(error) = self.stop(&id).await {
                tracing::warn!(delivery_id = %id, %error, "failed to stop worker during shutdown");
            }
        }
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<WorkerEvent> {
        self.events.subscribe()
    }
}

fn spawn_worker_actor(
    delivery_id: String,
    mut child: Child,
    mut control: TcpStream,
    mut commands: mpsc::Receiver<WorkerCommand>,
    config_path: PathBuf,
    workers: Arc<Mutex<BTreeMap<String, WorkerHandle>>>,
    events: broadcast::Sender<WorkerEvent>,
) {
    tokio::spawn(async move {
        let outcome = tokio::select! {
            status = child.wait() => match status {
                Ok(status) => WorkerOutcome::Exited {
                    success: status.success(),
                    message: status.to_string(),
                },
                Err(error) => WorkerOutcome::Exited {
                    success: false,
                    message: error.to_string(),
                },
            },
            command = commands.recv() => match command {
                Some(WorkerCommand::Stop { completed }) => {
                    let result = stop_child(&mut child, &mut control).await;
                    let _ignored = completed.send(result);
                    WorkerOutcome::Stopped
                }
                None => {
                    let _ignored = stop_child(&mut child, &mut control).await;
                    WorkerOutcome::Stopped
                }
            }
        };
        workers.lock().await.remove(&delivery_id);
        let _ignored = tokio::fs::remove_file(config_path).await;
        let _ignored = events.send(WorkerEvent {
            delivery_id,
            outcome,
        });
    });
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
    tokio::fs::write(path, contents).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
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
