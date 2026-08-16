use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::delivery::preparation::ResolvedDeliveryConfig;

pub mod local;

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RunId(pub String);

#[derive(Clone, Debug)]
pub enum WorkerOutcome {
    Stopped,
    Exited { success: bool, message: String },
}

#[derive(Clone, Debug)]
pub struct WorkerEvent {
    pub delivery_id: String,

    pub run_id: RunId,

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
    #[error("delivery '{delivery_id}' is running under a different run id")]
    RunMismatch { delivery_id: String },
    #[error("worker startup failed: {0}")]
    Startup(String),
    #[error("worker startup was cancelled")]
    StartupCancelled,
    #[error("worker supervisor is shutting down")]
    ShuttingDown,
    #[error("worker stop failed: {0}")]
    Stop(String),
    #[error("worker event receiver was already taken")]
    EventsAlreadyTaken,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
pub trait WorkerSupervisor: Send + Sync {
    async fn start(
        &self,
        delivery_id: &str,
        run_id: &RunId,
        config: &ResolvedDeliveryConfig,
    ) -> Result<WorkerInfo, SupervisorError>;

    async fn stop(&self, delivery_id: &str, run_id: &RunId) -> Result<(), SupervisorError>;

    async fn shutdown_all(&self) -> Result<(), SupervisorError>;

    fn take_events(&self) -> Result<mpsc::UnboundedReceiver<WorkerEvent>, SupervisorError>;
}
