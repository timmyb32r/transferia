use async_trait::async_trait;
use tokio::sync::broadcast;

use super::supervisor::{SupervisorError, WorkerEvent, WorkerInfo, WorkerSupervisor};

pub(super) struct TestSupervisor {
    events: broadcast::Sender<WorkerEvent>,
}

impl TestSupervisor {
    pub(super) fn new() -> Self {
        let (events, _) = broadcast::channel(8);
        Self { events }
    }
}

#[async_trait]
impl WorkerSupervisor for TestSupervisor {
    async fn start(
        &self,
        _delivery_id: &str,
        _config_yaml: &str,
    ) -> Result<WorkerInfo, SupervisorError> {
        Err(SupervisorError::Startup("not configured".to_owned()))
    }

    async fn stop(&self, delivery_id: &str) -> Result<(), SupervisorError> {
        Err(SupervisorError::NotRunning(delivery_id.to_owned()))
    }

    async fn shutdown_all(&self) -> Result<(), SupervisorError> {
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<WorkerEvent> {
        self.events.subscribe()
    }
}
