use async_trait::async_trait;
use std::sync::Mutex;

use tokio::sync::mpsc;

use super::model::RunId;
use super::supervisor::{SupervisorError, WorkerEvent, WorkerInfo, WorkerSupervisor};

mod ui_catalog_contract;

pub(super) struct TestSupervisor {
    events: Mutex<Option<mpsc::UnboundedReceiver<WorkerEvent>>>,
}

impl TestSupervisor {
    pub(super) fn new() -> Self {
        let (_sender, events) = mpsc::unbounded_channel();
        Self {
            events: Mutex::new(Some(events)),
        }
    }
}

#[async_trait]
impl WorkerSupervisor for TestSupervisor {
    async fn start(
        &self,
        _delivery_id: &str,
        _run_id: &RunId,
        _config_yaml: &str,
        _composition_fingerprint: &str,
    ) -> Result<WorkerInfo, SupervisorError> {
        Err(SupervisorError::Startup("not configured".to_owned()))
    }

    async fn stop(&self, delivery_id: &str, _run_id: &RunId) -> Result<(), SupervisorError> {
        Err(SupervisorError::NotRunning(delivery_id.to_owned()))
    }

    async fn shutdown_all(&self) -> Result<(), SupervisorError> {
        Ok(())
    }

    fn take_events(&self) -> Result<mpsc::UnboundedReceiver<WorkerEvent>, SupervisorError> {
        self.events
            .lock()
            .expect("events mutex is healthy")
            .take()
            .ok_or(SupervisorError::EventsAlreadyTaken)
    }
}
