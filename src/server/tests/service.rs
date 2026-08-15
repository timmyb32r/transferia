use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

use super::*;

struct TestSupervisor {
    events: tokio::sync::broadcast::Sender<WorkerEvent>,
    shutdown: Arc<AtomicBool>,
}

impl TestSupervisor {
    fn new() -> Self {
        let (events, _) = tokio::sync::broadcast::channel(8);
        Self {
            events,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl WorkerSupervisor for TestSupervisor {
    async fn start(
        &self,
        _delivery_id: &str,
        _config_yaml: &str,
    ) -> Result<super::super::supervisor::WorkerInfo, SupervisorError> {
        Err(SupervisorError::Startup("not configured".to_owned()))
    }

    async fn stop(&self, delivery_id: &str) -> Result<(), SupervisorError> {
        Err(SupervisorError::NotRunning(delivery_id.to_owned()))
    }

    async fn shutdown_all(&self) -> Result<(), SupervisorError> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<WorkerEvent> {
        self.events.subscribe()
    }
}

#[tokio::test]
async fn shutdown_always_delegates_worker_termination() -> anyhow::Result<()> {
    let supervisor = Arc::new(TestSupervisor::new());
    let shutdown = Arc::clone(&supervisor.shutdown);
    let service = ControlPlane::new(Arc::new(MemoryStore::default()), supervisor);
    service.shutdown().await?;
    assert!(shutdown.load(Ordering::SeqCst));
    Ok(())
}

fn service() -> ControlPlane {
    ControlPlane::new(
        Arc::new(MemoryStore::default()),
        Arc::new(TestSupervisor::new()),
    )
}

#[derive(Default)]
struct MemoryStore {
    deliveries: Mutex<BTreeMap<String, DeliveryRecord>>,
}

#[async_trait]
impl DeliveryStore for MemoryStore {
    async fn list(&self) -> Result<Vec<DeliveryRecord>, StoreError> {
        Ok(self.deliveries.lock().await.values().cloned().collect())
    }

    async fn get(&self, id: &str) -> Result<DeliveryRecord, StoreError> {
        self.deliveries
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.to_owned()))
    }

    async fn insert(&self, delivery: DeliveryRecord) -> Result<(), StoreError> {
        let mut deliveries = self.deliveries.lock().await;
        if deliveries.contains_key(&delivery.id) {
            return Err(StoreError::AlreadyExists(delivery.id));
        }
        deliveries.insert(delivery.id.clone(), delivery);
        drop(deliveries);
        Ok(())
    }

    async fn replace(
        &self,
        delivery: DeliveryRecord,
        expected_revision: u64,
    ) -> Result<(), StoreError> {
        let mut deliveries = self.deliveries.lock().await;
        let current = deliveries
            .get(&delivery.id)
            .ok_or_else(|| StoreError::NotFound(delivery.id.clone()))?;
        if current.revision != expected_revision {
            return Err(StoreError::RevisionConflict {
                id: delivery.id,
                expected: expected_revision,
                actual: current.revision,
            });
        }
        deliveries.insert(delivery.id.clone(), delivery);
        drop(deliveries);
        Ok(())
    }
}

#[tokio::test]
async fn editing_increments_revision_and_invalidates_validation() -> anyhow::Result<()> {
    let service = service();
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    let updated = service
        .update_draft(
            &created.id,
            created.revision,
            "changed".to_owned(),
            "description".to_owned(),
            serde_json::json!({"source": {}}),
        )
        .await?;

    assert_eq!(updated.revision, 2);
    assert_eq!(updated.description, "description");
    assert_eq!(updated.validation, ValidationState::Draft);
    assert_eq!(updated.runtime, RuntimeState::Stopped);
    Ok(())
}

#[tokio::test]
async fn stale_update_is_rejected_without_overwriting_newer_draft() -> anyhow::Result<()> {
    let service = service();
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    service
        .update_draft(
            &created.id,
            created.revision,
            "newer".to_owned(),
            String::new(),
            serde_json::json!({"revision": 2}),
        )
        .await?;

    assert!(matches!(
        service
            .update_draft(
                &created.id,
                created.revision,
                "stale".to_owned(),
                String::new(),
                serde_json::json!({"revision": 1}),
            )
            .await,
        Err(ServiceError::Conflict(_))
    ));
    assert_eq!(service.get(&created.id).await?.name, "newer");
    Ok(())
}
