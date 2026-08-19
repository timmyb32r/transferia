use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;

use super::*;
use transferia_runtime::WorkerInfo;

struct TestSupervisor {
    events: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<WorkerEvent>>>,
    shutdown: Arc<AtomicBool>,
    stop_calls: Arc<AtomicUsize>,
}

impl TestSupervisor {
    fn new() -> Self {
        let (_sender, events) = tokio::sync::mpsc::unbounded_channel();
        Self {
            events: std::sync::Mutex::new(Some(events)),
            shutdown: Arc::new(AtomicBool::new(false)),
            stop_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl WorkerSupervisor for TestSupervisor {
    async fn start(
        &self,
        _delivery_id: &str,
        _run_id: &RunId,
        _config: &transferia_runtime::WorkerLaunchSpec,
    ) -> Result<WorkerInfo, SupervisorError> {
        Err(SupervisorError::Startup("not configured".to_owned()))
    }

    async fn stop(&self, delivery_id: &str, _run_id: &RunId) -> Result<(), SupervisorError> {
        self.stop_calls.fetch_add(1, Ordering::SeqCst);
        Err(SupervisorError::NotRunning(delivery_id.to_owned()))
    }

    async fn shutdown_all(&self) -> Result<(), SupervisorError> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn take_events(
        &self,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<WorkerEvent>, SupervisorError> {
        self.events
            .lock()
            .expect("events mutex is healthy")
            .take()
            .ok_or(SupervisorError::EventsAlreadyTaken)
    }
}

#[tokio::test]
async fn shutdown_always_delegates_worker_termination() -> anyhow::Result<()> {
    let supervisor = Arc::new(TestSupervisor::new());
    let shutdown = Arc::clone(&supervisor.shutdown);
    let service = ControlPlane::new(
        Arc::new(MemoryStore::default()),
        supervisor,
        transferia_providers::extension::Transferia::public()?,
    );
    service.shutdown().await?;
    assert!(shutdown.load(Ordering::SeqCst));
    Ok(())
}

fn service() -> ControlPlane {
    ControlPlane::new(
        Arc::new(MemoryStore::default()),
        Arc::new(TestSupervisor::new()),
        transferia_providers::extension::Transferia::public().unwrap(),
    )
}

#[tokio::test]
async fn source_schema_preview_does_not_require_a_sink() -> anyhow::Result<()> {
    let result = service()
        .source_schema_preview(
            &serde_json::json!({
                "delivery_type": "stream",
                "source": {
                    "kafka": {
                        "installation": {
                            "type": "on_premise",
                            "brokers": ["localhost:9092"],
                            "security": { "type": "plaintext" }
                        },
                        "topics": ["events"],
                        "consumer_group": "consumer",
                        "offset_reset": "earliest",
                        "parser": {
                            "common": {
                                "table_naming": { "type": "from_config", "name": "events" }
                            },
                            "json_parser": {
                                "columns": [{
                                    "jsonpath": "$.id",
                                    "column_name": "id",
                                    "json_data_type": "number",
                                    "arrow_type": "Int64",
                                    "nullable": false
                                }],
                                "conversion_error": "drop",
                                "unknown_fields": { "action": "drop" }
                            }
                        },
                        "batch_max_messages": 1000,
                        "batch_max_bytes": 16_777_216,
                        "request_timeout_ms": 30000
                    }
                },
                "sink": {
                    "clickhouse": {
                        "installation": {
                            "type": "on_premise",
                            "hosts": [""],
                            "port": 9440
                        }
                    }
                }
            }),
            CancellationToken::new(),
        )
        .await?;

    assert_eq!(result.source, "kafka");
    assert_eq!(result.sink, "unselected");
    assert_eq!(result.datasets[0].name, "events");
    Ok(())
}

#[tokio::test]
async fn message_preview_race_returns_the_first_successful_endpoint() -> anyhow::Result<()> {
    let mut attempts = tokio::task::JoinSet::new();
    attempts.spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        Ok::<_, anyhow::Error>("slow")
    });
    attempts.spawn(async { Ok::<_, anyhow::Error>("fast") });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_successful_preview(&mut attempts, &CancellationToken::new()),
    )
    .await??;

    assert_eq!(result, "fast");
    attempts.abort_all();
    Ok(())
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
        if current.record_version != expected_revision {
            return Err(StoreError::RecordVersionConflict {
                id: delivery.id,
                expected: expected_revision,
                actual: current.record_version,
            });
        }
        if delivery.record_version != expected_revision.saturating_add(1) {
            return Err(StoreError::InvalidRecordVersion {
                id: delivery.id,
                expected: expected_revision.saturating_add(1),
                actual: delivery.record_version,
            });
        }
        deliveries.insert(delivery.id.clone(), delivery);
        drop(deliveries);
        Ok(())
    }

    async fn delete(
        &self,
        id: &str,
        expected_record_version: u64,
    ) -> Result<DeliveryRecord, StoreError> {
        let mut deliveries = self.deliveries.lock().await;
        let current = deliveries
            .get(id)
            .ok_or_else(|| StoreError::NotFound(id.to_owned()))?;
        if current.record_version != expected_record_version {
            return Err(StoreError::RecordVersionConflict {
                id: id.to_owned(),
                expected: expected_record_version,
                actual: current.record_version,
            });
        }
        deliveries
            .remove(id)
            .ok_or_else(|| StoreError::NotFound(id.to_owned()))
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
            created.record_version,
            "changed".to_owned(),
            "description".to_owned(),
            serde_json::json!({"source": {}}),
        )
        .await?;

    assert_eq!(updated.revision, 2);
    assert_eq!(updated.description, "description");
    assert_eq!(updated.validation, ValidationState::Draft);
    assert_eq!(created.runtime, RuntimeState::Created);
    assert_eq!(updated.runtime, RuntimeState::Created);
    Ok(())
}

#[tokio::test]
async fn deleting_a_draft_is_versioned_and_removes_it() -> anyhow::Result<()> {
    let service = service();
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;

    let deleted = service
        .delete(&created.id, created.revision, created.record_version)
        .await?;

    assert_eq!(deleted, created);
    assert!(matches!(
        service.get(&created.id).await,
        Err(ServiceError::NotFound(_))
    ));
    Ok(())
}

#[tokio::test]
async fn delivery_names_are_never_silently_trimmed() -> anyhow::Result<()> {
    let service = service();
    for name in [" leading", "trailing ", "\tname", "name\n"] {
        let error = service
            .create_draft(name.to_owned(), String::new(), serde_json::json!({}))
            .await
            .expect_err("surrounding whitespace must be rejected");
        assert!(
            matches!(error, ServiceError::InvalidInput(message) if message.contains("leading or trailing whitespace"))
        );
    }
    assert!(service.list().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_validation_returns_the_committed_record_version() -> anyhow::Result<()> {
    let service = service();
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;

    let result = service
        .validate_saved(
            &created.id,
            created.revision,
            created.record_version,
            CancellationToken::new(),
        )
        .await?;

    assert!(result.discovery.is_none());
    assert_eq!(result.delivery.record_version, created.record_version + 1);
    assert!(matches!(
        result.delivery.validation,
        ValidationState::Invalid { revision: 1, .. }
    ));
    assert_eq!(service.get(&created.id).await?, result.delivery);
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
            created.record_version,
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
                created.record_version,
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

#[tokio::test]
async fn runtime_only_change_invalidates_a_draft_update_request() -> anyhow::Result<()> {
    let store = Arc::new(MemoryStore::default());
    let service = ControlPlane::new(
        store.clone(),
        Arc::new(TestSupervisor::new()),
        transferia_providers::extension::Transferia::public()?,
    );
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    let mut changed = created.clone();
    changed.record_version += 1;
    changed.validation = ValidationState::Invalid {
        revision: created.revision,
        message: "runtime validation changed".to_owned(),
    };
    store
        .replace(changed.clone(), created.record_version)
        .await?;

    assert!(matches!(
        service
            .update_draft(
                &created.id,
                created.revision,
                created.record_version,
                "stale".to_owned(),
                String::new(),
                serde_json::json!({}),
            )
            .await,
        Err(ServiceError::Conflict(_))
    ));
    assert_eq!(store.get(&created.id).await?, changed);
    Ok(())
}

#[tokio::test]
async fn stale_worker_event_cannot_overwrite_a_newer_run() -> anyhow::Result<()> {
    let store = Arc::new(MemoryStore::default());
    let service = ControlPlane::new(
        store.clone(),
        Arc::new(TestSupervisor::new()),
        transferia_providers::extension::Transferia::public()?,
    );
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    let new_run = RunId("new-run".to_owned());
    let mut running = store.get(&created.id).await?;
    let expected_record_version = running.record_version;
    running.record_version += 1;
    running.runtime = RuntimeState::Running {
        run_id: new_run.clone(),
        pid: 42,
    };
    store.replace(running, expected_record_version).await?;

    service
        .apply_worker_event(WorkerEvent {
            delivery_id: created.id.clone(),
            run_id: RunId("old-run".to_owned()),
            outcome: WorkerOutcome::Exited {
                success: false,
                message: "old worker failed".to_owned(),
            },
        })
        .await?;

    assert_eq!(
        service.get(&created.id).await?.runtime,
        RuntimeState::Running {
            run_id: new_run,
            pid: 42,
        }
    );
    Ok(())
}

#[tokio::test]
async fn stale_stop_request_cannot_stop_a_newer_run() -> anyhow::Result<()> {
    let store = Arc::new(MemoryStore::default());
    let supervisor = Arc::new(TestSupervisor::new());
    let stop_calls = Arc::clone(&supervisor.stop_calls);
    let service = ControlPlane::new(
        store.clone(),
        supervisor,
        transferia_providers::extension::Transferia::public()?,
    );
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    let current_run = RunId("run-b".to_owned());
    let mut running = store.get(&created.id).await?;
    let previous_record_version = running.record_version;
    running.record_version += 1;
    running.runtime = RuntimeState::Running {
        run_id: current_run.clone(),
        pid: 42,
    };
    store
        .replace(running.clone(), previous_record_version)
        .await?;

    assert!(matches!(
        service
            .stop(
                &created.id,
                running.revision,
                running.record_version,
                &RunId("run-a".to_owned()),
            )
            .await,
        Err(ServiceError::Conflict(_))
    ));
    assert_eq!(stop_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.get(&created.id).await?.runtime, running.runtime);
    Ok(())
}

#[tokio::test]
async fn delayed_stop_event_cannot_overwrite_a_terminal_stop_failure() -> anyhow::Result<()> {
    let store = Arc::new(MemoryStore::default());
    let service = ControlPlane::new(
        store.clone(),
        Arc::new(TestSupervisor::new()),
        transferia_providers::extension::Transferia::public()?,
    );
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    let run_id = RunId("failed-stop".to_owned());
    let mut stopping = store.get(&created.id).await?;
    let expected_record_version = stopping.record_version;
    stopping.record_version += 1;
    stopping.runtime = RuntimeState::Stopping {
        run_id: run_id.clone(),
    };
    store.replace(stopping, expected_record_version).await?;
    service
        .mark_failed(&created.id, &run_id, "kill failed")
        .await?;

    service
        .apply_worker_event(WorkerEvent {
            delivery_id: created.id.clone(),
            run_id: run_id.clone(),
            outcome: WorkerOutcome::Stopped,
        })
        .await?;

    assert_eq!(
        service.get(&created.id).await?.runtime,
        RuntimeState::Failed {
            run_id,
            message: "kill failed".to_owned(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_start_event_moves_starting_run_to_stopped() -> anyhow::Result<()> {
    let store = Arc::new(MemoryStore::default());
    let service = ControlPlane::new(
        store.clone(),
        Arc::new(TestSupervisor::new()),
        transferia_providers::extension::Transferia::public()?,
    );
    let created = service
        .create_draft("test".to_owned(), String::new(), serde_json::json!({}))
        .await?;
    let run_id = RunId("cancelled-start".to_owned());
    let mut starting = store.get(&created.id).await?;
    let expected_record_version = starting.record_version;
    starting.record_version += 1;
    starting.runtime = RuntimeState::Starting {
        run_id: run_id.clone(),
    };
    store.replace(starting, expected_record_version).await?;

    service
        .apply_worker_event(WorkerEvent {
            delivery_id: created.id.clone(),
            run_id,
            outcome: WorkerOutcome::Stopped,
        })
        .await?;

    assert_eq!(
        service.get(&created.id).await?.runtime,
        RuntimeState::Stopped
    );
    Ok(())
}
