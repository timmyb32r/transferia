use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::model::{DeliveryRecord, RuntimeState, ValidationState};
use super::store::{DeliveryStore, StoreError};
use super::supervisor::{SupervisorError, WorkerEvent, WorkerOutcome, WorkerSupervisor};
use transferia::application::delivery_plan::build_delivery_plan;
use transferia::config::yaml::Config;
use transferia::delivery::{DeliveryDiscovery, SinkLimitsDescription};
use transferia::providers::traits::SinkProvider;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Validation(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<StoreError> for ServiceError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(id) => Self::NotFound(format!("delivery '{id}' does not exist")),
            StoreError::AlreadyExists(id) => {
                Self::Conflict(format!("delivery '{id}' already exists"))
            }
            StoreError::RevisionConflict {
                id,
                expected,
                actual,
            } => Self::Conflict(format!(
                "delivery '{id}' changed: expected revision {expected}, current revision {actual}"
            )),
            StoreError::Internal(error) => Self::Internal(error),
        }
    }
}

impl From<SupervisorError> for ServiceError {
    fn from(error: SupervisorError) -> Self {
        match error {
            SupervisorError::AlreadyRunning(id) => {
                Self::Conflict(format!("delivery '{id}' is already running"))
            }
            SupervisorError::NotRunning(id) => {
                Self::Conflict(format!("delivery '{id}' is not running"))
            }
            SupervisorError::Startup(message) => Self::Validation(message),
            SupervisorError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DiscoveryResult {
    pub source: String,
    pub sink: String,
    pub datasets: Vec<DatasetView>,
    pub sink_limits: SinkLimitsDescription,
}

#[derive(Clone, Debug, Serialize)]
pub struct DatasetView {
    pub role: String,
    pub name: String,
    pub columns: Vec<ColumnView>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnView {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub low_cardinality: bool,
    pub max_length: Option<usize>,
}

pub struct ControlPlane {
    store: Arc<dyn DeliveryStore>,
    supervisor: Arc<dyn WorkerSupervisor>,
    mutation: Mutex<()>,
}

impl ControlPlane {
    #[must_use]
    pub fn new(store: Arc<dyn DeliveryStore>, supervisor: Arc<dyn WorkerSupervisor>) -> Self {
        Self {
            store,
            supervisor,
            mutation: Mutex::new(()),
        }
    }

    pub async fn list(&self) -> Result<Vec<DeliveryRecord>, ServiceError> {
        self.store.list().await.map_err(Into::into)
    }

    pub async fn get(&self, id: &str) -> Result<DeliveryRecord, ServiceError> {
        self.store.get(id).await.map_err(Into::into)
    }

    pub async fn create_draft(
        &self,
        name: String,
        config: Value,
    ) -> Result<DeliveryRecord, ServiceError> {
        let name = validate_name(&name)?;
        validate_draft_shape(&config)?;
        let _mutation = self.mutation.lock().await;
        let now = now_ms();
        let record = DeliveryRecord {
            id: new_id()?,
            name,
            config,
            revision: 1,
            validation: ValidationState::Draft,
            runtime: RuntimeState::Stopped,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.store.insert(record.clone()).await?;
        Ok(record)
    }

    pub async fn update_draft(
        &self,
        id: &str,
        expected_revision: u64,
        name: String,
        config: Value,
    ) -> Result<DeliveryRecord, ServiceError> {
        let name = validate_name(&name)?;
        validate_draft_shape(&config)?;
        let _mutation = self.mutation.lock().await;
        let mut record = self.store.get(id).await?;
        if record.runtime.is_running_or_transitioning() {
            return Err(ServiceError::Conflict(
                "stop the delivery before editing its configuration".to_owned(),
            ));
        }
        if record.revision != expected_revision {
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                record.revision
            )));
        }
        record.name = name;
        record.config = config;
        record.revision = record.revision.saturating_add(1);
        record.validation = ValidationState::Draft;
        record.runtime = RuntimeState::Stopped;
        record.updated_at_ms = now_ms();
        self.store
            .replace(record.clone(), expected_revision)
            .await?;
        Ok(record)
    }

    pub async fn validate_preview(
        &self,
        config: &Value,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryResult, ServiceError> {
        let yaml = config_yaml_from_json(config)?;
        let parsed = Config::from_yaml(&yaml)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let plan = build_delivery_plan(parsed, cancellation)
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        Ok(discovery_result(
            plan.source_kind,
            plan.sink_kind,
            &plan.discovery,
            plan.sink_provider.as_ref(),
        ))
    }

    pub async fn validate_saved(
        &self,
        id: &str,
        expected_revision: u64,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryResult, ServiceError> {
        let snapshot = self.store.get(id).await?;
        if snapshot.revision != expected_revision {
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                snapshot.revision
            )));
        }
        let result = self.validate_preview(&snapshot.config, cancellation).await;
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.revision != expected_revision {
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' changed while validation was running"
            )));
        }
        current.validation = match &result {
            Ok(_) => ValidationState::Ready {
                revision: expected_revision,
            },
            Err(error) => ValidationState::Invalid {
                revision: expected_revision,
                message: error.to_string(),
            },
        };
        current.updated_at_ms = now_ms();
        self.store.replace(current, expected_revision).await?;
        result
    }

    pub async fn activate(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<DeliveryRecord, ServiceError> {
        let snapshot = {
            let _mutation = self.mutation.lock().await;
            let mut record = self.store.get(id).await?;
            if record.revision != expected_revision {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                    record.revision
                )));
            }
            if record.runtime.is_running_or_transitioning() {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' is already running or changing state"
                )));
            }
            if record.validation
                != (ValidationState::Ready {
                    revision: expected_revision,
                })
            {
                return Err(ServiceError::Conflict(
                    "validate the current delivery revision before activation".to_owned(),
                ));
            }
            record.runtime = RuntimeState::Starting;
            record.updated_at_ms = now_ms();
            self.store
                .replace(record.clone(), expected_revision)
                .await?;
            record
        };

        if let Err(error) = self
            .ensure_unique_runtime_delivery_id(&snapshot)
            .await
            .and_then(|()| config_yaml_from_json(&snapshot.config))
        {
            self.mark_failed(id, expected_revision, &error.to_string())
                .await?;
            return Err(error);
        }
        let discovery = self
            .validate_preview(&snapshot.config, CancellationToken::new())
            .await;
        if let Err(error) = discovery {
            self.mark_failed(id, expected_revision, &error.to_string())
                .await?;
            return Err(error);
        }
        let yaml = config_yaml_from_json(&snapshot.config)?;
        let worker = match self.supervisor.start(id, &yaml).await {
            Ok(worker) => worker,
            Err(error) => {
                let error = ServiceError::from(error);
                self.mark_failed(id, expected_revision, &error.to_string())
                    .await?;
                return Err(error);
            }
        };

        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.revision != expected_revision {
            let _ignored = self.supervisor.stop(id).await;
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' changed while activation was running"
            )));
        }
        if current.runtime != RuntimeState::Starting {
            return Err(ServiceError::Validation(
                "worker exited before activation completed".to_owned(),
            ));
        }
        current.runtime = RuntimeState::Running { pid: worker.pid };
        current.updated_at_ms = now_ms();
        if let Err(error) = self.store.replace(current.clone(), expected_revision).await {
            let _ignored = self.supervisor.stop(id).await;
            return Err(error.into());
        }
        Ok(current)
    }

    pub async fn stop(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<DeliveryRecord, ServiceError> {
        {
            let _mutation = self.mutation.lock().await;
            let mut record = self.store.get(id).await?;
            if record.revision != expected_revision {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                    record.revision
                )));
            }
            if !matches!(record.runtime, RuntimeState::Running { .. }) {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' is not running"
                )));
            }
            record.runtime = RuntimeState::Stopping;
            record.updated_at_ms = now_ms();
            self.store.replace(record, expected_revision).await?;
        }
        if let Err(error) = self.supervisor.stop(id).await {
            let error = ServiceError::from(error);
            self.mark_failed(id, expected_revision, &error.to_string())
                .await?;
            return Err(error);
        }
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        current.runtime = RuntimeState::Stopped;
        current.updated_at_ms = now_ms();
        self.store
            .replace(current.clone(), expected_revision)
            .await?;
        Ok(current)
    }

    pub fn spawn_supervisor_monitor(self: &Arc<Self>) {
        let mut events = self.supervisor.subscribe();
        let control_plane = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let event = match events.recv().await {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "worker event consumer lagged");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                let Some(control_plane) = control_plane.upgrade() else {
                    break;
                };
                if let Err(error) = control_plane.apply_worker_event(event).await {
                    tracing::error!(error = ?error, "failed to persist worker state change");
                }
            }
        });
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        self.supervisor.shutdown_all().await?;
        let _mutation = self.mutation.lock().await;
        for mut record in self.store.list().await? {
            if record.runtime.is_running_or_transitioning() {
                record.runtime = RuntimeState::Stopped;
                record.updated_at_ms = now_ms();
                let revision = record.revision;
                self.store.replace(record, revision).await?;
            }
        }
        Ok(())
    }

    pub fn render_yaml(config: &Value) -> Result<String, ServiceError> {
        serde_yaml::to_string(config)
            .map_err(anyhow::Error::from)
            .map_err(ServiceError::Internal)
    }

    async fn ensure_unique_runtime_delivery_id(
        &self,
        candidate: &DeliveryRecord,
    ) -> Result<(), ServiceError> {
        let yaml = config_yaml_from_json(&candidate.config)?;
        let configured_id = Config::from_yaml(&yaml)
            .map_err(|error| ServiceError::Validation(error.to_string()))?
            .delivery_id;
        for existing in self.store.list().await? {
            if existing.id == candidate.id {
                continue;
            }
            let Ok(existing_yaml) = config_yaml_from_json(&existing.config) else {
                continue;
            };
            let Ok(existing_config) = Config::from_yaml(&existing_yaml) else {
                continue;
            };
            if existing_config.delivery_id == configured_id {
                return Err(ServiceError::Conflict(format!(
                    "delivery_id '{configured_id}' is already used by delivery '{}'",
                    existing.name
                )));
            }
        }
        Ok(())
    }

    async fn mark_failed(
        &self,
        id: &str,
        expected_revision: u64,
        message: &str,
    ) -> Result<(), ServiceError> {
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.revision != expected_revision {
            return Ok(());
        }
        current.runtime = RuntimeState::Failed {
            message: message.to_owned(),
        };
        current.updated_at_ms = now_ms();
        self.store.replace(current, expected_revision).await?;
        Ok(())
    }

    async fn apply_worker_event(&self, event: WorkerEvent) -> Result<(), ServiceError> {
        let _mutation = self.mutation.lock().await;
        let mut record = match self.store.get(&event.delivery_id).await {
            Ok(record) => record,
            Err(StoreError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        record.runtime = match event.outcome {
            WorkerOutcome::Stopped | WorkerOutcome::Exited { success: true, .. } => {
                RuntimeState::Stopped
            }
            WorkerOutcome::Exited {
                success: false,
                message,
            } => RuntimeState::Failed { message },
        };
        record.updated_at_ms = now_ms();
        let revision = record.revision;
        self.store.replace(record, revision).await?;
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<String, ServiceError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ServiceError::InvalidInput(
            "delivery name must not be empty".to_owned(),
        ));
    }
    if trimmed.len() > 128 {
        return Err(ServiceError::InvalidInput(
            "delivery name must contain at most 128 bytes".to_owned(),
        ));
    }
    Ok(trimmed.to_owned())
}

fn validate_draft_shape(config: &Value) -> Result<(), ServiceError> {
    if !config.is_object() {
        return Err(ServiceError::InvalidInput(
            "delivery configuration must be a JSON object".to_owned(),
        ));
    }
    Ok(())
}

fn config_yaml_from_json(config: &Value) -> Result<String, ServiceError> {
    validate_draft_shape(config)?;
    ControlPlane::render_yaml(config)
}

fn discovery_result(
    source: String,
    sink: String,
    discovery: &DeliveryDiscovery,
    sink_provider: &dyn SinkProvider,
) -> DiscoveryResult {
    DiscoveryResult {
        source,
        sink,
        datasets: discovery
            .datasets
            .iter()
            .map(|dataset| DatasetView {
                role: format!("{:?}", dataset.role),
                name: dataset.name.to_string(),
                columns: dataset
                    .stored_schema
                    .columns
                    .iter()
                    .map(|column| ColumnView {
                        name: column.name.clone(),
                        arrow_type: format!("{:?}", column.data_type),
                        nullable: column.nullable,
                        primary_key: column.primary_key,
                        low_cardinality: column.low_cardinality,
                        max_length: column.max_length,
                    })
                    .collect(),
            })
            .collect(),
        sink_limits: sink_provider.limits().description(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn new_id() -> Result<String, ServiceError> {
    use std::fmt::Write as _;

    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(anyhow::Error::from)?;
    let mut id = String::with_capacity(32);
    for byte in bytes {
        write!(id, "{byte:02x}").map_err(anyhow::Error::from)?;
    }
    Ok(id)
}

#[cfg(test)]
#[path = "tests/service.rs"]
mod tests;
