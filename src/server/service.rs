use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::model::{DeliveryRecord, RuntimeState, ValidationState};
use super::store::{DeliveryStore, StoreError};
use transferia::core::delivery::{DatasetRole, DeliveryDiscovery, SinkLimitsDescription};
use transferia::delivery::config::yaml::Config;
use transferia::delivery::preparation::build_delivery_plan_with;
use transferia::extension::{DynamicOptions, OptionsRequest, Transferia};
use transferia::providers::traits::SinkProvider;
use transferia::runtime::{RunId, SupervisorError, WorkerEvent, WorkerOutcome, WorkerSupervisor};

const MAX_MESSAGE_PREVIEW_BYTES: usize = 32 * 1024 * 1024;
const INLINE_MESSAGE_PREVIEW_BYTES: usize = 16 * 1024;

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
            StoreError::RecordVersionConflict {
                id,
                expected,
                actual,
            } => Self::Conflict(format!(
                "delivery '{id}' changed: expected record version {expected}, current record version {actual}"
            )),
            StoreError::InvalidRecordVersion {
                id,
                expected,
                actual,
            } => Self::Internal(anyhow::anyhow!(
                "invalid record version for delivery '{id}': expected {expected}, got {actual}"
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
            SupervisorError::RunMismatch { delivery_id } => Self::Conflict(format!(
                "delivery '{delivery_id}' is running under a different run id"
            )),
            SupervisorError::Startup(message) => Self::Validation(message),
            SupervisorError::StartupCancelled => {
                Self::Validation("worker startup was cancelled".to_owned())
            }
            SupervisorError::ShuttingDown => {
                Self::Conflict("worker supervisor is shutting down".to_owned())
            }
            SupervisorError::Stop(message) => Self::Internal(anyhow::anyhow!(message)),
            SupervisorError::EventsAlreadyTaken => {
                Self::Internal(anyhow::anyhow!("worker event receiver was already taken"))
            }
            SupervisorError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryResult {
    pub source: String,

    pub sink: String,

    pub pipeline_count: usize,

    pub datasets: Vec<DatasetView>,

    pub sink_limits: SinkLimitsDescription,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationCommandResult {
    pub delivery: DeliveryRecord,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub discovery: Option<DiscoveryResult>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetView {
    pub role: DatasetRoleView,
    pub name: String,
    pub intermediate_columns: Vec<ColumnView>,
    pub final_columns: Vec<DestinationColumnView>,
}

#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
pub enum DatasetRoleView {
    Main,
    DeadLetterQueue,
}

impl From<DatasetRole> for DatasetRoleView {
    fn from(role: DatasetRole) -> Self {
        match role {
            DatasetRole::Main => Self::Main,
            DatasetRole::DeadLetterQueue => Self::DeadLetterQueue,
        }
    }
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnView {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub low_cardinality: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub max_length: Option<usize>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationColumnView {
    #[serde(flatten)]
    pub column: ColumnView,

    pub destination_type: String,
}

pub struct ControlPlane {
    store: Arc<dyn DeliveryStore>,
    supervisor: Arc<dyn WorkerSupervisor>,
    transferia: Transferia,
    mutation: Mutex<()>,
    shutdown: CancellationToken,
}

impl ControlPlane {
    pub async fn sql_playground(
        &self,
        sql: String,
        rows: Vec<serde_json::Value>,
    ) -> Result<SqlPlaygroundResult, ServiceError> {
        let middleware = crate::middleware::datafusion::DataFusionMiddleware::new(sql)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let (batch, rows) = middleware
            .execute_json_rows(&rows)
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| ColumnView {
                name: field.name().clone(),
                arrow_type: format!("{:?}", field.data_type()),
                nullable: field.is_nullable(),
                primary_key: false,
                low_cardinality: false,
                max_length: None,
            })
            .collect();
        Ok(SqlPlaygroundResult { columns, rows })
    }

    #[must_use]
    pub fn new(
        store: Arc<dyn DeliveryStore>,
        supervisor: Arc<dyn WorkerSupervisor>,
        transferia: Transferia,
    ) -> Self {
        Self {
            store,
            supervisor,
            transferia,
            mutation: Mutex::new(()),
            shutdown: CancellationToken::new(),
        }
    }

    pub async fn dynamic_options(
        &self,
        key: &str,
        request: OptionsRequest,
        cancellation: CancellationToken,
    ) -> Result<DynamicOptions, ServiceError> {
        self.transferia
            .registry()
            .options(key, request, cancellation)
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))
    }

    pub async fn check_connection(
        &self,
        provider: &str,
        role: crate::extension::EndpointRole,
        config: Value,
        cancellation: CancellationToken,
    ) -> Result<crate::providers::traits::ConnectionCheckResult, ServiceError> {
        let total_started = std::time::Instant::now();
        let raw = serde_yaml::to_value(config)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let resolve_started = std::time::Instant::now();
        let resolved = self
            .transferia
            .registry()
            .resolve_many(provider, role, raw, cancellation.clone())
            .await;
        tracing::info!(
            provider,
            ?role,
            stage = "installation_resolution",
            elapsed_ms = resolve_started.elapsed().as_millis(),
            success = resolved.is_ok(),
            "connection check stage completed"
        );
        let resolved = resolved.map_err(|error| ServiceError::Validation(error.to_string()))?;
        let catalog = crate::providers::catalog::build_provider_catalog_with(
            &self.transferia,
            &Arc::new(crate::metrics::MetricsRegistry::new()),
        )
        .map_err(ServiceError::Internal)?;
        let check_started = std::time::Instant::now();
        let result = async {
            let mut combined = crate::providers::traits::ConnectionCheckResult::default();
            for endpoint in resolved {
                let checked = tokio::select! {
                    () = cancellation.cancelled() => return Err(ServiceError::Validation("connection check cancelled".to_owned())),
                    result = catalog.check_connection(provider, role, endpoint) => {
                        result.map_err(|error| ServiceError::Validation(error.to_string()))?
                    }
                };
                for (key, values) in checked.options {
                    let combined_values = combined.options.entry(key).or_default();
                    for value in values {
                        if !combined_values.contains(&value) {
                            combined_values.push(value);
                        }
                    }
                }
            }
            Ok(combined)
        };
        let result = result.await;
        tracing::info!(
            provider,
            ?role,
            stage = "provider_connection_check",
            elapsed_ms = check_started.elapsed().as_millis(),
            total_elapsed_ms = total_started.elapsed().as_millis(),
            success = result.is_ok(),
            "connection check completed"
        );
        result
    }

    pub async fn preview_message(
        &self,
        provider: &str,
        config: Value,
        max_bytes: usize,
        cancellation: CancellationToken,
    ) -> Result<MessagePreviewResult, ServiceError> {
        if !(1..=MAX_MESSAGE_PREVIEW_BYTES).contains(&max_bytes) {
            return Err(ServiceError::Validation(format!(
                "message preview max_bytes must be in 1..={MAX_MESSAGE_PREVIEW_BYTES}"
            )));
        }
        if provider != "logbroker" {
            return Err(ServiceError::Validation(format!(
                "{provider} source does not support message preview"
            )));
        }
        let raw = serde_yaml::to_value(config)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let resolved = self
            .transferia
            .registry()
            .resolve(
                provider,
                crate::extension::EndpointRole::Source,
                raw,
                cancellation.clone(),
            )
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let config: crate::providers::logbroker::src_stream::LogbrokerSourceConnectionConfig =
            serde_yaml::from_value(resolved).map_err(|error| {
                ServiceError::Validation(format!("invalid source configuration: {error}"))
            })?;
        let preview =
            crate::providers::logbroker::preview_message(&config, max_bytes, cancellation)
                .await
                .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let preview_bytes = preview.payload.len().min(INLINE_MESSAGE_PREVIEW_BYTES);
        let detection_payloads = preview
            .detection_payloads
            .iter()
            .map(bytes::Bytes::as_ref)
            .collect::<Vec<_>>();
        Ok(MessagePreviewResult {
            text_preview: String::from_utf8_lossy(&preview.payload[..preview_bytes]).into_owned(),
            payload_preview_base64: base64::engine::general_purpose::STANDARD
                .encode(&preview.payload[..preview_bytes]),
            payload_base64: base64::engine::general_purpose::STANDARD.encode(&preview.payload),
            byte_length: preview.payload.len(),
            preview_bytes,
            metadata: MessagePreviewMetadata::from(preview.metadata),
            detections: crate::parsers::detection::detect_samples(&detection_payloads, 1_000),
        })
    }

    #[must_use]
    pub fn request_cancellation(&self) -> CancellationToken {
        self.shutdown.child_token()
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
        description: String,
        config: Value,
    ) -> Result<DeliveryRecord, ServiceError> {
        let name = validate_name(&name)?;
        validate_draft_shape(&config)?;
        let _mutation = self.mutation.lock().await;
        let now = now_ms();
        let record = DeliveryRecord {
            id: new_id()?,
            name,
            description,
            config,
            revision: 1,
            record_version: 1,
            validation: ValidationState::Draft,
            runtime: RuntimeState::Created,
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
        expected_record_version: u64,
        name: String,
        description: String,
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
        ensure_record_version(id, record.record_version, expected_record_version)?;
        record.name = name;
        record.description = description;
        record.config = config;
        record.revision = next_version(record.revision)?;
        let expected_record_version = record.record_version;
        record.record_version = next_version(record.record_version)?;
        record.validation = ValidationState::Draft;
        if record.runtime != RuntimeState::Created {
            record.runtime = RuntimeState::Stopped;
        }
        record.updated_at_ms = now_ms();
        self.store
            .replace(record.clone(), expected_record_version)
            .await?;
        Ok(record)
    }

    pub async fn delete(
        &self,
        id: &str,
        expected_revision: u64,
        expected_record_version: u64,
    ) -> Result<DeliveryRecord, ServiceError> {
        let _mutation = self.mutation.lock().await;
        let record = self.store.get(id).await?;
        if record.revision != expected_revision {
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                record.revision
            )));
        }
        ensure_record_version(id, record.record_version, expected_record_version)?;
        if record.runtime.is_running_or_transitioning() {
            return Err(ServiceError::Conflict(
                "stop the delivery before deleting it".to_owned(),
            ));
        }
        self.store
            .delete(id, expected_record_version)
            .await
            .map_err(Into::into)
    }

    pub async fn validate_preview(
        &self,
        config: &Value,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryResult, ServiceError> {
        let yaml = config_yaml_from_json(config)?;
        let parsed = Config::from_yaml(&yaml)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let plan = tokio::select! {
            () = self.shutdown.cancelled() => {
                return Err(ServiceError::Conflict("the control plane is shutting down".to_owned()));
            }
            result = build_delivery_plan_with(parsed, cancellation, &self.transferia) => {
                result.map_err(|error| ServiceError::Validation(error.to_string()))?
            }
        };
        let primary = plan
            .primary()
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        discovery_result(
            primary.source_kind.clone(),
            primary.sink_kind.clone(),
            plan.pipelines.len(),
            &primary.discovery,
            primary.sink_provider.as_ref(),
        )
        .map_err(|error| ServiceError::Validation(error.to_string()))
    }

    pub async fn validate_saved(
        &self,
        id: &str,
        expected_revision: u64,
        expected_record_version: u64,
        cancellation: CancellationToken,
    ) -> Result<ValidationCommandResult, ServiceError> {
        let snapshot = self.store.get(id).await?;
        if snapshot.revision != expected_revision {
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                snapshot.revision
            )));
        }
        ensure_record_version(id, snapshot.record_version, expected_record_version)?;
        let result = self.validate_preview(&snapshot.config, cancellation).await;
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.revision != expected_revision
            || current.record_version != expected_record_version
        {
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
        let expected_record_version = current.record_version;
        current.record_version = next_version(current.record_version)?;
        current.updated_at_ms = now_ms();
        self.store
            .replace(current.clone(), expected_record_version)
            .await?;
        Ok(ValidationCommandResult {
            delivery: current,
            discovery: result.ok(),
        })
    }

    pub async fn activate(
        &self,
        id: &str,
        expected_revision: u64,
        expected_record_version: u64,
    ) -> Result<DeliveryRecord, ServiceError> {
        let snapshot = {
            let _mutation = self.mutation.lock().await;
            let record = self.store.get(id).await?;
            if record.revision != expected_revision {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                    record.revision
                )));
            }
            ensure_record_version(id, record.record_version, expected_record_version)?;
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
            record
        };

        self.ensure_unique_runtime_delivery_id(&snapshot).await?;
        let yaml = config_yaml_from_json(&snapshot.config)?;
        let parsed = Config::from_yaml(&yaml)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let plan = tokio::select! {
            () = self.shutdown.cancelled() => {
                return Err(ServiceError::Conflict("the control plane is shutting down".to_owned()));
            }
            result = build_delivery_plan_with(
                parsed,
                self.shutdown.child_token(),
                &self.transferia,
            ) => result.map_err(|error| ServiceError::Validation(error.to_string()))?,
        };
        let resolved = plan.resolved_config().map_err(ServiceError::Internal)?;
        let run_id = new_run_id()?;

        {
            let _mutation = self.mutation.lock().await;
            let mut current = self.store.get(id).await?;
            if current.revision != expected_revision
                || current.record_version != snapshot.record_version
            {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' changed while activation was being prepared"
                )));
            }
            current.runtime = RuntimeState::Starting {
                run_id: run_id.clone(),
            };
            let expected_record_version = current.record_version;
            current.record_version = next_version(current.record_version)?;
            current.updated_at_ms = now_ms();
            self.store.replace(current, expected_record_version).await?;
        }

        let worker = match self.supervisor.start(id, &run_id, &resolved).await {
            Ok(worker) => worker,
            Err(SupervisorError::StartupCancelled) => {
                self.mark_stopped(id, &run_id).await?;
                return Err(ServiceError::Validation(
                    "worker startup was cancelled".to_owned(),
                ));
            }
            Err(error) => {
                let error = ServiceError::from(error);
                if let Err(persist_error) = self.mark_failed(id, &run_id, &error.to_string()).await
                {
                    return Err(ServiceError::Internal(anyhow::anyhow!(
                        "worker startup failed: {error}; persisting the failed state also failed: {persist_error}"
                    )));
                }
                return Err(error);
            }
        };

        let mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.revision != expected_revision {
            let conflict = ServiceError::Conflict(format!(
                "delivery '{id}' changed while activation was running"
            ));
            drop(mutation);
            return match self.supervisor.stop(id, &run_id).await {
                Ok(()) => Err(conflict),
                Err(cleanup) => Err(compound_activation_error(&conflict, &cleanup)),
            };
        }
        if current.runtime
            != (RuntimeState::Starting {
                run_id: run_id.clone(),
            })
        {
            return Err(ServiceError::Validation(
                "worker exited before activation completed".to_owned(),
            ));
        }
        current.runtime = RuntimeState::Running {
            run_id: run_id.clone(),
            pid: worker.pid,
        };
        let expected_record_version = current.record_version;
        current.record_version = next_version(current.record_version)?;
        current.updated_at_ms = now_ms();
        if let Err(error) = self
            .store
            .replace(current.clone(), expected_record_version)
            .await
        {
            drop(mutation);
            let persist_error = ServiceError::from(error);
            return match self.supervisor.stop(id, &run_id).await {
                Ok(()) => Err(persist_error),
                Err(cleanup) => Err(compound_activation_error(&persist_error, &cleanup)),
            };
        }
        Ok(current)
    }

    pub async fn stop(
        &self,
        id: &str,
        expected_revision: u64,
        expected_record_version: u64,
        expected_run_id: &RunId,
    ) -> Result<DeliveryRecord, ServiceError> {
        let run_id = {
            let _mutation = self.mutation.lock().await;
            let mut record = self.store.get(id).await?;
            if record.revision != expected_revision {
                return Err(ServiceError::Conflict(format!(
                    "delivery '{id}' changed: expected revision {expected_revision}, current revision {}",
                    record.revision
                )));
            }
            ensure_record_version(id, record.record_version, expected_record_version)?;
            let run_id = match &record.runtime {
                RuntimeState::Running { run_id, .. } if run_id == expected_run_id => run_id.clone(),
                RuntimeState::Running { .. } => {
                    return Err(ServiceError::Conflict(format!(
                        "delivery '{id}' started a newer run"
                    )))
                }
                _ => {
                    return Err(ServiceError::Conflict(format!(
                        "delivery '{id}' is not running"
                    )))
                }
            };
            record.runtime = RuntimeState::Stopping {
                run_id: run_id.clone(),
            };
            let expected_record_version = record.record_version;
            record.record_version = next_version(record.record_version)?;
            record.updated_at_ms = now_ms();
            self.store.replace(record, expected_record_version).await?;
            run_id
        };
        if let Err(error) = self.supervisor.stop(id, &run_id).await {
            let error = ServiceError::from(error);
            self.mark_failed(id, &run_id, &error.to_string()).await?;
            return Err(error);
        }
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.runtime == RuntimeState::Stopped {
            return Ok(current);
        }
        if current.runtime.run_id() != Some(&run_id) {
            return Err(ServiceError::Conflict(format!(
                "delivery '{id}' started a newer run while stop was in progress"
            )));
        }
        current.runtime = RuntimeState::Stopped;
        let expected_record_version = current.record_version;
        current.record_version = next_version(current.record_version)?;
        current.updated_at_ms = now_ms();
        self.store
            .replace(current.clone(), expected_record_version)
            .await?;
        Ok(current)
    }

    pub fn spawn_supervisor_monitor(self: &Arc<Self>) -> Result<(), ServiceError> {
        let mut events = self.supervisor.take_events()?;
        let control_plane = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(event) = events.recv().await else {
                    break;
                };
                let Some(control_plane) = control_plane.upgrade() else {
                    break;
                };
                if let Err(error) = control_plane.apply_worker_event(event).await {
                    tracing::error!(error = ?error, "failed to persist worker state change");
                }
            }
        });
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        self.shutdown.cancel();
        self.supervisor.shutdown_all().await?;
        let _mutation = self.mutation.lock().await;
        for mut record in self.store.list().await? {
            if record.runtime.is_running_or_transitioning() {
                record.runtime = RuntimeState::Stopped;
                let expected_record_version = record.record_version;
                record.record_version = next_version(record.record_version)?;
                record.updated_at_ms = now_ms();
                self.store.replace(record, expected_record_version).await?;
            }
        }
        Ok(())
    }

    pub fn render_yaml(config: &Value) -> Result<String, ServiceError> {
        serde_yaml::to_string(config)
            .map_err(anyhow::Error::from)
            .map_err(ServiceError::Internal)
    }

    pub fn parse_yaml(yaml: &str) -> Result<Value, ServiceError> {
        let config: Value = serde_yaml::from_str(yaml)
            .map_err(|error| ServiceError::InvalidInput(format!("invalid YAML: {error}")))?;
        if !config.is_object() {
            return Err(ServiceError::InvalidInput(
                "the YAML configuration root must be a mapping".to_owned(),
            ));
        }
        Ok(config)
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
        run_id: &RunId,
        message: &str,
    ) -> Result<(), ServiceError> {
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.runtime.run_id() != Some(run_id) {
            return Ok(());
        }
        current.runtime = RuntimeState::Failed {
            run_id: run_id.clone(),
            message: message.to_owned(),
        };
        let expected_record_version = current.record_version;
        current.record_version = next_version(current.record_version)?;
        current.updated_at_ms = now_ms();
        self.store.replace(current, expected_record_version).await?;
        Ok(())
    }

    async fn mark_stopped(&self, id: &str, run_id: &RunId) -> Result<(), ServiceError> {
        let _mutation = self.mutation.lock().await;
        let mut current = self.store.get(id).await?;
        if current.runtime.run_id() != Some(run_id) {
            return Ok(());
        }
        current.runtime = RuntimeState::Stopped;
        let expected_record_version = current.record_version;
        current.record_version = next_version(current.record_version)?;
        current.updated_at_ms = now_ms();
        self.store.replace(current, expected_record_version).await?;
        Ok(())
    }

    async fn apply_worker_event(&self, event: WorkerEvent) -> Result<(), ServiceError> {
        let _mutation = self.mutation.lock().await;
        let mut record = match self.store.get(&event.delivery_id).await {
            Ok(record) => record,
            Err(StoreError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if record.runtime.run_id() != Some(&event.run_id) {
            tracing::debug!(
                delivery_id = %event.delivery_id,
                run_id = %event.run_id.0,
                "ignoring a stale worker event"
            );
            return Ok(());
        }
        if matches!(record.runtime, RuntimeState::Failed { .. }) {
            tracing::debug!(
                delivery_id = %event.delivery_id,
                run_id = %event.run_id.0,
                "ignoring a worker event after a terminal control-plane failure"
            );
            return Ok(());
        }
        record.runtime = match event.outcome {
            WorkerOutcome::Stopped | WorkerOutcome::Exited { success: true, .. } => {
                RuntimeState::Stopped
            }
            WorkerOutcome::Exited {
                success: false,
                message,
            } => RuntimeState::Failed {
                run_id: event.run_id,
                message,
            },
        };
        let expected_record_version = record.record_version;
        record.record_version = next_version(record.record_version)?;
        record.updated_at_ms = now_ms();
        self.store.replace(record, expected_record_version).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, schemars::JsonSchema, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlPlaygroundResult {
    pub columns: Vec<ColumnView>,

    pub rows: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, schemars::JsonSchema, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessagePreviewResult {
    pub text_preview: String,

    pub payload_preview_base64: String,

    pub payload_base64: String,

    pub byte_length: usize,

    pub preview_bytes: usize,

    pub metadata: MessagePreviewMetadata,

    pub detections: Vec<crate::parsers::detection::ParserDetection>,
}

#[derive(Clone, Debug, schemars::JsonSchema, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessagePreviewMetadata {
    pub topic: String,

    pub partition: i64,

    pub partition_session_id: i64,

    pub offset: i64,

    pub sequence_number: i64,

    pub created_at_ms: Option<i64>,

    pub written_at_ms: Option<i64>,

    pub producer_id: String,

    pub message_group_id: Option<String>,

    pub codec: String,

    pub compressed_size: usize,

    pub declared_uncompressed_size: Option<usize>,

    pub message_metadata: Vec<MessagePreviewMetadataItem>,

    pub write_session_metadata: std::collections::BTreeMap<String, String>,
}

impl From<crate::providers::logbroker::src_stream::PreviewMessageMetadata>
    for MessagePreviewMetadata
{
    fn from(value: crate::providers::logbroker::src_stream::PreviewMessageMetadata) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            partition_session_id: value.partition_session_id,
            offset: value.offset,
            sequence_number: value.sequence_number,
            created_at_ms: value.created_at_ms,
            written_at_ms: value.written_at_ms,
            producer_id: value.producer_id,
            message_group_id: value.message_group_id,
            codec: value.codec,
            compressed_size: value.compressed_size,
            declared_uncompressed_size: value.declared_uncompressed_size,
            message_metadata: value
                .message_metadata
                .into_iter()
                .map(MessagePreviewMetadataItem::from)
                .collect(),
            write_session_metadata: value.write_session_metadata,
        }
    }
}

#[derive(Clone, Debug, schemars::JsonSchema, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessagePreviewMetadataItem {
    pub key: String,

    pub value_base64: String,

    pub value_text: Option<String>,
}

impl From<crate::providers::logbroker::src_stream::PreviewMetadataItem>
    for MessagePreviewMetadataItem
{
    fn from(value: crate::providers::logbroker::src_stream::PreviewMetadataItem) -> Self {
        let value_text = String::from_utf8(value.value.clone()).ok();
        Self {
            key: value.key,
            value_base64: base64::engine::general_purpose::STANDARD.encode(value.value),
            value_text,
        }
    }
}

fn validate_name(name: &str) -> Result<String, ServiceError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ServiceError::InvalidInput(
            "delivery name must not be empty".to_owned(),
        ));
    }
    if name != trimmed {
        return Err(ServiceError::InvalidInput(
            "delivery name must not contain leading or trailing whitespace".to_owned(),
        ));
    }
    if name.len() > 128 {
        return Err(ServiceError::InvalidInput(
            "delivery name must contain at most 128 bytes".to_owned(),
        ));
    }
    Ok(name.to_owned())
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
    pipeline_count: usize,
    discovery: &DeliveryDiscovery,
    sink_provider: &dyn SinkProvider,
) -> anyhow::Result<DiscoveryResult> {
    Ok(DiscoveryResult {
        source,
        sink,
        pipeline_count,
        datasets: discovery
            .datasets
            .iter()
            .map(|dataset| {
                Ok(DatasetView {
                    role: dataset.role.into(),
                    name: dataset.name.to_string(),
                    intermediate_columns: dataset
                        .stored_schema
                        .columns
                        .iter()
                        .map(column_view)
                        .collect(),
                    final_columns: dataset
                        .stored_schema
                        .columns
                        .iter()
                        .map(|column| {
                            Ok(DestinationColumnView {
                                column: column_view(column),
                                destination_type: sink_provider.destination_type(column)?,
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        sink_limits: sink_provider.limits().description(),
    })
}

fn column_view(column: &crate::core::data::schema::SchemaColumn) -> ColumnView {
    ColumnView {
        name: column.name.clone(),
        arrow_type: column
            .arrow_extension_name
            .map_or_else(|| format!("{:?}", column.data_type), str::to_owned),
        nullable: column.nullable,
        primary_key: column.primary_key,
        low_cardinality: column.low_cardinality,
        max_length: column.max_length,
    }
}

fn ensure_record_version(id: &str, actual: u64, expected: u64) -> Result<(), ServiceError> {
    if actual == expected {
        return Ok(());
    }
    Err(ServiceError::Conflict(format!(
        "delivery '{id}' changed: expected record version {expected}, current record version {actual}"
    )))
}

fn compound_activation_error(primary: &ServiceError, cleanup: &SupervisorError) -> ServiceError {
    ServiceError::Internal(anyhow::anyhow!(
        "activation failed: {primary}; stopping the spawned worker also failed: {cleanup}"
    ))
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

fn new_run_id() -> Result<RunId, ServiceError> {
    new_id().map(RunId)
}

fn next_version(version: u64) -> Result<u64, ServiceError> {
    version
        .checked_add(1)
        .ok_or_else(|| ServiceError::Internal(anyhow::anyhow!("version counter overflow")))
}

#[cfg(test)]
#[path = "tests/service.rs"]
mod tests;
