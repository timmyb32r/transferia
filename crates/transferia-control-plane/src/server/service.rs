use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use base64::Engine as _;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::logs::WorkerLogReader;
use super::store::{DeliveryStore, StoreError};
use transferia_connectors::extension::{EndpointRole, Transferia};
use transferia_core::delivery::{
    DeliveryDiscovery, DeliveryDiscoveryRequest, SinkLimitsDescription,
};
use transferia_delivery::delivery::config::yaml::{Config, DeliveryType};
use transferia_delivery::delivery::preparation::{
    build_delivery_plan_with, build_delivery_plan_with_replay_identity,
    build_resolved_delivery_document_with, ResolvedConfigDocument,
};
use transferia_registry::tuning::{
    tune_source_and_sink, EndpointTuningRequest, TuningBudget, TuningEvaluationCancelled,
    TuningParameter, TuningResult,
};
use transferia_registry::{
    Composition, DynamicOptions, OptionsRequest, SinkConnector, SourceDiscoveryContext,
};
use transferia_runtime::{
    RunId, SupervisorError, WorkerEvent, WorkerLaunchSpec, WorkerOutcome, WorkerSupervisor,
};
pub use transferia_server_contracts::api::{
    ColumnView, DatasetView, DestinationColumnView, DiscoveryResult, MessagePreviewMetadata,
    MessagePreviewMetadataItem, MessagePreviewResult, SpeedtestColumnProfileView,
    SpeedtestDatasetProfileView, SpeedtestEstimateResult, SpeedtestMeasurementView,
    SpeedtestProfileView, SpeedtestTuneResult, SpeedtestTuningBudgetView,
    SpeedtestTuningResultView, SpeedtestTuningTrialView, SqlPlaygroundResult,
    ValidationCommandResult, WorkerLogChunkView, WorkerLogView, WorkerLogsResult,
};
use transferia_server_contracts::{DeliveryRecord, RuntimeState, ValidationState};

const MAX_MESSAGE_PREVIEW_BYTES: usize = 32 * 1024 * 1024;
const INLINE_MESSAGE_PREVIEW_BYTES: usize = 16 * 1024;
const CONNECTION_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const CONNECTION_TIMEOUT_MESSAGE: &str =
    "Connection timed out after 3 seconds; this usually means there is no network access to the endpoint.";

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
    #[error("{0}")]
    OperationFailed(String),
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

pub struct ControlPlane {
    store: Arc<dyn DeliveryStore>,
    supervisor: Arc<dyn WorkerSupervisor>,
    transferia: Transferia,
    worker_logs: Option<WorkerLogReader>,
    mutation: Mutex<()>,

    speedtest_tasks: TaskTracker,

    shutdown: CancellationToken,
}

impl ControlPlane {
    pub async fn spawn_speedtest_estimate(
        self: &Arc<Self>,
        config: Value,
        duration_seconds: u64,
        cleanup_timeout_seconds: u64,
        cancellation: CancellationToken,
    ) -> Result<SpeedtestEstimateResult, ServiceError> {
        if self.shutdown.is_cancelled() {
            return Err(ServiceError::Conflict(
                "the control plane is shutting down".to_owned(),
            ));
        }
        let control_plane = Arc::clone(self);
        self.speedtest_tasks
            .spawn(async move {
                control_plane
                    .speedtest_estimate(
                        &config,
                        duration_seconds,
                        cleanup_timeout_seconds,
                        cancellation,
                    )
                    .await
            })
            .await
            .map_err(|error| {
                ServiceError::Internal(anyhow::anyhow!("speedtest task failed: {error}"))
            })?
    }

    pub async fn spawn_speedtest_tune(
        self: &Arc<Self>,
        config: Value,
        budget: SpeedtestTuningBudgetView,
        trial_duration_seconds: u64,
        cleanup_timeout_seconds: u64,
        cancellation: CancellationToken,
    ) -> Result<SpeedtestTuneResult, ServiceError> {
        if self.shutdown.is_cancelled() {
            return Err(ServiceError::Conflict(
                "the control plane is shutting down".to_owned(),
            ));
        }
        let control_plane = Arc::clone(self);
        self.speedtest_tasks
            .spawn(async move {
                control_plane
                    .speedtest_tune(
                        config,
                        budget,
                        trial_duration_seconds,
                        cleanup_timeout_seconds,
                        cancellation,
                    )
                    .await
            })
            .await
            .map_err(|error| {
                ServiceError::Internal(anyhow::anyhow!("speedtest tuning task failed: {error}"))
            })?
    }

    pub async fn speedtest_estimate(
        &self,
        config: &Value,
        duration_seconds: u64,
        cleanup_timeout_seconds: u64,
        cancellation: CancellationToken,
    ) -> Result<SpeedtestEstimateResult, ServiceError> {
        let duration = validated_speedtest_duration(duration_seconds, "duration_seconds")?;
        let cleanup_timeout =
            validated_speedtest_duration(cleanup_timeout_seconds, "cleanup_timeout_seconds")?;
        let plan = self.speedtest_plan(config, &cancellation).await?;
        let estimate = transferia_delivery::delivery::speedtest::estimate_delivery(
            plan,
            cancellation,
            duration,
            cleanup_timeout,
            self.speedtest_tasks.clone(),
        )
        .await
        .map_err(speedtest_service_error)?;
        let sampled_rows = estimate
            .profile
            .datasets
            .iter()
            .try_fold(0_usize, |total, dataset| total.checked_add(dataset.rows))
            .ok_or_else(|| ServiceError::Internal(anyhow::anyhow!("sample row count overflow")))?;
        let sampled_arrow_bytes = estimate
            .profile
            .datasets
            .iter()
            .try_fold(0_usize, |total, dataset| {
                total.checked_add(dataset.arrow_bytes)
            })
            .ok_or_else(|| ServiceError::Internal(anyhow::anyhow!("sample byte count overflow")))?;
        Ok(SpeedtestEstimateResult {
            logical_streams: estimate.logical_streams,
            source: speedtest_measurement_view(&estimate.source),
            destination: speedtest_measurement_view(&estimate.destination),
            profile: SpeedtestProfileView {
                sampled_rows,
                sampled_arrow_bytes,
                sampled_deliveries: estimate.profile.sampled_deliveries,
                sample_limit_bytes: estimate.profile.sample_limit_bytes,
                truncated: estimate.profile.truncated,
                datasets: estimate
                    .profile
                    .datasets
                    .into_iter()
                    .map(speedtest_dataset_profile_view)
                    .collect(),
            },
        })
    }

    async fn speedtest_tune(
        self: &Arc<Self>,
        config: Value,
        budget: SpeedtestTuningBudgetView,
        trial_duration_seconds: u64,
        cleanup_timeout_seconds: u64,
        cancellation: CancellationToken,
    ) -> Result<SpeedtestTuneResult, ServiceError> {
        let cleanup_timeout =
            validated_speedtest_duration(cleanup_timeout_seconds, "cleanup_timeout_seconds")?;
        let mut tuning_budget = tuning_budget(budget, trial_duration_seconds)?;
        let tuning_cancellation = cancellation.child_token();
        let (source_kind, source_configuration) = endpoint_configuration(&config, "source")?;
        let (sink_kind, sink_configuration) = endpoint_configuration(&config, "sink")?;
        let registry = self
            .transferia
            .build_registry(&Arc::new(
                transferia_connectors::metrics::MetricsRegistry::new(),
            ))
            .map_err(ServiceError::Internal)?;
        let source_parameters = registry
            .tuning_parameters(&source_kind, transferia_registry::EndpointRole::Source)
            .map_err(|error| ServiceError::Validation(error.to_string()))?
            .to_vec();
        let sink_parameters = registry
            .tuning_parameters(&sink_kind, transferia_registry::EndpointRole::Sink)
            .map_err(|error| ServiceError::Validation(error.to_string()))?
            .to_vec();
        let source_initial = endpoint_initial(
            &registry,
            &source_kind,
            transferia_registry::EndpointRole::Source,
        )?;
        let sink_initial = endpoint_initial(
            &registry,
            &sink_kind,
            transferia_registry::EndpointRole::Sink,
        )?;
        let source_baseline_configuration = tuning_default_configuration(
            &source_configuration,
            source_initial,
            &source_parameters,
        )?;
        let sink_baseline_configuration =
            tuning_default_configuration(&sink_configuration, sink_initial, &sink_parameters)?;

        let trial_duration = Duration::from_secs(trial_duration_seconds);
        let baseline_config = endpoint_candidate(
            &config,
            "source",
            &source_kind,
            source_baseline_configuration.clone(),
        )
        .map_err(ServiceError::Internal)?;
        let mut baseline_plan = self
            .speedtest_plan(&baseline_config, &tuning_cancellation)
            .await?;
        transferia_delivery::delivery::speedtest::validate_destination_speedtest(
            &baseline_plan,
            tuning_cancellation.child_token(),
        )
        .await
        .map_err(speedtest_service_error)?;
        let baseline_source = transferia_delivery::delivery::speedtest::benchmark_source(
            &mut baseline_plan,
            tuning_cancellation.child_token(),
            trial_duration,
        )
        .await
        .map_err(speedtest_service_error)?;
        // The explicitly selected tuning budget covers optimizer trials. The
        // one-time empirical profile and source baseline are separately bounded
        // by the user-visible trial duration and happen before this deadline.
        let request_deadline = tuning_request_deadline(budget)?;
        let _deadline_guard =
            DeadlineCancellationGuard::new(request_deadline, tuning_cancellation.clone());
        tuning_budget =
            remaining_tuning_budget(tuning_budget, request_deadline, &tuning_cancellation)?;
        let sample = baseline_source.sample;
        let baseline_source_score = baseline_source.measurement.rows_per_second();

        let source_parameters_for_logging = source_parameters.clone();
        let sink_parameters_for_logging = sink_parameters.clone();
        let source_request = EndpointTuningRequest {
            configuration: source_baseline_configuration.clone(),
            parameters: source_parameters,
            budget: tuning_budget,
        };
        let destination_request = EndpointTuningRequest {
            configuration: sink_baseline_configuration,
            parameters: sink_parameters,
            budget: tuning_budget,
        };

        let source_control_plane = Arc::clone(self);
        let source_base = config.clone();
        let source_kind_for_trial = source_kind.clone();
        let mut cached_baseline = Some(baseline_source_score);
        let mut source_trial_index = 0_usize;
        let destination_control_plane = Arc::clone(self);
        let destination_base = config;
        let destination_kind_for_trial = sink_kind.clone();
        let mut destination_trial_index = 0_usize;
        let pair = tune_source_and_sink(
            source_request,
            destination_request,
            tuning_cancellation,
            move |candidate, trial_cancellation| {
                let trial_index = source_trial_index;
                source_trial_index = source_trial_index.saturating_add(1);
                let parameters =
                    declared_parameter_values(&candidate, &source_parameters_for_logging);
                let connector = source_kind_for_trial.clone();
                let cached = if candidate == source_baseline_configuration {
                    cached_baseline.take()
                } else {
                    None
                };
                let control_plane = Arc::clone(&source_control_plane);
                let config =
                    endpoint_candidate(&source_base, "source", &source_kind_for_trial, candidate);
                async move {
                    let started = std::time::Instant::now();
                    let result: anyhow::Result<f64> = async {
                        if let Some(score) = cached {
                            return Ok(score);
                        }
                        let config = config?;
                        let mut plan = control_plane
                            .speedtest_plan_internal(&config, &trial_cancellation)
                            .await?;
                        let result =
                            transferia_delivery::delivery::speedtest::benchmark_source_throughput(
                                &mut plan,
                                trial_cancellation,
                                trial_duration,
                            )
                            .await;
                        match result {
                            Ok(result) => Ok(result.rows_per_second()),
                            Err(error)
                                if error.is::<
                                    transferia_delivery::delivery::speedtest::SpeedtestCancelled,
                                >() =>
                            {
                                Err(TuningEvaluationCancelled.into())
                            }
                            Err(error) => Err(error),
                        }
                    }
                    .await;
                    log_tuning_trial(
                        "source",
                        &connector,
                        trial_index,
                        &parameters,
                        started.elapsed(),
                        result.as_ref().ok().copied(),
                    );
                    result
                }
            },
            move |candidate, trial_cancellation| {
                let trial_index = destination_trial_index;
                destination_trial_index = destination_trial_index.saturating_add(1);
                let parameters =
                    declared_parameter_values(&candidate, &sink_parameters_for_logging);
                let connector = destination_kind_for_trial.clone();
                let control_plane = Arc::clone(&destination_control_plane);
                let config = endpoint_candidate(
                    &destination_base,
                    "sink",
                    &destination_kind_for_trial,
                    candidate,
                );
                let sample = sample.clone();
                async move {
                    let started = std::time::Instant::now();
                    let result: anyhow::Result<f64> = async {
                        let config = config?;
                        let plan = control_plane
                            .speedtest_plan_internal(&config, &trial_cancellation)
                            .await?;
                        let result =
                            transferia_delivery::delivery::speedtest::benchmark_destination(
                                &plan,
                                &sample,
                                trial_cancellation,
                                trial_duration,
                                cleanup_timeout,
                                control_plane.speedtest_tasks.clone(),
                            )
                            .await;
                        match result {
                            Ok(result) => Ok(result.rows_per_second()),
                            Err(error)
                                if error.is::<
                                    transferia_delivery::delivery::speedtest::SpeedtestCancelled,
                                >() =>
                            {
                                Err(TuningEvaluationCancelled.into())
                            }
                            Err(error) => Err(error),
                        }
                    }
                    .await;
                    log_tuning_trial(
                        "destination",
                        &connector,
                        trial_index,
                        &parameters,
                        started.elapsed(),
                        result.as_ref().ok().copied(),
                    );
                    result
                }
            },
        )
        .await
        .map_err(speedtest_service_error)?;

        log_tuning_result("source", &source_kind, &pair.source);
        log_tuning_result("destination", &sink_kind, &pair.destination);
        tracing::info!(
            source = %source_kind,
            destination = %sink_kind,
            source_baseline_rows_per_second = pair.source.baseline_rows_per_second,
            source_optimized_rows_per_second = pair.source.optimized_rows_per_second,
            destination_baseline_rows_per_second = pair.destination.baseline_rows_per_second,
            destination_optimized_rows_per_second = pair.destination.optimized_rows_per_second,
            "speedtest tuning completed"
        );
        Ok(SpeedtestTuneResult {
            source: speedtest_tuning_result_view(pair.source),
            destination: speedtest_tuning_result_view(pair.destination),
        })
    }

    async fn speedtest_plan(
        &self,
        config: &Value,
        cancellation: &CancellationToken,
    ) -> Result<transferia_delivery::delivery::preparation::DeliveryPlan, ServiceError> {
        self.speedtest_plan_internal(config, cancellation)
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))
    }

    async fn speedtest_plan_internal(
        &self,
        config: &Value,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<transferia_delivery::delivery::preparation::DeliveryPlan> {
        let config = self
            .materialize_speedtest_config(config, cancellation)
            .await?;
        let yaml =
            config_yaml_from_json(&config).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let parsed = Config::from_yaml(&yaml)?;
        tokio::select! {
            () = self.shutdown.cancelled() => anyhow::bail!("the control plane is shutting down"),
            () = cancellation.cancelled() => anyhow::bail!("speedtest was cancelled"),
            result = build_resolved_delivery_document_with(
                ResolvedConfigDocument {
                    replay_identity: None,
                    pipelines: vec![parsed],
                },
                cancellation.child_token(),
                &self.transferia,
            ) => result,
        }
    }

    async fn materialize_speedtest_config(
        &self,
        config: &Value,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Value> {
        let (source_kind, source_configuration) = endpoint_configuration(config, "source")
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (sink_kind, sink_configuration) = endpoint_configuration(config, "sink")
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        anyhow::ensure!(
            !cancellation.is_cancelled(),
            transferia_delivery::delivery::speedtest::SpeedtestCancelled
        );

        let resolved_sources = self
            .transferia
            .resolve_many(
                &source_kind,
                EndpointRole::Source,
                serde_yaml::to_value(&source_configuration)?,
                cancellation.child_token(),
            )
            .await
            .context("speedtest source installation resolution failed")?;
        anyhow::ensure!(
            resolved_sources.len() == 1,
            "speedtest requires one logical source stream, but installation resolution produced {} source configurations",
            resolved_sources.len()
        );
        anyhow::ensure!(
            !cancellation.is_cancelled(),
            transferia_delivery::delivery::speedtest::SpeedtestCancelled
        );
        let resolved_sinks = self
            .transferia
            .resolve_many(
                &sink_kind,
                EndpointRole::Sink,
                serde_yaml::to_value(&sink_configuration)?,
                cancellation.child_token(),
            )
            .await
            .context("speedtest destination installation resolution failed")?;
        anyhow::ensure!(
            resolved_sinks.len() == 1,
            "speedtest requires one logical destination, but installation resolution produced {} destination configurations",
            resolved_sinks.len()
        );
        anyhow::ensure!(
            !cancellation.is_cancelled(),
            transferia_delivery::delivery::speedtest::SpeedtestCancelled
        );

        let registry = self.transferia.build_registry(&Arc::new(
            transferia_connectors::metrics::MetricsRegistry::new(),
        ))?;
        let resolved_source = resolved_sources
            .into_iter()
            .next()
            .context("speedtest source installation resolution returned no configuration")?;
        let source = registry
            .build_source(&source_kind, resolved_source.clone())
            .context("speedtest source configuration is invalid")?;
        let resolved_sink = resolved_sinks
            .into_iter()
            .next()
            .context("speedtest destination installation resolution returned no configuration")?;
        let descriptor = source.compatibility();
        let delivery_type = resolve_delivery_type(config, &source_kind, &descriptor)?;

        let speedtest_id = new_speedtest_delivery_id()?;
        let mut materialized = serde_json::Map::new();
        materialized.insert(
            "delivery_id".to_owned(),
            Value::String(speedtest_id.clone()),
        );
        materialized.insert(
            "durable_storage".to_owned(),
            serde_json::json!({
                "type": "local_file",
                "path": std::env::temp_dir()
                    .join("transferia-speedtest-state")
                    .join(&speedtest_id),
            }),
        );
        materialized.insert(
            "delivery_type".to_owned(),
            serde_json::to_value(delivery_type)?,
        );
        materialized.insert(
            "source".to_owned(),
            singleton_endpoint_value(&source_kind, serde_json::to_value(resolved_source)?),
        );
        materialized.insert(
            "sink".to_owned(),
            singleton_endpoint_value(&sink_kind, serde_json::to_value(resolved_sink)?),
        );
        materialized.insert(
            "middlewares".to_owned(),
            // A speedtest measures the endpoint ceilings: actual source into
            // discard, then the captured source profile into the actual sink.
            // Common transforms are intentionally excluded, so an incomplete
            // editor middleware cannot gate or distort an endpoint speedtest.
            Value::Array(Vec::new()),
        );
        materialized.insert(
            "pipeline_memory_limit_bytes".to_owned(),
            speedtest_pipeline_memory_limit(config),
        );
        materialized.insert(
            "metrics".to_owned(),
            config
                .get("metrics")
                .filter(|value| value.is_null() || value.is_object())
                .cloned()
                .unwrap_or(Value::Null),
        );
        Ok(Value::Object(materialized))
    }

    pub async fn sql_playground(
        &self,
        sql: String,
        rows: Vec<serde_json::Value>,
    ) -> Result<SqlPlaygroundResult, ServiceError> {
        let metrics = Arc::new(transferia_connectors::metrics::MetricsRegistry::new());
        let registry = self
            .transferia
            .build_registry(&metrics)
            .map_err(ServiceError::Internal)?;
        let preview = registry
            .preview_middleware(
                "datafusion",
                serde_yaml::to_value(serde_json::json!({ "sql": sql }))
                    .map_err(anyhow::Error::from)
                    .map_err(ServiceError::Internal)?,
                rows,
            )
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let columns = preview
            .columns
            .into_iter()
            .map(|column| ColumnView {
                name: column.name,
                arrow_type: column.arrow_type,
                nullable: column.nullable,
                primary_key: false,
                low_cardinality: false,
                max_length: None,
            })
            .collect();
        Ok(SqlPlaygroundResult {
            columns,
            rows: preview.rows,
        })
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
            worker_logs: None,
            mutation: Mutex::new(()),
            speedtest_tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
        }
    }

    #[must_use]
    pub fn with_worker_logs(mut self, worker_logs: WorkerLogReader) -> Self {
        self.worker_logs = Some(worker_logs);
        self
    }

    pub async fn worker_logs(&self, delivery_id: &str) -> Result<WorkerLogsResult, ServiceError> {
        let delivery = self.store.get(delivery_id).await?;
        let active_run = match &delivery.runtime {
            RuntimeState::Starting { run_id }
            | RuntimeState::Running { run_id, .. }
            | RuntimeState::Stopping { run_id } => Some(run_id.0.as_str()),
            RuntimeState::Created | RuntimeState::Stopped | RuntimeState::Failed { .. } => None,
        };
        let reader = self.worker_logs.as_ref().ok_or_else(|| {
            ServiceError::Internal(anyhow::anyhow!("worker log storage is unavailable"))
        })?;
        let workers = reader
            .list(delivery_id)
            .await?
            .into_iter()
            .map(|entry| WorkerLogView {
                active: active_run == Some(entry.worker_id.as_str()),
                worker_id: entry.worker_id,
                size_bytes: entry.size_bytes,
            })
            .collect();
        Ok(WorkerLogsResult { workers })
    }

    pub async fn worker_log(
        &self,
        delivery_id: &str,
        worker_id: &str,
        cursor: Option<u64>,
        limit_bytes: Option<usize>,
    ) -> Result<WorkerLogChunkView, ServiceError> {
        self.store.get(delivery_id).await?;
        let reader = self.worker_logs.as_ref().ok_or_else(|| {
            ServiceError::Internal(anyhow::anyhow!("worker log storage is unavailable"))
        })?;
        let chunk = reader
            .read(delivery_id, worker_id, cursor, limit_bytes)
            .await?;
        Ok(WorkerLogChunkView {
            text: chunk.text,
            start_offset: chunk.start_offset,
            next_offset: chunk.next_offset,
            end_offset: chunk.end_offset,
            truncated_before: chunk.truncated_before,
        })
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
        connector: &str,
        role: transferia_connectors::extension::EndpointRole,
        config: Value,
        cancellation: CancellationToken,
    ) -> Result<transferia_registry::ConnectionCheckResult, ServiceError> {
        let total_started = std::time::Instant::now();
        let raw = serde_yaml::to_value(config)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let resolve_started = std::time::Instant::now();
        let resolved = self
            .transferia
            .registry()
            .resolve_many(connector, role, raw, cancellation.clone())
            .await;
        tracing::info!(
            connector,
            ?role,
            stage = "installation_resolution",
            elapsed_ms = resolve_started.elapsed().as_millis(),
            success = resolved.is_ok(),
            "connection check stage completed"
        );
        let resolved = resolved.map_err(|error| ServiceError::Validation(error.to_string()))?;
        let catalog = transferia_connectors::connectors::catalog::build_connector_catalog_with(
            &self.transferia,
            &Arc::new(transferia_connectors::metrics::MetricsRegistry::new()),
        )
        .map_err(ServiceError::Internal)?;
        let check_started = std::time::Instant::now();
        let result = async {
            let mut combined = transferia_registry::ConnectionCheckResult::default();
            for endpoint in resolved {
                let checked = tokio::select! {
                    () = cancellation.cancelled() => return Err(ServiceError::Validation("connection check cancelled".to_owned())),
                    result = tokio::time::timeout(
                        CONNECTION_CHECK_TIMEOUT,
                        catalog.check_connection(connector, role, endpoint),
                    ) => {
                        result
                            .map_err(|_| ServiceError::Validation(CONNECTION_TIMEOUT_MESSAGE.to_owned()))?
                            .map_err(connection_check_service_error)?
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
                if matches!(
                    checked.status,
                    transferia_registry::ConnectionCheckStatus::NetworkReachable
                ) {
                    combined.status = checked.status;
                    combined.message = checked.message;
                } else if checked.message.is_some() {
                    combined.message = checked.message;
                }
            }
            Ok(combined)
        };
        let result = result.await;
        tracing::info!(
            connector,
            ?role,
            stage = "connector_connection_check",
            elapsed_ms = check_started.elapsed().as_millis(),
            total_elapsed_ms = total_started.elapsed().as_millis(),
            success = result.is_ok(),
            "connection check completed"
        );
        result
    }

    pub async fn preview_message(
        &self,
        connector: &str,
        config: Value,
        max_bytes: usize,
        cancellation: CancellationToken,
    ) -> Result<MessagePreviewResult, ServiceError> {
        if !(1..=MAX_MESSAGE_PREVIEW_BYTES).contains(&max_bytes) {
            return Err(ServiceError::Validation(format!(
                "message preview max_bytes must be in 1..={MAX_MESSAGE_PREVIEW_BYTES}"
            )));
        }
        let supports_preview = self
            .transferia
            .composition()
            .connector_definitions()
            .iter()
            .find(|definition| definition.key == connector)
            .and_then(|definition| definition.source.as_ref())
            .is_some_and(|source| source.message_preview);
        if !supports_preview {
            return Err(ServiceError::Validation(format!(
                "{connector} source does not support message preview"
            )));
        }
        let raw = serde_yaml::to_value(config)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let resolved = self
            .transferia
            .registry()
            .resolve_many(
                connector,
                transferia_connectors::extension::EndpointRole::Source,
                raw,
                cancellation.clone(),
            )
            .await
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let attempts_cancellation = cancellation.child_token();
        let mut attempts = tokio::task::JoinSet::new();
        let catalog = Arc::new(
            transferia_connectors::connectors::catalog::build_connector_catalog_with(
                &self.transferia,
                &Arc::new(transferia_connectors::metrics::MetricsRegistry::new()),
            )
            .map_err(ServiceError::Internal)?,
        );
        for endpoint in resolved {
            let endpoint_cancellation = attempts_cancellation.clone();
            let catalog = Arc::clone(&catalog);
            let connector = connector.to_owned();
            attempts.spawn(async move {
                catalog
                    .preview_source(&connector, endpoint, max_bytes, endpoint_cancellation)
                    .await
            });
        }
        let preview = first_successful_preview(&mut attempts, &cancellation).await?;
        attempts_cancellation.cancel();
        attempts.abort_all();
        let preview_bytes = preview.payload.len().min(INLINE_MESSAGE_PREVIEW_BYTES);
        let detection_payloads = preview
            .detection_payloads
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        Ok(MessagePreviewResult {
            text_preview: String::from_utf8_lossy(&preview.payload[..preview_bytes]).into_owned(),
            payload_preview_base64: base64::engine::general_purpose::STANDARD
                .encode(&preview.payload[..preview_bytes]),
            payload_base64: base64::engine::general_purpose::STANDARD.encode(&preview.payload),
            byte_length: preview.payload.len(),
            preview_bytes,
            metadata: message_preview_metadata(preview.metadata),
            detections: self
                .transferia
                .detect_parser_samples(&detection_payloads, 1_000),
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
        mut config: Value,
    ) -> Result<DeliveryRecord, ServiceError> {
        let name = validate_name(&name)?;
        validate_draft_shape(&config)?;
        let _mutation = self.mutation.lock().await;
        let id = new_transfer_id()?;
        set_runtime_delivery_identity(&mut config, &id, &name)?;
        let now = now_ms();
        let record = DeliveryRecord {
            id,
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
        mut config: Value,
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
        set_runtime_delivery_identity(&mut config, &record.id, &name)?;
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
            primary.sink_connector.as_ref(),
        )
        .map_err(|error| ServiceError::Validation(error.to_string()))
    }

    pub async fn source_schema_preview(
        &self,
        config: &Value,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryResult, ServiceError> {
        self.validate_source_schema_preview(config, cancellation)
            .await
    }

    async fn validate_source_schema_preview(
        &self,
        config: &Value,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryResult, ServiceError> {
        validate_draft_shape(config)?;
        let sources = config
            .get("source")
            .and_then(Value::as_object)
            .ok_or_else(|| ServiceError::Validation("choose a source first".to_owned()))?;
        if sources.len() != 1 {
            return Err(ServiceError::Validation(
                "source schema discovery requires exactly one source".to_owned(),
            ));
        }
        let (source_kind, source_config) = sources.iter().next().ok_or_else(|| {
            ServiceError::Validation("source schema discovery requires a source".to_owned())
        })?;
        let raw = serde_yaml::to_value(source_config)
            .map_err(|error| ServiceError::Validation(error.to_string()))?;
        let catalog = transferia_connectors::connectors::catalog::build_connector_catalog_with(
            &self.transferia,
            &Arc::new(transferia_connectors::metrics::MetricsRegistry::new()),
        )
        .map_err(ServiceError::Internal)?;
        let request = DeliveryDiscoveryRequest {
            keep_system_columns: true,
        };
        let (pipeline_count, discovery) = if catalog.supports_source_schema_preview(source_kind) {
            let discovery = catalog
                .preview_source_schema(source_kind, raw, request, cancellation.child_token())
                .await
                .map_err(|error| ServiceError::Validation(format!("{error:#}")))?;
            (1, discovery)
        } else {
            let resolved = self
                .transferia
                .registry()
                .resolve_many(
                    source_kind,
                    transferia_connectors::extension::EndpointRole::Source,
                    raw,
                    cancellation.child_token(),
                )
                .await
                .map_err(|error| ServiceError::Validation(error.to_string()))?;
            let pipeline_count = resolved.len();
            let source_config = resolved.into_iter().next().ok_or_else(|| {
                ServiceError::Validation("source installation resolved no endpoints".to_owned())
            })?;
            let source_connector = catalog
                .build_source(source_kind, source_config)
                .map_err(|error| ServiceError::Validation(error.to_string()))?;
            let delivery_type =
                resolve_delivery_type(config, source_kind, &source_connector.compatibility())
                    .map_err(|error| ServiceError::Validation(error.to_string()))?;
            let discovery = source_connector
                .delivery_discovery(SourceDiscoveryContext {
                    request,
                    cancellation: cancellation.child_token(),
                    delivery_type,
                })
                .await
                .map_err(|error| ServiceError::Validation(format!("{error:#}")))?;
            (pipeline_count, discovery)
        };

        let configured_sink = config
            .get("sink")
            .and_then(Value::as_object)
            .filter(|sinks| sinks.len() == 1)
            .and_then(|sinks| sinks.iter().next());
        if let Some((sink_kind, sink_config)) = configured_sink {
            let raw = serde_yaml::to_value(sink_config)
                .map_err(|error| ServiceError::Validation(error.to_string()))?;
            match self
                .transferia
                .registry()
                .resolve_many(
                    sink_kind,
                    EndpointRole::Sink,
                    raw,
                    cancellation.child_token(),
                )
                .await
            {
                Ok(resolved) => {
                    if let Some(sink_config) = resolved.into_iter().next() {
                        match catalog.build_sink(sink_kind, sink_config) {
                            Ok(sink_connector) => {
                                return discovery_result(
                                    source_kind.clone(),
                                    sink_kind.clone(),
                                    pipeline_count,
                                    &discovery,
                                    sink_connector.as_ref(),
                                )
                                .map_err(ServiceError::Internal);
                            }
                            Err(error) => tracing::debug!(
                                sink = %sink_kind,
                                error = %error,
                                "destination type preview is waiting for a complete sink configuration",
                            ),
                        }
                    }
                }
                Err(error) => tracing::debug!(
                    sink = %sink_kind,
                    error = %error,
                    "destination type preview is waiting for a resolvable sink installation",
                ),
            }
        }
        Ok(source_discovery_result(
            source_kind.clone(),
            pipeline_count,
            &discovery,
        ))
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
            result = build_delivery_plan_with_replay_identity(
                parsed,
                format!("control-plane-delivery:{}:revision:{expected_revision}", snapshot.id),
                self.shutdown.child_token(),
                &self.transferia,
            ) => result.map_err(|error| ServiceError::Validation(error.to_string()))?,
        };
        let resolved = plan.resolved_config().map_err(ServiceError::Internal)?;
        let launch = WorkerLaunchSpec {
            yaml: resolved.yaml().to_owned(),
            composition_fingerprint: resolved.composition_fingerprint().to_owned(),
        };
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

        let worker = match self.supervisor.start(id, &run_id, &launch).await {
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
        self.speedtest_tasks.close();
        self.speedtest_tasks.wait().await;
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

fn log_tuning_trial(
    endpoint_role: &str,
    connector: &str,
    trial_index: usize,
    parameters: &std::collections::BTreeMap<String, Value>,
    elapsed: Duration,
    rows_per_second: Option<f64>,
) {
    tracing::info!(
        endpoint_role,
        connector,
        trial_index,
        parameters = ?parameters,
        rows_per_second,
        elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
        outcome = if rows_per_second.is_some() { "completed" } else { "failed" },
        "speedtest tuning trial finished"
    );
}

fn log_tuning_result(endpoint_role: &str, connector: &str, result: &TuningResult) {
    tracing::info!(
        endpoint_role,
        connector,
        baseline_rows_per_second = result.baseline_rows_per_second,
        optimized_rows_per_second = result.optimized_rows_per_second,
        gain_percent = result.gain_percent,
        trials = result.trials,
        parameters = ?result.parameters,
        "speedtest endpoint tuning completed"
    );
}

fn declared_parameter_values(
    configuration: &Value,
    parameters: &[TuningParameter],
) -> std::collections::BTreeMap<String, Value> {
    parameters
        .iter()
        .filter_map(|parameter| {
            configuration
                .pointer(parameter.pointer())
                .cloned()
                .map(|value| (parameter.pointer().to_owned(), value))
        })
        .collect()
}

fn speedtest_measurement_view(
    measurement: &transferia_delivery::delivery::speedtest::SpeedtestMeasurement,
) -> SpeedtestMeasurementView {
    SpeedtestMeasurementView {
        rows: measurement.rows,
        arrow_bytes: measurement.arrow_bytes,
        duration_ms: measurement.elapsed.as_secs_f64() * 1_000.0,
        rows_per_second: measurement.rows_per_second(),
        bytes_per_second: measurement.bytes_per_second(),
        completed: measurement.completed,
    }
}

fn speedtest_dataset_profile_view(
    profile: transferia_delivery::delivery::speedtest::SpeedtestDatasetProfile,
) -> SpeedtestDatasetProfileView {
    SpeedtestDatasetProfileView {
        dataset: profile.name,
        is_dlq: profile.is_dlq,
        sampled_rows: profile.rows,
        sampled_arrow_bytes: profile.arrow_bytes,
        columns: profile
            .columns
            .into_iter()
            .map(|column| {
                let (numeric_min, numeric_max, temporal_min, temporal_max) = match column.range_kind
                {
                    Some(transferia_delivery::delivery::speedtest::SpeedtestRangeKind::Numeric) => {
                        (column.min_value, column.max_value, None, None)
                    }
                    Some(
                        transferia_delivery::delivery::speedtest::SpeedtestRangeKind::Temporal,
                    ) => (None, None, column.min_value, column.max_value),
                    None => (None, None, None, None),
                };
                SpeedtestColumnProfileView {
                    name: column.name,
                    arrow_type: column.arrow_type,
                    null_count: column.null_count,
                    cardinality: column.distinct_count,
                    numeric_min,
                    numeric_max,
                    temporal_min,
                    temporal_max,
                    min_length: column.min_length,
                    max_length: column.max_length,
                }
            })
            .collect(),
    }
}

fn endpoint_configuration(config: &Value, section: &str) -> Result<(String, Value), ServiceError> {
    let endpoints = config
        .get(section)
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ServiceError::InvalidInput(format!("speedtest config has no '{section}' object"))
        })?;
    if endpoints.len() != 1 {
        return Err(ServiceError::InvalidInput(format!(
            "speedtest config '{section}' must contain exactly one connector"
        )));
    }
    let (kind, configuration) = endpoints.iter().next().ok_or_else(|| {
        ServiceError::InvalidInput(format!("speedtest config '{section}' is empty"))
    })?;
    Ok((kind.clone(), configuration.clone()))
}

fn resolve_delivery_type(
    config: &Value,
    source_kind: &str,
    descriptor: &transferia_delivery_contracts::semantics::EndpointDescriptor,
) -> anyhow::Result<DeliveryType> {
    let explicit = config
        .get("delivery_type")
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value::<DeliveryType>(value.clone()))
        .transpose()
        .context("delivery_type is invalid")?;
    match explicit {
        Some(delivery_type) => {
            anyhow::ensure!(
                descriptor.supports_delivery_type(delivery_type),
                "source '{source_kind}' does not support delivery_type '{}'",
                delivery_type.label()
            );
            Ok(delivery_type)
        }
        None if descriptor.supports_delivery_type(DeliveryType::Batch) => {
            // A source that supports both modes is benchmarked as a finite
            // snapshot unless the user explicitly selected stream delivery.
            // This must never infer a combined run and create stream identities.
            Ok(DeliveryType::Batch)
        }
        None if descriptor.supports_delivery_type(DeliveryType::Stream) => Ok(DeliveryType::Stream),
        None => anyhow::bail!("source '{source_kind}' does not support a speedtest delivery mode"),
    }
}

fn singleton_endpoint_value(kind: &str, configuration: Value) -> Value {
    let mut endpoint = serde_json::Map::new();
    endpoint.insert(kind.to_owned(), configuration);
    Value::Object(endpoint)
}

fn speedtest_pipeline_memory_limit(config: &Value) -> Value {
    const DEFAULT_SPEEDTEST_PIPELINE_MEMORY_BYTES: usize = 1024 * 1024 * 1024;

    let configured = config
        .get("pipeline_memory_limit_bytes")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SPEEDTEST_PIPELINE_MEMORY_BYTES);
    Value::Number(configured.into())
}

fn new_speedtest_delivery_id() -> anyhow::Result<String> {
    use std::fmt::Write as _;

    let mut entropy = [0_u8; 16];
    getrandom::fill(&mut entropy)?;
    let mut id = String::with_capacity(42);
    id.push_str("speedtest-");
    for byte in entropy {
        write!(id, "{byte:02x}")?;
    }
    Ok(id)
}

fn endpoint_candidate(
    base: &Value,
    section: &str,
    kind: &str,
    candidate: Value,
) -> anyhow::Result<Value> {
    let mut config = base.clone();
    let endpoints = config
        .get_mut(section)
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("speedtest config has no '{section}' object"))?;
    anyhow::ensure!(
        endpoints.len() == 1 && endpoints.contains_key(kind),
        "speedtest config '{section}' changed while tuning"
    );
    endpoints.insert(kind.to_owned(), candidate);
    Ok(config)
}

fn endpoint_initial<'a>(
    registry: &'a transferia_registry::Registry,
    kind: &str,
    role: transferia_registry::EndpointRole,
) -> Result<&'a Value, ServiceError> {
    let definition = registry
        .definitions()
        .iter()
        .find(|definition| definition.key == kind)
        .ok_or_else(|| ServiceError::Validation(format!("unknown connector '{kind}'")))?;
    let endpoint = match role {
        transferia_registry::EndpointRole::Source => definition.source.as_ref(),
        transferia_registry::EndpointRole::Sink => definition.sink.as_ref(),
    }
    .ok_or_else(|| {
        ServiceError::Validation(format!("connector '{kind}' has no {role:?} endpoint"))
    })?;
    Ok(&endpoint.initial)
}

fn tuning_default_configuration(
    current: &Value,
    initial: &Value,
    parameters: &[TuningParameter],
) -> Result<Value, ServiceError> {
    let mut baseline = current.clone();
    for parameter in parameters {
        if current.pointer(parameter.pointer()).is_none() {
            continue;
        }
        let Some(default) = initial.pointer(parameter.pointer()) else {
            continue;
        };
        let target = baseline.pointer_mut(parameter.pointer()).ok_or_else(|| {
            ServiceError::Internal(anyhow::anyhow!(
                "declared tuning pointer '{}' disappeared while deriving defaults",
                parameter.pointer()
            ))
        })?;
        *target = default.clone();
    }
    Ok(baseline)
}

fn tuning_budget(
    budget: SpeedtestTuningBudgetView,
    trial_duration_seconds: u64,
) -> Result<TuningBudget, ServiceError> {
    validated_speedtest_duration(trial_duration_seconds, "trial_duration_seconds")?;
    match budget {
        SpeedtestTuningBudgetView::Automatic { max_trials } => {
            if max_trials == 0 {
                return Err(ServiceError::InvalidInput(
                    "automatic tuning max_trials must be greater than zero".to_owned(),
                ));
            }
            Ok(TuningBudget {
                max_trials,
                max_duration_ms: None,
            })
        }
        SpeedtestTuningBudgetView::Time { seconds } => {
            if seconds < trial_duration_seconds {
                return Err(ServiceError::InvalidInput(format!(
                    "time budget must be at least one trial ({trial_duration_seconds} seconds)"
                )));
            }
            let max_duration_ms = seconds.checked_mul(1_000).ok_or_else(|| {
                ServiceError::InvalidInput("tuning time budget is too large".to_owned())
            })?;
            let max_trials = usize::try_from(seconds / trial_duration_seconds).map_err(|_| {
                ServiceError::InvalidInput("tuning time budget is too large".to_owned())
            })?;
            validate_tuning_wall_clock(max_duration_ms)?;
            Ok(TuningBudget {
                max_trials,
                max_duration_ms: Some(max_duration_ms),
            })
        }
    }
}

fn tuning_request_deadline(
    budget: SpeedtestTuningBudgetView,
) -> Result<Option<tokio::time::Instant>, ServiceError> {
    let SpeedtestTuningBudgetView::Time { seconds } = budget else {
        return Ok(None);
    };
    tokio::time::Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .map(Some)
        .ok_or_else(|| ServiceError::InvalidInput("tuning time budget is too large".to_owned()))
}

fn remaining_tuning_budget(
    mut budget: TuningBudget,
    deadline: Option<tokio::time::Instant>,
    cancellation: &CancellationToken,
) -> Result<TuningBudget, ServiceError> {
    let Some(deadline) = deadline else {
        return Ok(budget);
    };
    if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
        return Err(ServiceError::Validation(
            "tuning time budget expired before optimizer trials began".to_owned(),
        ));
    }
    let remaining_ms = u64::try_from(
        deadline
            .duration_since(tokio::time::Instant::now())
            .as_millis(),
    )
    .map_err(|_| ServiceError::InvalidInput("tuning time budget is too large".to_owned()))?;
    if remaining_ms == 0 {
        return Err(ServiceError::Validation(
            "tuning time budget expired before optimizer trials began".to_owned(),
        ));
    }
    budget.max_duration_ms = Some(remaining_ms);
    Ok(budget)
}

struct DeadlineCancellationGuard {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl DeadlineCancellationGuard {
    fn new(deadline: Option<tokio::time::Instant>, cancellation: CancellationToken) -> Self {
        let task = deadline.map(|deadline| {
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                cancellation.cancel();
            })
        });
        Self { task }
    }
}

impl Drop for DeadlineCancellationGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn validated_speedtest_duration(seconds: u64, field: &str) -> Result<Duration, ServiceError> {
    if seconds == 0 {
        return Err(ServiceError::InvalidInput(format!(
            "speedtest {field} must be greater than zero"
        )));
    }
    let duration = Duration::from_secs(seconds);
    if tokio::time::Instant::now().checked_add(duration).is_none() {
        return Err(ServiceError::InvalidInput(format!(
            "speedtest {field} is too large"
        )));
    }
    Ok(duration)
}

fn speedtest_service_error(error: anyhow::Error) -> ServiceError {
    if let Some(error) = error.downcast_ref::<transferia_registry::SpeedtestUnsupported>() {
        return ServiceError::Validation(error.to_string());
    }
    if error
        .downcast_ref::<transferia_delivery::delivery::speedtest::SpeedtestCancelled>()
        .is_some()
    {
        return ServiceError::Conflict("speedtest was cancelled".to_owned());
    }
    if let Some(error) =
        error.downcast_ref::<transferia_delivery::delivery::speedtest::SpeedtestCleanupFailure>()
    {
        return ServiceError::OperationFailed(error.to_string());
    }
    if let Some(error) = error
        .downcast_ref::<transferia_delivery::delivery::speedtest::SpeedtestSourceCleanupFailure>(
    ) {
        return ServiceError::OperationFailed(error.to_string());
    }
    ServiceError::Internal(error)
}

fn validate_tuning_wall_clock(max_duration_ms: u64) -> Result<(), ServiceError> {
    if tokio::time::Instant::now()
        .checked_add(Duration::from_millis(max_duration_ms))
        .is_none()
    {
        return Err(ServiceError::InvalidInput(
            "tuning wall-clock budget is too large".to_owned(),
        ));
    }
    Ok(())
}

fn speedtest_tuning_result_view(result: TuningResult) -> SpeedtestTuningResultView {
    SpeedtestTuningResultView {
        baseline_rows_per_second: result.baseline_rows_per_second,
        optimized_rows_per_second: result.optimized_rows_per_second,
        gain_percent: result.gain_percent,
        trials: result.trials,
        parameters: result.parameters,
        trial_history: result
            .trial_history
            .into_iter()
            .map(|trial| SpeedtestTuningTrialView {
                rows_per_second: trial.rows_per_second,
                parameters: trial.parameters,
            })
            .collect(),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err transfers ownership of the anyhow error into this boundary"
)]
fn connection_check_service_error(error: anyhow::Error) -> ServiceError {
    let permission_denied = error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
    }) || error
        .to_string()
        .contains("Permission denied (os error 13)");
    if permission_denied {
        ServiceError::Validation(
            "No network access to the endpoint: the operating system denied the outgoing connection."
                .to_owned(),
        )
    } else {
        ServiceError::Validation(error.to_string())
    }
}

async fn first_successful_preview<T: Send + 'static>(
    attempts: &mut tokio::task::JoinSet<anyhow::Result<T>>,
    cancellation: &CancellationToken,
) -> Result<T, ServiceError> {
    let mut failures = Vec::new();
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => {
                return Err(ServiceError::Validation("message preview cancelled".to_owned()));
            }
            next = attempts.join_next() => next,
        };
        let Some(result) = next else {
            return Err(ServiceError::Validation(format!(
                "message preview failed on every resolved endpoint: {}",
                failures.join("; ")
            )));
        };
        match result {
            Ok(Ok(preview)) => return Ok(preview),
            Ok(Err(error)) => failures.push(error.to_string()),
            Err(error) => failures.push(format!("preview task failed: {error}")),
        }
    }
}

fn message_preview_metadata(
    value: transferia_registry::SourcePreviewMetadata,
) -> MessagePreviewMetadata {
    MessagePreviewMetadata {
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
            .map(message_preview_metadata_item)
            .collect(),
        write_session_metadata: value.write_session_metadata,
    }
}

fn message_preview_metadata_item(
    value: transferia_registry::SourcePreviewMetadataItem,
) -> MessagePreviewMetadataItem {
    let value_text = String::from_utf8(value.value.clone()).ok();
    MessagePreviewMetadataItem {
        key: value.key,
        value_base64: base64::engine::general_purpose::STANDARD.encode(value.value),
        value_text,
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

fn set_runtime_delivery_identity(
    config: &mut Value,
    delivery_id: &str,
    delivery_name: &str,
) -> Result<(), ServiceError> {
    let object = config.as_object_mut().ok_or_else(|| {
        ServiceError::InvalidInput("delivery configuration must be a JSON object".to_owned())
    })?;
    object.insert(
        "delivery_id".to_owned(),
        Value::String(delivery_id.to_owned()),
    );
    object.insert(
        "delivery_name".to_owned(),
        Value::String(delivery_name.to_owned()),
    );
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
    sink_connector: &dyn SinkConnector,
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
                                destination_type: sink_connector.destination_type(column)?,
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        performance_advice: discovery.performance_advice.clone(),
        sink_limits: sink_connector.limits().description(),
    })
}

fn source_discovery_result(
    source: String,
    pipeline_count: usize,
    discovery: &DeliveryDiscovery,
) -> DiscoveryResult {
    DiscoveryResult {
        source,
        sink: "unselected".to_owned(),
        pipeline_count,
        datasets: discovery
            .datasets
            .iter()
            .map(|dataset| {
                let intermediate_columns = dataset
                    .stored_schema
                    .columns
                    .iter()
                    .map(column_view)
                    .collect::<Vec<_>>();
                let final_columns = intermediate_columns
                    .iter()
                    .cloned()
                    .map(|column| DestinationColumnView {
                        destination_type: column.arrow_type.clone(),
                        column,
                    })
                    .collect();
                DatasetView {
                    role: dataset.role.into(),
                    name: dataset.name.to_string(),
                    intermediate_columns,
                    final_columns,
                }
            })
            .collect(),
        performance_advice: discovery.performance_advice.clone(),
        sink_limits: SinkLimitsDescription {
            sink: "unselected",
            dataset_name: None,
            column_name: None,
            supported_arrow_types: Vec::new(),
            object_key: None,
        },
    }
}

fn column_view(column: &transferia_core::data::schema::SchemaColumn) -> ColumnView {
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

fn new_transfer_id() -> Result<String, ServiceError> {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuv0123456789";

    let mut entropy = [0_u8; 17];
    getrandom::fill(&mut entropy).map_err(anyhow::Error::from)?;
    let mut id = String::with_capacity(20);
    id.push_str("dtt");
    id.extend(
        entropy
            .into_iter()
            .map(|byte| char::from(ALPHABET[usize::from(byte & 31)])),
    );
    Ok(id)
}

fn new_run_id() -> Result<RunId, ServiceError> {
    use std::fmt::Write as _;

    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(anyhow::Error::from)?;
    let mut id = String::with_capacity(32);
    for byte in bytes {
        write!(id, "{byte:02x}").map_err(anyhow::Error::from)?;
    }
    Ok(RunId(id))
}

fn next_version(version: u64) -> Result<u64, ServiceError> {
    version
        .checked_add(1)
        .ok_or_else(|| ServiceError::Internal(anyhow::anyhow!("version counter overflow")))
}

#[cfg(test)]
#[path = "tests/service.rs"]
mod tests;
