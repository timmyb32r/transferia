use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tonic::codec::Streaming;
use ydb_grpc::ydb_proto::feature_flag;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::{ReadTableRequest, ReadTableResponse};

use super::config::{YdbSourceConfig, YdbTableConfig};
use super::src_batch_and_stream::{OverlapExecution, OverlapSnapshot};
use super::src_stream::{
    discover_replication_resources, prepare_replication, replication_contract_violation,
    replication_discovery, PreparedReplication, YdbReplicationSource,
};
use super::transport::{is_not_found_error, YdbClient};
use super::types::{column_plans, dataset_schema, result_set_to_batch, ColumnPlan};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::{
    PreparedSourceExecution, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
    SourceExecutionContext, SourceExecutionPhase, SourcePhase, SpeedtestUnsupported,
};

const SYSTEM_COLUMN_KINDS: [SystemColumnKind; 4] = [
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

#[derive(Clone)]
pub(super) struct DiscoveredTable {
    pub(super) config: YdbTableConfig,
    pub(super) schema: DatasetSchema,
    pub(super) columns: Vec<ColumnPlan>,
}

pub struct YdbSourceConnector {
    config: YdbSourceConfig,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    replication_prepared: tokio::sync::Mutex<Option<Arc<PreparedReplication>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
    delivery_type: OnceLock<DeliveryType>,
    overlap: tokio::sync::Mutex<Option<Arc<OverlapExecution>>>,
}

impl YdbSourceConnector {
    pub fn from_config(
        config: YdbSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            replication_prepared: tokio::sync::Mutex::new(None),
            counters: Mutex::new(HashMap::new()),
            delivery_type: OnceLock::new(),
            overlap: tokio::sync::Mutex::new(None),
        })
    }

    fn bind_mode(&self, mode: DeliveryType) -> anyhow::Result<()> {
        anyhow::ensure!(
            *self.delivery_type.get_or_init(|| mode) == mode,
            "YDB connector cannot be reused across delivery modes"
        );
        Ok(())
    }

    async fn prepared_overlap(
        &self,
        prepared: Arc<PreparedReplication>,
        durable: transferia_registry::durable::DurableContext,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Arc<OverlapExecution>> {
        let mut guard = self.overlap.lock().await;
        if let Some(execution) = guard.as_ref() {
            return Ok(Arc::clone(execution));
        }
        let execution = Arc::new(
            OverlapExecution::prepare(&self.config, prepared, durable, cancellation)
                .await
                .map_err(|error| anyhow::Error::new(DataPlaneFailure::fatal(error)))?,
        );
        *guard = Some(Arc::clone(&execution));
        drop(guard);
        Ok(execution)
    }

    async fn discovered_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async {
                let mut client = YdbClient::connect(&self.config.connection).await?;
                let mut discovered = Vec::with_capacity(self.config.tables.len());
                for table in &self.config.tables {
                    let description = client.describe_table(table.path.clone()).await?;
                    let columns = column_plans(description.columns, &description.primary_key)?;
                    anyhow::ensure!(
                        !columns.is_empty(),
                        "YDB table '{}' has no columns",
                        table.path
                    );
                    discovered.push(DiscoveredTable {
                        config: table.clone(),
                        schema: dataset_schema(&columns),
                        columns,
                    });
                }
                Ok(Arc::new(discovered))
            })
            .await
            .map(Arc::clone)
    }

    fn counters(&self, partition_id: i64) -> Arc<SourceCounters> {
        Arc::clone(
            self.counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(partition_id)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }

    async fn prepared_replication(
        &self,
        durable: &transferia_registry::durable::DurableContext,
        cancellation: &tokio_util::sync::CancellationToken,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<Arc<PreparedReplication>> {
        let mut prepared = self.replication_prepared.lock().await;
        if let Some(current) = prepared.as_ref() {
            if current.delivery_id.as_ref() != durable.delivery_id.as_ref()
                || current.replay_identity.as_ref() != replay_identity.as_ref()
            {
                return Err(replication_contract_violation(anyhow::anyhow!(
                    "YDB replication connector was prepared for a different delivery or replay identity"
                )));
            }
            if !current.fence_lost.is_cancelled() {
                return Ok(Arc::clone(current));
            }
            // A completed overlap keeps its phase in durable storage. Release
            // the stale bootstrap owner too, so live stream recovery can fence
            // a new execution rather than retaining its own expired lease.
            self.overlap.lock().await.take();
        }
        // Release this connector's stale local execution lease before trying to reacquire it.
        // A still-live source keeps its own Arc and therefore continues to fence overlap.
        drop(prepared.take());
        let replacement = Arc::new(
            prepare_replication(&self.config, durable, cancellation, replay_identity).await?,
        );
        *prepared = Some(Arc::clone(&replacement));
        drop(prepared);
        Ok(replacement)
    }
}

impl SourceConnector for YdbSourceConnector {
    fn compatibility(
        &self,
        _delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::YdbSource(SourceDescriptor {
            behavior: if self.config.replication.is_some() {
                SourceBehavior::ChangelogRows
            } else {
                SourceBehavior::FiniteAppendOnlyRows
            },
            delivery_modes: if self.config.replication.is_some() {
                SourceDeliveryModes::STREAM_AND_BATCH_AND_STREAM
            } else {
                SourceDeliveryModes::BATCH
            },
        })
    }

    fn validate_pipeline_memory_limit(&self, limit_bytes: usize) -> anyhow::Result<()> {
        let Some(replication) = self.config.replication.as_ref() else {
            return Ok(());
        };
        let required_bytes = replication.minimum_pipeline_memory_bytes()?;
        anyhow::ensure!(
            required_bytes <= limit_bytes,
            "YDB replication max_response_bytes={} needs at least {required_bytes} bytes of pipeline memory for bounded Topic protobuf decoding, but pipeline_memory_limit_bytes is {limit_bytes}",
            replication.max_response_bytes
        );
        Ok(())
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let SourceDiscoveryContext {
                request,
                cancellation,
                delivery_type,
            } = context;
            self.bind_mode(delivery_type)?;
            if self.config.replication.is_some() {
                anyhow::ensure!(
                    matches!(delivery_type, DeliveryType::Stream | DeliveryType::BatchAndStream),
                    "YDB replication configuration supports stream and batch_and_stream delivery, got '{}'",
                    delivery_type.label()
                );
                let resources = discover_replication_resources(&self.config, &cancellation).await?;
                let mut discovery = replication_discovery(request, &resources)?;
                if delivery_type == DeliveryType::BatchAndStream {
                    discovery.source_topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
                    for dataset in &mut discovery.datasets {
                        dataset.update_policy =
                            transferia_core::delivery::UpdatePolicy::FullImageUpsert;
                    }
                }
                return Ok(discovery);
            }
            anyhow::ensure!(
                delivery_type == DeliveryType::Batch,
                "YDB snapshot configuration supports only batch delivery, got '{}'",
                delivery_type.label()
            );
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("YDB discovery cancelled"),
                tables = self.discovered_tables() => tables?,
            };
            let system_columns = SYSTEM_COLUMN_KINDS
                .iter()
                .copied()
                .map(Into::into)
                .collect::<Vec<_>>();
            let datasets = tables
                .iter()
                .map(|table| {
                    let mut incoming_schema = table.schema.clone();
                    incoming_schema
                        .columns
                        .extend(SYSTEM_COLUMN_KINDS.iter().map(|kind| {
                            SchemaColumn::new(
                                kind.default_name().to_owned(),
                                kind.data_type(),
                                false,
                            )
                        }));
                    DiscoveredDataset {
                        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                        role: DatasetRole::Main,
                        name: Arc::from(table.config.name()),
                        stored_schema: if request.keep_system_columns {
                            incoming_schema.clone()
                        } else {
                            table.schema.clone()
                        },
                        incoming_schema,
                        system_columns: system_columns.clone(),
                    }
                })
                .collect();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("ydb"),
                source_topology: SourceTopology::StaticPartitions(
                    (0..tables.len())
                        .map(i64::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: request.keep_system_columns,
                datasets,
                performance_advice: Vec::new(),
            })
        })
    }

    fn prepare_execution(
        &self,
        context: SourceExecutionContext,
    ) -> BoxFuture<'_, anyhow::Result<Option<PreparedSourceExecution>>> {
        Box::pin(async move {
            self.bind_mode(context.delivery_type)?;
            if self.config.replication.is_none() {
                anyhow::ensure!(
                    context.delivery_type == DeliveryType::Batch,
                    "YDB snapshot configuration supports only batch delivery"
                );
                return Ok(None);
            }
            anyhow::ensure!(
                matches!(
                    context.delivery_type,
                    DeliveryType::Stream | DeliveryType::BatchAndStream
                ),
                "YDB replication configuration supports stream and batch_and_stream delivery"
            );
            let replay_identity = context.replay_identity.ok_or_else(|| {
                replication_contract_violation(anyhow::anyhow!(
                    "YDB replication requires a non-secret replay identity bound to the complete replay-affecting delivery configuration"
                ))
            })?;
            if replay_identity.is_empty() {
                return Err(replication_contract_violation(anyhow::anyhow!(
                    "YDB replication replay identity must not be empty"
                )));
            }
            let durable = context.durable;
            let cancellation = context.cancellation;
            let prepared = self
                .prepared_replication(&durable, &cancellation, Arc::clone(&replay_identity))
                .await?;
            if prepared.delivery_id.as_ref() != durable.delivery_id.as_ref()
                || prepared.replay_identity.as_ref() != replay_identity.as_ref()
            {
                return Err(replication_contract_violation(anyhow::anyhow!(
                    "YDB replication connector was prepared for a different delivery or replay identity"
                )));
            }
            let mut discovery = replication_discovery(context.request, &prepared.resources)
                .map_err(replication_contract_violation)?;
            let mut remaining_phases = self.execution_phases(context.delivery_type, &discovery)?;
            if context.delivery_type == DeliveryType::BatchAndStream {
                discovery.source_topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
                for dataset in &mut discovery.datasets {
                    dataset.update_policy =
                        transferia_core::delivery::UpdatePolicy::FullImageUpsert;
                }
                let overlap = self
                    .prepared_overlap(Arc::clone(&prepared), durable.clone(), &cancellation)
                    .await?;
                if overlap.streaming().await {
                    remaining_phases.remove(0);
                }
            }
            Ok(Some(PreparedSourceExecution {
                discovery,
                remaining_phases,
            }))
        })
    }

    fn execution_phases(
        &self,
        mode: DeliveryType,
        discovery: &DeliveryDiscovery,
    ) -> anyhow::Result<Vec<SourceExecutionPhase>> {
        Ok(match mode {
            DeliveryType::Batch => vec![SourceExecutionPhase {
                phase: SourcePhase::Snapshot,
                topology: discovery.source_topology.clone(),
                finite: true,
            }],
            DeliveryType::Stream => vec![SourceExecutionPhase {
                phase: SourcePhase::Stream,
                topology: discovery.source_topology.clone(),
                finite: false,
            }],
            DeliveryType::BatchAndStream => vec![
                SourceExecutionPhase {
                    phase: SourcePhase::Snapshot,
                    topology: SourceTopology::CoLocatedStaticPartitions(vec![0]),
                    finite: true,
                },
                SourceExecutionPhase {
                    phase: SourcePhase::Stream,
                    topology: SourceTopology::CoLocatedStaticPartitions(vec![0]),
                    finite: false,
                },
            ],
        })
    }

    fn complete_execution_phase(
        &self,
        phase: SourcePhase,
        durable: transferia_registry::durable::DurableContext,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if phase == SourcePhase::Snapshot
                && self.delivery_type.get() == Some(&DeliveryType::BatchAndStream)
            {
                let execution = self
                    .overlap
                    .lock()
                    .await
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("YDB overlap execution missing"))?;
                anyhow::ensure!(
                    execution.prepared.delivery_id == durable.delivery_id,
                    "YDB overlap completion delivery mismatch"
                );
                execution
                    .prepared
                    .validate_resources(&self.config, &cancellation)
                    .await
                    .map_err(replication_contract_violation)?;
                execution
                    .complete_snapshot()
                    .await
                    .map_err(|error| anyhow::Error::new(DataPlaneFailure::fatal(error)))?;
            }
            Ok(())
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let partition_id = context.partition_id;
            self.bind_mode(context.delivery_type)?;
            if context.delivery_type == DeliveryType::BatchAndStream
                && context.phase == SourcePhase::Snapshot
            {
                anyhow::ensure!(partition_id == 0, "YDB overlap snapshot has one partition");
                let execution = self
                    .overlap
                    .lock()
                    .await
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("YDB overlap snapshot was not prepared"))?;
                anyhow::ensure!(
                    execution.prepared.delivery_id == context.durable.delivery_id
                        && context.replay_identity.as_deref()
                            == Some(execution.prepared.replay_identity.as_ref()),
                    "YDB overlap snapshot identity mismatch"
                );
                let counters = self.counters(partition_id);
                self.metrics
                    .register_source(partition_id, Arc::clone(&counters));
                return OverlapSnapshot::new(
                    self.config.clone(),
                    Arc::clone(&execution),
                    context.cancellation,
                    counters,
                    context.memory,
                )
                .map(|source| Box::new(source) as Box<dyn Source>)
                .map_err(|error| anyhow::Error::new(DataPlaneFailure::fatal(error)));
            }
            if self.config.replication.is_some() {
                if !matches!(
                    context.delivery_type,
                    DeliveryType::Stream | DeliveryType::BatchAndStream
                ) || context.phase != SourcePhase::Stream
                {
                    return Err(replication_contract_violation(anyhow::anyhow!(
                        "YDB replication source can be built only for the stream phase"
                    )));
                }
                if partition_id != 0 {
                    return Err(replication_contract_violation(anyhow::anyhow!(
                        "YDB replication has exactly one stream partition"
                    )));
                }
                let replay_identity = context.replay_identity.as_ref().ok_or_else(|| {
                    replication_contract_violation(anyhow::anyhow!(
                        "YDB replication source build has no replay identity"
                    ))
                })?;
                if replay_identity.is_empty() {
                    return Err(replication_contract_violation(anyhow::anyhow!(
                        "YDB replication replay identity must not be empty"
                    )));
                }
                let prepared = self
                    .prepared_replication(
                        &context.durable,
                        &context.cancellation,
                        Arc::clone(replay_identity),
                    )
                    .await?;
                if prepared.delivery_id.as_ref() != context.durable.delivery_id.as_ref()
                    || prepared.replay_identity.as_ref() != replay_identity.as_ref()
                {
                    return Err(replication_contract_violation(anyhow::anyhow!(
                        "YDB replication source build uses a different delivery or replay identity"
                    )));
                }
                let counters = self.counters(partition_id);
                self.metrics
                    .register_source(partition_id, Arc::clone(&counters));
                let start_offsets = if context.delivery_type == DeliveryType::BatchAndStream {
                    let execution = self
                        .prepared_overlap(
                            Arc::clone(&prepared),
                            context.durable.clone(),
                            &context.cancellation,
                        )
                        .await?;
                    anyhow::ensure!(
                        execution.streaming().await,
                        "YDB snapshot has not crossed the durable commit barrier"
                    );
                    execution.start_offsets.clone()
                } else {
                    HashMap::new()
                };
                let source = YdbReplicationSource::new(
                    &self.config,
                    prepared,
                    context.cancellation,
                    context.memory,
                    counters,
                    start_offsets,
                )
                .await?;
                return Ok(Box::new(source) as Box<dyn Source>);
            }
            anyhow::ensure!(
                context.delivery_type == DeliveryType::Batch
                    && context.phase == SourcePhase::Snapshot,
                "YDB snapshot source can be built only for the batch snapshot phase"
            );
            let tables = self.discovered_tables().await?;
            let table = tables
                .get(usize::try_from(partition_id)?)
                .ok_or_else(|| {
                    anyhow::anyhow!("YDB source partition {partition_id} does not exist")
                })?
                .clone();
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            Ok(Box::new(
                YdbSource::new(
                    YdbClient::connect(&self.config.connection).await?,
                    table,
                    partition_id,
                    self.config.batch_rows,
                    self.config.session_shutdown_timeout(),
                    self.config.session_shutdown_retry_initial(),
                    counters,
                    false,
                )
                .await?,
            ) as Box<dyn Source>)
        })
    }

    fn build_speedtest_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        if self.config.replication.is_some() {
            Box::pin(async { Err(SpeedtestUnsupported::SourceIsolation.into()) })
        } else {
            self.build_source(context)
        }
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

pub(super) struct YdbSource {
    client: YdbClient,
    session_id: Option<String>,
    stream: Streaming<ReadTableResponse>,
    table: DiscoveredTable,
    partition_id: i64,
    offset: i64,
    finished: bool,
    session_shutdown_timeout: Duration,
    session_shutdown_retry_initial: Duration,
    counters: Arc<SourceCounters>,
}

impl YdbSource {
    pub(super) async fn new(
        mut client: YdbClient,
        table: DiscoveredTable,
        partition_id: i64,
        batch_rows: usize,
        session_shutdown_timeout: Duration,
        session_shutdown_retry_initial: Duration,
        counters: Arc<SourceCounters>,
        ordered: bool,
    ) -> anyhow::Result<Self> {
        let active_session_id = client.create_session().await?;
        let mut session_id = Some(active_session_id.clone());
        let request = client.request(ReadTableRequest {
            session_id: active_session_id,
            path: table.config.path.clone(),
            key_range: None,
            columns: table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            ordered,
            row_limit: 0,
            use_snapshot: feature_flag::Status::Enabled as i32,
            batch_limit_bytes: 0,
            batch_limit_rows: u64::try_from(batch_rows)?,
            return_not_null_data_as_optional: feature_flag::Status::Disabled as i32,
        });
        let response = tokio::time::timeout(
            client.timeout(),
            client.service().stream_read_table(request),
        )
        .await
        .map_err(|_| anyhow::anyhow!("YDB StreamReadTable timed out while opening"))
        .and_then(|response| response.map_err(anyhow::Error::new));
        let response = match response {
            Ok(response) => response,
            Err(open_error) => {
                if close_ydb_session(
                    &mut client,
                    &mut session_id,
                    session_shutdown_timeout,
                    session_shutdown_retry_initial,
                )
                .await
                .is_err()
                {
                    tracing::warn!(
                        "YDB session cleanup failed after StreamReadTable could not be opened"
                    );
                }
                return Err(open_error);
            }
        };
        Ok(Self {
            client,
            session_id,
            stream: response.into_inner(),
            table,
            partition_id,
            offset: 0,
            finished: false,
            session_shutdown_timeout,
            session_shutdown_retry_initial,
            counters,
        })
    }

    fn output(&mut self, batch: &RecordBatch) -> anyhow::Result<SourceBatch> {
        let source_rows = u64::try_from(batch.num_rows())?;
        let decoded_bytes = u64::try_from(batch.get_array_memory_size())?;
        let rows = batch.num_rows();
        let rows_i64 = i64::try_from(rows)?;
        let base = batch.num_columns();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut arrays = batch.columns().to_vec();
        fields.extend(
            SYSTEM_COLUMN_KINDS
                .map(|kind| Arc::new(Field::new(kind.default_name(), kind.data_type(), false))),
        );
        arrays.extend([
            Arc::new(StringArray::from(vec![
                self.table.config.path.clone();
                rows
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![self.partition_id; rows])) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(
                self.offset
                    ..self
                        .offset
                        .checked_add(rows_i64)
                        .ok_or_else(|| anyhow::anyhow!("YDB source offset overflow"))?,
            )) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef,
        ]);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        self.offset = self
            .offset
            .checked_add(rows_i64)
            .ok_or_else(|| anyhow::anyhow!("YDB source offset overflow"))?;
        self.counters.add_records(source_rows);
        self.counters.add_network_decoded_bytes(decoded_bytes);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::from(self.table.config.name()),
                false,
                batch,
                routing_system_columns(base),
            )],
            source_rows,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }

    fn finish(&mut self) -> SourceBatch {
        if !self.finished {
            self.finished = true;
            tracing::info!(
                table = %self.table.config.path,
                emitted_rows = self.offset,
                "YDB snapshot source completed"
            );
        }
        SourceBatch::Finished
    }
}

impl Source for YdbSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(SourceBatch::Finished);
            }
            loop {
                let response = tokio::time::timeout(self.client.timeout(), self.stream.message())
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::retryable(anyhow::anyhow!(
                            "YDB snapshot response timed out after {} ms",
                            self.client.timeout().as_millis()
                        ))
                    })?
                    .map_err(|error| DataPlaneFailure::retryable(error.into()))?;
                let Some(response) = response else {
                    return Ok(self.finish());
                };
                let status =
                    StatusCode::try_from(response.status).unwrap_or(StatusCode::Unspecified);
                if status != StatusCode::Success {
                    let error = anyhow::anyhow!(
                        "YDB StreamReadTable failed with {status:?}: {}",
                        serde_json::to_string(&response.issues)
                            .unwrap_or_else(|_| "<issues cannot be serialized>".to_owned())
                    );
                    return Err(if is_retryable_status(status) {
                        DataPlaneFailure::retryable(error)
                    } else {
                        DataPlaneFailure::fatal(error)
                    });
                }
                let result = response
                    .result
                    .and_then(|result| result.result_set)
                    .ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "YDB StreamReadTable returned no result set"
                        ))
                    })?;
                let batch = result_set_to_batch(&result, &self.table.columns)
                    .map_err(DataPlaneFailure::fatal)?;
                if batch.num_rows() > 0 {
                    return self.output(&batch).map_err(DataPlaneFailure::fatal);
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            close_ydb_session(
                &mut self.client,
                &mut self.session_id,
                self.session_shutdown_timeout,
                self.session_shutdown_retry_initial,
            )
            .await
            .map_err(|error| {
                tracing::error!(
                    table = %self.table.config.path,
                    timeout_ms = self.session_shutdown_timeout.as_millis(),
                    "YDB snapshot session cleanup exhausted its configured retry deadline"
                );
                DataPlaneFailure::fatal(error.context(format!(
                    "failed to close YDB snapshot session for table '{}'",
                    self.table.config.path
                )))
            })
        })
    }
}

pub(super) trait YdbSessionClient {
    fn delete_session(&mut self, session_id: String) -> BoxFuture<'_, anyhow::Result<()>>;

    fn is_session_absent(&self, _error: &anyhow::Error) -> bool {
        false
    }
}

impl YdbSessionClient for YdbClient {
    fn delete_session(&mut self, session_id: String) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(Self::delete_session(self, session_id))
    }

    fn is_session_absent(&self, error: &anyhow::Error) -> bool {
        is_not_found_error(error)
            || error
                .downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::NotFound)
    }
}

pub(super) async fn close_ydb_session(
    client: &mut impl YdbSessionClient,
    session_id: &mut Option<String>,
    timeout: Duration,
    retry_initial: Duration,
) -> anyhow::Result<()> {
    let Some(active_session_id) = session_id.clone() else {
        return Ok(());
    };
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow::anyhow!("YDB session shutdown timeout exceeds clock range"))?;
    let mut attempts = 0_u64;
    let mut retry_delay = retry_initial;
    loop {
        attempts = attempts.saturating_add(1);
        let result =
            tokio::time::timeout_at(deadline, client.delete_session(active_session_id.clone()))
                .await;
        match result {
            Ok(Ok(())) => {
                *session_id = None;
                return Ok(());
            }
            Ok(Err(error)) if client.is_session_absent(&error) => {
                *session_id = None;
                return Ok(());
            }
            Ok(Err(_)) | Err(_) => {}
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(anyhow::anyhow!(
                "YDB DeleteSession did not complete within the configured {} ms after {} attempts",
                timeout.as_millis(),
                attempts
            ));
        }
        let retry_at = now.checked_add(retry_delay).unwrap_or(deadline);
        tokio::time::sleep_until(deadline.min(retry_at)).await;
        retry_delay = retry_delay.saturating_mul(2);
    }
}

const fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::Aborted
            | StatusCode::Unavailable
            | StatusCode::Overloaded
            | StatusCode::Timeout
            | StatusCode::BadSession
            | StatusCode::SessionExpired
            | StatusCode::Cancelled
            | StatusCode::Undetermined
            | StatusCode::SessionBusy
            | StatusCode::ExternalError
    )
}

fn routing_system_columns(base: usize) -> SystemColumns {
    SystemColumns::new(vec![
        SystemColumn {
            kind: SystemColumnKind::Topic,
            name: Arc::from(SystemColumnKind::Topic.default_name()),
            index: base,
        },
        SystemColumn {
            kind: SystemColumnKind::Partition,
            name: Arc::from(SystemColumnKind::Partition.default_name()),
            index: base + 1,
        },
        SystemColumn {
            kind: SystemColumnKind::Offset,
            name: Arc::from(SystemColumnKind::Offset.default_name()),
            index: base + 2,
        },
        SystemColumn {
            kind: SystemColumnKind::MessageIndex,
            name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
            index: base + 3,
        },
    ])
}
