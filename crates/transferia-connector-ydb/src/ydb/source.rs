use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tonic::codec::Streaming;
use ydb_grpc::ydb_proto::feature_flag;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::{ReadTableRequest, ReadTableResponse};

use super::config::{YdbSourceConfig, YdbTableConfig};
use super::transport::YdbClient;
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
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

const SYSTEM_COLUMN_KINDS: [SystemColumnKind; 4] = [
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

#[derive(Clone)]
struct DiscoveredTable {
    config: YdbTableConfig,
    schema: DatasetSchema,
    columns: Vec<ColumnPlan>,
}

pub struct YdbSourceConnector {
    config: YdbSourceConfig,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
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
            counters: Mutex::new(HashMap::new()),
        })
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
}

impl SourceConnector for YdbSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YdbSource(SourceDescriptor {
            behavior: SourceBehavior::FiniteAppendOnlyRows,
            delivery_modes: SourceDeliveryModes::BATCH,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let SourceDiscoveryContext {
                request,
                cancellation,
            } = context;
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

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let partition_id = context.partition_id;
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
                    counters,
                )
                .await?,
            ) as Box<dyn Source>)
        })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

struct YdbSource {
    client: YdbClient,
    session_id: String,
    stream: Streaming<ReadTableResponse>,
    table: DiscoveredTable,
    partition_id: i64,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

impl YdbSource {
    async fn new(
        mut client: YdbClient,
        table: DiscoveredTable,
        partition_id: i64,
        batch_rows: usize,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        let session_id = client.create_session().await?;
        let request = client.request(ReadTableRequest {
            session_id: session_id.clone(),
            path: table.config.path.clone(),
            key_range: None,
            columns: table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            ordered: false,
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
        .map_err(|_| anyhow::anyhow!("YDB StreamReadTable timed out while opening"))??;
        Ok(Self {
            client,
            session_id,
            stream: response.into_inner(),
            table,
            partition_id,
            offset: 0,
            finished: false,
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

    async fn finish(&mut self) -> transferia_core::failure::DataPlaneResult<SourceBatch> {
        if !self.finished {
            self.finished = true;
            if let Err(error) = self.client.delete_session(self.session_id.clone()).await {
                // Session cleanup has no durability semantics and YDB expires
                // abandoned sessions. Replaying a completed snapshot merely
                // because cleanup failed would duplicate every emitted row.
                tracing::warn!(
                    table = %self.table.config.path,
                    error = %error,
                    "YDB snapshot completed but session cleanup failed"
                );
            }
            tracing::info!(
                table = %self.table.config.path,
                emitted_rows = self.offset,
                "YDB snapshot source completed"
            );
        }
        Ok(SourceBatch::Finished)
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
                    return self.finish().await;
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
