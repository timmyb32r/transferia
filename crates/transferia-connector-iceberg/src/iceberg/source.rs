use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use iceberg::table::Table;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::metrics::{MetricsRegistry, SourceCounters};
use transferia_connector_support::parsers::ParserPlan;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

use super::catalog::{build_catalog, table_ident};
use super::config::IcebergSourceConfig;

pub struct IcebergSourceConnector {
    config: IcebergSourceConfig,
    metrics: Arc<MetricsRegistry>,
    parser: ParserPlan,
    tables: tokio::sync::OnceCell<Vec<Table>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

pub async fn check_connection(config: &IcebergSourceConfig) -> anyhow::Result<()> {
    config.validate()?;
    let catalog = build_catalog(&config.catalog, &config.storage).await?;
    for table_name in &config.table_names {
        catalog
            .load_table(&table_ident(&config.table_ref(table_name))?)
            .await?;
    }
    Ok(())
}

impl IcebergSourceConnector {
    pub fn from_config(
        config: IcebergSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            metrics,
            parser: ParserPlan::native_source(),
            tables: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn load_tables(&self) -> anyhow::Result<&Vec<Table>> {
        self.tables
            .get_or_try_init(|| async {
                let catalog = build_catalog(&self.config.catalog, &self.config.storage).await?;
                let mut tables = Vec::with_capacity(self.config.table_names.len());
                for table_name in &self.config.table_names {
                    tables.push(
                        catalog
                            .load_table(&table_ident(&self.config.table_ref(table_name))?)
                            .await?,
                    );
                }
                Ok(tables)
            })
            .await
    }

    fn counters(&self, partition_id: i64) -> Arc<SourceCounters> {
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            counters
                .entry(partition_id)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }
}

impl SourceConnector for IcebergSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::IcebergSource(SourceDescriptor {
            behavior: SourceBehavior::FiniteSnapshotRows,
            delivery_modes: SourceDeliveryModes::BATCH,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let tables = tokio::select! {
                () = context.cancellation.cancelled() => anyhow::bail!("Iceberg discovery cancelled"),
                result = self.load_tables() => result?,
            };
            let mut datasets = Vec::with_capacity(tables.len());
            for (table_name, table) in self.config.table_names.iter().zip(tables) {
                let iceberg_schema = table.metadata().current_schema();
                let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)?;
                let schema = dataset_schema(&arrow_schema, iceberg_schema);
                datasets.push(DiscoveredDataset {
                    role: DatasetRole::Main,
                    name: Arc::from(table_name.as_str()),
                    incoming_schema: schema.clone(),
                    stored_schema: schema,
                    system_columns: Vec::new(),
                });
            }
            Ok(DeliveryDiscovery {
                source_name: Arc::from(self.config.namespace.as_str()),
                source_topology: SourceTopology::StaticPartitions(
                    (0..tables.len() as i64).collect(),
                ),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: context.request.keep_system_columns,
                datasets,
            })
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let table_index = usize::try_from(context.partition_id)
                .map_err(|_| anyhow::anyhow!("Iceberg partition does not fit usize"))?;
            let table = self
                .load_tables()
                .await?
                .get(table_index)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown Iceberg partition {}", context.partition_id))?;
            let table_name = self
                .config
                .table_names
                .get(table_index)
                .ok_or_else(|| anyhow::anyhow!("Iceberg table is missing for partition {}", context.partition_id))?;
            let stream = table
                .scan()
                .with_batch_size(Some(self.config.read_batch_rows))
                .build()?
                .to_arrow()
                .await?;
            let counters = self.counters(context.partition_id);
            self.metrics
                .register_source(context.partition_id, Arc::clone(&counters));
            Ok(Box::new(IcebergSource {
                output_name: Arc::from(table_name.as_str()),
                stream: Box::pin(stream),
                cancellation: context.cancellation,
                memory: context.memory,
                counters,
                emitted_rows: 0,
            }) as Box<dyn Source>)
        })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser.parser()
    }

    fn parses_rows(&self) -> bool {
        true
    }
}

struct IcebergSource {
    output_name: Arc<str>,
    stream: std::pin::Pin<Box<iceberg::scan::ArrowRecordBatchStream>>,
    cancellation: CancellationToken,
    memory: PipelineMemory,
    counters: Arc<SourceCounters>,
    emitted_rows: u64,
}

impl Source for IcebergSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let next = tokio::select! {
                () = self.cancellation.cancelled() => return Err(DataPlaneFailure::retryable(anyhow::anyhow!("Iceberg scan cancelled"))),
                next = self.stream.next() => next,
            };
            let Some(batch) = next else {
                return Ok(SourceBatch::Finished);
            };
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    return Err(classify_scan_failure(self.emitted_rows, error.into()));
                }
            };
            let batch = restore_transferia_types(batch).map_err(DataPlaneFailure::fatal)?;
            let rows = batch.num_rows() as u64;
            let output = source_batch(&self.output_name, batch, &self.memory, &self.counters).await?;
            self.emitted_rows = self.emitted_rows.saturating_add(rows);
            Ok(output)
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [transferia_core::source::CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

pub(super) fn classify_scan_failure(
    emitted_rows: u64,
    error: anyhow::Error,
) -> DataPlaneFailure {
    if emitted_rows == 0 {
        return DataPlaneFailure::retryable(error);
    }
    DataPlaneFailure::fatal(error.context(format!(
        "Iceberg snapshot scan failed after emitting {emitted_rows} rows; restarting from the beginning would duplicate data"
    )))
}

async fn source_batch(
    output_name: &Arc<str>,
    batch: RecordBatch,
    memory: &PipelineMemory,
    counters: &SourceCounters,
) -> transferia_core::failure::DataPlaneResult<SourceBatch> {
    let bytes = batch.get_array_memory_size();
    let rows = batch.num_rows() as u64;
    let lease = memory.reserve_progress_source(bytes).await;
    counters.add_messages(rows);
    counters.add_decompressed_bytes(bytes as u64);
    Ok(SourceBatch::Typed {
        tables: vec![TableData::new(
            Arc::clone(output_name),
            false,
            batch,
            SystemColumns::default(),
        )],
        source_rows: rows,
        commit_marker: None,
        memory: vec![lease],
    })
}

fn dataset_schema(
    schema: &arrow::datatypes::Schema,
    iceberg_schema: &iceberg::spec::Schema,
) -> DatasetSchema {
    let identifiers = iceberg_schema
        .identifier_field_ids()
        .filter_map(|field_id| iceberg_schema.name_by_field_id(field_id))
        .collect::<std::collections::HashSet<_>>();
    DatasetSchema::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                SchemaColumn::new(
                    field.name().clone(),
                    logical_data_type(field.name(), field.data_type()),
                    field.is_nullable(),
                )
                .with_constraints(
                    identifiers.contains(field.name().as_str()),
                    false,
                    None,
                )
            })
            .collect(),
    )
}

fn logical_data_type(name: &str, physical: &DataType) -> DataType {
    if name == SystemColumnKind::MessageIndex.default_name()
        && physical == &DataType::Decimal128(20, 0)
    {
        DataType::UInt64
    } else {
        physical.clone()
    }
}

pub(super) fn restore_transferia_types(batch: RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let mut changed = false;
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let data_type = logical_data_type(field.name(), field.data_type());
        if &data_type == field.data_type() {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(column));
            continue;
        }
        changed = true;
        fields.push(Arc::new(
            Field::new(field.name(), data_type.clone(), field.is_nullable())
                .with_metadata(field.metadata().clone()),
        ));
        columns.push(cast(column, &data_type)?);
    }
    if !changed {
        return Ok(batch);
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}
