use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use iceberg::table::Table;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::metrics::{MetricsRegistry, SourceCounters};
use transferia_connector_support::parsers::ParserPlan;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumns;
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
    table: tokio::sync::OnceCell<Table>,
    counters: Mutex<Option<Arc<SourceCounters>>>,
}

pub async fn check_connection(config: &IcebergSourceConfig) -> anyhow::Result<()> {
    config.validate()?;
    let catalog = build_catalog(&config.catalog, &config.storage).await?;
    catalog.load_table(&table_ident(&config.table)?).await?;
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
            table: tokio::sync::OnceCell::new(),
            counters: Mutex::new(None),
        })
    }

    async fn load_table(&self) -> anyhow::Result<Table> {
        self.table
            .get_or_try_init(|| async {
                let catalog = build_catalog(&self.config.catalog, &self.config.storage).await?;
                Ok(catalog
                    .load_table(&table_ident(&self.config.table)?)
                    .await?)
            })
            .await
            .cloned()
    }

    fn counters(&self) -> Arc<SourceCounters> {
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(counters.get_or_insert_with(|| Arc::new(SourceCounters::new())))
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
            let table = tokio::select! {
                () = context.cancellation.cancelled() => anyhow::bail!("Iceberg discovery cancelled"),
                result = self.load_table() => result?,
            };
            let iceberg_schema = table.metadata().current_schema();
            let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)?;
            let schema = dataset_schema(&arrow_schema, iceberg_schema);
            Ok(DeliveryDiscovery {
                source_name: Arc::from(self.config.output_name.as_str()),
                source_topology: SourceTopology::StaticPartitions(vec![0]),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: context.request.keep_system_columns,
                datasets: vec![DiscoveredDataset {
                    role: DatasetRole::Main,
                    name: Arc::from(self.config.output_name.as_str()),
                    incoming_schema: schema.clone(),
                    stored_schema: schema,
                    system_columns: Vec::new(),
                }],
            })
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            anyhow::ensure!(
                context.partition_id == 0,
                "Iceberg source has only partition 0"
            );
            let table = self.load_table().await?;
            let stream = table.scan().build()?.to_arrow().await?;
            let counters = self.counters();
            self.metrics.register_source(0, Arc::clone(&counters));
            Ok(Box::new(IcebergSource {
                output_name: Arc::from(self.config.output_name.as_str()),
                stream: Box::pin(stream),
                cancellation: context.cancellation,
                memory: context.memory,
                counters,
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
            let batch = batch.map_err(|error| DataPlaneFailure::retryable(error.into()))?;
            source_batch(&self.output_name, batch, &self.memory, &self.counters).await
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [transferia_core::source::CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
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
                    field.data_type().clone(),
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
