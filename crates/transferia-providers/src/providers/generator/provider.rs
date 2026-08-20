use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::Deserialize;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::metrics::{MetricsRegistry, SourceCounters};
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_provider_support::parsers::ParserPlan;
use transferia_registry::{SourceBuildContext, SourceDiscoveryContext, SourceProvider};

use super::source::DataGeneratorSource;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DataGeneratorConfig {
    pub table_name: String,

    #[schemars(range(min = 1))]
    pub column_count: usize,

    #[schemars(range(min = 1), extend("x-ui" = { "widget": "byte_size" }))]
    pub data_size_bytes: u64,

    #[schemars(range(min = 1), extend("x-ui" = { "section": "advanced" }))]
    pub batch_rows: usize,
}

impl DataGeneratorConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.table_name.is_empty(),
            "generator.table_name must not be empty"
        );
        anyhow::ensure!(
            self.column_count > 0,
            "generator.column_count must be positive"
        );
        anyhow::ensure!(self.batch_rows > 0, "generator.batch_rows must be positive");
        anyhow::ensure!(
            self.data_size_bytes > 0,
            "generator.data_size_bytes must be positive"
        );
        let row_bytes = self.row_bytes()?;
        anyhow::ensure!(
            self.data_size_bytes.is_multiple_of(row_bytes),
            "generator.data_size_bytes ({}) must be divisible by the generated row width ({row_bytes} bytes)",
            self.data_size_bytes
        );
        Ok(())
    }

    pub(super) fn row_bytes(&self) -> anyhow::Result<u64> {
        u64::try_from(self.column_count)?
            .checked_mul(8)
            .ok_or_else(|| anyhow::anyhow!("generator row width overflow"))
    }

    fn schema(&self) -> DatasetSchema {
        DatasetSchema::new(
            (1..=self.column_count)
                .map(|index| SchemaColumn::new(format!("column_{index}"), DataType::UInt64, false))
                .collect(),
        )
    }
}

pub struct DataGeneratorSourceProvider {
    config: DataGeneratorConfig,
    metrics: Arc<MetricsRegistry>,
    counters: Mutex<Option<Arc<SourceCounters>>>,
    parser: ParserPlan,
}

impl DataGeneratorSourceProvider {
    pub fn from_config(
        config: DataGeneratorConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            metrics,
            counters: Mutex::new(None),
            parser: ParserPlan::native_source(),
        })
    }

    fn counters(&self) -> Arc<SourceCounters> {
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(counters.get_or_insert_with(|| {
            let counters = Arc::new(SourceCounters::new());
            self.metrics.register_source(0, Arc::clone(&counters));
            counters
        }))
    }
}

impl SourceProvider for DataGeneratorSourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::DataGenerator(SourceDescriptor {
            behavior: SourceBehavior::FiniteSnapshotRows,
            delivery_modes: SourceDeliveryModes::BATCH,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            anyhow::ensure!(
                !context.cancellation.is_cancelled(),
                "data generator discovery cancelled"
            );
            let schema = self.config.schema();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("data_generator"),
                source_topology: SourceTopology::StaticPartitions(vec![0]),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: context.request.keep_system_columns,
                datasets: vec![DiscoveredDataset {
                    role: DatasetRole::Main,
                    name: Arc::from(self.config.table_name.as_str()),
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
                "data generator partition must be 0"
            );
            Ok(Box::new(DataGeneratorSource::new(
                self.config.clone(),
                context.memory,
                self.counters(),
            )?) as Box<dyn Source>)
        })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser.parser()
    }

    fn parses_rows(&self) -> bool {
        true
    }
}
