use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::Deserialize;
use transferia_connector_support::parsers::ParserPlan;
use transferia_core::data::schema::DatasetSchema;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::metrics::{MetricsRegistry, SourceCounters};
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

use super::source::DataGeneratorSource;
use super::DataGeneratorPreset;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GenerationAmount {
    #[schemars(title = "Rows")]
    Rows {
        #[schemars(
            title = "Row count",
            range(min = 1),
            extend("x-ui" = { "widget": "grouped_integer" })
        )]
        row_count: u64,
    },
    #[schemars(title = "Data size")]
    DataSize {
        #[schemars(
            title = "Data size",
            range(min = 1),
            extend("x-ui" = { "widget": "byte_size" })
        )]
        data_size_bytes: u64,
    },
}

impl GenerationAmount {
    fn total_rows(&self, row_bytes: u64) -> anyhow::Result<u64> {
        match self {
            Self::Rows { row_count } => {
                anyhow::ensure!(*row_count > 0, "generator.row_count must be positive");
                Ok(*row_count)
            }
            Self::DataSize { data_size_bytes } => {
                anyhow::ensure!(
                    *data_size_bytes > 0,
                    "generator.data_size_bytes must be positive"
                );
                anyhow::ensure!(
                    data_size_bytes.is_multiple_of(row_bytes),
                    "generator.data_size_bytes ({data_size_bytes}) must be divisible by the selected preset row width ({row_bytes} bytes)"
                );
                Ok(data_size_bytes / row_bytes)
            }
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DataGeneratorConfig {
    pub table_name: String,

    #[schemars(title = "Preset")]
    pub preset: DataGeneratorPreset,

    #[schemars(title = "Amount", extend("x-ui" = { "control_width": "wide" }))]
    pub amount: GenerationAmount,

    /// First generated row identifier. This is useful when independently
    /// generated ranges are later concatenated without duplicating primary-key
    /// values.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub start_row: u64,
}

impl DataGeneratorConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.table_name.is_empty(),
            "generator.table_name must not be empty"
        );
        self.preset.validate()?;
        let total_rows = self.total_rows()?;
        let _ = self
            .start_row
            .checked_add(total_rows)
            .ok_or_else(|| anyhow::anyhow!("generator row range overflows u64"))?;
        self.preset.validate_range(self.start_row, total_rows)?;
        Ok(())
    }

    pub(super) fn total_rows(&self) -> anyhow::Result<u64> {
        self.amount.total_rows(self.row_bytes()?)
    }

    pub(super) fn row_bytes(&self) -> anyhow::Result<u64> {
        self.preset.logical_row_bytes()
    }

    pub(super) fn schema(&self) -> DatasetSchema {
        self.preset.schema()
    }

    pub(super) fn batch_bytes(&self, start: u64, rows: u64) -> anyhow::Result<u64> {
        self.preset.batch_bytes(start, rows)
    }

    pub(super) fn rows_for_batch(
        &self,
        start: u64,
        remaining: u64,
        target_bytes: u64,
    ) -> anyhow::Result<u64> {
        let mut rows = remaining.min((target_bytes / self.row_bytes()?).max(1));
        loop {
            let bytes = self.batch_bytes(start, rows)?;
            if bytes <= target_bytes || rows == 1 {
                return Ok(rows);
            }
            let scaled = rows
                .saturating_mul(target_bytes)
                .checked_div(bytes)
                .unwrap_or(0)
                .max(1);
            rows = scaled.min(rows - 1);
        }
    }
}

pub struct DataGeneratorSourceConnector {
    config: DataGeneratorConfig,
    metrics: Arc<MetricsRegistry>,
    counters: Mutex<Option<Arc<SourceCounters>>>,
    parser: ParserPlan,
}

impl DataGeneratorSourceConnector {
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

impl SourceConnector for DataGeneratorSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::DataGenerator(SourceDescriptor {
            behavior: SourceBehavior::FiniteAppendOnlyRows,
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
                performance_advice: Vec::new(),
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

    fn build_speedtest_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        self.build_source(context)
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser.parser()
    }

    fn parses_rows(&self) -> bool {
        true
    }
}
