use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use futures_util::TryStreamExt as _;
use object_store::path::Path;
use tokio_util::sync::CancellationToken;

use super::config::S3SourceConfig;
use super::reader::S3Source;
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::{CommonParserConfig, ParserPlan, TableNaming};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

pub struct S3SourceConnector {
    config: S3SourceConfig,
    store: Arc<dyn object_store::ObjectStore>,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    snapshot: tokio::sync::OnceCell<Arc<Vec<Path>>>,
    parquet_schema: tokio::sync::OnceCell<arrow::datatypes::SchemaRef>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl S3SourceConnector {
    pub fn from_config(
        config: S3SourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let source_name = if config.path_prefix.is_empty() {
            config.bucket.as_str()
        } else {
            config.path_prefix.as_str()
        };
        let parser_plan = match &config.parser {
            super::config::S3InputParser::Json {
                common,
                json_parser,
            } => ParserPlan::from_json_config(
                &CommonParserConfig {
                    table_naming: TableNaming::FromConfig {
                        name: config.table_name.clone(),
                    },
                    system_columns: common.system_columns.clone(),
                },
                json_parser,
                source_name,
            )?,
            super::config::S3InputParser::Discard { .. } => {
                ParserPlan::from_benchmark_discard(source_name)
            }
            super::config::S3InputParser::Parquet { .. } => ParserPlan::native_source(),
        };
        let store = config.build_store()?;
        Ok(Self {
            config,
            store,
            parser_plan,
            metrics,
            snapshot: tokio::sync::OnceCell::new(),
            parquet_schema: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn parquet_schema(
        &self,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        self.parquet_schema
            .get_or_try_init(|| async {
                let keys = self.snapshot(cancellation).await?;
                let first = keys
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("S3 snapshot is empty"))?;
                let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
                    Arc::clone(&self.store),
                    first.clone(),
                );
                let builder = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => anyhow::bail!("S3 Parquet schema read cancelled"),
                    result = tokio::time::timeout(
                        self.config.timeout(),
                        parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader),
                    ) => result.map_err(|_| anyhow::anyhow!(
                        "S3 Parquet schema read '{first}' timed out"
                    ))??,
                };
                Ok(builder.schema().clone())
            })
            .await
            .map(Arc::clone)
    }

    async fn snapshot(&self, cancellation: &CancellationToken) -> anyhow::Result<Arc<Vec<Path>>> {
        self.snapshot.get_or_try_init(|| async {
            let prefix = if self.config.path_prefix.is_empty() {
                None
            } else {
                Some(Path::parse(&self.config.path_prefix)?)
            };
            let listed = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("S3 listing cancelled"), result = tokio::time::timeout(self.config.timeout(), self.store.list(prefix.as_ref()).try_collect::<Vec<_>>()) => result.map_err(|_| anyhow::anyhow!("S3 listing timed out"))?? };
            let mut keys = listed.into_iter().filter(|object| object.size > 0).map(|object| object.location).collect::<Vec<_>>();
            keys.sort();
            let mut unique = HashSet::with_capacity(keys.len());
            for key in &keys { anyhow::ensure!(unique.insert(key.as_ref()), "S3 listing returned duplicate key '{key}'"); }
            anyhow::ensure!(
                !keys.is_empty(),
                "S3 path prefix '{}' contains no non-empty objects",
                self.config.path_prefix
            );
            Ok(Arc::new(keys))
        }).await.map(Arc::clone)
    }

    fn counters(&self, partition: i64) -> Arc<SourceCounters> {
        Arc::clone(
            self.counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(partition)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }
}

impl SourceConnector for S3SourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::S3Source(SourceDescriptor {
            behavior: SourceBehavior::FiniteSnapshotRows,
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
            drop(self.snapshot(&cancellation).await?);
            if self.config.parser.parquet_batch_rows().is_some() {
                let schema = self.parquet_schema(&cancellation).await?;
                let dataset_schema = DatasetSchema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|field| {
                            SchemaColumn::new(
                                field.name().clone(),
                                field.data_type().clone(),
                                field.is_nullable(),
                            )
                        })
                        .collect(),
                );
                return Ok(DeliveryDiscovery {
                    source_name: Arc::from(if self.config.path_prefix.is_empty() {
                        self.config.bucket.as_str()
                    } else {
                        self.config.path_prefix.as_str()
                    }),
                    source_topology: SourceTopology::StaticPartitions(vec![0]),
                    schema_origin: SchemaOrigin::SourceNative,
                    keep_system_columns: request.keep_system_columns,
                    datasets: vec![DiscoveredDataset {
                        role: DatasetRole::Main,
                        name: Arc::from(self.config.table_name.as_str()),
                        incoming_schema: dataset_schema.clone(),
                        stored_schema: dataset_schema,
                        system_columns: Vec::new(),
                    }],
                    performance_advice: Vec::new(),
                });
            }
            self.parser_plan.delivery_discovery(
                Arc::from(self.config.path_prefix.as_str()),
                SourceTopology::StaticPartitions(vec![0]),
                request,
            )
        })
    }
    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let SourceBuildContext {
                partition_id,
                cancellation,
                memory,
                ..
            } = context;
            anyhow::ensure!(partition_id == 0, "S3 source has only partition 0");
            let keys = self.snapshot(&cancellation).await?;
            let counters = self.counters(partition_id);
            let parquet = if let Some(batch_rows) = self.config.parser.parquet_batch_rows() {
                Some((
                    Arc::from(self.config.table_name.as_str()),
                    batch_rows,
                    self.parquet_schema(&cancellation).await?,
                ))
            } else {
                None
            };
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            Ok(Box::new(S3Source::new(
                Arc::clone(&self.store),
                keys,
                self.config.timeout(),
                cancellation,
                memory,
                counters,
                parquet,
            )) as Box<dyn Source>)
        })
    }
    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}
