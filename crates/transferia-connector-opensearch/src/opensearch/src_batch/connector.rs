use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde::Deserialize;
use transferia_connector_support::metrics::{MetricsRegistry, SourceCounters};
use transferia_connector_support::parsers::ParserPlan;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{
    ConnectionCheckResult, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
};

use super::super::OpenSearchClient;
use super::config::OpenSearchSourceConfig;
use super::source::OpenSearchSource;

const SOURCE_COLUMNS: usize = 4;
const OPEN_SEARCH_ID_MAX_BYTES: usize = 512;

#[derive(Clone)]
struct DiscoveredIndex {
    name: Arc<str>,

    schema: DatasetSchema,

    shard_count: usize,
}

#[derive(Deserialize)]
struct IndexMapping {
    mappings: Mappings,
}

#[derive(Deserialize)]
struct Mappings {
    #[serde(rename = "_source", default)]
    source: SourceMapping,
}

#[derive(Deserialize)]
struct SourceMapping {
    #[serde(default = "enabled_by_default")]
    enabled: bool,
}

#[derive(Deserialize)]
struct IndexSettings {
    settings: Settings,
}

#[derive(Deserialize)]
struct Settings {
    index: CoreIndexSettings,
}

#[derive(Deserialize)]
struct CoreIndexSettings {
    number_of_shards: String,
}

impl Default for SourceMapping {
    fn default() -> Self {
        Self {
            enabled: enabled_by_default(),
        }
    }
}

pub struct OpenSearchSourceConnector {
    config: OpenSearchSourceConfig,

    client: OpenSearchClient,

    metrics: Arc<MetricsRegistry>,

    parser: ParserPlan,

    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredIndex>>>,

    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl OpenSearchSourceConnector {
    pub fn from_config(
        config: OpenSearchSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let client = OpenSearchClient::new(&config.connection)?;
        Ok(Self {
            config,
            client,
            metrics,
            parser: ParserPlan::native_source(),
            discovered: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    pub async fn check_connection(
        config: OpenSearchSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<ConnectionCheckResult> {
        let connector = Self::from_config(config, metrics)?;
        connector.discovered_indices().await?;
        Ok(ConnectionCheckResult::default())
    }

    async fn discovered_indices(&self) -> anyhow::Result<Arc<Vec<DiscoveredIndex>>> {
        self.discovered
            .get_or_try_init(|| async {
                let mut discovered = Vec::with_capacity(self.config.indices.len());
                for index in &self.config.indices {
                    let mappings: HashMap<String, IndexMapping> = self
                        .client
                        .json(Method::GET, &[&index.name, "_mapping"], &[], None)
                        .await
                        .map_err(anyhow::Error::from)?;
                    anyhow::ensure!(
                        mappings.len() == 1 && mappings.contains_key(&index.name),
                        "OpenSearch name '{}' is not one exact concrete index (aliases and wildcard expansion are not accepted)",
                        index.name
                    );
                    let Some(mapping) = mappings.get(&index.name) else {
                        anyhow::bail!(
                            "OpenSearch mapping response omitted exact index '{}'",
                            index.name
                        );
                    };
                    anyhow::ensure!(
                        mapping.mappings.source.enabled,
                        "OpenSearch index '{}' has _source disabled and cannot be read losslessly",
                        index.name
                    );
                    let settings: HashMap<String, IndexSettings> = self
                        .client
                        .json(
                            Method::GET,
                            &[&index.name, "_settings", "index.number_of_shards"],
                            &[],
                            None,
                        )
                        .await
                        .map_err(anyhow::Error::from)?;
                    anyhow::ensure!(
                        settings.len() == 1 && settings.contains_key(&index.name),
                        "OpenSearch settings response for '{}' did not resolve to the exact concrete index",
                        index.name
                    );
                    let shard_count = settings
                        .get(&index.name)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "OpenSearch settings response omitted exact index '{}'",
                                index.name
                            )
                        })?
                        .settings
                        .index
                        .number_of_shards
                        .parse::<usize>()?;
                    anyhow::ensure!(
                        shard_count > 0,
                        "OpenSearch index '{}' reported no primary shards",
                        index.name
                    );
                    discovered.push(DiscoveredIndex {
                        name: Arc::from(index.name.as_str()),
                        schema: document_schema(),
                        shard_count,
                    });
                }
                Ok(Arc::new(discovered))
            })
            .await
            .map(Arc::clone)
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

impl SourceConnector for OpenSearchSourceConnector {
    fn compatibility(
        &self,
        _delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::OpenSearchSource(SourceDescriptor {
            behavior: SourceBehavior::FiniteAppendOnlyRows,
            delivery_modes: SourceDeliveryModes::BATCH,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let indices = tokio::select! {
                biased;
                () = context.cancellation.cancelled() => anyhow::bail!("OpenSearch discovery cancelled"),
                indices = self.discovered_indices() => indices?,
            };
            let kinds = routing_system_kinds();
            let system_columns = kinds.iter().copied().map(Into::into).collect::<Vec<_>>();
            let datasets = indices
                .iter()
                .map(|index| {
                    let mut incoming = index.schema.clone();
                    incoming.columns.extend(kinds.iter().map(|kind| {
                        SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)
                    }));
                    DiscoveredDataset {
                        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                        role: DatasetRole::Main,
                        name: Arc::clone(&index.name),
                        incoming_schema: incoming,
                        stored_schema: if context.request.keep_system_columns {
                            let mut schema = index.schema.clone();
                            schema.columns.extend(kinds.iter().map(|kind| {
                                SchemaColumn::new(
                                    kind.default_name().to_owned(),
                                    kind.data_type(),
                                    false,
                                )
                            }));
                            schema
                        } else {
                            index.schema.clone()
                        },
                        system_columns: system_columns.clone(),
                    }
                })
                .collect();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("opensearch"),
                source_topology: SourceTopology::StaticPartitions(
                    (0..indices.len())
                        .map(i64::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: context.request.keep_system_columns,
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
            let partition = usize::try_from(context.partition_id)
                .map_err(|_| anyhow::anyhow!("OpenSearch source partition must be non-negative"))?;
            let indices = self.discovered_indices().await?;
            let index = indices.get(partition).ok_or_else(|| {
                anyhow::anyhow!(
                    "OpenSearch source partition {} does not exist",
                    context.partition_id
                )
            })?;
            anyhow::ensure!(
                index.schema.columns.len() == SOURCE_COLUMNS,
                "OpenSearch source schema changed after discovery"
            );
            let counters = self.counters(context.partition_id);
            self.metrics
                .register_source(context.partition_id, Arc::clone(&counters));
            Ok(Box::new(
                OpenSearchSource::open(
                    self.client.clone(),
                    Arc::clone(&index.name),
                    context.partition_id,
                    self.config.page_rows,
                    self.config.read_concurrency,
                    index.shard_count,
                    self.config.pit_keep_alive(),
                    self.config.retry_initial_ms,
                    self.config.retry_max_ms,
                    self.config.retry_max_attempts,
                    context.cancellation,
                    context.memory,
                    counters,
                )
                .await?,
            ) as Box<dyn Source>)
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

fn document_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false).with_constraints(
            true,
            false,
            Some(OPEN_SEARCH_ID_MAX_BYTES),
        ),
        SchemaColumn::new("_routing".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("_source".to_owned(), DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
        SchemaColumn::new("_routing_key".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, None),
    ])
}

const fn routing_system_kinds() -> [SystemColumnKind; 4] {
    [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
    ]
}

const fn enabled_by_default() -> bool {
    true
}

#[cfg(test)]
pub(super) fn schema_for_tests() -> DatasetSchema {
    document_schema()
}

#[cfg(test)]
pub(super) fn source_enabled_for_tests(mapping: serde_json::Value) -> anyhow::Result<bool> {
    Ok(serde_json::from_value::<IndexMapping>(mapping)?
        .mappings
        .source
        .enabled)
}
