mod config;
mod source;

use alloc::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock};

use futures_util::future::BoxFuture;
use prost::Message as _;
use serde_yaml::Value;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use ydb_grpc::ydb_proto::operations::operation_params::OperationMode;
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use ydb_grpc::ydb_proto::topic::{
    AutoPartitioningStrategy, DescribeTopicRequest, DescribeTopicResult,
};

use crate::compatibility::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use crate::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::providers::traits::SourceProvider;
use crate::providers::ydb_transport::{connect_http2_prior_knowledge, H2Service};

pub use config::{TopologyDiscovery, YdbTopicAuthConfig, YdbTopicSourceConfig};
use source::YdbTopicSource;

const NETWORK_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(10);
const MAX_READ_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const YDB_DATABASE: &str = "/Root";

pub struct YdbTopicSourceProvider {
    cfg: YdbTopicSourceConfig,
    parser_plan: ParserPlan,
    metrics_registry: Arc<MetricsRegistry>,
    behavior: SourceBehavior,
    source_counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
    token: OnceCell<Arc<str>>,
    discovered_partitions: RwLock<Vec<i64>>,
}

impl YdbTopicSourceProvider {
    pub fn from_config(
        value: Value,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        let cfg: YdbTopicSourceConfig = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse YDB Topic source config: {error}"))?;
        validate_config(&cfg)?;
        let parser_kind = cfg.parser.parser.kind()?;
        anyhow::ensure!(
            parser_kind != "benchmark_discard",
            "ydb_topic does not support the benchmark_discard parser"
        );
        let parser_plan = ParserPlan::from_config(&cfg.parser, &cfg.topic_path)?;
        Ok(Self {
            cfg,
            parser_plan,
            metrics_registry,
            behavior: SourceBehavior::ProducesRows,
            source_counters: Mutex::new(HashMap::new()),
            token: OnceCell::new(),
            discovered_partitions: RwLock::new(Vec::new()),
        })
    }

    fn configured_delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        partitions: Vec<i64>,
    ) -> anyhow::Result<DeliveryDiscovery> {
        DeliveryDiscovery::parser_projection(
            Arc::from(self.cfg.topic_path.as_str()),
            partitions,
            &self.parser_plan,
            request,
        )
    }

    fn counters_for_partition(&self, partition_id: i64) -> Arc<SourceCounters> {
        let mut counters = self
            .source_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            counters
                .entry(partition_id)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }

    async fn token(&self) -> anyhow::Result<Arc<str>> {
        self.token
            .get_or_try_init(|| async { self.cfg.auth.load_token().map(Arc::<str>::from) })
            .await
            .map(Arc::clone)
    }
}

fn validate_config(cfg: &YdbTopicSourceConfig) -> anyhow::Result<()> {
    crate::providers::address::validate_host("ydb_topic.host", &cfg.host)?;
    crate::providers::address::validate_port("ydb_topic.port", cfg.port)?;
    anyhow::ensure!(
        !cfg.topic_path.is_empty(),
        "ydb_topic.topic_path must not be empty"
    );
    anyhow::ensure!(
        !cfg.consumer_name.is_empty(),
        "ydb_topic.consumer_name must not be empty"
    );
    anyhow::ensure!(
        cfg.trusted_plaintext,
        "ydb_topic.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network"
    );
    anyhow::ensure!(
        (1..=MAX_READ_BUFFER_BYTES).contains(&cfg.read_buffer_bytes),
        "ydb_topic.read_buffer_bytes must be in 1..={MAX_READ_BUFFER_BYTES}"
    );
    let mut partitions = HashSet::with_capacity(cfg.partition_ids.len());
    for partition_id in &cfg.partition_ids {
        anyhow::ensure!(
            *partition_id >= 0,
            "ydb_topic.partition_ids must be nonnegative, got {partition_id}"
        );
        anyhow::ensure!(
            partitions.insert(*partition_id),
            "ydb_topic.partition_ids contains duplicate partition {partition_id}"
        );
    }
    if matches!(cfg.topology_discovery, TopologyDiscovery::Configured) {
        anyhow::ensure!(
            !cfg.partition_ids.is_empty(),
            "ydb_topic.partition_ids must not be empty when topology_discovery is configured"
        );
    }
    cfg.auth.validate()
}

fn set_ydb_headers(metadata: &mut tonic::metadata::MetadataMap, token: &str) -> anyhow::Result<()> {
    metadata.insert(
        "x-ydb-auth-ticket",
        tonic::metadata::AsciiMetadataValue::try_from(token)
            .map_err(|_| anyhow::anyhow!("YDB access token is not valid ASCII metadata"))?,
    );
    metadata.insert(
        "x-ydb-database",
        tonic::metadata::AsciiMetadataValue::from_static(YDB_DATABASE),
    );
    Ok(())
}

async fn connect_client(
    host: &str,
    port: u16,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<(TopicServiceClient<H2Service>, http::Uri)> {
    let uri: http::Uri = crate::providers::address::url("http", host, port)
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid YDB Topic endpoint {host}:{port}: {error}"))?;
    let service = connect_http2_prior_knowledge(&uri, timeout, cancellation).await?;
    Ok((TopicServiceClient::with_origin(service, uri.clone()), uri))
}

fn operation_failure(operation: &ydb_grpc::ydb_proto::operations::Operation) -> anyhow::Error {
    let status = StatusCode::try_from(operation.status).map_or_else(
        |_| format!("UNKNOWN({})", operation.status),
        |status| status.as_str_name().to_owned(),
    );
    let issues = operation
        .issues
        .iter()
        .map(|issue| issue.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    anyhow::anyhow!("YDB Topic request failed: status={status}, issues={issues}")
}

async fn describe_topic(
    cfg: &YdbTopicSourceConfig,
    token: &str,
    cancellation: &CancellationToken,
) -> anyhow::Result<DescribeTopicResult> {
    let timeout = NETWORK_TIMEOUT;
    let (mut client, _) = connect_client(&cfg.host, cfg.port, timeout, cancellation).await?;
    let mut request = Request::new(DescribeTopicRequest {
        operation_params: Some(OperationParams {
            operation_mode: OperationMode::Sync as i32,
            ..OperationParams::default()
        }),
        path: cfg.topic_path.clone(),
        include_stats: false,
        include_location: false,
    });
    set_ydb_headers(request.metadata_mut(), token)?;
    let response = tokio::time::timeout(timeout, client.describe_topic(request)).await;
    let response = response
        .map_err(|_| {
            anyhow::anyhow!(
                "DescribeTopic timed out after {} ms",
                NETWORK_TIMEOUT.as_millis()
            )
        })??
        .into_inner();
    let operation = response
        .operation
        .ok_or_else(|| anyhow::anyhow!("DescribeTopic returned no operation"))?;
    anyhow::ensure!(operation.ready, "DescribeTopic operation is not ready");
    if operation.status != StatusCode::Success as i32 {
        return Err(operation_failure(&operation));
    }
    let result = operation
        .result
        .ok_or_else(|| anyhow::anyhow!("DescribeTopic returned no result"))?;
    DescribeTopicResult::decode(result.value.as_slice())
        .map_err(|error| anyhow::anyhow!("Failed to decode DescribeTopic result: {error}"))
}

fn select_partitions(
    cfg: &YdbTopicSourceConfig,
    topic: &DescribeTopicResult,
) -> anyhow::Result<Vec<i64>> {
    if let Some(strategy) = topic
        .partitioning_settings
        .and_then(|settings| settings.auto_partitioning_settings)
        .map(|settings| settings.strategy)
    {
        anyhow::ensure!(
            strategy == AutoPartitioningStrategy::Disabled as i32
                || strategy == AutoPartitioningStrategy::Unspecified as i32,
            "ydb_topic requires fixed topic partitions, but auto-partitioning strategy is {}",
            AutoPartitioningStrategy::try_from(strategy)
                .map_or("UNKNOWN", |strategy| strategy.as_str_name())
        );
    }
    let active = topic
        .partitions
        .iter()
        .filter(|partition| partition.active)
        .map(|partition| partition.partition_id)
        .collect::<HashSet<_>>();
    anyhow::ensure!(!active.is_empty(), "YDB Topic has no active partitions");
    let mut selected = if cfg.partition_ids.is_empty() {
        active.iter().copied().collect::<Vec<_>>()
    } else {
        for partition_id in &cfg.partition_ids {
            anyhow::ensure!(
                active.contains(partition_id),
                "ydb_topic.partition_ids contains {partition_id}, which is not an active topic partition"
            );
        }
        cfg.partition_ids.clone()
    };
    selected.sort_unstable();
    Ok(selected)
}

impl SourceProvider for YdbTopicSourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YdbTopic(SourceDescriptor {
            behavior: self.behavior,
            delivery_modes: SourceDeliveryModes::STREAM,
        })
    }

    fn delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let token = self.token().await?;
            let partitions = match self.cfg.topology_discovery {
                TopologyDiscovery::TopicApi => {
                    let topic = describe_topic(&self.cfg, token.as_ref(), &cancellation)
                        .await
                        .map_err(|error| error.context("YDB Topic delivery discovery failed"))?;
                    anyhow::ensure!(
                        topic
                            .consumers
                            .iter()
                            .any(|consumer| consumer.name == self.cfg.consumer_name),
                        "ydb_topic.consumer_name '{}' is not configured on topic '{}'",
                        self.cfg.consumer_name,
                        self.cfg.topic_path
                    );
                    select_partitions(&self.cfg, &topic)?
                }
                TopologyDiscovery::Configured => self.cfg.partition_ids.clone(),
            };
            let discovery = self.configured_delivery_discovery(request, partitions.clone())?;
            *self
                .discovered_partitions
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = partitions;
            Ok(discovery)
        })
    }

    fn build_source(
        &self,
        partition_id: i64,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        _durable: crate::durable::DurableContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let discovered = self
                .discovered_partitions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            anyhow::ensure!(
                discovered.contains(&partition_id),
                "partition {partition_id} was not selected during YDB Topic discovery"
            );
            let counters = self.counters_for_partition(partition_id);
            self.metrics_registry
                .register_source(partition_id, Arc::clone(&counters));
            let token = self.token().await?;
            let source = YdbTopicSource::connect(
                &self.cfg,
                token,
                partition_id,
                counters,
                cancellation,
                memory,
            )
            .await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }

    fn partitions_for_worker(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        Box::pin(async move {
            anyhow::ensure!(total_workers > 0, "total_workers must be positive");
            anyhow::ensure!(worker_index < total_workers, "worker_index out of range");
            let partitions = self
                .discovered_partitions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            anyhow::ensure!(
                !partitions.is_empty(),
                "YDB Topic partitions are unavailable before delivery discovery"
            );
            Ok(partitions
                .iter()
                .copied()
                .enumerate()
                .filter_map(|(index, partition)| {
                    (index % total_workers as usize == worker_index as usize).then_some(partition)
                })
                .collect())
        })
    }

    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}

#[cfg(test)]
mod tests;
