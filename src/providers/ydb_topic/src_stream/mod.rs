mod config;
mod source;

use alloc::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;

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

pub use config::{YdbTopicAuthConfig, YdbTopicReadConfig, YdbTopicSourceConfig};
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
        let primary_topic = &cfg.topics[0].path;
        if cfg.topics.len() > 1
            && matches!(
                cfg.parser.common.table_naming,
                crate::parsers::TableNaming::FromTopicName
            )
        {
            anyhow::bail!("ydb_topic with multiple topics requires table_naming.type=from_config");
        }
        let parser_plan = ParserPlan::from_config(&cfg.parser, primary_topic)?;
        Ok(Self {
            cfg,
            parser_plan,
            metrics_registry,
            behavior: SourceBehavior::ProducesRows,
            source_counters: Mutex::new(HashMap::new()),
            token: OnceCell::new(),
        })
    }

    fn configured_delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
    ) -> anyhow::Result<DeliveryDiscovery> {
        DeliveryDiscovery::parser_projection(
            Arc::from(self.cfg.topics[0].path.as_str()),
            vec![0],
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
    anyhow::ensure!(!cfg.topics.is_empty(), "ydb_topic.topics must not be empty");
    let mut topic_paths = HashSet::with_capacity(cfg.topics.len());
    for topic in &cfg.topics {
        anyhow::ensure!(
            !topic.path.is_empty(),
            "ydb_topic.topics[].path must not be empty"
        );
        anyhow::ensure!(
            topic_paths.insert(topic.path.as_str()),
            "ydb_topic.topics contains duplicate path '{}'",
            topic.path
        );
        let mut partitions = HashSet::with_capacity(topic.partitions.len());
        for partition_id in &topic.partitions {
            anyhow::ensure!(
                *partition_id >= 0,
                "ydb_topic topic '{}' contains negative partition {partition_id}",
                topic.path
            );
            anyhow::ensure!(
                partitions.insert(*partition_id),
                "ydb_topic topic '{}' contains duplicate partition {partition_id}",
                topic.path
            );
        }
    }
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
            anyhow::ensure!(
                !cancellation.is_cancelled(),
                "YDB Topic delivery discovery cancelled"
            );
            self.configured_delivery_discovery(request)
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
            anyhow::ensure!(
                partition_id >= 0,
                "YDB Topic reader lane must be nonnegative"
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
            Ok(vec![i64::from(worker_index)])
        })
    }

    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}

#[cfg(test)]
mod tests;
