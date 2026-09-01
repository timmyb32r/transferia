mod config;
mod source;

use alloc::sync::Arc;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use futures_util::future::BoxFuture;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;

use crate::connectors::logbroker::transport::{connect_http2_prior_knowledge, H2Service};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::{ParserPlan, ParserPluginRegistry};
use transferia_core::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest, SourceTopology};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

#[cfg(test)]
use crate::connectors::logbroker::LogbrokerAuthConfig;
use crate::connectors::logbroker::LogbrokerDriver;
pub use config::LogbrokerSourceConnectionConfig;
pub use config::{LogbrokerSourceCheckConfig, LogbrokerSourceConfig, LogbrokerTopicConfig};
use source::YdbTopicSource;

const NETWORK_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(10);

pub async fn check_network_connection(
    host: &str,
    port: u16,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    crate::connectors::address::validate_host("logbroker.host", host)?;
    crate::connectors::address::validate_port("logbroker.port", port)?;
    let address = crate::connectors::address::host_port(host, port);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("Logbroker network check cancelled"),
        result = tokio::time::timeout(NETWORK_TIMEOUT, tokio::net::TcpStream::connect(&address)) => {
            result
                .map_err(|_| anyhow::anyhow!("Logbroker TCP connection to {address} timed out"))?
                .map_err(|error| anyhow::anyhow!("Logbroker TCP connection to {address} failed: {error}"))?;
        }
    }
    Ok(())
}

pub async fn check_authentication(
    host: &str,
    port: u16,
    auth: &crate::connectors::logbroker::LogbrokerAuthConfig,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    crate::connectors::address::validate_host("logbroker.host", host)?;
    crate::connectors::address::validate_port("logbroker.port", port)?;
    let token = auth.load_token()?;
    let uri: http::Uri = crate::connectors::address::url("http", host, port)
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid Logbroker endpoint {host}:{port}: {error}"))?;
    let service = connect_http2_prior_knowledge(&uri, NETWORK_TIMEOUT, &cancellation).await?;
    let mut client = tonic::client::Grpc::<H2Service>::with_origin(service, uri);
    let mut request = tonic::Request::new(
        crate::connectors::logbroker::proto::discovery::WhoAmIRequest {
            include_groups: false,
        },
    );
    set_ydb_headers(request.metadata_mut(), &token)?;
    let path = http::uri::PathAndQuery::from_static("/Ydb.Discovery.V1.DiscoveryService/WhoAmI");
    let response: crate::connectors::logbroker::proto::discovery::WhoAmIResponse = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("Logbroker authentication check cancelled"),
        result = tokio::time::timeout(NETWORK_TIMEOUT, async {
            client.ready().await.map_err(|error| anyhow::anyhow!("Logbroker authentication service is unavailable: {error}"))?;
            client
                .unary(
                    request,
                    path,
                    tonic_prost::ProstCodec::<
                        crate::connectors::logbroker::proto::discovery::WhoAmIRequest,
                        crate::connectors::logbroker::proto::discovery::WhoAmIResponse,
                    >::default(),
                )
                .await
                .map(tonic::Response::into_inner)
                .map_err(|error| anyhow::anyhow!("Logbroker rejected the authentication token: {error}"))
        }) => result.map_err(|_| anyhow::anyhow!("Logbroker authentication check timed out"))??,
    };
    let operation = response
        .operation
        .ok_or_else(|| anyhow::anyhow!("Logbroker authentication response has no operation"))?;
    validate_authentication_operation(&operation)
}

fn validate_authentication_operation(
    operation: &crate::connectors::logbroker::proto::operations::Operation,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        operation.ready,
        "Logbroker authentication check did not complete"
    );
    let success = crate::connectors::logbroker::proto::status_ids::StatusCode::Success as i32;
    if operation.status != success {
        let status =
            crate::connectors::logbroker::proto::status_ids::StatusCode::try_from(operation.status)
                .ok();
        let status_name = status.map_or("UNKNOWN", |status| status.as_str_name());
        anyhow::bail!(
            "Logbroker rejected the authentication token: status={} ({status_name})",
            operation.status
        );
    }
    Ok(())
}
const CONTROL_PLANE_MAX_GRPC_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
const MAX_READ_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const YDB_DATABASE: &str = "/Root";

pub struct YdbDriverSourceConnector {
    cfg: LogbrokerSourceConfig,
    parser_plan: ParserPlan,
    metrics_registry: Arc<MetricsRegistry>,
    behavior: SourceBehavior,
    source_counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
    token: OnceCell<Arc<str>>,
}

struct PqV1DriverSourceConnector {
    inner: crate::connectors::logbroker::pqv1::src_stream::PqV1SourceConnector,
    behavior: SourceBehavior,
}

impl YdbDriverSourceConnector {
    pub fn from_config(
        cfg: LogbrokerSourceConfig,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        Self::from_config_with_parsers(cfg, metrics_registry, &ParserPluginRegistry::default())
    }

    pub fn from_config_with_parsers(
        cfg: LogbrokerSourceConfig,
        metrics_registry: Arc<MetricsRegistry>,
        parser_plugins: &ParserPluginRegistry,
    ) -> anyhow::Result<Self> {
        validate_config(&cfg)?;
        anyhow::ensure!(
            cfg.driver == LogbrokerDriver::Ydb,
            "YdbDriverSourceConnector requires driver=ydb"
        );
        let benchmark_discard = cfg.parser.parser.kind()? == "benchmark_discard";
        let from_topic_name = matches!(
            cfg.parser.common.table_naming,
            crate::parsers::TableNaming::FromTopicName
        );
        let primary_topic = source::canonical_topic_path(&cfg.topics[0].path);
        let mut parser_plan =
            ParserPlan::from_config_with_plugins(&cfg.parser, primary_topic, parser_plugins)?;
        if from_topic_name {
            parser_plan = parser_plan.route_by_message_topic();
        }
        Ok(Self {
            cfg,
            parser_plan,
            metrics_registry,
            behavior: if benchmark_discard {
                SourceBehavior::BenchmarkDiscard
            } else {
                SourceBehavior::AppendOnlyRows
            },
            source_counters: Mutex::new(HashMap::new()),
            token: OnceCell::new(),
        })
    }

    fn configured_delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
    ) -> anyhow::Result<DeliveryDiscovery> {
        let primary: Arc<str> = Arc::from(source::canonical_topic_path(&self.cfg.topics[0].path));
        if !matches!(
            self.cfg.parser.common.table_naming,
            crate::parsers::TableNaming::FromTopicName
        ) {
            return self.parser_plan.delivery_discovery(
                primary,
                SourceTopology::DynamicWorkerLanes,
                request,
            );
        }

        let mut discovery = self.parser_plan.delivery_discovery(
            Arc::clone(&primary),
            SourceTopology::DynamicWorkerLanes,
            request,
        )?;
        for topic in self.cfg.topics.iter().skip(1) {
            let table: Arc<str> = Arc::from(source::canonical_topic_path(&topic.path));
            let mut topic_discovery = self.parser_plan.delivery_discovery(
                Arc::clone(&table),
                SourceTopology::DynamicWorkerLanes,
                request,
            )?;
            for dataset in &mut topic_discovery.datasets {
                dataset.name = if dataset.role == transferia_core::delivery::DatasetRole::Main {
                    Arc::clone(&table)
                } else {
                    transferia_core::data::table_data::dlq_name(&table).into()
                };
            }
            discovery.datasets.extend(topic_discovery.datasets);
        }
        Ok(discovery)
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

pub fn build_source_connector(
    cfg: LogbrokerSourceConfig,
    metrics_registry: Arc<MetricsRegistry>,
) -> anyhow::Result<Box<dyn SourceConnector>> {
    build_source_connector_with_parsers(cfg, metrics_registry, &ParserPluginRegistry::default())
}

pub fn build_source_connector_with_parsers(
    cfg: LogbrokerSourceConfig,
    metrics_registry: Arc<MetricsRegistry>,
    parser_plugins: &ParserPluginRegistry,
) -> anyhow::Result<Box<dyn SourceConnector>> {
    validate_config(&cfg)?;
    match cfg.driver {
        LogbrokerDriver::Ydb => {
            anyhow::ensure!(
                cfg.pqv1_decompression_concurrency
                    == config::default_pqv1_decompression_concurrency()
                    && !cfg.pqv1_discard_before_decompression,
                "PQv1-only settings require logbroker.driver=pqv1"
            );
            Ok(Box::new(
                YdbDriverSourceConnector::from_config_with_parsers(
                    cfg,
                    metrics_registry,
                    parser_plugins,
                )?,
            ))
        }
        LogbrokerDriver::Pqv1 => {
            anyhow::ensure!(
                cfg.topics.len() == 1,
                "logbroker.driver=pqv1 currently requires exactly one topic"
            );
            anyhow::ensure!(
                cfg.read_buffer_bytes == config::default_read_buffer_bytes(),
                "logbroker.read_buffer_bytes is supported only by driver=ydb"
            );
            anyhow::ensure!(
                cfg.pqv1_decompression_concurrency > 0,
                "logbroker PQv1 decompression concurrency must be positive"
            );

            let auth = match cfg.auth {
                crate::connectors::logbroker::LogbrokerAuthConfig::Token { token } => {
                    crate::connectors::logbroker::pqv1::config::PqV1AuthConfig {
                        auth_type: "access_token".to_owned(),
                        token: Some(token),
                        token_file: None,
                    }
                }
                crate::connectors::logbroker::LogbrokerAuthConfig::TokenFile { token_file } => {
                    crate::connectors::logbroker::pqv1::config::PqV1AuthConfig {
                        auth_type: "access_token".to_owned(),
                        token: None,
                        token_file: Some(token_file),
                    }
                }
            };
            let pqv1 = crate::connectors::logbroker::pqv1::src_stream::PqV1SourceConfig {
                host: cfg.host,
                port: cfg.port,
                topic_path: cfg.topics[0].path.clone(),
                consumer_name: cfg.consumer_name,
                auth,
                parser: cfg.parser,
                partition_group_ids: cfg.topics[0].partitions.clone(),
                network_timeout_ms: 30_000,
                decompression_concurrency: cfg.pqv1_decompression_concurrency,
                benchmark_discard_before_decompression: cfg.pqv1_discard_before_decompression,
                allow_ttl_rewind: cfg.allow_ttl_rewind,
            };
            let inner = crate::connectors::logbroker::pqv1::src_stream::PqV1SourceConnector::from_config_with_parsers(
                pqv1,
                metrics_registry,
                parser_plugins,
            )?;
            let behavior = inner
                .compatibility()
                .source_behavior()
                .ok_or_else(|| anyhow::anyhow!("PQv1 driver did not expose source behavior"))?;
            Ok(Box::new(PqV1DriverSourceConnector { inner, behavior }))
        }
    }
}

pub async fn check_connection(
    cfg: &LogbrokerSourceConnectionConfig,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    validate_connection_config(cfg)?;
    let token = cfg.auth.load_token()?;
    match cfg.driver {
        LogbrokerDriver::Ydb => source::check_read_connection(cfg, &token, cancellation).await,
        LogbrokerDriver::Pqv1 => {
            let endpoint = crate::connectors::address::url("grpc", &cfg.host, cfg.port);
            crate::connectors::logbroker::pqv1::pq_v1::PqV1Client::describe_topic(
                &endpoint,
                &cfg.topics[0].path,
                &token,
                NETWORK_TIMEOUT,
                &cancellation,
            )
            .await
            .map(|_| ())
        }
    }
}

pub async fn preview_message(
    cfg: &LogbrokerSourceConnectionConfig,
    max_bytes: usize,
    cancellation: CancellationToken,
) -> anyhow::Result<PreviewMessage> {
    validate_connection_config(cfg)?;
    anyhow::ensure!(
        cfg.driver == LogbrokerDriver::Ydb,
        "message preview currently requires logbroker.driver=ydb"
    );
    let token = cfg.auth.load_token()?;
    source::preview_message(cfg, &token, max_bytes, cancellation).await
}

pub struct PreviewMessage {
    pub payload: bytes::Bytes,

    pub metadata: PreviewMessageMetadata,

    pub detection_payloads: Vec<bytes::Bytes>,
}

pub struct PreviewMessageMetadata {
    pub topic: String,

    pub partition: i64,

    pub partition_session_id: i64,

    pub offset: i64,

    pub sequence_number: i64,

    pub created_at_ms: Option<i64>,

    pub written_at_ms: Option<i64>,

    pub producer_id: String,

    pub message_group_id: Option<String>,

    pub codec: String,

    pub compressed_size: usize,

    pub declared_uncompressed_size: Option<usize>,

    pub message_metadata: Vec<PreviewMetadataItem>,

    pub write_session_metadata: BTreeMap<String, String>,
}

pub struct PreviewMetadataItem {
    pub key: String,

    pub value: Vec<u8>,
}

fn validate_connection_config(cfg: &LogbrokerSourceConnectionConfig) -> anyhow::Result<()> {
    crate::connectors::address::validate_host("logbroker.host", &cfg.host)?;
    crate::connectors::address::validate_port("logbroker.port", cfg.port)?;
    anyhow::ensure!(!cfg.topics.is_empty(), "logbroker.topics must not be empty");
    anyhow::ensure!(
        !cfg.topics[0].path.is_empty(),
        "logbroker.topics[0].path must not be empty"
    );
    anyhow::ensure!(
        !cfg.consumer_name.is_empty(),
        "logbroker.consumer_name must not be empty"
    );
    anyhow::ensure!(
        cfg.trusted_plaintext,
        "logbroker.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network"
    );
    cfg.auth.validate()
}

pub async fn check_topic_connection(
    host: &str,
    port: u16,
    topic_path: &str,
    auth: &crate::connectors::logbroker::LogbrokerAuthConfig,
    driver: LogbrokerDriver,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    let token = auth.load_token()?;
    match driver {
        LogbrokerDriver::Ydb => {
            let (mut client, _) =
                connect_client(host, port, NETWORK_TIMEOUT, &cancellation).await?;
            let mut request =
                tonic::Request::new(ydb_grpc::ydb_proto::topic::DescribeTopicRequest {
                    operation_params: None,
                    path: topic_path.to_owned(),
                    include_stats: false,
                    include_location: false,
                });
            set_ydb_headers(request.metadata_mut(), &token)?;
            let response = tokio::time::timeout(NETWORK_TIMEOUT, client.describe_topic(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB Topic connection check timed out"))??
                .into_inner();
            let operation = response
                .operation
                .ok_or_else(|| anyhow::anyhow!("YDB Topic DescribeTopic returned no operation"))?;
            anyhow::ensure!(operation.ready, "YDB Topic DescribeTopic did not complete");
            if operation.status != ydb_grpc::ydb_proto::status_ids::StatusCode::Success as i32 {
                let status =
                    ydb_grpc::ydb_proto::status_ids::StatusCode::try_from(operation.status).ok();
                let status_name = status.map_or("UNKNOWN", |status| status.as_str_name());
                let issues = operation
                    .issues
                    .iter()
                    .map(|issue| issue.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                anyhow::bail!(
                    "YDB Topic DescribeTopic failed: status={} ({status_name}), issues={issues}",
                    operation.status
                );
            }
        }
        LogbrokerDriver::Pqv1 => {
            let endpoint = crate::connectors::address::url("grpc", host, port);
            crate::connectors::logbroker::pqv1::pq_v1::PqV1Client::describe_topic(
                &endpoint,
                topic_path,
                &token,
                NETWORK_TIMEOUT,
                &cancellation,
            )
            .await?;
        }
    }
    Ok(())
}

impl SourceConnector for PqV1DriverSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Logbroker(SourceDescriptor {
            behavior: self.behavior,
            delivery_modes: SourceDeliveryModes::STREAM,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        self.inner.delivery_discovery(context)
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        self.inner.build_source(context)
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.inner.parser()
    }

    fn parses_rows(&self) -> bool {
        self.inner.parses_rows()
    }
}

fn validate_config(cfg: &LogbrokerSourceConfig) -> anyhow::Result<()> {
    crate::connectors::address::validate_host("logbroker.host", &cfg.host)?;
    crate::connectors::address::validate_port("logbroker.port", cfg.port)?;
    anyhow::ensure!(!cfg.topics.is_empty(), "logbroker.topics must not be empty");
    let mut topic_paths = HashSet::with_capacity(cfg.topics.len());
    for topic in &cfg.topics {
        anyhow::ensure!(
            !topic.path.is_empty(),
            "logbroker.topics[].path must not be empty"
        );
        anyhow::ensure!(
            !topic.path.starts_with("//"),
            "logbroker topic path '{}' must have at most one leading slash",
            topic.path
        );
        anyhow::ensure!(
            topic_paths.insert(source::canonical_topic_path(&topic.path)),
            "logbroker.topics contains duplicate path '{}'",
            topic.path
        );
        let mut partitions = HashSet::with_capacity(topic.partitions.len());
        for partition_id in &topic.partitions {
            anyhow::ensure!(
                *partition_id >= 0,
                "logbroker topic '{}' contains negative partition {partition_id}",
                topic.path
            );
            anyhow::ensure!(
                partitions.insert(*partition_id),
                "logbroker topic '{}' contains duplicate partition {partition_id}",
                topic.path
            );
        }
    }
    anyhow::ensure!(
        !cfg.consumer_name.is_empty(),
        "logbroker.consumer_name must not be empty"
    );
    anyhow::ensure!(
        !cfg.consumer_name.starts_with("//"),
        "logbroker.consumer_name must have at most one leading slash"
    );
    anyhow::ensure!(
        cfg.trusted_plaintext,
        "logbroker.trusted_plaintext must be true; use a verified TLS tunnel outside a trusted network"
    );
    anyhow::ensure!(
        (1..=MAX_READ_BUFFER_BYTES).contains(&cfg.read_buffer_bytes),
        "logbroker.read_buffer_bytes must be in 1..={MAX_READ_BUFFER_BYTES}"
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
    let uri: http::Uri = crate::connectors::address::url("http", host, port)
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid YDB Topic endpoint {host}:{port}: {error}"))?;
    let service = connect_http2_prior_knowledge(&uri, timeout, cancellation).await?;
    Ok((TopicServiceClient::with_origin(service, uri.clone()), uri))
}

impl SourceConnector for YdbDriverSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Logbroker(SourceDescriptor {
            behavior: self.behavior,
            delivery_modes: SourceDeliveryModes::STREAM,
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
            anyhow::ensure!(
                !cancellation.is_cancelled(),
                "YDB Topic delivery discovery cancelled"
            );
            self.configured_delivery_discovery(request)
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

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

#[cfg(test)]
mod tests;
