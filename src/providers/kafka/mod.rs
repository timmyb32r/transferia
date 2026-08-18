mod config;
mod sink;
mod source;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;

pub use config::{KafkaSecurityConfig, KafkaSinkConfig, KafkaSourceConfig};
use sink::KafkaSink;

use crate::core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, SourceTopology, TextLimit,
};
use crate::core::sink::Sink;
use crate::core::source::Source;
use crate::delivery::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::providers::traits::{
    SinkBuildContext, SinkPrepare, SinkProvider, SourceBuildContext, SourceDiscoveryContext,
    SourceProvider,
};
use crate::serializer::JsonBatchEncoder;

pub struct KafkaSourceProvider {
    config: Arc<KafkaSourceConfig>,
    parser_plan: ParserPlan,
    metrics_registry: Arc<MetricsRegistry>,
    source_counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl KafkaSourceProvider {
    pub fn from_config(
        config: KafkaSourceConfig,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        validate_source_config(&config)?;
        let parser_plan = ParserPlan::from_config(&config.parser, &config.topics[0])?;
        Ok(Self {
            config: Arc::new(config),
            parser_plan,
            metrics_registry,
            source_counters: Mutex::new(HashMap::new()),
        })
    }

    fn consumer(&self) -> anyhow::Result<StreamConsumer> {
        let mut config = config::base_client_config(
            &self.config.brokers,
            &self.config.security,
            self.config.request_timeout_ms,
        )?;
        config
            .set("group.id", &self.config.consumer_group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", self.config.offset_reset.as_str())
            .set("enable.partition.eof", "false");
        config.create().map_err(Into::into)
    }

    fn counters_for_partition(&self, partition_id: i64) -> Arc<SourceCounters> {
        let mut counters = self
            .source_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(counters.entry(partition_id).or_insert_with(|| {
            let counters = Arc::new(SourceCounters::new());
            self.metrics_registry
                .register_source(partition_id, Arc::clone(&counters));
            counters
        }))
    }
}

impl SourceProvider for KafkaSourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Kafka(SourceDescriptor {
            behavior: SourceBehavior::ProducesRows,
            delivery_modes: SourceDeliveryModes::STREAM,
        })
    }

    fn delivery_discovery(
        &self,
        context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            self.parser_plan.delivery_discovery(
                Arc::from(self.config.topics[0].as_str()),
                SourceTopology::DynamicWorkerLanes,
                context.request,
            )
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let consumer = self.consumer()?;
            let topics = self
                .config
                .topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            consumer.subscribe(&topics)?;
            Ok(Box::new(source::KafkaSource::new(
                consumer,
                Arc::clone(&self.config),
                context.cancellation,
                context.memory,
                self.counters_for_partition(context.partition_id),
            )) as Box<dyn Source>)
        })
    }

    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}

pub struct KafkaSinkProvider {
    config: Arc<KafkaSinkConfig>,
}

impl KafkaSinkProvider {
    pub fn from_config(config: KafkaSinkConfig) -> anyhow::Result<Self> {
        validate_sink_config(&config)?;
        Ok(Self {
            config: Arc::new(config),
        })
    }

    fn producer(&self) -> anyhow::Result<FutureProducer> {
        let mut config = config::base_client_config(
            &self.config.brokers,
            &self.config.security,
            self.config.request_timeout_ms,
        )?;
        config
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("max.in.flight.requests.per.connection", "5");
        config.create().map_err(Into::into)
    }
}

impl SinkLimits for KafkaSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "kafka",
            dataset_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
            column_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Date64,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "Kafka sink requires at least one dataset"
        );
        for dataset in &discovery.datasets {
            validate_stored_projection(discovery, dataset)?;
            let schema = arrow::datatypes::Schema::new(
                dataset
                    .stored_schema
                    .columns
                    .iter()
                    .map(|column| {
                        arrow::datatypes::Field::new(
                            &column.name,
                            column.data_type.clone(),
                            column.nullable,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            let arrays = schema
                .fields()
                .iter()
                .map(|field| arrow::array::new_null_array(field.data_type(), 0))
                .collect::<Vec<_>>();
            let batch = arrow::record_batch::RecordBatch::try_new(Arc::new(schema), arrays)?;
            JsonBatchEncoder::new(&batch, |_| true)?;
        }
        Ok(())
    }
}

impl SinkProvider for KafkaSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::KafkaSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let producer = self.producer()?;
            let serializer = crate::serializer::DeliverySerializer::new(&self.config.serializer)?;
            Ok(Box::new(KafkaSink::new(
                Arc::clone(&self.config),
                producer,
                serializer,
                context,
            )) as Box<dyn Sink>)
        })
    }
}

pub async fn check_source_connection(config: &KafkaSourceConfig) -> anyhow::Result<()> {
    validate_source_config(config)?;
    check_metadata(
        &config.brokers,
        &config.security,
        config.request_timeout_ms,
        &config.topics,
    )
    .await
}

pub async fn check_sink_connection(config: &KafkaSinkConfig) -> anyhow::Result<()> {
    validate_sink_config(config)?;
    check_metadata(
        &config.brokers,
        &config.security,
        config.request_timeout_ms,
        core::slice::from_ref(&config.topic),
    )
    .await
}

async fn check_metadata(
    brokers: &[String],
    security: &KafkaSecurityConfig,
    request_timeout_ms: u64,
    topics: &[String],
) -> anyhow::Result<()> {
    let config = config::base_client_config(brokers, security, request_timeout_ms)?;
    let consumer: rdkafka::consumer::BaseConsumer = config.create()?;
    let topic = topics.first().cloned();
    let metadata = tokio::task::spawn_blocking(move || {
        consumer.fetch_metadata(topic.as_deref(), config::timeout(request_timeout_ms))
    })
    .await??;
    for topic in topics {
        let metadata_topic = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .ok_or_else(|| anyhow::anyhow!("Kafka metadata did not include topic '{topic}'"))?;
        anyhow::ensure!(
            metadata_topic.error().is_none(),
            "Kafka rejected topic '{topic}': {:?}",
            metadata_topic.error()
        );
    }
    Ok(())
}

fn validate_source_config(config: &KafkaSourceConfig) -> anyhow::Result<()> {
    config::validate_brokers(&config.brokers)?;
    anyhow::ensure!(!config.topics.is_empty(), "kafka.topics must not be empty");
    for topic in &config.topics {
        anyhow::ensure!(
            !topic.is_empty(),
            "kafka.topics must not contain empty values"
        );
        anyhow::ensure!(
            topic == topic.trim(),
            "Kafka topic names must not have leading or trailing whitespace"
        );
    }
    anyhow::ensure!(
        !config.consumer_group.is_empty(),
        "kafka.consumer_group must not be empty"
    );
    anyhow::ensure!(
        config.consumer_group == config.consumer_group.trim(),
        "Kafka consumer group must not have leading or trailing whitespace"
    );
    anyhow::ensure!(
        config.batch_max_messages > 0,
        "kafka.batch_max_messages must be positive"
    );
    anyhow::ensure!(
        config.batch_max_bytes > 0,
        "kafka.batch_max_bytes must be positive"
    );
    anyhow::ensure!(
        config.request_timeout_ms > 0,
        "kafka.request_timeout_ms must be positive"
    );
    if config.topics.len() > 1
        && matches!(
            config.parser.common.table_naming,
            crate::parsers::TableNaming::FromTopicName
        )
    {
        anyhow::bail!("Kafka with multiple topics requires table_naming.type=from_config until multi-dataset parser discovery is configured");
    }
    Ok(())
}

fn validate_sink_config(config: &KafkaSinkConfig) -> anyhow::Result<()> {
    config::validate_brokers(&config.brokers)?;
    anyhow::ensure!(!config.topic.is_empty(), "kafka.topic must not be empty");
    anyhow::ensure!(
        config.topic == config.topic.trim(),
        "Kafka topic must not have leading or trailing whitespace"
    );
    anyhow::ensure!(
        config.request_timeout_ms > 0,
        "kafka.request_timeout_ms must be positive"
    );
    anyhow::ensure!(
        config.max_in_flight > 0,
        "kafka.max_in_flight must be positive"
    );
    config.serializer.validate()?;
    Ok(())
}

#[cfg(test)]
mod tests;
