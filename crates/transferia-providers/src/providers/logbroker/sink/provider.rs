use std::sync::Arc;

use futures_util::future::BoxFuture;

use super::config::LogbrokerSinkConfig;
use super::writer::YdbTopicSink;
use crate::providers::logbroker::{LogbrokerAuthConfig, LogbrokerDriver};
use crate::providers::traits::{SinkBuildContext, SinkPrepare, SinkProvider};
use crate::serializer::JsonBatchEncoder;
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::EndpointDescriptor;

pub struct YdbDriverSinkProvider {
    config: Arc<LogbrokerSinkConfig>,
    token: Arc<str>,
}

impl YdbDriverSinkProvider {
    fn from_config(config: LogbrokerSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        anyhow::ensure!(
            config.driver == LogbrokerDriver::Ydb,
            "YdbDriverSinkProvider requires driver=ydb"
        );
        let token = config.auth.load_token()?;
        Ok(Self {
            config: Arc::new(config),
            token: Arc::from(token),
        })
    }
}

pub fn build_sink_provider(config: LogbrokerSinkConfig) -> anyhow::Result<Box<dyn SinkProvider>> {
    config.validate()?;
    match config.driver {
        LogbrokerDriver::Ydb => Ok(Box::new(YdbDriverSinkProvider::from_config(config)?)),
        LogbrokerDriver::Pqv1 => {
            let partition_id = config.partition_id.ok_or_else(|| {
                anyhow::anyhow!("logbroker.driver=pqv1 requires an explicit partition_id")
            })?;
            let auth = match config.auth {
                LogbrokerAuthConfig::Token { token } => {
                    crate::providers::logbroker::pqv1::config::PqV1AuthConfig {
                        auth_type: "access_token".to_owned(),
                        token: Some(token),
                        token_file: None,
                    }
                }
                LogbrokerAuthConfig::TokenFile { token_file } => {
                    crate::providers::logbroker::pqv1::config::PqV1AuthConfig {
                        auth_type: "access_token".to_owned(),
                        token: None,
                        token_file: Some(token_file),
                    }
                }
            };
            let pqv1 = crate::providers::logbroker::pqv1::config::PqV1SinkConfig {
                host: config.host,
                port: config.port,
                topic_path: config.topic_path,
                message_group_id: None,
                partition_group_id: partition_id,
                auth,
                serializer: config.serializer,
                trusted_plaintext: config.trusted_plaintext,
                network_timeout_ms: 30_000,
            };
            Ok(Box::new(
                crate::providers::logbroker::pqv1::PqV1SinkProvider::from_config(pqv1)?,
            ))
        }
    }
}

pub async fn check_connection(
    config: &LogbrokerSinkConfig,
    cancellation: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    config.validate()?;
    crate::providers::logbroker::src_stream::check_topic_connection(
        &config.host,
        config.port,
        &config.topic_path,
        &config.auth,
        config.driver,
        cancellation,
    )
    .await
}

impl SinkLimits for LogbrokerSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "logbroker",
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
            "Logbroker sink requires at least one dataset"
        );
        for dataset in &discovery.datasets {
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "Logbroker JSON message for dataset '{}' cannot have an empty schema",
                dataset.name
            );
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
            JsonBatchEncoder::new(&batch, |_| true).map_err(|error| {
                error.context(format!(
                    "dataset '{}' is not serializable as Logbroker JSON",
                    dataset.name
                ))
            })?;
        }
        let producer_id = &discovery
            .dataset(transferia_core::delivery::DatasetRole::Main)?
            .name;
        anyhow::ensure!(
            producer_id.len() <= 2048,
            "Logbroker producer identity derived from table name must contain at most 2048 UTF-8 bytes"
        );
        Ok(())
    }
}

impl SinkProvider for YdbDriverSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::LogbrokerSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(
        &self,
        column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        self.config.serializer.destination_type(&column.data_type)
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            Ok(Box::new(YdbTopicSink::new(
                Arc::clone(&self.config),
                Arc::clone(&self.token),
                context,
            )?) as Box<dyn Sink>)
        })
    }
}
