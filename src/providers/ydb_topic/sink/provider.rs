use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;

use super::config::YdbTopicSinkConfig;
use super::writer::YdbTopicSink;
use crate::compatibility::EndpointDescriptor;
use crate::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use crate::pipeline::sink::Sink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};
use crate::providers::ydb_topic::src_stream::{YdbTopicAuthConfig, YdbTopicDriver};
use crate::serializer::JsonBatchEncoder;

pub struct YdbTopicSinkProvider {
    config: Arc<YdbTopicSinkConfig>,
    token: Arc<str>,
}

impl YdbTopicSinkProvider {
    fn from_config(config: YdbTopicSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        anyhow::ensure!(
            config.driver == YdbTopicDriver::Ydb,
            "YdbTopicSinkProvider requires driver=ydb"
        );
        let token = config.auth.load_token()?;
        Ok(Self {
            config: Arc::new(config),
            token: Arc::from(token),
        })
    }
}

pub fn build_sink_provider(value: Value) -> anyhow::Result<Box<dyn SinkProvider>> {
    let config: YdbTopicSinkConfig = serde_yaml::from_value(value)
        .map_err(|error| anyhow::anyhow!("Failed to parse Logbroker sink config: {error}"))?;
    config.validate()?;
    match config.driver {
        YdbTopicDriver::Ydb => Ok(Box::new(YdbTopicSinkProvider::from_config(config)?)),
        YdbTopicDriver::Pqv1 => {
            let partition_id = config.partition_id.ok_or_else(|| {
                anyhow::anyhow!("ydb_topic.driver=pqv1 requires an explicit partition_id")
            })?;
            let auth = match config.auth {
                YdbTopicAuthConfig::Token { token } => serde_yaml::to_value(serde_json::json!({
                    "type": "access_token",
                    "token": token,
                    "token_file": null
                }))?,
                YdbTopicAuthConfig::TokenFile { token_file } => {
                    serde_yaml::to_value(serde_json::json!({
                        "type": "access_token",
                        "token": null,
                        "token_file": token_file
                    }))?
                }
            };
            let pqv1 = serde_yaml::to_value(serde_json::json!({
                "host": config.host,
                "port": config.port,
                "topic_path": config.topic_path,
                "message_group_id": config.producer_id,
                "partition_group_id": partition_id,
                "auth": auth,
                "trusted_plaintext": config.trusted_plaintext,
                "network_timeout_ms": 30_000
            }))?;
            Ok(Box::new(
                crate::providers::pqv1::PqV1SinkProvider::from_config(pqv1)?,
            ))
        }
    }
}

impl SinkLimits for YdbTopicSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "ydb_topic",
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
        Ok(())
    }
}

impl SinkProvider for YdbTopicSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PqV1Sink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            Ok(Box::new(YdbTopicSink::new(
                Arc::clone(&self.config),
                Arc::clone(&self.token),
                context,
            )) as Box<dyn Sink>)
        })
    }
}
