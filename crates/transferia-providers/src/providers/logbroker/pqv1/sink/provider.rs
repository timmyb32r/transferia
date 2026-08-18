use std::sync::Arc;

use futures_util::future::BoxFuture;

use super::writer::PqV1Sink;
use crate::providers::logbroker::pqv1::config::PqV1SinkConfig;
use crate::providers::traits::{SinkBuildContext, SinkPrepare, SinkProvider};
use crate::serializer::JsonBatchEncoder;
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::EndpointDescriptor;

pub struct PqV1SinkProvider {
    config: Arc<PqV1SinkConfig>,
    token: Arc<str>,
}

impl PqV1SinkProvider {
    pub fn from_config(config: PqV1SinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let token =
            crate::providers::logbroker::pqv1::credentials::load_access_token(&config.auth)?;
        Ok(Self {
            config: Arc::new(config),
            token: Arc::from(token),
        })
    }
}

impl SinkLimits for PqV1SinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "pqv1",
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
            "PQv1 sink requires at least one dataset"
        );
        for dataset in &discovery.datasets {
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "PQv1 JSON message for dataset '{}' cannot have an empty schema",
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
                    "dataset '{}' is not serializable as PQv1 JSON",
                    dataset.name
                ))
            })?;
        }
        let message_group_id = &discovery
            .dataset(transferia_core::delivery::DatasetRole::Main)?
            .name;
        anyhow::ensure!(
            message_group_id.len() <= 2048,
            "PQv1 message group derived from table name must contain at most 2048 UTF-8 bytes"
        );
        Ok(())
    }
}

impl SinkProvider for PqV1SinkProvider {
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
            let message_group_id = match &self.config.message_group_id {
                Some(configured) => configured.clone(),
                None => context
                    .discovery
                    .dataset(transferia_core::delivery::DatasetRole::Main)?
                    .name
                    .to_string(),
            };
            Ok(Box::new(PqV1Sink::new(
                Arc::clone(&self.config),
                Arc::from(message_group_id),
                Arc::clone(&self.token),
                context,
            )) as Box<dyn Sink>)
        })
    }
}
