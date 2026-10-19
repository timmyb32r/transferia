use std::sync::Arc;

use futures_util::future::BoxFuture;

use super::writer::PqV1Sink;
use crate::connectors::logbroker::pqv1::config::PqV1SinkConfig;
use crate::serializer::JsonBatchEncoder;
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::{EndpointDescriptor, QueueSinkDescriptor};
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

pub struct PqV1SinkConnector {
    config: Arc<PqV1SinkConfig>,
    token: Arc<str>,
}

impl PqV1SinkConnector {
    pub fn from_config(config: PqV1SinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let token =
            crate::connectors::logbroker::pqv1::credentials::load_access_token(&config.auth)?;
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
                max_utf8_bytes: Some(2048),
            }),
            column_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
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

    fn validate_batch(
        &self,
        discovery: &DeliveryDiscovery,
        batch: &transferia_core::SinkBatch,
    ) -> anyhow::Result<()> {
        crate::connectors::logbroker::validate_producer_id(&batch.table)?;
        transferia_core::delivery::validate_batch_against_discovery(discovery, batch)?;
        Ok(())
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty()
                || discovery.schema_origin == transferia_core::SchemaOrigin::ParserRuntime,
            "PQv1 sink requires at least one dataset"
        );
        for dataset in &discovery.datasets {
            crate::connectors::logbroker::validate_producer_id(&dataset.name)?;
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
        self.serializer.validate_discovery(discovery)?;
        Ok(())
    }
}

impl SinkConnector for PqV1SinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::LogbrokerSink(QueueSinkDescriptor {
            changelog: self.config.serializer.supports_changelog(),
        })
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
    fn prepare_runtime(
        &self,
        request: SinkPrepare,
        _previous: Option<transferia_core::DiscoveredDataset>,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        // This destination has no fixed table schema to ALTER. Serialization
        // and value-dependent constraints are checked again before writing.
        self.prepare(request)
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            Ok(Box::new(PqV1Sink::new(
                Arc::clone(&self.config),
                Arc::clone(&self.token),
                context,
            )) as Box<dyn Sink>)
        })
    }
}
