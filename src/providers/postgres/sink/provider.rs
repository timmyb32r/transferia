use std::sync::Arc;

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;

use super::config::PostgresSinkConfig;
use super::writer::PostgresSink;
use crate::core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use crate::core::sink::Sink;
use crate::delivery::semantics::EndpointDescriptor;
use crate::providers::postgres::common::{
    arrow_to_postgres, connect, quote_identifier, validate_identifier, MAX_IDENTIFIER_BYTES,
};
use crate::providers::traits::{SinkBuildContext, SinkPrepare, SinkProvider};

pub struct PostgresSinkProvider {
    config: Arc<PostgresSinkConfig>,
}

impl PostgresSinkProvider {
    pub fn from_config(config: PostgresSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl SinkLimits for PostgresSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let name = TextLimit {
            syntax: NameSyntax::AsciiIdentifier,
            max_utf8_bytes: Some(MAX_IDENTIFIER_BYTES),
        };
        SinkLimitsDescription {
            sink: "postgres",
            dataset_name: Some(name.clone()),
            column_name: Some(name),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "PostgreSQL sink requires at least one dataset"
        );
        let mut names = std::collections::HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "PostgreSQL datasets repeat table '{}'",
                dataset.name
            );
            validate_identifier("table", &dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "PostgreSQL table '{}' cannot have an empty schema",
                dataset.name
            );
            for column in &dataset.stored_schema.columns {
                validate_identifier("column", &column.name)?;
                arrow_to_postgres(&column.data_type)?;
            }
        }
        Ok(())
    }
}

impl SinkProvider for PostgresSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PostgresSink
    }
    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(
        &self,
        column: &crate::core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        let data_type = postgres_sql_type(&column.data_type)?;
        Ok(format!(
            "{data_type} {}",
            if column.nullable { "NULL" } else { "NOT NULL" }
        ))
    }
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if !self.config.create_tables {
                return Ok(());
            }
            let client = connect(&self.config.connection).await?;
            for dataset in request.datasets {
                let columns = dataset
                    .schema
                    .columns
                    .iter()
                    .map(|column| {
                        let sql_type = postgres_sql_type(&column.data_type)?;
                        Ok(format!(
                            "{} {sql_type}{}",
                            quote_identifier(&column.name),
                            if column.nullable { "" } else { " NOT NULL" }
                        ))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?
                    .join(", ");
                client
                    .batch_execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {} ({columns})",
                        quote_identifier(&dataset.table)
                    ))
                    .await?;
            }
            Ok(())
        })
    }
    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let client = connect(&self.config.connection).await?;
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            Ok(Box::new(PostgresSink::new(
                client,
                context.counters,
                context.discovery,
                limits,
            )) as Box<dyn Sink>)
        })
    }
}

#[expect(
    clippy::unreachable,
    reason = "arrow_to_postgres rejects every type outside this exhaustive supported subset"
)]
fn postgres_sql_type(data_type: &DataType) -> anyhow::Result<&'static str> {
    arrow_to_postgres(data_type)?;
    Ok(match data_type {
        DataType::Boolean => "boolean",
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Utf8 => "text",
        DataType::Date32 => "date",
        DataType::Timestamp(_, None) => "timestamp",
        _ => unreachable!(),
    })
}
