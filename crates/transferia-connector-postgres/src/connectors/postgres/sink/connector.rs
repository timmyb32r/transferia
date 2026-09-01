use std::sync::Arc;

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;

use super::config::PostgresSinkConfig;
use super::writer::PostgresSink;
use crate::connectors::postgres::common::{
    arrow_to_postgres, connect, quote_identifier, validate_identifier, MAX_IDENTIFIER_BYTES,
};
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::SystemColumnKind;
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

pub struct PostgresSinkConnector {
    config: Arc<PostgresSinkConfig>,
}

impl PostgresSinkConnector {
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
            let mut primary_keys = 0;
            for column in &dataset.stored_schema.columns {
                validate_identifier("column", &column.name)?;
                arrow_to_postgres(&column.data_type)?;
                if column.primary_key {
                    primary_keys += 1;
                    anyhow::ensure!(
                        !column.nullable,
                        "PostgreSQL primary-key column '{}.{}' must not be nullable",
                        dataset.name,
                        column.name
                    );
                }
            }
            if dataset
                .system_columns
                .iter()
                .any(|column| column.kind == SystemColumnKind::ChangeOperation)
            {
                anyhow::ensure!(
                    primary_keys > 0,
                    "PostgreSQL changelog dataset '{}' requires a primary key",
                    dataset.name
                );
            }
        }
        Ok(())
    }
}

impl SinkConnector for PostgresSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PostgresSink
    }
    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(
        &self,
        column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        let data_type = postgres_sql_type(&column.data_type)?;
        Ok(format!(
            "{data_type} {}",
            if column.nullable { "NULL" } else { "NOT NULL" }
        ))
    }
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let client = connect(&self.config.connection).await?;
            for dataset in request.datasets {
                if self.config.create_tables {
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
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let primary_key = dataset
                        .schema
                        .columns
                        .iter()
                        .filter(|column| column.primary_key)
                        .map(|column| quote_identifier(&column.name))
                        .collect::<Vec<_>>();
                    let mut definitions = columns;
                    if !primary_key.is_empty() {
                        definitions.push(format!("PRIMARY KEY ({})", primary_key.join(", ")));
                    }
                    client
                        .batch_execute(&format!(
                            "CREATE TABLE IF NOT EXISTS {} ({})",
                            quote_identifier(&dataset.table),
                            definitions.join(", ")
                        ))
                        .await?;
                }
                validate_changelog_primary_key(&client, &dataset).await?;
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

async fn validate_changelog_primary_key(
    client: &tokio_postgres::Client,
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<()> {
    if !dataset.changelog {
        return Ok(());
    }
    let rows = client
        .query(
            "SELECT attribute.attname \
             FROM pg_index AS idx \
             JOIN pg_class AS table_class ON table_class.oid = idx.indrelid \
             JOIN pg_namespace AS namespace ON namespace.oid = table_class.relnamespace \
             JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS key(attnum, position) ON TRUE \
             JOIN pg_attribute AS attribute ON attribute.attrelid = table_class.oid AND attribute.attnum = key.attnum \
             WHERE namespace.nspname = current_schema() AND table_class.relname = $1 AND idx.indisprimary \
             ORDER BY key.position",
            &[&dataset.table.as_ref()],
        )
        .await?;
    let actual = rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    let expected = dataset
        .schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual.iter().map(String::as_str).collect::<Vec<_>>() == expected,
        "PostgreSQL changelog table '{}' has primary key {actual:?}, expected {expected:?}",
        dataset.table
    );
    Ok(())
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
