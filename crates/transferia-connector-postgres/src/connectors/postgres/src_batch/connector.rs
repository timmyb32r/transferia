use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;

use super::config::{PostgresSourceConfig, TableConfig};
use super::reader::PostgresSource;
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::connectors::postgres::common::{
    connect, postgres_to_arrow, quote_identifier, validate_identifier,
};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceDiscoveryContext, SourceConnector};

#[derive(Clone)]
struct DiscoveredTable {
    config: TableConfig,
    schema: DatasetSchema,
}

pub struct PostgresSourceConnector {
    config: PostgresSourceConfig,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl PostgresSourceConnector {
    pub fn from_config(
        config: PostgresSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn discovered_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async {
                let client = connect(&self.config.connection).await?;
                let mut tables = Vec::with_capacity(self.config.tables.len());
                for table in &self.config.tables {
                    tables.push(discover_table(&client, table.clone()).await?);
                }
                Ok(Arc::new(tables))
            })
            .await
            .map(Arc::clone)
    }

    fn counters(&self, partition: i64) -> Arc<SourceCounters> {
        Arc::clone(
            self.counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(partition)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }
}

impl SourceConnector for PostgresSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Postgres(SourceDescriptor {
            behavior: SourceBehavior::FiniteSnapshotRows,
            delivery_modes: SourceDeliveryModes::BATCH,
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
            let tables = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("PostgreSQL discovery cancelled"), tables = self.discovered_tables() => tables? };
            let system_columns = [
                SystemColumnKind::Topic,
                SystemColumnKind::Partition,
                SystemColumnKind::Offset,
                SystemColumnKind::MessageIndex,
            ];
            let discovered_system_columns = system_columns
                .iter()
                .copied()
                .map(Into::into)
                .collect::<Vec<_>>();
            let datasets = tables
                .iter()
                .map(|table| {
                    let mut incoming = table.schema.clone();
                    incoming.columns.extend(system_columns.iter().map(|kind| {
                        SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)
                    }));
                    let stored = if request.keep_system_columns {
                        incoming.clone()
                    } else {
                        table.schema.clone()
                    };
                    DiscoveredDataset {
                        role: DatasetRole::Main,
                        name: Arc::from(table.config.name.as_str()),
                        incoming_schema: incoming,
                        stored_schema: stored,
                        system_columns: discovered_system_columns.clone(),
                    }
                })
                .collect();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("postgres"),
                source_topology: SourceTopology::StaticPartitions(
                    (0..tables.len())
                        .map(i64::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: request.keep_system_columns,
                datasets,
            })
        })
    }

    fn build_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let partition_id = context.partition_id;
            let tables = self.discovered_tables().await?;
            let index = usize::try_from(partition_id)?;
            let table = tables
                .get(index)
                .ok_or_else(|| {
                    anyhow::anyhow!("PostgreSQL source partition {partition_id} does not exist")
                })?
                .clone();
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            let client = connect(&self.config.connection).await?;
            Ok(Box::new(
                PostgresSource::new(
                    client,
                    table.config,
                    table.schema,
                    self.config.batch_rows,
                    counters,
                )
                .await?,
            ) as Box<dyn Source>)
        })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

async fn discover_table(
    client: &tokio_postgres::Client,
    table: TableConfig,
) -> anyhow::Result<DiscoveredTable> {
    let query = format!(
        "SELECT * FROM {}.{} LIMIT 0",
        quote_identifier(&table.schema),
        quote_identifier(&table.name)
    );
    let statement = client.prepare(&query).await.map_err(|error| {
        anyhow::anyhow!(
            "cannot inspect PostgreSQL table '{}.{}': {error}",
            table.schema,
            table.name
        )
    })?;
    anyhow::ensure!(
        !statement.columns().is_empty(),
        "PostgreSQL table '{}.{}' has no columns",
        table.schema,
        table.name
    );
    let nullability = client.query(
        "SELECT column_name, is_nullable = 'YES' FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
        &[&table.schema, &table.name],
    ).await?.into_iter().map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1))).collect::<HashMap<_, _>>();
    let columns = statement
        .columns()
        .iter()
        .map(|column| {
            validate_identifier("column", column.name())?;
            let nullable = *nullability.get(column.name()).ok_or_else(|| {
                anyhow::anyhow!(
                    "missing nullability metadata for column '{}'",
                    column.name()
                )
            })?;
            Ok(SchemaColumn::new(
                column.name().to_owned(),
                postgres_to_arrow(column.type_())?,
                nullable,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(DiscoveredTable {
        config: table,
        schema: DatasetSchema::new(columns),
    })
}
