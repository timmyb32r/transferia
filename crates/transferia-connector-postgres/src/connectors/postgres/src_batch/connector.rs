use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;

use super::config::{PostgresSourceConfig, TableConfig};
use super::reader::PostgresSource;
use crate::connectors::postgres::common::{
    connect, postgres_to_arrow, quote_identifier, validate_identifier,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA,
    SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

pub const POSTGRES_REPLICATION_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
    SystemColumnKind::ChangeOperation,
    SystemColumnKind::ChangedColumns,
];

pub struct PostgresCdcMetadataColumn {
    pub(crate) name: &'static str,

    pub(crate) role: &'static str,

    pub(crate) data_type: arrow::datatypes::DataType,
}

pub const POSTGRES_CDC_METADATA_COLUMNS: &[PostgresCdcMetadataColumn] = &[
    PostgresCdcMetadataColumn {
        name: "_system_source_database",
        role: SYSTEM_ROLE_SOURCE_DATABASE,
        data_type: arrow::datatypes::DataType::Utf8,
    },
    PostgresCdcMetadataColumn {
        name: "_system_source_schema",
        role: SYSTEM_ROLE_SOURCE_SCHEMA,
        data_type: arrow::datatypes::DataType::Utf8,
    },
    PostgresCdcMetadataColumn {
        name: "_system_source_table",
        role: SYSTEM_ROLE_SOURCE_TABLE,
        data_type: arrow::datatypes::DataType::Utf8,
    },
    PostgresCdcMetadataColumn {
        name: "_system_source_transaction_id",
        role: SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
        data_type: arrow::datatypes::DataType::UInt64,
    },
    PostgresCdcMetadataColumn {
        name: "_system_source_timestamp_ms",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresCdcMetadataColumn {
        name: "_system_source_timestamp_us",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresCdcMetadataColumn {
        name: "_system_source_timestamp_ns",
        role: SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresCdcMetadataColumn {
        name: "_system_event_timestamp_ms",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresCdcMetadataColumn {
        name: "_system_event_timestamp_us",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        data_type: arrow::datatypes::DataType::Int64,
    },
    PostgresCdcMetadataColumn {
        name: "_system_event_timestamp_ns",
        role: SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
        data_type: arrow::datatypes::DataType::Int64,
    },
];

const POSTGRES_SNAPSHOT_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

#[derive(Clone)]
pub struct DiscoveredTable {
    pub(crate) config: TableConfig,
    pub(crate) schema: DatasetSchema,
    pub(crate) type_oids: Vec<u32>,
    pub(crate) replica_identity_full: bool,
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
            behavior: if self.config.replication.is_some() {
                SourceBehavior::ChangelogRows
            } else {
                SourceBehavior::FiniteAppendOnlyRows
            },
            delivery_modes: if self.config.replication.is_some() {
                SourceDeliveryModes::STREAM
            } else {
                SourceDeliveryModes::BATCH
            },
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
            let system_columns = if self.config.replication.is_some() {
                POSTGRES_REPLICATION_SYSTEM_COLUMNS
            } else {
                POSTGRES_SNAPSHOT_SYSTEM_COLUMNS
            };
            let discovered_system_columns = system_columns
                .iter()
                .copied()
                .map(Into::into)
                .collect::<Vec<_>>();
            let datasets = tables
                .iter()
                .map(|table| {
                    let mut incoming = incoming_user_schema(&table.schema);
                    if self.config.replication.is_some() {
                        if table.replica_identity_full {
                            incoming
                                .columns
                                .extend(table.schema.columns.iter().enumerate().map(
                                    |(index, column)| {
                                        SchemaColumn::new(
                                            old_value_column_name(index),
                                            column.data_type.clone(),
                                            true,
                                        )
                                        .with_old_value_of(column.name.clone())
                                    },
                                ));
                        } else {
                            incoming.columns.extend(
                                table
                                    .schema
                                    .columns
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, column)| column.primary_key)
                                    .map(|(index, column)| {
                                        SchemaColumn::new(
                                            old_key_column_name(index),
                                            column.data_type.clone(),
                                            true,
                                        )
                                        .with_old_key_of(column.name.clone())
                                    }),
                            );
                        }
                        incoming
                            .columns
                            .extend(POSTGRES_CDC_METADATA_COLUMNS.iter().map(|column| {
                                SchemaColumn::new(
                                    column.name.to_owned(),
                                    column.data_type.clone(),
                                    false,
                                )
                                .with_system_role(column.role)
                            }));
                    }
                    incoming.columns.extend(system_columns.iter().map(|kind| {
                        SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)
                    }));
                    let mut stored = table.schema.clone();
                    if request.keep_system_columns {
                        stored.columns.extend(
                            system_columns
                                .iter()
                                .filter(|kind| {
                                    !matches!(
                                        kind,
                                        SystemColumnKind::ChangeOperation
                                            | SystemColumnKind::ChangedColumns
                                    )
                                })
                                .map(|kind| {
                                    SchemaColumn::new(
                                        kind.default_name().to_owned(),
                                        kind.data_type(),
                                        false,
                                    )
                                }),
                        );
                    }
                    DiscoveredDataset {
                        role: DatasetRole::Main,
                        name: Arc::from(table.config.name.as_str()),
                        incoming_schema: incoming,
                        stored_schema: stored,
                        system_columns: discovered_system_columns.clone(),
                    }
                })
                .collect();
            let source_topology = if self.config.replication.is_some() {
                SourceTopology::StaticPartitions(vec![0])
            } else {
                SourceTopology::StaticPartitions(
                    (0..tables.len())
                        .map(i64::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                )
            };
            Ok(DeliveryDiscovery {
                source_name: Arc::from("postgres"),
                source_topology,
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: request.keep_system_columns,
                datasets,
                performance_advice: Vec::new(),
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
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            let client = connect(&self.config.connection).await?;
            if let Some(replication) = &self.config.replication {
                anyhow::ensure!(
                    partition_id == 0,
                    "PostgreSQL replication has exactly one source partition"
                );
                return Ok(Box::new(
                    crate::connectors::postgres::src_dblog::PostgresReplicationSource::new(
                        client,
                        replication.clone(),
                        Arc::from(self.config.connection.database.as_str()),
                        tables.as_ref().clone(),
                        counters,
                        context.cancellation,
                        context.durable,
                    )
                    .await?,
                ) as Box<dyn Source>);
            }
            let index = usize::try_from(partition_id)?;
            let table = tables
                .get(index)
                .ok_or_else(|| {
                    anyhow::anyhow!("PostgreSQL source partition {partition_id} does not exist")
                })?
                .clone();
            Ok(Box::new(
                PostgresSource::new(
                    client,
                    table.config,
                    incoming_user_schema(&table.schema),
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

pub(super) fn incoming_user_schema(stored: &DatasetSchema) -> DatasetSchema {
    // Snapshot and CDC expose one stable Arrow user schema. CDC needs nullable
    // incoming fields for unchanged TOAST values; snapshots use the same
    // representation so consumers cannot distinguish the modes by data fields.
    let mut incoming = stored.clone();
    for column in &mut incoming.columns {
        column.nullable = true;
    }
    incoming
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
    let physical_types = client
        .query(
            "SELECT a.attname, a.atttypid, EXISTS (\
                 SELECT 1 FROM pg_index AS i \
                 WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)\
             ) AS primary_key \
             FROM pg_attribute AS a \
             JOIN pg_class AS c ON c.oid = a.attrelid \
             JOIN pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&table.schema, &table.name],
        )
        .await?;
    let replica_identity = client
        .query_one(
            "SELECT c.relreplident::text FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2",
            &[&table.schema, &table.name],
        )
        .await?
        .get::<_, String>(0);
    anyhow::ensure!(
        physical_types.len() == statement.columns().len(),
        "PostgreSQL physical schema for '{}.{}' has {} columns, query declared {}",
        table.schema,
        table.name,
        physical_types.len(),
        statement.columns().len()
    );
    let columns = statement
        .columns()
        .iter()
        .zip(&physical_types)
        .map(|(column, physical)| {
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
            )
            .with_constraints(physical.get::<_, bool>(2), false, None))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let type_oids = physical_types
        .iter()
        .zip(statement.columns())
        .map(|(row, column)| {
            let name: String = row.get(0);
            anyhow::ensure!(
                name == column.name(),
                "PostgreSQL physical/query schema order differs at '{}' versus '{}'",
                name,
                column.name()
            );
            Ok(row.get::<_, u32>(1))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for index in 0..columns.len() {
        for reserved in [old_value_column_name(index), old_key_column_name(index)] {
            anyhow::ensure!(
                columns.iter().all(|column| column.name != reserved),
                "PostgreSQL table '{}.{}' column '{}' conflicts with a reserved CDC control column name",
                table.schema,
                table.name,
                reserved,
            );
        }
    }
    Ok(DiscoveredTable {
        config: table,
        schema: DatasetSchema::new(columns),
        type_oids,
        replica_identity_full: replica_identity == "f",
    })
}

pub fn old_value_column_name(index: usize) -> String {
    format!("_system_old_value_{index}")
}

pub fn old_key_column_name(index: usize) -> String {
    format!("_system_old_key_{index}")
}
