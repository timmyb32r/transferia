use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};

use arrow::array::{Array as _, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_arrow::{ClientBuilder, Type};
use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use super::config::{ClickHouseSourceConfig, TableConfig};
use super::reader::ClickHouseSource;
use crate::compatibility::{EndpointDescriptor, SourceBehavior, SourceDescriptor};
use crate::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::providers::clickhouse::sink::client::{quote_identifier, ReconnectingClient};
use crate::providers::clickhouse::sink::identifier::validate_identifier;
use crate::providers::clickhouse::sink::table::quote_string_literal;
use crate::providers::traits::SourceProvider;
use crate::types::schema::{DatasetSchema, SchemaColumn};
use crate::types::system_columns::SystemColumnKind;

#[derive(Clone)]
pub(super) struct DiscoveredTable {
    pub config: TableConfig,
    pub schema: DatasetSchema,
}

pub struct ClickHouseSourceProvider {
    config: ClickHouseSourceConfig,
    client: Arc<ReconnectingClient>,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl ClickHouseSourceProvider {
    pub fn from_config(value: Value, metrics: Arc<MetricsRegistry>) -> anyhow::Result<Self> {
        let config: ClickHouseSourceConfig = serde_yaml::from_value(value).map_err(|error| {
            anyhow::anyhow!("Failed to parse ClickHouse source config: {error}")
        })?;
        config.validate()?;
        let builders = config
            .hosts
            .iter()
            .map(|host| {
                ClientBuilder::new()
                    .with_destination(crate::providers::address::host_port(host, config.port))
                    .with_database("default")
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_tls(false)
            })
            .collect();
        let client = Arc::new(ReconnectingClient::from_connections(
            builders,
            config.connect_timeout(),
            config.request_timeout(),
        ));
        Ok(Self {
            config,
            client,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn discovered_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async {
                self.client
                    .ensure_connected()
                    .await
                    .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
                let mut tables = Vec::with_capacity(self.config.tables.len());
                for table in &self.config.tables {
                    tables.push(discover_table(&self.client, table.clone()).await?);
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

impl SourceProvider for ClickHouseSourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::ClickHouseSource(SourceDescriptor {
            behavior: SourceBehavior::FiniteSnapshotRows,
        })
    }

    fn delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let tables = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("ClickHouse discovery cancelled"), tables = self.discovered_tables() => tables? };
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
                    DiscoveredDataset {
                        role: DatasetRole::Main,
                        name: Arc::from(table.config.output_name.as_str()),
                        incoming_schema: incoming.clone(),
                        stored_schema: if request.keep_system_columns {
                            incoming
                        } else {
                            table.schema.clone()
                        },
                        system_columns: discovered_system_columns.clone(),
                    }
                })
                .collect();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("clickhouse"),
                source_partitions: (0..tables.len())
                    .map(i64::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: request.keep_system_columns,
                datasets,
            })
        })
    }

    fn build_source(
        &self,
        partition_id: i64,
        cancellation: CancellationToken,
        _memory: PipelineMemory,
        _durable: crate::durable::DurableContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let tables = self.discovered_tables().await?;
            let table = tables
                .get(usize::try_from(partition_id)?)
                .ok_or_else(|| {
                    anyhow::anyhow!("ClickHouse source partition {partition_id} does not exist")
                })?
                .clone();
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            let query = snapshot_query(&table.config);
            let stream = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("ClickHouse read cancelled"), stream = self.client.query_stream(&query) => stream.map_err(|error| anyhow::anyhow!("ClickHouse snapshot query failed: {error}"))? };
            Ok(Box::new(ClickHouseSource::new(
                table,
                partition_id,
                stream,
                self.config.batch_rows,
                self.config.request_timeout(),
                counters,
            )) as Box<dyn Source>)
        })
    }

    fn partitions_for_worker(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        Box::pin(async move {
            anyhow::ensure!(
                total_workers > 0 && worker_index < total_workers,
                "invalid worker assignment"
            );
            Ok((0..self.config.tables.len())
                .filter(|index| (*index as u32) % total_workers == worker_index)
                .map(i64::try_from)
                .collect::<Result<Vec<_>, _>>()?)
        })
    }

    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}

fn snapshot_query(table: &TableConfig) -> String {
    let order = table
        .order_by
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT * FROM {}.{} ORDER BY {order}",
        quote_identifier(&table.database),
        quote_identifier(&table.name)
    )
}

async fn discover_table(
    client: &ReconnectingClient,
    table: TableConfig,
) -> anyhow::Result<DiscoveredTable> {
    let query = format!("SELECT name, type, default_kind FROM system.columns WHERE database = {} AND table = {} ORDER BY position", quote_string_literal(&table.database), quote_string_literal(&table.name));
    let batches = client.query_all(&query).await.map_err(|error| {
        anyhow::anyhow!(
            "cannot inspect ClickHouse table '{}.{}': {error}",
            table.database,
            table.name
        )
    })?;
    let mut columns = Vec::new();
    let mut names = HashSet::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() == 3,
            "ClickHouse schema query returned {} columns instead of 3",
            batch.num_columns()
        );
        let name_values = cast(batch.column(0), &DataType::Utf8)?;
        let type_values = cast(batch.column(1), &DataType::Utf8)?;
        let default_values = cast(batch.column(2), &DataType::Utf8)?;
        let name_values = name_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse schema names are not strings"))?;
        let type_values = type_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse schema types are not strings"))?;
        let default_values = default_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse default kinds are not strings"))?;
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !name_values.is_null(row)
                    && !type_values.is_null(row)
                    && !default_values.is_null(row),
                "ClickHouse schema metadata contains NULL"
            );
            let name = name_values.value(row);
            validate_identifier(name).map_err(|error| {
                error.context(format!("unsupported ClickHouse source column {name:?}"))
            })?;
            anyhow::ensure!(
                names.insert(name.to_owned()),
                "ClickHouse table '{}.{}' contains duplicate column '{name}'",
                table.database,
                table.name
            );
            anyhow::ensure!(default_values.value(row).is_empty(), "ClickHouse source column '{}.{}.{name}' is generated ({}) and cannot be snapshotted through SELECT *", table.database, table.name, default_values.value(row));
            let clickhouse_type = Type::from_str(type_values.value(row))?;
            columns.push(SchemaColumn::new(
                name.to_owned(),
                source_arrow_type(&clickhouse_type)?,
                clickhouse_type.is_nullable(),
            ));
        }
    }
    anyhow::ensure!(
        !columns.is_empty(),
        "ClickHouse source table '{}.{}' does not exist or has no columns",
        table.database,
        table.name
    );
    for key in &table.order_by {
        anyhow::ensure!(
            columns.iter().any(|column| &column.name == key),
            "ClickHouse order_by column '{key}' is absent from '{}.{}'",
            table.database,
            table.name
        );
    }
    Ok(DiscoveredTable {
        config: table,
        schema: DatasetSchema::new(columns),
    })
}

fn source_arrow_type(clickhouse_type: &Type) -> anyhow::Result<DataType> {
    Ok(match clickhouse_type.strip_null() {
        Type::Int8 => DataType::Int8,
        Type::Int16 => DataType::Int16,
        Type::Int32 => DataType::Int32,
        Type::Int64 => DataType::Int64,
        Type::UInt8 => DataType::UInt8,
        Type::UInt16 => DataType::UInt16,
        Type::UInt32 => DataType::UInt32,
        Type::UInt64 => DataType::UInt64,
        Type::Float32 => DataType::Float32,
        Type::Float64 => DataType::Float64,
        Type::String => DataType::Binary,
        Type::DateTime(timezone) => {
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from(timezone.name())))
        }
        Type::DateTime64(precision, timezone) => DataType::Timestamp(
            match precision {
                0 => TimeUnit::Second,
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                7..=9 => TimeUnit::Nanosecond,
                _ => anyhow::bail!("unsupported ClickHouse DateTime64 precision {precision}"),
            },
            Some(Arc::from(timezone.name())),
        ),
        other => anyhow::bail!("unsupported ClickHouse source type {other}"),
    })
}
