use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

use super::config::{MySqlSourceConfig, TableConfig};
use super::reader::MySqlSource;
use crate::connectors::mysql::common::{connect, quote_identifier, validate_identifier};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MySqlColumnKind {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Binary,
    Utf8,
    Json,
}

impl MySqlColumnKind {
    pub(super) const fn arrow_type(self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::UInt8 => DataType::UInt8,
            Self::Int16 => DataType::Int16,
            Self::UInt16 => DataType::UInt16,
            Self::Int32 => DataType::Int32,
            Self::UInt32 => DataType::UInt32,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Binary => DataType::Binary,
            Self::Utf8 | Self::Json => DataType::Utf8,
        }
    }
}

#[derive(Clone)]
pub(super) struct ColumnPlan {
    pub name: String,
    pub kind: MySqlColumnKind,
    pub nullable: bool,
    pub primary_key: bool,
    pub max_length: Option<usize>,
    pub expression: String,
}

#[derive(Clone)]
struct DiscoveredTable {
    config: TableConfig,
    schema: DatasetSchema,
    columns: Vec<ColumnPlan>,
}

pub struct MySqlSourceConnector {
    config: MySqlSourceConfig,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl MySqlSourceConnector {
    pub fn from_config(
        config: MySqlSourceConfig,
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
                let mut connection = connect(&self.config.connection).await?;
                let mut tables = Vec::with_capacity(self.config.tables.len());
                for table in &self.config.tables {
                    tables.push(
                        discover_table(
                            &mut connection,
                            &self.config.connection.database,
                            table.clone(),
                        )
                        .await?,
                    );
                }
                connection.disconnect().await?;
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

impl SourceConnector for MySqlSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::MySql(SourceDescriptor {
            behavior: SourceBehavior::FiniteAppendOnlyRows,
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
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("MySQL discovery cancelled"),
                tables = self.discovered_tables() => tables?,
            };
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
                source_name: Arc::from("mysql"),
                source_topology: SourceTopology::StaticPartitions(
                    (0..tables.len())
                        .map(i64::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
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
            let table = tables
                .get(usize::try_from(partition_id)?)
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL source partition {partition_id} does not exist")
                })?
                .clone();
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            let connection = connect(&self.config.connection).await?;
            Ok(Box::new(
                MySqlSource::new(
                    connection,
                    self.config.connection.database.clone(),
                    table.config,
                    table.schema,
                    table.columns,
                    self.config.batch_rows,
                    self.config.read_protocol,
                    counters,
                )
                .await?,
            ) as Box<dyn Source>)
        })
    }

    fn build_speedtest_source(
        &self,
        context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        self.build_source(context)
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

async fn discover_table(
    connection: &mut Conn,
    database: &str,
    table: TableConfig,
) -> anyhow::Result<DiscoveredTable> {
    let rows: Vec<Row> = connection
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (database, table.name.as_str()),
        )
        .await?;
    anyhow::ensure!(
        !rows.is_empty(),
        "MySQL table '{}.{}' does not exist or has no columns",
        database,
        table.name
    );
    let columns = rows
        .iter()
        .map(column_plan)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let schema = DatasetSchema::new(
        columns
            .iter()
            .map(|column| {
                let mut schema = SchemaColumn::new(
                    column.name.clone(),
                    column.kind.arrow_type(),
                    column.nullable,
                )
                .with_constraints(column.primary_key, false, column.max_length);
                if column.kind == MySqlColumnKind::Json {
                    schema = schema.with_arrow_extension(ARROW_JSON_EXTENSION_NAME);
                }
                schema
            })
            .collect(),
    );
    Ok(DiscoveredTable {
        config: table,
        schema,
        columns,
    })
}

fn column_plan(row: &Row) -> anyhow::Result<ColumnPlan> {
    let name = required::<String>(row, "COLUMN_NAME")?;
    validate_identifier("column", &name)?;
    let data_type = required::<String>(row, "DATA_TYPE")?.to_ascii_lowercase();
    let column_type = required::<String>(row, "COLUMN_TYPE")?.to_ascii_lowercase();
    let nullable = required::<String>(row, "IS_NULLABLE")? == "YES";
    let primary_key = required::<String>(row, "COLUMN_KEY")? == "PRI";
    let max_length = row
        .get::<Option<u64>, _>("CHARACTER_MAXIMUM_LENGTH")
        .flatten()
        .map(usize::try_from)
        .transpose()?;
    let unsigned = column_type
        .split_ascii_whitespace()
        .any(|token| token == "unsigned");
    let kind = match data_type.as_str() {
        "tinyint" => if unsigned { MySqlColumnKind::UInt8 } else { MySqlColumnKind::Int8 },
        "smallint" => if unsigned { MySqlColumnKind::UInt16 } else { MySqlColumnKind::Int16 },
        "mediumint" | "int" | "integer" => if unsigned { MySqlColumnKind::UInt32 } else { MySqlColumnKind::Int32 },
        "bigint" => if unsigned { MySqlColumnKind::UInt64 } else { MySqlColumnKind::Int64 },
        "float" => MySqlColumnKind::Float32,
        "double" | "real" => MySqlColumnKind::Float64,
        "bit" | "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" | "geometry" | "point" | "linestring" | "polygon"
        | "multipoint" | "multilinestring" | "multipolygon" | "geometrycollection"
        | "vector" => MySqlColumnKind::Binary,
        "json" => MySqlColumnKind::Json,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
        | "enum" | "set" | "inet4" | "inet6" | "uuid" => MySqlColumnKind::Utf8,
        "decimal" | "numeric" | "date" | "time" | "datetime" | "timestamp" | "year" => {
            MySqlColumnKind::Utf8
        }
        _ => anyhow::bail!(
            "unsupported MySQL/MariaDB column type '{data_type}' ({column_type}) for column '{name}'"
        ),
    };
    let quoted = quote_identifier(&name);
    let canonical_text = matches!(
        data_type.as_str(),
        "decimal" | "numeric" | "date" | "time" | "datetime" | "timestamp" | "year"
    );
    let expression = if canonical_text {
        format!("CAST({quoted} AS CHAR) AS {quoted}")
    } else {
        quoted
    };
    Ok(ColumnPlan {
        name,
        kind,
        nullable,
        primary_key,
        max_length,
        expression,
    })
}

fn required<T>(row: &Row, name: &str) -> anyhow::Result<T>
where
    T: mysql_async::prelude::FromValue,
{
    row.get(name)
        .ok_or_else(|| anyhow::anyhow!("MySQL metadata omitted required column '{name}'"))
}
