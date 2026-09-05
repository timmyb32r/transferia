use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};

use arrow::array::{Array as _, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_arrow::{ClientBuilder, Type};
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;

use super::config::{ClickHouseSnapshotReader, ClickHouseSourceConfig, TableConfig};
use super::parquet::{ParquetReadSettings, ParquetTransport};
use super::reader::ClickHouseSource;
use super::reader::SnapshotStream;
use crate::connectors::clickhouse::sink::client::probe_network;
use crate::connectors::clickhouse::sink::client::{quote_identifier, ReconnectingClient};
use crate::connectors::clickhouse::sink::identifier::validate_identifier;
use crate::connectors::clickhouse::sink::table::quote_string_literal;
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{
    ConnectionCheckResult, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
};

#[derive(Clone)]
pub(super) struct DiscoveredTable {
    pub config: TableConfig,
    pub schema: DatasetSchema,
    pub physical_system_columns: SystemColumns,
}

pub(super) const SYSTEM_COLUMN_KINDS: [SystemColumnKind; 4] = [
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

pub struct ClickHouseSourceConnector {
    config: ClickHouseSourceConfig,
    client: Arc<ReconnectingClient>,
    parquet: Option<ParquetTransport>,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl ClickHouseSourceConnector {
    pub fn from_config(
        config: ClickHouseSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let batch_rows = i64::try_from(config.batch_rows)?;
        let (native_max_threads, native_compression, parquet_settings) =
            match &config.snapshot_reader {
                ClickHouseSnapshotReader::Parquet {
                    compression,
                    max_threads,
                    row_group_rows,
                    decode_threads,
                    max_response_bytes,
                } => (
                    1,
                    crate::connectors::clickhouse::sink::ClickHouseCompression::Lz4,
                    Some(ParquetReadSettings {
                        compression: *compression,
                        max_threads: *max_threads,
                        row_group_rows: *row_group_rows,
                        decode_threads: *decode_threads,
                        max_response_bytes: *max_response_bytes,
                    }),
                ),
                ClickHouseSnapshotReader::Native {
                    max_threads,
                    compression,
                } => (*max_threads, *compression, None),
            };
        let native_max_threads = i64::try_from(native_max_threads)?;
        let builders = config
            .hosts
            .iter()
            .map(|host| {
                let builder = ClientBuilder::new()
                    .with_destination(crate::connectors::address::host_port(host, config.port))
                    .with_database("default")
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_compression(native_compression.into())
                    .with_setting("max_block_size", batch_rows)
                    // ClickHouse otherwise targets roughly 1 MiB result blocks. That
                    // produces many small Arrow batches for wide rows and forces the
                    // client to spend time on framing instead of useful transfer work.
                    .with_setting("preferred_block_size_bytes", 0_i64)
                    .with_setting("max_threads", native_max_threads)
                    .with_tls(!config.trusted_plaintext);
                if let Some(path) = &config.tls_ca_file {
                    builder.with_cafile(path)
                } else {
                    builder
                }
            })
            .collect();
        let parquet = parquet_settings
            .map(|settings| ParquetTransport::new(&config, settings))
            .transpose()?;
        let client = Arc::new(ReconnectingClient::from_connections(
            builders,
            config.connect_timeout(),
            config.request_timeout(),
        ));
        Ok(Self {
            config,
            client,
            parquet,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    pub async fn check_connection(
        config: ClickHouseSourceConfig,
        _metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<ConnectionCheckResult> {
        config.validate_connection()?;
        if config.username.is_empty() {
            probe_network(&config.hosts, config.port, config.connect_timeout()).await?;
            return Ok(ConnectionCheckResult::network_reachable());
        }
        let builders = config
            .hosts
            .iter()
            .map(|host| {
                let builder = ClientBuilder::new()
                    .with_destination(crate::connectors::address::host_port(host, config.port))
                    .with_database("default")
                    .with_username(config.username.as_str())
                    .with_password(config.password.as_str())
                    .with_tls(!config.trusted_plaintext);
                if let Some(path) = &config.tls_ca_file {
                    builder.with_cafile(path)
                } else {
                    builder
                }
            })
            .collect();
        let client = ReconnectingClient::from_connections(
            builders,
            config.connect_timeout(),
            config.request_timeout(),
        );
        client
            .ensure_connected()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
        Ok(ConnectionCheckResult::default())
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

impl SourceConnector for ClickHouseSourceConnector {
    fn compatibility(
        &self,
        _delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::ClickHouseSource(SourceDescriptor {
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
                delivery_type: _,
            } = context;
            let tables = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("ClickHouse discovery cancelled"), tables = self.discovered_tables() => tables? };
            let discovered_system_columns = SYSTEM_COLUMN_KINDS
                .iter()
                .copied()
                .map(Into::into)
                .collect::<Vec<_>>();
            let datasets = tables
                .iter()
                .map(|table| {
                    let mut incoming = table.schema.clone();
                    if table.physical_system_columns.is_empty() {
                        incoming
                            .columns
                            .extend(SYSTEM_COLUMN_KINDS.iter().map(|kind| {
                                SchemaColumn::new(
                                    kind.default_name().to_owned(),
                                    kind.data_type(),
                                    false,
                                )
                            }));
                    }
                    let stored_schema = if request.keep_system_columns {
                        incoming.clone()
                    } else if table.physical_system_columns.is_empty() {
                        table.schema.clone()
                    } else {
                        without_system_columns(&table.schema, &table.physical_system_columns)
                    };
                    DiscoveredDataset {
                        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                        role: DatasetRole::Main,
                        name: Arc::from(table.config.name.as_str()),
                        incoming_schema: incoming.clone(),
                        stored_schema,
                        system_columns: discovered_system_columns.clone(),
                    }
                })
                .collect();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("clickhouse"),
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
            let SourceBuildContext {
                partition_id,
                cancellation,
                delivery_type: _,
                phase: _,
                ..
            } = context;
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
            let stream: SnapshotStream = if let Some(parquet) = &self.parquet {
                parquet.snapshot_stream(
                    table.clone(),
                    self.config.batch_rows,
                    Arc::clone(&counters),
                    cancellation.clone(),
                )
            } else {
                let query = snapshot_query(&table.config);
                let stream = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("ClickHouse read cancelled"), stream = self.client.query_stream(&query) => stream.map_err(|error| anyhow::anyhow!("ClickHouse snapshot query failed: {error}"))? };
                Box::pin(stream.map(|result| result.map_err(anyhow::Error::from)))
            };
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

fn snapshot_query(table: &TableConfig) -> String {
    format!(
        "SELECT * FROM {}.{}",
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
            let declared_type = type_values.value(row);
            let clickhouse_type = Type::from_str(declared_type)?;
            let data_type = source_arrow_type(&clickhouse_type, declared_type)?;
            let data_type = match system_column_kind(name) {
                Some(kind) => {
                    anyhow::ensure!(
                        !clickhouse_type.is_nullable(),
                        "ClickHouse source system column '{}.{}.{name}' must be non-nullable",
                        table.database,
                        table.name,
                    );
                    let expected = kind.data_type();
                    let compatible = data_type == expected
                        || (kind == SystemColumnKind::Topic && data_type == DataType::Binary);
                    anyhow::ensure!(
                        compatible,
                        "ClickHouse source system column '{}.{}.{name}' has Arrow type {data_type:?}, expected {expected:?}",
                        table.database,
                        table.name,
                    );
                    expected
                }
                None => data_type,
            };
            columns.push(SchemaColumn::new(
                name.to_owned(),
                data_type,
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
    let mut schema = DatasetSchema::new(columns);
    apply_declared_primary_key(&mut schema, &table)?;
    let physical_system_columns = classify_system_columns(&schema)?;
    Ok(DiscoveredTable {
        config: table,
        schema,
        physical_system_columns,
    })
}

pub(super) fn apply_declared_primary_key(
    schema: &mut DatasetSchema,
    table: &TableConfig,
) -> anyhow::Result<()> {
    for key in &table.primary_key {
        let column = schema
            .columns
            .iter_mut()
            .find(|column| column.name == *key)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "ClickHouse source primary-key column '{}.{}.{}' does not exist",
                    table.database,
                    table.name,
                    key,
                )
            })?;
        anyhow::ensure!(
            !column.nullable,
            "ClickHouse source primary-key column '{}.{}.{}' must be non-nullable",
            table.database,
            table.name,
            key,
        );
        anyhow::ensure!(
            system_column_kind(&column.name).is_none(),
            "ClickHouse source system column '{}.{}.{}' cannot be a primary key",
            table.database,
            table.name,
            key,
        );
        column.primary_key = true;
    }
    Ok(())
}

fn system_column_kind(name: &str) -> Option<SystemColumnKind> {
    SYSTEM_COLUMN_KINDS
        .into_iter()
        .find(|kind| kind.default_name() == name)
}

pub(super) fn classify_system_columns(schema: &DatasetSchema) -> anyhow::Result<SystemColumns> {
    let columns = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            system_column_kind(&column.name).map(|kind| (index, column, kind))
        })
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Ok(SystemColumns::default());
    }
    anyhow::ensure!(
        columns.len() == SYSTEM_COLUMN_KINDS.len(),
        "ClickHouse source table contains only {}/{} reserved system columns; either all or none must be present",
        columns.len(),
        SYSTEM_COLUMN_KINDS.len(),
    );
    let mut result = Vec::with_capacity(columns.len());
    for kind in SYSTEM_COLUMN_KINDS {
        let (index, column, _) = columns
            .iter()
            .find(|(_, _, candidate)| *candidate == kind)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing ClickHouse source system column '{}'",
                    kind.default_name()
                )
            })?;
        anyhow::ensure!(
            column.data_type == kind.data_type() && !column.nullable,
            "ClickHouse source system column '{}' must have Arrow type {:?} and be non-nullable",
            column.name,
            kind.data_type(),
        );
        result.push(SystemColumn {
            kind,
            index: *index,
            name: Arc::from(column.name.as_str()),
        });
    }
    Ok(SystemColumns::new(result))
}

fn without_system_columns(schema: &DatasetSchema, system_columns: &SystemColumns) -> DatasetSchema {
    let indexes = system_columns
        .iter()
        .map(|column| column.index)
        .collect::<HashSet<_>>();
    DatasetSchema::new(
        schema
            .columns
            .iter()
            .enumerate()
            .filter(|(index, _)| !indexes.contains(index))
            .map(|(_, column)| column.clone())
            .collect(),
    )
}

pub(super) fn source_arrow_type(
    clickhouse_type: &Type,
    declared_type: &str,
) -> anyhow::Result<DataType> {
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
        Type::Date | Type::Date32 => DataType::Date32,
        Type::DateTime(timezone) => DataType::Timestamp(
            TimeUnit::Second,
            declared_timestamp_timezone(declared_type, timezone.name()),
        ),
        Type::DateTime64(precision, timezone) => DataType::Timestamp(
            match precision {
                0 => TimeUnit::Second,
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                7..=9 => TimeUnit::Nanosecond,
                _ => anyhow::bail!("unsupported ClickHouse DateTime64 precision {precision}"),
            },
            declared_timestamp_timezone(declared_type, timezone.name()),
        ),
        other => anyhow::bail!("unsupported ClickHouse source type {other}"),
    })
}

fn declared_timestamp_timezone(declared_type: &str, timezone: &str) -> Option<Arc<str>> {
    let mut declaration = declared_type.trim();
    while let Some(inner) = declaration
        .strip_prefix("Nullable(")
        .and_then(|value| value.strip_suffix(')'))
    {
        declaration = inner.trim();
    }
    let explicit = if declaration == "DateTime" {
        false
    } else if declaration.starts_with("DateTime(") {
        true
    } else if let Some(arguments) = declaration
        .strip_prefix("DateTime64(")
        .and_then(|value| value.strip_suffix(')'))
    {
        arguments.contains(',')
    } else {
        false
    };
    explicit.then(|| Arc::from(timezone))
}
