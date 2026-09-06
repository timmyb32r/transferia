use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{Array as _, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use clickhouse_arrow::ClientBuilder;
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;

use super::config::{ClickHouseSnapshotReader, ClickHouseSourceConfig, TableConfig, UnsupportedTypePolicy};
use super::parquet::{ParquetReadSettings, ParquetTransport};
use super::reader::ClickHouseSource;
use super::reader::SnapshotStream;
use crate::connectors::clickhouse::sink::client::probe_network;
use crate::connectors::clickhouse::sink::client::{quote_identifier, ReconnectingClient};
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
                    .with_arrow_options(clickhouse_arrow::ArrowOptions::strict().with_source_type_metadata(true))
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
            .map_err(|error| super::super::sink::connection_check_error(&error))?;
        Ok(ConnectionCheckResult {
            // Cache the full readable catalog in the editor. Its visibility
            // filter must not require another authenticated connection check.
            tables: Some(list_tables(&client, false).await?),
            ..Default::default()
        })
    }

    async fn discovered_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async {
                self.client
                    .ensure_connected()
                    .await
                    .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
                let catalog = list_tables(&self.client, self.config.hide_system_tables).await?;
                let selected = self
                    .config
                    .tables
                    .compile()?
                    .resolve(&catalog)?
                    .selected_tables()?;
                let mut tables = Vec::with_capacity(selected.len());
                for table in selected {
                    tables.push(
                        discover_table(
                            &self.client,
                            TableConfig {
                                database: table.namespace,
                                name: table.name,
                            },
                            self.config.unsupported_types,
                        )
                        .await?,
                    );
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
                        namespace: Some(Arc::from(table.config.database.as_str())),
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
                let query = snapshot_query(&table);
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
            )?) as Box<dyn Source>)
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

pub(super) fn snapshot_query(table: &DiscoveredTable) -> String {
    // Qualified input references avoid ClickHouse's global SELECT-alias substitution.
    let reference = |column: &SchemaColumn| format!("source.{}", quote_identifier(&column.name));
    let projection = table.schema.columns.iter().map(|column| {
        let value = if super::types::is_string_conversion(column) {
            format!("CAST(toString({}) AS {})", reference(column),
                if column.nullable { "Nullable(String)" } else { "String" })
        } else { reference(column) };
        format!("{value} AS {}", quote_identifier(&column.name))
    }).collect::<Vec<_>>().join(", ");
    // Constant type guards also protect Parquet and explicit string conversions, whose
    // output type alone cannot reveal a change to the original ClickHouse declaration.
    let guards = table.schema.columns.iter().filter_map(|column| {
        super::types::source_declaration(column).map(|declaration| format!(
            "throwIf(toTypeName({}) != {}, {}) = 0", reference(column),
            quote_string_literal(&declaration),
            quote_string_literal(&format!("ClickHouse source schema drifted at {}.{} column {} (expected {})", table.config.database, table.config.name, column.name, declaration)),
        ))
    }).collect::<Vec<_>>();
    let condition = if guards.is_empty() { String::new() } else { format!(" WHERE {}", guards.join(" AND ")) };
    format!("SELECT {projection} FROM {}.{} AS source{condition}",
        quote_identifier(&table.config.database), quote_identifier(&table.config.name))
}

async fn list_tables(
    client: &ReconnectingClient,
    hide_system_tables: bool,
) -> anyhow::Result<Vec<transferia_registry::TableIdentity>> {
    let batches = transferia_connector_support::external_request::observe_external_request(
        "clickhouse",
        "list_tables",
        client.query_all(
            "SELECT database, name FROM system.tables \
            WHERE is_temporary = 0 \
            ORDER BY database, name",
        ),
    )
    .await?;
    let mut tables = Vec::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() == 2,
            "ClickHouse table catalog must have two columns"
        );
        let namespaces = cast(batch.column(0), &DataType::Utf8)?;
        let names = cast(batch.column(1), &DataType::Utf8)?;
        let namespaces = namespaces
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse catalog database is not a string"))?;
        let names = names
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse catalog table is not a string"))?;
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !namespaces.is_null(row) && !names.is_null(row),
                "ClickHouse table catalog contains NULL names"
            );
            if hide_system_tables && super::config::is_system_database(namespaces.value(row)) {
                continue;
            }
            tables.push(transferia_registry::TableIdentity {
                namespace: namespaces.value(row).to_owned(),
                name: names.value(row).to_owned(),
            });
        }
    }
    let mut readable = Vec::with_capacity(tables.len());
    for table in tables {
        let query = format!(
            "CHECK GRANT SELECT ON {}.{}",
            quote_identifier(&table.namespace),
            quote_identifier(&table.name)
        );
        let grants = transferia_connector_support::external_request::observe_external_request(
            "clickhouse",
            "check_table_read_access",
            client.query_all(&query),
        )
        .await?;
        anyhow::ensure!(
            grants
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>()
                == 1,
            "ClickHouse CHECK GRANT must return exactly one result"
        );
        let grant = grants
            .iter()
            .find(|batch| batch.num_rows() == 1)
            .ok_or_else(|| anyhow::anyhow!("ClickHouse CHECK GRANT returned no result"))?;
        anyhow::ensure!(
            grant.num_columns() == 1,
            "ClickHouse CHECK GRANT must return one column"
        );
        let values = cast(grant.column(0), &DataType::UInt8)?;
        let values = values
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse CHECK GRANT returned an invalid type"))?;
        anyhow::ensure!(
            !values.is_null(0) && values.value(0) <= 1,
            "ClickHouse CHECK GRANT returned an invalid decision"
        );
        if values.value(0) == 1 {
            readable.push(table);
        }
    }
    Ok(readable)
}

pub(super) async fn discover_table(
    client: &ReconnectingClient,
    table: TableConfig,
    unsupported_types: UnsupportedTypePolicy,
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
            // Catalog names are already ClickHouse identifiers. Preserve their
            // exact text and quote every SQL reference; sink naming limits do
            // not apply to reading existing columns (including flattened Nested).
            anyhow::ensure!(
                names.insert(name.to_owned()),
                "ClickHouse table '{}.{}' contains duplicate column '{name}'",
                table.database,
                table.name
            );
            validate_source_column_kind(&table, name, default_values.value(row))?;
            let declared_type = type_values.value(row);
            let mut column = source_column_type(&table, name, declared_type, unsupported_types)?;
            if super::types::is_string_conversion(&column) {
                tracing::info!(database = %table.database, table = %table.name, column = name,
                    conversion = "to_string", "ClickHouse source column uses explicit text conversion");
            }
            let data_type = column.data_type.clone();
            let data_type = match system_column_kind(name) {
                Some(kind) => {
                    anyhow::ensure!(
                        !column.nullable && !super::types::is_string_conversion(&column),
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
            column.data_type = data_type;
            columns.push(column);
        }
    }
    anyhow::ensure!(
        !columns.is_empty(),
        "ClickHouse source table '{}.{}' does not exist or has no columns",
        table.database,
        table.name
    );
    let mut schema = DatasetSchema::new(columns);
    let key_query = format!(
        "SELECT primary_key, sorting_key FROM system.tables WHERE database = {} AND name = {}",
        quote_string_literal(&table.database),
        quote_string_literal(&table.name),
    );
    let key_batches = client
        .query_all(&key_query)
        .await
        .map_err(|error| anyhow::anyhow!("cannot inspect ClickHouse source table key: {error}"))?;
    let mut found_key = false;
    for batch in key_batches {
        anyhow::ensure!(
            batch.num_columns() == 2,
            "ClickHouse key metadata must contain two columns"
        );
        let primary = cast(batch.column(0), &DataType::Utf8)?;
        let sorting = cast(batch.column(1), &DataType::Utf8)?;
        let primary = primary
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse primary key metadata is not a string"))?;
        let sorting = sorting
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse sorting key metadata is not a string"))?;
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !found_key,
                "ClickHouse returned duplicate table key metadata"
            );
            anyhow::ensure!(
                !primary.is_null(row) && !sorting.is_null(row),
                "ClickHouse key metadata contains NULL"
            );
            apply_discovered_primary_key(
                &mut schema,
                &table,
                primary.value(row),
                sorting.value(row),
            )?;
            found_key = true;
        }
    }
    anyhow::ensure!(
        found_key,
        "ClickHouse source table disappeared during key discovery"
    );
    let physical_system_columns = classify_system_columns(&schema)?;
    let discovered = DiscoveredTable {
        config: table,
        schema,
        physical_system_columns,
    };
    validate_projection(client, &discovered).await?;
    Ok(discovered)
}

pub(super) fn validate_source_column_kind(
    table: &TableConfig,
    column: &str,
    kind: &str,
) -> anyhow::Result<()> {
    // Snapshot queries name columns explicitly, so readable computed columns
    // must not be rejected merely because SELECT * would omit some of them.
    match kind {
        "" | "DEFAULT" | "MATERIALIZED" | "ALIAS" => Ok(()),
        "EPHEMERAL" => anyhow::bail!(
            "ClickHouse source column {}.{}.{} is EPHEMERAL (input-only) and cannot be read by SELECT",
            quote_identifier(&table.database),
            quote_identifier(&table.name),
            quote_identifier(column),
        ),
        _ => anyhow::bail!(
            "ClickHouse source column {}.{}.{} has unsupported column kind {kind:?}",
            quote_identifier(&table.database),
            quote_identifier(&table.name),
            quote_identifier(column),
        ),
    }
}

async fn validate_projection(client: &ReconnectingClient, table: &DiscoveredTable) -> anyhow::Result<()> {
    let query = format!("DESCRIBE ({})", snapshot_query(table));
    let batches = transferia_connector_support::external_request::observe_external_request(
        "clickhouse", "describe_snapshot", client.query_all(&query),
    ).await.map_err(|error| anyhow::anyhow!(
        "ClickHouse source table {}.{}: snapshot projection validation failed (including explicit to_string conversions): {error}",
        quote_identifier(&table.config.database), quote_identifier(&table.config.name),
    ))?;
    let mut index = 0;
    for batch in batches {
        anyhow::ensure!(batch.num_columns() >= 2, "ClickHouse DESCRIBE returned no column types");
        let names = cast(batch.column(0), &DataType::Utf8)?;
        let types = cast(batch.column(1), &DataType::Utf8)?;
        let names = names.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("DESCRIBE names must be strings"))?;
        let types = types.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("DESCRIBE types must be strings"))?;
        for row in 0..batch.num_rows() {
            let expected = table.schema.columns.get(index).ok_or_else(|| anyhow::anyhow!("ClickHouse snapshot projection gained a column"))?;
            anyhow::ensure!(!names.is_null(row) && !types.is_null(row) && names.value(row) == expected.name,
                "ClickHouse snapshot projection column {} changed", expected.name);
            let field = arrow::datatypes::Field::new(&expected.name, expected.data_type.clone(), expected.nullable)
                .with_metadata(HashMap::from([("clickhouse.type".to_owned(), types.value(row).to_owned())]));
            super::types::validate_wire_type(&field, expected)?;
            index += 1;
        }
    }
    anyhow::ensure!(index == table.schema.columns.len(), "ClickHouse snapshot projection lost columns");
    Ok(())
}

pub(super) fn apply_discovered_primary_key(
    schema: &mut DatasetSchema,
    table: &TableConfig,
    primary_key: &str,
    sorting_key: &str,
) -> anyhow::Result<()> {
    let selected = if primary_key == sorting_key {
        sorting_key
    } else {
        primary_key
    };
    if matches!(selected.trim(), "" | "tuple()" | "()") {
        return Ok(());
    }
    let columns = super::identifiers::key_columns(selected).map_err(|error| anyhow::anyhow!(
        "ClickHouse source key for '{}.{}' must contain plain column names; expressions are not supported: {error:#}",
        table.database, table.name,
    ))?;
    let mut keys = HashSet::new();
    for key in columns {
        anyhow::ensure!(
            keys.insert(key.clone()),
            "ClickHouse source key repeats column '{key}'"
        );
        let column = schema
            .columns
            .iter_mut()
            .find(|column| column.name == key)
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

pub(super) fn source_column_type(
    table: &TableConfig,
    column: &str,
    declared_type: &str,
    policy: UnsupportedTypePolicy,
) -> anyhow::Result<SchemaColumn> {
    super::types::source_column(column, declared_type, policy).map_err(|error| anyhow::anyhow!(
        "ClickHouse source table {}.{}, column {}, type {}: {error:#}",
        quote_identifier(&table.database), quote_identifier(&table.name),
        quote_identifier(column), declared_type,
    ))
}
