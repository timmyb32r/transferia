use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, Int64Array, StringArray,
    TimestampMicrosecondBuilder, TimestampSecondArray, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use iceberg::table::Table;
use iceberg::spec::FormatVersion;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation, TableIdent};
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use transferia_connector_support::external_request::elapsed_millis;
use transferia_core::data::changelog::{
    project_sink_batch, ChangelogAction, ChangelogBatch, ProjectedSinkBatch,
};
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    validate_batch_against_discovery, validate_stored_projection, ArrowTypeFamily,
    DeliveryDiscovery, DiscoveredDataset, NameSyntax, SinkLimits, SinkLimitsDescription, TextLimit,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkBatch, SinkEvent, SinkIo};
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::durable::{CompareExchangeResult, DurableContext};
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
    SnapshotDatasetRowCount, SnapshotRowCountStrategy,
};

use super::catalog::{build_catalog, table_ident};
use super::config::{IcebergParquetCompression, IcebergSinkConfig, IcebergTableRef};

pub struct IcebergSinkConnector {
    config: Arc<IcebergSinkConfig>,
    catalog: tokio::sync::OnceCell<Arc<dyn Catalog>>,
}

pub async fn check_connection(config: &IcebergSinkConfig) -> anyhow::Result<()> {
    config.validate()?;
    let catalog = build_catalog(&config.catalog, &config.storage).await?;
    let namespace = iceberg::NamespaceIdent::new(config.namespace.clone());
    catalog.list_tables(&namespace).await?;
    Ok(())
}

impl IcebergSinkConnector {
    pub fn from_config(config: IcebergSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            catalog: tokio::sync::OnceCell::new(),
        })
    }

    async fn catalog(&self) -> anyhow::Result<Arc<dyn Catalog>> {
        self.catalog
            .get_or_try_init(|| build_catalog(&self.config.catalog, &self.config.storage))
            .await
            .map(Arc::clone)
    }
}

impl SinkLimits for IcebergSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "iceberg",
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
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Decimal,
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
            "Iceberg sink requires at least one dataset"
        );
        let has_changelog = discovery.datasets.iter().any(dataset_is_changelog);
        if has_changelog {
            anyhow::ensure!(
                matches!(discovery.source_name.as_ref(), "mysql" | "postgres"),
                "Iceberg replica mode supports exact PostgreSQL and MySQL changelogs, not source '{}'",
                discovery.source_name
            );
        }
        for dataset in &discovery.datasets {
            self.table_for_dataset(&dataset.name)?
                .validate("dataset table")?;
            validate_stored_projection(discovery, dataset)?;
            iceberg_schema(&dataset.stored_schema)?;
            if dataset_is_changelog(dataset) {
                validate_replica_dataset(dataset)?;
            }
        }
        Ok(())
    }

    fn validate_batch(
        &self,
        discovery: &DeliveryDiscovery,
        batch: &transferia_core::sink::SinkBatch,
    ) -> anyhow::Result<()> {
        validate_batch_against_discovery(discovery, batch)?;
        self.table_for_dataset(&batch.table)?;
        validate_timestamp_values(&batch.batch)?;
        Ok(())
    }
}

fn dataset_is_changelog(dataset: &DiscoveredDataset) -> bool {
    dataset
        .system_columns
        .iter()
        .any(|column| column.kind == SystemColumnKind::ChangeOperation)
}

fn validate_replica_dataset(dataset: &DiscoveredDataset) -> anyhow::Result<()> {
    anyhow::ensure!(
        dataset.stored_schema.columns.iter().any(|column| column.primary_key),
        "Iceberg replica dataset '{}' requires a non-empty primary key",
        dataset.name
    );
    for kind in [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::ChangeOperation,
        SystemColumnKind::ChangedColumns,
    ] {
        anyhow::ensure!(
            dataset.system_columns.iter().any(|column| column.kind == kind),
            "Iceberg replica dataset '{}' is missing required {:?} metadata",
            dataset.name,
            kind
        );
    }
    let transaction_columns = dataset
        .incoming_schema
        .columns
        .iter()
        .filter(|column| column.system_role.as_deref() == Some(SYSTEM_ROLE_SOURCE_TRANSACTION_ID))
        .count();
    anyhow::ensure!(
        transaction_columns == 1,
        "Iceberg replica dataset '{}' requires exactly one source transaction identity column",
        dataset.name
    );
    for current in &dataset.stored_schema.columns {
        let old_values = dataset
            .incoming_schema
            .columns
            .iter()
            .filter(|column| column.old_value_of.as_deref() == Some(current.name.as_str()))
            .count();
        anyhow::ensure!(
            old_values == 1,
            "Iceberg replica dataset '{}' requires a complete old image; column '{}' has {old_values} old-value mappings",
            dataset.name,
            current.name
        );
    }
    Ok(())
}

impl IcebergSinkConfig {
    fn table_for_dataset(&self, dataset: &str) -> anyhow::Result<IcebergTableRef> {
        let table = IcebergTableRef {
            namespace: vec![self.namespace.clone()],
            name: dataset.to_owned(),
        };
        table.validate("dataset table")?;
        Ok(table)
    }
}

impl SinkConnector for IcebergSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::IcebergSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String> {
        let schema = DatasetSchema::new(vec![column.clone()]);
        let converted = iceberg_schema(&schema)?;
        Ok(converted.as_struct().fields()[0].field_type.to_string())
    }

    fn snapshot_row_count_strategy(&self) -> Option<SnapshotRowCountStrategy> {
        Some(SnapshotRowCountStrategy::AdditiveBaseline)
    }

    fn snapshot_row_counts<'a>(
        &'a self,
        discovery: &'a DeliveryDiscovery,
    ) -> BoxFuture<'a, anyhow::Result<Vec<SnapshotDatasetRowCount>>> {
        Box::pin(async move {
            let catalog = self.catalog().await?;
            snapshot_iceberg_row_counts(&catalog, &self.config, discovery).await
        })
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let catalog = self.catalog().await?;
            let transfer_id = request.transfer_id;
            let replay_identity = request.replay_identity;
            for dataset in request.datasets {
                let replica_owner = if dataset.changelog {
                    let replay_identity = replay_identity.as_deref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "Iceberg replica dataset '{}' requires a stable source replay identity",
                            dataset.table
                        )
                    })?;
                    anyhow::ensure!(
                        !transfer_id.is_empty() && !replay_identity.is_empty(),
                        "Iceberg replica ownership identities cannot be empty"
                    );
                    Some((transfer_id.as_ref(), replay_identity))
                } else {
                    None
                };
                let table_ref = self.config.table_for_dataset(&dataset.table)?;
                let ident = table_ident(&table_ref)?;
                let exists = catalog
                    .list_tables(ident.namespace())
                    .await?
                    .contains(&ident);
                let table = if exists {
                    catalog.load_table(&ident).await?
                } else {
                    anyhow::ensure!(
                        self.config.create_if_missing,
                        "Iceberg table '{ident}' does not exist and create_if_missing is false"
                    );
                    let creation = TableCreation::builder()
                        .name(table_ref.name.clone())
                        .schema(iceberg_schema(&dataset.schema)?)
                        .properties(replica_owner.into_iter().flat_map(|(delivery, replay)| {
                            [
                                (
                                    ICEBERG_REPLICA_DELIVERY_PROPERTY.to_owned(),
                                    delivery.to_owned(),
                                ),
                                (
                                    ICEBERG_REPLICA_REPLAY_PROPERTY.to_owned(),
                                    replay.to_owned(),
                                ),
                            ]
                        }))
                        .build();
                    catalog.create_table(ident.namespace(), creation).await?
                };
                ensure_table_schema(&table, &dataset.schema)?;
                if let Some((delivery, replay)) = replica_owner {
                    ensure_replica_table(&table, &dataset.schema, delivery, replay)?;
                }
            }
            Ok(())
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let catalog = self.catalog().await?;
            Ok(Box::new(IcebergSink {
                config: Arc::clone(&self.config),
                catalog,
                counters: context.counters,
                discovery: context.discovery,
                keep_system_columns: context.keep_system_columns,
                partition_id: context.partition_id,
                durable: context.durable,
                idempotent_snapshot_replay: context.finite_source,
                replay_identity: context.replay_identity,
            }) as Box<dyn Sink>)
        })
    }

    fn isolate_speedtest(
        self: Arc<Self>,
        _discovery: Arc<DeliveryDiscovery>,
        _isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async move {
            drop(self);
            anyhow::bail!(
                "Iceberg sink speedtests are disabled because the supported storage APIs cannot prove connector-exclusive scratch ownership and complete physical cleanup before external I/O"
            )
        })
    }
}

pub(super) trait IcebergRowCountCatalog: Sync {
    fn row_count<'a>(
        &'a self,
        table: &'a TableIdent,
    ) -> BoxFuture<'a, anyhow::Result<Option<u64>>>;
}

impl IcebergRowCountCatalog for Arc<dyn Catalog> {
    fn row_count<'a>(
        &'a self,
        ident: &'a TableIdent,
    ) -> BoxFuture<'a, anyhow::Result<Option<u64>>> {
        Box::pin(async move {
            if !self.table_exists(ident).await? {
                return Ok(None);
            }
            let table = self.load_table(ident).await?;
            Ok(Some(iceberg_table_row_count(&table)?))
        })
    }
}

pub(super) async fn snapshot_iceberg_row_counts(
    catalog: &impl IcebergRowCountCatalog,
    config: &IcebergSinkConfig,
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<Vec<SnapshotDatasetRowCount>> {
    let mut counts = Vec::with_capacity(discovery.datasets.len());
    for dataset in &discovery.datasets {
        let table_ref = config.table_for_dataset(&dataset.name)?;
        let ident = table_ident(&table_ref)?;
        let target: Arc<str> = Arc::from(ident.to_string());
        match catalog.row_count(&ident).await? {
            Some(rows) => counts.push(SnapshotDatasetRowCount {
                role: dataset.role,
                table: Arc::clone(&dataset.name),
                target,
                exists: true,
                rows,
            }),
            None => counts.push(SnapshotDatasetRowCount {
                role: dataset.role,
                table: Arc::clone(&dataset.name),
                target,
                exists: false,
                rows: 0,
            }),
        }
    }
    Ok(counts)
}

fn iceberg_table_row_count(table: &Table) -> anyhow::Result<u64> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(0);
    };
    exact_iceberg_total_records(
        snapshot.snapshot_id(),
        snapshot
            .summary()
            .additional_properties
            .get("total-records")
            .map(String::as_str),
    )
}

pub(super) fn exact_iceberg_total_records(
    snapshot_id: i64,
    value: Option<&str>,
) -> anyhow::Result<u64> {
    let value = value.ok_or_else(|| {
        anyhow::anyhow!(
            "Iceberg current snapshot {snapshot_id} has no exact total-records summary"
        )
    })?;
    value.parse::<u64>().map_err(|_| {
        anyhow::anyhow!(
            "Iceberg current snapshot {snapshot_id} has invalid total-records summary"
        )
    })
}

struct IcebergSink {
    config: Arc<IcebergSinkConfig>,
    catalog: Arc<dyn Catalog>,
    counters: Arc<transferia_delivery_contracts::metrics::SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    keep_system_columns: bool,
    partition_id: i64,
    durable: DurableContext,
    idempotent_snapshot_replay: bool,
    replay_identity: Option<Arc<str>>,
}

enum DatasetWrite {
    Append {
        batches: Vec<RecordBatch>,
        delivery_ids: Vec<u64>,
    },
    Replica {
        changelog: ChangelogBatch,
        identity: StableCommitSource,
    },
}

impl DatasetWrite {
    fn rows(&self) -> usize {
        match self {
            Self::Append { batches, .. } => batches.iter().map(RecordBatch::num_rows).sum(),
            Self::Replica { changelog, .. } => changelog.rows().num_rows(),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Append { batches, .. } => batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum(),
            Self::Replica { changelog, .. } => changelog.rows().get_array_memory_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StableCommitSource {
    FiniteDelivery { delivery_ids: Vec<u64> },
    Replica {
        transaction: StableTransactionIdentity,
        coordinates: Vec<StableCoordinate>,
        operations: Vec<StableOperationRun>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum StableTransactionIdentity {
    UInt64(u64),
    Binary(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StableCoordinate {
    topic: String,
    partition: i64,
    offset: i64,
    message_index_ranges: Vec<StableIndexRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StableIndexRange {
    first: u64,
    last: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StableOperationRun {
    operation: String,
    count: u64,
}

fn replica_source_identity(
    dataset: &DiscoveredDataset,
    batch: &SinkBatch,
) -> anyhow::Result<StableCommitSource> {
    let topic = system_array::<StringArray>(batch, SystemColumnKind::Topic, "Utf8")?;
    let partition = system_array::<Int64Array>(batch, SystemColumnKind::Partition, "Int64")?;
    let offset = system_array::<Int64Array>(batch, SystemColumnKind::Offset, "Int64")?;
    let message_index =
        system_array::<UInt64Array>(batch, SystemColumnKind::MessageIndex, "UInt64")?;
    let operation =
        system_array::<StringArray>(batch, SystemColumnKind::ChangeOperation, "Utf8")?;
    let transaction_index = dataset
        .incoming_schema
        .columns
        .iter()
        .position(|column| {
            column.system_role.as_deref() == Some(SYSTEM_ROLE_SOURCE_TRANSACTION_ID)
        })
        .ok_or_else(|| anyhow::anyhow!("Iceberg replica source transaction identity is absent"))?;
    let transaction = stable_transaction_identity(batch, transaction_index)?;
    let mut grouped = BTreeMap::<(String, i64, i64), Vec<u64>>::new();
    let mut operations = Vec::<StableOperationRun>::new();
    for row in 0..batch.rows() {
        anyhow::ensure!(
            !topic.is_null(row)
                && !partition.is_null(row)
                && !offset.is_null(row)
                && !message_index.is_null(row)
                && !operation.is_null(row),
            "Iceberg replica source coordinate is null at row {row}"
        );
        grouped
            .entry((
                topic.value(row).to_owned(),
                partition.value(row),
                offset.value(row),
            ))
            .or_default()
            .push(message_index.value(row));
        let code = operation.value(row);
        match operations.last_mut() {
            Some(run) if run.operation == code => {
                run.count = run.count.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("Iceberg replica operation count overflow")
                })?;
            }
            _ => operations.push(StableOperationRun {
                operation: code.to_owned(),
                count: 1,
            }),
        }
    }
    let coordinates = grouped
        .into_iter()
        .map(|((topic, partition, offset), indexes)| {
            Ok(StableCoordinate {
                topic,
                partition,
                offset,
                message_index_ranges: stable_index_ranges(&indexes)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(StableCommitSource::Replica {
        transaction,
        coordinates,
        operations,
    })
}

fn system_array<'a, T: 'static>(
    batch: &'a SinkBatch,
    kind: SystemColumnKind,
    expected: &str,
) -> anyhow::Result<&'a T> {
    let column = batch.system_columns.get(kind).ok_or_else(|| {
        anyhow::anyhow!("Iceberg replica batch is missing required {kind:?} metadata")
    })?;
    batch
        .batch
        .column(column.index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            anyhow::anyhow!("Iceberg replica {kind:?} metadata must be Arrow {expected}")
        })
}

fn stable_transaction_identity(
    batch: &SinkBatch,
    index: usize,
) -> anyhow::Result<StableTransactionIdentity> {
    let array = batch.batch.column(index);
    let first = if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        anyhow::ensure!(!values.is_null(0), "Iceberg source transaction identity is null");
        StableTransactionIdentity::UInt64(values.value(0))
    } else if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        anyhow::ensure!(!values.is_null(0), "Iceberg source transaction identity is null");
        StableTransactionIdentity::Binary(hex_bytes(values.value(0)))
    } else {
        anyhow::bail!(
            "Iceberg source transaction identity must be Arrow UInt64 or Binary, found {:?}",
            array.data_type()
        );
    };
    for row in 1..batch.rows() {
        let current = if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
            anyhow::ensure!(
                !values.is_null(row),
                "Iceberg source transaction identity is null at row {row}"
            );
            StableTransactionIdentity::UInt64(values.value(row))
        } else {
            let values = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("transaction identity type changed"))?;
            anyhow::ensure!(
                !values.is_null(row),
                "Iceberg source transaction identity is null at row {row}"
            );
            StableTransactionIdentity::Binary(hex_bytes(values.value(row)))
        };
        anyhow::ensure!(
            current == first,
            "Iceberg replica batch mixes source transaction identities"
        );
    }
    Ok(first)
}

fn stable_index_ranges(indexes: &[u64]) -> anyhow::Result<Vec<StableIndexRange>> {
    let mut ranges = Vec::<StableIndexRange>::new();
    for &index in indexes {
        if let Some(last) = ranges.last_mut() {
            anyhow::ensure!(
                index > last.last,
                "Iceberg replica message indexes must be strictly increasing"
            );
            if index == last.last.saturating_add(1) {
                last.last = index;
                continue;
            }
        }
        ranges.push(StableIndexRange {
            first: index,
            last: index,
        });
    }
    Ok(ranges)
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

impl IcebergSink {
    async fn write_deliveries(&self, deliveries: &[Delivery]) -> anyhow::Result<()> {
        let grouped = (|| -> anyhow::Result<BTreeMap<String, DatasetWrite>> {
            for delivery in deliveries {
                for batch in &delivery.outputs {
                    self.config.validate_batch(&self.discovery, batch)?;
                }
            }
            let mut grouped = BTreeMap::<String, DatasetWrite>::new();
            for delivery in deliveries {
                for batch in &delivery.outputs {
                    if batch.rows() == 0 {
                        continue;
                    }
                    let dataset = self
                        .discovery
                        .datasets
                        .iter()
                        .find(|dataset| dataset.name == batch.table)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Iceberg batch targets unknown dataset '{}'", batch.table)
                        })?;
                    if dataset_is_changelog(dataset) {
                        anyhow::ensure!(
                            deliveries.len() == 1,
                            "Iceberg replica commits exactly one source delivery at a time"
                        );
                        let identity = replica_source_identity(dataset, batch)?;
                        let ProjectedSinkBatch::Changelog(changelog) =
                            project_sink_batch(&self.discovery, batch)?
                        else {
                            anyhow::bail!("Iceberg replica dataset '{}' was not projected as changelog", dataset.name);
                        };
                        anyhow::ensure!(
                            grouped
                                .insert(
                                    batch.table.to_string(),
                                    DatasetWrite::Replica { changelog, identity },
                                )
                                .is_none(),
                            "Iceberg source delivery contains duplicate batches for replica dataset '{}'",
                            batch.table
                        );
                    } else {
                        let stored = if self.keep_system_columns {
                            batch.batch.clone()
                        } else {
                            project_user_columns(&batch.batch, &batch.system_columns)?
                        };
                        match grouped.entry(batch.table.to_string()).or_insert_with(|| {
                            DatasetWrite::Append {
                                batches: Vec::new(),
                                delivery_ids: Vec::new(),
                            }
                        }) {
                            DatasetWrite::Append {
                                batches,
                                delivery_ids,
                            } => {
                                batches.push(stored);
                                if delivery_ids.last().copied() != Some(delivery.id.get()) {
                                    delivery_ids.push(delivery.id.get());
                                }
                            }
                            DatasetWrite::Replica { .. } => {
                                anyhow::bail!(
                                    "Iceberg dataset '{}' mixes append and replica batches",
                                    batch.table
                                );
                            }
                        }
                    }
                }
            }
            Ok(grouped)
        })()
        .map_err(DataPlaneFailure::fatal)?;
        for (dataset, write) in grouped {
            let started = Instant::now();
            let rows = write.rows();
            let bytes = write.bytes();
            match write {
                DatasetWrite::Append {
                    batches,
                    delivery_ids,
                } => self.append(&dataset, batches, &delivery_ids).await?,
                DatasetWrite::Replica { changelog, identity } => {
                    self.apply_replica_delta(&dataset, changelog, identity)
                        .await?;
                }
            }
            self.counters.add_busy(started.elapsed());
            self.counters.add_rows(rows as u64);
            self.counters.add_bytes(bytes as u64);
            self.counters.add_flush();
        }
        Ok(())
    }

    async fn append(
        &self,
        dataset: &str,
        batches: Vec<RecordBatch>,
        delivery_ids: &[u64],
    ) -> anyhow::Result<()> {
        let table_ref = self.config.table_for_dataset(dataset)?;
        let table = self.catalog.load_table(&table_ident(&table_ref)?).await?;
        let commit = self.idempotent_snapshot_replay.then(|| {
            IcebergCommitIdentity::new(
                self.durable.delivery_id.as_ref(),
                self.replay_identity.as_deref(),
                self.partition_id,
                dataset,
                table.metadata().uuid(),
                StableCommitSource::FiniteDelivery {
                    delivery_ids: delivery_ids.to_vec(),
                },
            )
        }).transpose()?;
        let delivery_id = delivery_ids.last().copied().ok_or_else(|| {
            anyhow::anyhow!("Iceberg append requires at least one source delivery id")
        })?;
        if let Some(commit) = &commit {
            if self.commit_is_durable(commit).await? {
                if !has_commit_identity(&table, commit) {
                    return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                        "Iceberg durable commit '{}' is absent from the active table snapshot lineage",
                        commit.durable_key
                    ))
                    .into());
                }
                tracing::info!(
                    dataset,
                    delivery_id,
                    "Iceberg append was already committed; skipping replay"
                );
                return Ok(());
            }
        }
        if let Some(commit) = &commit {
            if has_commit_identity(&table, commit) {
                self.mark_commit_durable(commit).await?;
                tracing::info!(
                    dataset,
                    delivery_id,
                    "Iceberg snapshot proves append was already committed; skipping replay"
                );
                return Ok(());
            }
        }
        let commit_uuid = commit
            .as_ref()
            .map_or_else(uuid::Uuid::new_v4, |commit| commit.uuid);
        let files = self
            .write_data_files(
                &table,
                batches,
                format!("transferia-{delivery_id}-{commit_uuid}"),
            )
            .await?;
        let transaction = Transaction::new(&table);
        let mut append = transaction.fast_append().add_data_files(files);
        if let Some(commit) = &commit {
            append = append
                .set_commit_uuid(commit.uuid)
                .set_snapshot_properties(commit_snapshot_properties(commit));
        }
        let transaction = append.apply(transaction)?;
        let commit_started = Instant::now();
        let commit_result = transaction.commit(self.catalog.as_ref()).await;
        tracing::info!(
            target: "transferia.external_request",
            external_system = "iceberg_rest_catalog",
            operation = "commit_snapshot",
            dataset,
            delivery_id,
            elapsed_ms = elapsed_millis(commit_started),
            success = commit_result.is_ok(),
            "Iceberg REST catalog commit completed"
        );
        if let Err(commit_error) = commit_result {
            let committed_after_error = if let Some(commit) = &commit {
                let refreshed = self.catalog.load_table(&table_ident(&table_ref)?).await;
                matches!(
                    refreshed.as_ref(),
                    Ok(table) if has_commit_identity(table, commit)
                )
            } else {
                false
            };
            if !committed_after_error {
                return Err(commit_error.into());
            }
            tracing::warn!(
                dataset,
                delivery_id,
                error = %commit_error,
                "Iceberg commit response was ambiguous, but the committed snapshot was found"
            );
        }
        if let Some(commit) = &commit {
            self.mark_commit_durable(commit).await?;
        }
        Ok(())
    }

    async fn apply_replica_delta(
        &self,
        dataset: &str,
        changelog: ChangelogBatch,
        source: StableCommitSource,
    ) -> anyhow::Result<()> {
        let replay_identity = self.replay_identity.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Iceberg replica commit requires a stable source replay identity")
        })?;
        let table_ref = self.config.table_for_dataset(dataset)?;
        let ident = table_ident(&table_ref)?;
        let table = self.catalog.load_table(&ident).await?;
        let discovered = self
            .discovery
            .datasets
            .iter()
            .find(|candidate| candidate.name.as_ref() == dataset)
            .ok_or_else(|| anyhow::anyhow!("Iceberg replica dataset '{dataset}' is unknown"))?;
        ensure_table_schema(&table, &discovered.stored_schema).map_err(DataPlaneFailure::fatal)?;
        ensure_replica_table(
            &table,
            &discovered.stored_schema,
            self.durable.delivery_id.as_ref(),
            replay_identity,
        )
        .map_err(DataPlaneFailure::fatal)?;
        let commit = IcebergCommitIdentity::new(
            self.durable.delivery_id.as_ref(),
            Some(replay_identity),
            self.partition_id,
            dataset,
            table.metadata().uuid(),
            source,
        )
        .map_err(DataPlaneFailure::fatal)?;
        if self.commit_is_durable(&commit).await? {
            if !has_commit_identity(&table, &commit) {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "Iceberg durable commit '{}' is absent from the active table snapshot lineage",
                    commit.durable_key
                ))
                .into());
            }
            tracing::info!(dataset, token = %commit.token, "Iceberg row delta was already committed");
            return Ok(());
        }
        if has_commit_identity(&table, &commit) {
            self.mark_commit_durable(&commit).await?;
            tracing::info!(dataset, token = %commit.token, "Iceberg snapshot proves row delta was already committed");
            return Ok(());
        }

        let table_arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);
        let mut data_batches = Vec::new();
        let mut delete_batches = Vec::new();
        for run in changelog
            .collapsed_full_image_runs()
            .map_err(DataPlaneFailure::fatal)?
        {
            match run.action {
                ChangelogAction::Upsert => {
                    let batch = with_schema(&run.batch, Arc::clone(&table_arrow_schema))
                        .map_err(DataPlaneFailure::fatal)?;
                    delete_batches.push(batch.clone());
                    data_batches.push(batch);
                }
                ChangelogAction::Delete => {
                    delete_batches.push(full_delete_batch(
                        &run.batch,
                        Arc::clone(&table_arrow_schema),
                    )
                    .map_err(DataPlaneFailure::fatal)?);
                }
            }
        }
        anyhow::ensure!(
            !delete_batches.is_empty(),
            "Iceberg replica row delta contains no rows after changelog collapse"
        );
        let (data_files, delete_files) = tokio::try_join!(
            self.write_data_files(
                &table,
                data_batches,
                format!("transferia-data-{}", commit.uuid),
            ),
            self.write_equality_delete_files(
                &table,
                delete_batches,
                format!("transferia-delete-{}", commit.uuid),
            ),
        )?;
        let transaction = Transaction::new(&table);
        let properties = commit_snapshot_properties(&commit);
        let row_delta = transaction
            .row_delta()
            .add_data_files(data_files)
            .add_delete_files(delete_files)
            .set_commit_uuid(commit.uuid)
            .set_snapshot_properties(properties.clone())
            .set_idempotency_properties(properties);
        let transaction = row_delta.apply(transaction)?;
        let commit_started = Instant::now();
        let commit_result = transaction.commit(self.catalog.as_ref()).await;
        tracing::info!(
            target: "transferia.external_request",
            external_system = "iceberg_rest_catalog",
            operation = "commit_replica_row_delta",
            dataset,
            token = %commit.token,
            elapsed_ms = elapsed_millis(commit_started),
            success = commit_result.is_ok(),
            "Iceberg REST catalog row-delta commit completed"
        );
        if let Err(commit_error) = commit_result {
            let refreshed = self.catalog.load_table(&ident).await;
            if !matches!(refreshed.as_ref(), Ok(table) if has_commit_identity(table, &commit)) {
                return Err(commit_error.into());
            }
            tracing::warn!(
                dataset,
                token = %commit.token,
                error = %commit_error,
                "Iceberg row-delta response was ambiguous, but the exact snapshot was found"
            );
        }
        self.mark_commit_durable(&commit).await
    }

    async fn write_data_files(
        &self,
        table: &Table,
        batches: Vec<RecordBatch>,
        prefix: String,
    ) -> anyhow::Result<Vec<iceberg::spec::DataFile>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;
        let names = DefaultFileNameGenerator::new(
            prefix,
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let properties = WriterProperties::builder()
            .set_compression(parquet_compression(self.config.parquet_compression))
            .set_max_row_group_size(self.config.parquet_row_group_rows)
            .build();
        let parquet =
            ParquetWriterBuilder::new(properties, table.metadata().current_schema().clone());
        let shards = distribute_batches(batches, self.config.write_concurrency);
        let mut writers = JoinSet::new();
        for shard in shards {
            let parquet = parquet.clone();
            let file_io = table.file_io().clone();
            let location = location.clone();
            let names = names.clone();
            let arrow_schema = Arc::clone(&arrow_schema);
            let target_file_size_bytes = self.config.target_file_size_bytes;
            writers.spawn(async move {
                let rolling = RollingFileWriterBuilder::new(
                    parquet,
                    target_file_size_bytes,
                    file_io,
                    location,
                    names,
                );
                let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
                for batch in shard {
                    writer
                        .write(with_schema(&batch, Arc::clone(&arrow_schema))?)
                        .await?;
                }
                Ok::<_, anyhow::Error>(writer.close().await?)
            });
        }
        collect_writer_results(writers).await
    }

    async fn write_equality_delete_files(
        &self,
        table: &Table,
        batches: Vec<RecordBatch>,
        prefix: String,
    ) -> anyhow::Result<Vec<iceberg::spec::DataFile>> {
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;
        let names = DefaultFileNameGenerator::new(
            prefix,
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let properties = WriterProperties::builder()
            .set_compression(parquet_compression(self.config.parquet_compression))
            .set_max_row_group_size(self.config.parquet_row_group_rows)
            .build();
        let parquet =
            ParquetWriterBuilder::new(properties, table.metadata().current_schema().clone());
        let equality_ids = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect::<Vec<_>>();
        let shards = distribute_batches(batches, self.config.write_concurrency);
        let mut writers = JoinSet::new();
        for shard in shards {
            let rolling = RollingFileWriterBuilder::new(
                parquet.clone(),
                self.config.target_file_size_bytes,
                table.file_io().clone(),
                location.clone(),
                names.clone(),
            );
            let config = EqualityDeleteWriterConfig::new(
                equality_ids.clone(),
                table.metadata().current_schema().clone(),
            )?;
            writers.spawn(async move {
                let mut writer =
                    EqualityDeleteFileWriterBuilder::new(rolling, config)
                        .build(None)
                        .await?;
                for batch in shard {
                    writer.write(batch).await?;
                }
                Ok::<_, anyhow::Error>(writer.close().await?)
            });
        }
        collect_writer_results(writers).await
    }

    async fn commit_is_durable(&self, commit: &IcebergCommitIdentity) -> anyhow::Result<bool> {
        let Some(value) = self.durable.storage.read(&commit.durable_key).await? else {
            return Ok(false);
        };
        anyhow::ensure!(
            value.payload == commit.exact.as_bytes(),
            "Iceberg durable commit record '{}' is corrupt",
            commit.durable_key
        );
        Ok(true)
    }

    async fn mark_commit_durable(&self, commit: &IcebergCommitIdentity) -> anyhow::Result<()> {
        match self
            .durable
            .storage
            .compare_exchange(&commit.durable_key, None, commit.exact.as_bytes())
            .await?
        {
            CompareExchangeResult::Applied(_) => Ok(()),
            CompareExchangeResult::Conflict(Some(value))
                if value.payload == commit.exact.as_bytes() =>
            {
                Ok(())
            }
            CompareExchangeResult::Conflict(_) => anyhow::bail!(
                "Iceberg durable commit record '{}' conflicts with this append",
                commit.durable_key
            ),
        }
    }
}

pub(super) async fn collect_writer_results<T>(
    mut writers: JoinSet<anyhow::Result<Vec<T>>>,
) -> anyhow::Result<Vec<T>>
where
    T: Send + 'static,
{
    let mut output = Vec::new();
    let mut first_error = None;
    while let Some(result) = writers.join_next().await {
        match result {
            Ok(Ok(files)) if first_error.is_none() => output.extend(files),
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error.context("Iceberg file writer failed"));
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(
                        anyhow::Error::new(error).context("Iceberg file writer task failed"),
                    );
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(output)
}

const ICEBERG_COMMIT_TOKEN_PROPERTY: &str = "transferia.commit-token";
const ICEBERG_COMMIT_IDENTITY_PROPERTY: &str = "transferia.commit-identity";
const ICEBERG_REPLICA_DELIVERY_PROPERTY: &str = "transferia.replication.delivery-id";
const ICEBERG_REPLICA_REPLAY_PROPERTY: &str = "transferia.replication.replay-identity";

pub(super) struct IcebergCommitIdentity {
    pub(super) token: String,
    pub(super) exact: String,
    pub(super) durable_key: String,
    pub(super) uuid: uuid::Uuid,
}

#[derive(Serialize)]
struct PersistedIcebergCommitIdentity<'a> {
    version: u8,
    delivery: &'a str,
    replay_identity: Option<&'a str>,
    worker_partition: i64,
    dataset: &'a str,
    table_uuid: uuid::Uuid,
    source: StableCommitSource,
}

impl IcebergCommitIdentity {
    fn new(
        delivery: &str,
        replay_identity: Option<&str>,
        partition: i64,
        dataset: &str,
        table_uuid: uuid::Uuid,
        source: StableCommitSource,
    ) -> anyhow::Result<Self> {
        let exact = serde_json::to_string(&PersistedIcebergCommitIdentity {
            version: 2,
            delivery,
            replay_identity,
            worker_partition: partition,
            dataset,
            table_uuid,
            source,
        })?;
        let digest = murmur3::murmur3_x64_128(&mut Cursor::new(exact.as_bytes()), 0)?;
        let token = format!("v2:{digest:032x}");
        let mut uuid_bytes = digest.to_be_bytes();
        uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x80;
        uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;
        Ok(Self {
            durable_key: format!("iceberg-sink/commits/{digest:032x}"),
            token,
            exact,
            uuid: uuid::Uuid::from_bytes(uuid_bytes),
        })
    }

    #[cfg(test)]
    pub(super) fn new_finite_for_test(
        delivery: &str,
        replay_identity: Option<&str>,
        partition: i64,
        dataset: &str,
        table_uuid: uuid::Uuid,
        delivery_ids: Vec<u64>,
    ) -> anyhow::Result<Self> {
        Self::new(
            delivery,
            replay_identity,
            partition,
            dataset,
            table_uuid,
            StableCommitSource::FiniteDelivery { delivery_ids },
        )
    }
}

fn has_commit_identity(table: &Table, commit: &IcebergCommitIdentity) -> bool {
    let mut snapshot = table.metadata().current_snapshot();
    while let Some(current) = snapshot {
        let properties = &current.summary().additional_properties;
        if properties
            .get(ICEBERG_COMMIT_TOKEN_PROPERTY)
            .is_some_and(|candidate| candidate == &commit.token)
            && properties
                .get(ICEBERG_COMMIT_IDENTITY_PROPERTY)
                .is_some_and(|candidate| candidate == &commit.exact)
        {
            return true;
        }
        snapshot = current
            .parent_snapshot_id()
            .and_then(|parent| table.metadata().snapshot_by_id(parent));
    }
    false
}

fn commit_snapshot_properties(commit: &IcebergCommitIdentity) -> HashMap<String, String> {
    HashMap::from([
        (
            ICEBERG_COMMIT_TOKEN_PROPERTY.to_owned(),
            commit.token.clone(),
        ),
        (
            ICEBERG_COMMIT_IDENTITY_PROPERTY.to_owned(),
            commit.exact.clone(),
        ),
    ])
}

impl Sink for IcebergSink {
    fn run(
        self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let replica_mode = self.discovery.datasets.iter().any(dataset_is_changelog);
            let mut committed_deliveries = 0_u64;
            let mut committed_rows = 0_u64;
            let mut last_delivery_id = None;
            let mut pending = Vec::new();
            let mut pending_bytes = 0_usize;
            loop {
                let next = tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv() => delivery,
                };
                let input_closed = next.is_none();
                if let Some(delivery) = next {
                    pending_bytes = pending_bytes.saturating_add(
                        delivery
                            .outputs
                            .iter()
                            .map(transferia_core::SinkBatch::bytes)
                            .sum::<usize>(),
                    );
                    pending.push(delivery);
                }
                if pending.is_empty() {
                    if input_closed {
                        break;
                    }
                    continue;
                }
                if !replica_mode
                    && !delivery_group_ready(
                        pending_bytes,
                        self.config.commit_target_size_bytes(),
                        input_closed,
                    )
                {
                    continue;
                }
                let id = pending
                    .last()
                    .ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!("missing pending Iceberg delivery"))
                    })?
                    .id;
                let source_messages = pending
                    .iter()
                    .map(|delivery| delivery.meta.source_messages)
                    .sum::<u64>();
                self.write_deliveries(&pending)
                    .await
                    .map_err(DataPlaneFailure::retryable_or_passthrough)?;
                self.counters.add_source_messages(source_messages);
                io.events
                    .send(SinkEvent::CommittedThrough(id))
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "Iceberg sink event receiver closed"
                        ))
                    })?;
                committed_deliveries = committed_deliveries.saturating_add(pending.len() as u64);
                committed_rows = committed_rows.saturating_add(source_messages);
                last_delivery_id = Some(id.get());
                pending.clear();
                pending_bytes = 0;
                if input_closed {
                    break;
                }
            }
            tracing::info!(
                committed_deliveries,
                committed_rows,
                last_delivery_id,
                "Iceberg sink drained all deliveries"
            );
            Ok(())
        })
    }
}

pub(super) const fn delivery_group_ready(
    pending_bytes: usize,
    target_bytes: usize,
    input_closed: bool,
) -> bool {
    input_closed || pending_bytes >= target_bytes
}

fn parquet_compression(compression: IcebergParquetCompression) -> ParquetCompression {
    match compression {
        IcebergParquetCompression::None => ParquetCompression::UNCOMPRESSED,
        IcebergParquetCompression::Lz4 => ParquetCompression::LZ4_RAW,
        IcebergParquetCompression::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
    }
}

fn distribute_batches(batches: Vec<RecordBatch>, concurrency: usize) -> Vec<Vec<RecordBatch>> {
    let shard_count = concurrency.min(batches.len()).max(1);
    let mut shards = (0..shard_count).map(|_| Vec::new()).collect::<Vec<_>>();
    let mut shard_bytes = vec![0_usize; shard_count];
    for batch in batches {
        let Some((index, bytes)) = shard_bytes
            .iter_mut()
            .enumerate()
            .min_by_key(|(_, bytes)| **bytes)
        else {
            return shards;
        };
        *bytes = bytes.saturating_add(batch.get_array_memory_size());
        shards[index].push(batch);
    }
    shards
}

pub(super) fn iceberg_schema(schema: &DatasetSchema) -> anyhow::Result<iceberg::spec::Schema> {
    let primary_keys = schema
        .columns
        .iter()
        .map(|column| column.primary_key)
        .collect::<Vec<_>>();
    let arrow = iceberg_arrow_schema(schema);
    let converted = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow)?;
    let identifier_ids = converted
        .as_struct()
        .fields()
        .iter()
        .zip(primary_keys)
        .filter_map(|(field, primary_key)| primary_key.then_some(field.id))
        .collect::<Vec<_>>();
    Ok(converted
        .into_builder()
        .with_identifier_field_ids(identifier_ids)
        .build()?)
}

fn ensure_table_schema(table: &Table, expected: &DatasetSchema) -> anyhow::Result<()> {
    anyhow::ensure!(
        table.metadata().default_partition_spec().is_unpartitioned(),
        "Iceberg sink table '{}' is partitioned; partitioned writes are not supported yet",
        table.identifier()
    );
    let actual = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
    let expected = iceberg_arrow_schema(expected);
    let mismatches = actual
        .fields()
        .iter()
        .zip(expected.fields())
        .filter(|(actual, expected)| {
            actual.name() != expected.name()
                || actual.data_type() != expected.data_type()
                || actual.is_nullable() != expected.is_nullable()
        })
        .map(|(actual, expected)| format!("actual={actual:?}, expected={expected:?}"))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual.fields().len() == expected.fields().len() && mismatches.is_empty(),
        "Iceberg table '{}' schema differs from the discovered dataset: column_count actual={} expected={}; {}",
        table.identifier(),
        actual.fields().len(),
        expected.fields().len(),
        mismatches.join("; ")
    );
    Ok(())
}

fn ensure_replica_table(
    table: &Table,
    expected: &DatasetSchema,
    delivery_id: &str,
    replay_identity: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        table.metadata().format_version() == FormatVersion::V2,
        "Iceberg replica table '{}' must use format version 2",
        table.identifier()
    );
    let properties = table.metadata().properties();
    anyhow::ensure!(
        properties
            .get(ICEBERG_REPLICA_DELIVERY_PROPERTY)
            .is_some_and(|value| value == delivery_id)
            && properties
                .get(ICEBERG_REPLICA_REPLAY_PROPERTY)
                .is_some_and(|value| value == replay_identity),
        "Iceberg replica table '{}' is not exclusively owned by delivery '{}' and its exact replay identity",
        table.identifier(),
        delivery_id
    );
    let expected_keys = expected
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let schema = table.metadata().current_schema();
    let actual_keys = schema
        .identifier_field_ids()
        .map(|field_id| {
            schema.name_by_field_id(field_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Iceberg replica table '{}' identifier field id {field_id} is absent from its schema",
                    table.identifier()
                )
            })
        })
        .collect::<anyhow::Result<BTreeSet<_>>>()?;
    anyhow::ensure!(
        actual_keys == expected_keys,
        "Iceberg replica table '{}' identifier fields {:?} differ from discovered primary key {:?}",
        table.identifier(),
        actual_keys,
        expected_keys
    );
    Ok(())
}

pub(super) fn with_schema(batch: &RecordBatch, schema: Arc<Schema>) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch.num_columns() == schema.fields().len(),
        "runtime Arrow batch has {} columns but Iceberg table has {}",
        batch.num_columns(),
        schema.fields().len()
    );
    let actual = batch.schema();
    let columns = actual
        .fields()
        .iter()
        .zip(schema.fields())
        .zip(batch.columns())
        .map(|((actual, expected), column)| {
            anyhow::ensure!(
                actual.name() == expected.name() && actual.is_nullable() == expected.is_nullable(),
                "runtime Arrow field '{}' does not match Iceberg field '{}'",
                actual.name(),
                expected.name()
            );
            if actual.data_type() == expected.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast_losslessly(column, expected.data_type())
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn full_delete_batch(
    keys: &RecordBatch,
    schema: Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| match keys.schema().index_of(field.name()) {
            Ok(index) => {
                let column = keys.column(index);
                if column.data_type() == field.data_type() {
                    Ok(Arc::clone(column))
                } else {
                    cast_losslessly(column, field.data_type())
                }
            }
            Err(_) => Ok(new_null_array(field.data_type(), keys.num_rows())),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn cast_losslessly(column: &ArrayRef, target: &DataType) -> anyhow::Result<ArrayRef> {
    if column.data_type() == &DataType::Timestamp(TimeUnit::Second, None)
        && target == &DataType::Timestamp(TimeUnit::Microsecond, None)
    {
        let values = column
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?;
        let mut widened = TimestampMicrosecondBuilder::with_capacity(values.len());
        for value in values.iter() {
            match value {
                Some(value) => widened.append_value(value.checked_mul(1_000_000).ok_or_else(
                    || {
                        anyhow::anyhow!(
                            "Iceberg timestamp seconds value cannot be widened to microseconds"
                        )
                    },
                )?),
                None => widened.append_null(),
            }
        }
        return Ok(Arc::new(widened.finish()));
    }
    Ok(cast(column, target)?)
}

pub(super) fn validate_timestamp_values(batch: &RecordBatch) -> anyhow::Result<()> {
    for column in batch.columns() {
        if column.data_type() != &DataType::Timestamp(TimeUnit::Second, None) {
            continue;
        }
        let values = column
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?;
        anyhow::ensure!(
            values
                .iter()
                .flatten()
                .all(|value| value.checked_mul(1_000_000).is_some()),
            "Iceberg timestamp seconds value cannot be widened to microseconds"
        );
    }
    Ok(())
}

fn iceberg_arrow_schema(schema: &DatasetSchema) -> Schema {
    Schema::new(
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.name,
                    iceberg_arrow_data_type(&column.data_type),
                    column.nullable,
                )
                .with_metadata(column.arrow_metadata())
            })
            .collect::<Vec<_>>(),
    )
}

fn iceberg_arrow_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::LargeBinary,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            DataType::Int32
        }
        DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::Float16 | DataType::Float32 => DataType::Float32,
        DataType::Timestamp(TimeUnit::Second, None) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        other => other.clone(),
    }
}

fn project_user_columns(
    batch: &RecordBatch,
    system_columns: &transferia_core::data::system_columns::SystemColumns,
) -> anyhow::Result<RecordBatch> {
    let system = system_columns
        .iter()
        .map(|column| column.index)
        .collect::<HashSet<_>>();
    Ok(batch.project(
        &(0..batch.num_columns())
            .filter(|index| !system.contains(index))
            .collect::<Vec<_>>(),
    )?)
}
