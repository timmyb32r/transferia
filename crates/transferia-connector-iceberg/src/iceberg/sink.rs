use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use parquet::file::properties::WriterProperties;
use sha2::{Digest as _, Sha256};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    validate_batch_against_discovery, validate_stored_projection, ArrowTypeFamily,
    DeliveryDiscovery, NameSyntax, SinkLimits, SinkLimitsDescription, TextLimit,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::durable::{CompareExchangeResult, DurableContext};
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

use super::catalog::{build_catalog, table_ident};
use super::config::{IcebergSinkConfig, IcebergTableRef};

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
        for dataset in &discovery.datasets {
            self.table_for_dataset(&dataset.name)?
                .validate("dataset table")?;
            validate_stored_projection(discovery, dataset)?;
            iceberg_schema(&dataset.stored_schema)?;
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
        Ok(())
    }
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

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let catalog = self.catalog().await?;
            for dataset in request.datasets {
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
                        .build();
                    catalog.create_table(ident.namespace(), creation).await?
                };
                ensure_table_schema(&table, &dataset.schema)?;
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
            }) as Box<dyn Sink>)
        })
    }
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
}

impl IcebergSink {
    async fn write_deliveries(&self, deliveries: &[Delivery]) -> anyhow::Result<()> {
        for delivery in deliveries {
            for batch in &delivery.outputs {
                self.config.validate_batch(&self.discovery, batch)?;
            }
        }
        let mut grouped = HashMap::<&str, Vec<RecordBatch>>::new();
        for delivery in deliveries {
            for batch in &delivery.outputs {
                if batch.rows() == 0 {
                    continue;
                }
                let stored = if self.keep_system_columns {
                    batch.batch.clone()
                } else {
                    project_user_columns(&batch.batch, &batch.system_columns)?
                };
                grouped
                    .entry(batch.table.as_ref())
                    .or_default()
                    .push(stored);
            }
        }
        let delivery_id = deliveries
            .last()
            .ok_or_else(|| anyhow::anyhow!("Iceberg sink cannot write an empty delivery group"))?
            .id
            .get();
        for (dataset, batches) in grouped {
            let started = Instant::now();
            let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            let bytes = batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
            self.append(dataset, batches, delivery_id).await?;
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
        delivery_id: u64,
    ) -> anyhow::Result<()> {
        let table_ref = self.config.table_for_dataset(dataset)?;
        let table = self.catalog.load_table(&table_ident(&table_ref)?).await?;
        let commit = self.idempotent_snapshot_replay.then(|| {
            IcebergCommitIdentity::new(
                self.durable.delivery_id.as_ref(),
                self.partition_id,
                dataset,
                table.metadata().uuid(),
                delivery_id,
            )
        });
        if let Some(commit) = &commit {
            if self.commit_is_durable(commit).await? {
                tracing::info!(
                    dataset,
                    delivery_id,
                    "Iceberg append was already committed; skipping replay"
                );
                return Ok(());
            }
        }
        if let Some(commit) = &commit {
            if has_commit_token(&table, &commit.token) {
                self.mark_commit_durable(commit).await?;
                tracing::info!(
                    dataset,
                    delivery_id,
                    "Iceberg snapshot proves append was already committed; skipping replay"
                );
                return Ok(());
            }
        }
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;
        let commit_uuid = commit
            .as_ref()
            .map_or_else(uuid::Uuid::new_v4, |commit| commit.uuid);
        let names = DefaultFileNameGenerator::new(
            format!("transferia-{delivery_id}-{commit_uuid}"),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            self.config.target_file_size_bytes,
            table.file_io().clone(),
            location,
            names,
        );
        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        for batch in batches {
            writer
                .write(with_schema(&batch, Arc::clone(&arrow_schema))?)
                .await?;
        }
        let files = writer.close().await?;
        let transaction = Transaction::new(&table);
        let mut append = transaction.fast_append().add_data_files(files);
        if let Some(commit) = &commit {
            append = append
                .set_commit_uuid(commit.uuid)
                .set_snapshot_properties(HashMap::from([(
                    ICEBERG_COMMIT_TOKEN_PROPERTY.to_owned(),
                    commit.token.clone(),
                )]));
        }
        let transaction = append.apply(transaction)?;
        if let Err(commit_error) = transaction.commit(self.catalog.as_ref()).await {
            let committed_after_error = if let Some(commit) = &commit {
                let refreshed = self.catalog.load_table(&table_ident(&table_ref)?).await;
                matches!(
                    refreshed.as_ref(),
                    Ok(table) if has_commit_token(table, &commit.token)
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

    async fn commit_is_durable(&self, commit: &IcebergCommitIdentity) -> anyhow::Result<bool> {
        let Some(value) = self.durable.storage.read(&commit.durable_key).await? else {
            return Ok(false);
        };
        anyhow::ensure!(
            value.payload == commit.token.as_bytes(),
            "Iceberg durable commit record '{}' is corrupt",
            commit.durable_key
        );
        Ok(true)
    }

    async fn mark_commit_durable(&self, commit: &IcebergCommitIdentity) -> anyhow::Result<()> {
        match self
            .durable
            .storage
            .compare_exchange(&commit.durable_key, None, commit.token.as_bytes())
            .await?
        {
            CompareExchangeResult::Applied(_) => Ok(()),
            CompareExchangeResult::Conflict(Some(value))
                if value.payload == commit.token.as_bytes() =>
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

const ICEBERG_COMMIT_TOKEN_PROPERTY: &str = "transferia.commit-token";

pub(super) struct IcebergCommitIdentity {
    pub(super) token: String,
    pub(super) durable_key: String,
    pub(super) uuid: uuid::Uuid,
}

impl IcebergCommitIdentity {
    pub(super) fn new(
        delivery: &str,
        partition: i64,
        dataset: &str,
        table_uuid: uuid::Uuid,
        delivery_id: u64,
    ) -> Self {
        let mut hasher = Sha256::new();
        for component in [
            delivery.as_bytes(),
            &partition.to_be_bytes(),
            dataset.as_bytes(),
            table_uuid.as_bytes(),
            &delivery_id.to_be_bytes(),
        ] {
            hasher.update((component.len() as u64).to_be_bytes());
            hasher.update(component);
        }
        let digest = hasher.finalize();
        let token = format!("v1:{digest:x}");
        let mut uuid_bytes = [0_u8; 16];
        uuid_bytes.copy_from_slice(&digest[..16]);
        uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x50;
        uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;
        Self {
            durable_key: format!("iceberg-sink/commits/{digest:x}"),
            token,
            uuid: uuid::Uuid::from_bytes(uuid_bytes),
        }
    }
}

fn has_commit_token(table: &Table, token: &str) -> bool {
    table.metadata().snapshots().any(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(ICEBERG_COMMIT_TOKEN_PROPERTY)
            .is_some_and(|candidate| candidate == token)
    })
}

impl Sink for IcebergSink {
    fn run(
        self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
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
                if !delivery_group_ready(
                    pending_bytes,
                    self.config.target_file_size_bytes,
                    input_closed,
                ) {
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
                Ok(cast(column, expected.data_type())?)
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
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
