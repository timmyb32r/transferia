use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{Field, Schema};
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
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    validate_batch_against_discovery, validate_stored_projection, ArrowTypeFamily,
    DeliveryDiscovery, NameSyntax, SinkLimits, SinkLimitsDescription, TextLimit,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::semantics::EndpointDescriptor;
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
    let namespace = iceberg::NamespaceIdent::from_vec(config.namespace.clone())?;
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
            self.table_for_dataset(&dataset.name)?.validate("dataset table")?;
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
            namespace: self.namespace.clone(),
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
}

impl IcebergSink {
    async fn write_delivery(&self, delivery: &Delivery) -> anyhow::Result<()> {
        for batch in &delivery.outputs {
            self.config.validate_batch(&self.discovery, batch)?;
        }
        let mut grouped = HashMap::<&str, Vec<RecordBatch>>::new();
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
        for (dataset, batches) in grouped {
            let started = Instant::now();
            let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            let bytes = batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
            self.append(dataset, batches, delivery.id.get()).await?;
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
        let table = self
            .catalog
            .load_table(&table_ident(&table_ref)?)
            .await?;
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;
        let names = DefaultFileNameGenerator::new(
            format!("transferia-{delivery_id}-{}", uuid::Uuid::new_v4()),
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
        let transaction = transaction
            .fast_append()
            .add_data_files(files)
            .apply(transaction)?;
        transaction.commit(self.catalog.as_ref()).await?;
        Ok(())
    }
}

impl Sink for IcebergSink {
    fn run(
        self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            while let Some(delivery) = tokio::select! { () = io.cancellation.cancelled() => None, delivery = io.deliveries.recv() => delivery }
            {
                let id = delivery.id;
                let source_messages = delivery.meta.source_messages;
                self.write_delivery(&delivery)
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
            }
            Ok(())
        })
    }
}

pub(super) fn iceberg_schema(schema: &DatasetSchema) -> anyhow::Result<iceberg::spec::Schema> {
    let primary_keys = schema
        .columns
        .iter()
        .map(|column| column.primary_key)
        .collect::<Vec<_>>();
    let arrow = Schema::new(
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(&column.name, column.data_type.clone(), column.nullable)
                    .with_metadata(column.arrow_metadata())
            })
            .collect::<Vec<_>>(),
    );
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
    let expected = Schema::new(
        expected
            .columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    );
    anyhow::ensure!(
        actual.fields().len() == expected.fields().len()
            && actual
                .fields()
                .iter()
                .zip(expected.fields())
                .all(|(actual, expected)| actual.name() == expected.name()
                    && actual.data_type() == expected.data_type()
                    && actual.is_nullable() == expected.is_nullable()),
        "Iceberg table '{}' schema differs from the discovered dataset",
        table.identifier()
    );
    Ok(())
}

fn with_schema(batch: &RecordBatch, schema: Arc<Schema>) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch
            .schema()
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(actual, expected)| actual.name() == expected.name()
                && actual.data_type() == expected.data_type()
                && actual.is_nullable() == expected.is_nullable()),
        "runtime Arrow batch does not match Iceberg table schema"
    );
    Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
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
