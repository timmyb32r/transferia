use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::{
    write_message, CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;

use super::config::YdbSinkConfig;
use super::transport::{is_retryable_error, YdbClient};
use super::types::{
    column_plans, ColumnKind, ARROW_UUID_EXTENSION, YDB_DYNUMBER_EXTENSION,
    YDB_TZ_DATETIME_EXTENSION, YDB_TZ_DATE_EXTENSION, YDB_TZ_TIMESTAMP_EXTENSION,
    YDB_YSON_EXTENSION,
};
use transferia_core::data::schema::{SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

pub struct YdbSinkConnector {
    config: Arc<YdbSinkConfig>,
}

impl YdbSinkConnector {
    pub fn from_config(config: YdbSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl SinkLimits for YdbSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let name = TextLimit {
            syntax: NameSyntax::AnyNonEmptyUtf8,
            max_utf8_bytes: None,
        };
        SinkLimitsDescription {
            sink: "ydb",
            dataset_name: Some(name.clone()),
            column_name: Some(name),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Decimal,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Timestamp,
                ArrowTypeFamily::Duration,
                ArrowTypeFamily::FixedSizeBinary,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "YDB sink requires at least one dataset"
        );
        let mut names = HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "YDB datasets repeat table '{}'",
                dataset.name
            );
            self.table_path(&dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "YDB table '{}' cannot have an empty schema",
                dataset.name
            );
            let mut primary_keys = 0_usize;
            for column in &dataset.stored_schema.columns {
                validate_name("column", &column.name)?;
                let kind = column_kind(column)?;
                if column.primary_key {
                    primary_keys += 1;
                    anyhow::ensure!(
                        !column.nullable,
                        "YDB primary-key column '{}.{}' must not be nullable",
                        dataset.name,
                        column.name
                    );
                    ensure_primary_key_type(&kind, column)?;
                }
            }
            anyhow::ensure!(
                primary_keys > 0,
                "YDB table '{}' requires at least one primary-key column",
                dataset.name
            );
        }
        anyhow::ensure!(
            names.len() == self.tables.len(),
            "YDB sink table mappings must exactly match discovered datasets"
        );
        Ok(())
    }
}

impl SinkConnector for YdbSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YdbSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String> {
        let data_type = yql_type(column)?;
        Ok(if column.nullable {
            format!("{data_type}?")
        } else {
            data_type
        })
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let mut client = YdbClient::connect(&self.config.connection).await?;
            for dataset in request.datasets {
                let path = self.config.table_path(&dataset.table)?;
                if self.config.create_tables {
                    execute_scheme_query_with_retry(
                        &mut client,
                        create_table_query(path, &dataset.schema)?,
                        self.config.retry_max_ms,
                    )
                    .await?;
                }
                let description =
                    describe_table_with_retry(&mut client, path, self.config.retry_max_ms).await?;
                let actual = column_plans(description.columns, &description.primary_key)?;
                ensure_table_schema(path, &dataset.schema, &actual)?;
            }
            Ok(())
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            Ok(Box::new(YdbSink {
                client: YdbClient::connect(&self.config.connection).await?,
                table_paths: self
                    .config
                    .tables
                    .iter()
                    .map(|table| (table.name.clone(), table.path.clone()))
                    .collect(),
                counters: context.counters,
                discovery: context.discovery,
                limits,
            }) as Box<dyn Sink>)
        })
    }
}

async fn execute_scheme_query_with_retry(
    client: &mut YdbClient,
    query: String,
    retry_max_ms: u64,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let mut delay = std::time::Duration::from_millis(20);
    loop {
        match client.execute_scheme_query(query.clone()).await {
            Ok(()) => return Ok(()),
            Err(error)
                if is_retryable_error(&error)
                    && started.elapsed() < std::time::Duration::from_millis(retry_max_ms) =>
            {
                tracing::warn!(error = %error, delay_ms = delay.as_millis(), "retrying transient YDB schema operation");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(1));
            }
            Err(error) => return Err(error),
        }
    }
}

async fn describe_table_with_retry(
    client: &mut YdbClient,
    path: &str,
    retry_max_ms: u64,
) -> anyhow::Result<ydb_grpc::ydb_proto::table::DescribeTableResult> {
    let started = std::time::Instant::now();
    let mut delay = std::time::Duration::from_millis(20);
    loop {
        match client.describe_table(path.to_owned()).await {
            Ok(description) => return Ok(description),
            Err(error)
                if is_retryable_error(&error)
                    && started.elapsed() < std::time::Duration::from_millis(retry_max_ms) =>
            {
                tracing::warn!(table = path, error = %error, delay_ms = delay.as_millis(), "retrying transient YDB table description");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(1));
            }
            Err(error) => return Err(error),
        }
    }
}

struct YdbSink {
    client: YdbClient,
    table_paths: HashMap<String, String>,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
}

struct EncodedBatch {
    path: String,
    schema: Vec<u8>,
    data: Vec<u8>,
    rows: u64,
    bytes: u64,
}

impl YdbSink {
    async fn write_delivery(&mut self, delivery: &Delivery) -> anyhow::Result<()> {
        for batch in &delivery.outputs {
            self.limits.validate_batch(&self.discovery, batch)?;
        }
        let keep_system_columns = self.discovery.keep_system_columns;
        let work = delivery
            .outputs
            .iter()
            .filter(|batch| batch.rows() > 0)
            .map(|batch| {
                let path = self
                    .table_paths
                    .get(batch.table.as_ref())
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "YDB sink has no physical table mapping for dataset '{}'",
                            batch.table
                        )
                    })?;
                let stored = if keep_system_columns {
                    batch.batch.clone()
                } else {
                    without_system_columns(&batch.batch, &batch.system_columns)?
                };
                Ok((path, stored, batch.rows() as u64, batch.bytes() as u64))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        // IPC serialization is CPU work. Keep it off Tokio's I/O workers, and
        // finish every conversion before the first irreversible BulkUpsert.
        let encoded = tokio::task::spawn_blocking(move || {
            work.into_iter()
                .map(|(path, batch, rows, bytes)| {
                    let (schema, data) = encode_arrow_batch(&batch)?;
                    Ok(EncodedBatch {
                        path,
                        schema,
                        data,
                        rows,
                        bytes,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .await??;

        let started = std::time::Instant::now();
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for batch in encoded {
            self.client
                .bulk_upsert(batch.path, batch.schema, batch.data)
                .await?;
            rows += batch.rows;
            bytes += batch.bytes;
            self.counters.add_flush();
        }
        self.counters.add_busy(started.elapsed());
        self.counters.add_rows(rows);
        self.counters.add_bytes(bytes);
        Ok(())
    }
}

impl Sink for YdbSink {
    fn run(
        mut self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                while let Some(delivery) = tokio::select! {
                    biased;
                    () = io.cancellation.cancelled() => None,
                    delivery = io.deliveries.recv() => delivery,
                } {
                    let id = delivery.id;
                    let source_messages = delivery.meta.source_messages;
                    self.write_delivery(&delivery).await?;
                    self.counters.add_source_messages(source_messages);
                    io.events
                        .send(SinkEvent::CommittedThrough(id))
                        .await
                        .map_err(|_| anyhow::anyhow!("YDB sink event receiver closed"))?;
                }
                Ok(())
            }
            .await;
            result.map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}

pub(super) fn create_table_query(
    path: &str,
    schema: &transferia_core::data::schema::DatasetSchema,
) -> anyhow::Result<String> {
    let columns = schema
        .columns
        .iter()
        .map(|column| {
            Ok(format!(
                "{} {}{}",
                quote_identifier(&column.name),
                yql_type(column)?,
                if column.nullable { "" } else { " NOT NULL" }
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let primary_key = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !primary_key.is_empty(),
        "YDB table '{path}' requires a primary key"
    );
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}));",
        quote_identifier(path),
        columns.join(", "),
        primary_key.join(", ")
    ))
}

fn ensure_table_schema(
    path: &str,
    expected: &transferia_core::data::schema::DatasetSchema,
    actual: &[super::types::ColumnPlan],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected.columns.len() == actual.len(),
        "YDB table '{path}' has {} columns, delivery requires {}",
        actual.len(),
        expected.columns.len()
    );
    for (expected, actual) in expected.columns.iter().zip(actual) {
        let kind = column_kind(expected)?;
        anyhow::ensure!(
            expected.name == actual.name
                && expected.nullable == actual.nullable
                && expected.primary_key == actual.primary_key
                && kind == actual.kind,
            "YDB table '{path}' column '{}' does not match delivery discovery",
            expected.name
        );
    }
    Ok(())
}

fn validate_name(kind: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.is_empty(), "YDB {kind} name must not be empty");
    anyhow::ensure!(
        !value.contains('\0'),
        "YDB {kind} name must not contain NUL"
    );
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn yql_type(column: &SchemaColumn) -> anyhow::Result<String> {
    Ok(match column_kind(column)? {
        ColumnKind::Bool => "Bool".to_owned(),
        ColumnKind::Int8 => "Int8".to_owned(),
        ColumnKind::UInt8 => "Uint8".to_owned(),
        ColumnKind::Int16 => "Int16".to_owned(),
        ColumnKind::UInt16 => "Uint16".to_owned(),
        ColumnKind::Int32 => "Int32".to_owned(),
        ColumnKind::UInt32 => "Uint32".to_owned(),
        ColumnKind::Int64 => "Int64".to_owned(),
        ColumnKind::UInt64 => "Uint64".to_owned(),
        ColumnKind::Float32 => "Float".to_owned(),
        ColumnKind::Float64 => "Double".to_owned(),
        ColumnKind::Date32 => "Date32".to_owned(),
        ColumnKind::TimestampSecond => "Datetime64".to_owned(),
        ColumnKind::TimestampMicrosecond => "Timestamp64".to_owned(),
        ColumnKind::DurationMicrosecond => "Interval64".to_owned(),
        ColumnKind::Binary(None) => "String".to_owned(),
        ColumnKind::Binary(Some(YDB_YSON_EXTENSION)) => "Yson".to_owned(),
        ColumnKind::Utf8(None) => "Utf8".to_owned(),
        ColumnKind::Utf8(Some(ARROW_JSON_EXTENSION_NAME)) => "Json".to_owned(),
        ColumnKind::Utf8(Some(YDB_DYNUMBER_EXTENSION)) => "DyNumber".to_owned(),
        ColumnKind::Utf8(Some(YDB_TZ_DATE_EXTENSION)) => "TzDate".to_owned(),
        ColumnKind::Utf8(Some(YDB_TZ_DATETIME_EXTENSION)) => "TzDatetime".to_owned(),
        ColumnKind::Utf8(Some(YDB_TZ_TIMESTAMP_EXTENSION)) => "TzTimestamp".to_owned(),
        ColumnKind::Decimal { precision, scale } => format!("Decimal({precision}, {scale})"),
        ColumnKind::Uuid => "Uuid".to_owned(),
        ColumnKind::Binary(Some(extension)) | ColumnKind::Utf8(Some(extension)) => {
            anyhow::bail!("unsupported Arrow extension '{extension}' for YDB sink")
        }
    })
}

fn column_kind(column: &SchemaColumn) -> anyhow::Result<ColumnKind> {
    let extension = column.arrow_extension_name;
    Ok(match (&column.data_type, extension) {
        (DataType::Boolean, None) => ColumnKind::Bool,
        (DataType::Int8, None) => ColumnKind::Int8,
        (DataType::UInt8, None) => ColumnKind::UInt8,
        (DataType::Int16, None) => ColumnKind::Int16,
        (DataType::UInt16, None) => ColumnKind::UInt16,
        (DataType::Int32, None) => ColumnKind::Int32,
        (DataType::UInt32, None) => ColumnKind::UInt32,
        (DataType::Int64, None) => ColumnKind::Int64,
        (DataType::UInt64, None) => ColumnKind::UInt64,
        (DataType::Float32, None) => ColumnKind::Float32,
        (DataType::Float64, None) => ColumnKind::Float64,
        (DataType::Date32, None) => ColumnKind::Date32,
        (DataType::Timestamp(TimeUnit::Second, None), None) => ColumnKind::TimestampSecond,
        (DataType::Timestamp(TimeUnit::Microsecond, None), None) => {
            ColumnKind::TimestampMicrosecond
        }
        (DataType::Duration(TimeUnit::Microsecond), None) => ColumnKind::DurationMicrosecond,
        (DataType::Binary, None) => ColumnKind::Binary(None),
        (DataType::Binary, Some(YDB_YSON_EXTENSION)) => {
            ColumnKind::Binary(Some(YDB_YSON_EXTENSION))
        }
        (DataType::Utf8, None) => ColumnKind::Utf8(None),
        (DataType::Utf8, Some(ARROW_JSON_EXTENSION_NAME)) => {
            ColumnKind::Utf8(Some(ARROW_JSON_EXTENSION_NAME))
        }
        (DataType::Utf8, Some(YDB_DYNUMBER_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_DYNUMBER_EXTENSION))
        }
        (DataType::Utf8, Some(YDB_TZ_DATE_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_TZ_DATE_EXTENSION))
        }
        (DataType::Utf8, Some(YDB_TZ_DATETIME_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_TZ_DATETIME_EXTENSION))
        }
        (DataType::Utf8, Some(YDB_TZ_TIMESTAMP_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_TZ_TIMESTAMP_EXTENSION))
        }
        (DataType::Decimal128(precision, scale), None) if *precision <= 35 => ColumnKind::Decimal {
            precision: *precision,
            scale: *scale,
        },
        (DataType::FixedSizeBinary(16), Some(ARROW_UUID_EXTENSION)) => ColumnKind::Uuid,
        (data_type, extension) => anyhow::bail!(
            "unsupported Arrow type {data_type:?} with extension {extension:?} for YDB sink"
        ),
    })
}

fn ensure_primary_key_type(kind: &ColumnKind, column: &SchemaColumn) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(
            kind,
            ColumnKind::Bool
                | ColumnKind::Int8
                | ColumnKind::UInt8
                | ColumnKind::Int16
                | ColumnKind::UInt16
                | ColumnKind::Int32
                | ColumnKind::UInt32
                | ColumnKind::Int64
                | ColumnKind::UInt64
                | ColumnKind::Date32
                | ColumnKind::TimestampSecond
                | ColumnKind::TimestampMicrosecond
                | ColumnKind::Binary(None)
                | ColumnKind::Utf8(None)
                | ColumnKind::Decimal { .. }
                | ColumnKind::Uuid
        ),
        "YDB type for primary-key column '{}' is not key-compatible",
        column.name
    );
    Ok(())
}

fn without_system_columns(
    batch: &RecordBatch,
    system_columns: &SystemColumns,
) -> anyhow::Result<RecordBatch> {
    if system_columns.is_empty() {
        return Ok(batch.clone());
    }
    let system_indexes = system_columns
        .iter()
        .map(|column| column.index)
        .collect::<HashSet<_>>();
    let indexes = (0..batch.num_columns())
        .filter(|index| !system_indexes.contains(index))
        .collect::<Vec<_>>();
    Ok(batch.project(&indexes)?)
}

pub(super) fn encode_arrow_batch(batch: &RecordBatch) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())?;
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let mut compression = CompressionContext::default();
    let schema_message = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema().as_ref(),
        &mut dictionary_tracker,
        &options,
    );
    let mut schema = Vec::new();
    write_message(&mut schema, schema_message, &options)?;
    let (dictionaries, record_batch) =
        generator.encode(&batch, &mut dictionary_tracker, &options, &mut compression)?;
    anyhow::ensure!(
        dictionaries.is_empty(),
        "YDB BulkUpsert does not accept Arrow dictionary side messages"
    );
    let mut data = Vec::new();
    write_message(&mut data, record_batch, &options)?;
    Ok((schema, data))
}
