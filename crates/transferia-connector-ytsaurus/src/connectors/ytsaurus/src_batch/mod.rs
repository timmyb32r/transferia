use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt as _};

use super::client::{classify_http_failure, YTsaurusClient};
use super::config::{SourceTableConfig, YTsaurusSourceConfig};
use super::schema::{parse_schema, schemas_equal};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;

#[derive(Clone)]
struct DiscoveredTable {
    config: SourceTableConfig,
    schema: DatasetSchema,
}

pub struct YTsaurusSourceConnector {
    config: YTsaurusSourceConfig,
    client: YTsaurusClient,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl YTsaurusSourceConnector {
    pub fn from_config(
        config: YTsaurusSourceConfig,
        metrics: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let client = YTsaurusClient::new(&config.connection)?;
        Ok(Self {
            config,
            client,
            parser_plan: ParserPlan::native_source(),
            metrics,
            discovered: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn discover_tables(&self) -> anyhow::Result<Arc<Vec<DiscoveredTable>>> {
        self.discovered
            .get_or_try_init(|| async {
                let mut tables = Vec::with_capacity(self.config.tables.len());
                for table in &self.config.tables {
                    let dynamic = self
                        .client
                        .get_json(&format!("{}/@dynamic", table.path))
                        .await?;
                    anyhow::ensure!(
                        dynamic == serde_json::Value::Bool(false),
                        "YTsaurus source table '{}' must be static",
                        table.path
                    );
                    let schema = parse_schema(
                        self.client
                            .get_json(&format!("{}/@schema", table.path))
                            .await?,
                    )?;
                    tables.push(DiscoveredTable {
                        config: table.clone(),
                        schema,
                    });
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

impl SourceConnector for YTsaurusSourceConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YTsaurus(SourceDescriptor {
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
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("YTsaurus discovery cancelled"),
                tables = self.discover_tables() => tables?,
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
                .map(|table| -> anyhow::Result<DiscoveredDataset> {
                    let system_names = system_columns
                        .iter()
                        .map(|kind| kind.default_name())
                        .collect::<HashSet<_>>();
                    let physical_system_columns = table
                        .schema
                        .columns
                        .iter()
                        .filter(|column| system_names.contains(column.name.as_str()))
                        .count();
                    anyhow::ensure!(
                        physical_system_columns == 0
                            || physical_system_columns == system_columns.len(),
                        "YTsaurus table '{}' contains {physical_system_columns} of {} reserved system columns; either all or none must be present",
                        table.config.path,
                        system_columns.len(),
                    );
                    let mut incoming = table.schema.clone();
                    if physical_system_columns == 0 {
                        incoming.columns.extend(system_columns.iter().map(|kind| {
                            SchemaColumn::new(
                                kind.default_name().to_owned(),
                                kind.data_type(),
                                false,
                            )
                        }));
                    }
                    let stored_schema = if request.keep_system_columns {
                        incoming.clone()
                    } else {
                        let mut stored = table.schema.clone();
                        stored
                            .columns
                            .retain(|column| !system_names.contains(column.name.as_str()));
                        stored
                    };
                    Ok(DiscoveredDataset {
                        role: DatasetRole::Main,
                        name: Arc::from(table.config.name.as_str()),
                        incoming_schema: incoming,
                        stored_schema,
                        system_columns: discovered_system_columns.clone(),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(DeliveryDiscovery {
                source_name: Arc::from("ytsaurus"),
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
            let SourceBuildContext {
                partition_id,
                cancellation,
                ..
            } = context;
            let tables = self.discover_tables().await?;
            let table = tables
                .get(usize::try_from(partition_id)?)
                .ok_or_else(|| {
                    anyhow::anyhow!("YTsaurus source partition {partition_id} does not exist")
                })?
                .clone();
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            let response = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("YTsaurus read cancelled"),
                response = tokio::time::timeout(
                    Duration::from_millis(self.config.stream_open_timeout_ms),
                    self.client.read_arrow(&table.config.path, 0),
                ) => response
                    .map_err(|_| anyhow::anyhow!(
                        "YTsaurus snapshot stream did not open within {} ms",
                        self.config.stream_open_timeout_ms,
                    ))?
                    .map_err(classify_http_failure)?,
            };
            Ok(Box::new(YTsaurusSource {
                table,
                partition_id,
                client: self.client.clone(),
                stream: Box::pin(response.bytes_stream()),
                decoder: StreamDecoder::new(),
                queued: VecDeque::new(),
                batch_rows: self.config.batch_rows,
                stream_retry_max_attempts: self.config.stream_retry_max_attempts,
                stream_retry_initial: Duration::from_millis(self.config.stream_retry_initial_ms),
                stream_retry_max: Duration::from_millis(self.config.stream_retry_max_ms),
                stream_open_timeout: Duration::from_millis(self.config.stream_open_timeout_ms),
                stream_idle_timeout: Duration::from_millis(self.config.stream_idle_timeout_ms),
                consecutive_stream_failures: 0,
                offset: 0,
                finished: false,
                counters,
            }) as Box<dyn Source>)
        })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        self.parser_plan.parser()
    }

    fn parses_rows(&self) -> bool {
        self.parser_plan.parses_rows()
    }
}

struct YTsaurusSource {
    table: DiscoveredTable,
    partition_id: i64,
    client: YTsaurusClient,
    stream: ResponseStream,
    decoder: StreamDecoder,
    queued: VecDeque<RecordBatch>,
    batch_rows: usize,
    stream_retry_max_attempts: usize,
    stream_retry_initial: Duration,
    stream_retry_max: Duration,
    stream_open_timeout: Duration,
    stream_idle_timeout: Duration,
    consecutive_stream_failures: usize,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

impl YTsaurusSource {
    fn queue_validated(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        let batch = normalize_read_batch(batch, &self.table.schema)?;
        validate_read_schema(&batch, &self.table.schema)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = self.batch_rows.min(batch.num_rows() - offset);
            self.queued.push_back(batch.slice(offset, len));
            offset += len;
        }
        Ok(())
    }

    fn decode_bytes(&mut self, bytes: bytes::Bytes) -> anyhow::Result<()> {
        let mut buffer = Buffer::from(bytes);
        while !buffer.is_empty() {
            match self.decoder.decode(&mut buffer) {
                Ok(Some(batch)) => self.queue_validated(&batch)?,
                Ok(None) => {}
                Err(arrow::error::ArrowError::IpcError(message)) if message == "Unexpected EOS" => {
                    self.decoder = StreamDecoder::new();
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn output_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<SourceBatch> {
        let rows = batch.num_rows();
        let len_i64 = i64::try_from(rows)?;
        let schema = batch.schema();
        let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
        let mut arrays = batch.columns().to_vec();
        let kinds = [
            SystemColumnKind::Topic,
            SystemColumnKind::Partition,
            SystemColumnKind::Offset,
            SystemColumnKind::MessageIndex,
        ];
        let mut system_indices = kinds
            .iter()
            .filter_map(|kind| {
                schema
                    .fields()
                    .iter()
                    .position(|field| field.name() == kind.default_name())
            })
            .collect::<Vec<_>>();
        if system_indices.is_empty() {
            let base = fields.len();
            fields.extend(kinds.iter().map(|kind| {
                Arc::new(Field::new(
                    kind.default_name(),
                    kind.data_type(),
                    false,
                ))
            }));
            arrays.extend([
                Arc::new(StringArray::from(vec![
                    self.table.config.path.as_str();
                    rows
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![self.partition_id; rows])) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    self.offset
                        ..self
                            .offset
                            .checked_add(len_i64)
                            .ok_or_else(|| anyhow::anyhow!("YTsaurus source offset overflow"))?,
                )) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef,
            ]);
            system_indices = (base..base + kinds.len()).collect();
        }
        anyhow::ensure!(
            system_indices.len() == kinds.len(),
            "YTsaurus runtime batch contains a partial set of reserved system columns"
        );
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        self.offset = self
            .offset
            .checked_add(len_i64)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus source offset overflow"))?;
        self.consecutive_stream_failures = 0;
        self.counters.add_messages(rows as u64);
        let batch_bytes = batch.get_array_memory_size();
        self.counters.add_decompressed_bytes(batch_bytes as u64);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::from(self.table.config.name.as_str()),
                false,
                batch,
                SystemColumns::new(
                    kinds
                        .into_iter()
                        .zip(system_indices)
                        .map(|(kind, index)| SystemColumn {
                            kind,
                            name: Arc::from(kind.default_name()),
                            index,
                        })
                        .collect::<Vec<_>>(),
                ),
            )],
            source_rows: rows as u64,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }

    async fn recover_stream(
        &mut self,
        mut failure: DataPlaneFailure,
    ) -> transferia_core::failure::DataPlaneResult<()> {
        loop {
            if !failure.is_retryable() {
                return Err(failure);
            }
            if self.consecutive_stream_failures >= self.stream_retry_max_attempts {
                return Err(DataPlaneFailure::fatal(failure.into_source().context(format!(
                    "YTsaurus snapshot stream could not resume at row {} after {} attempts",
                    self.offset, self.stream_retry_max_attempts
                ))));
            }
            self.consecutive_stream_failures += 1;
            let exponent = u32::try_from(self.consecutive_stream_failures.saturating_sub(1))
                .unwrap_or(u32::MAX)
                .min(31);
            let delay = self
                .stream_retry_initial
                .saturating_mul(1_u32 << exponent)
                .min(self.stream_retry_max);
            tracing::warn!(
                row_index = self.offset,
                attempt = self.consecutive_stream_failures,
                max_attempts = self.stream_retry_max_attempts,
                delay_ms = delay.as_millis(),
                error = %failure,
                "YTsaurus snapshot stream interrupted; resuming from the last emitted row"
            );
            tokio::time::sleep(delay).await;
            match tokio::time::timeout(
                self.stream_open_timeout,
                self.client.read_arrow(&self.table.config.path, self.offset),
            )
            .await
            {
                Ok(Ok(response)) => {
                    self.stream = Box::pin(response.bytes_stream());
                    self.decoder = StreamDecoder::new();
                    self.queued.clear();
                    return Ok(());
                }
                Ok(Err(error)) => {
                    failure = DataPlaneFailure::retryable_or_passthrough(
                        classify_http_failure(error),
                    );
                }
                Err(_) => {
                    failure = DataPlaneFailure::retryable(anyhow::anyhow!(
                        "YTsaurus snapshot stream did not open within {} ms",
                        self.stream_open_timeout.as_millis()
                    ));
                }
            }
        }
    }
}

impl Source for YTsaurusSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some(batch) = self.queued.pop_front() {
                    return self.output_batch(&batch).map_err(DataPlaneFailure::fatal);
                }
                if self.finished {
                    return Ok(SourceBatch::Finished);
                }
                match tokio::time::timeout(self.stream_idle_timeout, self.stream.next()).await {
                    Ok(Some(Ok(bytes))) => {
                        self.decode_bytes(bytes).map_err(DataPlaneFailure::fatal)?;
                    }
                    Ok(Some(Err(error))) => {
                        let failure = DataPlaneFailure::retryable_or_passthrough(
                            classify_http_failure(error.into()),
                        );
                        self.recover_stream(failure).await?;
                    }
                    Ok(None) => {
                        self.decoder
                            .finish()
                            .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                        self.finished = true;
                    }
                    Err(_) => {
                        self.recover_stream(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "YTsaurus snapshot stream delivered no data for {} ms",
                            self.stream_idle_timeout.as_millis()
                        )))
                        .await?;
                    }
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn normalize_read_batch(
    batch: &RecordBatch,
    expected: &DatasetSchema,
) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch.num_columns() == expected.columns.len(),
        "YTsaurus read schema has {} columns, discovery declared {}",
        batch.num_columns(),
        expected.columns.len()
    );
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(expected.columns.len());
    let mut columns = Vec::with_capacity(expected.columns.len());
    for ((field, array), expected) in schema
        .fields()
        .iter()
        .zip(batch.columns())
        .zip(&expected.columns)
    {
        anyhow::ensure!(
            field.name() == &expected.name,
            "YTsaurus read column is '{}', expected '{}'",
            field.name(),
            expected.name
        );
        anyhow::ensure!(
            field.is_nullable() == expected.nullable,
            "YTsaurus read column '{}' has nullable={}, discovery declared nullable={}",
            expected.name,
            field.is_nullable(),
            expected.nullable
        );
        let array = if field.data_type() == &expected.data_type {
            Arc::clone(array)
        } else if matches!(
            field.data_type(),
            DataType::Dictionary(_, value) if value.as_ref() == &expected.data_type
        ) {
            arrow::compute::cast(array.as_ref(), &expected.data_type)?
        } else {
            anyhow::bail!(
                "YTsaurus read column '{}' has type {:?}, discovery declared {:?}",
                expected.name,
                field.data_type(),
                expected.data_type
            );
        };
        fields.push(Field::new(
            expected.name.clone(),
            expected.data_type.clone(),
            expected.nullable,
        ));
        columns.push(array);
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

pub(super) fn validate_read_schema(
    batch: &RecordBatch,
    expected: &DatasetSchema,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        batch.num_columns() == expected.columns.len(),
        "YTsaurus read schema has {} columns, discovery declared {}",
        batch.num_columns(),
        expected.columns.len()
    );
    for (position, (field, column)) in batch
        .schema()
        .fields()
        .iter()
        .zip(&expected.columns)
        .enumerate()
    {
        anyhow::ensure!(
            field.name() == &column.name,
            "YTsaurus read column {position} is '{}', expected '{}'",
            field.name(),
            column.name
        );
        anyhow::ensure!(
            field.data_type() == &column.data_type,
            "YTsaurus read column '{}' has type {:?}, discovery declared {:?}",
            column.name,
            field.data_type(),
            column.data_type
        );
        anyhow::ensure!(
            field.is_nullable() == column.nullable,
            "YTsaurus read column '{}' has nullable={}, discovery declared nullable={}",
            column.name,
            field.is_nullable(),
            column.nullable
        );
    }
    let actual = DatasetSchema::new(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                SchemaColumn::new(
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect(),
    );
    anyhow::ensure!(
        schemas_equal(&actual, expected),
        "YTsaurus read schema drifted"
    );
    Ok(())
}
