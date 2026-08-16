use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

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
use crate::core::data::message::SourceBatch;
use crate::core::data::schema::{DatasetSchema, SchemaColumn};
use crate::core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use crate::core::data::table_data::TableData;
use crate::core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use crate::core::failure::DataPlaneFailure;
use crate::core::source::{CommitMarker, Source};
use crate::delivery::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::providers::traits::{SourceBuildContext, SourceDiscoveryContext, SourceProvider};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;

#[derive(Clone)]
struct DiscoveredTable {
    config: SourceTableConfig,
    schema: DatasetSchema,
}

pub struct YTsaurusSourceProvider {
    config: YTsaurusSourceConfig,
    client: YTsaurusClient,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    discovered: tokio::sync::OnceCell<Arc<Vec<DiscoveredTable>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl YTsaurusSourceProvider {
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

impl SourceProvider for YTsaurusSourceProvider {
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
                response = self.client.read_arrow(&table.config.path) => response.map_err(classify_http_failure)?,
            };
            Ok(Box::new(YTsaurusSource {
                table,
                partition_id,
                stream: Box::pin(response.bytes_stream()),
                decoder: StreamDecoder::new(),
                queued: VecDeque::new(),
                batch_rows: self.config.batch_rows,
                offset: 0,
                finished: false,
                counters,
            }) as Box<dyn Source>)
        })
    }

    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}

struct YTsaurusSource {
    table: DiscoveredTable,
    partition_id: i64,
    stream: ResponseStream,
    decoder: StreamDecoder,
    queued: VecDeque<RecordBatch>,
    batch_rows: usize,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

impl YTsaurusSource {
    fn queue_validated(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        validate_read_schema(batch, &self.table.schema)?;
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
        let base = batch.num_columns();
        let len_i64 = i64::try_from(rows)?;
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut arrays = batch.columns().to_vec();
        fields.extend([
            Arc::new(Field::new(
                SystemColumnKind::Topic.default_name(),
                DataType::Utf8,
                false,
            )),
            Arc::new(Field::new(
                SystemColumnKind::Partition.default_name(),
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                SystemColumnKind::Offset.default_name(),
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                SystemColumnKind::MessageIndex.default_name(),
                DataType::UInt64,
                false,
            )),
        ]);
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
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        self.offset = self
            .offset
            .checked_add(len_i64)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus source offset overflow"))?;
        self.counters.add_messages(rows as u64);
        let batch_bytes = batch.get_array_memory_size();
        self.counters.add_decompressed_bytes(batch_bytes as u64);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::from(self.table.config.output_name.as_str()),
                false,
                batch,
                SystemColumns::new(vec![
                    SystemColumn {
                        kind: SystemColumnKind::Topic,
                        name: Arc::from(SystemColumnKind::Topic.default_name()),
                        index: base,
                    },
                    SystemColumn {
                        kind: SystemColumnKind::Partition,
                        name: Arc::from(SystemColumnKind::Partition.default_name()),
                        index: base + 1,
                    },
                    SystemColumn {
                        kind: SystemColumnKind::Offset,
                        name: Arc::from(SystemColumnKind::Offset.default_name()),
                        index: base + 2,
                    },
                    SystemColumn {
                        kind: SystemColumnKind::MessageIndex,
                        name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
                        index: base + 3,
                    },
                ]),
            )],
            source_rows: rows as u64,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }
}

impl Source for YTsaurusSource {
    fn read_batch(&mut self) -> BoxFuture<'_, crate::core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some(batch) = self.queued.pop_front() {
                    return self.output_batch(&batch).map_err(DataPlaneFailure::fatal);
                }
                if self.finished {
                    return Ok(SourceBatch::Finished);
                }
                match self.stream.next().await {
                    Some(Ok(bytes)) => self.decode_bytes(bytes).map_err(DataPlaneFailure::fatal)?,
                    Some(Err(error)) => {
                        return Err(DataPlaneFailure::retryable_or_passthrough(
                            classify_http_failure(error.into()),
                        ));
                    }
                    None => {
                        self.decoder
                            .finish()
                            .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                        self.finished = true;
                    }
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, crate::core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
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
