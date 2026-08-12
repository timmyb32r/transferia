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
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use super::client::{runtime_http_failure, YTsaurusClient};
use super::config::{SourceTableConfig, YTsaurusSourceConfig};
use super::schema::{parse_schema, schemas_equal};
use crate::compatibility::{EndpointDescriptor, SourceBehavior, SourceDescriptor};
use crate::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::{CommitMarker, Source};
use crate::providers::traits::SourceProvider;
use crate::types::message::SourceBatch;
use crate::types::schema::{DatasetSchema, SchemaColumn};
use crate::types::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use crate::types::table_data::TableData;

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
    pub fn from_config(value: Value, metrics: Arc<MetricsRegistry>) -> anyhow::Result<Self> {
        let config: YTsaurusSourceConfig = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse YTsaurus source config: {error}"))?;
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
        })
    }

    fn delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("YTsaurus discovery cancelled"),
                tables = self.discover_tables() => tables?,
            };
            let system_columns = vec![
                SystemColumnKind::Topic,
                SystemColumnKind::Partition,
                SystemColumnKind::Offset,
                SystemColumnKind::MessageIndex,
            ];
            let datasets = tables
                .iter()
                .map(|table| {
                    let mut incoming = table.schema.clone();
                    incoming.columns.extend(system_columns.iter().map(|kind| {
                        SchemaColumn::new(kind.name().to_owned(), kind.data_type(), false)
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
                        system_columns: system_columns.clone(),
                    }
                })
                .collect();
            Ok(DeliveryDiscovery {
                source_name: Arc::from("ytsaurus"),
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
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
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
                response = self.client.read_arrow(&table.config.path) => response.map_err(runtime_http_failure)?,
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
    fn queue_normalized(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        let normalized = normalize_batch(batch, &self.table.schema)?;
        let mut offset = 0;
        while offset < normalized.num_rows() {
            let len = self.batch_rows.min(normalized.num_rows() - offset);
            self.queued.push_back(normalized.slice(offset, len));
            offset += len;
        }
        Ok(())
    }

    fn decode_bytes(&mut self, bytes: bytes::Bytes) -> anyhow::Result<()> {
        let mut buffer = Buffer::from(bytes);
        while !buffer.is_empty() {
            match self.decoder.decode(&mut buffer) {
                Ok(Some(batch)) => self.queue_normalized(&batch)?,
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
                SystemColumnKind::Topic.name(),
                DataType::Utf8,
                false,
            )),
            Arc::new(Field::new(
                SystemColumnKind::Partition.name(),
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                SystemColumnKind::Offset.name(),
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                SystemColumnKind::MessageIndex.name(),
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
                        index: base,
                    },
                    SystemColumn {
                        kind: SystemColumnKind::Partition,
                        index: base + 1,
                    },
                    SystemColumn {
                        kind: SystemColumnKind::Offset,
                        index: base + 2,
                    },
                    SystemColumn {
                        kind: SystemColumnKind::MessageIndex,
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
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some(batch) = self.queued.pop_front() {
                    return self.output_batch(&batch);
                }
                if self.finished {
                    return Ok(SourceBatch::Finished);
                }
                match self.stream.next().await {
                    Some(Ok(bytes)) => self.decode_bytes(bytes)?,
                    Some(Err(error)) => return Err(runtime_http_failure(error.into())),
                    None => {
                        self.decoder.finish()?;
                        self.finished = true;
                    }
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn normalize_batch(batch: &RecordBatch, expected: &DatasetSchema) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch.num_columns() == expected.columns.len(),
        "YTsaurus runtime schema has {} columns, discovery declared {}",
        batch.num_columns(),
        expected.columns.len()
    );
    let mut arrays = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (position, (field, column)) in batch
        .schema()
        .fields()
        .iter()
        .zip(&expected.columns)
        .enumerate()
    {
        anyhow::ensure!(
            field.name() == &column.name,
            "YTsaurus runtime column {position} is '{}', expected '{}'",
            field.name(),
            column.name
        );
        let array = if field.data_type() == &column.data_type {
            Arc::clone(batch.column(position))
        } else {
            arrow::compute::cast(batch.column(position), &column.data_type).map_err(|error| {
                anyhow::anyhow!(
                    "YTsaurus runtime column '{}' cannot be normalized from {:?} to discovered {:?}: {error}",
                    column.name,
                    field.data_type(),
                    column.data_type
                )
            })?
        };
        arrays.push(array);
        fields.push(Field::new(
            &column.name,
            column.data_type.clone(),
            column.nullable,
        ));
    }
    let normalized = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    let actual = DatasetSchema::new(
        normalized
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
        "YTsaurus runtime schema drifted"
    );
    Ok(normalized)
}
