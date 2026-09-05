#![allow(
    clippy::expect_used,
    clippy::needless_pass_by_value,
    reason = "source state is validated before hot-path access and Bytes ownership matches the decoder interface"
)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt as _};

use super::client::{classify_http_failure, YTsaurusClient};
use super::config::{
    SourceTableConfig, YTsaurusBenchmarkDiscardConfig, YTsaurusBenchmarkTransport,
    YTsaurusReadFormat, YTsaurusReadOrdering, YTsaurusSourceConfig,
};
use super::discard::{output_format, DiscardDecoder};
use super::native_rpc::{
    decode_arrow_bytes, NativePartitionedReadStream, NativePipelinedReadStream, NativeReadFormat,
    NativeReadPayload, NativeReadStream,
};
use super::schema::parse_schema;
use super::yt_wire::{count_wire_rows, YtWireDecoder};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::record_batch::compact_record_batch_chunks;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, PerformanceAdvice,
    PerformanceAdviceSeverity, SchemaOrigin, SourceTopology,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_registry::{SourceBuildContext, SourceConnector, SourceDiscoveryContext};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;
enum YTsaurusReadStream {
    Http(ResponseStream),
    Native(NativePipelinedReadStream),
    Partitioned(NativePartitionedReadStream),
}

impl YTsaurusReadStream {
    async fn next_chunk(&mut self) -> Result<Option<ReadChunk>, DataPlaneFailure> {
        match self {
            Self::Http(stream) => stream
                .next()
                .await
                .transpose()
                .map(|chunk| {
                    chunk.map(|bytes| ReadChunk {
                        network_raw_bytes: 0,
                        network_decoded_bytes: bytes.len() as u64,
                        network_decode_duration: Duration::ZERO,
                        payload: ReadChunkPayload::Bytes(bytes),
                        stream_id: None,
                        end_of_stream: false,
                        cumulative_rows: None,
                    })
                })
                .map_err(|error| {
                    DataPlaneFailure::retryable_or_passthrough(classify_http_failure(error.into()))
                }),
            Self::Native(stream) => stream
                .next_block()
                .await
                .map(|block| block.map(native_block_to_chunk))
                .map_err(DataPlaneFailure::retryable),
            Self::Partitioned(stream) => stream
                .next_block()
                .await
                .map(|block| block.map(native_block_to_chunk))
                .map_err(DataPlaneFailure::retryable),
        }
    }
}

fn native_block_to_chunk(block: super::native_rpc::NativeReadBlock) -> ReadChunk {
    let payload = match block.payload {
        NativeReadPayload::Encoded(payload) => match block.format {
            NativeReadFormat::Arrow => ReadChunkPayload::Bytes(payload),
            NativeReadFormat::YtWire => ReadChunkPayload::YtWire {
                bytes: payload,
                name_table_entries: block.name_table_entries,
            },
        },
        NativeReadPayload::Decoded(batches) => ReadChunkPayload::RecordBatches(batches),
    };
    ReadChunk {
        network_raw_bytes: block.network_raw_bytes,
        network_decoded_bytes: block.network_decoded_bytes,
        network_decode_duration: block.network_decode_duration,
        payload,
        stream_id: block.stream_id,
        end_of_stream: block.end_of_stream,
        cumulative_rows: block.cumulative_rows,
    }
}

pub(super) struct ReadChunk {
    payload: ReadChunkPayload,
    pub(super) network_raw_bytes: u64,
    pub(super) network_decoded_bytes: u64,
    pub(super) network_decode_duration: Duration,
    stream_id: Option<usize>,
    end_of_stream: bool,
    pub(super) cumulative_rows: Option<u64>,
}

enum ReadChunkPayload {
    Bytes(Bytes),
    RecordBatches(Vec<RecordBatch>),
    YtWire {
        bytes: Bytes,
        name_table_entries: Vec<String>,
    },
}

#[derive(Clone)]
pub(super) struct DiscoveredTable {
    pub(super) config: SourceTableConfig,
    pub(super) dataset_name: Arc<str>,
    pub(super) schema: DatasetSchema,
    pub(super) optimize_for_scan: bool,
    pub(super) physical_layout: PhysicalChunkLayout,
}

#[derive(Clone, Copy)]
pub(super) struct PhysicalChunkLayout {
    pub(super) total: u64,
    pub(super) columnar: u64,
    pub(super) non_columnar: u64,
}

impl PhysicalChunkLayout {
    pub(super) fn from_statistics(
        total: u64,
        statistics: &serde_json::Value,
    ) -> anyhow::Result<Self> {
        let formats = statistics
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("YTsaurus chunk_format_statistics must be an object"))?;
        let mut observed = 0_u64;
        let mut columnar = 0_u64;
        for (format, value) in formats {
            let count = value
                .get("chunk_count")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    anyhow::anyhow!("YTsaurus chunk format '{format}' has no unsigned chunk_count")
                })?;
            observed = observed
                .checked_add(count)
                .ok_or_else(|| anyhow::anyhow!("YTsaurus physical chunk count overflow"))?;
            if format == "table_unversioned_columnar" {
                columnar = count;
            }
        }
        anyhow::ensure!(
            observed == total,
            "YTsaurus chunk_format_statistics accounts for {observed} chunks, but @chunk_count is {total}"
        );
        Ok(Self {
            total,
            columnar,
            non_columnar: total - columnar,
        })
    }

    pub(super) const fn all_columnar(&self) -> bool {
        self.non_columnar == 0
    }
}

pub(super) fn performance_advice(
    tables: &[DiscoveredTable],
    proxy_role: Option<&str>,
) -> Vec<PerformanceAdvice> {
    let mut advice = tables
        .iter()
        .filter_map(|table| {
            if !table.optimize_for_scan {
                return Some(PerformanceAdvice {
                    code: "YT_OPTIMIZE_FOR_LOOKUP".to_owned(),
                    severity: PerformanceAdviceSeverity::Info,
                    summary: format!(
                        "Table '{}' is optimized for point lookups",
                        table.dataset_name
                    ),
                    explanation: format!(
                        "YTsaurus table '{}' has optimize_for=lookup; sequential snapshot reads may be slower.",
                        table.config.path
                    ),
                    remediation: "Rewrite the table with optimize_for=scan before a large snapshot delivery when read throughput matters.".to_owned(),
                    config_paths: vec!["source.ytsaurus.tables".to_owned()],
                });
            }
            (!table.physical_layout.all_columnar()).then(|| PerformanceAdvice {
                code: "YT_SCAN_HAS_NON_COLUMNAR_CHUNKS".to_owned(),
                severity: PerformanceAdviceSeverity::Warning,
                summary: format!(
                    "Table '{}' contains non-columnar chunks",
                    table.dataset_name
                ),
                explanation: format!(
                    "YTsaurus table '{}' has optimize_for=scan, but only {} of {} physical chunks are table_unversioned_columnar; Transferia will use YT wire rowsets instead of Arrow.",
                    table.config.path,
                    table.physical_layout.columnar,
                    table.physical_layout.total,
                ),
                remediation: "Physically rewrite every chunk with optimize_for=scan and chunk_format=table_unversioned_columnar, then verify @chunk_format_statistics before retrying.".to_owned(),
                config_paths: vec!["source.ytsaurus.tables".to_owned()],
            })
        })
        .collect::<Vec<_>>();
    if proxy_role.is_none() {
        advice.push(PerformanceAdvice {
            code: "YT_SHARED_RPC_PROXIES".to_owned(),
            severity: PerformanceAdviceSeverity::Info,
            summary: "No dedicated YTsaurus RPC proxy role is selected".to_owned(),
            explanation: "Snapshot reads use the cluster's default RPC proxy pool, which may be shared and contended.".to_owned(),
            remediation: "Provision a dedicated RPC proxy role and select it in the YTsaurus source advanced settings when sustained read throughput matters.".to_owned(),
            config_paths: vec!["source.ytsaurus.proxy_role".to_owned()],
        });
    }
    advice
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
        let client = YTsaurusClient::new_with_proxy_role(
            &config.connection,
            (!config.proxy_role.is_empty()).then_some(config.proxy_role.as_str()),
        )?;
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
                    let dataset_name = Arc::from(table.dataset_name()?);
                    let node_type = self
                        .client
                        .get_json(&super::attribute_path(&table.path, "type"))
                        .await?
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "YTsaurus source path '{}' returned an invalid node type",
                                table.path
                            )
                        })?;
                    anyhow::ensure!(
                        node_type == "table",
                        "YTsaurus source path '{}' is a {node_type}, not a table; select a table path from the suggestions",
                        table.path
                    );
                    let dynamic = self
                        .client
                        .get_json(&super::attribute_path(&table.path, "dynamic"))
                        .await?;
                    anyhow::ensure!(
                        dynamic == serde_json::Value::Bool(false),
                        "YTsaurus source table '{}' must be static",
                        table.path
                    );
                    let optimize_for = self
                        .client
                        .get_json(&super::attribute_path(&table.path, "optimize_for"))
                        .await?
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "YTsaurus source table '{}' has a non-string optimize_for attribute",
                                table.path
                            )
                        })?;
                    anyhow::ensure!(
                        matches!(optimize_for.as_str(), "lookup" | "scan"),
                        "YTsaurus source table '{}' has unsupported optimize_for value '{optimize_for}'",
                        table.path
                    );
                    if optimize_for == "lookup" {
                        tracing::info!(
                            table_path = table.path,
                            optimize_for,
                            recommendation = "set optimize_for=scan for sequential snapshot reads",
                            "YTsaurus table is optimized for lookup; optimize_for=scan may improve read throughput"
                        );
                    }
                    let chunk_count = self
                        .client
                        .get_json(&super::attribute_path(&table.path, "chunk_count"))
                        .await?
                        .as_u64()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "YTsaurus source table '{}' has a non-unsigned chunk_count attribute",
                                table.path
                            )
                        })?;
                    let physical_layout = PhysicalChunkLayout::from_statistics(
                        chunk_count,
                        &self
                            .client
                            .get_json(&super::attribute_path(
                                &table.path,
                                "chunk_format_statistics",
                            ))
                            .await?,
                    )?;
                    if optimize_for == "scan" && !physical_layout.all_columnar() {
                        tracing::warn!(
                            table_path = table.path,
                            total_chunks = physical_layout.total,
                            columnar_chunks = physical_layout.columnar,
                            non_columnar_chunks = physical_layout.non_columnar,
                            selected_rowset_format = "yt_wire",
                            "YTsaurus table is not physically columnar; Arrow will not be requested"
                        );
                    } else if physical_layout.all_columnar() {
                        tracing::info!(
                            table_path = table.path,
                            total_chunks = physical_layout.total,
                            "YTsaurus table is physically columnar; Arrow rowsets are eligible"
                        );
                    }
                    let schema = parse_schema(
                        self.client
                            .get_json(&super::attribute_path(&table.path, "schema"))
                            .await?,
                    )?;
                    tables.push(DiscoveredTable {
                        config: table.clone(),
                        dataset_name,
                        schema,
                        optimize_for_scan: optimize_for == "scan",
                        physical_layout,
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
    fn compatibility(
        &self,
        _delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::YTsaurus(SourceDescriptor {
            behavior: if self.config.benchmark_discard.is_some() {
                SourceBehavior::BenchmarkDiscard
            } else {
                SourceBehavior::FiniteAppendOnlyRows
            },
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
            let tables = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("YTsaurus discovery cancelled"),
                tables = self.discover_tables() => tables?,
            };
            let system_columns = SYSTEM_COLUMN_KINDS;
            if self.config.benchmark_discard.is_some() {
                let performance_advice = performance_advice(
                    &tables,
                    (!self.config.proxy_role.is_empty()).then_some(self.config.proxy_role.as_str()),
                );
                let datasets = tables
                    .iter()
                    .map(|table| DiscoveredDataset {
                        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                        role: DatasetRole::Main,
                        name: Arc::clone(&table.dataset_name),
                        incoming_schema: DatasetSchema::default(),
                        stored_schema: DatasetSchema::default(),
                        system_columns: Vec::new(),
                    })
                    .collect();
                return Ok(DeliveryDiscovery {
                    source_name: Arc::from("ytsaurus"),
                    source_topology: SourceTopology::StaticPartitions(
                        (0..tables.len())
                            .map(i64::try_from)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                    schema_origin: SchemaOrigin::SourceNative,
                    keep_system_columns: request.keep_system_columns,
                    datasets,
                    performance_advice,
                });
            }
            let performance_advice = performance_advice(
                &tables,
                (!self.config.proxy_role.is_empty()).then_some(self.config.proxy_role.as_str()),
            );
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
                    let (has_physical_system_columns, _) = system_column_layout(&table.schema)?;
                    let mut incoming = table.schema.clone();
                    if !has_physical_system_columns {
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
                        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                        role: DatasetRole::Main,
                        name: Arc::clone(&table.dataset_name),
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
                performance_advice,
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
                memory,
                delivery_type: _,
                phase: _,
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
            let benchmark_discard = self
                .config
                .benchmark_discard
                .as_ref()
                .map(|config| BenchmarkDiscardState::new(config, &table.schema))
                .transpose()?;
            let uses_native_rpc = benchmark_discard.as_ref().is_none_or(|state| {
                state.config.transport == YTsaurusBenchmarkTransport::NativeRpc
            });
            let rpc_endpoints = if uses_native_rpc {
                Some(self.client.discover_rpc_endpoints().await?)
            } else {
                None
            };
            let native_token = uses_native_rpc.then(|| self.client.token().to_owned());
            let dataset_name = Arc::clone(&table.dataset_name);
            let expected_arrow_schema = dataset_arrow_schema(&table.schema);
            let (source_has_system_columns, system_columns) = system_column_layout(&table.schema)?;
            let stream = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("YTsaurus read cancelled"),
                stream = tokio::time::timeout(
                    Duration::from_millis(self.config.stream_open_timeout_ms),
                    open_read_stream(
                        &self.client,
                        &table,
                        benchmark_discard.as_ref(),
                        &self.config.read_ordering,
                        &self.config.table_reader,
                        rpc_endpoints.as_deref(),
                        native_token.as_deref(),
                        0,
                    ),
                ) => stream
                    .map_err(|_| anyhow::anyhow!(
                        "YTsaurus snapshot stream did not open within {} ms",
                        self.config.stream_open_timeout_ms,
                    ))?
                    .map_err(DataPlaneFailure::into_source)?,
            };
            let yt_wire_decoder = YtWireDecoder::new(&table.schema);
            Ok(Box::new(YTsaurusSource {
                table,
                dataset_name,
                expected_arrow_schema,
                source_has_system_columns,
                system_columns,
                partition_id,
                client: self.client.clone(),
                stream,
                decoder: StreamDecoder::new(),
                yt_wire_decoder,
                partition_arrow_decoders: HashMap::new(),
                partition_wire_decoders: HashMap::new(),
                benchmark_discard,
                read_ordering: self.config.read_ordering.clone(),
                table_reader: self.config.table_reader.clone(),
                rpc_endpoints,
                native_token,
                memory,
                queued: VecDeque::new(),
                queued_discard_rows: 0,
                batch_rows: self.config.batch_rows,
                stream_retry_max_attempts: self.config.stream_retry_max_attempts,
                stream_retry_initial: Duration::from_millis(self.config.stream_retry_initial_ms),
                stream_retry_max: Duration::from_millis(self.config.stream_retry_max_ms),
                stream_open_timeout: Duration::from_millis(self.config.stream_open_timeout_ms),
                stream_idle_timeout: Duration::from_millis(self.config.stream_idle_timeout_ms),
                consecutive_stream_failures: 0,
                offset: 0,
                table_offset: 0,
                finished: false,
                counters,
            }) as Box<dyn Source>)
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

struct YTsaurusSource {
    table: DiscoveredTable,
    dataset_name: Arc<str>,
    expected_arrow_schema: Arc<Schema>,
    source_has_system_columns: bool,
    system_columns: SystemColumns,
    partition_id: i64,
    client: YTsaurusClient,
    stream: YTsaurusReadStream,
    decoder: StreamDecoder,
    yt_wire_decoder: YtWireDecoder,
    partition_arrow_decoders: HashMap<usize, StreamDecoder>,
    partition_wire_decoders: HashMap<usize, YtWireDecoder>,
    benchmark_discard: Option<BenchmarkDiscardState>,
    read_ordering: YTsaurusReadOrdering,
    table_reader: super::config::YTsaurusTableReaderConfig,
    rpc_endpoints: Option<Vec<String>>,
    native_token: Option<String>,
    memory: PipelineMemory,
    queued: VecDeque<RecordBatch>,
    queued_discard_rows: u64,
    batch_rows: usize,
    stream_retry_max_attempts: usize,
    stream_retry_initial: Duration,
    stream_retry_max: Duration,
    stream_open_timeout: Duration,
    stream_idle_timeout: Duration,
    consecutive_stream_failures: usize,
    offset: i64,
    table_offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

struct BenchmarkDiscardState {
    config: YTsaurusBenchmarkDiscardConfig,
    output_format: Option<String>,
    decoder: Option<DiscardDecoder>,
    partition_decoders: HashMap<usize, DiscardDecoder>,
    cumulative_rows: u64,
    partition_cumulative_rows: HashMap<usize, u64>,
    wire_bytes: u64,
    last_progress: Instant,
}

impl BenchmarkDiscardState {
    fn new(
        config: &YTsaurusBenchmarkDiscardConfig,
        schema: &DatasetSchema,
    ) -> anyhow::Result<Self> {
        let http = config.transport == YTsaurusBenchmarkTransport::Http;
        Ok(Self {
            config: config.clone(),
            output_format: http
                .then(|| output_format(config.format, schema))
                .transpose()?,
            decoder: http
                .then(|| DiscardDecoder::new(config.format, schema))
                .transpose()?,
            partition_decoders: HashMap::new(),
            cumulative_rows: 0,
            partition_cumulative_rows: HashMap::new(),
            wire_bytes: 0,
            last_progress: Instant::now(),
        })
    }

    fn reset_decoder(&mut self, schema: &DatasetSchema) -> anyhow::Result<()> {
        if self.decoder.is_some() {
            self.decoder = Some(DiscardDecoder::new(self.config.format, schema)?);
        }
        self.partition_decoders.clear();
        self.cumulative_rows = 0;
        self.partition_cumulative_rows.clear();
        Ok(())
    }

    fn add_cumulative_rows(
        &mut self,
        stream_id: Option<usize>,
        cumulative_rows: u64,
    ) -> anyhow::Result<u64> {
        let previous = match stream_id {
            Some(stream_id) => self
                .partition_cumulative_rows
                .insert(stream_id, cumulative_rows)
                .unwrap_or(0),
            None => std::mem::replace(&mut self.cumulative_rows, cumulative_rows),
        };
        cumulative_rows.checked_sub(previous).ok_or_else(|| {
            anyhow::anyhow!(
                "YTsaurus row counter moved backwards from {previous} to {cumulative_rows}"
            )
        })
    }

    fn add_decoded_partition_rows(&mut self, stream_id: usize, rows: u64) -> anyhow::Result<()> {
        let observed = self.partition_cumulative_rows.entry(stream_id).or_default();
        *observed = observed
            .checked_add(rows)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus partition row counter overflow"))?;
        Ok(())
    }
}

async fn read_response(
    client: &YTsaurusClient,
    table: &DiscoveredTable,
    benchmark_discard: Option<&BenchmarkDiscardState>,
    read_ordering: &YTsaurusReadOrdering,
    start_row_index: i64,
) -> anyhow::Result<reqwest::Response> {
    if let Some(benchmark_discard) = benchmark_discard {
        client
            .read_table(
                &table.config.path,
                start_row_index,
                benchmark_discard.output_format.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("native benchmark discard has no HTTP output format")
                })?,
                benchmark_discard.config.unordered,
                &benchmark_discard.config.table_reader,
            )
            .await
    } else {
        client
            .read_arrow(
                &table.config.path,
                start_row_index,
                read_ordering.is_unordered(),
            )
            .await
    }
}

async fn open_read_stream(
    client: &YTsaurusClient,
    table: &DiscoveredTable,
    benchmark_discard: Option<&BenchmarkDiscardState>,
    read_ordering: &YTsaurusReadOrdering,
    table_reader: &super::config::YTsaurusTableReaderConfig,
    rpc_endpoints: Option<&[String]>,
    native_token: Option<&str>,
    start_row_index: i64,
) -> Result<YTsaurusReadStream, DataPlaneFailure> {
    let native_benchmark = benchmark_discard
        .is_some_and(|state| state.config.transport == YTsaurusBenchmarkTransport::NativeRpc);
    if benchmark_discard.is_none() || native_benchmark {
        let endpoints = rpc_endpoints.ok_or_else(|| {
            DataPlaneFailure::fatal(anyhow::anyhow!(
                "native YTsaurus RPC endpoints were not discovered"
            ))
        })?;
        let token = native_token.ok_or_else(|| {
            DataPlaneFailure::fatal(anyhow::anyhow!(
                "native YTsaurus Arrow reader requires an authentication token"
            ))
        })?;
        let requested_format = if let Some(state) = benchmark_discard {
            match state.config.format {
                YTsaurusReadFormat::Arrow => NativeReadFormat::Arrow,
                YTsaurusReadFormat::YtWire => NativeReadFormat::YtWire,
                _ => {
                    return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                        "native YTsaurus benchmark supports only Arrow or YT wire"
                    )));
                }
            }
        } else if table.physical_layout.all_columnar() {
            NativeReadFormat::Arrow
        } else {
            NativeReadFormat::YtWire
        };
        if let Some(partition_config) = read_ordering.partition_tables() {
            if start_row_index != 0 {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "YTsaurus PartitionTables streams cannot resume from row {start_row_index}"
                )));
            }
            return NativePartitionedReadStream::open(
                endpoints,
                token,
                &table.config.path,
                partition_config,
                table_reader,
                requested_format,
                benchmark_discard.is_none() && requested_format == NativeReadFormat::Arrow,
                (benchmark_discard.is_none() && requested_format == NativeReadFormat::YtWire)
                    .then(|| table.schema.clone()),
            )
            .await
            .map(YTsaurusReadStream::Partitioned)
            .map_err(DataPlaneFailure::retryable);
        }
        let stream = NativeReadStream::open(
            endpoints,
            token,
            &table.config.path,
            start_row_index,
            benchmark_discard.map_or_else(
                || read_ordering.is_unordered(),
                |state| state.config.unordered,
            ),
            table_reader,
            requested_format,
        )
        .await
        .map_err(DataPlaneFailure::retryable)?;
        return Ok(YTsaurusReadStream::Native(NativePipelinedReadStream::new(
            stream,
            benchmark_discard.is_none() && requested_format == NativeReadFormat::Arrow,
        )));
    }
    read_response(
        client,
        table,
        benchmark_discard,
        read_ordering,
        start_row_index,
    )
    .await
    .map(|response| YTsaurusReadStream::Http(Box::pin(response.bytes_stream())))
    .map_err(|error| DataPlaneFailure::retryable_or_passthrough(classify_http_failure(error)))
}

impl YTsaurusSource {
    fn decode_discard_bytes(
        &mut self,
        bytes: bytes::Bytes,
        stream_id: Option<usize>,
        cumulative_rows: Option<u64>,
    ) -> anyhow::Result<()> {
        let byte_count = bytes.len() as u64;
        let started = Instant::now();
        let state = self
            .benchmark_discard
            .as_mut()
            .expect("discard decoder exists in benchmark mode");
        let rows = if let Some(cumulative_rows) = cumulative_rows {
            state.add_cumulative_rows(stream_id, cumulative_rows)?
        } else if let Some(stream_id) = stream_id {
            let decoder = match state.partition_decoders.entry(stream_id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
                    DiscardDecoder::new(state.config.format, &self.table.schema)?,
                ),
            };
            let rows = decoder.decode(bytes)?;
            state.add_decoded_partition_rows(stream_id, rows)?;
            rows
        } else {
            state
                .decoder
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("native YT wire discard has no byte decoder"))?
                .decode(bytes)?
        };
        state.wire_bytes = state.wire_bytes.saturating_add(byte_count);
        self.queued_discard_rows = self.queued_discard_rows.saturating_add(rows);
        self.counters.add_network_decode_busy(started.elapsed());
        Ok(())
    }

    fn decode_discard_yt_wire(
        &mut self,
        bytes: Bytes,
        stream_id: Option<usize>,
        cumulative_rows: Option<u64>,
    ) -> anyhow::Result<()> {
        let byte_count = u64::try_from(bytes.len())?;
        let started = Instant::now();
        let state = self
            .benchmark_discard
            .as_mut()
            .expect("discard state exists in benchmark mode");
        let rows = if let Some(cumulative_rows) = cumulative_rows {
            state.add_cumulative_rows(stream_id, cumulative_rows)?
        } else {
            let rows = count_wire_rows(&bytes)?;
            if let Some(stream_id) = stream_id {
                state.add_decoded_partition_rows(stream_id, rows)?;
            }
            rows
        };
        state.wire_bytes = state.wire_bytes.saturating_add(byte_count);
        self.queued_discard_rows = self.queued_discard_rows.saturating_add(rows);
        self.counters.add_network_decode_busy(started.elapsed());
        Ok(())
    }

    fn output_discard_batch(&mut self) -> anyhow::Result<SourceBatch> {
        let rows = self
            .queued_discard_rows
            .min(u64::try_from(self.batch_rows)?);
        self.queued_discard_rows -= rows;
        let rows_usize = usize::try_from(rows)?;
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(rows_usize)),
        )?;
        self.offset = self
            .offset
            .checked_add(i64::try_from(rows)?)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus source offset overflow"))?;
        self.table_offset = self
            .table_offset
            .checked_add(i64::try_from(rows)?)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus table offset overflow"))?;
        self.consecutive_stream_failures = 0;
        self.counters.add_records(rows);
        if let Some(state) = &mut self.benchmark_discard {
            if state.last_progress.elapsed() >= Duration::from_secs(1) {
                tracing::info!(
                    target: "transferia_benchmark",
                    format = state.config.format.name(),
                    rows_read = self.offset,
                    wire_bytes = state.wire_bytes,
                    "YTsaurus benchmark discard progress"
                );
                state.last_progress = Instant::now();
            }
        }
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::clone(&self.dataset_name),
                false,
                batch,
                SystemColumns::default(),
            )],
            source_rows: rows,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }

    fn queue_validated(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        let batch = normalize_read_batch(batch, &self.table.schema, &self.expected_arrow_schema)?;
        for batch in compact_record_batch_chunks(batch, self.batch_rows)? {
            self.queued.push_back(batch);
        }
        Ok(())
    }

    fn decode_bytes(
        &mut self,
        bytes: bytes::Bytes,
        stream_id: Option<usize>,
    ) -> anyhow::Result<()> {
        let started = Instant::now();
        let batches = if let Some(stream_id) = stream_id {
            decode_arrow_bytes(
                self.partition_arrow_decoders.entry(stream_id).or_default(),
                bytes,
            )?
        } else {
            decode_arrow_bytes(&mut self.decoder, bytes)?
        };
        for batch in batches {
            self.queue_validated(batch)?;
        }
        self.counters.add_network_decode_busy(started.elapsed());
        Ok(())
    }

    fn decode_yt_wire(
        &mut self,
        bytes: Bytes,
        name_table_entries: &[String],
        stream_id: Option<usize>,
    ) -> anyhow::Result<()> {
        let started = Instant::now();
        let batch = if let Some(stream_id) = stream_id {
            self.partition_wire_decoders
                .entry(stream_id)
                .or_insert_with(|| YtWireDecoder::new(&self.table.schema))
                .decode(name_table_entries, bytes)?
        } else {
            self.yt_wire_decoder.decode(name_table_entries, bytes)?
        };
        self.queue_validated(batch)?;
        self.counters.add_network_decode_busy(started.elapsed());
        Ok(())
    }

    fn finish_partition_stream(
        &mut self,
        stream_id: usize,
        payload: ReadChunkPayload,
        cumulative_rows: Option<u64>,
    ) -> anyhow::Result<()> {
        if let Some(state) = &mut self.benchmark_discard {
            let mut rows = 0_u64;
            if matches!(payload, ReadChunkPayload::Bytes(_)) {
                if let Some(mut decoder) = state.partition_decoders.remove(&stream_id) {
                    let trailing_rows = decoder.finish()?;
                    state.add_decoded_partition_rows(stream_id, trailing_rows)?;
                    rows = rows.saturating_add(trailing_rows);
                }
            }
            if let Some(cumulative_rows) = cumulative_rows {
                rows = rows
                    .saturating_add(state.add_cumulative_rows(Some(stream_id), cumulative_rows)?);
            }
            self.queued_discard_rows = self.queued_discard_rows.saturating_add(rows);
            return Ok(());
        }
        match payload {
            ReadChunkPayload::RecordBatches(batches) => {
                anyhow::ensure!(
                    batches.is_empty(),
                    "YTsaurus partition ended with unexpected decoded data batches"
                );
            }
            ReadChunkPayload::Bytes(_) => {
                let Some(mut decoder) = self.partition_arrow_decoders.remove(&stream_id) else {
                    anyhow::ensure!(
                        cumulative_rows == Some(0),
                        "YTsaurus partition {stream_id} ended before Arrow data"
                    );
                    return Ok(());
                };
                decoder.finish().map_err(|error| {
                    anyhow::anyhow!(
                        "YTsaurus partition {stream_id} ended with an incomplete Arrow IPC message: {error}"
                    )
                })?;
            }
            ReadChunkPayload::YtWire { .. } => {
                if self.partition_wire_decoders.remove(&stream_id).is_none() {
                    anyhow::ensure!(
                        cumulative_rows == Some(0),
                        "YTsaurus partition {stream_id} ended before YT wire data"
                    );
                }
            }
        }
        Ok(())
    }

    async fn output_batch(&mut self, batch: RecordBatch) -> anyhow::Result<SourceBatch> {
        let rows = batch.num_rows();
        let len_i64 = i64::try_from(rows)?;
        let batch =
            if self.source_has_system_columns {
                batch
            } else {
                let schema = batch.schema();
                let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
                let mut arrays = batch.columns().to_vec();
                fields.extend(SYSTEM_COLUMN_KINDS.iter().map(|kind| {
                    Arc::new(Field::new(kind.default_name(), kind.data_type(), false))
                }));
                arrays.extend([
                    Arc::new(StringArray::from(vec![
                        self.table.config.path.as_str();
                        rows
                    ])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![self.partition_id; rows])) as ArrayRef,
                    Arc::new(Int64Array::from_iter_values(
                        self.offset
                            ..self.offset.checked_add(len_i64).ok_or_else(|| {
                                anyhow::anyhow!("YTsaurus source offset overflow")
                            })?,
                    )) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef,
                ]);
                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?
            };
        let batch_bytes = batch.get_array_memory_size();
        let memory = self.memory.reserve_progress_source(batch_bytes).await;
        self.offset = self
            .offset
            .checked_add(len_i64)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus source offset overflow"))?;
        self.consecutive_stream_failures = 0;
        self.counters.add_records(rows as u64);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::clone(&self.dataset_name),
                false,
                batch,
                self.system_columns.clone(),
            )],
            source_rows: rows as u64,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: vec![memory],
        })
    }

    async fn recover_stream(
        &mut self,
        mut failure: DataPlaneFailure,
    ) -> transferia_core::failure::DataPlaneResult<()> {
        loop {
            if self
                .benchmark_discard
                .as_ref()
                .is_some_and(|state| state.config.unordered)
            {
                return Err(DataPlaneFailure::fatal(failure.into_source().context(
                    "unordered YTsaurus benchmark streams cannot resume without biasing row counts",
                )));
            }
            if self.read_ordering.is_unordered() {
                return Err(DataPlaneFailure::fatal(failure.into_source().context(
                    "unordered YTsaurus streams cannot resume at an exact row; restart the delivery to preserve at-least-once semantics",
                )));
            }
            if !failure.is_retryable() {
                return Err(failure);
            }
            if self.consecutive_stream_failures >= self.stream_retry_max_attempts {
                return Err(DataPlaneFailure::fatal(failure.into_source().context(
                    format!(
                        "YTsaurus snapshot stream could not resume at row {} after {} attempts",
                        self.offset, self.stream_retry_max_attempts
                    ),
                )));
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
                open_read_stream(
                    &self.client,
                    &self.table,
                    self.benchmark_discard.as_ref(),
                    &self.read_ordering,
                    &self.table_reader,
                    self.rpc_endpoints.as_deref(),
                    self.native_token.as_deref(),
                    if self.benchmark_discard.is_some() {
                        self.table_offset
                    } else {
                        self.offset
                    },
                ),
            )
            .await
            {
                Ok(Ok(stream)) => {
                    self.stream = stream;
                    self.decoder = StreamDecoder::new();
                    self.yt_wire_decoder = YtWireDecoder::new(&self.table.schema);
                    if let Some(state) = &mut self.benchmark_discard {
                        state
                            .reset_decoder(&self.table.schema)
                            .map_err(DataPlaneFailure::fatal)?;
                    }
                    self.queued.clear();
                    self.queued_discard_rows = 0;
                    return Ok(());
                }
                Ok(Err(error)) => failure = error,
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
                if self.queued_discard_rows > 0 {
                    return self.output_discard_batch().map_err(DataPlaneFailure::fatal);
                }
                if let Some(batch) = self.queued.pop_front() {
                    return self
                        .output_batch(batch)
                        .await
                        .map_err(DataPlaneFailure::fatal);
                }
                if self.finished {
                    return Ok(SourceBatch::Finished);
                }
                let wait_started = Instant::now();
                let response =
                    tokio::time::timeout(self.stream_idle_timeout, self.stream.next_chunk()).await;
                self.counters.add_response_wait(wait_started.elapsed());
                match response {
                    Ok(Ok(Some(chunk))) => {
                        self.counters.add_network_raw_bytes(chunk.network_raw_bytes);
                        self.counters
                            .add_network_decoded_bytes(chunk.network_decoded_bytes);
                        self.counters
                            .add_network_decode_busy(chunk.network_decode_duration);
                        if chunk.end_of_stream {
                            let stream_id = chunk.stream_id.ok_or_else(|| {
                                DataPlaneFailure::fatal(anyhow::anyhow!(
                                    "YTsaurus partition end marker has no stream id"
                                ))
                            })?;
                            self.finish_partition_stream(
                                stream_id,
                                chunk.payload,
                                chunk.cumulative_rows,
                            )
                            .map_err(DataPlaneFailure::fatal)?;
                            continue;
                        }
                        if self.benchmark_discard.is_some() {
                            match chunk.payload {
                                ReadChunkPayload::RecordBatches(_) => {
                                    return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                                        "YTsaurus reader returned decoded Arrow data in benchmark discard mode"
                                    )));
                                }
                                ReadChunkPayload::Bytes(bytes) => self
                                    .decode_discard_bytes(
                                        bytes,
                                        chunk.stream_id,
                                        chunk.cumulative_rows,
                                    )
                                    .map_err(DataPlaneFailure::fatal)?,
                                ReadChunkPayload::YtWire { bytes, .. } => self
                                    .decode_discard_yt_wire(
                                        bytes,
                                        chunk.stream_id,
                                        chunk.cumulative_rows,
                                    )
                                    .map_err(DataPlaneFailure::fatal)?,
                            }
                        } else {
                            match chunk.payload {
                                ReadChunkPayload::RecordBatches(batches) => {
                                    let started = Instant::now();
                                    for batch in batches {
                                        self.queue_validated(batch)
                                            .map_err(DataPlaneFailure::fatal)?;
                                    }
                                    self.counters.add_network_decode_busy(started.elapsed());
                                }
                                ReadChunkPayload::Bytes(bytes) => {
                                    self.decode_bytes(bytes, chunk.stream_id)
                                        .map_err(DataPlaneFailure::fatal)?;
                                }
                                ReadChunkPayload::YtWire {
                                    bytes,
                                    name_table_entries,
                                } => {
                                    self.decode_yt_wire(
                                        bytes,
                                        &name_table_entries,
                                        chunk.stream_id,
                                    )
                                    .map_err(DataPlaneFailure::fatal)?;
                                }
                            }
                        }
                    }
                    Ok(Err(failure)) => {
                        self.recover_stream(failure).await?;
                    }
                    Ok(Ok(None)) => {
                        if let Some(state) = &mut self.benchmark_discard {
                            let rows = state
                                .decoder
                                .as_mut()
                                .map(DiscardDecoder::finish)
                                .transpose()
                                .map_err(DataPlaneFailure::fatal)?
                                .unwrap_or(0);
                            self.queued_discard_rows =
                                self.queued_discard_rows.saturating_add(rows);
                        } else if self.table.physical_layout.all_columnar() {
                            self.decoder.finish().map_err(|error| {
                                DataPlaneFailure::fatal(anyhow::anyhow!(
                                    "YTsaurus Arrow stream ended with an incomplete IPC message: {error}"
                                ))
                            })?;
                        }
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

impl Drop for YTsaurusSource {
    fn drop(&mut self) {
        if let Some(state) = &self.benchmark_discard {
            tracing::info!(
                target: "transferia_benchmark",
                format = state.config.format.name(),
                rows_read = self.offset,
                wire_bytes = state.wire_bytes,
                "YTsaurus benchmark discard summary"
            );
        }
    }
}

const SYSTEM_COLUMN_KINDS: [SystemColumnKind; 4] = [
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

pub(super) fn system_column_layout(
    schema: &DatasetSchema,
) -> anyhow::Result<(bool, SystemColumns)> {
    let indices = SYSTEM_COLUMN_KINDS
        .iter()
        .map(|kind| {
            schema
                .columns
                .iter()
                .position(|column| column.name == kind.default_name())
        })
        .collect::<Vec<_>>();
    let present_count = indices.iter().flatten().count();
    anyhow::ensure!(
        present_count == 0 || present_count == SYSTEM_COLUMN_KINDS.len(),
        "YTsaurus source schema contains a partial set of reserved system columns"
    );
    let source_has_system_columns = present_count != 0;
    let indices = if source_has_system_columns {
        SYSTEM_COLUMN_KINDS
            .iter()
            .zip(indices)
            .map(|(kind, index)| {
                let index = index.expect("all reserved system columns were counted");
                let column = &schema.columns[index];
                anyhow::ensure!(
                    column.data_type == kind.data_type() && !column.nullable,
                    "YTsaurus system column '{}' must have non-nullable Arrow type {:?}",
                    kind.default_name(),
                    kind.data_type(),
                );
                Ok(index)
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    } else {
        (schema.columns.len()..schema.columns.len() + SYSTEM_COLUMN_KINDS.len()).collect()
    };
    Ok((
        source_has_system_columns,
        SystemColumns::new(
            SYSTEM_COLUMN_KINDS
                .into_iter()
                .zip(indices)
                .map(|(kind, index)| SystemColumn {
                    kind,
                    index,
                    name: Arc::from(kind.default_name()),
                })
                .collect::<Vec<_>>(),
        ),
    ))
}

pub(super) fn normalize_read_batch(
    batch: RecordBatch,
    expected: &DatasetSchema,
    expected_arrow_schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch.num_columns() == expected.columns.len(),
        "YTsaurus read schema has {} columns, discovery declared {}",
        batch.num_columns(),
        expected.columns.len()
    );
    let schema = batch.schema();
    if schema.as_ref() == expected_arrow_schema.as_ref() {
        return Ok(batch);
    }

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
        columns.push(array);
    }
    Ok(RecordBatch::try_new(
        Arc::clone(expected_arrow_schema),
        columns,
    )?)
}

pub(super) fn dataset_arrow_schema(schema: &DatasetSchema) -> Arc<Schema> {
    Arc::new(Schema::new(
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                )
                .with_metadata(column.arrow_metadata())
            })
            .collect::<Vec<_>>(),
    ))
}
