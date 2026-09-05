use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, DurationMicrosecondArray,
    FixedSizeBinaryBuilder, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int64Builder, Int8Array, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use super::super::config::YdbSourceConfig;
use super::super::source::DiscoveredTable;
use super::super::transport::YdbClient;
use super::super::types::{ColumnKind, ColumnPlan};
use super::decoder::{DecodedYdbCdcEvent, YdbCdcDecoder, YdbCdcValue};
use super::setup::{ActiveReplicationSource, PreparedReplication, ReplicationResources};
use super::topic::{TopicBatch, TopicRecord, TopicSession};
use crate::metrics::SourceCounters;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME,
    META_CHANGE_OPERATION, META_LOW_CARDINALITY, META_MAX_LENGTH, META_OLD_KEY_OF,
    META_OLD_VALUE_OF, META_PRIMARY_KEY, META_SYSTEM_ROLE, SYSTEM_ROLE_SOURCE_DATABASE,
    SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    SYSTEM_ROLE_SOURCE_VERSION,
};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset, SchemaOrigin,
    SourceTopology,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::{MemoryReservation, PipelineMemory};
use transferia_core::source::{CommitMarker, Source};
use transferia_core::ChangeOperation;

const YDB_REPLICATION_SYSTEM_COLUMNS: &[SystemColumnKind] = &[
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
    SystemColumnKind::WriteTimestampMs,
    SystemColumnKind::ChangeOperation,
    SystemColumnKind::ChangedColumns,
];
// A YDB changefeed Topic message is exactly one row event. Unlike envelopes which expand one
// source message into multiple rows, its replay identity never depends on delivery batch shape.
const YDB_CHANGEFEED_MESSAGE_INDEX: u64 = 0;

struct SourceMetadataColumn {
    name: &'static str,
    role: &'static str,
    data_type: DataType,
}

const fn source_metadata_columns() -> [SourceMetadataColumn; 5] {
    [
        SourceMetadataColumn {
            name: "_system_source_version",
            role: SYSTEM_ROLE_SOURCE_VERSION,
            data_type: DataType::UInt64,
        },
        SourceMetadataColumn {
            name: "_system_source_database",
            role: SYSTEM_ROLE_SOURCE_DATABASE,
            data_type: DataType::Utf8,
        },
        SourceMetadataColumn {
            name: "_system_source_table",
            role: SYSTEM_ROLE_SOURCE_TABLE,
            data_type: DataType::Utf8,
        },
        SourceMetadataColumn {
            name: "_system_source_transaction_id",
            role: SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
            data_type: DataType::FixedSizeBinary(16),
        },
        SourceMetadataColumn {
            name: "_system_source_timestamp_ms",
            role: SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            data_type: DataType::Int64,
        },
    ]
}

pub(in crate::ydb) fn replication_discovery(
    request: DeliveryDiscoveryRequest,
    resources: &ReplicationResources,
) -> anyhow::Result<DeliveryDiscovery> {
    for table in resources.tables.iter() {
        validate_generated_column_names(table)?;
    }
    let discovered_system_columns = YDB_REPLICATION_SYSTEM_COLUMNS
        .iter()
        .copied()
        .map(Into::into)
        .collect::<Vec<_>>();
    let datasets = resources
        .tables
        .iter()
        .map(|table| {
            let mut incoming_columns = table
                .schema
                .columns
                .iter()
                .cloned()
                .map(|mut column| {
                    column.nullable = true;
                    column
                })
                .collect::<Vec<_>>();
            incoming_columns.extend(
                table
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| old_schema_column(index, column)),
            );
            incoming_columns.extend(source_metadata_columns().map(|column| {
                SchemaColumn::new(
                    column.name.to_owned(),
                    column.data_type,
                    metadata_nullable(column.role),
                )
                .with_system_role(column.role)
            }));
            incoming_columns.extend(YDB_REPLICATION_SYSTEM_COLUMNS.iter().map(|kind| {
                SchemaColumn::new(
                    kind.default_name().to_owned(),
                    kind.data_type(),
                    *kind == SystemColumnKind::WriteTimestampMs,
                )
            }));

            let mut stored_schema = table.schema.clone();
            if request.keep_system_columns {
                stored_schema.columns.extend(
                    YDB_REPLICATION_SYSTEM_COLUMNS
                        .iter()
                        .filter(|kind| {
                            !matches!(
                                kind,
                                SystemColumnKind::ChangeOperation
                                    | SystemColumnKind::ChangedColumns
                            )
                        })
                        .map(|kind| {
                            SchemaColumn::new(
                                kind.default_name().to_owned(),
                                kind.data_type(),
                                *kind == SystemColumnKind::WriteTimestampMs,
                            )
                        }),
                );
            }
            DiscoveredDataset {
                update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                role: DatasetRole::Main,
                name: Arc::from(table.config.name()),
                incoming_schema: DatasetSchema::new(incoming_columns),
                stored_schema,
                system_columns: discovered_system_columns.clone(),
            }
        })
        .collect();
    Ok(DeliveryDiscovery {
        source_name: Arc::from("ydb"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: request.keep_system_columns,
        datasets,
        performance_advice: Vec::new(),
    })
}

fn metadata_nullable(role: &str) -> bool {
    matches!(
        role,
        SYSTEM_ROLE_SOURCE_TRANSACTION_ID | SYSTEM_ROLE_SOURCE_TIMESTAMP_MS
    )
}

fn old_schema_column(index: usize, column: &SchemaColumn) -> SchemaColumn {
    let mut old = SchemaColumn::new(old_value_column_name(index), column.data_type.clone(), true)
        .with_old_value_of(column.name.clone());
    if let Some(extension) = column.arrow_extension_name {
        old = old.with_arrow_extension(extension);
    }
    if let (Some(extension), Some(metadata)) = (
        column.arrow_extension_name,
        column.arrow_extension_metadata.clone(),
    ) {
        old = old.with_arrow_extension_metadata(extension, metadata);
    }
    old
}

const fn empty_batch() -> SourceBatch {
    SourceBatch::Typed {
        tables: Vec::new(),
        source_rows: 0,
        commit_marker: None,
        memory: Vec::new(),
    }
}

fn fatal_connector_error(error: anyhow::Error) -> anyhow::Error {
    DataPlaneFailure::fatal_or_passthrough(error).into()
}

struct ReplicationTable {
    table: DiscoveredTable,
    expected_partition_id: i64,
    decoder: YdbCdcDecoder,
    schema: Arc<Schema>,
}

struct DecodedRecord {
    source_version: u64,
    topic_path: Arc<str>,
    partition_id: i64,
    offset: i64,
    message_index: u64,
    written_at_ms: i64,
    event: DecodedYdbCdcEvent,
}

pub(in crate::ydb) struct YdbReplicationSource {
    session: TopicSession,
    decode_state: Arc<ReplicationDecodeState>,
    cancellation: CancellationToken,
    fence_lost: CancellationToken,
    session_cancellation: CancellationToken,
    counters: Arc<SourceCounters>,
    _cancellation_actor: tokio::task::JoinHandle<()>,
    _active_source: ActiveReplicationSource,
    _prepared: Arc<PreparedReplication>,
}

struct ReplicationDecodeState {
    overlap: bool,
    tables: Vec<ReplicationTable>,
    table_by_topic: HashMap<Arc<str>, usize>,
    database: Arc<str>,
    schema_memory: MemoryReservation,
}

impl YdbReplicationSource {
    pub(in crate::ydb) async fn new(
        config: &YdbSourceConfig,
        prepared: Arc<PreparedReplication>,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
        start_offsets: HashMap<String, i64>,
    ) -> anyhow::Result<Self> {
        let replication = config.replication.as_ref().ok_or_else(|| {
            fatal_connector_error(anyhow::anyhow!("YDB replication configuration is missing"))
        })?;
        let active_source = prepared.claim_source()?;
        prepared
            .validate_resources(config, &cancellation)
            .await
            .map_err(fatal_connector_error)?;
        if prepared.resources.tables.len() != prepared.resources.topics.len()
            || prepared.resources.tables.len() != prepared.resources.topic_partition_ids.len()
        {
            return Err(fatal_connector_error(anyhow::anyhow!(
                "YDB replication preparation has inconsistent table/topic/partition cardinality"
            )));
        }
        for table in prepared.resources.tables.iter() {
            validate_generated_column_names(table).map_err(fatal_connector_error)?;
        }
        let schema_admission = prepared
            .resources
            .tables
            .iter()
            .try_fold(size_of::<Vec<Arc<Schema>>>(), |total, table| {
                total
                    .checked_add(schema_materialization_admission_bytes(table)?)
                    .ok_or_else(|| anyhow::anyhow!("YDB CDC schema admission overflow"))
            })
            .map_err(fatal_connector_error)?;
        let schema_memory = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication schema construction cancelled"),
            () = prepared.fence_lost.cancelled() => anyhow::bail!("YDB replication global Coordination fence was lost before schema construction"),
            reservation = memory.reserve(schema_admission) => reservation,
        };
        let mut tables = Vec::with_capacity(prepared.resources.tables.len());
        let mut table_by_topic = HashMap::with_capacity(prepared.resources.tables.len());
        for (index, (table, topic)) in prepared
            .resources
            .tables
            .iter()
            .zip(prepared.resources.topics.iter())
            .enumerate()
        {
            let expected_partition_id = *prepared
                .resources
                .topic_partition_ids
                .get(index)
                .ok_or_else(|| {
                    fatal_connector_error(anyhow::anyhow!(
                        "YDB replication preparation has no partition identity for topic '{topic}'"
                    ))
                })?;
            let canonical_topic = Arc::<str>::from(canonical_topic_path(topic));
            if table_by_topic
                .insert(Arc::clone(&canonical_topic), index)
                .is_some()
            {
                return Err(fatal_connector_error(anyhow::anyhow!(
                    "YDB replication preparation repeats topic '{topic}'"
                )));
            }
            tables.push(ReplicationTable {
                table: table.clone(),
                expected_partition_id,
                decoder: YdbCdcDecoder::new(
                    Arc::from(table.columns.clone()),
                    replication.max_message_bytes,
                )
                .map_err(fatal_connector_error)?,
                schema: build_table_schema(table).map_err(fatal_connector_error)?,
            });
        }
        let retained_schema_bytes =
            retained_schemas_bytes(&tables).map_err(fatal_connector_error)?;
        let _shrunk = schema_memory.shrink_to(retained_schema_bytes);
        anyhow::ensure!(
            !prepared.fence_lost.is_cancelled(),
            "YDB replication global Coordination fence was lost before source construction"
        );
        let client = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication source construction cancelled"),
            () = prepared.fence_lost.cancelled() => anyhow::bail!("YDB replication global Coordination fence was lost before source construction"),
            client = observe_external_request(
                "ydb",
                "replication_stream_connect",
                YdbClient::connect(&config.connection),
            ) => client?,
        };
        let session_cancellation = CancellationToken::new();
        let overlap = !start_offsets.is_empty();
        let session = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication source construction cancelled"),
            () = prepared.fence_lost.cancelled() => anyhow::bail!("YDB replication global Coordination fence was lost while opening the Topic session"),
            session = TopicSession::connect(
                &client,
                prepared
                    .resources
                    .topics
                    .iter()
                    .cloned()
                    .zip(prepared.resources.topic_partition_ids.iter().copied())
                    .collect(),
                replication.consumer_name.clone(),
                format!("transferia-{}", prepared.delivery_id),
                replication.read_buffer_bytes,
                replication.max_message_bytes,
                replication.max_batch_bytes,
                replication.max_response_bytes,
                replication.commit_timeout(),
                session_cancellation.clone(),
                memory.clone(),
                Arc::clone(&counters),
                start_offsets,
            ) => session.map_err(|error| {
                anyhow::Error::new(DataPlaneFailure::fatal_or_passthrough(error))
            })?,
        };
        let actor_session_cancellation = session_cancellation.clone();
        let actor_delivery_cancellation = cancellation.clone();
        let actor_fence_lost = prepared.fence_lost.clone();
        let cancellation_actor = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = actor_delivery_cancellation.cancelled() => {}
                () = actor_fence_lost.cancelled() => {}
                () = actor_session_cancellation.cancelled() => return,
            }
            actor_session_cancellation.cancel();
        });
        let decode_state = Arc::new(ReplicationDecodeState {
            overlap,
            tables,
            table_by_topic,
            database: Arc::from(config.connection.database.as_str()),
            schema_memory,
        });
        Ok(Self {
            session,
            decode_state,
            cancellation,
            fence_lost: prepared.fence_lost.clone(),
            session_cancellation,
            counters,
            _cancellation_actor: cancellation_actor,
            _active_source: active_source,
            _prepared: prepared,
        })
    }
}

impl ReplicationDecodeState {
    fn decode_batch(&self, batch: TopicBatch) -> anyhow::Result<SourceBatch> {
        let TopicBatch {
            records,
            commit_marker,
            memory: raw_memory,
        } = batch;
        let admission_bytes = decode_and_materialization_admission_bytes(
            &records,
            &self.tables,
            &self.table_by_topic,
            self.database.as_ref(),
        )?;
        let decoded_memory = raw_memory.reserve_source_companion(admission_bytes)?;
        let mut group_lengths = vec![0_usize; self.tables.len()];
        for record in &records {
            let table_index = self
                .table_by_topic
                .get(record.topic_path.as_ref())
                .copied()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "YDB changefeed record belongs to unconfigured topic '{}'",
                        record.topic_path
                    )
                })?;
            anyhow::ensure!(
                record.partition_id == self.tables[table_index].expected_partition_id,
                "YDB changefeed topic '{}' delivered unexpected partition {}, prepared identity requires {}",
                record.topic_path,
                record.partition_id,
                self.tables[table_index].expected_partition_id
            );
            group_lengths[table_index] = group_lengths[table_index]
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("YDB CDC table row count overflow"))?;
        }
        let mut grouped = group_lengths
            .into_iter()
            .map(Vec::with_capacity)
            .collect::<Vec<Vec<DecodedRecord>>>();
        for record in records {
            let table_index = self
                .table_by_topic
                .get(record.topic_path.as_ref())
                .copied()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "YDB changefeed record belongs to unconfigured topic '{}'",
                        record.topic_path
                    )
                })?;
            let event = self.tables[table_index].decoder.decode(&record.payload)?;
            if self.overlap {
                reconcile_overlap(&event)?;
            }
            grouped[table_index].push(DecodedRecord {
                source_version: cdc_row_version(record.offset, self.overlap)?,
                topic_path: record.topic_path,
                partition_id: record.partition_id,
                offset: record.offset,
                message_index: YDB_CHANGEFEED_MESSAGE_INDEX,
                written_at_ms: record.written_at_ms,
                event,
            });
        }
        let mut tables = Vec::with_capacity(grouped.iter().filter(|rows| !rows.is_empty()).count());
        let mut source_rows = 0_u64;
        for (table, rows) in self.tables.iter().zip(&grouped) {
            if rows.is_empty() {
                continue;
            }
            source_rows = source_rows
                .checked_add(u64::try_from(rows.len())?)
                .ok_or_else(|| anyhow::anyhow!("YDB replication source row count overflow"))?;
            tables.push(materialize_table(
                &table.table,
                Arc::clone(&table.schema),
                self.database.as_ref(),
                rows,
            )?);
        }
        anyhow::ensure!(
            source_rows > 0,
            "YDB Topic returned a non-empty batch without materializable records"
        );
        let arrow_bytes = retained_source_batch_bytes(&tables, tables.capacity())?;
        anyhow::ensure!(
            arrow_bytes <= decoded_memory.bytes(),
            "YDB replication Arrow output exceeded its checked source-memory reservation"
        );
        drop(grouped);
        let _shrunk = decoded_memory.shrink_to(arrow_bytes.max(1));
        Ok(SourceBatch::Typed {
            tables,
            source_rows,
            commit_marker: Some(commit_marker),
            memory: vec![raw_memory, decoded_memory, self.schema_memory.clone()],
        })
    }
}

fn blocking_decode_result(
    result: Result<anyhow::Result<SourceBatch>, tokio::task::JoinError>,
) -> transferia_core::failure::DataPlaneResult<SourceBatch> {
    match result {
        Ok(decoded) => decoded.map_err(DataPlaneFailure::fatal_or_passthrough),
        Err(error) => {
            let outcome = if error.is_panic() {
                "panicked"
            } else {
                "was cancelled"
            };
            Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "YDB CDC blocking decode/materialization task {outcome}"
            )))
        }
    }
}

impl Drop for YdbReplicationSource {
    fn drop(&mut self) {
        self.session_cancellation.cancel();
        self._cancellation_actor.abort();
    }
}

impl Source for YdbReplicationSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let batch = tokio::select! {
                biased;
                () = self.fence_lost.cancelled() => {
                    return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                        "YDB replication global Coordination fence was lost"
                    )));
                }
                () = self.cancellation.cancelled() => return Ok(SourceBatch::Finished),
                batch = self.session.read_batch() => {
                    batch.map_err(DataPlaneFailure::fatal_or_passthrough)?
                }
            };
            let Some(batch) = batch else {
                return Ok(empty_batch());
            };
            if self.fence_lost.is_cancelled() {
                return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                    "YDB replication global Coordination fence was lost before batch decoding"
                )));
            }
            let decode_state = Arc::clone(&self.decode_state);
            let started = Instant::now();
            let joined =
                tokio::task::spawn_blocking(move || decode_state.decode_batch(batch)).await;
            self.counters.add_network_decode_busy(started.elapsed());
            if self.fence_lost.is_cancelled() {
                return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                    "YDB replication global Coordination fence was lost during batch decoding"
                )));
            }
            blocking_decode_result(joined)
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                () = self.fence_lost.cancelled() => {
                    Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                        "YDB replication global Coordination fence was lost before offset commit"
                    )))
                }
                () = self.cancellation.cancelled() => {
                    Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                        "YDB replication delivery was cancelled before offset commit"
                    )))
                }
                result = self.session.commit_offsets(markers) => {
                    result.map_err(DataPlaneFailure::fatal_or_passthrough)
                }
            }
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            self.session_cancellation.cancel();
            Ok(())
        })
    }
}

fn decode_and_materialization_admission_bytes(
    records: &[TopicRecord],
    tables: &[ReplicationTable],
    table_by_topic: &HashMap<Arc<str>, usize>,
    database: &str,
) -> anyhow::Result<usize> {
    let mut initial = size_of::<Vec<Vec<DecodedRecord>>>()
        .checked_add(size_of::<Vec<usize>>())
        .and_then(|value| {
            value.checked_add(tables.len().checked_mul(
                size_of::<Vec<DecodedRecord>>() + size_of::<TableData>() + size_of::<usize>(),
            )?)
        })
        .and_then(|value| value.checked_add(3 * size_of::<MemoryReservation>()))
        .ok_or_else(|| anyhow::anyhow!("YDB CDC output container admission overflow"))?;
    for table in tables {
        let field_count = table.schema.fields().len();
        initial = initial
            .checked_add(table.table.config.name().len() + 2 * size_of::<usize>())
            .and_then(|value| {
                value.checked_add(size_of::<RecordBatch>() + size_of::<Arc<Schema>>())
            })
            .and_then(|value| {
                value.checked_add(field_count.checked_mul(
                    size_of::<ArrayRef>()
                        + size_of::<arrow::array::ArrayData>()
                        + 2 * size_of::<usize>(),
                )?)
            })
            .and_then(|value| {
                value.checked_add(
                    YDB_REPLICATION_SYSTEM_COLUMNS
                        .len()
                        .checked_mul(size_of::<SystemColumn>())?,
                )
            })
            .ok_or_else(|| anyhow::anyhow!("YDB CDC output metadata admission overflow"))?;
        for kind in YDB_REPLICATION_SYSTEM_COLUMNS {
            initial = initial
                .checked_add(kind.default_name().len() + 2 * size_of::<usize>())
                .ok_or_else(|| anyhow::anyhow!("YDB CDC system-column name admission overflow"))?;
        }
    }
    records.iter().try_fold(initial.max(1), |total, record| {
        let table_index = table_by_topic
            .get(record.topic_path.as_ref())
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "YDB changefeed record belongs to unconfigured topic '{}'",
                    record.topic_path
                )
            })?;
        let columns = tables[table_index].table.columns.len();
        let decode = tables[table_index]
            .decoder
            .decode_admission_bytes(record.payload.len())?;
        let arrow_value_slots = columns
            .checked_mul(2)
            .and_then(|value| value.checked_mul(size_of::<YdbCdcValue>()))
            .and_then(|value| value.checked_mul(2))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC Arrow value admission overflow"))?;
        // A decoded string/binary byte can be retained once in each of the current and old
        // Arrow projections while the decoder-owned value is still live. The decoder helper
        // separately accounts its serde/raw envelope and decoded row images.
        let arrow_variable_payload = record
            .payload
            .len()
            .checked_mul(2)
            .ok_or_else(|| anyhow::anyhow!("YDB CDC Arrow variable admission overflow"))?;
        let changed_mask = columns.div_ceil(8);
        let metadata = database
            .len()
            .checked_add(tables[table_index].table.config.path.len())
            .and_then(|value| value.checked_add(record.topic_path.len()))
            .and_then(|value| value.checked_add(ChangeOperation::Update.code().len()))
            .and_then(|value| value.checked_add(16))
            .and_then(|value| value.checked_add(6 * size_of::<i64>()))
            .and_then(|value| value.checked_add(5 * size_of::<i32>()))
            .and_then(|value| value.checked_add(changed_mask.checked_mul(2)?))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC metadata memory admission overflow"))?;
        total
            .checked_add(size_of::<DecodedRecord>())
            .and_then(|value| value.checked_add(decode))
            .and_then(|value| value.checked_add(arrow_value_slots))
            .and_then(|value| value.checked_add(arrow_variable_payload))
            .and_then(|value| value.checked_add(metadata))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC peak memory admission overflow"))
    })
}

pub(in crate::ydb) fn build_table_schema(table: &DiscoveredTable) -> anyhow::Result<Arc<Schema>> {
    anyhow::ensure!(
        table.columns.len() == table.schema.columns.len(),
        "YDB CDC physical and discovered schema widths differ for table '{}'",
        table.config.path
    );
    let field_count = table
        .columns
        .len()
        .checked_mul(2)
        .and_then(|value| value.checked_add(source_metadata_columns().len()))
        .and_then(|value| value.checked_add(YDB_REPLICATION_SYSTEM_COLUMNS.len()))
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema column count overflow"))?;
    let mut fields = Vec::with_capacity(field_count);
    for (column, discovered) in table.columns.iter().zip(&table.schema.columns) {
        anyhow::ensure!(
            column.name == discovered.name && column.kind.arrow_type() == discovered.data_type,
            "YDB CDC schema drifted at column '{}'",
            column.name
        );
        let mut incoming = discovered.clone();
        incoming.nullable = true;
        fields.push(
            Field::new(&column.name, discovered.data_type.clone(), true)
                .with_metadata(incoming.arrow_metadata()),
        );
    }
    for (index, discovered) in table.schema.columns.iter().enumerate() {
        let old = old_schema_column(index, discovered);
        let metadata = old.arrow_metadata();
        fields.push(Field::new(old.name, old.data_type, true).with_metadata(metadata));
    }
    fields.extend(source_metadata_columns().map(|column| {
        Field::new(
            column.name,
            column.data_type.clone(),
            metadata_nullable(column.role),
        )
        .with_metadata(
            SchemaColumn::new(
                column.name.to_owned(),
                column.data_type,
                metadata_nullable(column.role),
            )
            .with_system_role(column.role)
            .arrow_metadata(),
        )
    }));
    fields.extend(YDB_REPLICATION_SYSTEM_COLUMNS.iter().map(|kind| {
        let field = Field::new(
            kind.default_name(),
            kind.data_type(),
            *kind == SystemColumnKind::WriteTimestampMs,
        );
        if *kind == SystemColumnKind::ChangeOperation {
            field.with_metadata(HashMap::from([(
                META_CHANGE_OPERATION.to_owned(),
                "true".to_owned(),
            )]))
        } else {
            field
        }
    }));
    Ok(Arc::new(Schema::new(fields)))
}

pub(in crate::ydb) fn schema_materialization_admission_bytes(
    table: &DiscoveredTable,
) -> anyhow::Result<usize> {
    let field_count = table
        .schema
        .columns
        .len()
        .checked_mul(2)
        .and_then(|value| value.checked_add(source_metadata_columns().len()))
        .and_then(|value| value.checked_add(YDB_REPLICATION_SYSTEM_COLUMNS.len()))
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema field count overflow"))?;
    let mut bytes = size_of::<Schema>()
        .checked_add(
            field_count
                .checked_mul(size_of::<Field>() + size_of::<Arc<Field>>())
                .ok_or_else(|| anyhow::anyhow!("YDB CDC schema field memory overflow"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema memory accounting overflow"))?;
    for (index, column) in table.schema.columns.iter().enumerate() {
        let (current_entries, current_payload) = schema_column_metadata_shape(column)?;
        bytes = bytes
            .checked_add(field_materialization_bytes(
                column.name.len(),
                current_entries,
                current_payload,
            )?)
            .ok_or_else(|| anyhow::anyhow!("YDB CDC current field memory overflow"))?;

        let old = old_schema_column(index, column);
        let (old_entries, old_payload) = schema_column_metadata_shape(&old)?;
        bytes = bytes
            .checked_add(field_materialization_bytes(
                old.name.len(),
                old_entries,
                old_payload,
            )?)
            .ok_or_else(|| anyhow::anyhow!("YDB CDC old field memory overflow"))?;
    }
    for column in source_metadata_columns() {
        let payload = META_SYSTEM_ROLE
            .len()
            .checked_add(column.role.len())
            .ok_or_else(|| anyhow::anyhow!("YDB CDC source metadata schema overflow"))?;
        bytes = bytes
            .checked_add(field_materialization_bytes(column.name.len(), 1, payload)?)
            .ok_or_else(|| anyhow::anyhow!("YDB CDC source metadata field overflow"))?;
    }
    for kind in YDB_REPLICATION_SYSTEM_COLUMNS {
        let (entries, payload) = if *kind == SystemColumnKind::ChangeOperation {
            (
                1,
                META_CHANGE_OPERATION
                    .len()
                    .checked_add("true".len())
                    .ok_or_else(|| anyhow::anyhow!("YDB CDC system metadata overflow"))?,
            )
        } else {
            (0, 0)
        };
        bytes = bytes
            .checked_add(field_materialization_bytes(
                kind.default_name().len(),
                entries,
                payload,
            )?)
            .ok_or_else(|| anyhow::anyhow!("YDB CDC system field memory overflow"))?;
    }
    bytes
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema working-set accounting overflow"))
}

fn schema_column_metadata_shape(column: &SchemaColumn) -> anyhow::Result<(usize, usize)> {
    let mut entries = 0_usize;
    let mut payload = 0_usize;
    if column.primary_key {
        add_metadata_shape(&mut entries, &mut payload, META_PRIMARY_KEY, "true".len())?;
    }
    if column.low_cardinality {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_LOW_CARDINALITY,
            "true".len(),
        )?;
    }
    if let Some(max_length) = column.max_length {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_MAX_LENGTH,
            decimal_digit_count(max_length),
        )?;
    }
    if let Some(extension) = column.arrow_extension_name {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_ARROW_EXTENSION_NAME,
            extension.len(),
        )?;
    }
    if let Some(metadata) = &column.arrow_extension_metadata {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_ARROW_EXTENSION_METADATA,
            metadata.len(),
        )?;
    }
    if let Some(role) = &column.system_role {
        add_metadata_shape(&mut entries, &mut payload, META_SYSTEM_ROLE, role.len())?;
    }
    if let Some(current) = &column.old_value_of {
        add_metadata_shape(&mut entries, &mut payload, META_OLD_VALUE_OF, current.len())?;
    }
    if let Some(current) = &column.old_key_of {
        add_metadata_shape(&mut entries, &mut payload, META_OLD_KEY_OF, current.len())?;
    }
    Ok((entries, payload))
}

fn add_metadata_shape(
    entries: &mut usize,
    payload: &mut usize,
    key: &str,
    value_len: usize,
) -> anyhow::Result<()> {
    *entries = entries
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema metadata entry count overflow"))?;
    *payload = payload
        .checked_add(key.len())
        .and_then(|value| value.checked_add(value_len))
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema metadata payload overflow"))?;
    Ok(())
}

fn field_materialization_bytes(
    name_len: usize,
    metadata_entries: usize,
    metadata_payload: usize,
) -> anyhow::Result<usize> {
    name_len
        .checked_mul(2)
        .and_then(|value| value.checked_add(metadata_payload.checked_mul(2)?))
        .and_then(|value| {
            value.checked_add(metadata_entries.checked_mul(4 * size_of::<(String, String)>())?)
        })
        .ok_or_else(|| anyhow::anyhow!("YDB CDC Arrow field materialization overflow"))
}

fn decimal_digit_count(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        usize::try_from(value.ilog10())
            .unwrap_or(usize::MAX)
            .saturating_add(1)
    }
}

fn retained_schemas_bytes(tables: &[ReplicationTable]) -> anyhow::Result<usize> {
    let mut bytes = tables
        .len()
        .checked_mul(size_of::<Arc<Schema>>())
        .ok_or_else(|| anyhow::anyhow!("YDB CDC retained schema vector overflow"))?;
    for table in tables {
        let schema = &table.schema;
        let arc_headers = schema
            .fields()
            .len()
            .checked_add(2)
            .and_then(|arcs| arcs.checked_mul(2 * size_of::<usize>()))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC retained Arrow Arc state overflow"))?;
        bytes = bytes
            .checked_add(size_of::<Schema>())
            .and_then(|value| value.checked_add(schema.fields().size()))
            .and_then(|value| value.checked_add(arc_headers))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC retained Arrow schema overflow"))?;
    }
    Ok(bytes)
}

fn retained_source_batch_bytes(
    tables: &[TableData],
    table_capacity: usize,
) -> anyhow::Result<usize> {
    let mut bytes = table_capacity
        .checked_mul(size_of::<TableData>())
        .and_then(|value| value.checked_add(3 * size_of::<MemoryReservation>()))
        .ok_or_else(|| anyhow::anyhow!("YDB CDC output container accounting overflow"))?;
    for table in tables {
        bytes = bytes
            .checked_add(table.batch.get_array_memory_size())
            .and_then(|value| {
                value.checked_add(
                    table
                        .batch
                        .num_columns()
                        .checked_mul(size_of::<ArrayRef>() + 2 * size_of::<usize>())?,
                )
            })
            .and_then(|value| {
                value.checked_add(table.table.len().checked_add(2 * size_of::<usize>())?)
            })
            .and_then(|value| value.checked_add(size_of::<Arc<Schema>>()))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC Arrow batch memory accounting overflow"))?;
        bytes = bytes
            .checked_add(
                table
                    .system_columns
                    .iter()
                    .len()
                    .checked_mul(size_of::<SystemColumn>())
                    .ok_or_else(|| anyhow::anyhow!("YDB CDC system-column accounting overflow"))?,
            )
            .and_then(|value| value.checked_add(2 * size_of::<usize>()))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC output metadata accounting overflow"))?;
        for column in table.system_columns.iter() {
            bytes = bytes
                .checked_add(
                    column
                        .name
                        .len()
                        .checked_add(2 * size_of::<usize>())
                        .ok_or_else(|| {
                            anyhow::anyhow!("YDB CDC system-column name accounting overflow")
                        })?,
                )
                .ok_or_else(|| anyhow::anyhow!("YDB CDC system-column name accounting overflow"))?;
        }
    }
    Ok(bytes)
}

fn materialize_table(
    table: &DiscoveredTable,
    schema: Arc<Schema>,
    database: &str,
    rows: &[DecodedRecord],
) -> anyhow::Result<TableData> {
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for (index, column) in table.columns.iter().enumerate() {
        arrays.push(cdc_column_array(
            rows,
            column,
            index,
            ValueProjection::Current,
        )?);
    }
    for (index, column) in table.columns.iter().enumerate() {
        arrays.push(cdc_column_array(rows, column, index, ValueProjection::Old)?);
    }
    for metadata in source_metadata_columns() {
        arrays.push(source_metadata_array(metadata.role, database, table, rows)?);
    }
    let system_start = arrays.len();
    for kind in YDB_REPLICATION_SYSTEM_COLUMNS {
        arrays.push(system_array(*kind, rows)?);
    }
    let batch = RecordBatch::try_new(schema, arrays)?;
    let system_columns = YDB_REPLICATION_SYSTEM_COLUMNS
        .iter()
        .enumerate()
        .map(|(offset, kind)| SystemColumn {
            kind: *kind,
            index: system_start + offset,
            name: Arc::from(kind.default_name()),
        })
        .collect::<Vec<_>>();
    Ok(TableData::new(
        Arc::from(table.config.name()),
        false,
        batch,
        SystemColumns::new(system_columns),
    ))
}

#[derive(Clone, Copy)]
enum ValueProjection {
    Current,
    Old,
}

fn projected_value(
    row: &DecodedRecord,
    index: usize,
    projection: ValueProjection,
) -> anyhow::Result<&YdbCdcValue> {
    let values = match projection {
        ValueProjection::Current if row.event.operation == ChangeOperation::Delete => {
            &row.event.old
        }
        ValueProjection::Current => &row.event.current,
        ValueProjection::Old => &row.event.old,
    };
    values.get(index).ok_or_else(|| {
        anyhow::anyhow!(
            "YDB CDC event has {} values for schema column index {index}",
            values.len()
        )
    })
}

fn cdc_column_array(
    rows: &[DecodedRecord],
    column: &ColumnPlan,
    index: usize,
    projection: ValueProjection,
) -> anyhow::Result<ArrayRef> {
    macro_rules! primitive_array {
        ($variant:ident, $native:ty, $array:ty) => {{
            let values = rows
                .iter()
                .map(|row| match projected_value(row, index, projection)? {
                    YdbCdcValue::Absent | YdbCdcValue::Null => Ok(None),
                    YdbCdcValue::$variant(value) => Ok(Some(*value)),
                    _ => anyhow::bail!(
                        "YDB CDC column '{}' does not match its discovered type",
                        column.name
                    ),
                })
                .collect::<anyhow::Result<Vec<Option<$native>>>>()?;
            Arc::new(<$array>::from(values)) as ArrayRef
        }};
    }
    Ok(match &column.kind {
        ColumnKind::Bool => primitive_array!(Bool, bool, BooleanArray),
        ColumnKind::Int8 => primitive_array!(Int8, i8, Int8Array),
        ColumnKind::UInt8 => primitive_array!(UInt8, u8, UInt8Array),
        ColumnKind::Int16 => primitive_array!(Int16, i16, Int16Array),
        ColumnKind::UInt16 => primitive_array!(UInt16, u16, UInt16Array),
        ColumnKind::Int32 => primitive_array!(Int32, i32, Int32Array),
        ColumnKind::UInt32 => primitive_array!(UInt32, u32, UInt32Array),
        ColumnKind::Int64 => primitive_array!(Int64, i64, Int64Array),
        ColumnKind::UInt64 => primitive_array!(UInt64, u64, UInt64Array),
        ColumnKind::Float32 => primitive_array!(Float32, f32, Float32Array),
        ColumnKind::Float64 => primitive_array!(Float64, f64, Float64Array),
        ColumnKind::Date32 => primitive_array!(Date32, i32, Date32Array),
        ColumnKind::TimestampSecond => {
            primitive_array!(TimestampSecond, i64, TimestampSecondArray)
        }
        ColumnKind::TimestampMicrosecond => {
            primitive_array!(TimestampMicrosecond, i64, TimestampMicrosecondArray)
        }
        ColumnKind::DurationMicrosecond => {
            primitive_array!(DurationMicrosecond, i64, DurationMicrosecondArray)
        }
        ColumnKind::Binary(_) => {
            let data_bytes = rows.iter().try_fold(0_usize, |total, row| {
                let bytes = match projected_value(row, index, projection)? {
                    YdbCdcValue::Absent | YdbCdcValue::Null => 0,
                    YdbCdcValue::Binary(value) => value.len(),
                    _ => anyhow::bail!(
                        "YDB CDC column '{}' does not match its discovered type",
                        column.name
                    ),
                };
                total
                    .checked_add(bytes)
                    .ok_or_else(|| anyhow::anyhow!("YDB CDC binary column size overflow"))
            })?;
            let mut builder = BinaryBuilder::with_capacity(rows.len(), data_bytes);
            for row in rows {
                match projected_value(row, index, projection)? {
                    YdbCdcValue::Absent | YdbCdcValue::Null => builder.append_null(),
                    YdbCdcValue::Binary(value) => builder.append_value(value),
                    _ => anyhow::bail!(
                        "YDB CDC column '{}' does not match its discovered type",
                        column.name
                    ),
                }
            }
            Arc::new(builder.finish())
        }
        ColumnKind::Utf8(_) => {
            let data_bytes = rows.iter().try_fold(0_usize, |total, row| {
                let bytes = match projected_value(row, index, projection)? {
                    YdbCdcValue::Absent | YdbCdcValue::Null => 0,
                    YdbCdcValue::Utf8(value) => value.len(),
                    _ => anyhow::bail!(
                        "YDB CDC column '{}' does not match its discovered type",
                        column.name
                    ),
                };
                total
                    .checked_add(bytes)
                    .ok_or_else(|| anyhow::anyhow!("YDB CDC string column size overflow"))
            })?;
            let mut builder = StringBuilder::with_capacity(rows.len(), data_bytes);
            for row in rows {
                match projected_value(row, index, projection)? {
                    YdbCdcValue::Absent | YdbCdcValue::Null => builder.append_null(),
                    YdbCdcValue::Utf8(value) => builder.append_value(value),
                    _ => anyhow::bail!(
                        "YDB CDC column '{}' does not match its discovered type",
                        column.name
                    ),
                }
            }
            Arc::new(builder.finish())
        }
        ColumnKind::Decimal { .. } => anyhow::bail!(
            "YDB CDC column '{}' is Decimal, which replication rejects during discovery",
            column.name
        ),
        ColumnKind::Uuid => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                match projected_value(row, index, projection)? {
                    YdbCdcValue::Absent | YdbCdcValue::Null => builder.append_null(),
                    YdbCdcValue::Uuid(value) => builder.append_value(value)?,
                    _ => anyhow::bail!(
                        "YDB CDC column '{}' does not match its discovered type",
                        column.name
                    ),
                }
            }
            Arc::new(builder.finish())
        }
    })
}

fn source_metadata_array(
    role: &str,
    database: &str,
    table: &DiscoveredTable,
    rows: &[DecodedRecord],
) -> anyhow::Result<ArrayRef> {
    Ok(match role {
        SYSTEM_ROLE_SOURCE_VERSION => Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.source_version)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        SYSTEM_ROLE_SOURCE_DATABASE => {
            Arc::new(StringArray::from(vec![database; rows.len()])) as ArrayRef
        }
        SYSTEM_ROLE_SOURCE_TABLE => Arc::new(StringArray::from(vec![
            table.config.path.as_str();
            rows.len()
        ])) as ArrayRef,
        SYSTEM_ROLE_SOURCE_TRANSACTION_ID => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                builder.append_value(row.event.transaction.as_bytes())?;
            }
            Arc::new(builder.finish())
        }
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                builder.append_value(i64::try_from(row.event.transaction.step()).map_err(
                    |_| {
                        anyhow::anyhow!(
                            "YDB CDC transaction step {} does not fit source.timestamp_ms",
                            row.event.transaction.step()
                        )
                    },
                )?);
            }
            Arc::new(builder.finish())
        }
        _ => anyhow::bail!("unknown YDB source metadata role '{role}'"),
    })
}

fn cdc_row_version(offset: i64, overlap: bool) -> anyhow::Result<u64> {
    // Version zero belongs to the snapshot; preserve the actual cursor in Offset.
    Ok(u64::try_from(offset)? + u64::from(overlap))
}

fn reconcile_overlap(event: &DecodedYdbCdcEvent) -> anyhow::Result<()> {
    if event.operation == ChangeOperation::Update {
        anyhow::ensure!(
            event
                .current
                .iter()
                .enumerate()
                .all(|(index, value)| !matches!(value, YdbCdcValue::Absent)
                    && event
                        .changed_columns
                        .get(index / 8)
                        .is_some_and(|mask| mask & (1 << (index % 8)) != 0)),
            "YDB overlap upsert requires a complete current row and changed-column mask"
        );
        // Preserve the original UPDATE and before-image. Discovery selects
        // full-image upsert application for state sinks in overlap mode only.
    }
    Ok(())
}

fn system_array(kind: SystemColumnKind, rows: &[DecodedRecord]) -> anyhow::Result<ArrayRef> {
    Ok(match kind {
        SystemColumnKind::Topic => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.topic_path.as_ref()),
        )) as ArrayRef,
        SystemColumnKind::Partition => Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.partition_id),
        )),
        SystemColumnKind::Offset => Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.offset),
        )),
        SystemColumnKind::MessageIndex => Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.message_index),
        )),
        SystemColumnKind::WriteTimestampMs => Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.written_at_ms),
        )),
        SystemColumnKind::ChangeOperation => Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.event.operation.code()),
        )),
        SystemColumnKind::ChangedColumns => {
            let data_bytes = rows.iter().try_fold(0_usize, |total, row| {
                total
                    .checked_add(row.event.changed_columns.len())
                    .ok_or_else(|| anyhow::anyhow!("YDB CDC changed-column mask size overflow"))
            })?;
            let mut builder = BinaryBuilder::with_capacity(rows.len(), data_bytes);
            for row in rows {
                builder.append_value(&row.event.changed_columns);
            }
            Arc::new(builder.finish())
        }
    })
}

fn old_value_column_name(index: usize) -> String {
    format!("_system_old_value_{index}")
}

fn validate_generated_column_names(table: &DiscoveredTable) -> anyhow::Result<()> {
    let generated_count = table
        .schema
        .columns
        .len()
        .checked_mul(2)
        .and_then(|value| value.checked_add(source_metadata_columns().len()))
        .and_then(|value| value.checked_add(YDB_REPLICATION_SYSTEM_COLUMNS.len()))
        .ok_or_else(|| anyhow::anyhow!("YDB CDC schema column count overflow"))?;
    let mut names = HashSet::with_capacity(generated_count);
    for column in &table.schema.columns {
        anyhow::ensure!(
            names.insert(column.name.clone()),
            "YDB CDC table '{}' repeats user column '{}'",
            table.config.path,
            column.name
        );
    }
    for index in 0..table.schema.columns.len() {
        let name = old_value_column_name(index);
        anyhow::ensure!(
            names.insert(name.clone()),
            "YDB CDC table '{}' user columns collide with generated old-value column '{name}'",
            table.config.path
        );
    }
    for metadata in source_metadata_columns() {
        anyhow::ensure!(
            names.insert(metadata.name.to_owned()),
            "YDB CDC table '{}' user columns collide with generated source metadata column '{}'",
            table.config.path,
            metadata.name
        );
    }
    for kind in YDB_REPLICATION_SYSTEM_COLUMNS {
        anyhow::ensure!(
            names.insert(kind.default_name().to_owned()),
            "YDB CDC table '{}' user columns collide with generated system column '{}'",
            table.config.path,
            kind.default_name()
        );
    }
    Ok(())
}

fn canonical_topic_path(path: &str) -> &str {
    path.strip_prefix('/').unwrap_or(path)
}

#[cfg(test)]
#[path = "tests/source.rs"]
mod tests;
