use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

use super::config::{LogicalDecoder, PostgresReplicationConfig};
use super::event::{ChangeEvent, LogicalValue, OldValuesKind};
use super::pgoutput::{PgOutputDecoder, PgOutputEvent};
use super::slot_recovery::{advance_slot, ReplicationSlotTracker};
use super::wal2json;
use crate::connectors::postgres::src_batch::{
    old_key_column_name, old_value_column_name, DiscoveredTable,
};
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{META_CHANGE_OPERATION, META_OLD_KEY_OF, META_OLD_VALUE_OF};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};
use transferia_core::ChangeOperation;
use transferia_registry::durable::DurableContext;

pub(crate) struct PostgresReplicationSource {
    client: Client,
    config: PostgresReplicationConfig,
    tables: HashMap<(Arc<str>, Arc<str>), DiscoveredTable>,
    pgoutput: PgOutputDecoder,
    pending: VecDeque<QueuedBatch>,
    last_peek_lsn: u64,
    committed_lsn: u64,
    counters: Arc<SourceCounters>,
    cancellation: CancellationToken,
    slot_tracker: ReplicationSlotTracker,
}

struct QueuedBatch {
    tables: Vec<TableData>,
    rows: u64,
    lsn: u64,
}

#[derive(Debug)]
struct ReplicationMarker {
    lsn: u64,
}

impl PostgresReplicationSource {
    pub(crate) async fn new(
        client: Client,
        config: PostgresReplicationConfig,
        tables: Vec<DiscoveredTable>,
        counters: Arc<SourceCounters>,
        cancellation: CancellationToken,
        durable: DurableContext,
    ) -> anyhow::Result<Self> {
        let (slot_tracker, committed_lsn) =
            ReplicationSlotTracker::prepare(&client, &config, durable).await?;
        let tables = tables
            .into_iter()
            .map(|table| {
                (
                    (
                        Arc::from(table.config.schema.as_str()),
                        Arc::from(table.config.name.as_str()),
                    ),
                    table,
                )
            })
            .collect();
        Ok(Self {
            client,
            config,
            tables,
            pgoutput: PgOutputDecoder::default(),
            pending: VecDeque::new(),
            last_peek_lsn: committed_lsn,
            committed_lsn,
            counters,
            cancellation,
            slot_tracker,
        })
    }

    async fn refill(&mut self) -> anyhow::Result<()> {
        if self.last_peek_lsn > self.committed_lsn {
            return Ok(());
        }
        let limit = i32::try_from(self.config.max_changes)?;
        let rows = match &self.config.decoder {
            LogicalDecoder::Pgoutput { publication } => {
                self.client
                    .query(
                        "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)",
                        &[&self.config.slot, &limit, publication],
                    )
                    .await?
            }
            LogicalDecoder::Wal2Json => {
                self.client
                    .query(
                        "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'include-lsn', '1', 'include-timestamp', '1', 'include-types', '1', 'include-xids', '1', 'include-type-oids', '1')",
                        &[&self.config.slot, &limit],
                    )
                    .await?
            }
        };
        if rows.is_empty() {
            return Ok(());
        }
        match self.config.decoder {
            LogicalDecoder::Pgoutput { .. } => {
                for row in rows {
                    let data: Vec<u8> = row.get(2);
                    let decoded = self.pgoutput.decode(&data)?;
                    if let Some(end_lsn) = decoded.first().map(|event| event.event.lsn) {
                        let events = decoded
                            .into_iter()
                            .map(|event| self.normalize_pgoutput(event))
                            .collect::<anyhow::Result<Vec<_>>>()?
                            .into_iter()
                            .flatten()
                            .collect();
                        self.enqueue_transaction(events, end_lsn)?;
                    }
                }
            }
            LogicalDecoder::Wal2Json => {
                for row in rows {
                    let data: Vec<u8> = row.get(2);
                    let transaction = wal2json::decode(&data)?;
                    let events = transaction
                        .events
                        .into_iter()
                        .map(|event| self.normalize_wal2json(event))
                        .collect::<anyhow::Result<Vec<_>>>()?
                        .into_iter()
                        .flatten()
                        .collect();
                    self.enqueue_transaction(events, transaction.end_lsn)?;
                }
            }
        }
        Ok(())
    }

    fn normalize_pgoutput(
        &self,
        decoded: PgOutputEvent,
    ) -> anyhow::Result<Option<ChangeEvent>> {
        let Some(table) = self.tables.get(&(
            Arc::clone(&decoded.event.schema),
            Arc::clone(&decoded.event.table),
        )) else {
            return Ok(None);
        };
        normalize_pgoutput_event(table, decoded).map(Some)
    }

    fn normalize_wal2json(
        &self,
        decoded: wal2json::Wal2JsonEvent,
    ) -> anyhow::Result<Option<ChangeEvent>> {
        let Some(table) = self
            .tables
            .get(&(Arc::clone(&decoded.event.schema), Arc::clone(&decoded.event.table)))
        else {
            return Ok(None);
        };
        normalize_wal2json_event(table, decoded).map(Some)
    }

    fn enqueue_transaction(&mut self, events: Vec<ChangeEvent>, lsn: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            events.iter().all(|event| event.lsn == lsn),
            "PostgreSQL transaction contains multiple commit LSNs"
        );
        let mut by_table: BTreeMap<(Arc<str>, Arc<str>), Vec<ChangeEvent>> = BTreeMap::new();
        for event in events {
            if self.tables.contains_key(&(Arc::clone(&event.schema), Arc::clone(&event.table))) {
                by_table
                    .entry((Arc::clone(&event.schema), Arc::clone(&event.table)))
                    .or_default()
                    .push(event);
            }
        }
        let mut tables = Vec::with_capacity(by_table.len());
        let mut row_count = 0_u64;
        for (key, events) in by_table {
            let table = self
                .tables
                .get(&key)
                .ok_or_else(|| anyhow::anyhow!("configured PostgreSQL table disappeared"))?;
            row_count = row_count
                .checked_add(u64::try_from(events.len())?)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL CDC row count overflow"))?;
            tables.push(events_to_table_data(table, &events)?);
        }
        self.last_peek_lsn = self.last_peek_lsn.max(lsn);
        self.pending.push_back(QueuedBatch {
            tables,
            rows: row_count,
            lsn,
        });
        Ok(())
    }
}

pub(super) fn normalize_pgoutput_event(
    table: &DiscoveredTable,
    mut decoded: PgOutputEvent,
) -> anyhow::Result<ChangeEvent> {
    anyhow::ensure!(
        (decoded.relation.replica_identity == b'f') == table.replica_identity_full,
        "pgoutput relation '{}.{}' replica identity changed after discovery",
        decoded.event.schema,
        decoded.event.table,
    );
    anyhow::ensure!(
        decoded.relation.columns.len() == table.schema.columns.len(),
        "pgoutput relation '{}.{}' has {} columns, discovery declared {}",
        decoded.event.schema,
        decoded.event.table,
        decoded.relation.columns.len(),
        table.schema.columns.len()
    );
    for (index, ((actual, expected), expected_oid)) in decoded
        .relation
        .columns
        .iter()
        .zip(&table.schema.columns)
        .zip(&table.type_oids)
        .enumerate()
    {
        anyhow::ensure!(
            actual.name.as_ref() == expected.name && actual.type_oid == *expected_oid,
            "pgoutput relation '{}.{}' column {index} metadata changed: received '{}' OID {}, expected '{}' OID {}",
            decoded.event.schema,
            decoded.event.table,
            actual.name,
            actual.type_oid,
            expected.name,
            expected_oid
        );
    }
    if decoded.event.old_values_kind == Some(OldValuesKind::Key) {
        if let Some(old_values) = &mut decoded.event.old_values {
            for (value, column) in old_values.iter_mut().zip(decoded.relation.columns.iter()) {
                if !column.key {
                    *value = LogicalValue::Null;
                }
            }
        }
    }
    Ok(decoded.event)
}

pub(super) fn normalize_wal2json_event(
    table: &DiscoveredTable,
    mut decoded: wal2json::Wal2JsonEvent,
) -> anyhow::Result<ChangeEvent> {
    let mut positions = HashMap::with_capacity(decoded.column_names.len());
    let expected = table
        .schema
        .columns
        .iter()
        .zip(&table.type_oids)
        .map(|(column, oid)| (column.name.as_str(), *oid))
        .collect::<HashMap<_, _>>();
    for (index, (name, oid)) in decoded
        .column_names
        .iter()
        .zip(&decoded.column_type_oids)
        .enumerate()
    {
        anyhow::ensure!(
            positions.insert(name.as_str(), index).is_none(),
            "wal2json repeats column '{name}'"
        );
        anyhow::ensure!(
            expected.get(name.as_str()) == Some(oid),
            "wal2json column '{name}' OID {oid} does not match discovery"
        );
    }
    let values = table
        .schema
        .columns
        .iter()
        .map(|column| {
            positions.get(column.name.as_str()).map_or_else(
                || match decoded.event.operation {
                    transferia_core::ChangeOperation::Create => anyhow::bail!(
                        "wal2json INSERT omits column '{}'",
                        column.name
                    ),
                    transferia_core::ChangeOperation::Update => {
                        Ok(LogicalValue::UnchangedToast)
                    }
                    transferia_core::ChangeOperation::Delete => Ok(LogicalValue::Null),
                    transferia_core::ChangeOperation::SnapshotRead => anyhow::bail!(
                        "wal2json cannot emit snapshot-read events"
                    ),
                },
                |index| Ok(decoded.event.values[*index].clone()),
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let old_values = decoded
        .event
        .old_values
        .take()
        .map(|old| {
            for (name, oid) in decoded
                .old_key_names
                .iter()
                .zip(&decoded.old_key_type_oids)
            {
                anyhow::ensure!(
                    expected.get(name.as_str()) == Some(oid),
                    "wal2json old-key column '{name}' OID {oid} does not match discovery"
                );
            }
            let by_name = decoded
                .old_key_names
                .iter()
                .zip(old)
                .collect::<HashMap<_, _>>();
            Ok(table
                .schema
                .columns
                .iter()
                .map(|column| {
                    by_name
                        .get(&column.name)
                        .cloned()
                        .unwrap_or(LogicalValue::Null)
                })
                .collect::<Vec<_>>())
        })
        .transpose()?;
    decoded.event.old_values_kind = old_values.as_ref().map(|_| {
        if decoded.old_key_names.len() == table.schema.columns.len() {
            OldValuesKind::Full
        } else {
            OldValuesKind::Key
        }
    });
    Ok(ChangeEvent {
        values,
        old_values,
        ..decoded.event
    })
}

impl Source for PostgresReplicationSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some(batch) = self.pending.pop_front() {
                    self.counters.add_records(batch.rows);
                    self.counters.add_network_decoded_bytes(
                        batch
                            .tables
                            .iter()
                            .map(|table| table.batch.get_array_memory_size() as u64)
                            .sum(),
                    );
                    return Ok(SourceBatch::Typed {
                        tables: batch.tables,
                        source_rows: batch.rows,
                        commit_marker: Some(CommitMarker::new(ReplicationMarker { lsn: batch.lsn })),
                        memory: Vec::new(),
                    });
                }
                self.refill().await.map_err(DataPlaneFailure::retryable)?;
                if !self.pending.is_empty() {
                    continue;
                }
                if self.last_peek_lsn > self.committed_lsn {
                    return Ok(empty_batch());
                }
                tokio::select! {
                    () = self.cancellation.cancelled() => return Ok(SourceBatch::Finished),
                    () = tokio::time::sleep(Duration::from_millis(self.config.poll_interval_ms)) => {}
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let lsn = markers
                .iter()
                .map(|marker| marker.value::<ReplicationMarker>().map(|marker| marker.lsn))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| DataPlaneFailure::fatal(error.into()))?
                .into_iter()
                .max()
                .ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "PostgreSQL replication commit has no markers"
                    ))
                })?;
            if lsn < self.committed_lsn {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "PostgreSQL replication commit LSN moved backwards"
                )));
            }
            self.slot_tracker
                .store(lsn)
                .await
                .map_err(DataPlaneFailure::fatal)?;
            advance_slot(&self.client, &self.config.slot, lsn)
                .await
                .map_err(DataPlaneFailure::retryable)?;
            self.committed_lsn = lsn;
            Ok::<(), DataPlaneFailure>(())
        })
    }
}

fn empty_batch() -> SourceBatch {
    SourceBatch::Typed {
        tables: Vec::new(),
        source_rows: 0,
        commit_marker: None,
        memory: Vec::new(),
    }
}

pub(super) fn events_to_table_data(
    table: &DiscoveredTable,
    events: &[ChangeEvent],
) -> anyhow::Result<TableData> {
    validate_old_values(table, events)?;
    let old_columns = if table.replica_identity_full {
        table.schema.columns.len()
    } else {
        table
            .schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .count()
    };
    let mut fields = Vec::with_capacity(table.schema.columns.len() + old_columns + 6);
    let mut arrays = Vec::with_capacity(table.schema.columns.len() + old_columns + 6);
    for (index, column) in table.schema.columns.iter().enumerate() {
        fields.push(
            Field::new(&column.name, column.data_type.clone(), true)
                .with_metadata(column.arrow_metadata()),
        );
        arrays.push(logical_array(
            events,
            index,
            &column.data_type,
            LogicalProjection::Current {
                old_fallback: table.replica_identity_full,
            },
        )?);
    }
    if table.replica_identity_full {
        for (index, column) in table.schema.columns.iter().enumerate() {
            fields.push(
                Field::new(old_value_column_name(index), column.data_type.clone(), true)
                    .with_metadata(HashMap::from([(
                        META_OLD_VALUE_OF.to_owned(),
                        column.name.clone(),
                    )])),
            );
            arrays.push(logical_array(
                events,
                index,
                &column.data_type,
                LogicalProjection::Old,
            )?);
        }
    } else {
        for (index, column) in table
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
        {
            fields.push(
                Field::new(old_key_column_name(index), column.data_type.clone(), true)
                    .with_metadata(HashMap::from([(
                        META_OLD_KEY_OF.to_owned(),
                        column.name.clone(),
                    )])),
            );
            arrays.push(logical_array(
                events,
                index,
                &column.data_type,
                LogicalProjection::OldKey,
            )?);
        }
    }
    let routing_index = fields.len();
    let routing = [
        (SystemColumnKind::Topic, DataType::Utf8),
        (SystemColumnKind::Partition, DataType::Int64),
        (SystemColumnKind::Offset, DataType::Int64),
        (SystemColumnKind::MessageIndex, DataType::UInt64),
    ];
    for (kind, data_type) in routing {
        fields.push(Field::new(kind.default_name(), data_type, false));
    }
    arrays.extend([
        Arc::new(StringArray::from(vec!["postgres"; events.len()])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0_i64; events.len()])) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            events
                .iter()
                .map(|event| i64::try_from(event.lsn))
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        Arc::new(UInt64Array::from_iter_values(
            (0..events.len()).map(u64::try_from).collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef,
    ]);
    let operation_index = fields.len();
    fields.push(
        Field::new(
            SystemColumnKind::ChangeOperation.default_name(),
            DataType::Utf8,
            false,
        )
        .with_metadata(HashMap::from([(
            META_CHANGE_OPERATION.to_owned(),
            "true".to_owned(),
        )])),
    );
    arrays.push(Arc::new(StringArray::from_iter_values(
        events.iter().map(|event| event.operation.code()),
    )) as ArrayRef);
    let changed_columns_index = fields.len();
    fields.push(Field::new(
        SystemColumnKind::ChangedColumns.default_name(),
        DataType::Binary,
        false,
    ));
    let changed_columns = events
        .iter()
        .map(|event| changed_columns_mask(table, event))
        .collect::<anyhow::Result<Vec<_>>>()?;
    arrays.push(Arc::new(BinaryArray::from_iter_values(
        changed_columns.iter().map(Vec::as_slice),
    )) as ArrayRef);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    Ok(TableData::new(
        Arc::from(table.config.name.as_str()),
        false,
        batch,
        SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::Topic,
                index: routing_index,
                name: Arc::from(SystemColumnKind::Topic.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::Partition,
                index: routing_index + 1,
                name: Arc::from(SystemColumnKind::Partition.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::Offset,
                index: routing_index + 2,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::MessageIndex,
                index: routing_index + 3,
                name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                index: operation_index,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangedColumns,
                index: changed_columns_index,
                name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
            },
        ]),
    ))
}

fn changed_columns_mask(
    table: &DiscoveredTable,
    event: &ChangeEvent,
) -> anyhow::Result<Vec<u8>> {
    let mut mask = vec![0_u8; table.schema.columns.len().div_ceil(8)];
    for (index, column) in table.schema.columns.iter().enumerate() {
        let changed = match event.operation {
            transferia_core::ChangeOperation::Create
            | transferia_core::ChangeOperation::SnapshotRead => {
                anyhow::ensure!(
                    !matches!(event.values[index], LogicalValue::UnchangedToast),
                    "PostgreSQL insert marks column '{}' as unchanged TOAST",
                    column.name
                );
                true
            }
            transferia_core::ChangeOperation::Update => {
                table.replica_identity_full
                    || !matches!(event.values[index], LogicalValue::UnchangedToast)
            }
            transferia_core::ChangeOperation::Delete => column.primary_key,
        };
        if changed {
            mask[index / 8] |= 1 << (index % 8);
        }
    }
    Ok(mask)
}

fn logical_array(
    events: &[ChangeEvent],
    index: usize,
    data_type: &DataType,
    projection: LogicalProjection,
) -> anyhow::Result<ArrayRef> {
    macro_rules! parsed {
        ($ty:ty, $array:ty) => {{
            Arc::new(<$array>::from(
                events
                    .iter()
                    .map(|event| parse_value::<$ty>(event_value(event, index, projection)))
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )) as ArrayRef
        }};
    }
    Ok(match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(
            events
                .iter()
                .map(|event| parse_bool(event_value(event, index, projection)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(
            events
                .iter()
                .map(|event| parse_postgres_char(event_value(event, index, projection)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )) as ArrayRef,
        DataType::Int16 => parsed!(i16, Int16Array),
        DataType::Int32 => parsed!(i32, Int32Array),
        DataType::Int64 => parsed!(i64, Int64Array),
        DataType::UInt32 => parsed!(u32, UInt32Array),
        DataType::Float32 => parsed!(f32, Float32Array),
        DataType::Float64 => parsed!(f64, Float64Array),
        DataType::Binary => Arc::new(BinaryArray::from_iter(
            events
                .iter()
                .map(|event| parse_binary(event_value(event, index, projection)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )) as ArrayRef,
        DataType::Utf8 => Arc::new(StringArray::from_iter(
            events
                .iter()
                .map(|event| parse_text(event_value(event, index, projection)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )) as ArrayRef,
        other => anyhow::bail!("unsupported PostgreSQL CDC Arrow type {other:?}"),
    })
}

#[derive(Clone, Copy)]
enum LogicalProjection {
    Current { old_fallback: bool },
    Old,
    OldKey,
}

static NULL_LOGICAL_VALUE: LogicalValue = LogicalValue::Null;

fn event_value(
    event: &ChangeEvent,
    index: usize,
    projection: LogicalProjection,
) -> &LogicalValue {
    match projection {
        LogicalProjection::Old => event
            .old_values
            .as_ref()
            .map_or(&NULL_LOGICAL_VALUE, |values| &values[index]),
        LogicalProjection::OldKey => match event.operation {
            ChangeOperation::Create | ChangeOperation::SnapshotRead => &NULL_LOGICAL_VALUE,
            ChangeOperation::Update => event
                .old_values
                .as_ref()
                .map_or(&event.values[index], |values| &values[index]),
            ChangeOperation::Delete => event
                .old_values
                .as_ref()
                .map_or(&NULL_LOGICAL_VALUE, |values| &values[index]),
        },
        LogicalProjection::Current { old_fallback } => {
            if event.operation == ChangeOperation::Delete {
                if let Some(old_values) = &event.old_values {
                    return &old_values[index];
                }
            }
            if old_fallback && matches!(event.values[index], LogicalValue::UnchangedToast) {
                if let Some(old_values) = &event.old_values {
                    return &old_values[index];
                }
            }
            &event.values[index]
        }
    }
}

fn validate_old_values(table: &DiscoveredTable, events: &[ChangeEvent]) -> anyhow::Result<()> {
    for (row, event) in events.iter().enumerate() {
        let requires_old = matches!(
            event.operation,
            ChangeOperation::Update | ChangeOperation::Delete
        );
        if table.replica_identity_full {
            anyhow::ensure!(
                !requires_old
                    || (event.old_values_kind == Some(OldValuesKind::Full)
                        && event
                            .old_values
                            .as_ref()
                            .is_some_and(|values| {
                                values.len() == table.schema.columns.len()
                                    && values
                                        .iter()
                                        .all(|value| !matches!(value, LogicalValue::UnchangedToast))
                            })),
                "PostgreSQL REPLICA IDENTITY FULL row {row} has no complete old tuple",
            );
        } else if requires_old {
            for (index, column) in table.schema.columns.iter().enumerate() {
                if !column.primary_key {
                    continue;
                }
                let old_key = event.old_values.as_ref().map(|values| &values[index]);
                match event.operation {
                    ChangeOperation::Update => anyhow::ensure!(
                        old_key.is_some_and(|value| !matches!(value, LogicalValue::Null))
                            || !matches!(event.values[index], LogicalValue::Null),
                        "PostgreSQL UPDATE row {row} has no old or current primary-key value for '{}'",
                        column.name,
                    ),
                    ChangeOperation::Delete => anyhow::ensure!(
                        old_key.is_some_and(|value| !matches!(value, LogicalValue::Null)),
                        "PostgreSQL DELETE row {row} has no old primary-key value for '{}'",
                        column.name,
                    ),
                    ChangeOperation::Create | ChangeOperation::SnapshotRead => unreachable!(),
                }
            }
        }
    }
    Ok(())
}

fn parse_text(value: &LogicalValue) -> anyhow::Result<Option<&str>> {
    match value {
        LogicalValue::Null => Ok(None),
        LogicalValue::Text(value) => Ok(Some(std::str::from_utf8(value)?)),
        LogicalValue::Binary(_) => {
            anyhow::bail!("binary pgoutput value cannot populate an Utf8 column")
        }
        LogicalValue::UnchangedToast => Ok(None),
    }
}

fn parse_binary(value: &LogicalValue) -> anyhow::Result<Option<Vec<u8>>> {
    match value {
        LogicalValue::Null => Ok(None),
        LogicalValue::Binary(value) => Ok(Some(value.to_vec())),
        LogicalValue::Text(value) => {
            let value = std::str::from_utf8(value)?;
            let hex = value
                .strip_prefix("\\x")
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL bytea text is not hexadecimal"))?;
            anyhow::ensure!(
                hex.len() % 2 == 0,
                "PostgreSQL bytea hexadecimal length is odd"
            );
            (0..hex.len())
                .step_by(2)
                .map(|index| {
                    u8::from_str_radix(&hex[index..index + 2], 16).map_err(Into::into)
                })
                .collect::<anyhow::Result<Vec<_>>>()
                .map(Some)
        }
        LogicalValue::UnchangedToast => Ok(None),
    }
}

fn parse_bool(value: &LogicalValue) -> anyhow::Result<Option<bool>> {
    match parse_text(value)? {
        None => Ok(None),
        Some("t" | "true") => Ok(Some(true)),
        Some("f" | "false") => Ok(Some(false)),
        Some(value) => anyhow::bail!("invalid PostgreSQL boolean '{value}'"),
    }
}

pub(super) fn parse_postgres_char(value: &LogicalValue) -> anyhow::Result<Option<i8>> {
    let Some(value) = parse_text(value)? else {
        return Ok(None);
    };
    if let Ok(value) = value.parse() {
        return Ok(Some(value));
    }
    let [value] = value.as_bytes() else {
        anyhow::bail!("invalid PostgreSQL internal char {value:?}")
    };
    Ok(Some(i8::from_ne_bytes([*value])))
}

fn parse_value<T>(value: &LogicalValue) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    parse_text(value)?
        .map(str::parse)
        .transpose()
        .map_err(Into::into)
}

pub(super) fn parse_lsn(value: &str) -> anyhow::Result<u64> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL LSN '{value}'"))?;
    Ok((u64::from_str_radix(high, 16)? << 32) | u64::from_str_radix(low, 16)?)
}

pub(super) fn format_lsn(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value & u64::from(u32::MAX))
}
