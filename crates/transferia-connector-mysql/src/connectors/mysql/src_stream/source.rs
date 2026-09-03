use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, Conn, Error as MySqlError, Value};
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{SchemaColumn, META_CHANGE_OPERATION, META_OLD_VALUE_OF};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::memory::{MemoryReservation, PipelineMemory};
use transferia_core::source::{CommitMarker, Source};
use transferia_core::ChangeOperation;
use transferia_registry::durable::DurableContext;

use super::config::heartbeat_period_nanoseconds;
use super::decoder::{
    CommittedTransaction, DecodedBinlogEvent, DecodedRowsEvent, MySqlBinlogDecoder,
    MySqlRowOperation, MySqlTransactionIdentity, MySqlTransactionMarker,
};
use super::identity::encode_transaction_identity;
use super::offset::MySqlReplicationOffsetTracker;
use super::position::{GtidSet, MySqlBinlogPosition, MySqlResumePosition};
use super::MySqlReplicationConfig;
use crate::connectors::mysql::src_batch::{
    old_value_column_name, ColumnPlan, DiscoveredTable, MYSQL_REPLICATION_SYSTEM_COLUMNS,
    MYSQL_SOURCE_METADATA_COLUMNS,
};
use crate::connectors::mysql::src_batch::optional_value_column_array;
use crate::connectors::mysql::src_batch_and_stream::{
    is_replication_safety_violation, replication_safety_violation, AuthoritativeTableIdentity,
    MySqlBinlogBoundary, MySqlSourceIdentity,
};
use crate::metrics::SourceCounters;

pub struct MySqlReplicationSource {
    stream: Option<BinlogStream>,
    decoder: MySqlBinlogDecoder,
    config: MySqlReplicationConfig,
    database: Arc<str>,
    tables: Vec<DiscoveredTable>,
    table_indexes: BTreeMap<Vec<u8>, usize>,
    active: Option<BufferedTransaction>,
    committed_position: MySqlBinlogPosition,
    emitted_position: MySqlBinlogPosition,
    committed_gtids: GtidSet,
    emitted_gtids: GtidSet,
    offset_tracker: MySqlReplicationOffsetTracker,
    memory: PipelineMemory,
    event_decode_admission_bytes: usize,
    counters: Arc<SourceCounters>,
    cancellation: CancellationToken,
    finished: bool,
}

struct BufferedTransaction {
    marker: Arc<MySqlTransactionMarker>,
    changes: Vec<BufferedRowChange>,
    next_message_index: u64,
    encoded_bytes: usize,
    table_map_count: usize,
    memory: MemoryReservation,
}

struct BufferedRowChange {
    table_index: usize,
    operation: ChangeOperation,
    before: Option<Vec<Option<Value>>>,
    after: Option<Vec<Option<Value>>>,
    message_index: u64,
    source_timestamp_seconds: u32,
}

#[derive(Clone, Debug)]
struct MySqlReplicationMarker {
    previous_position: MySqlBinlogPosition,
    next_position: MySqlBinlogPosition,
    previous_gtids: GtidSet,
    next_gtids: GtidSet,
}

impl MySqlReplicationSource {
    #[allow(
        clippy::too_many_arguments,
        reason = "the binlog reader receives every replay and schema identity explicitly"
    )]
    pub async fn new(
        mut connection: Conn,
        config: MySqlReplicationConfig,
        source_identity: MySqlSourceIdentity,
        tables: Vec<DiscoveredTable>,
        authoritative_tables: Vec<AuthoritativeTableIdentity>,
        counters: Arc<SourceCounters>,
        cancellation: CancellationToken,
        durable: DurableContext,
        memory: PipelineMemory,
        exact_start_boundary: Option<MySqlBinlogBoundary>,
        current_executed_gtids: GtidSet,
        current_purged_gtids: GtidSet,
        replay_identity: Arc<str>,
    ) -> anyhow::Result<Self> {
        validate_replication_tables(&source_identity, &tables, &authoritative_tables)
            .map_err(replication_safety_violation)?;
        let event_decode_admission_bytes =
            event_decode_admission_bytes(&config, &source_identity.database, &tables)
                .map_err(replication_safety_violation)?;
        let (offset_tracker, committed_position, committed_gtids) =
            MySqlReplicationOffsetTracker::prepare(
                &config,
                &source_identity,
                &authoritative_tables,
                durable,
                exact_start_boundary.as_ref(),
                &current_executed_gtids,
                &current_purged_gtids,
                replay_identity,
            )
            .await?;
        let mut decoder = MySqlBinlogDecoder::new(config.clone(), committed_position.clone())
            .map_err(|error| replication_safety_violation(error.into()))?;
        decoder.enable_gtid_auto_position();
        decoder.retain_rows_for_tables(
            source_identity.database.as_bytes(),
            tables
                .iter()
                .map(|table| table.config.name.as_bytes().to_vec()),
        );
        let resume = MySqlResumePosition::Gtid {
            executed: committed_gtids.clone(),
            fallback_position: committed_position.clone(),
        };
        let request = resume
            .request(config.server_id)
            .map_err(|error| replication_safety_violation(error.into()))?
            .with_max_event_bytes(config.max_transaction_bytes);
        let timeout = Duration::from_millis(config.bootstrap_timeout_ms);
        configure_binlog_heartbeat(&mut connection, &config, &cancellation, timeout).await?;
        let stream = observe_bounded_mysql_request(
            &cancellation,
            timeout,
            "start_binlog_stream",
            connection.get_binlog_stream(request),
        )
        .await
        .map_err(classify_binlog_start_error)?;
        let database = Arc::<str>::from(source_identity.database);
        let table_indexes = tables
            .iter()
            .enumerate()
            .map(|(index, table)| (table.config.name.as_bytes().to_vec(), index))
            .collect();
        Ok(Self {
            stream: Some(stream),
            decoder,
            config,
            database,
            tables,
            table_indexes,
            active: None,
            committed_position: committed_position.clone(),
            emitted_position: committed_position,
            committed_gtids: committed_gtids.clone(),
            emitted_gtids: committed_gtids,
            offset_tracker,
            memory,
            event_decode_admission_bytes,
            counters,
            cancellation,
            finished: false,
        })
    }

    fn accept_event(
        &mut self,
        event: DecodedBinlogEvent,
        event_reservation: Option<MemoryReservation>,
        event_bytes: usize,
    ) -> anyhow::Result<Option<SourceBatch>> {
        match event {
            DecodedBinlogEvent::TransactionStarted(marker) => {
                anyhow::ensure!(
                    self.active.is_none(),
                    "MySQL binlog actor received overlapping transactions"
                );
                self.active = Some(BufferedTransaction {
                    marker,
                    changes: Vec::new(),
                    next_message_index: 0,
                    encoded_bytes: event_bytes,
                    table_map_count: 0,
                    memory: event_reservation.ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL transaction started without an admitted memory reservation"
                        )
                    })?,
                });
                Ok(None)
            }
            DecodedBinlogEvent::Rows(rows) => {
                anyhow::ensure!(
                    event_reservation.is_none(),
                    "MySQL active transaction unexpectedly acquired a second memory reservation"
                );
                self.append_rows(rows)?;
                Ok(None)
            }
            DecodedBinlogEvent::TransactionCommitted(committed) => {
                anyhow::ensure!(
                    event_reservation.is_none(),
                    "MySQL active transaction unexpectedly acquired a second memory reservation"
                );
                self.finish_transaction(committed).map(Some)
            }
            DecodedBinlogEvent::TransactionRolledBack(rolled_back) => {
                anyhow::ensure!(
                    event_reservation.is_none(),
                    "MySQL active transaction unexpectedly acquired a second memory reservation"
                );
                let active = self.active.take().ok_or_else(|| {
                    anyhow::anyhow!("MySQL binlog actor received rollback without a transaction")
                })?;
                anyhow::ensure!(
                    Arc::ptr_eq(&active.marker, &rolled_back.transaction)
                        || active.marker == rolled_back.transaction,
                    "MySQL binlog rollback identity does not match the buffered transaction"
                );
                Ok(None)
            }
            DecodedBinlogEvent::TableMapped(table) => {
                anyhow::ensure!(
                    event_reservation.is_none(),
                    "MySQL active transaction unexpectedly acquired a second memory reservation"
                );
                self.validate_table_map(&table)?;
                let active = self.active.as_mut().ok_or_else(|| {
                    anyhow::anyhow!("MySQL binlog actor received a table map without a transaction")
                })?;
                active.table_map_count = active.table_map_count.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("MySQL retained table-map count overflow")
                })?;
                Ok(None)
            }
            DecodedBinlogEvent::Ignored(ignored) => {
                if ignored.inside_transaction
                    && ignored.event_type == mysql_async::binlog::EventType::TABLE_MAP_EVENT
                {
                    let active = self.active.as_mut().ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL binlog actor ignored a table map without a transaction"
                        )
                    })?;
                    active.table_map_count =
                        active.table_map_count.checked_add(1).ok_or_else(|| {
                            anyhow::anyhow!("MySQL retained table-map count overflow")
                        })?;
                }
                Ok(None)
            }
            DecodedBinlogEvent::BinlogRotated(_) => Ok(None),
        }
    }

    fn append_rows(&mut self, rows: DecodedRowsEvent) -> anyhow::Result<()> {
        let active = self.active.as_mut().ok_or_else(|| {
            anyhow::anyhow!("MySQL binlog actor received row changes without a transaction")
        })?;
        anyhow::ensure!(
            Arc::ptr_eq(&active.marker, &rows.transaction) || active.marker == rows.transaction,
            "MySQL binlog row identity does not match the buffered transaction"
        );
        if rows.table.database != self.database.as_bytes() {
            return Ok(());
        }
        let Some(table_index) = self.table_indexes.get(rows.table.table.as_slice()).copied() else {
            return Ok(());
        };
        let table = self.tables.get(table_index).ok_or_else(|| {
            anyhow::anyhow!("MySQL configured table index disappeared during replication")
        })?;
        validate_selected_table_map(table, &rows.table)?;
        anyhow::ensure!(
            usize::try_from(rows.table.columns)? == table.columns.len(),
            "MySQL binlog table '{}.{}' has {} columns, discovery declared {}",
            self.database,
            table.config.name,
            rows.table.columns,
            table.columns.len()
        );
        for row in rows.rows {
            validate_row_columns(table, row.before.as_deref())?;
            validate_row_columns(table, row.after.as_deref())?;
            let message_index = active.next_message_index;
            active.next_message_index = active.next_message_index.checked_add(1).ok_or_else(|| {
                anyhow::anyhow!("MySQL transaction message index overflow")
            })?;
            active.changes.push(BufferedRowChange {
                table_index,
                operation: change_operation(rows.operation),
                before: row.before,
                after: row.after,
                message_index,
                source_timestamp_seconds: rows.source_timestamp_seconds,
            });
        }
        Ok(())
    }

    fn finish_transaction(
        &mut self,
        committed: CommittedTransaction,
    ) -> anyhow::Result<SourceBatch> {
        let active = self.active.take().ok_or_else(|| {
            anyhow::anyhow!("MySQL binlog actor received commit without a transaction")
        })?;
        anyhow::ensure!(
            Arc::ptr_eq(&active.marker, &committed.transaction)
                || active.marker == committed.transaction,
            "MySQL binlog commit identity does not match the buffered transaction"
        );
        anyhow::ensure!(
            committed.next_position == *self.decoder.current_position(),
            "MySQL binlog decoder commit position diverged from its current position"
        );
        anyhow::ensure!(
            usize::try_from(committed.encoded_bytes)? == active.encoded_bytes,
            "MySQL transaction encoded-byte accounting diverged from the binlog decoder"
        );
        let retained_transaction_bytes = retained_transaction_bytes(&active)?;
        let materialization_bytes = arrow_materialization_admission_bytes(
            &active,
            &self.database,
            &self.tables,
            &committed.next_position,
            &self.emitted_position,
            &self.emitted_gtids,
        )?;
        active.memory.grow_progress_source_to(
            retained_transaction_bytes
                .checked_add(materialization_bytes)
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL CDC materialization memory accounting overflow")
                })?,
        )?;
        let event_timestamp_us = system_time_micros()?;
        let transaction_identity = encode_transaction_identity(&active.marker.identity)?;
        let mut grouped = (0..self.tables.len())
            .map(|_| Vec::<&BufferedRowChange>::new())
            .collect::<Vec<_>>();
        for change in &active.changes {
            grouped
                .get_mut(change.table_index)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "MySQL transaction refers to an unknown configured table index"
                    )
                })?
                .push(change);
        }
        let touched_table_count = grouped.iter().filter(|changes| !changes.is_empty()).count();
        let mut tables = Vec::with_capacity(touched_table_count);
        let mut row_count = 0_u64;
        for (table, changes) in self.tables.iter().zip(grouped) {
            if changes.is_empty() {
                continue;
            }
            row_count = row_count
                .checked_add(u64::try_from(changes.len())?)
                .ok_or_else(|| anyhow::anyhow!("MySQL replication row count overflow"))?;
            tables.push(changes_to_table_data(
                table,
                &self.database,
                &transaction_identity,
                event_timestamp_us,
                &committed.next_position,
                &changes,
            )?);
        }
        let previous_position = self.emitted_position.clone();
        let previous_gtids = self.emitted_gtids.clone();
        let mut next_gtids = previous_gtids.clone();
        let MySqlTransactionIdentity::Gtid { sid, tag, gno } = &active.marker.identity else {
            anyhow::bail!(
                "MySQL GTID-mode replication committed a transaction without an exact GTID"
            );
        };
        next_gtids.include_transaction(*sid, tag.clone(), *gno)?;
        let marker = MySqlReplicationMarker {
            previous_position,
            next_position: committed.next_position,
            previous_gtids,
            next_gtids,
        };
        let output_bytes = retained_source_batch_bytes(&tables, &marker)?;
        let memory = active.memory.clone();
        drop(active);
        let _ = memory.shrink_to(output_bytes);
        self.emitted_position = marker.next_position.clone();
        self.emitted_gtids = marker.next_gtids.clone();
        self.counters.add_records(row_count);
        Ok(SourceBatch::Typed {
            tables,
            source_rows: row_count,
            commit_marker: Some(CommitMarker::new(marker)),
            memory: vec![memory],
        })
    }

    fn validate_table_map(
        &self,
        table_map: &super::decoder::MySqlTableIdentity,
    ) -> anyhow::Result<()> {
        if table_map.database != self.database.as_bytes() {
            return Ok(());
        }
        let Some(table_index) = self.table_indexes.get(table_map.table.as_slice()).copied() else {
            return Ok(());
        };
        let table = self.tables.get(table_index).ok_or_else(|| {
            anyhow::anyhow!("MySQL configured table index disappeared during replication")
        })?;
        validate_selected_table_map(table, table_map)
    }
}

async fn configure_binlog_heartbeat(
    connection: &mut Conn,
    config: &MySqlReplicationConfig,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> anyhow::Result<()> {
    // An idle MySQL binlog sender waits for new log data and does not otherwise
    // read COM_QUIT or observe the peer's closed socket. A bounded heartbeat
    // makes it write again, notice the close, terminate the dump thread, and
    // release every connection-owned named lock before the next acquisition.
    let heartbeat_nanoseconds = heartbeat_period_nanoseconds(config.poll_interval_ms)
        .map_err(replication_safety_violation)?;
    observe_bounded_mysql_request(
        cancellation,
        timeout,
        "configure_binlog_heartbeat",
        connection.exec_drop(
            "SET @master_heartbeat_period = ?, @source_heartbeat_period = ?",
            (heartbeat_nanoseconds, heartbeat_nanoseconds),
        ),
    )
    .await?;
    let observed = observe_bounded_mysql_request(
        cancellation,
        timeout,
        "verify_binlog_heartbeat",
        connection.exec_first::<(u64, u64), _, _>(
            "SELECT @master_heartbeat_period, @source_heartbeat_period",
            (),
        ),
    )
    .await?;
    verify_binlog_heartbeat(heartbeat_nanoseconds, observed)
        .map_err(replication_safety_violation)
}

pub(super) fn verify_binlog_heartbeat(
    expected_nanoseconds: u64,
    observed: Option<(u64, u64)>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        observed == Some((expected_nanoseconds, expected_nanoseconds)),
        "MySQL did not retain the exact configured replication heartbeat period of {expected_nanoseconds} nanoseconds"
    );
    Ok(())
}

impl Source for MySqlReplicationSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(SourceBatch::Finished);
            }
            loop {
                let previous_transaction_bytes = self
                    .active
                    .as_ref()
                    .map(retained_transaction_bytes)
                    .transpose()
                    .map_err(|error| {
                        DataPlaneFailure::fatal(replication_safety_violation(error))
                    })?
                    .unwrap_or(0);
                let read_admission_bytes = previous_transaction_bytes
                    .checked_add(self.event_decode_admission_bytes)
                    .ok_or_else(|| {
                        DataPlaneFailure::fatal(replication_safety_violation(anyhow::anyhow!(
                            "MySQL binlog read memory admission overflow"
                        )))
                    })?;
                let event_reservation = if let Some(transaction) = self.active.as_ref() {
                    transaction
                        .memory
                        .grow_progress_source_to(read_admission_bytes)
                        .map_err(|error| {
                            DataPlaneFailure::fatal(replication_safety_violation(error))
                    })?;
                    None
                } else {
                    self.memory
                        .used()
                        .checked_add(read_admission_bytes)
                        .ok_or_else(|| {
                            DataPlaneFailure::fatal(replication_safety_violation(anyhow::anyhow!(
                                "MySQL pipeline memory reservation would overflow this platform"
                            )))
                        })?;
                    let reserve = self
                        .memory
                        .reserve_progress_source(read_admission_bytes);
                    Some(tokio::select! {
                        biased;
                        () = self.cancellation.cancelled() => {
                            if let Some(transaction) = self.active.as_ref() {
                                let _ = transaction.memory.shrink_to(previous_transaction_bytes);
                            }
                            self.finished = true;
                            return Ok(SourceBatch::Finished);
                        }
                        reservation = reserve => reservation,
                    })
                };
                let wait_started = Instant::now();
                let next = {
                    let stream = self.stream.as_mut().ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "MySQL binlog stream is unavailable before shutdown"
                        ))
                    })?;
                    tokio::select! {
                        biased;
                        () = self.cancellation.cancelled() => {
                            if let Some(transaction) = self.active.as_ref() {
                                let _ = transaction.memory.shrink_to(previous_transaction_bytes);
                            }
                            self.finished = true;
                            return Ok(SourceBatch::Finished);
                        }
                        next = tokio::time::timeout(
                            Duration::from_millis(self.config.poll_interval_ms),
                            stream.next(),
                        ) => next,
                    }
                };
                self.counters.add_response_wait(wait_started.elapsed());
                let event = match next {
                    Err(_) => {
                        if let Some(transaction) = self.active.as_ref() {
                            let _ = transaction.memory.shrink_to(previous_transaction_bytes);
                        }
                        return Ok(empty_batch());
                    }
                    Ok(Some(Ok(event))) => event,
                    Ok(Some(Err(error))) => {
                        if let Some(transaction) = self.active.as_ref() {
                            let _ = transaction.memory.shrink_to(previous_transaction_bytes);
                        }
                        return Err(classify_binlog_read_error(error));
                    }
                    Ok(None) => {
                        if let Some(transaction) = self.active.as_ref() {
                            let _ = transaction.memory.shrink_to(previous_transaction_bytes);
                        }
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "MySQL binlog stream ended before cancellation"
                        )))
                    }
                };
                let event_bytes = usize::try_from(event.header().event_size()).map_err(|_| {
                    DataPlaneFailure::fatal(replication_safety_violation(anyhow::anyhow!(
                        "MySQL binlog event length does not fit this platform"
                    )))
                })?;
                if event_bytes > self.config.max_transaction_bytes {
                    return Err(DataPlaneFailure::fatal(replication_safety_violation(
                        anyhow::anyhow!(
                            "MySQL binlog event has {event_bytes} bytes, exceeding configured max_transaction_bytes {}",
                            self.config.max_transaction_bytes
                        ),
                    )));
                }
                let next_transaction_bytes = if let Some(transaction) = self.active.as_ref() {
                    Some(
                        transaction
                            .encoded_bytes
                            .checked_add(event_bytes)
                            .ok_or_else(|| {
                                DataPlaneFailure::fatal(replication_safety_violation(
                                    anyhow::anyhow!(
                                        "MySQL transaction memory accounting overflow"
                                    ),
                                ))
                            })?,
                    )
                } else {
                    None
                };
                self.counters
                    .add_network_decoded_bytes(u64::from(event.header().event_size()));
                let format_description_event = matches!(
                    event.header().event_type(),
                    Ok(mysql_async::binlog::EventType::FORMAT_DESCRIPTION_EVENT)
                );
                let decode_started = Instant::now();
                let decoded = self.decoder.decode(&event).map_err(|error| {
                    DataPlaneFailure::fatal(replication_safety_violation(error.into()))
                })?;
                self.counters
                    .add_network_decode_busy(decode_started.elapsed());
                let table_map_boundary = matches!(
                    &decoded,
                    DecodedBinlogEvent::TransactionCommitted(_)
                        | DecodedBinlogEvent::TransactionRolledBack(_)
                        | DecodedBinlogEvent::BinlogRotated(_)
                );
                drop(event);
                if table_map_boundary || format_description_event {
                    self.stream
                        .as_mut()
                        .ok_or_else(|| {
                            DataPlaneFailure::fatal(anyhow::anyhow!(
                                "MySQL binlog stream is unavailable while clearing table maps"
                            ))
                        })?
                        .clear_table_maps()
                        .map_err(|error| {
                            DataPlaneFailure::fatal(replication_safety_violation(error.into()))
                        })?;
                }
                if let Some(next_bytes) = next_transaction_bytes {
                    let transaction = self.active.as_mut().ok_or_else(|| {
                        DataPlaneFailure::fatal(replication_safety_violation(anyhow::anyhow!(
                            "MySQL active transaction disappeared during event decode"
                        )))
                    })?;
                    transaction.encoded_bytes = next_bytes;
                }
                if let Some(batch) = self
                    .accept_event(decoded, event_reservation, event_bytes)
                    .map_err(|error| {
                        DataPlaneFailure::fatal(replication_safety_violation(error))
                    })?
                {
                    return Ok(batch);
                }
                if let Some(transaction) = self.active.as_ref() {
                    let retained_bytes = retained_transaction_bytes(transaction).map_err(|error| {
                        DataPlaneFailure::fatal(replication_safety_violation(error))
                    })?;
                    let _ = transaction.memory.shrink_to(retained_bytes);
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let mut expected = self.committed_position.clone();
            let mut expected_gtids = self.committed_gtids.clone();
            for marker in markers {
                let marker = marker
                    .value::<MySqlReplicationMarker>()
                    .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
                if marker.previous_position != expected
                    || marker.previous_gtids != expected_gtids
                {
                    return Err(DataPlaneFailure::fatal(replication_safety_violation(
                        anyhow::anyhow!(
                            "MySQL replication commit markers are not one contiguous emitted prefix"
                        ),
                    )));
                }
                marker.next_position.validate().map_err(|error| {
                    DataPlaneFailure::fatal(replication_safety_violation(error.into()))
                })?;
                marker.next_gtids.validate().map_err(|error| {
                    DataPlaneFailure::fatal(replication_safety_violation(error.into()))
                })?;
                expected = marker.next_position.clone();
                expected_gtids = marker.next_gtids.clone();
            }
            if markers.is_empty() {
                return Err(DataPlaneFailure::fatal(replication_safety_violation(
                    anyhow::anyhow!("MySQL replication commit has no offset markers"),
                )));
            }
            self.offset_tracker
                .store(&expected, &expected_gtids)
                .await
                .map_err(|error| {
                    if is_replication_safety_violation(&error) {
                        DataPlaneFailure::fatal(error)
                    } else {
                        DataPlaneFailure::retryable(error)
                    }
                })?;
            self.committed_position = expected;
            self.committed_gtids = expected_gtids;
            Ok(())
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            self.finished = true;
            self.active = None;
            let Some(stream) = self.stream.take() else {
                return Ok(());
            };
            let timeout = Duration::from_millis(self.config.bootstrap_timeout_ms);
            observe_external_request("mysql", "close_binlog_stream", async move {
                tokio::time::timeout(timeout, stream.close())
                    .await
                    .map_err(|_| anyhow::anyhow!("MySQL binlog stream close timed out"))?
                    .map_err(anyhow::Error::from)
            })
            .await
            .map_err(DataPlaneFailure::retryable)
        })
    }
}

/// Conservatively covers the raw event, mysql_common's decoded rows, and the
/// connector-owned rows while one event is being moved into the transaction
/// buffer. JSON is rejected for CDC, so decimal text (at most four bytes of
/// output per packed input byte) is the largest supported payload expansion.
fn event_decode_admission_bytes(
    config: &MySqlReplicationConfig,
    database: &str,
    tables: &[DiscoveredTable],
) -> anyhow::Result<usize> {
    let max_columns = tables
        .iter()
        .map(|table| table.columns.len())
        .max()
        .unwrap_or(0);
    let row_images = config
        .max_events
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC row-image memory accounting overflow"))?;
    let value_slots = row_images
        .checked_mul(max_columns)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC value-slot memory accounting overflow"))?;
    let value_slot_bytes = size_of::<Option<mysql_async::binlog::value::BinlogValue<'static>>>()
        .checked_add(size_of::<Option<Value>>())
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC value-slot size accounting overflow"))?;
    let decoded_value_state = value_slots
        .checked_mul(value_slot_bytes)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC decoded-value memory accounting overflow"))?;
    let row_state = config
        .max_events
        .checked_mul(2)
        .and_then(|rows| {
            rows.checked_mul(
                size_of::<super::decoder::MySqlRowChange>()
                    + size_of::<BufferedRowChange>(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC decoded-row memory accounting overflow"))?;
    let table_map_state = max_columns
        .checked_mul(
            size_of::<mysql_async::Column>() + size_of::<super::decoder::MySqlBinlogColumnIdentity>(),
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC table-map memory accounting overflow"))?;
    let row_column_state = tables
        .iter()
        .map(|table| {
            table.columns.iter().try_fold(0_usize, |bytes, column| {
                bytes
                    .checked_add(size_of::<mysql_async::Column>())
                    .and_then(|value| value.checked_add(database.len()))
                    .and_then(|value| value.checked_add(table.config.name.len().checked_mul(2)?))
                    .and_then(|value| value.checked_add(column.name.len()))
                    .ok_or_else(|| {
                        anyhow::anyhow!("MySQL CDC row-column metadata accounting overflow")
                    })
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .max()
        .unwrap_or(0)
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC row-column memory accounting overflow"))?;
    let raw_and_expanded_payload = config
        .max_transaction_bytes
        .checked_mul(5)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC event-payload memory accounting overflow"))?;
    raw_and_expanded_payload
        .checked_add(decoded_value_state)
        .and_then(|bytes| bytes.checked_add(row_state))
        .and_then(|bytes| bytes.checked_add(table_map_state))
        .and_then(|bytes| bytes.checked_add(row_column_state))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC event decode memory admission overflow"))
}

fn retained_transaction_bytes(transaction: &BufferedTransaction) -> anyhow::Result<usize> {
    let mut bytes = transaction
        .encoded_bytes
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC retained table-map accounting overflow"))?
        .checked_add(
            transaction
                .changes
                .capacity()
                .checked_mul(size_of::<BufferedRowChange>())
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL CDC buffered-change memory accounting overflow")
                })?,
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC transaction memory accounting overflow"))?;
    bytes = bytes
        .checked_add(
            transaction
                .table_map_count
                .checked_mul(4 * size_of::<mysql_async::binlog::events::TableMapEvent<'static>>())
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL CDC retained table-map state accounting overflow")
                })?,
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC table-map memory accounting overflow"))?;
    bytes = bytes
        .checked_add(transaction_marker_heap_bytes(&transaction.marker)?)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC transaction marker accounting overflow"))?;
    for row in transaction
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
    {
        bytes = bytes
            .checked_add(retained_row_heap_bytes(row)?)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC retained-row memory accounting overflow"))?;
    }
    Ok(bytes)
}

fn retained_row_heap_bytes(row: &Vec<Option<Value>>) -> anyhow::Result<usize> {
    let mut bytes = row
        .capacity()
        .checked_mul(size_of::<Option<Value>>())
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC row value memory accounting overflow"))?;
    for index in 0..row.len() {
        if let Some(Value::Bytes(value)) = row.get(index).and_then(Option::as_ref) {
            bytes = bytes.checked_add(value.capacity()).ok_or_else(|| {
                anyhow::anyhow!("MySQL CDC row payload memory accounting overflow")
            })?;
        }
    }
    Ok(bytes)
}

fn transaction_marker_heap_bytes(marker: &MySqlTransactionMarker) -> anyhow::Result<usize> {
    let identity_bytes = match &marker.identity {
        MySqlTransactionIdentity::Gtid { tag, .. } => {
            tag.as_ref().map_or(0, String::capacity)
        }
        MySqlTransactionIdentity::Anonymous { begin_position }
        | MySqlTransactionIdentity::FilePosition { begin_position } => {
            begin_position.filename.capacity()
        }
    };
    size_of::<MySqlTransactionMarker>()
        .checked_add(marker.begin_position.filename.capacity())
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC transaction identity accounting overflow"))
}

fn arrow_materialization_admission_bytes(
    transaction: &BufferedTransaction,
    database: &str,
    tables: &[DiscoveredTable],
    next_position: &MySqlBinlogPosition,
    previous_position: &MySqlBinlogPosition,
    previous_gtids: &GtidSet,
) -> anyhow::Result<usize> {
    let transaction_identity_bytes = transaction_identity_encoded_len(&transaction.marker.identity)?;
    let mut bytes = transaction
        .changes
        .len()
        .checked_mul(2)
        .and_then(|rows| rows.checked_mul(size_of::<&BufferedRowChange>()))
        .and_then(|value| {
            value.checked_add(tables.len().checked_mul(size_of::<Vec<&BufferedRowChange>>())?)
        })
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC grouped-row memory accounting overflow"))?;
    for change in &transaction.changes {
        let table = tables.get(change.table_index).ok_or_else(|| {
            anyhow::anyhow!("MySQL transaction refers to an unknown configured table index")
        })?;
        let output_cells = table
            .columns
            .len()
            .checked_mul(2)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC output cell count overflow"))?;
        bytes = bytes
            .checked_add(output_cells.checked_mul(64).ok_or_else(|| {
                anyhow::anyhow!("MySQL CDC Arrow cell memory accounting overflow")
            })?)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow memory accounting overflow"))?;
        for row in output_rows(change) {
            bytes = bytes
                .checked_add(
                    row.map(row_payload_bytes)
                        .transpose()?
                        .unwrap_or(0)
                        .checked_mul(2)
                        .ok_or_else(|| {
                            anyhow::anyhow!("MySQL CDC Arrow payload memory accounting overflow")
                        })?,
                )
                .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow memory accounting overflow"))?;
        }
        let changed_columns_bytes = table.columns.len().div_ceil(8);
        let metadata_payload = database
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(table.config.name.len()))
            .and_then(|value| value.checked_add(transaction_identity_bytes))
            .and_then(|value| value.checked_add(next_position.filename.len()))
            .and_then(|value| value.checked_add(change.operation.code().len()))
            .and_then(|value| value.checked_add(changed_columns_bytes))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC metadata payload accounting overflow"))?;
        bytes = bytes
            .checked_add(metadata_payload.checked_mul(2).ok_or_else(|| {
                anyhow::anyhow!("MySQL CDC metadata allocation accounting overflow")
            })?)
            .and_then(|value| value.checked_add(256))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC metadata memory accounting overflow"))?;
    }
    let touched_tables = tables
        .iter()
        .enumerate()
        .filter(|(table_index, _)| {
            transaction
                .changes
                .iter()
                .any(|change| change.table_index == *table_index)
        });
    for (_, table) in touched_tables {
        let fields = table
            .columns
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
            .and_then(|value| value.checked_add(MYSQL_REPLICATION_SYSTEM_COLUMNS.len()))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema field count overflow"))?;
        bytes = bytes
            .checked_add(fields.checked_mul(1_024).ok_or_else(|| {
                anyhow::anyhow!("MySQL CDC schema materialization accounting overflow")
            })?)
            .and_then(|value| value.checked_add(size_of::<TableData>()))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC table materialization accounting overflow"))?;
    }
    let marker_bytes = replication_marker_admission_bytes(
        previous_position,
        next_position,
        previous_gtids,
    )?
    .checked_add(
        transaction_marker_heap_bytes(&transaction.marker)?
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC GTID marker accounting overflow"))?,
    )
    .ok_or_else(|| anyhow::anyhow!("MySQL CDC marker memory accounting overflow"))?;
    bytes
        .checked_add(marker_bytes)
        .and_then(|value| {
            value.checked_add(
                tables
                    .len()
                    .checked_mul(size_of::<TableData>())?,
            )
        })
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC materialization admission overflow"))
}

fn output_rows(change: &BufferedRowChange) -> [Option<&[Option<Value>]>; 2] {
    let current = match change.operation {
        ChangeOperation::Create | ChangeOperation::Update => change.after.as_deref(),
        ChangeOperation::Delete => change.before.as_deref(),
        ChangeOperation::SnapshotRead => None,
    };
    let old = match change.operation {
        ChangeOperation::Create | ChangeOperation::SnapshotRead => None,
        ChangeOperation::Update | ChangeOperation::Delete => change.before.as_deref(),
    };
    [current, old]
}

fn row_payload_bytes(row: &[Option<Value>]) -> anyhow::Result<usize> {
    let mut bytes = 0_usize;
    for index in 0..row.len() {
        if let Some(Value::Bytes(value)) = row.get(index).and_then(Option::as_ref) {
            bytes = bytes.checked_add(value.len()).ok_or_else(|| {
                anyhow::anyhow!("MySQL CDC output value payload accounting overflow")
            })?;
        }
    }
    Ok(bytes)
}

fn transaction_identity_encoded_len(identity: &MySqlTransactionIdentity) -> anyhow::Result<usize> {
    let fields = match identity {
        MySqlTransactionIdentity::Gtid { tag, .. } => {
            let optional_tag_bytes = match tag {
                Some(tag) => 9_usize.checked_add(tag.len()),
                None => Some(1),
            };
            optional_tag_bytes
                .and_then(|tag_bytes| 16_usize.checked_add(tag_bytes))
                .and_then(|value| value.checked_add(8))
        }
        MySqlTransactionIdentity::Anonymous { begin_position }
        | MySqlTransactionIdentity::FilePosition { begin_position } => begin_position
            .filename
            .len()
            .checked_add(size_of::<u32>()),
    }
    .ok_or_else(|| anyhow::anyhow!("MySQL transaction identity length overflow"))?;
    b"transferia.mysql.source-transaction-id"
        .len()
        .checked_add(2)
        .and_then(|value| value.checked_add(fields))
        .and_then(|value| value.checked_add(3 * (1 + size_of::<u64>())))
        .ok_or_else(|| anyhow::anyhow!("MySQL transaction identity length overflow"))
}

fn replication_marker_admission_bytes(
    previous_position: &MySqlBinlogPosition,
    next_position: &MySqlBinlogPosition,
    previous_gtids: &GtidSet,
) -> anyhow::Result<usize> {
    let previous_gtid_bytes = gtid_set_heap_bytes(previous_gtids)?;
    size_of::<MySqlReplicationMarker>()
        .checked_add(previous_position.filename.len())
        .and_then(|value| value.checked_add(next_position.filename.len()))
        .and_then(|value| value.checked_add(previous_gtid_bytes.checked_mul(4)?))
        .and_then(|value| value.checked_add(size_of::<super::position::GtidSid>()))
        .and_then(|value| value.checked_add(size_of::<super::position::GtidInterval>()))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC commit marker admission overflow"))
}

fn retained_source_batch_bytes(
    tables: &[TableData],
    marker: &MySqlReplicationMarker,
) -> anyhow::Result<usize> {
    let mut bytes = tables
        .len()
        .checked_mul(size_of::<TableData>())
        .and_then(|value| value.checked_add(size_of::<MemoryReservation>()))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC output container accounting overflow"))?;
    for table in tables {
        bytes = bytes
            .checked_add(table.batch.get_array_memory_size())
            .and_then(|value| value.checked_add(table.table.len().checked_add(2 * size_of::<usize>())?))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow batch memory accounting overflow"))?;
        let schema = table.batch.schema();
        bytes = bytes
            .checked_add(size_of::<Schema>())
            .and_then(|value| {
                value.checked_add(schema.fields().len().checked_mul(size_of::<Arc<Field>>())?)
            })
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow schema accounting overflow"))?;
        for field in schema.fields() {
            bytes = bytes
                .checked_add(size_of::<Field>())
                .and_then(|value| value.checked_add(field.name().len().checked_mul(2)?))
                .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow field accounting overflow"))?;
            for (key, value) in field.metadata() {
                bytes = bytes
                    .checked_add(4 * size_of::<(String, String)>())
                    .and_then(|total| total.checked_add(key.len().checked_mul(2)?))
                    .and_then(|total| total.checked_add(value.len().checked_mul(2)?))
                    .ok_or_else(|| {
                        anyhow::anyhow!("MySQL CDC Arrow field metadata accounting overflow")
                    })?;
            }
        }
        bytes = bytes
            .checked_add(
                table
                    .system_columns
                    .iter()
                    .len()
                    .checked_mul(size_of::<SystemColumn>())
                    .ok_or_else(|| {
                        anyhow::anyhow!("MySQL CDC system-column accounting overflow")
                    })?,
            )
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC output metadata accounting overflow"))?;
        for column in table.system_columns.iter() {
            bytes = bytes.checked_add(
                column
                    .name
                    .len()
                    .checked_add(2 * size_of::<usize>())
                    .ok_or_else(|| {
                        anyhow::anyhow!("MySQL CDC system-column name accounting overflow")
                    })?,
            ).ok_or_else(|| {
                anyhow::anyhow!("MySQL CDC system-column name accounting overflow")
            })?;
        }
    }
    bytes
        .checked_add(replication_marker_heap_bytes(marker)?)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC retained output memory accounting overflow"))
}

fn replication_marker_heap_bytes(marker: &MySqlReplicationMarker) -> anyhow::Result<usize> {
    size_of::<MySqlReplicationMarker>()
        .checked_add(marker.previous_position.filename.capacity())
        .and_then(|value| value.checked_add(marker.next_position.filename.capacity()))
        .and_then(|value| value.checked_add(gtid_set_heap_bytes(&marker.previous_gtids).ok()?))
        .and_then(|value| value.checked_add(gtid_set_heap_bytes(&marker.next_gtids).ok()?))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC commit marker memory accounting overflow"))
}

fn gtid_set_heap_bytes(gtids: &GtidSet) -> anyhow::Result<usize> {
    let mut bytes = gtids
        .0
        .capacity()
        .checked_mul(size_of::<super::position::GtidSid>())
        .ok_or_else(|| anyhow::anyhow!("MySQL GTID set memory accounting overflow"))?;
    for sid in &gtids.0 {
        bytes = bytes
            .checked_add(sid.tag.as_ref().map_or(0, String::capacity))
            .and_then(|value| {
                value.checked_add(
                    sid.intervals
                        .capacity()
                        .checked_mul(size_of::<super::position::GtidInterval>())?,
                )
            })
            .ok_or_else(|| anyhow::anyhow!("MySQL GTID state memory accounting overflow"))?;
    }
    Ok(bytes)
}

fn changes_to_table_data(
    table: &DiscoveredTable,
    database: &str,
    transaction_identity: &[u8],
    event_timestamp_us: i64,
    position: &MySqlBinlogPosition,
    changes: &[&BufferedRowChange],
) -> anyhow::Result<TableData> {
    let mut fields = Vec::with_capacity(
        table
            .columns
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
            .and_then(|value| value.checked_add(MYSQL_REPLICATION_SYSTEM_COLUMNS.len()))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema column count overflow"))?,
    );
    let mut arrays = Vec::with_capacity(fields.capacity());
    let current_rows = changes
        .iter()
        .map(|change| match change.operation {
            ChangeOperation::Create | ChangeOperation::Update => change.after.as_deref(),
            ChangeOperation::Delete => change.before.as_deref(),
            ChangeOperation::SnapshotRead => None,
        })
        .collect::<Vec<_>>();
    let old_rows = changes
        .iter()
        .map(|change| match change.operation {
            ChangeOperation::Create | ChangeOperation::SnapshotRead => None,
            ChangeOperation::Update | ChangeOperation::Delete => change.before.as_deref(),
        })
        .collect::<Vec<_>>();
    for (index, (column, discovered)) in table
        .columns
        .iter()
        .zip(&table.schema.columns)
        .enumerate()
    {
        anyhow::ensure!(
            column.name == discovered.name && column.kind.arrow_type() == discovered.data_type,
            "MySQL CDC schema drifted at column '{}'",
            column.name
        );
        fields.push(
            Field::new(&column.name, discovered.data_type.clone(), true)
                .with_metadata(discovered.arrow_metadata()),
        );
        arrays.push(optional_value_column_array(
            &current_rows,
            index,
            column,
        )?);
    }
    for (index, (column, discovered)) in table
        .columns
        .iter()
        .zip(&table.schema.columns)
        .enumerate()
    {
        fields.push(
            Field::new(
                old_value_column_name(index),
                discovered.data_type.clone(),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                META_OLD_VALUE_OF.to_owned(),
                discovered.name.clone(),
            )])),
        );
        arrays.push(optional_value_column_array(&old_rows, index, column)?);
    }
    fields.extend(MYSQL_SOURCE_METADATA_COLUMNS.iter().map(|column| {
        Field::new(column.name, column.data_type.clone(), false).with_metadata(
            SchemaColumn::new(column.name.to_owned(), column.data_type.clone(), false)
                .with_system_role(column.role)
                .arrow_metadata(),
        )
    }));
    fields.extend(MYSQL_REPLICATION_SYSTEM_COLUMNS.iter().map(|kind| {
        let field = Field::new(kind.default_name(), kind.data_type(), false);
        if *kind == SystemColumnKind::ChangeOperation {
            field.with_metadata(std::collections::HashMap::from([(
                META_CHANGE_OPERATION.to_owned(),
                "true".to_owned(),
            )]))
        } else {
            field
        }
    }));

    let len = changes.len();
    let source_timestamp_us = changes
        .iter()
        .map(|change| {
            i64::from(change.source_timestamp_seconds)
                .checked_mul(1_000_000)
                .ok_or_else(|| anyhow::anyhow!("MySQL source timestamp microseconds overflow"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let source_timestamp_ms = source_timestamp_us
        .iter()
        .map(|timestamp| timestamp.div_euclid(1_000))
        .collect::<Vec<_>>();
    let source_timestamp_ns = source_timestamp_us
        .iter()
        .map(|timestamp| {
            timestamp
                .checked_mul(1_000)
                .ok_or_else(|| anyhow::anyhow!("MySQL source timestamp nanoseconds overflow"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let event_timestamp_ms = event_timestamp_us.div_euclid(1_000);
    let event_timestamp_ns = event_timestamp_us
        .checked_mul(1_000)
        .ok_or_else(|| anyhow::anyhow!("MySQL event timestamp nanoseconds overflow"))?;
    arrays.extend([
        Arc::new(StringArray::from(vec![database; len])) as ArrayRef,
        Arc::new(StringArray::from(vec![database; len])) as ArrayRef,
        Arc::new(StringArray::from(vec![table.config.name.as_str(); len])) as ArrayRef,
        Arc::new(BinaryArray::from_iter_values(
            std::iter::repeat(transaction_identity).take(len),
        )) as ArrayRef,
        Arc::new(Int64Array::from(source_timestamp_ms)) as ArrayRef,
        Arc::new(Int64Array::from(source_timestamp_us)) as ArrayRef,
        Arc::new(Int64Array::from(source_timestamp_ns)) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_ms; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_us; len])) as ArrayRef,
        Arc::new(Int64Array::from(vec![event_timestamp_ns; len])) as ArrayRef,
    ]);
    let offset = i64::from(position.position);
    let changed_columns = changes
        .iter()
        .map(|change| changed_columns_mask(table, change))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for kind in MYSQL_REPLICATION_SYSTEM_COLUMNS {
        arrays.push(match kind {
            SystemColumnKind::Topic => Arc::new(StringArray::from_iter_values(
                std::iter::repeat(
                    std::str::from_utf8(&position.filename).map_err(|error| {
                        anyhow::anyhow!("MySQL binlog filename is not valid UTF-8: {error}")
                    })?,
                )
                .take(len),
            )) as ArrayRef,
            SystemColumnKind::Partition => {
                Arc::new(Int64Array::from(vec![0_i64; len])) as ArrayRef
            }
            SystemColumnKind::Offset => {
                Arc::new(Int64Array::from(vec![offset; len])) as ArrayRef
            }
            SystemColumnKind::MessageIndex => Arc::new(UInt64Array::from_iter_values(
                changes.iter().map(|change| change.message_index),
            )) as ArrayRef,
            SystemColumnKind::ChangeOperation => Arc::new(StringArray::from_iter_values(
                changes.iter().map(|change| change.operation.code()),
            )) as ArrayRef,
            SystemColumnKind::ChangedColumns => Arc::new(BinaryArray::from_iter_values(
                changed_columns.iter().map(Vec::as_slice),
            )) as ArrayRef,
            SystemColumnKind::WriteTimestampMs => {
                anyhow::bail!("MySQL replication does not expose a write timestamp")
            }
        });
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    let system_start = table
        .columns
        .len()
        .checked_mul(2)
        .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC system-column index overflow"))?;
    let system_columns: Vec<SystemColumn> = MYSQL_REPLICATION_SYSTEM_COLUMNS
        .iter()
        .enumerate()
        .map(|(offset, kind)| SystemColumn {
            kind: *kind,
            index: system_start + offset,
            name: Arc::from(kind.default_name()),
        })
        .collect();
    Ok(TableData::new(
        Arc::from(table.config.name.as_str()),
        false,
        batch,
        SystemColumns::new(system_columns),
    ))
}

fn changed_columns_mask(
    table: &DiscoveredTable,
    change: &BufferedRowChange,
) -> anyhow::Result<Vec<u8>> {
    let mut mask = vec![0_u8; table.columns.len().div_ceil(8)];
    for (index, column) in table.columns.iter().enumerate() {
        let changed = match change.operation {
            ChangeOperation::Create | ChangeOperation::SnapshotRead => true,
            ChangeOperation::Delete => column.primary_key,
            ChangeOperation::Update => {
                let before = change
                    .before
                    .as_ref()
                    .and_then(|row| row.get(index))
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL update before-image omits column '{}'",
                            column.name
                        )
                    })?;
                let after = change
                    .after
                    .as_ref()
                    .and_then(|row| row.get(index))
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL update after-image omits column '{}'",
                            column.name
                        )
                    })?;
                !mysql_values_equal(before, after)
            }
        };
        if changed {
            mask[index / 8] |= 1 << (index % 8);
        }
    }
    Ok(mask)
}

fn mysql_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Float(left), Value::Float(right)) => left.to_bits() == right.to_bits(),
        (Value::Double(left), Value::Double(right)) => left.to_bits() == right.to_bits(),
        _ => left == right,
    }
}

fn validate_replication_tables(
    source: &MySqlSourceIdentity,
    tables: &[DiscoveredTable],
    authoritative_tables: &[AuthoritativeTableIdentity],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        tables.len() == authoritative_tables.len() && !tables.is_empty(),
        "MySQL replication runtime table identity count differs from discovery"
    );
    let mut names = BTreeSet::new();
    for (table, authoritative) in tables.iter().zip(authoritative_tables) {
        anyhow::ensure!(
            authoritative.database == source.database
                && authoritative.table == table.config.name
                && authoritative.engine == table.engine
                && authoritative.columns.len() == table.columns.len()
                && table.schema.columns.len() == table.columns.len(),
            "MySQL replication runtime table identity differs from the authoritative discovery"
        );
        anyhow::ensure!(
            names.insert(table.config.name.as_str()),
            "MySQL replication repeats table '{}'",
            table.config.name
        );
        for ((column, discovered), exact) in table
            .columns
            .iter()
            .zip(&table.schema.columns)
            .zip(&authoritative.columns)
        {
            anyhow::ensure!(
                exact.name == column.name
                    && exact.column_type == column.column_type
                    && exact.nullable == column.nullable
                    && exact.character_set == column.character_set
                    && exact.collation == column.collation
                    && exact.collation_id == column.collation_id
                    && exact.extra == column.extra
                    && exact.generation_expression == column.generation_expression
                    && exact.primary_key_ordinal == column.primary_key_ordinal
                    && exact.primary_key_prefix_length == column.primary_key_prefix_length
                    && exact.primary_key_direction == column.primary_key_direction
                    && discovered.name == column.name
                    && discovered.data_type == column.kind.arrow_type()
                    && discovered.nullable == column.nullable
                    && discovered.primary_key == column.primary_key
                    && discovered.max_length == column.max_length,
                "MySQL replication runtime column identity differs from the authoritative discovery for '{}.{}'",
                source.database,
                table.config.name
            );
            validate_replication_column_plan(column)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_replication_column_plan(column: &ColumnPlan) -> anyhow::Result<()> {
    let data_type = mysql_data_type(&column.column_type);
    anyhow::ensure!(
        !matches!(
            data_type,
            "json" | "timestamp" | "time" | "enum" | "set" | "year"
        ),
        "MySQL replication does not yet support physical column type '{}' for column '{}': its row-binlog encoding cannot be converted losslessly to the discovered snapshot schema",
        column.column_type,
        column.name
    );
    if let Some(character_set) = column.character_set.as_deref() {
        anyhow::ensure!(
            matches!(character_set, "ascii" | "utf8mb3" | "utf8mb4"),
            "MySQL replication does not yet support character set '{}' for textual column '{}': raw binlog bytes cannot be assumed to be UTF-8",
            character_set,
            column.name
        );
        anyhow::ensure!(
            column.collation_id.is_some(),
            "MySQL replication requires the numeric collation id for textual column '{}'",
            column.name
        );
    }
    anyhow::ensure!(
        column
            .generation_expression
            .as_deref()
            .is_none_or(str::is_empty)
            && !has_column_type_modifier(&column.extra, "invisible"),
        "MySQL replication does not yet support generated or invisible column '{}' because its row-binlog physical image cannot be matched losslessly to the snapshot projection",
        column.name
    );
    Ok(())
}

pub(super) fn validate_selected_table_map(
    table: &DiscoveredTable,
    table_map: &super::decoder::MySqlTableIdentity,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        table_map.table == table.config.name.as_bytes()
            && usize::try_from(table_map.columns)? == table.columns.len()
            && table_map.column_identities.len() == table.columns.len(),
        "MySQL binlog table-map physical layout differs from discovery for table '{}'",
        table.config.name
    );
    for (index, (expected, actual)) in table
        .columns
        .iter()
        .zip(&table_map.column_identities)
        .enumerate()
    {
        let expected_type = expected_binlog_column_type(expected)?;
        let expected_unsigned = expected_type
            .is_numeric_type()
            .then(|| has_column_type_modifier(&expected.column_type, "unsigned"));
        let expected_collation = expected.collation_id.or_else(|| {
            matches!(
                mysql_data_type(&expected.column_type),
                "binary"
                    | "varbinary"
                    | "tinyblob"
                    | "blob"
                    | "mediumblob"
                    | "longblob"
            )
            .then_some(63)
        });
        anyhow::ensure!(
            actual.name == expected.name.as_bytes()
                && actual.column_type == expected_type
                && actual.nullable == expected.nullable
                && actual.unsigned == expected_unsigned
                && actual.collation_id == expected_collation
                && actual.primary_key_ordinal == expected.primary_key_ordinal
                && actual.primary_key_prefix_length == expected.primary_key_prefix_length,
            "MySQL binlog table-map column {} differs from authoritative physical identity for '{}.{}'",
            index + 1,
            table.config.name,
            expected.name
        );
        validate_column_metadata(expected, actual)?;
    }
    Ok(())
}

fn expected_binlog_column_type(column: &ColumnPlan) -> anyhow::Result<mysql_async::consts::ColumnType> {
    use mysql_async::consts::ColumnType;

    let column_type = match mysql_data_type(&column.column_type) {
        "decimal" | "numeric" => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        "tinyint" => ColumnType::MYSQL_TYPE_TINY,
        "smallint" => ColumnType::MYSQL_TYPE_SHORT,
        "mediumint" => ColumnType::MYSQL_TYPE_INT24,
        "int" | "integer" => ColumnType::MYSQL_TYPE_LONG,
        "bigint" => ColumnType::MYSQL_TYPE_LONGLONG,
        "float" => ColumnType::MYSQL_TYPE_FLOAT,
        "double" | "real" => ColumnType::MYSQL_TYPE_DOUBLE,
        "bit" => ColumnType::MYSQL_TYPE_BIT,
        "binary" | "char" => ColumnType::MYSQL_TYPE_STRING,
        "varbinary" | "varchar" | "inet4" | "inet6" | "uuid" => {
            ColumnType::MYSQL_TYPE_VARCHAR
        }
        "tinyblob" | "blob" | "mediumblob" | "longblob" | "tinytext" | "text"
        | "mediumtext" | "longtext" => ColumnType::MYSQL_TYPE_BLOB,
        "json" => ColumnType::MYSQL_TYPE_JSON,
        "date" => ColumnType::MYSQL_TYPE_NEWDATE,
        "datetime" => ColumnType::MYSQL_TYPE_DATETIME2,
        "timestamp" => ColumnType::MYSQL_TYPE_TIMESTAMP2,
        "time" => ColumnType::MYSQL_TYPE_TIME2,
        "year" => ColumnType::MYSQL_TYPE_YEAR,
        "enum" => ColumnType::MYSQL_TYPE_ENUM,
        "set" => ColumnType::MYSQL_TYPE_SET,
        "geometry" | "point" | "linestring" | "polygon" | "multipoint"
        | "multilinestring" | "multipolygon" | "geometrycollection" => {
            ColumnType::MYSQL_TYPE_GEOMETRY
        }
        "vector" => ColumnType::MYSQL_TYPE_VECTOR,
        unsupported => anyhow::bail!(
            "MySQL replication has no table-map type contract for physical type '{}' on column '{}'",
            unsupported,
            column.name
        ),
    };
    Ok(column_type)
}

fn validate_column_metadata(
    expected: &ColumnPlan,
    actual: &super::decoder::MySqlBinlogColumnIdentity,
) -> anyhow::Result<()> {
    use mysql_async::consts::ColumnType;

    let valid = match actual.column_type {
        ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_YEAR
        | ColumnType::MYSQL_TYPE_NEWDATE => actual.metadata.is_empty(),
        ColumnType::MYSQL_TYPE_FLOAT => actual.metadata == [4],
        ColumnType::MYSQL_TYPE_DOUBLE => actual.metadata == [8],
        ColumnType::MYSQL_TYPE_TIME2
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            actual.metadata == [fractional_seconds_precision(&expected.column_type)?]
        }
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            actual.metadata == [blob_length_bytes(mysql_data_type(&expected.column_type))?]
        }
        ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_VECTOR => actual.metadata == [4],
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let (precision, scale) = decimal_precision_scale(&expected.column_type)?;
            actual.metadata == [precision, scale]
        }
        ColumnType::MYSQL_TYPE_BIT => {
            let bits = single_type_parameter(&expected.column_type)?;
            actual.metadata == [bits % 8, bits / 8]
        }
        ColumnType::MYSQL_TYPE_VARCHAR => {
            let maximum_bytes = u16::try_from(character_maximum_bytes(expected)?)?;
            actual.metadata == maximum_bytes.to_le_bytes()
        }
        ColumnType::MYSQL_TYPE_STRING => {
            let maximum_bytes = u16::try_from(character_maximum_bytes(expected)?)?;
            actual.metadata == string_metadata(maximum_bytes)
        }
        ColumnType::MYSQL_TYPE_ENUM | ColumnType::MYSQL_TYPE_SET => actual.metadata.len() == 2,
        _ => false,
    };
    anyhow::ensure!(
        valid,
        "MySQL binlog table-map metadata differs from authoritative physical type '{}' for column '{}'",
        expected.column_type,
        expected.name
    );
    Ok(())
}

fn character_maximum_bytes(column: &ColumnPlan) -> anyhow::Result<usize> {
    let characters = column.max_length.ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL character column '{}' has no declared maximum length",
            column.name
        )
    })?;
    let bytes_per_character = match column.character_set.as_deref() {
        None | Some("ascii") => 1,
        Some("utf8mb3") => 3,
        Some("utf8mb4") => 4,
        Some(character_set) => anyhow::bail!(
            "MySQL replication has no exact width mapping for character set '{character_set}'"
        ),
    };
    characters
        .checked_mul(bytes_per_character)
        .ok_or_else(|| anyhow::anyhow!("MySQL character maximum byte length overflow"))
}

fn string_metadata(maximum_bytes: u16) -> [u8; 2] {
    let [low, high] = maximum_bytes.to_le_bytes();
    let high_length_bits = (high & 0x03) << 4;
    [
        (mysql_async::consts::ColumnType::MYSQL_TYPE_STRING as u8) ^ high_length_bits,
        low,
    ]
}

fn mysql_data_type(column_type: &str) -> &str {
    let end = column_type
        .find(|character: char| character == '(' || character.is_ascii_whitespace())
        .unwrap_or(column_type.len());
    &column_type[..end]
}

fn has_column_type_modifier(column_type: &str, modifier: &str) -> bool {
    column_type
        .split_ascii_whitespace()
        .any(|token| token.eq_ignore_ascii_case(modifier))
}

fn single_type_parameter(column_type: &str) -> anyhow::Result<u8> {
    let parameters = type_parameters(column_type)?;
    parameters.parse::<u8>().map_err(|_| {
        anyhow::anyhow!("MySQL physical type '{column_type}' has an invalid numeric parameter")
    })
}

fn fractional_seconds_precision(column_type: &str) -> anyhow::Result<u8> {
    if column_type.contains('(') {
        let precision = single_type_parameter(column_type)?;
        anyhow::ensure!(precision <= 6, "MySQL temporal precision exceeds 6");
        Ok(precision)
    } else {
        Ok(0)
    }
}

fn decimal_precision_scale(column_type: &str) -> anyhow::Result<(u8, u8)> {
    let parameters = type_parameters(column_type)?;
    let (precision, scale) = parameters.split_once(',').ok_or_else(|| {
        anyhow::anyhow!("MySQL decimal physical type '{column_type}' omits precision or scale")
    })?;
    Ok((
        precision.trim().parse::<u8>()?,
        scale.trim().parse::<u8>()?,
    ))
}

fn type_parameters(column_type: &str) -> anyhow::Result<&str> {
    let start = column_type.find('(').ok_or_else(|| {
        anyhow::anyhow!("MySQL physical type '{column_type}' omits required parameters")
    })?;
    let end = column_type.rfind(')').ok_or_else(|| {
        anyhow::anyhow!("MySQL physical type '{column_type}' has unterminated parameters")
    })?;
    anyhow::ensure!(start < end, "MySQL physical type has empty parameter framing");
    Ok(&column_type[start + 1..end])
}

fn blob_length_bytes(data_type: &str) -> anyhow::Result<u8> {
    match data_type {
        "tinyblob" | "tinytext" => Ok(1),
        "blob" | "text" => Ok(2),
        "mediumblob" | "mediumtext" => Ok(3),
        "longblob" | "longtext" => Ok(4),
        _ => anyhow::bail!("MySQL physical BLOB metadata has unexpected type '{data_type}'"),
    }
}

fn validate_row_columns(
    table: &DiscoveredTable,
    row: Option<&[Option<Value>]>,
) -> anyhow::Result<()> {
    let Some(row) = row else {
        return Ok(());
    };
    anyhow::ensure!(
        row.len() == table.columns.len(),
        "MySQL binlog row width changed after discovery for table '{}'",
        table.config.name
    );
    Ok(())
}

const fn change_operation(operation: MySqlRowOperation) -> ChangeOperation {
    match operation {
        MySqlRowOperation::Write => ChangeOperation::Create,
        MySqlRowOperation::Update => ChangeOperation::Update,
        MySqlRowOperation::Delete => ChangeOperation::Delete,
    }
}

fn system_time_micros() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| anyhow::anyhow!("system clock precedes Unix epoch: {error}"))?
            .as_micros(),
    )?)
}

const fn empty_batch() -> SourceBatch {
    SourceBatch::Typed {
        tables: Vec::new(),
        source_rows: 0,
        commit_marker: None,
        memory: Vec::new(),
    }
}

async fn observe_bounded_mysql_request<T>(
    cancellation: &CancellationToken,
    timeout: Duration,
    operation: &'static str,
    request: impl Future<Output = mysql_async::Result<T>>,
) -> anyhow::Result<T> {
    observe_external_request("mysql", operation, async {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                anyhow::bail!("MySQL external request '{operation}' cancelled")
            }
            result = tokio::time::timeout(timeout, request) => match result {
                Ok(result) => result.map_err(anyhow::Error::from),
                Err(_) => anyhow::bail!(
                    "MySQL external request '{operation}' exceeded its configured timeout"
                ),
            }
        }
    })
    .await
}

fn classify_binlog_start_error(error: anyhow::Error) -> anyhow::Error {
    if is_purged_binlog_error(&error) {
        replication_safety_violation(anyhow::anyhow!(
            "the exact MySQL binlog resume position is no longer available: {error}"
        ))
    } else {
        error
    }
}

fn classify_binlog_read_error(error: MySqlError) -> DataPlaneFailure {
    let error = anyhow::Error::from(error);
    if is_purged_binlog_error(&error) {
        DataPlaneFailure::fatal(replication_safety_violation(anyhow::anyhow!(
            "the exact MySQL binlog resume position is no longer available: {error}"
        )))
    } else if error.downcast_ref::<MySqlError>().is_some_and(|error| {
        matches!(
            error,
            MySqlError::Io(_)
                | MySqlError::Driver(mysql_async::DriverError::ConnectionClosed)
        )
    }) {
        DataPlaneFailure::retryable(error)
    } else {
        DataPlaneFailure::fatal(replication_safety_violation(error))
    }
}

fn is_purged_binlog_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<MySqlError>()
        .is_some_and(|error| matches!(error, MySqlError::Server(server) if server.code == 1236))
}
