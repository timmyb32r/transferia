use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use mysql_async::binlog::jsonb::Value as JsonbValue;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, Conn, Error as MySqlError, Value};
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME, META_CHANGE_OPERATION,
    META_LOW_CARDINALITY, META_MAX_LENGTH, META_OLD_KEY_OF, META_OLD_VALUE_OF, META_PRIMARY_KEY,
    META_SYSTEM_ROLE,
};
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
use super::position::{format_uuid, GtidSet, MySqlBinlogPosition, MySqlResumePosition};
use super::MySqlReplicationConfig;
use crate::connectors::mysql::src_batch::optional_value_column_array;
use crate::connectors::mysql::src_batch::{
    mysql_column_kind, old_value_schema_column, ColumnPlan, DiscoveredTable, MySqlColumnKind,
    MYSQL_REPLICATION_SYSTEM_COLUMNS, MYSQL_SOURCE_METADATA_COLUMNS,
};
use crate::connectors::mysql::src_batch_and_stream::{
    is_replication_safety_violation, replication_safety_violation, AuthoritativeTableIdentity,
    MySqlBinlogBoundary, MySqlSourceIdentity,
};
use crate::metrics::SourceCounters;

pub struct MySqlReplicationSource {
    stream: Option<BinlogStream>,
    decoder: MySqlBinlogDecoder,
    config: MySqlReplicationConfig,
    tables: Vec<DiscoveredTable>,
    schemas: Vec<Arc<Schema>>,
    table_indexes: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, usize>>,
    active: Option<BufferedTransaction>,
    committed_position: MySqlBinlogPosition,
    emitted_position: MySqlBinlogPosition,
    committed_gtids: GtidSet,
    emitted_gtids: GtidSet,
    offset_tracker: MySqlReplicationOffsetTracker,
    memory: PipelineMemory,
    schema_memory: MemoryReservation,
    event_decode_admission_bytes: usize,
    counters: Arc<SourceCounters>,
    cancellation: CancellationToken,
    finished: bool,
    admission_config: crate::connectors::mysql::src_batch::MySqlSourceConfig,
    pending_table: Option<PendingTable>,
    admitted_schema_memory: Vec<MemoryReservation>,
}

struct PendingTable {
    table: DiscoveredTable,
    authoritative: AuthoritativeTableIdentity,
    schema: Arc<Schema>,
    memory: MemoryReservation,
    position: MySqlBinlogPosition,
    event_decode_admission_bytes: usize,
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
    source_server_id: u32,
    source_binlog_position: u32,
    source_row_in_event: i32,
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
        admission_config: crate::connectors::mysql::src_batch::MySqlSourceConfig,
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
        validate_replication_tables(&tables, &authoritative_tables)
            .map_err(replication_safety_violation)?;
        let schema_admission = tables
            .iter()
            .try_fold(size_of::<Vec<Arc<Schema>>>(), |bytes, table| {
                bytes
                    .checked_add(schema_materialization_admission_bytes(table)?)
                    .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema admission overflow"))
            })
            .map_err(replication_safety_violation)?;
        let schema_memory = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                anyhow::bail!("MySQL CDC schema construction cancelled")
            }
            reservation = memory.reserve(schema_admission) => reservation,
        };
        let schemas = tables
            .iter()
            .map(build_table_schema)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(replication_safety_violation)?;
        let retained_schema_bytes =
            retained_schemas_bytes(&schemas).map_err(replication_safety_violation)?;
        let _ = schema_memory.shrink_to(retained_schema_bytes);
        let event_decode_admission_bytes =
            event_decode_admission_bytes(&config, &tables)
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
        decoder.set_table_selection(admission_config.tables.compile()?);
        decoder.retain_rows_for_tables(
            tables
                .iter()
                .map(|table| (table.config.database.as_bytes().to_vec(), table.config.name.as_bytes().to_vec())),
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
        let mut table_indexes = BTreeMap::<Vec<u8>, BTreeMap<Vec<u8>, usize>>::new();
        for (index, table) in tables.iter().enumerate() {
            table_indexes.entry(table.config.database.as_bytes().to_vec()).or_default()
                .insert(table.config.name.as_bytes().to_vec(), index);
        }
        Ok(Self {
            stream: Some(stream),
            decoder,
            config,
            tables,
            schemas,
            table_indexes,
            active: None,
            committed_position: committed_position.clone(),
            emitted_position: committed_position,
            committed_gtids: committed_gtids.clone(),
            emitted_gtids: committed_gtids,
            offset_tracker,
            memory,
            schema_memory,
            event_decode_admission_bytes,
            counters,
            cancellation,
            finished: false,
            admission_config,
            pending_table: None,
            admitted_schema_memory: Vec::new(),
        })
    }

    async fn admit_created_table(
        &mut self,
        identity: transferia_registry::TableIdentity,
        committed: CommittedTransaction,
    ) -> anyhow::Result<SourceBatch> {
        use crate::connectors::mysql::src_batch::{
            authoritative_table_identities, build_delivery_discovery, discover_table, TableConfig,
        };
        if !self.admission_config.includes_database(&identity.namespace) {
            return self.finish_transaction(committed);
        }
        let classification = self.admission_config.tables.compile()?.classify(&identity);
        anyhow::ensure!(classification.issues.is_empty(),
            "MySQL newly created table {:?} conflicts with configured table rules: {:?}",
            identity.qualified_name(), classification.issues);
        if classification.selected_by.is_empty()
            || self.admission_config.new_tables == crate::connectors::mysql::src_batch::NewTables::Ignore {
            return self.finish_transaction(committed);
        }
        anyhow::ensure!(self.pending_table.is_none(), "MySQL dataset admission is already pending");
        anyhow::ensure!(self.tables.iter().all(|table| table.config.name != identity.name),
            "MySQL newly created table {:?} has a name already used by another selected table",
            identity.qualified_name());
        anyhow::ensure!(self.active.as_ref().is_some_and(|active| active.changes.is_empty()),
            "MySQL CREATE transaction unexpectedly contains rows; a snapshot is required");
        let timeout = Duration::from_millis(self.config.bootstrap_timeout_ms);
        let discovery = observe_external_request("mysql", "discover_created_table", async {
                let mut connection = crate::connectors::mysql::common::connect(
                    &self.admission_config.connection).await?;
                let result = discover_table(&mut connection, &identity.namespace, TableConfig {
                    database: identity.namespace.clone(), name: identity.name.clone(),
                }, true, self.admission_config.read_protocol).await;
                let closed = connection.disconnect().await;
                let table = result?;
                closed?;
                Ok::<_, anyhow::Error>(table)
            });
        let table = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => anyhow::bail!("MySQL table discovery cancelled"),
            result = tokio::time::timeout(timeout, discovery) => result
                .map_err(|_| anyhow::anyhow!("MySQL table discovery timed out"))??,
        };
        let authoritative = authoritative_table_identities(std::slice::from_ref(&table));
        validate_replication_tables(std::slice::from_ref(&table), &authoritative)?;
        let schema_bytes = schema_materialization_admission_bytes(&table)?;
        let retained = retained_schemas_bytes(&self.schemas)?;
        if let Some(active) = self.active.as_ref() {
            // The Query event has been decoded and dropped. Its worst-case
            // row decoding reservation must not pin capacity during metadata I/O.
            let _ = active.memory.shrink_to(retained_transaction_bytes(active)?);
        }
        let active_bytes = self.active.as_ref().map(|active| active.memory.bytes()).unwrap_or(0);
        anyhow::ensure!(retained.checked_add(schema_bytes).and_then(|bytes| bytes.checked_add(active_bytes))
            .is_some_and(|bytes| bytes <= self.memory.limit()),
            "MySQL admitted table schemas exceed the configured pipeline memory limit");
        let reservation = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => anyhow::bail!("MySQL table admission cancelled"),
            reservation = self.memory.reserve(schema_bytes) => reservation,
        };
        let schema = build_table_schema(&table)?;
        let _ = reservation.shrink_to(retained_schemas_bytes(std::slice::from_ref(&schema))?);
        let mut all_tables = self.tables.clone();
        all_tables.push(table.clone());
        let event_decode_admission_bytes = event_decode_admission_bytes(&self.config, &all_tables)?;
        let mut discovery = build_delivery_discovery(true,
            transferia_delivery_contracts::DeliveryType::Stream,
            transferia_core::delivery::DeliveryDiscoveryRequest { keep_system_columns: false },
            std::slice::from_ref(&table))?;
        let position = committed.next_position.clone();
        let SourceBatch::Typed { tables, commit_marker: Some(commit_marker), mut memory, .. } =
            self.finish_transaction(committed)? else {
                anyhow::bail!("MySQL CREATE did not produce a durable marker")
            };
        anyhow::ensure!(tables.is_empty(), "MySQL CREATE produced unexpected row data");
        memory.push(reservation.clone());
        self.pending_table = Some(PendingTable {
            table, authoritative: authoritative.into_iter().next().expect("one table"),
            schema, memory: reservation, position, event_decode_admission_bytes,
        });
        Ok(SourceBatch::Dataset {
            dataset: Box::new(discovery.datasets.remove(0)), commit_marker, memory,
        })
    }

    fn accept_event(
        &mut self,
        event: DecodedBinlogEvent,
        event_reservation: Option<MemoryReservation>,
        event_bytes: usize,
    ) -> anyhow::Result<Option<SourceBatch>> {
        match event {
            DecodedBinlogEvent::TableCreated { table, .. } => {
                anyhow::bail!("MySQL created table {:?} requires dataset admission before its CREATE position can be committed", table.qualified_name())
            }
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
                active.table_map_count = active
                    .table_map_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("MySQL retained table-map count overflow"))?;
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
        let Some(table_index) = self.table_indexes.get(rows.table.database.as_slice())
            .and_then(|tables| tables.get(rows.table.table.as_slice())).copied() else {
            return Ok(());
        };
        let table = self.tables.get(table_index).ok_or_else(|| {
            anyhow::anyhow!("MySQL configured table index disappeared during replication")
        })?;
        validate_selected_table_map(table, &rows.table)?;
        anyhow::ensure!(
            usize::try_from(rows.table.columns)? == table.columns.len(),
            "MySQL binlog table '{}.{}' has {} columns, discovery declared {}",
            table.config.database,
            table.config.name,
            rows.table.columns,
            table.columns.len()
        );
        anyhow::ensure!(
            rows.event_position.filename == active.marker.begin_position.filename,
            "MySQL rows event filename changed inside an active transaction"
        );
        for row in rows.rows {
            let before = normalize_binlog_row(table, &rows.table, row.before)?;
            let after = normalize_binlog_row(table, &rows.table, row.after)?;
            let message_index = active.next_message_index;
            active.next_message_index = active
                .next_message_index
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("MySQL transaction message index overflow"))?;
            active.changes.push(BufferedRowChange {
                table_index,
                operation: change_operation(rows.operation),
                before,
                after,
                message_index,
                source_timestamp_seconds: rows.source_timestamp_seconds,
                source_server_id: rows.source_server_id,
                source_binlog_position: rows.event_position.position,
                source_row_in_event: row.row_in_event,
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
        let source_gtid = format_transaction_gtid(&active.marker.identity)?;
        let source_binlog_file = std::str::from_utf8(&active.marker.begin_position.filename)
            .map_err(|error| {
                anyhow::anyhow!("MySQL binlog filename is not valid UTF-8: {error}")
            })?;
        let mut grouped = (0..self.tables.len())
            .map(|_| Vec::<&BufferedRowChange>::new())
            .collect::<Vec<_>>();
        for change in &active.changes {
            grouped
                .get_mut(change.table_index)
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL transaction refers to an unknown configured table index")
                })?
                .push(change);
        }
        let touched_table_count = grouped.iter().filter(|changes| !changes.is_empty()).count();
        let mut tables = Vec::with_capacity(touched_table_count);
        let mut row_count = 0_u64;
        for ((table, schema), changes) in self.tables.iter().zip(&self.schemas).zip(grouped) {
            if changes.is_empty() {
                continue;
            }
            row_count = row_count
                .checked_add(u64::try_from(changes.len())?)
                .ok_or_else(|| anyhow::anyhow!("MySQL replication row count overflow"))?;
            tables.push(changes_to_table_data(
                table,
                Arc::clone(schema),
                &table.config.database,
                &transaction_identity,
                source_binlog_file,
                &source_gtid,
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
            memory: vec![memory, self.schema_memory.clone()],
        })
    }

    fn validate_table_map(
        &self,
        table_map: &super::decoder::MySqlTableIdentity,
    ) -> anyhow::Result<()> {
        let Some(table_index) = self.table_indexes.get(table_map.database.as_slice())
            .and_then(|tables| tables.get(table_map.table.as_slice())).copied() else {
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
    verify_binlog_heartbeat(heartbeat_nanoseconds, observed).map_err(replication_safety_violation)
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
    fn restored_datasets(&self) -> transferia_core::failure::DataPlaneResult<Option<Vec<transferia_core::DiscoveredDataset>>> {
        crate::connectors::mysql::src_batch::build_delivery_discovery(true,
            transferia_delivery_contracts::DeliveryType::Stream,
            transferia_core::delivery::DeliveryDiscoveryRequest { keep_system_columns: false },
            &self.tables).map(|discovery| Some(discovery.datasets)).map_err(DataPlaneFailure::fatal)
    }

    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(SourceBatch::Finished);
            }
            if self.pending_table.is_some() {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                    "MySQL cannot read beyond CREATE before dataset admission is committed")));
            }
            loop {
                let previous_transaction_bytes = self
                    .active
                    .as_ref()
                    .map(retained_transaction_bytes)
                    .transpose()
                    .map_err(|error| DataPlaneFailure::fatal(replication_safety_violation(error)))?
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
                    let reserve = self.memory.reserve_progress_source(read_admission_bytes);
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
                        )));
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
                                    anyhow::anyhow!("MySQL transaction memory accounting overflow"),
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
                        | DecodedBinlogEvent::TableCreated { .. }
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
                if let DecodedBinlogEvent::TableCreated { table, committed } = decoded {
                    return self.admit_created_table(table, committed).await
                        .map_err(|error| DataPlaneFailure::fatal(replication_safety_violation(error)));
                }
                if let Some(batch) = self
                    .accept_event(decoded, event_reservation, event_bytes)
                    .map_err(|error| DataPlaneFailure::fatal(replication_safety_violation(error)))?
                {
                    return Ok(batch);
                }
                if let Some(transaction) = self.active.as_ref() {
                    let retained_bytes =
                        retained_transaction_bytes(transaction).map_err(|error| {
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
                if marker.previous_position != expected || marker.previous_gtids != expected_gtids {
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
            let admitted = self.pending_table.as_ref()
                .filter(|pending| pending.position == expected)
                .map(|pending| std::slice::from_ref(&pending.authoritative)).unwrap_or(&[]);
            let stored = if admitted.is_empty() {
                self.offset_tracker.store(&expected, &expected_gtids).await
            } else {
                self.offset_tracker.store_admission(&expected, &expected_gtids, admitted).await
            };
            stored
                .map_err(|error| {
                    if is_replication_safety_violation(&error) {
                        DataPlaneFailure::fatal(error)
                    } else {
                        DataPlaneFailure::retryable(error)
                    }
                })?;
            if self.pending_table.as_ref().is_some_and(|pending| pending.position == expected) {
                let pending = self.pending_table.take().expect("checked pending table");
                self.table_indexes.entry(pending.table.config.database.as_bytes().to_vec())
                    .or_default().insert(pending.table.config.name.as_bytes().to_vec(), self.tables.len());
                self.tables.push(pending.table);
                self.schemas.push(pending.schema);
                self.admitted_schema_memory.push(pending.memory);
                self.event_decode_admission_bytes = pending.event_decode_admission_bytes;
                self.decoder.retain_rows_for_tables(self.tables.iter().map(|table| (
                    table.config.database.as_bytes().to_vec(), table.config.name.as_bytes().to_vec())));
            }
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

/// Conservatively covers the raw event, `mysql_common`'s decoded rows, and the
/// connector-owned normalized rows while one event is being moved into the
/// transaction buffer. JSON strings can expand to six escaped bytes per input
/// byte; the factor also covers exact owned JSONB copies and Vec capacity slack.
fn event_decode_admission_bytes(
    config: &MySqlReplicationConfig,
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
                size_of::<super::decoder::MySqlRowChange>() + size_of::<BufferedRowChange>(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC decoded-row memory accounting overflow"))?;
    let table_map_state = max_columns
        .checked_mul(
            size_of::<mysql_async::Column>()
                + size_of::<super::decoder::MySqlBinlogColumnIdentity>(),
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC table-map memory accounting overflow"))?;
    let row_column_state = tables
        .iter()
        .map(|table| {
            table.columns.iter().try_fold(0_usize, |bytes, column| {
                bytes
                    .checked_add(size_of::<mysql_async::Column>())
                    .and_then(|value| value.checked_add(table.config.database.len()))
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
    let fixed_binary_padding_state = tables
        .iter()
        .map(|table| {
            table
                .columns
                .iter()
                .filter(|column| column.data_type == "binary")
                .try_fold(0_usize, |bytes, column| {
                    bytes
                        .checked_add(column.character_octet_length.ok_or_else(|| {
                            anyhow::anyhow!(
                                "MySQL BINARY column '{}' has no authoritative octet length",
                                column.name
                            )
                        })?)
                        .ok_or_else(|| {
                            anyhow::anyhow!("MySQL CDC fixed-binary memory accounting overflow")
                        })
                })
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .max()
        .unwrap_or(0)
        .checked_mul(row_images)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC fixed-binary memory accounting overflow"))?;
    let raw_and_expanded_payload = config
        .max_transaction_bytes
        .checked_mul(16)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC event-payload memory accounting overflow"))?;
    raw_and_expanded_payload
        .checked_add(decoded_value_state)
        .and_then(|bytes| bytes.checked_add(row_state))
        .and_then(|bytes| bytes.checked_add(table_map_state))
        .and_then(|bytes| bytes.checked_add(row_column_state))
        .and_then(|bytes| bytes.checked_add(fixed_binary_padding_state))
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
        MySqlTransactionIdentity::Gtid { tag, .. } => tag.as_ref().map_or(0, String::capacity),
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
    tables: &[DiscoveredTable],
    next_position: &MySqlBinlogPosition,
    previous_position: &MySqlBinlogPosition,
    previous_gtids: &GtidSet,
) -> anyhow::Result<usize> {
    let transaction_identity_bytes =
        transaction_identity_encoded_len(&transaction.marker.identity)?;
    let transaction_gtid_bytes = transaction_gtid_text_len(&transaction.marker.identity)?;
    let mut bytes = transaction
        .changes
        .len()
        .checked_mul(2)
        .and_then(|rows| rows.checked_mul(size_of::<&BufferedRowChange>()))
        .and_then(|value| {
            value.checked_add(
                tables
                    .len()
                    .checked_mul(size_of::<Vec<&BufferedRowChange>>())?,
            )
        })
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC grouped-row memory accounting overflow"))?;
    bytes = bytes
        .checked_add(size_of::<String>())
        .and_then(|value| value.checked_add(transaction_gtid_bytes.checked_mul(2)?))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC GTID formatting admission overflow"))?;
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
        let metadata_payload = table.config.database
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(table.config.name.len()))
            .and_then(|value| value.checked_add(transaction_identity_bytes))
            .and_then(|value| value.checked_add(next_position.filename.len()))
            .and_then(|value| value.checked_add(transaction.marker.begin_position.filename.len()))
            .and_then(|value| value.checked_add(transaction_gtid_bytes))
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
    let touched_table_count = tables
        .iter()
        .enumerate()
        .filter(|(table_index, _)| {
            transaction
                .changes
                .iter()
                .any(|change| change.table_index == *table_index)
        })
        .count();
    bytes = bytes
        .checked_add(
            touched_table_count
                .checked_mul(size_of::<Arc<Schema>>() + size_of::<TableData>())
                .ok_or_else(|| {
                    anyhow::anyhow!("MySQL CDC table materialization accounting overflow")
                })?,
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC table materialization accounting overflow"))?;
    let marker_bytes =
        replication_marker_admission_bytes(previous_position, next_position, previous_gtids)?
            .checked_add(
                transaction_marker_heap_bytes(&transaction.marker)?
                    .checked_mul(4)
                    .ok_or_else(|| anyhow::anyhow!("MySQL CDC GTID marker accounting overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC marker memory accounting overflow"))?;
    bytes
        .checked_add(marker_bytes)
        .and_then(|value| value.checked_add(tables.len().checked_mul(size_of::<TableData>())?))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC materialization admission overflow"))
}

pub(super) fn schema_materialization_admission_bytes(
    table: &DiscoveredTable,
) -> anyhow::Result<usize> {
    let field_count = table
        .schema
        .columns
        .len()
        .checked_mul(2)
        .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
        .and_then(|value| value.checked_add(MYSQL_REPLICATION_SYSTEM_COLUMNS.len()))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema field count overflow"))?;
    let mut bytes = size_of::<Schema>()
        .checked_add(
            field_count
                .checked_mul(size_of::<Field>() + size_of::<Arc<Field>>())
                .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema field memory overflow"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema memory accounting overflow"))?;

    for (index, column) in table.schema.columns.iter().enumerate() {
        let (current_entries, current_payload) = schema_column_metadata_shape(column)?;
        bytes = bytes
            .checked_add(field_materialization_bytes(
                column.name.len(),
                current_entries,
                current_payload,
            )?)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC current field memory overflow"))?;

        let mut old_entries = 1_usize;
        let mut old_payload = META_OLD_VALUE_OF
            .len()
            .checked_add(column.name.len())
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC old-value metadata overflow"))?;
        if let Some(extension_name) = column.arrow_extension_name {
            add_metadata_shape(
                &mut old_entries,
                &mut old_payload,
                META_ARROW_EXTENSION_NAME,
                extension_name.len(),
            )?;
        }
        if let Some(extension_metadata) = &column.arrow_extension_metadata {
            add_metadata_shape(
                &mut old_entries,
                &mut old_payload,
                META_ARROW_EXTENSION_METADATA,
                extension_metadata.len(),
            )?;
        }
        let old_name_len = "_system_old_value_"
            .len()
            .checked_add(decimal_digit_count(index))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC old field name length overflow"))?;
        bytes = bytes
            .checked_add(field_materialization_bytes(
                old_name_len,
                old_entries,
                old_payload,
            )?)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC old field memory overflow"))?;
    }

    for column in MYSQL_SOURCE_METADATA_COLUMNS {
        let payload = META_SYSTEM_ROLE
            .len()
            .checked_add(column.role.len())
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC source metadata schema overflow"))?;
        bytes = bytes
            .checked_add(field_materialization_bytes(column.name.len(), 1, payload)?)
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC source metadata field overflow"))?;
    }
    for kind in MYSQL_REPLICATION_SYSTEM_COLUMNS {
        let (entries, payload) = if *kind == SystemColumnKind::ChangeOperation {
            (
                1,
                META_CHANGE_OPERATION
                    .len()
                    .checked_add("true".len())
                    .ok_or_else(|| anyhow::anyhow!("MySQL CDC system metadata overflow"))?,
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
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC system field memory overflow"))?;
    }

    // Schema::new moves each Field into an Arc allocation while the input Vec
    // is still live. The dynamic component above is exact from the discovered
    // names and metadata; doubling covers that bounded construction overlap and
    // allocator capacity without a fixed per-field guess.
    bytes
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema working-set accounting overflow"))
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
    if let Some(extension_name) = column.arrow_extension_name {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_ARROW_EXTENSION_NAME,
            extension_name.len(),
        )?;
    }
    if let Some(extension_metadata) = &column.arrow_extension_metadata {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_ARROW_EXTENSION_METADATA,
            extension_metadata.len(),
        )?;
    }
    if let Some(role) = &column.system_role {
        add_metadata_shape(&mut entries, &mut payload, META_SYSTEM_ROLE, role.len())?;
    }
    if let Some(current_column) = &column.old_value_of {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_OLD_VALUE_OF,
            current_column.len(),
        )?;
    }
    if let Some(current_column) = &column.old_key_of {
        add_metadata_shape(
            &mut entries,
            &mut payload,
            META_OLD_KEY_OF,
            current_column.len(),
        )?;
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
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema metadata entry count overflow"))?;
    *payload = payload
        .checked_add(key.len())
        .and_then(|value| value.checked_add(value_len))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema metadata payload overflow"))?;
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
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow field materialization overflow"))
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
            let optional_tag_bytes = tag
                .as_ref()
                .map_or(Some(1), |tag| 9_usize.checked_add(tag.len()));
            optional_tag_bytes
                .and_then(|tag_bytes| 16_usize.checked_add(tag_bytes))
                .and_then(|value| value.checked_add(8))
        }
        MySqlTransactionIdentity::Anonymous { begin_position }
        | MySqlTransactionIdentity::FilePosition { begin_position } => {
            begin_position.filename.len().checked_add(size_of::<u32>())
        }
    }
    .ok_or_else(|| anyhow::anyhow!("MySQL transaction identity length overflow"))?;
    b"transferia.mysql.source-transaction-id"
        .len()
        .checked_add(2)
        .and_then(|value| value.checked_add(fields))
        .and_then(|value| value.checked_add(3 * (1 + size_of::<u64>())))
        .ok_or_else(|| anyhow::anyhow!("MySQL transaction identity length overflow"))
}

fn transaction_gtid_text_len(identity: &MySqlTransactionIdentity) -> anyhow::Result<usize> {
    let MySqlTransactionIdentity::Gtid { tag, gno, .. } = identity else {
        anyhow::bail!("MySQL GTID-mode replication emitted a row without an exact GTID")
    };
    let tag_bytes = tag
        .as_ref()
        .map(|tag| {
            tag.len()
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("MySQL GTID tag length overflow"))
        })
        .transpose()?
        .unwrap_or(0);
    36_usize
        .checked_add(tag_bytes)
        .and_then(|value| value.checked_add(1))
        .and_then(|value| value.checked_add(decimal_digit_count_u64(*gno)))
        .ok_or_else(|| anyhow::anyhow!("MySQL GTID text length overflow"))
}

const fn decimal_digit_count_u64(mut value: u64) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

pub(super) fn format_transaction_gtid(
    identity: &MySqlTransactionIdentity,
) -> anyhow::Result<String> {
    let MySqlTransactionIdentity::Gtid { sid, tag, gno } = identity else {
        anyhow::bail!("MySQL GTID-mode replication emitted a row without an exact GTID")
    };
    let capacity = transaction_gtid_text_len(identity)?;
    let mut gtid = String::new();
    gtid.try_reserve_exact(capacity)
        .map_err(|error| anyhow::anyhow!("failed to reserve MySQL GTID text: {error}"))?;
    gtid.push_str(&format_uuid(*sid));
    if let Some(tag) = tag {
        gtid.push(':');
        gtid.push_str(tag);
    }
    gtid.push(':');
    gtid.push_str(&gno.to_string());
    anyhow::ensure!(
        gtid.len() == capacity,
        "MySQL GTID text length accounting diverged from formatting"
    );
    Ok(gtid)
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
        .and_then(|value| value.checked_add(2 * size_of::<MemoryReservation>()))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC output container accounting overflow"))?;
    for table in tables {
        bytes = bytes
            .checked_add(table.batch.get_array_memory_size())
            .and_then(|value| {
                value.checked_add(table.table.len().checked_add(2 * size_of::<usize>())?)
            })
            .and_then(|value| value.checked_add(size_of::<Arc<Schema>>()))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC Arrow batch memory accounting overflow"))?;
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
            bytes = bytes
                .checked_add(
                    column
                        .name
                        .len()
                        .checked_add(2 * size_of::<usize>())
                        .ok_or_else(|| {
                            anyhow::anyhow!("MySQL CDC system-column name accounting overflow")
                        })?,
                )
                .ok_or_else(|| {
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

pub(super) fn build_table_schema(table: &DiscoveredTable) -> anyhow::Result<Arc<Schema>> {
    anyhow::ensure!(
        table.columns.len() == table.schema.columns.len(),
        "MySQL CDC physical and discovered schema widths differ for table '{}'",
        table.config.name
    );
    let field_count = table
        .columns
        .len()
        .checked_mul(2)
        .and_then(|value| value.checked_add(MYSQL_SOURCE_METADATA_COLUMNS.len()))
        .and_then(|value| value.checked_add(MYSQL_REPLICATION_SYSTEM_COLUMNS.len()))
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC schema column count overflow"))?;
    let mut fields = Vec::with_capacity(field_count);
    for (column, discovered) in table.columns.iter().zip(&table.schema.columns) {
        anyhow::ensure!(
            column.name == discovered.name && column.kind.arrow_type() == discovered.data_type,
            "MySQL CDC schema drifted at column '{}'",
            column.name
        );
        fields.push(
            Field::new(&column.name, discovered.data_type.clone(), true)
                .with_metadata(discovered.arrow_metadata()),
        );
    }
    for (index, discovered) in table.schema.columns.iter().enumerate() {
        let old = old_value_schema_column(index, discovered);
        let old_metadata = old.arrow_metadata();
        fields.push(Field::new(old.name, old.data_type, old.nullable).with_metadata(old_metadata));
    }
    fields.extend(MYSQL_SOURCE_METADATA_COLUMNS.iter().map(|column| {
        Field::new(column.name, column.data_type.clone(), column.nullable).with_metadata(
            SchemaColumn::new(
                column.name.to_owned(),
                column.data_type.clone(),
                column.nullable,
            )
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
    Ok(Arc::new(Schema::new(fields)))
}

fn retained_schemas_bytes(schemas: &[Arc<Schema>]) -> anyhow::Result<usize> {
    let mut bytes = schemas
        .len()
        .checked_mul(size_of::<Arc<Schema>>())
        .ok_or_else(|| anyhow::anyhow!("MySQL CDC retained schema vector overflow"))?;
    for schema in schemas {
        let arc_headers = schema
            .fields()
            .len()
            .checked_add(2)
            .and_then(|arcs| arcs.checked_mul(2 * size_of::<usize>()))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC retained Arrow Arc state overflow"))?;
        bytes = bytes
            .checked_add(size_of::<Schema>())
            .and_then(|value| value.checked_add(schema.fields().size()))
            .and_then(|value| value.checked_add(arc_headers))
            .ok_or_else(|| anyhow::anyhow!("MySQL CDC retained Arrow schema overflow"))?;
    }
    Ok(bytes)
}

fn changes_to_table_data(
    table: &DiscoveredTable,
    schema: Arc<Schema>,
    database: &str,
    transaction_identity: &[u8],
    source_binlog_file: &str,
    source_gtid: &str,
    event_timestamp_us: i64,
    position: &MySqlBinlogPosition,
    changes: &[&BufferedRowChange],
) -> anyhow::Result<TableData> {
    let mut arrays = Vec::with_capacity(schema.fields().len());
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
    for (index, column) in table.columns.iter().enumerate() {
        arrays.push(optional_value_column_array(&current_rows, index, column)?);
    }
    for (index, column) in table.columns.iter().enumerate() {
        arrays.push(optional_value_column_array(&old_rows, index, column)?);
    }

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
        Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
            transaction_identity,
            len,
        ))) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            changes
                .iter()
                .map(|change| i64::from(change.source_server_id)),
        )) as ArrayRef,
        Arc::new(StringArray::from(vec![source_gtid; len])) as ArrayRef,
        Arc::new(StringArray::from(vec![source_binlog_file; len])) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            changes
                .iter()
                .map(|change| i64::from(change.source_binlog_position)),
        )) as ArrayRef,
        Arc::new(Int32Array::from_iter_values(
            changes.iter().map(|change| change.source_row_in_event),
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
            SystemColumnKind::Topic => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                std::str::from_utf8(&position.filename).map_err(|error| {
                    anyhow::anyhow!("MySQL binlog filename is not valid UTF-8: {error}")
                })?,
                len,
            ))) as ArrayRef,
            SystemColumnKind::Partition => Arc::new(Int64Array::from(vec![0_i64; len])) as ArrayRef,
            SystemColumnKind::Offset => Arc::new(Int64Array::from(vec![offset; len])) as ArrayRef,
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
    let batch = RecordBatch::try_new(schema, arrays)?;
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
                        anyhow::anyhow!("MySQL update before-image omits column '{}'", column.name)
                    })?;
                let after = change
                    .after
                    .as_ref()
                    .and_then(|row| row.get(index))
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        anyhow::anyhow!("MySQL update after-image omits column '{}'", column.name)
                    })?;
                column.primary_key || !mysql_values_equal(before, after)
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
            authoritative.database == table.config.database
                && authoritative.table == table.config.name
                && authoritative.engine == table.engine
                && authoritative.columns.len() == table.columns.len()
                && table.schema.columns.len() == table.columns.len(),
            "MySQL replication runtime table identity differs from the authoritative discovery"
        );
        anyhow::ensure!(
            names.insert((table.config.database.as_str(), table.config.name.as_str())),
            "MySQL replication repeats table '{}'",
            table.config.name
        );
        for ((column, discovered), exact) in table
            .columns
            .iter()
            .zip(&table.schema.columns)
            .zip(&authoritative.columns)
        {
            let extension_metadata = column.arrow_extension_metadata()?;
            anyhow::ensure!(
                exact.name == column.name
                    && exact.data_type == column.data_type
                    && exact.column_type == column.column_type
                    && exact.unsigned == column.unsigned
                    && exact.zerofill == column.zerofill
                    && exact.auto_increment == column.auto_increment
                    && exact.nullable == column.nullable
                    && exact.character_maximum_length == column.character_maximum_length
                    && exact.character_octet_length == column.character_octet_length
                    && exact.numeric_precision == column.numeric_precision
                    && exact.numeric_scale == column.numeric_scale
                    && exact.datetime_precision == column.datetime_precision
                    && exact.character_set == column.character_set
                    && exact.collation == column.collation
                    && exact.collation_id == column.collation_id
                    && exact.collation_padding == column.collation_padding
                    && exact.enum_set_values == column.enum_set_values
                    && exact.srs_id == column.srs_id
                    && exact.visibility == column.visibility
                    && exact.generation == column.generation
                    && exact.extra == column.extra
                    && exact.generation_expression == column.generation_expression
                    && exact.primary_key_ordinal == column.primary_key_ordinal
                    && exact.primary_key_prefix_length == column.primary_key_prefix_length
                    && exact.primary_key_direction == column.primary_key_direction
                    && discovered.name == column.name
                    && discovered.data_type == column.kind.arrow_type()
                    && discovered.arrow_extension_name
                        == Some(column.kind.arrow_extension_name())
                    && discovered.arrow_extension_metadata.as_deref()
                        == Some(extension_metadata.as_str())
                    && discovered.nullable == column.nullable
                    && discovered.primary_key == column.primary_key
                    && discovered.max_length == column.max_length,
                "MySQL replication runtime column identity differs from the authoritative discovery for '{}.{}'",
                table.config.database,
                table.config.name
            );
            validate_replication_column_plan(column)?;
        }
    }
    Ok(())
}

pub fn validate_replication_column_plan(column: &ColumnPlan) -> anyhow::Result<()> {
    anyhow::ensure!(
        mysql_data_type(&column.column_type).eq_ignore_ascii_case(&column.data_type),
        "MySQL CDC DATA_TYPE '{}' disagrees with physical type '{}' for column '{}'",
        column.data_type,
        column.column_type,
        column.name
    );
    expected_binlog_column_type(column)?;
    let expected_kind = mysql_column_kind(
        &column.data_type,
        column.unsigned,
        column.character_set.as_deref(),
    )?;
    anyhow::ensure!(
        column.kind == expected_kind,
        "MySQL CDC physical type '{}' for column '{}' requires logical kind {expected_kind:?}, discovery produced {:?}",
        column.column_type,
        column.name,
        column.kind
    );
    match column.kind {
        MySqlColumnKind::Utf8 => {
            anyhow::ensure!(
                matches!(
                    column.character_set.as_deref(),
                    Some("ascii" | "utf8mb3" | "utf8mb4")
                ),
                "MySQL UTF-8 CDC column '{}' has unsupported character set {:?}",
                column.name,
                column.character_set
            );
        }
        MySqlColumnKind::TextBytes => {
            anyhow::ensure!(
                column.character_set.as_deref() == Some("latin1"),
                "MySQL byte-preserving CDC text column '{}' currently requires latin1, got {:?}",
                column.name,
                column.character_set
            );
        }
        MySqlColumnKind::EnumOrdinal | MySqlColumnKind::SetBits => {
            anyhow::ensure!(
                matches!(
                    column.character_set.as_deref(),
                    Some("ascii" | "utf8mb3" | "utf8mb4" | "latin1")
                ),
                "MySQL ENUM/SET CDC column '{}' has unsupported character set {:?}",
                column.name,
                column.character_set
            );
        }
        _ => {}
    }
    if column.character_set.is_some() {
        anyhow::ensure!(
            column.collation_id.is_some(),
            "MySQL replication requires the numeric collation id for textual column '{}'",
            column.name
        );
    }
    anyhow::ensure!(
        column.collation.is_some() == column.collation_padding.is_some(),
        "MySQL CDC column '{}' must preserve collation padding exactly when a collation is declared",
        column.name
    );
    if column.data_type == "binary" {
        anyhow::ensure!(
            column.character_octet_length.is_some(),
            "MySQL BINARY CDC column '{}' has no authoritative octet length",
            column.name
        );
    }
    match column.data_type.as_str() {
        "enum" => anyhow::ensure!(
            column
                .enum_set_values
                .as_ref()
                .is_some_and(|values| !values.is_empty() && u16::try_from(values.len()).is_ok()),
            "MySQL ENUM column '{}' must declare between 1 and 65535 members",
            column.name
        ),
        "set" => anyhow::ensure!(
            column
                .enum_set_values
                .as_ref()
                .is_some_and(|values| !values.is_empty() && values.len() <= 64),
            "MySQL SET column '{}' must declare between 1 and 64 members",
            column.name
        ),
        _ => anyhow::ensure!(
            column.enum_set_values.is_none(),
            "MySQL non-ENUM/SET column '{}' unexpectedly declares members",
            column.name
        ),
    }
    anyhow::ensure!(
        column.generation != crate::connectors::mysql::src_batch_and_stream::MySqlColumnGeneration::Virtual,
        "MySQL replication does not support virtual generated column '{}' because MySQL does not guarantee it in row images",
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
        let expected_unsigned = if expected_type == mysql_async::consts::ColumnType::MYSQL_TYPE_YEAR
        {
            Some(true)
        } else {
            expected_type.is_numeric_type().then_some(expected.unsigned)
        };
        let expected_collation = expected.collation_id.or_else(|| {
            matches!(
                mysql_data_type(&expected.column_type),
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            )
            .then_some(63)
        });
        let expected_visible = expected.visibility
            == crate::connectors::mysql::src_batch_and_stream::MySqlColumnVisibility::Visible;
        if actual.name != expected.name.as_bytes()
            || actual.column_type != expected_type
            || actual.nullable != expected.nullable
            || actual.unsigned != expected_unsigned
            || actual.collation_id != expected_collation
            || actual.visible != expected_visible
            || actual.primary_key_ordinal != expected.primary_key_ordinal
            || actual.primary_key_prefix_length != expected.primary_key_prefix_length
        {
            anyhow::bail!(
                "MySQL binlog table-map column {} differs from authoritative physical identity for '{}.{}': actual name={:?}, type={:?}, nullable={}, unsigned={:?}, collation={:?}, visible={}, primary_key_ordinal={:?}, primary_key_prefix_length={:?}; expected name={:?}, type={:?}, nullable={}, unsigned={:?}, collation={:?}, visible={}, primary_key_ordinal={:?}, primary_key_prefix_length={:?}",
                index + 1,
                table.config.name,
                expected.name,
                actual.name,
                actual.column_type,
                actual.nullable,
                actual.unsigned,
                actual.collation_id,
                actual.visible,
                actual.primary_key_ordinal,
                actual.primary_key_prefix_length,
                expected.name.as_bytes(),
                expected_type,
                expected.nullable,
                expected_unsigned,
                expected_collation,
                expected_visible,
                expected.primary_key_ordinal,
                expected.primary_key_prefix_length,
            );
        }
        validate_extended_column_metadata(expected, actual)?;
        validate_column_metadata(expected, actual)?;
    }
    Ok(())
}

fn expected_binlog_column_type(
    column: &ColumnPlan,
) -> anyhow::Result<mysql_async::consts::ColumnType> {
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

fn validate_extended_column_metadata(
    expected: &ColumnPlan,
    actual: &super::decoder::MySqlBinlogColumnIdentity,
) -> anyhow::Result<()> {
    use mysql_async::consts::{ColumnType, GeometryType};

    let expected_enum_values = if actual.column_type == ColumnType::MYSQL_TYPE_ENUM {
        Some(encoded_enum_set_members(expected)?)
    } else {
        None
    };
    let expected_set_values = if actual.column_type == ColumnType::MYSQL_TYPE_SET {
        Some(encoded_enum_set_members(expected)?)
    } else {
        None
    };
    let expected_geometry_type = match expected.data_type.as_str() {
        "geometry" => Some(GeometryType::GEOM_GEOMETRY),
        "point" => Some(GeometryType::GEOM_POINT),
        "linestring" => Some(GeometryType::GEOM_LINESTRING),
        "polygon" => Some(GeometryType::GEOM_POLYGON),
        "multipoint" => Some(GeometryType::GEOM_MULTIPOINT),
        "multilinestring" => Some(GeometryType::GEOM_MULTILINESTRING),
        "multipolygon" => Some(GeometryType::GEOM_MULTIPOLYGON),
        "geometrycollection" => Some(GeometryType::GEOM_GEOMETRYCOLLECTION),
        _ => None,
    };
    let expected_vector_dimensionality = if expected.data_type == "vector" {
        Some(
            type_parameters(&expected.column_type)?
                .trim()
                .parse::<u64>()?,
        )
    } else {
        None
    };
    anyhow::ensure!(
        actual.enum_values == expected_enum_values
            && actual.set_values == expected_set_values
            && actual.geometry_type == expected_geometry_type
            && actual.vector_dimensionality == expected_vector_dimensionality,
        "MySQL binlog FULL table-map metadata differs from authoritative physical identity for column '{}'",
        expected.name
    );
    Ok(())
}

fn encoded_enum_set_members(column: &ColumnPlan) -> anyhow::Result<Vec<Vec<u8>>> {
    let values = column.enum_set_values.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL ENUM/SET column '{}' has no authoritative member list",
            column.name
        )
    })?;
    values
        .iter()
        .map(|value| encode_character_value(value, column))
        .collect()
}

fn encode_character_value(value: &str, column: &ColumnPlan) -> anyhow::Result<Vec<u8>> {
    match column.character_set.as_deref() {
        Some("ascii") => {
            anyhow::ensure!(
                value.is_ascii(),
                "MySQL ASCII ENUM/SET column '{}' contains a non-ASCII member",
                column.name
            );
            Ok(value.as_bytes().to_vec())
        }
        Some("utf8mb3" | "utf8mb4") => Ok(value.as_bytes().to_vec()),
        Some("latin1") => value
            .chars()
            .map(mysql_latin1_byte)
            .collect::<anyhow::Result<Vec<_>>>(),
        character_set => anyhow::bail!(
            "MySQL ENUM/SET column '{}' has unsupported character set {character_set:?}",
            column.name
        ),
    }
}

fn mysql_latin1_byte(value: char) -> anyhow::Result<u8> {
    let byte = match value {
        '\u{20ac}' => 0x80,
        '\u{201a}' => 0x82,
        '\u{0192}' => 0x83,
        '\u{201e}' => 0x84,
        '\u{2026}' => 0x85,
        '\u{2020}' => 0x86,
        '\u{2021}' => 0x87,
        '\u{02c6}' => 0x88,
        '\u{2030}' => 0x89,
        '\u{0160}' => 0x8a,
        '\u{2039}' => 0x8b,
        '\u{0152}' => 0x8c,
        '\u{017d}' => 0x8e,
        '\u{2018}' => 0x91,
        '\u{2019}' => 0x92,
        '\u{201c}' => 0x93,
        '\u{201d}' => 0x94,
        '\u{2022}' => 0x95,
        '\u{2013}' => 0x96,
        '\u{2014}' => 0x97,
        '\u{02dc}' => 0x98,
        '\u{2122}' => 0x99,
        '\u{0161}' => 0x9a,
        '\u{203a}' => 0x9b,
        '\u{0153}' => 0x9c,
        '\u{017e}' => 0x9e,
        '\u{0178}' => 0x9f,
        value if u32::from(value) <= 0xff => u8::try_from(u32::from(value))?,
        value => anyhow::bail!("character {value:?} is not representable in MySQL latin1"),
    };
    Ok(byte)
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
        ColumnType::MYSQL_TYPE_FLOAT
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_VECTOR => actual.metadata == [4],
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
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let (precision, scale) = decimal_precision_scale(&expected.column_type)?;
            let expected_metadata: [u8; 2] = (precision, scale).into();
            actual.metadata == expected_metadata
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
        ColumnType::MYSQL_TYPE_ENUM => {
            let variants = expected.enum_set_values.as_ref().map_or(0, Vec::len);
            let width = if variants < 256 { 1 } else { 2 };
            actual.metadata == [ColumnType::MYSQL_TYPE_ENUM as u8, width]
        }
        ColumnType::MYSQL_TYPE_SET => {
            let variants = expected.enum_set_values.as_ref().map_or(0, Vec::len);
            let width = u8::try_from(variants.div_ceil(8))?;
            actual.metadata == [ColumnType::MYSQL_TYPE_SET as u8, width]
        }
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
    column.character_octet_length.ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL character column '{}' has no declared maximum octet length",
            column.name
        )
    })
}

const fn string_metadata(maximum_bytes: u16) -> [u8; 2] {
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
    Ok((precision.trim().parse::<u8>()?, scale.trim().parse::<u8>()?))
}

fn type_parameters(column_type: &str) -> anyhow::Result<&str> {
    let start = column_type.find('(').ok_or_else(|| {
        anyhow::anyhow!("MySQL physical type '{column_type}' omits required parameters")
    })?;
    let end = column_type.rfind(')').ok_or_else(|| {
        anyhow::anyhow!("MySQL physical type '{column_type}' has unterminated parameters")
    })?;
    anyhow::ensure!(
        start < end,
        "MySQL physical type has empty parameter framing"
    );
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

fn normalize_binlog_row(
    table: &DiscoveredTable,
    table_map: &super::decoder::MySqlTableIdentity,
    row: Option<Vec<BinlogValue<'static>>>,
) -> anyhow::Result<Option<Vec<Option<Value>>>> {
    let Some(row) = row else {
        return Ok(None);
    };
    anyhow::ensure!(
        row.len() == table.columns.len()
            && table_map.column_identities.len() == table.columns.len(),
        "MySQL binlog row width changed after discovery for table '{}'",
        table.config.name
    );
    let values = row
        .into_iter()
        .zip(table.columns.iter().zip(&table_map.column_identities))
        .map(|(value, (column, identity))| {
            normalize_binlog_value(value, column, identity).map(Some)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Some(values))
}

#[allow(
    clippy::many_single_char_names,
    reason = "mysql_async exposes date tuple fields with conventional compact names"
)]
pub(super) fn normalize_binlog_value(
    value: BinlogValue<'static>,
    column: &ColumnPlan,
    identity: &super::decoder::MySqlBinlogColumnIdentity,
) -> anyhow::Result<Value> {
    if matches!(&value, BinlogValue::Value(Value::NULL)) {
        return Ok(Value::NULL);
    }
    let unexpected = |value: &BinlogValue<'_>| {
        anyhow::anyhow!(
            "MySQL binlog value for physical column '{}.{}' has an incompatible encoding: {value:?}",
            column.name,
            column.column_type
        )
    };
    match (column.kind, value) {
        // mysql_common 0.37 reads a signed 24-bit integer through an i32
        // helper that zero-extends the three wire bytes. Values above the
        // positive MEDIUMINT range are therefore the unambiguous two's-
        // complement encodings of negative values.
        (MySqlColumnKind::Int32, BinlogValue::Value(Value::Int(value)))
            if column.data_type == "mediumint" && value > 0x7f_ffff && value <= 0xff_ffff =>
        {
            Ok(Value::Int(value - (1_i64 << 24)))
        }
        (
            MySqlColumnKind::Int8
            | MySqlColumnKind::Int16
            | MySqlColumnKind::Int32
            | MySqlColumnKind::Int64,
            BinlogValue::Value(value @ Value::Int(_)),
        )
        | (
            MySqlColumnKind::UInt8
            | MySqlColumnKind::UInt16
            | MySqlColumnKind::UInt32
            | MySqlColumnKind::UInt64,
            BinlogValue::Value(value @ Value::UInt(_)),
        )
        | (MySqlColumnKind::Float32, BinlogValue::Value(value @ Value::Float(_)))
        | (MySqlColumnKind::Float64, BinlogValue::Value(value @ Value::Double(_)))
        | (MySqlColumnKind::DecimalText, BinlogValue::Value(value @ Value::Bytes(_))) => Ok(value),
        (MySqlColumnKind::Binary, BinlogValue::Value(Value::Bytes(mut value))) => {
            if column.data_type == "binary" {
                let width = column.character_octet_length.ok_or_else(|| {
                    anyhow::anyhow!(
                        "MySQL BINARY column '{}' has no authoritative octet length",
                        column.name
                    )
                })?;
                anyhow::ensure!(
                    value.len() <= width,
                    "MySQL BINARY column '{}' row value has {} bytes, exceeding its declared width {width}",
                    column.name,
                    value.len()
                );
                value.resize(width, 0);
            } else if is_geometry_data_type(&column.data_type) {
                let srid = value.get(..4).ok_or_else(|| {
                    anyhow::anyhow!(
                        "MySQL geometry column '{}' row value omits its SRID prefix",
                        column.name
                    )
                })?;
                let srid = u32::from_le_bytes(srid.try_into()?);
                if let Some(expected_srid) = column.srs_id {
                    anyhow::ensure!(
                        srid == expected_srid,
                        "MySQL geometry column '{}' row value has SRID {srid}, authoritative schema requires {expected_srid}",
                        column.name
                    );
                }
            }
            Ok(Value::Bytes(value))
        }
        (
            MySqlColumnKind::Utf8 | MySqlColumnKind::TextBytes,
            BinlogValue::Value(Value::Bytes(mut value)),
        ) => {
            // MySQL's SELECT result for CHAR removes its storage padding. The
            // row event carries the packed physical bytes, so discard only the
            // U+0020 pad bytes that CHAR retrieval itself does not expose.
            // BINARY follows the Binary arm above and retains every trailing NUL.
            if column.data_type == "char" {
                value.truncate(
                    value
                        .iter()
                        .rposition(|byte| *byte != b' ')
                        .map_or(0, |i| i + 1),
                );
            }
            Ok(Value::Bytes(value))
        }
        // mysql_common may use Int for a non-negative unsigned wire value when
        // the concrete value fits in i64. Canonicalize it before the existing
        // per-kind Arrow range check; negative values remain invalid.
        (
            MySqlColumnKind::UInt8
            | MySqlColumnKind::UInt16
            | MySqlColumnKind::UInt32
            | MySqlColumnKind::UInt64,
            BinlogValue::Value(Value::Int(value)),
        ) if value >= 0 => Ok(Value::UInt(u64::try_from(value)?)),
        (MySqlColumnKind::DateText, BinlogValue::Value(Value::Date(y, m, d, h, min, s, us))) => {
            anyhow::ensure!(
                (h, min, s, us) == (0, 0, 0, 0),
                "MySQL DATE binlog value for '{}' contains a time component",
                column.name
            );
            anyhow::ensure!(
                y <= 9999 && m <= 12 && d <= 31,
                "MySQL DATE binlog value for '{}' has an invalid component",
                column.name
            );
            Ok(Value::Bytes(format!("{y:04}-{m:02}-{d:02}").into_bytes()))
        }
        (
            MySqlColumnKind::DateTimeText,
            BinlogValue::Value(Value::Date(y, m, d, h, min, s, us)),
        ) => Ok(Value::Bytes(
            format_datetime_text(y, m, d, h, min, s, us, temporal_precision(column)?)?.into_bytes(),
        )),
        (MySqlColumnKind::TimestampText, BinlogValue::Value(value)) => Ok(Value::Bytes(
            format_timestamp_text(value, temporal_precision(column)?)?.into_bytes(),
        )),
        (
            MySqlColumnKind::TimeText,
            BinlogValue::Value(Value::Time(negative, days, hours, minutes, seconds, micros)),
        ) => Ok(Value::Bytes(
            format_time_text(
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
                temporal_precision(column)?,
            )?
            .into_bytes(),
        )),
        (MySqlColumnKind::YearText, BinlogValue::Value(Value::Bytes(value))) => {
            // mysql_common maps the wire zero sentinel to 1900. YEAR(1900) is
            // not a valid MySQL value, so this mapping is unambiguous.
            Ok(Value::Bytes(if value == b"1900" {
                b"0000".to_vec()
            } else {
                value
            }))
        }
        (MySqlColumnKind::EnumOrdinal, BinlogValue::Value(value)) => {
            Ok(Value::UInt(normalize_enum(value, identity, column)?))
        }
        (MySqlColumnKind::SetBits, BinlogValue::Value(value)) => {
            Ok(Value::UInt(normalize_set(value, identity, column)?))
        }
        (MySqlColumnKind::Json, BinlogValue::Jsonb(value)) => {
            Ok(Value::Bytes(serialize_mysql_json(value, column)?))
        }
        (_, value @ BinlogValue::JsonDiff(_)) => Err(anyhow::anyhow!(
            "MySQL partial JSON diff reached full-image normalization for column '{}': {value:?}",
            column.name
        )),
        (_, value) => Err(unexpected(&value)),
    }
}

fn is_geometry_data_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "geometry"
            | "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    )
}

fn temporal_precision(column: &ColumnPlan) -> anyhow::Result<usize> {
    let precision = usize::try_from(column.datetime_precision.unwrap_or(0))?;
    anyhow::ensure!(
        precision <= 6,
        "MySQL temporal column '{}' has precision {precision}, maximum is 6",
        column.name
    );
    Ok(precision)
}

pub(super) fn format_datetime_text(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
    precision: usize,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        year <= 9999
            && month <= 12
            && day <= 31
            && hour < 24
            && minute < 60
            && second < 60
            && micros < 1_000_000,
        "MySQL datetime has an invalid component"
    );
    let mut value = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    append_fraction(&mut value, micros, precision)?;
    Ok(value)
}

pub(super) fn format_time_text(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
    precision: usize,
) -> anyhow::Result<String> {
    let total_hours = days
        .checked_mul(24)
        .and_then(|value| value.checked_add(u32::from(hours)))
        .ok_or_else(|| anyhow::anyhow!("MySQL TIME hour calculation overflow"))?;
    anyhow::ensure!(
        total_hours <= 838 && minutes < 60 && seconds < 60 && micros < 1_000_000,
        "MySQL TIME binlog value has an invalid component"
    );
    let sign = if negative { "-" } else { "" };
    let mut value = format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}");
    append_fraction(&mut value, micros, precision)?;
    Ok(value)
}

pub(super) fn format_timestamp_text(value: Value, precision: usize) -> anyhow::Result<String> {
    let (seconds, micros) = match value {
        Value::Int(seconds) => (seconds, 0),
        Value::Bytes(value) => parse_timestamp_epoch(&value)?,
        value => anyhow::bail!("MySQL TIMESTAMP binlog value has unexpected encoding {value:?}"),
    };
    if seconds == 0 {
        return format_datetime_text(0, 0, 0, 0, 0, 0, micros, precision);
    }
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, micros * 1_000)
        .ok_or_else(|| anyhow::anyhow!("MySQL TIMESTAMP epoch value is out of range"))?;
    let mut result = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
    append_fraction(&mut result, micros, precision)?;
    Ok(result)
}

fn parse_timestamp_epoch(value: &[u8]) -> anyhow::Result<(i64, u32)> {
    let value = std::str::from_utf8(value)
        .map_err(|error| anyhow::anyhow!("MySQL TIMESTAMP epoch is not ASCII: {error}"))?;
    let (seconds, fraction) = value
        .split_once('.')
        .map_or((value, None), |(seconds, fraction)| {
            (seconds, Some(fraction))
        });
    let seconds = seconds.parse::<i64>()?;
    let micros = match fraction {
        None => 0,
        Some(fraction) => {
            anyhow::ensure!(
                fraction.len() == 6 && fraction.bytes().all(|byte| byte.is_ascii_digit()),
                "MySQL TIMESTAMP fractional epoch is not exactly six decimal digits"
            );
            fraction.parse::<u32>()?
        }
    };
    Ok((seconds, micros))
}

fn append_fraction(value: &mut String, micros: u32, precision: usize) -> anyhow::Result<()> {
    if precision == 0 {
        return Ok(());
    }
    let divisor = 10_u32
        .checked_pow(u32::try_from(6_usize.checked_sub(precision).ok_or_else(
            || anyhow::anyhow!("MySQL temporal precision exceeds 6"),
        )?)?)
        .ok_or_else(|| anyhow::anyhow!("MySQL temporal precision divisor overflow"))?;
    write!(value, ".{:0width$}", micros / divisor, width = precision)?;
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the owned mysql_async value is destructured and rejected without cloning"
)]
pub(super) fn normalize_enum(
    value: Value,
    identity: &super::decoder::MySqlBinlogColumnIdentity,
    column: &ColumnPlan,
) -> anyhow::Result<u64> {
    let Value::Int(ordinal) = value else {
        anyhow::bail!(
            "MySQL ENUM column '{}' has a non-ordinal row value",
            column.name
        )
    };
    anyhow::ensure!(
        ordinal >= 0,
        "MySQL ENUM column '{}' has a negative ordinal",
        column.name
    );
    let values = identity.enum_values.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL ENUM column '{}' has no FULL table-map members",
            column.name
        )
    })?;
    anyhow::ensure!(
        usize::try_from(ordinal)? <= values.len(),
        "MySQL ENUM column '{}' ordinal {ordinal} exceeds {} declared members",
        column.name,
        values.len()
    );
    Ok(u64::try_from(ordinal)?)
}

pub(super) fn normalize_set(
    value: Value,
    identity: &super::decoder::MySqlBinlogColumnIdentity,
    column: &ColumnPlan,
) -> anyhow::Result<u64> {
    let Value::Bytes(bits) = value else {
        anyhow::bail!(
            "MySQL SET column '{}' has a non-bitset row value",
            column.name
        )
    };
    anyhow::ensure!(
        bits.len() <= 8,
        "MySQL SET column '{}' exceeds 64 members",
        column.name
    );
    let selected = u64::from_le_bytes({
        let mut bytes = [0_u8; 8];
        bytes[..bits.len()].copy_from_slice(&bits);
        bytes
    });
    let values = identity.set_values.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL SET column '{}' has no FULL table-map members",
            column.name
        )
    })?;
    let valid_bits = values.len();
    anyhow::ensure!(
        valid_bits == 64 || selected >> valid_bits == 0,
        "MySQL SET column '{}' contains bits beyond its declared members",
        column.name
    );
    Ok(selected)
}

pub(super) fn serialize_mysql_json(
    value: JsonbValue<'_>,
    column: &ColumnPlan,
) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    append_mysql_json(value, &mut output).map_err(|error| {
        anyhow::anyhow!(
            "MySQL JSON column '{}' cannot be normalized losslessly: {error}",
            column.name
        )
    })?;
    Ok(output)
}

fn append_mysql_json(value: JsonbValue<'_>, output: &mut Vec<u8>) -> anyhow::Result<()> {
    append_mysql_json_at_depth(value, output, 0)
}

fn append_mysql_json_at_depth(
    value: JsonbValue<'_>,
    output: &mut Vec<u8>,
    depth: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(depth <= 100, "JSON exceeds MySQL's maximum nesting depth");
    match value {
        JsonbValue::Null => output.extend_from_slice(b"null"),
        JsonbValue::Bool(value) => output.extend_from_slice(if value { b"true" } else { b"false" }),
        JsonbValue::I16(value) => append_json_number(&value, output),
        JsonbValue::U16(value) => append_json_number(&value, output),
        JsonbValue::I32(value) => append_json_number(&value, output),
        JsonbValue::U32(value) => append_json_number(&value, output),
        JsonbValue::I64(value) => append_json_number(&value, output),
        JsonbValue::U64(value) => append_json_number(&value, output),
        JsonbValue::F64(value) => {
            anyhow::ensure!(value.is_finite(), "JSON contains a non-finite double");
            append_json_double(value, output)?;
        }
        JsonbValue::String(value) => append_json_string(value.str_raw(), output)?,
        JsonbValue::SmallArray(values) => append_json_array(values.iter(), output, depth)?,
        JsonbValue::LargeArray(values) => append_json_array(values.iter(), output, depth)?,
        JsonbValue::SmallObject(values) => append_json_object(values.iter(), output, depth)?,
        JsonbValue::LargeObject(values) => append_json_object(values.iter(), output, depth)?,
        JsonbValue::Opaque(value) => append_json_opaque(value, output)?,
    }
    Ok(())
}

fn append_json_number(value: &impl ToString, output: &mut Vec<u8>) {
    output.extend_from_slice(value.to_string().as_bytes());
}

fn append_json_double(value: f64, output: &mut Vec<u8>) -> anyhow::Result<()> {
    // Rust and MySQL both generate the shortest decimal which round-trips to
    // the same binary double, but choose fixed/scientific notation at different
    // thresholds. Derive MySQL's dtoa `decpt` and significant-digit count from
    // Rust's bounded scientific form, then apply my_gcvt's full-width choice:
    // fixed is allowed for -14 <= decpt <= 15, and for a larger non-integral
    // value whose significant digits extend beyond the decimal point.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.rsplit_once('e').ok_or_else(|| {
        anyhow::anyhow!("Rust did not produce scientific notation for a finite double")
    })?;
    let exponent = exponent
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Rust produced an invalid double exponent"))?;
    let decimal_point = exponent
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("double decimal-point position overflow"))?;
    let significant_digits = i32::try_from(mantissa.bytes().filter(u8::is_ascii_digit).count())?;
    let fixed = decimal_point >= -14 && (decimal_point <= 15 || significant_digits > decimal_point);
    let formatted = if fixed { value.to_string() } else { scientific };
    output.extend_from_slice(formatted.as_bytes());
    if !formatted.bytes().any(|byte| matches!(byte, b'.' | b'e')) {
        output.extend_from_slice(b".0");
    }
    Ok(())
}

fn append_json_string(value: &[u8], output: &mut Vec<u8>) -> anyhow::Result<()> {
    std::str::from_utf8(value)?;
    output.push(b'"');
    for &byte in value {
        match byte {
            b'"' => output.extend_from_slice(br#"\""#),
            b'\\' => output.extend_from_slice(br"\\"),
            0x08 => output.extend_from_slice(br"\b"),
            0x09 => output.extend_from_slice(br"\t"),
            0x0a => output.extend_from_slice(br"\n"),
            0x0c => output.extend_from_slice(br"\f"),
            0x0d => output.extend_from_slice(br"\r"),
            0x00..=0x1f => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                output.extend_from_slice(b"\\u00");
                output.push(HEX[usize::from(byte >> 4)]);
                output.push(HEX[usize::from(byte & 0x0f)]);
            }
            _ => output.push(byte),
        }
    }
    output.push(b'"');
    Ok(())
}

pub(super) fn append_json_array<'a>(
    values: impl Iterator<Item = std::io::Result<JsonbValue<'a>>>,
    output: &mut Vec<u8>,
    depth: usize,
) -> anyhow::Result<()> {
    output.push(b'[');
    for (index, value) in values.enumerate() {
        if index != 0 {
            output.extend_from_slice(b", ");
        }
        append_mysql_json_at_depth(value?, output, depth + 1)?;
    }
    output.push(b']');
    Ok(())
}

pub(super) fn append_json_object<'a>(
    values: impl Iterator<
        Item = std::io::Result<(mysql_async::binlog::jsonb::ObjectKey<'a>, JsonbValue<'a>)>,
    >,
    output: &mut Vec<u8>,
    depth: usize,
) -> anyhow::Result<()> {
    output.push(b'{');
    for (index, value) in values.enumerate() {
        if index != 0 {
            output.extend_from_slice(b", ");
        }
        let (key, value) = value?;
        append_json_string(key.value_raw(), output)?;
        output.extend_from_slice(b": ");
        append_mysql_json_at_depth(value, output, depth + 1)?;
    }
    output.push(b'}');
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the opaque mysql_async wrapper is a borrowed view consumed by exhaustive dispatch"
)]
fn append_json_opaque(
    value: mysql_async::binlog::jsonb::OpaqueValue<'_>,
    output: &mut Vec<u8>,
) -> anyhow::Result<()> {
    use mysql_async::consts::ColumnType;

    match value.value_type() {
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let header = value
                .data_raw()
                .get(..2)
                .ok_or_else(|| anyhow::anyhow!("JSON opaque DECIMAL omits precision or scale"))?;
            let precision = header[0];
            let scale = header[1];
            anyhow::ensure!(
                (1..=65).contains(&precision) && scale <= precision && scale <= 30,
                "JSON opaque DECIMAL has invalid precision {precision} or scale {scale}"
            );
            let mut input = std::io::Cursor::new(value.data_raw());
            let decimal = mysql_async::binlog::decimal::Decimal::read_packed(&mut input, false)?;
            anyhow::ensure!(
                usize::try_from(input.position())? == value.data_raw().len(),
                "JSON opaque DECIMAL has trailing bytes"
            );
            output.extend_from_slice(decimal.to_string().as_bytes());
        }
        value_type @ (ColumnType::MYSQL_TYPE_DATE
        | ColumnType::MYSQL_TYPE_TIME
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP) => {
            let raw: [u8; 8] = value.data_raw().try_into().map_err(|_| {
                anyhow::anyhow!("JSON opaque {value_type:?} is not exactly eight bytes")
            })?;
            let packed = i64::from_le_bytes(raw);
            anyhow::ensure!(
                packed != i64::MIN,
                "JSON opaque {value_type:?} has an invalid minimum packed value"
            );
            let time = match value_type {
                ColumnType::MYSQL_TYPE_DATE => {
                    mysql_async::binlog::time::MysqlTime::from_int64_date_packed(packed)
                }
                ColumnType::MYSQL_TYPE_TIME => {
                    mysql_async::binlog::time::MysqlTime::from_int64_time_packed(packed)
                }
                ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => {
                    mysql_async::binlog::time::MysqlTime::from_int64_datetime_packed(packed)
                }
                other => anyhow::bail!("JSON contains unsupported opaque type {other:?}"),
            };
            validate_json_opaque_time(value_type, &time)?;
            append_json_string(format!("{time:.6}").as_bytes(), output)?;
        }
        ColumnType::MYSQL_TYPE_VAR_STRING => append_json_string(value.data_raw(), output)?,
        value_type => append_json_base64_opaque(value_type, value.data_raw(), output)?,
    }
    Ok(())
}

fn append_json_base64_opaque(
    value_type: mysql_async::consts::ColumnType,
    data: &[u8],
    output: &mut Vec<u8>,
) -> anyhow::Result<()> {
    const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let encoded_bytes = data
        .len()
        .checked_add(2)
        .map(|bytes| bytes / 3)
        .and_then(|groups| groups.checked_mul(4))
        .ok_or_else(|| anyhow::anyhow!("JSON opaque base64 length overflow"))?;
    let line_breaks = encoded_bytes.saturating_sub(1) / 76;
    let mut encoded = Vec::with_capacity(
        "base64:type"
            .len()
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(encoded_bytes))
            .and_then(|bytes| bytes.checked_add(line_breaks))
            .ok_or_else(|| anyhow::anyhow!("JSON opaque base64 allocation overflow"))?,
    );
    encoded.extend_from_slice(b"base64:type");
    encoded.extend_from_slice(u8::from(value_type).to_string().as_bytes());
    encoded.push(b':');

    let mut line_length = 0_usize;
    for chunk in data.chunks(3) {
        if line_length == 76 {
            encoded.push(b'\n');
            line_length = 0;
        }
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.extend_from_slice(&[
            BASE64[usize::from(first >> 2)],
            BASE64[usize::from(((first & 0x03) << 4) | (second >> 4))],
            if chunk.len() > 1 {
                BASE64[usize::from(((second & 0x0f) << 2) | (third >> 6))]
            } else {
                b'='
            },
            if chunk.len() > 2 {
                BASE64[usize::from(third & 0x3f)]
            } else {
                b'='
            },
        ]);
        line_length += 4;
    }
    append_json_string(&encoded, output)
}

fn validate_json_opaque_time(
    value_type: mysql_async::consts::ColumnType,
    value: &mysql_async::binlog::time::MysqlTime,
) -> anyhow::Result<()> {
    use mysql_async::consts::ColumnType;

    anyhow::ensure!(
        value.minute < 60 && value.second < 60 && value.second_part < 1_000_000,
        "JSON opaque {value_type:?} has an invalid time component"
    );
    match value_type {
        ColumnType::MYSQL_TYPE_TIME => anyhow::ensure!(
            value.year == 0 && value.month == 0 && value.day == 0 && value.hour <= 838,
            "JSON opaque TIME has an invalid date or hour component"
        ),
        ColumnType::MYSQL_TYPE_DATE => anyhow::ensure!(
            !value.neg
                && value.year <= 9999
                && value.month <= 12
                && value.day <= 31
                && value.hour == 0
                && value.minute == 0
                && value.second == 0
                && value.second_part == 0,
            "JSON opaque DATE has an invalid component"
        ),
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => anyhow::ensure!(
            !value.neg
                && value.year <= 9999
                && value.month <= 12
                && value.day <= 31
                && value.hour < 24,
            "JSON opaque {value_type:?} has an invalid date or hour component"
        ),
        other => anyhow::bail!("JSON contains unsupported opaque type {other:?}"),
    }
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
            MySqlError::Io(_) | MySqlError::Driver(mysql_async::DriverError::ConnectionClosed)
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
