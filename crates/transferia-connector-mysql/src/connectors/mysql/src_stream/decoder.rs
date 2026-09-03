use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use mysql_async::binlog::events::{
    Event, EventData, OptionalMetaExtractor, OptionalMetadataField, RowsEventData, TableMapEvent,
};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventType;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::consts::{ColumnType, GeometryType};

use super::checksum::{is_artificial_rotate, BinlogChecksumError, BinlogChecksumVerifier};
use super::config::MySqlReplicationConfig;
use super::position::MySqlBinlogPosition;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MySqlTransactionIdentity {
    Gtid {
        sid: [u8; 16],
        tag: Option<String>,
        gno: u64,
    },
    Anonymous {
        begin_position: MySqlBinlogPosition,
    },
    FilePosition {
        begin_position: MySqlBinlogPosition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MySqlTransactionMarker {
    pub identity: MySqlTransactionIdentity,
    pub begin_position: MySqlBinlogPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MySqlTableIdentity {
    pub table_id: u64,
    pub database: Vec<u8>,
    pub table: Vec<u8>,
    pub columns: u64,
    pub column_identities: Vec<MySqlBinlogColumnIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MySqlBinlogColumnIdentity {
    pub name: Vec<u8>,
    pub column_type: ColumnType,
    pub metadata: Vec<u8>,
    pub nullable: bool,
    pub unsigned: Option<bool>,
    pub collation_id: Option<u16>,
    pub enum_values: Option<Vec<Vec<u8>>>,
    pub set_values: Option<Vec<Vec<u8>>>,
    pub geometry_type: Option<GeometryType>,
    pub vector_dimensionality: Option<u64>,
    pub visible: bool,
    pub primary_key_ordinal: Option<u64>,
    pub primary_key_prefix_length: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MySqlRowOperation {
    Write,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MySqlRowChange {
    pub before: Option<Vec<BinlogValue<'static>>>,
    pub after: Option<Vec<BinlogValue<'static>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRowsEvent {
    pub transaction: Arc<MySqlTransactionMarker>,
    pub operation: MySqlRowOperation,
    pub table: MySqlTableIdentity,
    pub before_columns: Vec<bool>,
    pub after_columns: Vec<bool>,
    pub rows: Vec<MySqlRowChange>,
    pub source_timestamp_seconds: u32,
    /// This is an observed event position, not a durable transaction commit marker.
    pub observed_next_position: MySqlBinlogPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedTransaction {
    pub transaction: Arc<MySqlTransactionMarker>,
    pub xid: Option<u64>,
    pub next_position: MySqlBinlogPosition,
    pub event_count: u64,
    pub encoded_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolledBackTransaction {
    pub transaction: Arc<MySqlTransactionMarker>,
    pub next_position: MySqlBinlogPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RotatedBinlog {
    pub previous_position: MySqlBinlogPosition,
    pub next_position: MySqlBinlogPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IgnoredBinlogEvent {
    pub event_type: EventType,
    pub next_position: MySqlBinlogPosition,
    pub inside_transaction: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DecodedBinlogEvent {
    TransactionStarted(Arc<MySqlTransactionMarker>),
    TableMapped(MySqlTableIdentity),
    Rows(DecodedRowsEvent),
    TransactionCommitted(CommittedTransaction),
    TransactionRolledBack(RolledBackTransaction),
    BinlogRotated(RotatedBinlog),
    Ignored(IgnoredBinlogEvent),
}

struct ActiveTransaction {
    marker: Arc<MySqlTransactionMarker>,
    event_count: u64,
    decoded_rows: u64,
    encoded_bytes: u64,
    saw_begin_query: bool,
}

pub struct MySqlBinlogDecoder {
    config: MySqlReplicationConfig,
    current_position: MySqlBinlogPosition,
    active_transaction: Option<ActiveTransaction>,
    table_maps: BTreeMap<u64, Arc<TableMapEvent<'static>>>,
    checksum: BinlogChecksumVerifier,
    allow_gtid_auto_position_rotate: bool,
    selected_tables: Option<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl MySqlBinlogDecoder {
    pub fn new(
        config: MySqlReplicationConfig,
        current_position: MySqlBinlogPosition,
    ) -> Result<Self, BinlogDecodeError> {
        config
            .validate()
            .map_err(|error| BinlogDecodeError::InvalidConfig(error.to_string()))?;
        current_position
            .validate()
            .map_err(|error| BinlogDecodeError::InvalidPosition(error.to_string()))?;
        Ok(Self {
            config,
            current_position,
            active_transaction: None,
            table_maps: BTreeMap::new(),
            checksum: BinlogChecksumVerifier::default(),
            allow_gtid_auto_position_rotate: false,
            selected_tables: None,
        })
    }

    pub fn enable_gtid_auto_position(&mut self) {
        self.allow_gtid_auto_position_rotate = true;
    }

    pub(crate) fn retain_rows_for_tables(
        &mut self,
        database: &[u8],
        tables: impl IntoIterator<Item = Vec<u8>>,
    ) {
        self.selected_tables = Some(
            tables
                .into_iter()
                .map(|table| (database.to_vec(), table))
                .collect(),
        );
    }

    pub fn current_position(&self) -> &MySqlBinlogPosition {
        &self.current_position
    }

    pub fn active_transaction(&self) -> Option<&Arc<MySqlTransactionMarker>> {
        self.active_transaction
            .as_ref()
            .map(|transaction| &transaction.marker)
    }

    /// Decodes one event without making any destination side effect.
    ///
    /// Callers must buffer all returned transaction events until
    /// `TransactionCommitted`; an observed row-event position is not safe to
    /// acknowledge because reconnecting there may omit the table map and the
    /// beginning of the transaction.
    pub fn decode(&mut self, event: &Event) -> Result<DecodedBinlogEvent, BinlogDecodeError> {
        self.checksum.verify(event)?;
        let header = event.header();
        let event_type = header
            .event_type()
            .map_err(|_| BinlogDecodeError::UnknownEventType(header.event_type_raw()))?;
        if event_type == EventType::TRANSACTION_PAYLOAD_EVENT {
            // The vendored mysql_async returns this event without expanding it.
            // Reject it before parsing so compressed payloads can never be
            // silently omitted or bypass the configured transaction bound.
            return Err(BinlogDecodeError::TransactionCompressionObserved);
        }
        let event_bytes = u64::from(header.event_size());
        if event_bytes > self.config.max_transaction_bytes as u64 {
            return Err(BinlogDecodeError::EventTooLarge {
                event_bytes,
                configured_max_transaction_bytes: self.config.max_transaction_bytes,
            });
        }

        let data = event
            .read_data()
            .map_err(|error| BinlogDecodeError::MalformedEvent {
                event_type,
                reason: error.to_string(),
            })?
            .ok_or(BinlogDecodeError::UnknownEventType(
                header.event_type_raw(),
            ))?;

        match data {
            EventData::GtidEvent(gtid) => {
                if self.active_transaction.is_some() {
                    return Err(BinlogDecodeError::TransactionAlreadyActive(event_type));
                }
                let next = self.next_position(event_type, header.log_pos())?;
                let marker = Arc::new(MySqlTransactionMarker {
                    identity: MySqlTransactionIdentity::Gtid {
                        sid: gtid.sid(),
                        tag: gtid.tag().map(|tag| tag.as_str().to_owned()),
                        gno: gtid.gno(),
                    },
                    begin_position: self.current_position.clone(),
                });
                self.active_transaction = Some(ActiveTransaction {
                    marker: Arc::clone(&marker),
                    event_count: 1,
                    decoded_rows: 0,
                    encoded_bytes: event_bytes,
                    saw_begin_query: false,
                });
                self.current_position = next;
                Ok(DecodedBinlogEvent::TransactionStarted(marker))
            }
            EventData::AnonymousGtidEvent(_) => {
                if self.active_transaction.is_some() {
                    return Err(BinlogDecodeError::TransactionAlreadyActive(event_type));
                }
                let next = self.next_position(event_type, header.log_pos())?;
                let marker = Arc::new(MySqlTransactionMarker {
                    identity: MySqlTransactionIdentity::Anonymous {
                        begin_position: self.current_position.clone(),
                    },
                    begin_position: self.current_position.clone(),
                });
                self.active_transaction = Some(ActiveTransaction {
                    marker: Arc::clone(&marker),
                    event_count: 1,
                    decoded_rows: 0,
                    encoded_bytes: event_bytes,
                    saw_begin_query: false,
                });
                self.current_position = next;
                Ok(DecodedBinlogEvent::TransactionStarted(marker))
            }
            EventData::QueryEvent(query) => {
                let schema = query.schema_raw().to_vec();
                let statement = trim_ascii(query.query_raw());
                if statement.eq_ignore_ascii_case(b"BEGIN") {
                    self.decode_begin(event_type, event_bytes, &header)
                } else if statement.eq_ignore_ascii_case(b"COMMIT") {
                    self.decode_commit(event_type, event_bytes, &header, None)
                } else if statement.eq_ignore_ascii_case(b"ROLLBACK") {
                    self.decode_rollback(event_type, event_bytes, &header)
                } else {
                    Err(BinlogDecodeError::UnsupportedStatement { schema })
                }
            }
            EventData::TableMapEvent(table_map) => {
                self.require_transaction(event_type)?;
                self.check_transaction_limits(event_type, event_bytes)?;
                let next = self.next_position(event_type, header.log_pos())?;
                let selected = self.table_is_selected(&table_map);
                let identity = selected.then(|| table_identity(&table_map)).transpose()?;
                self.table_maps
                    .insert(table_map.table_id(), Arc::new(table_map.into_owned()));
                self.apply_transaction_usage(event_type, event_bytes)?;
                self.current_position = next;
                match identity {
                    Some(identity) => Ok(DecodedBinlogEvent::TableMapped(identity)),
                    None => Ok(DecodedBinlogEvent::Ignored(IgnoredBinlogEvent {
                        event_type,
                        next_position: self.current_position.clone(),
                        inside_transaction: true,
                    })),
                }
            }
            EventData::RowsEvent(rows) => {
                self.decode_rows(event_type, event_bytes, &header, &rows)
            }
            EventData::XidEvent(xid) => {
                self.decode_commit(event_type, event_bytes, &header, Some(xid.xid))
            }
            EventData::RotateEvent(rotate) => {
                if self.active_transaction.is_some() {
                    return Err(BinlogDecodeError::RotateInsideTransaction);
                }
                if is_artificial_rotate(event_type, event) {
                    if self.allow_gtid_auto_position_rotate {
                        let next = MySqlBinlogPosition::new(
                            rotate.name_raw().to_vec(),
                            rotate.position(),
                        )
                        .map_err(|error| BinlogDecodeError::InvalidPosition(error.to_string()))?;
                        let previous_position = self.current_position.clone();
                        self.current_position = next.clone();
                        self.table_maps.clear();
                        self.allow_gtid_auto_position_rotate = false;
                        return Ok(DecodedBinlogEvent::BinlogRotated(RotatedBinlog {
                            previous_position,
                            next_position: next,
                        }));
                    }
                    if rotate.name_raw() != self.current_position.filename {
                        return Err(BinlogDecodeError::UnexpectedFakeRotate {
                            expected_filename: self.current_position.filename.clone(),
                            received_filename: rotate.name_raw().to_vec(),
                        });
                    }
                    return Ok(DecodedBinlogEvent::Ignored(IgnoredBinlogEvent {
                        event_type,
                        next_position: self.current_position.clone(),
                        inside_transaction: false,
                    }));
                }
                let next = MySqlBinlogPosition::new(rotate.name_raw().to_vec(), rotate.position())
                    .map_err(|error| BinlogDecodeError::InvalidPosition(error.to_string()))?;
                let previous_position = self.current_position.clone();
                self.current_position = next.clone();
                self.table_maps.clear();
                Ok(DecodedBinlogEvent::BinlogRotated(RotatedBinlog {
                    previous_position,
                    next_position: next,
                }))
            }
            EventData::FormatDescriptionEvent(_) => {
                self.require_outside_transaction(event_type)?;
                self.table_maps.clear();
                self.decode_ignored(event_type, event_bytes, header.log_pos())
            }
            EventData::PreviousGtidsEvent(_) => {
                self.require_outside_transaction(event_type)?;
                self.decode_ignored(event_type, event_bytes, header.log_pos())
            }
            EventData::HeartbeatEvent
            | EventData::RowsQueryEvent(_)
            | EventData::IgnorableEvent(_) => {
                self.decode_ignored(event_type, event_bytes, header.log_pos())
            }
            EventData::StopEvent if self.active_transaction.is_none() => {
                self.decode_ignored(event_type, event_bytes, header.log_pos())
            }
            _ => Err(BinlogDecodeError::UnsupportedEvent(event_type)),
        }
    }

    fn decode_begin(
        &mut self,
        event_type: EventType,
        event_bytes: u64,
        header: &mysql_async::binlog::events::BinlogEventHeader,
    ) -> Result<DecodedBinlogEvent, BinlogDecodeError> {
        if self.active_transaction.is_none() {
            let next = self.next_position(event_type, header.log_pos())?;
            let marker = Arc::new(MySqlTransactionMarker {
                identity: MySqlTransactionIdentity::FilePosition {
                    begin_position: self.current_position.clone(),
                },
                begin_position: self.current_position.clone(),
            });
            self.active_transaction = Some(ActiveTransaction {
                marker: Arc::clone(&marker),
                event_count: 1,
                decoded_rows: 0,
                encoded_bytes: event_bytes,
                saw_begin_query: true,
            });
            self.current_position = next;
            return Ok(DecodedBinlogEvent::TransactionStarted(marker));
        }

        self.check_transaction_limits(event_type, event_bytes)?;
        let transaction = self
            .active_transaction
            .as_mut()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?;
        if transaction.saw_begin_query {
            return Err(BinlogDecodeError::DuplicateBegin);
        }
        transaction.saw_begin_query = true;
        let next = self.next_position(event_type, header.log_pos())?;
        self.apply_transaction_usage(event_type, event_bytes)?;
        self.current_position = next.clone();
        Ok(DecodedBinlogEvent::Ignored(IgnoredBinlogEvent {
            event_type,
            next_position: next,
            inside_transaction: true,
        }))
    }

    fn decode_commit(
        &mut self,
        event_type: EventType,
        event_bytes: u64,
        header: &mysql_async::binlog::events::BinlogEventHeader,
        xid: Option<u64>,
    ) -> Result<DecodedBinlogEvent, BinlogDecodeError> {
        self.require_transaction(event_type)?;
        self.check_transaction_limits(event_type, event_bytes)?;
        let next = self.next_position(event_type, header.log_pos())?;
        self.apply_transaction_usage(event_type, event_bytes)?;
        let transaction = self
            .active_transaction
            .take()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?;
        self.table_maps.clear();
        self.current_position = next.clone();
        Ok(DecodedBinlogEvent::TransactionCommitted(
            CommittedTransaction {
                transaction: transaction.marker,
                xid,
                next_position: next,
                event_count: transaction.event_count,
                encoded_bytes: transaction.encoded_bytes,
            },
        ))
    }

    fn decode_rollback(
        &mut self,
        event_type: EventType,
        event_bytes: u64,
        header: &mysql_async::binlog::events::BinlogEventHeader,
    ) -> Result<DecodedBinlogEvent, BinlogDecodeError> {
        self.require_transaction(event_type)?;
        self.check_transaction_limits(event_type, event_bytes)?;
        let next = self.next_position(event_type, header.log_pos())?;
        let transaction = self
            .active_transaction
            .take()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?;
        self.table_maps.clear();
        self.current_position = next.clone();
        Ok(DecodedBinlogEvent::TransactionRolledBack(
            RolledBackTransaction {
                transaction: transaction.marker,
                next_position: next,
            },
        ))
    }

    fn decode_rows(
        &mut self,
        event_type: EventType,
        event_bytes: u64,
        header: &mysql_async::binlog::events::BinlogEventHeader,
        rows: &RowsEventData<'_>,
    ) -> Result<DecodedBinlogEvent, BinlogDecodeError> {
        self.require_transaction(event_type)?;
        self.check_transaction_limits(event_type, event_bytes)?;
        if matches!(rows, RowsEventData::PartialUpdateRowsEvent(_)) {
            return Err(BinlogDecodeError::PartialJsonUpdateObserved);
        }
        let table_map = self
            .table_maps
            .get(&rows.table_id())
            .cloned()
            .ok_or(BinlogDecodeError::MissingTableMap(rows.table_id()))?;
        if rows.num_columns() != table_map.columns_count() {
            return Err(BinlogDecodeError::ColumnCountMismatch {
                table_id: rows.table_id(),
                table_map_columns: table_map.columns_count(),
                rows_event_columns: rows.num_columns(),
            });
        }

        if !self.table_is_selected(&table_map) {
            let next = self.next_position(event_type, header.log_pos())?;
            self.apply_transaction_usage(event_type, event_bytes)?;
            self.current_position = next.clone();
            return Ok(DecodedBinlogEvent::Ignored(IgnoredBinlogEvent {
                event_type,
                next_position: next,
                inside_transaction: true,
            }));
        }

        let (operation, before_columns, after_columns) = row_event_shape(rows)?;
        require_full_image(rows.table_id(), rows.num_columns(), &before_columns, "before")?;
        require_full_image(rows.table_id(), rows.num_columns(), &after_columns, "after")?;

        let previously_decoded_rows = self
            .active_transaction
            .as_ref()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?
            .decoded_rows;
        let mut decoded_rows = Vec::new();
        for row in rows.rows(&table_map) {
            let (before, after) = row.map_err(|error| BinlogDecodeError::MalformedRows {
                table_id: rows.table_id(),
                reason: error.to_string(),
            })?;
            validate_row_shape(operation, &before, &after, rows.num_columns())?;
            let event_rows = u64::try_from(decoded_rows.len())
                .map_err(|_| BinlogDecodeError::TransactionRowCountOverflow)?;
            let transaction_row_count = previously_decoded_rows
                .checked_add(event_rows)
                .and_then(|count| count.checked_add(1))
                .ok_or(BinlogDecodeError::TransactionRowCountOverflow)?;
            if transaction_row_count > self.config.max_events as u64 {
                return Err(BinlogDecodeError::TooManyTransactionRows {
                    row_count: transaction_row_count,
                    configured_max_events: self.config.max_events,
                });
            }
            let before = binlog_row_values(before, rows.table_id(), "before")?;
            let after = binlog_row_values(after, rows.table_id(), "after")?;
            decoded_rows.push(MySqlRowChange { before, after });
        }

        let decoded_row_count = u64::try_from(decoded_rows.len())
            .map_err(|_| BinlogDecodeError::TransactionRowCountOverflow)?;
        let transaction_row_count = previously_decoded_rows
            .checked_add(decoded_row_count)
            .ok_or(BinlogDecodeError::TransactionRowCountOverflow)?;

        let next = self.next_position(event_type, header.log_pos())?;
        let transaction = Arc::clone(
            &self
                .active_transaction
                .as_ref()
                .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?
                .marker,
        );
        let table = table_identity(&table_map)?;
        self.apply_transaction_usage(event_type, event_bytes)?;
        self.active_transaction
            .as_mut()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?
            .decoded_rows = transaction_row_count;
        self.current_position = next.clone();
        Ok(DecodedBinlogEvent::Rows(DecodedRowsEvent {
            transaction,
            operation,
            table,
            before_columns,
            after_columns,
            rows: decoded_rows,
            source_timestamp_seconds: header.timestamp(),
            observed_next_position: next,
        }))
    }

    fn decode_ignored(
        &mut self,
        event_type: EventType,
        event_bytes: u64,
        log_pos: u32,
    ) -> Result<DecodedBinlogEvent, BinlogDecodeError> {
        if self.active_transaction.is_some() {
            self.check_transaction_limits(event_type, event_bytes)?;
        }
        let next = self.next_position(event_type, log_pos)?;
        let inside_transaction = self.active_transaction.is_some();
        if inside_transaction {
            self.apply_transaction_usage(event_type, event_bytes)?;
        }
        self.current_position = next.clone();
        Ok(DecodedBinlogEvent::Ignored(IgnoredBinlogEvent {
            event_type,
            next_position: next,
            inside_transaction,
        }))
    }

    fn require_transaction(&self, event_type: EventType) -> Result<(), BinlogDecodeError> {
        if self.active_transaction.is_none() {
            return Err(BinlogDecodeError::TransactionNotActive(event_type));
        }
        Ok(())
    }

    fn require_outside_transaction(
        &self,
        event_type: EventType,
    ) -> Result<(), BinlogDecodeError> {
        if self.active_transaction.is_some() {
            return Err(BinlogDecodeError::BootstrapEventInsideTransaction(event_type));
        }
        Ok(())
    }

    fn table_is_selected(&self, table_map: &TableMapEvent<'_>) -> bool {
        self.selected_tables.as_ref().is_none_or(|selected| {
            selected.iter().any(|(database, table)| {
                database.as_slice() == table_map.database_name_raw()
                    && table.as_slice() == table_map.table_name_raw()
            })
        })
    }

    fn check_transaction_limits(
        &self,
        event_type: EventType,
        event_bytes: u64,
    ) -> Result<(), BinlogDecodeError> {
        let transaction = self
            .active_transaction
            .as_ref()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?;
        let event_count = transaction.event_count.saturating_add(1);
        if event_count > self.config.max_events as u64 {
            return Err(BinlogDecodeError::TooManyTransactionEvents {
                event_count,
                configured_max_events: self.config.max_events,
            });
        }
        let encoded_bytes = transaction
            .encoded_bytes
            .checked_add(event_bytes)
            .ok_or(BinlogDecodeError::TransactionSizeOverflow)?;
        if encoded_bytes > self.config.max_transaction_bytes as u64 {
            return Err(BinlogDecodeError::TransactionTooLarge {
                encoded_bytes,
                configured_max_transaction_bytes: self.config.max_transaction_bytes,
            });
        }
        Ok(())
    }

    fn apply_transaction_usage(
        &mut self,
        event_type: EventType,
        event_bytes: u64,
    ) -> Result<(), BinlogDecodeError> {
        let transaction = self
            .active_transaction
            .as_mut()
            .ok_or(BinlogDecodeError::TransactionNotActive(event_type))?;
        transaction.event_count = transaction
            .event_count
            .checked_add(1)
            .ok_or(BinlogDecodeError::TransactionSizeOverflow)?;
        transaction.encoded_bytes = transaction
            .encoded_bytes
            .checked_add(event_bytes)
            .ok_or(BinlogDecodeError::TransactionSizeOverflow)?;
        Ok(())
    }

    fn next_position(
        &self,
        event_type: EventType,
        log_pos: u32,
    ) -> Result<MySqlBinlogPosition, BinlogDecodeError> {
        if log_pos == 0 {
            return Ok(self.current_position.clone());
        }
        if log_pos < self.current_position.position {
            if matches!(
                event_type,
                EventType::FORMAT_DESCRIPTION_EVENT | EventType::PREVIOUS_GTIDS_EVENT
            ) {
                return Ok(self.current_position.clone());
            }
            return Err(BinlogDecodeError::PositionMovedBackwards {
                filename: self.current_position.filename.clone(),
                previous: self.current_position.position,
                received: log_pos,
            });
        }
        MySqlBinlogPosition::new(self.current_position.filename.clone(), u64::from(log_pos))
            .map_err(|error| BinlogDecodeError::InvalidPosition(error.to_string()))
    }
}

fn row_event_shape(
    rows: &RowsEventData<'_>,
) -> Result<(MySqlRowOperation, Vec<bool>, Vec<bool>), BinlogDecodeError> {
    let before = rows
        .columns_before_image()
        .map(|bits| bits.iter().by_vals().collect())
        .unwrap_or_default();
    let after = rows
        .columns_after_image()
        .map(|bits| bits.iter().by_vals().collect())
        .unwrap_or_default();
    let operation = match rows {
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => {
            MySqlRowOperation::Write
        }
        RowsEventData::UpdateRowsEventV1(_) | RowsEventData::UpdateRowsEvent(_) => {
            MySqlRowOperation::Update
        }
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
            MySqlRowOperation::Delete
        }
        RowsEventData::PartialUpdateRowsEvent(_) => {
            return Err(BinlogDecodeError::PartialJsonUpdateObserved)
        }
    };
    Ok((operation, before, after))
}

fn require_full_image(
    table_id: u64,
    columns: u64,
    bitmap: &[bool],
    image: &'static str,
) -> Result<(), BinlogDecodeError> {
    if bitmap.is_empty() {
        return Ok(());
    }
    let expected = usize::try_from(columns)
        .map_err(|_| BinlogDecodeError::ColumnCountDoesNotFitPlatform(columns))?;
    if bitmap.len() != expected || bitmap.iter().any(|present| !present) {
        return Err(BinlogDecodeError::PartialRowImage {
            table_id,
            image,
            columns,
            present_columns: u64::try_from(
                bitmap.iter().filter(|present| **present).count(),
            )
            .map_err(|_| BinlogDecodeError::ColumnCountDoesNotFitPlatform(columns))?,
        });
    }
    Ok(())
}

fn validate_row_shape(
    operation: MySqlRowOperation,
    before: &Option<BinlogRow>,
    after: &Option<BinlogRow>,
    columns: u64,
) -> Result<(), BinlogDecodeError> {
    let expected = usize::try_from(columns)
        .map_err(|_| BinlogDecodeError::ColumnCountDoesNotFitPlatform(columns))?;
    let valid = match operation {
        MySqlRowOperation::Write => before.is_none() && after.is_some(),
        MySqlRowOperation::Update => before.is_some() && after.is_some(),
        MySqlRowOperation::Delete => before.is_some() && after.is_none(),
    };
    if !valid {
        return Err(BinlogDecodeError::UnexpectedRowShape(operation));
    }
    for row in before.iter().chain(after.iter()) {
        if row.len() != expected {
            return Err(BinlogDecodeError::DecodedColumnCountMismatch {
                expected: columns,
                decoded: row.len(),
            });
        }
    }
    Ok(())
}

fn binlog_row_values(
    mut row: Option<BinlogRow>,
    table_id: u64,
    image: &'static str,
) -> Result<Option<Vec<BinlogValue<'static>>>, BinlogDecodeError> {
    let Some(row) = row.as_mut() else {
        return Ok(None);
    };
    let mut values = Vec::with_capacity(row.len());
    for index in 0..row.len() {
        let value = row.take(index).ok_or(BinlogDecodeError::RowValueConversion {
            table_id,
            image,
            reason: format!("decoded row omitted value at column {}", index + 1),
        })?;
        values.push(value.into_owned());
    }
    Ok(Some(values))
}

fn table_identity(
    table_map: &TableMapEvent<'_>,
) -> Result<MySqlTableIdentity, BinlogDecodeError> {
    let column_count = usize::try_from(table_map.columns_count()).map_err(|_| {
        BinlogDecodeError::ColumnCountDoesNotFitPlatform(table_map.columns_count())
    })?;
    let metadata = OptionalMetaExtractor::new(table_map.iter_optional_meta()).map_err(|error| {
        BinlogDecodeError::MalformedTableMetadata {
            table_id: table_map.table_id(),
            reason: error.to_string(),
        }
    })?;
    let names = metadata
        .iter_column_name()
        .map(|name| name.map(|name| name.name_raw().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id: table_map.table_id(),
            reason: error.to_string(),
        })?;
    if names.len() != column_count {
        return Err(BinlogDecodeError::IncompleteFullTableMetadata {
            table_id: table_map.table_id(),
            field: "column names",
            expected: column_count,
            received: names.len(),
        });
    }
    let column_types = (0..column_count)
        .map(|index| {
            table_map
                .get_column_type(index)
                .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
                    table_id: table_map.table_id(),
                    reason: error.to_string(),
                })?
                .ok_or(BinlogDecodeError::IncompleteFullTableMetadata {
                    table_id: table_map.table_id(),
                    field: "column types",
                    expected: column_count,
                    received: index,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let numeric_count = column_types
        .iter()
        .filter(|column_type| column_type.is_numeric_type())
        .count();
    let signedness = metadata
        .iter_signedness()
        .take(numeric_count)
        .collect::<Vec<_>>();
    if signedness.len() != numeric_count {
        return Err(BinlogDecodeError::IncompleteFullTableMetadata {
            table_id: table_map.table_id(),
            field: "numeric signedness",
            expected: numeric_count,
            received: signedness.len(),
        });
    }
    let character_count = column_types
        .iter()
        .filter(|column_type| column_type.is_character_type())
        .count();
    let character_collations = metadata
        .iter_charset()
        .take(character_count)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id: table_map.table_id(),
            reason: error.to_string(),
        })?;
    if character_collations.len() != character_count {
        return Err(BinlogDecodeError::IncompleteFullTableMetadata {
            table_id: table_map.table_id(),
            field: "character collations",
            expected: character_count,
            received: character_collations.len(),
        });
    }
    let enum_set_count = column_types
        .iter()
        .filter(|column_type| column_type.is_enum_or_set_type())
        .count();
    let enum_set_collations = metadata
        .iter_enum_and_set_charset()
        .take(enum_set_count)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id: table_map.table_id(),
            reason: error.to_string(),
        })?;
    if enum_set_collations.len() != enum_set_count {
        return Err(BinlogDecodeError::IncompleteFullTableMetadata {
            table_id: table_map.table_id(),
            field: "enum/set collations",
            expected: enum_set_count,
            received: enum_set_collations.len(),
        });
    }
    let full_metadata = full_column_metadata(table_map, &column_types)?;
    let primary_key = primary_key_metadata(table_map, column_count)?;

    let mut next_numeric = 0;
    let mut next_character = 0;
    let mut next_enum_set = 0;
    let mut next_enum = 0;
    let mut next_set = 0;
    let mut next_geometry = 0;
    let mut next_vector = 0;
    let mut column_identities = Vec::with_capacity(column_count);
    for (index, column_type) in column_types.into_iter().enumerate() {
        let unsigned = column_type.is_numeric_type().then(|| {
            let value = signedness[next_numeric];
            next_numeric += 1;
            value
        });
        let collation_id = if column_type.is_character_type() {
            let value = character_collations[next_character];
            next_character += 1;
            Some(value)
        } else if column_type.is_enum_or_set_type() {
            let value = enum_set_collations[next_enum_set];
            next_enum_set += 1;
            Some(value)
        } else {
            None
        };
        let (primary_key_ordinal, primary_key_prefix_length) = primary_key[index]
            .map_or((None, None), |(ordinal, prefix)| (Some(ordinal), prefix));
        let enum_values = (column_type == ColumnType::MYSQL_TYPE_ENUM).then(|| {
            let values = full_metadata.enum_values[next_enum].clone();
            next_enum += 1;
            values
        });
        let set_values = (column_type == ColumnType::MYSQL_TYPE_SET).then(|| {
            let values = full_metadata.set_values[next_set].clone();
            next_set += 1;
            values
        });
        let geometry_type = (column_type == ColumnType::MYSQL_TYPE_GEOMETRY).then(|| {
            let value = full_metadata.geometry_types[next_geometry];
            next_geometry += 1;
            value
        });
        let vector_dimensionality = (column_type == ColumnType::MYSQL_TYPE_VECTOR).then(|| {
            let value = full_metadata.vector_dimensionalities[next_vector];
            next_vector += 1;
            value
        });
        column_identities.push(MySqlBinlogColumnIdentity {
            name: names[index].clone(),
            column_type,
            metadata: table_map
                .get_column_metadata(index)
                .unwrap_or_default()
                .to_vec(),
            nullable: table_map
                .null_bitmask()
                .get(index)
                .as_deref()
                .copied()
                .unwrap_or(false),
            unsigned,
            collation_id,
            enum_values,
            set_values,
            geometry_type,
            vector_dimensionality,
            visible: full_metadata.visibility[index],
            primary_key_ordinal,
            primary_key_prefix_length,
        });
    }

    Ok(MySqlTableIdentity {
        table_id: table_map.table_id(),
        database: table_map.database_name_raw().to_vec(),
        table: table_map.table_name_raw().to_vec(),
        columns: table_map.columns_count(),
        column_identities,
    })
}

struct FullColumnMetadata {
    enum_values: Vec<Vec<Vec<u8>>>,
    set_values: Vec<Vec<Vec<u8>>>,
    geometry_types: Vec<GeometryType>,
    vector_dimensionalities: Vec<u64>,
    visibility: Vec<bool>,
}

fn full_column_metadata(
    table_map: &TableMapEvent<'_>,
    column_types: &[ColumnType],
) -> Result<FullColumnMetadata, BinlogDecodeError> {
    let table_id = table_map.table_id();
    let character_count = column_types
        .iter()
        .filter(|column_type| column_type.is_character_type())
        .count();
    let enum_set_count = column_types
        .iter()
        .filter(|column_type| column_type.is_enum_or_set_type())
        .count();
    let mut enum_values = None;
    let mut set_values = None;
    let mut geometry_types = None;
    let mut vector_dimensionalities = None;
    let mut visibility = None;
    let mut signedness = false;
    let mut character_collations = false;
    let mut enum_set_collations = false;
    let mut column_names = false;
    let mut primary_key = false;
    for field in table_map.iter_optional_meta() {
        let field = field.map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id,
            reason: error.to_string(),
        })?;
        match field {
            OptionalMetadataField::Signedness(_) => {
                require_metadata_flag_absent(table_id, "numeric signedness", &mut signedness)?;
            }
            OptionalMetadataField::DefaultCharset(values) => {
                require_metadata_flag_absent(
                    table_id,
                    "character collations",
                    &mut character_collations,
                )?;
                validate_default_charset(table_id, "character collations", &values, character_count)?;
            }
            OptionalMetadataField::ColumnCharset(values) => {
                require_metadata_flag_absent(
                    table_id,
                    "character collations",
                    &mut character_collations,
                )?;
                validate_per_column_charset(
                    table_id,
                    "character collations",
                    values.iter_charsets(),
                    character_count,
                )?;
            }
            OptionalMetadataField::EnumAndSetDefaultCharset(values) => {
                require_metadata_flag_absent(
                    table_id,
                    "enum/set collations",
                    &mut enum_set_collations,
                )?;
                validate_default_charset(table_id, "enum/set collations", &values, enum_set_count)?;
            }
            OptionalMetadataField::EnumAndSetColumnCharset(values) => {
                require_metadata_flag_absent(
                    table_id,
                    "enum/set collations",
                    &mut enum_set_collations,
                )?;
                validate_per_column_charset(
                    table_id,
                    "enum/set collations",
                    values.iter_charsets(),
                    enum_set_count,
                )?;
            }
            OptionalMetadataField::ColumnName(_) => {
                require_metadata_flag_absent(table_id, "column names", &mut column_names)?;
            }
            OptionalMetadataField::SimplePrimaryKey(_)
            | OptionalMetadataField::PrimaryKeyWithPrefix(_) => {
                require_metadata_flag_absent(table_id, "primary key", &mut primary_key)?;
            }
            OptionalMetadataField::EnumStrValue(values) => {
                require_metadata_absent(table_id, "enum values", &enum_values)?;
                enum_values = Some(
                    values
                        .iter_values()
                        .map(|values| {
                            values.map(|values| {
                                values
                                    .values()
                                    .iter()
                                    .map(|value| value.value_raw().to_vec())
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
                            table_id,
                            reason: error.to_string(),
                        })?,
                );
            }
            OptionalMetadataField::SetStrValue(values) => {
                require_metadata_absent(table_id, "set values", &set_values)?;
                set_values = Some(
                    values
                        .iter_values()
                        .map(|values| {
                            values.map(|values| {
                                values
                                    .values()
                                    .iter()
                                    .map(|value| value.value_raw().to_vec())
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
                            table_id,
                            reason: error.to_string(),
                        })?,
                );
            }
            OptionalMetadataField::GeometryType(values) => {
                require_metadata_absent(table_id, "geometry types", &geometry_types)?;
                geometry_types = Some(
                    values
                        .iter_geometry_types()
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
                            table_id,
                            reason: error.to_string(),
                        })?,
                );
            }
            OptionalMetadataField::Dimensionality(values) => {
                require_metadata_absent(
                    table_id,
                    "vector dimensionalities",
                    &vector_dimensionalities,
                )?;
                vector_dimensionalities = Some(
                    values
                        .iter_dimensionalities()
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
                            table_id,
                            reason: error.to_string(),
                        })?,
                );
            }
            OptionalMetadataField::ColumnVisibility(bits) => {
                require_metadata_absent(table_id, "column visibility", &visibility)?;
                visibility = Some(bits.iter().by_vals().collect::<Vec<_>>());
            }
        }
    }

    let enum_count = count_type(column_types, ColumnType::MYSQL_TYPE_ENUM);
    let set_count = count_type(column_types, ColumnType::MYSQL_TYPE_SET);
    let geometry_count = count_type(column_types, ColumnType::MYSQL_TYPE_GEOMETRY);
    let vector_count = count_type(column_types, ColumnType::MYSQL_TYPE_VECTOR);
    Ok(FullColumnMetadata {
        enum_values: require_metadata_count(table_id, "enum values", enum_values, enum_count)?,
        set_values: require_metadata_count(table_id, "set values", set_values, set_count)?,
        geometry_types: require_metadata_count(
            table_id,
            "geometry types",
            geometry_types,
            geometry_count,
        )?,
        vector_dimensionalities: require_metadata_count(
            table_id,
            "vector dimensionalities",
            vector_dimensionalities,
            vector_count,
        )?,
        visibility: require_metadata_count(
            table_id,
            "column visibility",
            visibility,
            column_types.len(),
        )?,
    })
}

fn count_type(column_types: &[ColumnType], expected: ColumnType) -> usize {
    column_types
        .iter()
        .filter(|column_type| **column_type == expected)
        .count()
}

fn require_metadata_absent<T>(
    table_id: u64,
    field: &'static str,
    value: &Option<T>,
) -> Result<(), BinlogDecodeError> {
    if value.is_some() {
        return Err(BinlogDecodeError::DuplicateFullTableMetadata { table_id, field });
    }
    Ok(())
}

fn require_metadata_flag_absent(
    table_id: u64,
    field: &'static str,
    value: &mut bool,
) -> Result<(), BinlogDecodeError> {
    if *value {
        return Err(BinlogDecodeError::DuplicateFullTableMetadata { table_id, field });
    }
    *value = true;
    Ok(())
}

fn validate_default_charset(
    table_id: u64,
    field: &'static str,
    values: &mysql_async::binlog::events::DefaultCharset<'_>,
    expected: usize,
) -> Result<(), BinlogDecodeError> {
    let mut previous = None;
    for value in values.iter_non_default() {
        let value = value.map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id,
            reason: error.to_string(),
        })?;
        let index = usize::try_from(value.column_index()).map_err(|_| {
            BinlogDecodeError::ColumnCountDoesNotFitPlatform(value.column_index())
        })?;
        if index >= expected || previous.is_some_and(|previous| index <= previous) {
            return Err(BinlogDecodeError::MalformedTableMetadata {
                table_id,
                reason: format!("{field} contain an out-of-order or out-of-range override"),
            });
        }
        previous = Some(index);
    }
    Ok(())
}

fn validate_per_column_charset(
    table_id: u64,
    field: &'static str,
    values: impl Iterator<Item = std::io::Result<u16>>,
    expected: usize,
) -> Result<(), BinlogDecodeError> {
    let received = values
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id,
            reason: error.to_string(),
        })?
        .len();
    if received != expected {
        return Err(BinlogDecodeError::IncompleteFullTableMetadata {
            table_id,
            field,
            expected,
            received,
        });
    }
    Ok(())
}

fn require_metadata_count<T>(
    table_id: u64,
    field: &'static str,
    value: Option<Vec<T>>,
    expected: usize,
) -> Result<Vec<T>, BinlogDecodeError> {
    let value = value.unwrap_or_default();
    if value.len() != expected {
        return Err(BinlogDecodeError::IncompleteFullTableMetadata {
            table_id,
            field,
            expected,
            received: value.len(),
        });
    }
    Ok(value)
}

fn primary_key_metadata(
    table_map: &TableMapEvent<'_>,
    column_count: usize,
) -> Result<Vec<Option<(u64, Option<u64>)>>, BinlogDecodeError> {
    let mut keys = Vec::new();
    for field in table_map.iter_optional_meta() {
        match field.map_err(|error| BinlogDecodeError::MalformedTableMetadata {
            table_id: table_map.table_id(),
            reason: error.to_string(),
        })? {
            OptionalMetadataField::SimplePrimaryKey(primary) => {
                for index in primary.iter_indexes() {
                    keys.push((index.map_err(|error| {
                        BinlogDecodeError::MalformedTableMetadata {
                            table_id: table_map.table_id(),
                            reason: error.to_string(),
                        }
                    })?, None));
                }
            }
            OptionalMetadataField::PrimaryKeyWithPrefix(primary) => {
                for key in primary.iter_keys() {
                    let key = key.map_err(|error| BinlogDecodeError::MalformedTableMetadata {
                        table_id: table_map.table_id(),
                        reason: error.to_string(),
                    })?;
                    keys.push((
                        key.column_index(),
                        (key.prefix_length() != 0).then_some(key.prefix_length()),
                    ));
                }
            }
            _ => {}
        }
    }
    let mut result = vec![None; column_count];
    for (offset, (index, prefix)) in keys.into_iter().enumerate() {
        let index = usize::try_from(index).map_err(|_| {
            BinlogDecodeError::PrimaryKeyColumnOutOfBounds {
                table_id: table_map.table_id(),
                column: u64::MAX,
                columns: table_map.columns_count(),
            }
        })?;
        if index >= column_count || result[index].is_some() {
            return Err(BinlogDecodeError::PrimaryKeyColumnOutOfBounds {
                table_id: table_map.table_id(),
                column: u64::try_from(index).unwrap_or(u64::MAX),
                columns: table_map.columns_count(),
            });
        }
        result[index] = Some((
            u64::try_from(offset)
                .map_err(|_| BinlogDecodeError::ColumnCountDoesNotFitPlatform(u64::MAX))?
                .checked_add(1)
                .ok_or(BinlogDecodeError::ColumnCountDoesNotFitPlatform(u64::MAX))?,
            prefix,
        ));
    }
    Ok(result)
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinlogDecodeError {
    InvalidConfig(String),
    InvalidPosition(String),
    Checksum(BinlogChecksumError),
    UnknownEventType(u8),
    MalformedEvent {
        event_type: EventType,
        reason: String,
    },
    UnsupportedEvent(EventType),
    UnsupportedStatement { schema: Vec<u8> },
    EventTooLarge {
        event_bytes: u64,
        configured_max_transaction_bytes: usize,
    },
    TransactionAlreadyActive(EventType),
    TransactionNotActive(EventType),
    BootstrapEventInsideTransaction(EventType),
    DuplicateBegin,
    TooManyTransactionEvents {
        event_count: u64,
        configured_max_events: usize,
    },
    TransactionTooLarge {
        encoded_bytes: u64,
        configured_max_transaction_bytes: usize,
    },
    TransactionSizeOverflow,
    TransactionRowCountOverflow,
    TransactionCompressionObserved,
    PartialJsonUpdateObserved,
    MissingTableMap(u64),
    ColumnCountMismatch {
        table_id: u64,
        table_map_columns: u64,
        rows_event_columns: u64,
    },
    ColumnCountDoesNotFitPlatform(u64),
    PartialRowImage {
        table_id: u64,
        image: &'static str,
        columns: u64,
        present_columns: u64,
    },
    MalformedRows {
        table_id: u64,
        reason: String,
    },
    RowValueConversion {
        table_id: u64,
        image: &'static str,
        reason: String,
    },
    TooManyTransactionRows {
        row_count: u64,
        configured_max_events: usize,
    },
    UnexpectedRowShape(MySqlRowOperation),
    DecodedColumnCountMismatch {
        expected: u64,
        decoded: usize,
    },
    MalformedTableMetadata {
        table_id: u64,
        reason: String,
    },
    IncompleteFullTableMetadata {
        table_id: u64,
        field: &'static str,
        expected: usize,
        received: usize,
    },
    DuplicateFullTableMetadata {
        table_id: u64,
        field: &'static str,
    },
    PrimaryKeyColumnOutOfBounds {
        table_id: u64,
        column: u64,
        columns: u64,
    },
    RotateInsideTransaction,
    UnexpectedFakeRotate {
        expected_filename: Vec<u8>,
        received_filename: Vec<u8>,
    },
    PositionMovedBackwards {
        filename: Vec<u8>,
        previous: u32,
        received: u32,
    },
}

impl From<BinlogChecksumError> for BinlogDecodeError {
    fn from(error: BinlogChecksumError) -> Self {
        Self::Checksum(error)
    }
}

impl fmt::Display for BinlogDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL binlog decode failed: {self:?}")
    }
}

impl Error for BinlogDecodeError {}
