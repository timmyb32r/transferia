use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array as _, BinaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray, UInt64Array,
};
use transferia_core::data::schema::{
    META_OLD_KEY_OF, META_OLD_VALUE_OF, META_PRIMARY_KEY, META_SYSTEM_ROLE,
    SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_BINLOG_FILE, SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
    SYSTEM_ROLE_SOURCE_BINLOG_ROW, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_GTID,
    SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_SERVER_ID, SYSTEM_ROLE_SOURCE_TABLE,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::sink::SinkBatch;

use super::json_serializer::{JsonBatchEncoder, JsonColumnProjection};

const UNAVAILABLE_VALUE: &[u8] = b"\"__debezium_unavailable_value\"";

#[derive(Debug)]
pub struct SerializedMessage {
    pub key: Option<Vec<u8>>,

    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct SerializedBatch {
    pub table: Arc<str>,

    pub messages: Vec<SerializedMessage>,
}

#[derive(Debug)]
pub struct SerializedDelivery {
    pub batches: Vec<SerializedBatch>,

    pub source_rows: u64,
}

impl SerializedDelivery {
    pub fn payload_bytes(&self) -> anyhow::Result<u64> {
        self.batches
            .iter()
            .flat_map(|batch| &batch.messages)
            .try_fold(0_u64, |total, message| {
                let bytes = message.key.as_ref().map_or(0, Vec::len)
                    + message.value.as_ref().map_or(0, Vec::len);
                total
                    .checked_add(u64::try_from(bytes)?)
                    .ok_or_else(|| anyhow::anyhow!("serialized queue payload size overflow"))
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMessageMode {
    KeyedWithTombstones,
    ValuesOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DebeziumSourceDialect {
    Postgres,
    MySql,
    Ydb,
}

impl DebeziumSourceDialect {
    pub(super) fn from_source_name(source_name: &str) -> anyhow::Result<Self> {
        match source_name {
            "postgres" => Ok(Self::Postgres),
            "mysql" => Ok(Self::MySql),
            "ydb" => Ok(Self::Ydb),
            _ => anyhow::bail!(
                "Debezium serializer requires source_name to be exactly 'postgres', 'mysql', or 'ydb', got '{source_name}'"
            ),
        }
    }
}

pub(super) struct DebeziumJsonEncoder {
    logical_name: String,
    mode: QueueMessageMode,
}

impl DebeziumJsonEncoder {
    pub(super) const fn new(logical_name: String, mode: QueueMessageMode) -> Self {
        Self { logical_name, mode }
    }

    pub(super) fn encode_batch(
        &self,
        batch: &SinkBatch,
        dialect: DebeziumSourceDialect,
        message_size_limit: usize,
    ) -> anyhow::Result<SerializedBatch> {
        let encoder = DebeziumBatchEncoder::new(batch, dialect)?;
        let mut messages = Vec::with_capacity(batch.rows());
        for row in 0..batch.rows() {
            let operation = encoder.operation(row)?;
            if operation == "u" && !encoder.old_key.row_equals(&encoder.current_key, row) {
                let old_key = self.key(&encoder.old_key, row)?;
                let delete = encoder.envelope(row, &self.logical_name, "d", false, true, true)?;
                validate_message_size(old_key.as_deref(), Some(&delete), message_size_limit)?;
                messages.push(SerializedMessage {
                    key: old_key.clone(),
                    value: Some(delete),
                });
                if self.mode == QueueMessageMode::KeyedWithTombstones {
                    validate_message_size(old_key.as_deref(), None, message_size_limit)?;
                    messages.push(SerializedMessage {
                        key: old_key,
                        value: None,
                    });
                }
                let key = self.key(&encoder.current_key, row)?;
                let create = encoder.envelope(row, &self.logical_name, "c", true, false, true)?;
                validate_message_size(key.as_deref(), Some(&create), message_size_limit)?;
                messages.push(SerializedMessage {
                    key,
                    value: Some(create),
                });
                continue;
            }

            let key_encoder = if operation == "d" {
                &encoder.old_key
            } else {
                &encoder.current_key
            };
            let key = self.key(key_encoder, row)?;
            let value = encoder.envelope(
                row,
                &self.logical_name,
                operation,
                operation != "d",
                operation != "c" && operation != "r",
                operation == "u",
            )?;
            validate_message_size(key.as_deref(), Some(&value), message_size_limit)?;
            messages.push(SerializedMessage {
                key: key.clone(),
                value: Some(value),
            });
            if operation == "d" && self.mode == QueueMessageMode::KeyedWithTombstones {
                validate_message_size(key.as_deref(), None, message_size_limit)?;
                messages.push(SerializedMessage { key, value: None });
            }
        }
        Ok(SerializedBatch {
            table: Arc::clone(&batch.table),
            messages,
        })
    }

    fn key(&self, encoder: &JsonBatchEncoder, row: usize) -> anyhow::Result<Option<Vec<u8>>> {
        if self.mode == QueueMessageMode::ValuesOnly {
            return Ok(None);
        }
        let mut key = Vec::new();
        encoder.write_object(row, &mut key)?;
        Ok(Some(key))
    }
}

struct DebeziumBatchEncoder {
    dialect: DebeziumSourceDialect,
    current: JsonBatchEncoder,
    before: JsonBatchEncoder,
    current_key: JsonBatchEncoder,
    old_key: JsonBatchEncoder,
    changed_columns: Option<BinaryArray>,
    operation: Option<StringArray>,
    database: StringArray,
    source_table: StringArray,
    source_metadata: DebeziumSourceMetadata,
    source_timestamp_ms: Int64Array,
    user_ordinal_by_source_index: Vec<Option<usize>>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "boxing Arrow arrays would add allocation and indirection on every serialized batch"
)]
enum DebeziumSourceMetadata {
    Postgres {
        transaction_id: UInt64Array,
        lsn: Int64Array,
        source_schema: StringArray,
        source_timestamp_us: Int64Array,
        source_timestamp_ns: Int64Array,
        event_timestamp_ms: Int64Array,
        event_timestamp_us: Int64Array,
        event_timestamp_ns: Int64Array,
    },
    MySql {
        transaction_identity: BinaryArray,
        server_id: Int64Array,
        gtid: StringArray,
        binlog_file: StringArray,
        binlog_position: Int64Array,
        binlog_row: Int32Array,
        _source_schema: StringArray,
        source_timestamp_us: Int64Array,
        source_timestamp_ns: Int64Array,
        event_timestamp_ms: Int64Array,
        event_timestamp_us: Int64Array,
        event_timestamp_ns: Int64Array,
    },
    Ydb {
        transaction_identity: FixedSizeBinaryArray,
        event_timestamp_ms: Int64Array,
    },
}

impl DebeziumBatchEncoder {
    fn new(batch: &SinkBatch, dialect: DebeziumSourceDialect) -> anyhow::Result<Self> {
        let schema = batch.batch.schema();
        let system_indexes = batch
            .system_columns
            .iter()
            .map(|column| column.index)
            .collect::<std::collections::HashSet<_>>();
        let user_columns = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, field)| {
                !system_indexes.contains(index)
                    && !field.metadata().contains_key(META_OLD_VALUE_OF)
                    && !field.metadata().contains_key(META_OLD_KEY_OF)
                    && !field.metadata().contains_key(META_SYSTEM_ROLE)
            })
            .map(|(index, field)| (index, field.name().to_owned()))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !user_columns.is_empty(),
            "Debezium serializer requires at least one user column"
        );
        let old_value = mapped_columns(batch, META_OLD_VALUE_OF)?;
        let old_key = mapped_columns(batch, META_OLD_KEY_OF)?;
        if matches!(
            dialect,
            DebeziumSourceDialect::MySql | DebeziumSourceDialect::Ydb
        ) {
            for (current_index, name) in &user_columns {
                let old_index = old_value.get(name).copied().ok_or_else(|| {
                    anyhow::anyhow!(
                        "{dialect:?} Debezium input column '{name}' is missing its full old-value mapping"
                    )
                })?;
                let current = schema.field(*current_index);
                let old = schema.field(old_index);
                anyhow::ensure!(
                    old.data_type() == current.data_type()
                        && old.metadata().get(transferia_core::data::schema::META_ARROW_EXTENSION_NAME)
                            == current
                                .metadata()
                                .get(transferia_core::data::schema::META_ARROW_EXTENSION_NAME)
                        && old
                            .metadata()
                            .get(transferia_core::data::schema::META_ARROW_EXTENSION_METADATA)
                            == current
                                .metadata()
                                .get(transferia_core::data::schema::META_ARROW_EXTENSION_METADATA),
                    "{dialect:?} Debezium input old value for '{name}' does not preserve its exact physical Arrow type and extension metadata"
                );
            }
        }
        let current_projection = user_columns
            .iter()
            .map(|(index, name)| JsonColumnProjection {
                output_name: name.clone(),
                source_index: Some(*index),
            })
            .collect::<Vec<_>>();
        let current = projected_debezium(&batch.batch, current_projection, dialect)?;
        let before_projection = user_columns
            .iter()
            .map(|(_, name)| JsonColumnProjection {
                output_name: name.clone(),
                source_index: old_value.get(name).or_else(|| old_key.get(name)).copied(),
            })
            .collect::<Vec<_>>();
        let before = projected_debezium(&batch.batch, before_projection, dialect)?;
        let primary_keys = user_columns
            .iter()
            .filter(|(index, _)| {
                schema
                    .field(*index)
                    .metadata()
                    .get(META_PRIMARY_KEY)
                    .map(String::as_str)
                    == Some("true")
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !primary_keys.is_empty(),
            "Debezium serializer requires at least one primary-key column"
        );
        let current_key_projection = primary_keys
            .iter()
            .map(|(index, name)| JsonColumnProjection {
                output_name: (*name).clone(),
                source_index: Some(*index),
            })
            .collect::<Vec<_>>();
        let current_key = projected_debezium(&batch.batch, current_key_projection, dialect)?;
        let old_key_projection = primary_keys
            .iter()
            .map(|(index, name)| JsonColumnProjection {
                output_name: (*name).clone(),
                source_index: old_value
                    .get(name.as_str())
                    .or_else(|| old_key.get(name.as_str()))
                    .copied()
                    .or_else(|| {
                        (!batch
                            .system_columns
                            .contains(SystemColumnKind::ChangeOperation))
                        .then_some(*index)
                    }),
            })
            .collect::<Vec<_>>();
        let old_key = projected_debezium(&batch.batch, old_key_projection, dialect)?;
        let mut user_ordinal_by_source_index = vec![None; batch.batch.num_columns()];
        for (ordinal, (index, _)) in user_columns.iter().enumerate() {
            user_ordinal_by_source_index[*index] = Some(ordinal);
        }
        let changed_columns = match dialect {
            DebeziumSourceDialect::Postgres => {
                optional_system_array::<BinaryArray>(batch, SystemColumnKind::ChangedColumns)?
            }
            DebeziumSourceDialect::MySql | DebeziumSourceDialect::Ydb => {
                Some(system_array::<BinaryArray>(
                    batch,
                    SystemColumnKind::ChangedColumns,
                )?)
            }
        };
        let operation = match dialect {
            DebeziumSourceDialect::Postgres => {
                optional_system_array::<StringArray>(batch, SystemColumnKind::ChangeOperation)?
            }
            DebeziumSourceDialect::MySql | DebeziumSourceDialect::Ydb => {
                Some(system_array::<StringArray>(
                    batch,
                    SystemColumnKind::ChangeOperation,
                )?)
            }
        };
        let source_metadata = match dialect {
            DebeziumSourceDialect::Postgres => DebeziumSourceMetadata::Postgres {
                transaction_id: role_array::<UInt64Array>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                )?,
                lsn: system_array::<Int64Array>(batch, SystemColumnKind::Offset)?,
                source_schema: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_SCHEMA)?,
                source_timestamp_us: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
                )?,
                source_timestamp_ns: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
                )?,
                event_timestamp_ms: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
                )?,
                event_timestamp_us: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_EVENT_TIMESTAMP_US,
                )?,
                event_timestamp_ns: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
                )?,
            },
            DebeziumSourceDialect::MySql => DebeziumSourceMetadata::MySql {
                transaction_identity: role_array::<BinaryArray>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                )?,
                server_id: role_array::<Int64Array>(batch, SYSTEM_ROLE_SOURCE_SERVER_ID)?,
                gtid: nullable_role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_GTID)?,
                binlog_file: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_BINLOG_FILE)?,
                binlog_position: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
                )?,
                binlog_row: role_array::<Int32Array>(batch, SYSTEM_ROLE_SOURCE_BINLOG_ROW)?,
                _source_schema: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_SCHEMA)?,
                source_timestamp_us: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
                )?,
                source_timestamp_ns: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
                )?,
                event_timestamp_ms: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
                )?,
                event_timestamp_us: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_EVENT_TIMESTAMP_US,
                )?,
                event_timestamp_ns: role_array::<Int64Array>(
                    batch,
                    SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
                )?,
            },
            DebeziumSourceDialect::Ydb => DebeziumSourceMetadata::Ydb {
                transaction_identity: role_array::<FixedSizeBinaryArray>(
                    batch,
                    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                )?,
                event_timestamp_ms: system_array::<Int64Array>(
                    batch,
                    SystemColumnKind::WriteTimestampMs,
                )?,
            },
        };
        Ok(Self {
            dialect,
            current,
            before,
            current_key,
            old_key,
            changed_columns,
            operation,
            database: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_DATABASE)?,
            source_table: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_TABLE)?,
            source_metadata,
            source_timestamp_ms: role_array::<Int64Array>(batch, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS)?,
            user_ordinal_by_source_index,
        })
    }

    fn operation(&self, row: usize) -> anyhow::Result<&str> {
        let Some(operation) = &self.operation else {
            return Ok("r");
        };
        anyhow::ensure!(!operation.is_null(row), "Debezium operation is null");
        let operation = operation.value(row);
        anyhow::ensure!(
            matches!(operation, "c" | "r" | "u" | "d"),
            "unsupported Debezium operation '{operation}'"
        );
        Ok(operation)
    }

    fn envelope(
        &self,
        row: usize,
        logical_name: &str,
        operation: &str,
        include_after: bool,
        include_before: bool,
        source_is_update: bool,
    ) -> anyhow::Result<Vec<u8>> {
        self.validate_source_metadata(row, operation)?;
        let changed_columns = if source_is_update && self.dialect == DebeziumSourceDialect::Postgres
        {
            Some(self.changed_columns.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Debezium updates require the changed-column mask")
            })?)
        } else {
            None
        };
        let mut output = Vec::with_capacity(512);
        output.extend_from_slice(b"{\"before\":");
        if include_before {
            self.before.write_object(row, &mut output)?;
        } else {
            output.extend_from_slice(b"null");
        }
        output.extend_from_slice(b",\"after\":");
        if include_after {
            self.current
                .write_object_with(row, &mut output, |source_index, _, output| {
                    let Some(ordinal) =
                        source_index.and_then(|index| self.user_ordinal_by_source_index[index])
                    else {
                        return false;
                    };
                    if changed_columns.is_some_and(|columns| {
                        columns
                            .value(row)
                            .get(ordinal / 8)
                            .is_some_and(|byte| byte & (1 << (ordinal % 8)) == 0)
                    }) {
                        output.extend_from_slice(UNAVAILABLE_VALUE);
                        return true;
                    }
                    false
                })?;
        } else {
            output.extend_from_slice(b"null");
        }
        output.extend_from_slice(b",\"source\":{\"version\":");
        write_json_string(
            &mut output,
            if self.dialect == DebeziumSourceDialect::Ydb {
                "1.0.0"
            } else {
                "transferia"
            },
        );
        output.extend_from_slice(b",\"connector\":");
        match &self.source_metadata {
            DebeziumSourceMetadata::Postgres {
                transaction_id,
                lsn,
                source_schema,
                source_timestamp_us,
                source_timestamp_ns,
                event_timestamp_ms: _,
                event_timestamp_us: _,
                event_timestamp_ns: _,
            } => {
                output.extend_from_slice(b"\"postgresql\",\"name\":");
                write_json_string(&mut output, logical_name);
                output.extend_from_slice(b",\"ts_ms\":");
                write_i64(&mut output, self.source_timestamp_ms.value(row));
                output.extend_from_slice(b",\"snapshot\":");
                write_json_string(&mut output, if operation == "r" { "true" } else { "false" });
                output.extend_from_slice(b",\"db\":");
                write_json_string(&mut output, self.database.value(row));
                output.extend_from_slice(b",\"sequence\":null,\"ts_us\":");
                write_i64(&mut output, source_timestamp_us.value(row));
                output.extend_from_slice(b",\"ts_ns\":");
                write_i64(&mut output, source_timestamp_ns.value(row));
                output.extend_from_slice(b",\"schema\":");
                write_json_string(&mut output, source_schema.value(row));
                output.extend_from_slice(b",\"table\":");
                write_json_string(&mut output, self.source_table.value(row));
                output.extend_from_slice(b",\"txId\":");
                write_u64(&mut output, transaction_id.value(row));
                output.extend_from_slice(b",\"lsn\":");
                write_i64(&mut output, lsn.value(row));
                output.extend_from_slice(b",\"xmin\":null}");
            }
            DebeziumSourceMetadata::MySql {
                transaction_identity: _,
                server_id,
                gtid,
                binlog_file,
                binlog_position,
                binlog_row,
                _source_schema: _,
                source_timestamp_us,
                source_timestamp_ns,
                event_timestamp_ms: _,
                event_timestamp_us: _,
                event_timestamp_ns: _,
            } => {
                output.extend_from_slice(b"\"mysql\",\"name\":");
                write_json_string(&mut output, logical_name);
                output.extend_from_slice(b",\"ts_ms\":");
                write_i64(&mut output, self.source_timestamp_ms.value(row));
                output.extend_from_slice(b",\"snapshot\":");
                write_json_string(&mut output, if operation == "r" { "true" } else { "false" });
                output.extend_from_slice(b",\"db\":");
                write_json_string(&mut output, self.database.value(row));
                output.extend_from_slice(b",\"sequence\":null,\"ts_us\":");
                write_i64(&mut output, source_timestamp_us.value(row));
                output.extend_from_slice(b",\"ts_ns\":");
                write_i64(&mut output, source_timestamp_ns.value(row));
                output.extend_from_slice(b",\"table\":");
                write_json_string(&mut output, self.source_table.value(row));
                output.extend_from_slice(b",\"server_id\":");
                write_i64(&mut output, server_id.value(row));
                output.extend_from_slice(b",\"gtid\":");
                if gtid.is_null(row) {
                    output.extend_from_slice(b"null");
                } else {
                    write_json_string(&mut output, gtid.value(row));
                }
                output.extend_from_slice(b",\"file\":");
                write_json_string(&mut output, binlog_file.value(row));
                output.extend_from_slice(b",\"pos\":");
                write_i64(&mut output, binlog_position.value(row));
                output.extend_from_slice(b",\"row\":");
                write_i64(&mut output, i64::from(binlog_row.value(row)));
                output.extend_from_slice(b",\"thread\":null,\"query\":null}");
            }
            DebeziumSourceMetadata::Ydb {
                transaction_identity,
                event_timestamp_ms: _,
            } => {
                let (step, tx_id) = ydb_transaction(transaction_identity.value(row), row)?;
                output.extend_from_slice(b"\"ydb\",\"name\":");
                write_json_string(&mut output, logical_name);
                output.extend_from_slice(b",\"ts_ms\":");
                write_i64(&mut output, self.source_timestamp_ms.value(row));
                output.extend_from_slice(b",\"snapshot\":\"false\",\"db\":");
                write_json_string(&mut output, self.database.value(row));
                output.extend_from_slice(b",\"table\":");
                write_json_string(&mut output, self.source_table.value(row));
                output.extend_from_slice(b",\"step\":");
                write_u64(&mut output, step);
                output.extend_from_slice(b",\"txId\":");
                write_u64(&mut output, tx_id);
                output.push(b'}');
            }
        }
        output.extend_from_slice(b",\"op\":");
        write_json_string(&mut output, operation);
        output.extend_from_slice(b",\"ts_ms\":");
        match &self.source_metadata {
            DebeziumSourceMetadata::Postgres {
                event_timestamp_ms,
                event_timestamp_us,
                event_timestamp_ns,
                ..
            }
            | DebeziumSourceMetadata::MySql {
                event_timestamp_ms,
                event_timestamp_us,
                event_timestamp_ns,
                ..
            } => {
                write_i64(&mut output, event_timestamp_ms.value(row));
                output.extend_from_slice(b",\"ts_us\":");
                write_i64(&mut output, event_timestamp_us.value(row));
                output.extend_from_slice(b",\"ts_ns\":");
                write_i64(&mut output, event_timestamp_ns.value(row));
            }
            DebeziumSourceMetadata::Ydb {
                event_timestamp_ms, ..
            } => write_i64(&mut output, event_timestamp_ms.value(row)),
        }
        output.extend_from_slice(b",\"transaction\":null}");
        Ok(output)
    }

    fn validate_source_metadata(&self, row: usize, operation: &str) -> anyhow::Result<()> {
        match &self.source_metadata {
            DebeziumSourceMetadata::Postgres { .. } => Ok(()),
            DebeziumSourceMetadata::MySql {
                transaction_identity,
                server_id,
                gtid,
                binlog_file,
                binlog_position,
                binlog_row,
                ..
            } => {
                anyhow::ensure!(
                    !transaction_identity.value(row).is_empty(),
                    "MySQL Debezium transaction identity is empty at row {row}"
                );
                let server_id = server_id.value(row);
                anyhow::ensure!(
                    u32::try_from(server_id).is_ok(),
                    "MySQL Debezium server_id {server_id} is outside the unsigned 32-bit range at row {row}"
                );
                anyhow::ensure!(
                    !binlog_file.value(row).is_empty(),
                    "MySQL Debezium binlog filename is empty at row {row}"
                );
                let position = binlog_position.value(row);
                anyhow::ensure!(
                    position >= 4 && u32::try_from(position).is_ok(),
                    "MySQL Debezium binlog position {position} is outside the supported 4..=4294967295 range at row {row}"
                );
                let binlog_row = binlog_row.value(row);
                anyhow::ensure!(
                    binlog_row >= 0,
                    "MySQL Debezium binlog row {binlog_row} is negative at row {row}"
                );
                if operation == "r" {
                    anyhow::ensure!(
                        server_id == 0 && gtid.is_null(row) && binlog_row == 0,
                        "MySQL Debezium snapshot row {row} requires server_id=0, gtid=null, and binlog row=0"
                    );
                } else {
                    anyhow::ensure!(
                        !gtid.is_null(row),
                        "MySQL Debezium stream row {row} requires a GTID"
                    );
                    validate_mysql_gtid(gtid.value(row), row)?;
                }
                Ok(())
            }
            DebeziumSourceMetadata::Ydb {
                transaction_identity,
                event_timestamp_ms,
            } => {
                anyhow::ensure!(
                    operation != "r",
                    "YDB Debezium does not accept snapshot operations because YDB replication is stream-only"
                );
                anyhow::ensure!(
                    !self.database.value(row).is_empty()
                        && !self.source_table.value(row).is_empty(),
                    "YDB Debezium source database and table must be nonempty at row {row}"
                );
                let (step, _) = ydb_transaction(transaction_identity.value(row), row)?;
                let source_timestamp = self.source_timestamp_ms.value(row);
                anyhow::ensure!(
                    source_timestamp >= 0 && u64::try_from(source_timestamp)? == step,
                    "YDB Debezium source timestamp {source_timestamp} does not equal transaction step {step} at row {row}"
                );
                anyhow::ensure!(
                    event_timestamp_ms.value(row) >= 0,
                    "YDB Debezium broker write timestamp is negative at row {row}"
                );
                Ok(())
            }
        }
    }
}

fn ydb_transaction(value: &[u8], row: usize) -> anyhow::Result<(u64, u64)> {
    let bytes: [u8; 16] = value.try_into().map_err(|_| {
        anyhow::anyhow!(
            "YDB Debezium transaction identity must contain exactly 16 bytes at row {row}"
        )
    })?;
    let step = u64::from_be_bytes(bytes[..8].try_into().map_err(|_| {
        anyhow::anyhow!("YDB Debezium transaction step framing is invalid at row {row}")
    })?);
    let tx_id = u64::from_be_bytes(bytes[8..].try_into().map_err(|_| {
        anyhow::anyhow!("YDB Debezium transaction id framing is invalid at row {row}")
    })?);
    Ok((step, tx_id))
}

fn validate_mysql_gtid(gtid: &str, row: usize) -> anyhow::Result<()> {
    let components = gtid.split(':').collect::<Vec<_>>();
    anyhow::ensure!(
        matches!(components.len(), 2 | 3),
        "MySQL Debezium GTID '{gtid}' has non-canonical framing at row {row}"
    );
    let sid = components[0].as_bytes();
    anyhow::ensure!(
        sid.len() == 36
            && sid.iter().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    *byte == b'-'
                } else {
                    byte.is_ascii_digit() || matches!(*byte, b'a'..=b'f')
                }
            }),
        "MySQL Debezium GTID '{gtid}' has a non-canonical SID at row {row}"
    );
    if components.len() == 3 {
        let tag = components[1];
        anyhow::ensure!(
            (1..=32).contains(&tag.len())
                && tag
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "MySQL Debezium GTID '{gtid}' has a non-canonical tag at row {row}"
        );
    }
    let gno = components[components.len() - 1];
    let parsed = gno.parse::<u64>().map_err(|error| {
        anyhow::anyhow!("MySQL Debezium GTID '{gtid}' has an invalid GNO at row {row}: {error}")
    })?;
    anyhow::ensure!(
        parsed > 0 && parsed.to_string() == gno,
        "MySQL Debezium GTID '{gtid}' has a non-canonical GNO at row {row}"
    );
    Ok(())
}

fn projected_debezium(
    batch: &arrow::record_batch::RecordBatch,
    projection: Vec<JsonColumnProjection>,
    dialect: DebeziumSourceDialect,
) -> anyhow::Result<JsonBatchEncoder> {
    match dialect {
        DebeziumSourceDialect::Postgres => JsonBatchEncoder::projected_debezium(batch, projection),
        DebeziumSourceDialect::MySql => {
            JsonBatchEncoder::projected_debezium_mysql(batch, projection)
        }
        DebeziumSourceDialect::Ydb => JsonBatchEncoder::projected_debezium_ydb(batch, projection),
    }
}

fn mapped_columns(batch: &SinkBatch, metadata_key: &str) -> anyhow::Result<HashMap<String, usize>> {
    batch
        .batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            field
                .metadata()
                .get(metadata_key)
                .map(|current| (current.clone(), index))
        })
        .try_fold(HashMap::new(), |mut columns, (current, index)| {
            let previous = columns.insert(current.clone(), index);
            anyhow::ensure!(
                previous.is_none(),
                "Debezium input has duplicate {metadata_key} mapping for '{current}'"
            );
            Ok(columns)
        })
}

fn system_array<T>(batch: &SinkBatch, kind: SystemColumnKind) -> anyhow::Result<T>
where
    T: arrow::array::Array + Clone + 'static,
{
    let column = batch
        .system_columns
        .get(kind)
        .ok_or_else(|| anyhow::anyhow!("Debezium input is missing {kind:?}"))?;
    let array = batch
        .batch
        .column(column.index)
        .as_any()
        .downcast_ref::<T>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Debezium {kind:?} column has the wrong Arrow type"))?;
    anyhow::ensure!(
        array.null_count() == 0,
        "Debezium {kind:?} column contains null values"
    );
    Ok(array)
}

fn optional_system_array<T>(batch: &SinkBatch, kind: SystemColumnKind) -> anyhow::Result<Option<T>>
where
    T: arrow::array::Array + Clone + 'static,
{
    batch
        .system_columns
        .contains(kind)
        .then(|| system_array(batch, kind))
        .transpose()
}

fn role_array<T>(batch: &SinkBatch, role: &str) -> anyhow::Result<T>
where
    T: arrow::array::Array + Clone + 'static,
{
    let schema = batch.batch.schema();
    let mut matches = schema.fields().iter().enumerate().filter(|(_, field)| {
        field.metadata().get(META_SYSTEM_ROLE).map(String::as_str) == Some(role)
    });
    let (index, field) = matches
        .next()
        .ok_or_else(|| anyhow::anyhow!("Debezium input is missing system role '{role}'"))?;
    anyhow::ensure!(
        matches.next().is_none(),
        "Debezium input has duplicate system role '{role}'"
    );
    let array = batch
        .batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Debezium system role '{role}' column '{}' has the wrong Arrow type",
                field.name()
            )
        })?;
    anyhow::ensure!(
        array.null_count() == 0,
        "Debezium system role '{role}' column contains null values"
    );
    Ok(array)
}

fn nullable_role_array<T>(batch: &SinkBatch, role: &str) -> anyhow::Result<T>
where
    T: arrow::array::Array + Clone + 'static,
{
    let schema = batch.batch.schema();
    let mut matches = schema.fields().iter().enumerate().filter(|(_, field)| {
        field.metadata().get(META_SYSTEM_ROLE).map(String::as_str) == Some(role)
    });
    let (index, field) = matches
        .next()
        .ok_or_else(|| anyhow::anyhow!("Debezium input is missing system role '{role}'"))?;
    anyhow::ensure!(
        matches.next().is_none(),
        "Debezium input has duplicate system role '{role}'"
    );
    batch
        .batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Debezium system role '{role}' column '{}' has the wrong Arrow type",
                field.name()
            )
        })
}

fn validate_message_size(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    message_size_limit: usize,
) -> anyhow::Result<()> {
    let bytes = key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len);
    anyhow::ensure!(
        bytes <= message_size_limit,
        "serialized queue message exceeds configured transport limit: message_bytes={bytes}, transport_limit_bytes={message_size_limit}"
    );
    Ok(())
}

fn write_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
}

fn write_json_string(output: &mut Vec<u8>, value: &str) {
    output.push(b'"');
    for byte in value.bytes() {
        match byte {
            b'"' => output.extend_from_slice(b"\\\""),
            b'\\' => output.extend_from_slice(b"\\\\"),
            b'\n' => output.extend_from_slice(b"\\n"),
            b'\r' => output.extend_from_slice(b"\\r"),
            b'\t' => output.extend_from_slice(b"\\t"),
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
}

#[cfg(test)]
#[path = "tests/debezium.rs"]
mod tests;
