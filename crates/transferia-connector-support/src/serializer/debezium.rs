use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array as _, BinaryArray, Int64Array, StringArray, UInt64Array};
use transferia_core::data::schema::{
    META_OLD_KEY_OF, META_OLD_VALUE_OF, META_PRIMARY_KEY, META_SYSTEM_ROLE,
    SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA,
    SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
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

pub(super) struct DebeziumJsonEncoder {
    logical_name: String,
    mode: QueueMessageMode,
}

impl DebeziumJsonEncoder {
    pub(super) fn new(logical_name: String, mode: QueueMessageMode) -> Self {
        Self {
            logical_name,
            mode,
        }
    }

    pub(super) fn encode_batch(
        &self,
        batch: &SinkBatch,
        message_size_limit: usize,
    ) -> anyhow::Result<SerializedBatch> {
        let encoder = DebeziumBatchEncoder::new(batch)?;
        let mut messages = Vec::with_capacity(batch.rows());
        for row in 0..batch.rows() {
            let operation = encoder.operation(row)?;
            if operation == "u" && !encoder.old_key.row_equals(&encoder.current_key, row) {
                let old_key = self.key(&encoder.old_key, row);
                let delete = encoder.envelope(row, &self.logical_name, "d", false, true, true);
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
                let key = self.key(&encoder.current_key, row);
                let create = encoder.envelope(row, &self.logical_name, "c", true, false, true);
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
            let key = self.key(key_encoder, row);
            let value = encoder.envelope(
                row,
                &self.logical_name,
                operation,
                operation != "d",
                operation != "c" && operation != "r",
                operation == "u",
            );
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

    fn key(&self, encoder: &JsonBatchEncoder, row: usize) -> Option<Vec<u8>> {
        if self.mode == QueueMessageMode::ValuesOnly {
            return None;
        }
        let mut key = Vec::new();
        encoder.write_object(row, &mut key);
        Some(key)
    }
}

struct DebeziumBatchEncoder {
    current: JsonBatchEncoder,
    before: JsonBatchEncoder,
    current_key: JsonBatchEncoder,
    old_key: JsonBatchEncoder,
    changed_columns: BinaryArray,
    operation: StringArray,
    lsn: Int64Array,
    database: StringArray,
    source_schema: StringArray,
    source_table: StringArray,
    transaction_id: UInt64Array,
    source_timestamp_ms: Int64Array,
    source_timestamp_us: Int64Array,
    source_timestamp_ns: Int64Array,
    event_timestamp_ms: Int64Array,
    event_timestamp_us: Int64Array,
    event_timestamp_ns: Int64Array,
    user_ordinal_by_source_index: Vec<Option<usize>>,
}

impl DebeziumBatchEncoder {
    fn new(batch: &SinkBatch) -> anyhow::Result<Self> {
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
        let current = JsonBatchEncoder::projected_debezium(
            &batch.batch,
            user_columns
                .iter()
                .map(|(index, name)| JsonColumnProjection {
                    output_name: name.clone(),
                    source_index: Some(*index),
                }),
        )?;
        let before = JsonBatchEncoder::projected_debezium(
            &batch.batch,
            user_columns.iter().map(|(_, name)| JsonColumnProjection {
                output_name: name.clone(),
                source_index: old_value
                    .get(name)
                    .or_else(|| old_key.get(name))
                    .copied(),
            }),
        )?;
        let primary_keys = user_columns
            .iter()
            .filter(|(index, _)| {
                schema.field(*index).metadata().get(META_PRIMARY_KEY).map(String::as_str)
                    == Some("true")
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !primary_keys.is_empty(),
            "Debezium serializer requires at least one primary-key column"
        );
        let current_key = JsonBatchEncoder::projected_debezium(
            &batch.batch,
            primary_keys
                .iter()
                .map(|(index, name)| JsonColumnProjection {
                    output_name: (*name).clone(),
                    source_index: Some(*index),
                }),
        )?;
        let old_key = JsonBatchEncoder::projected_debezium(
            &batch.batch,
            primary_keys.iter().map(|(_, name)| JsonColumnProjection {
                output_name: (*name).clone(),
                source_index: old_value
                    .get(name.as_str())
                    .or_else(|| old_key.get(name.as_str()))
                    .copied(),
            }),
        )?;
        let mut user_ordinal_by_source_index = vec![None; batch.batch.num_columns()];
        for (ordinal, (index, _)) in user_columns.iter().enumerate() {
            user_ordinal_by_source_index[*index] = Some(ordinal);
        }
        Ok(Self {
            current,
            before,
            current_key,
            old_key,
            changed_columns: system_array::<BinaryArray>(batch, SystemColumnKind::ChangedColumns)?,
            operation: system_array::<StringArray>(batch, SystemColumnKind::ChangeOperation)?,
            lsn: system_array::<Int64Array>(batch, SystemColumnKind::Offset)?,
            database: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_DATABASE)?,
            source_schema: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_SCHEMA)?,
            source_table: role_array::<StringArray>(batch, SYSTEM_ROLE_SOURCE_TABLE)?,
            transaction_id: role_array::<UInt64Array>(
                batch,
                SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
            )?,
            source_timestamp_ms: role_array::<Int64Array>(
                batch,
                SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            )?,
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
            user_ordinal_by_source_index,
        })
    }

    fn operation(&self, row: usize) -> anyhow::Result<&str> {
        anyhow::ensure!(!self.operation.is_null(row), "Debezium operation is null");
        let operation = self.operation.value(row);
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
    ) -> Vec<u8> {
        let mut output = Vec::with_capacity(512);
        output.extend_from_slice(b"{\"before\":");
        if include_before {
            self.before.write_object(row, &mut output);
        } else {
            output.extend_from_slice(b"null");
        }
        output.extend_from_slice(b",\"after\":");
        if include_after {
            self.current.write_object_with(
                row,
                &mut output,
                |source_index, _, output| {
                    let Some(ordinal) = source_index
                        .and_then(|index| self.user_ordinal_by_source_index[index])
                    else {
                        return false;
                    };
                    let changed = self.changed_columns.value(row);
                    if source_is_update
                        && changed
                            .get(ordinal / 8)
                            .is_some_and(|byte| byte & (1 << (ordinal % 8)) == 0)
                    {
                        output.extend_from_slice(UNAVAILABLE_VALUE);
                        return true;
                    }
                    false
                },
            );
        } else {
            output.extend_from_slice(b"null");
        }
        output.extend_from_slice(b",\"source\":{\"version\":\"transferia\",\"connector\":\"postgresql\",\"name\":");
        write_json_string(&mut output, logical_name);
        output.extend_from_slice(b",\"ts_ms\":");
        write_i64(&mut output, self.source_timestamp_ms.value(row));
        output.extend_from_slice(b",\"snapshot\":\"false\",\"db\":");
        write_json_string(&mut output, self.database.value(row));
        output.extend_from_slice(b",\"sequence\":null,\"ts_us\":");
        write_i64(&mut output, self.source_timestamp_us.value(row));
        output.extend_from_slice(b",\"ts_ns\":");
        write_i64(&mut output, self.source_timestamp_ns.value(row));
        output.extend_from_slice(b",\"schema\":");
        write_json_string(&mut output, self.source_schema.value(row));
        output.extend_from_slice(b",\"table\":");
        write_json_string(&mut output, self.source_table.value(row));
        output.extend_from_slice(b",\"txId\":");
        write_u64(&mut output, self.transaction_id.value(row));
        output.extend_from_slice(b",\"lsn\":");
        write_i64(&mut output, self.lsn.value(row));
        output.extend_from_slice(b",\"xmin\":null},\"op\":");
        write_json_string(&mut output, operation);
        output.extend_from_slice(b",\"ts_ms\":");
        write_i64(&mut output, self.event_timestamp_ms.value(row));
        output.extend_from_slice(b",\"ts_us\":");
        write_i64(&mut output, self.event_timestamp_us.value(row));
        output.extend_from_slice(b",\"ts_ns\":");
        write_i64(&mut output, self.event_timestamp_ns.value(row));
        output.extend_from_slice(b",\"transaction\":null}");
        output
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
            anyhow::ensure!(
                columns.insert(current.clone(), index).is_none(),
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

fn role_array<T>(batch: &SinkBatch, role: &str) -> anyhow::Result<T>
where
    T: arrow::array::Array + Clone + 'static,
{
    let schema = batch.batch.schema();
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
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
