use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use transferia_core::ChangeOperation;

use super::event::{ChangeEvent, LogicalValue};

#[derive(Deserialize)]
struct ChangeSet {
    xid: u32,
    nextlsn: String,
    timestamp: String,
    change: Vec<Wal2JsonChange>,
}

#[derive(Deserialize)]
struct Wal2JsonChange {
    kind: String,
    schema: String,
    table: String,
    columnnames: Option<Vec<String>>,
    columntypeoids: Option<Vec<u32>>,
    columnvalues: Option<Vec<Value>>,
    oldkeys: Option<OldKeys>,
}

#[derive(Deserialize)]
struct OldKeys {
    keynames: Vec<String>,
    keytypeoids: Vec<u32>,
    keyvalues: Vec<Value>,
}

pub(super) struct Wal2JsonEvent {
    pub event: ChangeEvent,
    pub column_names: Vec<String>,
    pub column_type_oids: Vec<u32>,
    pub old_key_names: Vec<String>,
    pub old_key_type_oids: Vec<u32>,
}

pub(super) struct Wal2JsonTransaction {
    pub events: Vec<Wal2JsonEvent>,
    pub end_lsn: u64,
}

pub(super) fn decode(data: &[u8]) -> anyhow::Result<Wal2JsonTransaction> {
    let transaction: ChangeSet = serde_json::from_slice(data)?;
    let end_lsn = super::reader::parse_lsn(&transaction.nextlsn)?;
    let commit_timestamp_micros = chrono::DateTime::parse_from_str(
        &transaction.timestamp,
        "%Y-%m-%d %H:%M:%S%.f%#z",
    )?
    .timestamp_micros();
    let events = transaction
        .change
        .into_iter()
        .map(|change| {
            let operation = match change.kind.as_str() {
                "insert" => ChangeOperation::Create,
                "update" => ChangeOperation::Update,
                "delete" => ChangeOperation::Delete,
                other => anyhow::bail!("unsupported wal2json change kind '{other}'"),
            };
            let column_names = change.columnnames.unwrap_or_default();
            let column_type_oids = change.columntypeoids.unwrap_or_default();
            let values = change
                .columnvalues
                .unwrap_or_default()
                .into_iter()
                .map(json_value)
                .collect::<anyhow::Result<Vec<_>>>()?;
            anyhow::ensure!(
                column_names.len() == values.len() && column_names.len() == column_type_oids.len(),
                "wal2json column name/type/value count mismatch"
            );
            let (old_key_names, old_key_type_oids, old_values) = match change.oldkeys {
                Some(old) => {
                    anyhow::ensure!(
                        old.keynames.len() == old.keyvalues.len()
                            && old.keynames.len() == old.keytypeoids.len(),
                        "wal2json old-key name/type/value count mismatch"
                    );
                    (
                        old.keynames,
                        old.keytypeoids,
                        Some(
                            old.keyvalues
                                .into_iter()
                                .map(json_value)
                                .collect::<anyhow::Result<Vec<_>>>()?,
                        ),
                    )
                }
                None => (Vec::new(), Vec::new(), None),
            };
            Ok(Wal2JsonEvent {
                event: ChangeEvent {
                    schema: Arc::from(change.schema),
                    table: Arc::from(change.table),
                    operation,
                    values,
                    old_values,
                    old_values_kind: None,
                    lsn: end_lsn,
                    transaction_id: transaction.xid,
                    commit_timestamp_micros,
                },
                column_names,
                column_type_oids,
                old_key_names,
                old_key_type_oids,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Wal2JsonTransaction { events, end_lsn })
}

fn json_value(value: Value) -> anyhow::Result<LogicalValue> {
    Ok(match value {
        Value::Null => LogicalValue::Null,
        Value::String(value) => LogicalValue::Text(Bytes::from(value)),
        Value::Bool(value) => LogicalValue::Text(Bytes::from(value.to_string())),
        Value::Number(value) => LogicalValue::Text(Bytes::from(value.to_string())),
        Value::Array(_) | Value::Object(_) => {
            LogicalValue::Text(Bytes::from(serde_json::to_vec(&value)?))
        }
    })
}
