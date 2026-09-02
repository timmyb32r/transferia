use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use transferia_core::data::schema::{
    SchemaColumn, META_CHANGE_OPERATION, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_DATABASE,
    SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::SinkBatch;

use super::*;

#[tokio::test]
async fn kafka_emits_debezium_keys_tombstones_and_primary_key_change_sequence() {
    let batch = cdc_batch().await;
    let encoder = DebeziumJsonEncoder::new(
        "inventory".to_owned(),
        QueueMessageMode::KeyedWithTombstones,
    );
    let encoded = encoder.encode_batch(&batch, usize::MAX).unwrap();

    assert_eq!(encoded.messages.len(), 7);
    assert_eq!(json(encoded.messages[0].key.as_deref().unwrap())["id"], 1);
    let create = json(encoded.messages[0].value.as_deref().unwrap());
    assert!(create["before"].is_null());
    assert_eq!(create["after"]["payload"], "alpha");
    assert_eq!(create["source"]["version"], "transferia");
    assert_eq!(create["source"]["connector"], "postgresql");
    assert_eq!(create["source"]["name"], "inventory");
    assert_eq!(create["source"]["db"], "postgres");
    assert_eq!(create["source"]["schema"], "public");
    assert_eq!(create["source"]["table"], "accounts");
    assert_eq!(create["source"]["txId"], 7);
    assert_eq!(create["source"]["lsn"], 100);
    assert_eq!(create["op"], "c");

    let update = json(encoded.messages[1].value.as_deref().unwrap());
    assert_eq!(update["before"]["id"], 1);
    assert!(update["before"]["payload"].is_null());
    assert_eq!(update["after"]["payload"], "__debezium_unavailable_value");
    assert_eq!(update["op"], "u");

    assert_eq!(json(encoded.messages[2].key.as_deref().unwrap())["id"], 1);
    assert_eq!(
        json(encoded.messages[2].value.as_deref().unwrap())["op"],
        "d"
    );
    assert_eq!(json(encoded.messages[3].key.as_deref().unwrap())["id"], 1);
    assert!(encoded.messages[3].value.is_none());
    assert_eq!(json(encoded.messages[4].key.as_deref().unwrap())["id"], 2);
    let primary_key_create = json(encoded.messages[4].value.as_deref().unwrap());
    assert_eq!(primary_key_create["op"], "c");
    assert_eq!(
        primary_key_create["after"]["payload"],
        "__debezium_unavailable_value"
    );

    assert_eq!(json(encoded.messages[5].key.as_deref().unwrap())["id"], 2);
    let delete = json(encoded.messages[5].value.as_deref().unwrap());
    assert_eq!(delete["before"]["id"], 2);
    assert!(delete["after"].is_null());
    assert_eq!(delete["op"], "d");
    assert!(encoded.messages[6].value.is_none());
}

#[tokio::test]
async fn logbroker_omits_keys_and_tombstones_without_losing_pk_change_events() {
    let batch = cdc_batch().await;
    let encoder = DebeziumJsonEncoder::new("inventory".to_owned(), QueueMessageMode::ValuesOnly);
    let encoded = encoder.encode_batch(&batch, usize::MAX).unwrap();

    assert_eq!(encoded.messages.len(), 5);
    assert!(encoded
        .messages
        .iter()
        .all(|message| message.key.is_none() && message.value.is_some()));
    assert_eq!(
        encoded
            .messages
            .iter()
            .map(|message| json(message.value.as_deref().unwrap())["op"]
                .as_str()
                .unwrap()
                .to_owned())
            .collect::<Vec<_>>(),
        ["c", "u", "d", "c", "d"]
    );
}

#[tokio::test]
async fn append_only_rows_are_byte_exact_debezium_snapshot_events() {
    let batch = snapshot_batch().await;
    let encoder = DebeziumJsonEncoder::new(
        "inventory".to_owned(),
        QueueMessageMode::KeyedWithTombstones,
    );

    let encoded = encoder.encode_batch(&batch, usize::MAX).unwrap();

    assert_eq!(encoded.messages.len(), 1);
    assert_eq!(
        encoded.messages[0].key.as_deref(),
        Some(br#"{"id":1}"#.as_slice())
    );
    assert_eq!(
        encoded.messages[0].value.as_deref(),
        Some(
            br#"{"before":null,"after":{"id":1,"payload":"alpha"},"source":{"version":"transferia","connector":"postgresql","name":"inventory","ts_ms":1000,"snapshot":"true","db":"postgres","sequence":null,"ts_us":1000000,"ts_ns":1000000000,"schema":"public","table":"accounts","txId":7,"lsn":100,"xmin":null},"op":"r","ts_ms":2000,"ts_us":2000000,"ts_ns":2000000000,"transaction":null}"#
                .as_slice(),
        ),
    );
}

#[tokio::test]
async fn transport_limit_is_checked_after_debezium_envelope_expansion() {
    let batch = cdc_batch().await;
    let encoder = DebeziumJsonEncoder::new(
        "inventory".to_owned(),
        QueueMessageMode::KeyedWithTombstones,
    );
    let error = encoder.encode_batch(&batch, 32).unwrap_err().to_string();
    assert!(error.contains("transport limit"), "{error}");
}

async fn cdc_batch() -> SinkBatch {
    let user_id = SchemaColumn::new("id".to_owned(), DataType::Int64, true)
        .with_constraints(true, false, None);
    let user_payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, true);
    let old_key = SchemaColumn::new("_system_old_key_0".to_owned(), DataType::Int64, true)
        .with_old_key_of("id".to_owned());
    let roles = [
        (
            "_system_source_database",
            SYSTEM_ROLE_SOURCE_DATABASE,
            DataType::Utf8,
        ),
        (
            "_system_source_schema",
            SYSTEM_ROLE_SOURCE_SCHEMA,
            DataType::Utf8,
        ),
        (
            "_system_source_table",
            SYSTEM_ROLE_SOURCE_TABLE,
            DataType::Utf8,
        ),
        (
            "_system_source_transaction_id",
            SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
            DataType::UInt64,
        ),
        (
            "_system_source_timestamp_ms",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            DataType::Int64,
        ),
        (
            "_system_source_timestamp_us",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
            DataType::Int64,
        ),
        (
            "_system_source_timestamp_ns",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_ms",
            SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_us",
            SYSTEM_ROLE_EVENT_TIMESTAMP_US,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_ns",
            SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
            DataType::Int64,
        ),
    ];
    let mut fields = vec![
        Field::new("id", DataType::Int64, true).with_metadata(user_id.arrow_metadata()),
        Field::new("payload", DataType::Utf8, true).with_metadata(user_payload.arrow_metadata()),
        Field::new("_system_old_key_0", DataType::Int64, true)
            .with_metadata(old_key.arrow_metadata()),
    ];
    fields.extend(roles.into_iter().map(|(name, role, data_type)| {
        Field::new(name, data_type.clone(), false).with_metadata(
            SchemaColumn::new(name.to_owned(), data_type, false)
                .with_system_role(role)
                .arrow_metadata(),
        )
    }));
    fields.push(Field::new(
        SystemColumnKind::Offset.default_name(),
        DataType::Int64,
        false,
    ));
    fields.push(
        Field::new(
            SystemColumnKind::ChangeOperation.default_name(),
            DataType::Utf8,
            false,
        )
        .with_metadata(std::collections::HashMap::from([(
            META_CHANGE_OPERATION.to_owned(),
            "true".to_owned(),
        )])),
    );
    fields.push(Field::new(
        SystemColumnKind::ChangedColumns.default_name(),
        DataType::Binary,
        false,
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec![Some("alpha"), None, None, None])),
            Arc::new(Int64Array::from(vec![None, Some(1), Some(1), Some(2)])),
            Arc::new(StringArray::from(vec!["postgres"; 4])),
            Arc::new(StringArray::from(vec!["public"; 4])),
            Arc::new(StringArray::from(vec!["accounts"; 4])),
            Arc::new(UInt64Array::from(vec![7_u64; 4])),
            Arc::new(Int64Array::from(vec![1_000_i64; 4])),
            Arc::new(Int64Array::from(vec![1_000_000_i64; 4])),
            Arc::new(Int64Array::from(vec![1_000_000_000_i64; 4])),
            Arc::new(Int64Array::from(vec![2_000_i64; 4])),
            Arc::new(Int64Array::from(vec![2_000_000_i64; 4])),
            Arc::new(Int64Array::from(vec![2_000_000_000_i64; 4])),
            Arc::new(Int64Array::from(vec![100_i64, 101, 102, 103])),
            Arc::new(StringArray::from(vec!["c", "u", "u", "d"])),
            Arc::new(BinaryArray::from_iter_values([
                &[0b11_u8][..],
                &[0b01_u8][..],
                &[0b01_u8][..],
                &[0b01_u8][..],
            ])),
        ],
    )
    .unwrap();
    SinkBatch {
        table: Arc::from("accounts"),
        is_dlq: false,
        byte_size: batch.get_array_memory_size(),
        batch,
        memory: PipelineMemory::new(1024 * 1024).reserve(1).await,
        system_columns: SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::Offset,
                index: 13,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                index: 14,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangedColumns,
                index: 15,
                name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
            },
        ]),
    }
}

async fn snapshot_batch() -> SinkBatch {
    let user_id = SchemaColumn::new("id".to_owned(), DataType::Int64, true)
        .with_constraints(true, false, None);
    let user_payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, true);
    let roles = [
        (
            "_system_source_database",
            SYSTEM_ROLE_SOURCE_DATABASE,
            DataType::Utf8,
        ),
        (
            "_system_source_schema",
            SYSTEM_ROLE_SOURCE_SCHEMA,
            DataType::Utf8,
        ),
        (
            "_system_source_table",
            SYSTEM_ROLE_SOURCE_TABLE,
            DataType::Utf8,
        ),
        (
            "_system_source_transaction_id",
            SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
            DataType::UInt64,
        ),
        (
            "_system_source_timestamp_ms",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            DataType::Int64,
        ),
        (
            "_system_source_timestamp_us",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
            DataType::Int64,
        ),
        (
            "_system_source_timestamp_ns",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_ms",
            SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_us",
            SYSTEM_ROLE_EVENT_TIMESTAMP_US,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_ns",
            SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
            DataType::Int64,
        ),
    ];
    let mut fields = vec![
        Field::new("id", DataType::Int64, true).with_metadata(user_id.arrow_metadata()),
        Field::new("payload", DataType::Utf8, true).with_metadata(user_payload.arrow_metadata()),
    ];
    fields.extend(roles.into_iter().map(|(name, role, data_type)| {
        Field::new(name, data_type.clone(), false).with_metadata(
            SchemaColumn::new(name.to_owned(), data_type, false)
                .with_system_role(role)
                .arrow_metadata(),
        )
    }));
    fields.push(Field::new(
        SystemColumnKind::Offset.default_name(),
        DataType::Int64,
        false,
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(StringArray::from(vec!["alpha"])),
            Arc::new(StringArray::from(vec!["postgres"])),
            Arc::new(StringArray::from(vec!["public"])),
            Arc::new(StringArray::from(vec!["accounts"])),
            Arc::new(UInt64Array::from(vec![7_u64])),
            Arc::new(Int64Array::from(vec![1_000_i64])),
            Arc::new(Int64Array::from(vec![1_000_000_i64])),
            Arc::new(Int64Array::from(vec![1_000_000_000_i64])),
            Arc::new(Int64Array::from(vec![2_000_i64])),
            Arc::new(Int64Array::from(vec![2_000_000_i64])),
            Arc::new(Int64Array::from(vec![2_000_000_000_i64])),
            Arc::new(Int64Array::from(vec![100_i64])),
        ],
    )
    .unwrap();
    SinkBatch {
        table: Arc::from("accounts"),
        is_dlq: false,
        byte_size: batch.get_array_memory_size(),
        batch,
        memory: PipelineMemory::new(1024 * 1024).reserve(1).await,
        system_columns: SystemColumns::new(vec![SystemColumn {
            kind: SystemColumnKind::Offset,
            index: 12,
            name: Arc::from(SystemColumnKind::Offset.default_name()),
        }]),
    }
}

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}
