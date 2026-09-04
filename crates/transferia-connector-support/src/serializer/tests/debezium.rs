use std::sync::Arc;

use arrow::array::{BinaryArray, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use transferia_core::data::schema::{
    SchemaColumn, META_ARROW_EXTENSION_METADATA, META_CHANGE_OPERATION, META_OLD_VALUE_OF,
    SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_BINLOG_FILE, SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
    SYSTEM_ROLE_SOURCE_BINLOG_ROW, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_GTID,
    SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_SERVER_ID, SYSTEM_ROLE_SOURCE_TABLE,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
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
    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::Postgres, usize::MAX)
        .unwrap();

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
    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::Postgres, usize::MAX)
        .unwrap();

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

    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::Postgres, usize::MAX)
        .unwrap();

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
    let error = encoder
        .encode_batch(&batch, DebeziumSourceDialect::Postgres, 32)
        .unwrap_err()
        .to_string();
    assert!(error.contains("transport limit"), "{error}");
}

#[tokio::test]
async fn mysql_emits_exact_source_metadata_full_updates_and_pk_change_sequence() {
    let batch = mysql_cdc_batch().await;
    let encoder = DebeziumJsonEncoder::new(
        "inventory".to_owned(),
        QueueMessageMode::KeyedWithTombstones,
    );

    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::MySql, usize::MAX)
        .unwrap();

    assert_eq!(encoded.messages.len(), 8);
    assert_eq!(
        encoded.messages[0].value.as_deref(),
        Some(
            br#"{"before":null,"after":{"id":1,"payload":"snapshot"},"source":{"version":"transferia","connector":"mysql","name":"inventory","ts_ms":1000,"snapshot":"true","db":"inventory","sequence":null,"ts_us":1000000,"ts_ns":1000000000,"table":"accounts","server_id":0,"gtid":null,"file":"mysql-bin.000001","pos":4,"row":0,"thread":null,"query":null},"op":"r","ts_ms":2000,"ts_us":2000000,"ts_ns":2000000000,"transaction":null}"#
                .as_slice(),
        )
    );

    let create = json(encoded.messages[1].value.as_deref().unwrap());
    assert_eq!(create["op"], "c");
    assert_eq!(create["source"]["connector"], "mysql");
    assert_eq!(create["source"]["server_id"], 4_294_967_295_u64);
    assert_eq!(
        create["source"]["gtid"],
        "11111111-1111-1111-1111-111111111111:blue:2"
    );
    for postgres_field in ["schema", "txId", "lsn", "xmin"] {
        assert!(
            create["source"].get(postgres_field).is_none(),
            "unexpected PostgreSQL field {postgres_field}"
        );
    }

    let update = json(encoded.messages[2].value.as_deref().unwrap());
    assert_eq!(update["op"], "u");
    assert_eq!(update["before"]["payload"], "created");
    assert_eq!(update["after"]["payload"], "updated");
    assert_ne!(
        update["after"]["payload"],
        "__debezium_unavailable_value"
    );

    assert_eq!(json(encoded.messages[3].key.as_deref().unwrap())["id"], 2);
    assert_eq!(json(encoded.messages[3].value.as_deref().unwrap())["op"], "d");
    assert_eq!(json(encoded.messages[4].key.as_deref().unwrap())["id"], 2);
    assert!(encoded.messages[4].value.is_none());
    assert_eq!(json(encoded.messages[5].key.as_deref().unwrap())["id"], 3);
    let primary_key_create = json(encoded.messages[5].value.as_deref().unwrap());
    assert_eq!(primary_key_create["op"], "c");
    assert_eq!(primary_key_create["after"]["payload"], "renamed");

    assert_eq!(json(encoded.messages[6].key.as_deref().unwrap())["id"], 3);
    assert_eq!(json(encoded.messages[6].value.as_deref().unwrap())["op"], "d");
    assert!(encoded.messages[7].value.is_none());
}

#[tokio::test]
async fn mysql_values_only_retains_all_logical_events_without_tombstones() {
    let batch = mysql_cdc_batch().await;
    let encoder = DebeziumJsonEncoder::new("inventory".to_owned(), QueueMessageMode::ValuesOnly);

    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::MySql, usize::MAX)
        .unwrap();

    assert_eq!(encoded.messages.len(), 6);
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
        ["r", "c", "u", "d", "c", "d"]
    );
}

#[tokio::test]
async fn mysql_rejects_invalid_runtime_source_metadata_before_returning_messages() {
    let encoder = DebeziumJsonEncoder::new("inventory".to_owned(), QueueMessageMode::ValuesOnly);

    let mut empty_identity = mysql_cdc_batch().await;
    replace_column(
        &mut empty_identity,
        7,
        Arc::new(BinaryArray::from_iter_values([
            &b""[..],
            &b"tx-1"[..],
            &b"tx-2"[..],
            &b"tx-3"[..],
            &b"tx-4"[..],
        ])),
    );
    let error = encoder
        .encode_batch(
            &empty_identity,
            DebeziumSourceDialect::MySql,
            usize::MAX,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("transaction identity is empty"), "{error}");

    let mut invalid_server = mysql_cdc_batch().await;
    replace_column(
        &mut invalid_server,
        14,
        Arc::new(Int64Array::from(vec![0_i64, -1, 11, 11, 11])),
    );
    let error = encoder
        .encode_batch(
            &invalid_server,
            DebeziumSourceDialect::MySql,
            usize::MAX,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("unsigned 32-bit range"), "{error}");

    let mut invalid_position = mysql_cdc_batch().await;
    replace_column(
        &mut invalid_position,
        17,
        Arc::new(Int64Array::from(vec![3_i64, 100, 120, 140, 160])),
    );
    let error = encoder
        .encode_batch(
            &invalid_position,
            DebeziumSourceDialect::MySql,
            usize::MAX,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("4..=4294967295"), "{error}");

    let mut invalid_row = mysql_cdc_batch().await;
    replace_column(
        &mut invalid_row,
        18,
        Arc::new(Int32Array::from(vec![0_i32, -1, 1, 2, 3])),
    );
    let error = encoder
        .encode_batch(&invalid_row, DebeziumSourceDialect::MySql, usize::MAX)
        .unwrap_err()
        .to_string();
    assert!(error.contains("binlog row -1 is negative"), "{error}");

    let mut invalid_gtid = mysql_cdc_batch().await;
    replace_column(
        &mut invalid_gtid,
        15,
        Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("11111111-1111-1111-1111-111111111111:3"),
            Some("11111111-1111-1111-1111-111111111111:4"),
            Some("11111111-1111-1111-1111-111111111111:5"),
        ])),
    );
    let error = encoder
        .encode_batch(&invalid_gtid, DebeziumSourceDialect::MySql, usize::MAX)
        .unwrap_err()
        .to_string();
    assert!(error.contains("non-canonical framing"), "{error}");

    let mut missing_old_value = mysql_cdc_batch().await;
    replace_field_metadata(
        &mut missing_old_value,
        3,
        META_OLD_VALUE_OF,
        "unknown_current_column",
    );
    let error = encoder
        .encode_batch(
            &missing_old_value,
            DebeziumSourceDialect::MySql,
            usize::MAX,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("payload"), "{error}");
    assert!(error.contains("missing its full old-value mapping"), "{error}");

    let mut mismatched_old_extension = mysql_cdc_batch().await;
    replace_field_metadata(
        &mut mismatched_old_extension,
        3,
        META_ARROW_EXTENSION_METADATA,
        r#"{"version":1,"data_type":"char","column_type":"char(255)","unsigned":false,"character_set":"utf8mb4"}"#,
    );
    let error = encoder
        .encode_batch(
            &mismatched_old_extension,
            DebeziumSourceDialect::MySql,
            usize::MAX,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("exact physical Arrow type"), "{error}");
}

fn replace_column(batch: &mut SinkBatch, index: usize, array: arrow::array::ArrayRef) {
    let mut columns = batch.batch.columns().to_vec();
    columns[index] = array;
    batch.batch = RecordBatch::try_new(batch.batch.schema(), columns).unwrap();
    batch.byte_size = batch.batch.get_array_memory_size();
}

fn replace_field_metadata(batch: &mut SinkBatch, index: usize, key: &str, value: &str) {
    let schema = batch.batch.schema();
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut metadata = fields[index].metadata().clone();
    metadata.insert(key.to_owned(), value.to_owned());
    fields[index] = fields[index].clone().with_metadata(metadata);
    batch.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.batch.columns().to_vec())
        .unwrap();
    batch.byte_size = batch.batch.get_array_memory_size();
}

async fn mysql_cdc_batch() -> SinkBatch {
    let user_id = SchemaColumn::new("id".to_owned(), DataType::Int64, true)
        .with_constraints(true, false, None)
        .with_arrow_extension_metadata(
            "transferia.mysql.signed_integer",
            r#"{"version":1,"data_type":"bigint","column_type":"bigint","unsigned":false}"#,
        );
    let user_payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, true)
        .with_arrow_extension_metadata(
            "transferia.mysql.text",
            r#"{"version":1,"data_type":"varchar","column_type":"varchar(255)","unsigned":false,"character_set":"utf8mb4"}"#,
        );
    let old_id = SchemaColumn::new("_system_old_value_0".to_owned(), DataType::Int64, true)
        .with_old_value_of("id".to_owned())
        .with_arrow_extension_metadata(
            "transferia.mysql.signed_integer",
            r#"{"version":1,"data_type":"bigint","column_type":"bigint","unsigned":false}"#,
        );
    let old_payload = SchemaColumn::new("_system_old_value_1".to_owned(), DataType::Utf8, true)
        .with_old_value_of("payload".to_owned())
        .with_arrow_extension_metadata(
            "transferia.mysql.text",
            r#"{"version":1,"data_type":"varchar","column_type":"varchar(255)","unsigned":false,"character_set":"utf8mb4"}"#,
        );
    let roles = [
        (
            "_system_source_database",
            SYSTEM_ROLE_SOURCE_DATABASE,
            DataType::Utf8,
            false,
        ),
        (
            "_system_source_schema",
            SYSTEM_ROLE_SOURCE_SCHEMA,
            DataType::Utf8,
            false,
        ),
        (
            "_system_source_table",
            SYSTEM_ROLE_SOURCE_TABLE,
            DataType::Utf8,
            false,
        ),
        (
            "_system_source_transaction_id",
            SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
            DataType::Binary,
            false,
        ),
        (
            "_system_source_timestamp_ms",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            DataType::Int64,
            false,
        ),
        (
            "_system_source_timestamp_us",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
            DataType::Int64,
            false,
        ),
        (
            "_system_source_timestamp_ns",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
            DataType::Int64,
            false,
        ),
        (
            "_system_event_timestamp_ms",
            SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
            DataType::Int64,
            false,
        ),
        (
            "_system_event_timestamp_us",
            SYSTEM_ROLE_EVENT_TIMESTAMP_US,
            DataType::Int64,
            false,
        ),
        (
            "_system_event_timestamp_ns",
            SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
            DataType::Int64,
            false,
        ),
        (
            "_system_source_server_id",
            SYSTEM_ROLE_SOURCE_SERVER_ID,
            DataType::Int64,
            false,
        ),
        (
            "_system_source_gtid",
            SYSTEM_ROLE_SOURCE_GTID,
            DataType::Utf8,
            true,
        ),
        (
            "_system_source_binlog_file",
            SYSTEM_ROLE_SOURCE_BINLOG_FILE,
            DataType::Utf8,
            false,
        ),
        (
            "_system_source_binlog_position",
            SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
            DataType::Int64,
            false,
        ),
        (
            "_system_source_binlog_row",
            SYSTEM_ROLE_SOURCE_BINLOG_ROW,
            DataType::Int32,
            false,
        ),
    ];
    let mut fields = vec![
        Field::new("id", DataType::Int64, true).with_metadata(user_id.arrow_metadata()),
        Field::new("payload", DataType::Utf8, true).with_metadata(user_payload.arrow_metadata()),
        Field::new("_system_old_value_0", DataType::Int64, true)
            .with_metadata(old_id.arrow_metadata()),
        Field::new("_system_old_value_1", DataType::Utf8, true)
            .with_metadata(old_payload.arrow_metadata()),
    ];
    fields.extend(roles.into_iter().map(|(name, role, data_type, nullable)| {
        Field::new(name, data_type.clone(), nullable).with_metadata(
            SchemaColumn::new(name.to_owned(), data_type, nullable)
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
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("snapshot"),
                Some("created"),
                Some("updated"),
                Some("renamed"),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                None,
                None,
                Some(2),
                Some(2),
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![
                None,
                None,
                Some("created"),
                Some("updated"),
                Some("renamed"),
            ])),
            Arc::new(StringArray::from(vec!["inventory"; 5])),
            Arc::new(StringArray::from(vec!["inventory"; 5])),
            Arc::new(StringArray::from(vec!["accounts"; 5])),
            Arc::new(BinaryArray::from_iter_values([
                &b"snapshot"[..],
                &b"tx-1"[..],
                &b"tx-2"[..],
                &b"tx-3"[..],
                &b"tx-4"[..],
            ])),
            Arc::new(Int64Array::from(vec![1_000_i64; 5])),
            Arc::new(Int64Array::from(vec![1_000_000_i64; 5])),
            Arc::new(Int64Array::from(vec![1_000_000_000_i64; 5])),
            Arc::new(Int64Array::from(vec![2_000_i64; 5])),
            Arc::new(Int64Array::from(vec![2_000_000_i64; 5])),
            Arc::new(Int64Array::from(vec![2_000_000_000_i64; 5])),
            Arc::new(Int64Array::from(vec![
                0_i64,
                4_294_967_295,
                11,
                11,
                11,
            ])),
            Arc::new(StringArray::from(vec![
                None,
                Some("11111111-1111-1111-1111-111111111111:blue:2"),
                Some("11111111-1111-1111-1111-111111111111:3"),
                Some("11111111-1111-1111-1111-111111111111:4"),
                Some("11111111-1111-1111-1111-111111111111:5"),
            ])),
            Arc::new(StringArray::from(vec!["mysql-bin.000001"; 5])),
            Arc::new(Int64Array::from(vec![4_i64, 100, 120, 140, 160])),
            Arc::new(Int32Array::from(vec![0_i32, 0, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["r", "c", "u", "u", "d"])),
            Arc::new(BinaryArray::from_iter_values([
                &[0b11_u8][..],
                &[0b11_u8][..],
                &[0b01_u8][..],
                &[0b01_u8][..],
                &[0b11_u8][..],
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
                index: 19,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                index: 20,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangedColumns,
                index: 21,
                name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
            },
        ]),
    }
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
