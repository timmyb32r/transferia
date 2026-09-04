use std::sync::Arc;

use arrow::array::{
    BinaryArray, Date32Array, DurationMicrosecondArray, FixedSizeBinaryBuilder, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
    assert_ne!(update["after"]["payload"], "__debezium_unavailable_value");

    assert_eq!(json(encoded.messages[3].key.as_deref().unwrap())["id"], 2);
    assert_eq!(
        json(encoded.messages[3].value.as_deref().unwrap())["op"],
        "d"
    );
    assert_eq!(json(encoded.messages[4].key.as_deref().unwrap())["id"], 2);
    assert!(encoded.messages[4].value.is_none());
    assert_eq!(json(encoded.messages[5].key.as_deref().unwrap())["id"], 3);
    let primary_key_create = json(encoded.messages[5].value.as_deref().unwrap());
    assert_eq!(primary_key_create["op"], "c");
    assert_eq!(primary_key_create["after"]["payload"], "renamed");

    assert_eq!(json(encoded.messages[6].key.as_deref().unwrap())["id"], 3);
    assert_eq!(
        json(encoded.messages[6].value.as_deref().unwrap())["op"],
        "d"
    );
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
        .encode_batch(&empty_identity, DebeziumSourceDialect::MySql, usize::MAX)
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
        .encode_batch(&invalid_server, DebeziumSourceDialect::MySql, usize::MAX)
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
        .encode_batch(&invalid_position, DebeziumSourceDialect::MySql, usize::MAX)
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
        .encode_batch(&missing_old_value, DebeziumSourceDialect::MySql, usize::MAX)
        .unwrap_err()
        .to_string();
    assert!(error.contains("payload"), "{error}");
    assert!(
        error.contains("missing its full old-value mapping"),
        "{error}"
    );

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

#[tokio::test]
async fn ydb_emits_exact_source_full_images_and_physical_values() {
    let batch = ydb_cdc_batch().await;
    let encoder = DebeziumJsonEncoder::new(
        "inventory".to_owned(),
        QueueMessageMode::KeyedWithTombstones,
    );

    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::Ydb, usize::MAX)
        .unwrap();

    assert_eq!(encoded.messages.len(), 4);
    assert_eq!(
        encoded.messages[0].key.as_deref(),
        Some(br#"{"id":18446744073709551615}"#.as_slice())
    );
    let create = json(encoded.messages[0].value.as_deref().unwrap());
    assert!(create["before"].is_null());
    assert_eq!(create["after"]["id"], u64::MAX);
    assert_eq!(create["after"]["payload"], "created");
    assert_eq!(create["after"]["raw"], "AP9B");
    assert_eq!(create["after"]["event_date"], 19_782);
    assert_eq!(create["after"]["event_datetime"], 1_709_210_096_000_i64);
    assert_eq!(
        create["after"]["event_timestamp"],
        1_709_210_096_123_456_i64
    );
    assert_eq!(create["after"]["event_interval"], -123_456_i64);
    assert_eq!(
        create["after"]["uuid"],
        "00112233-4455-6677-8899-aabbccddeeff"
    );
    assert_eq!(create["after"]["dynumber"]["scale"], 2);
    assert_eq!(create["after"]["dynumber"]["value"], "MDk=");
    assert_eq!(create["after"]["document"], r#"{"a":1}"#);
    assert_eq!(
        create["source"],
        serde_json::json!({
            "version": "1.0.0",
            "connector": "ydb",
            "name": "inventory",
            "ts_ms": 1_700_000_000_001_i64,
            "snapshot": "false",
            "db": "/local",
            "table": "/local/accounts",
            "step": 1_700_000_000_001_u64,
            "txId": 41_u64,
        })
    );
    assert_eq!(create["op"], "c");
    assert_eq!(create["ts_ms"], 1_700_000_100_001_i64);
    assert!(create.get("ts_us").is_none());
    assert!(create.get("ts_ns").is_none());

    let update = json(encoded.messages[1].value.as_deref().unwrap());
    assert_eq!(update["op"], "u");
    assert_eq!(update["before"]["payload"], "created");
    assert_eq!(update["after"]["payload"], "updated");
    assert_ne!(update["after"]["raw"], "__debezium_unavailable_value");

    let delete = json(encoded.messages[2].value.as_deref().unwrap());
    assert_eq!(delete["op"], "d");
    assert_eq!(delete["before"]["payload"], "updated");
    assert!(delete["after"].is_null());
    assert!(encoded.messages[3].value.is_none());
}

#[tokio::test]
async fn ydb_values_only_and_runtime_metadata_are_strict() {
    let encoder = DebeziumJsonEncoder::new("inventory".to_owned(), QueueMessageMode::ValuesOnly);
    let batch = ydb_cdc_batch().await;
    let encoded = encoder
        .encode_batch(&batch, DebeziumSourceDialect::Ydb, usize::MAX)
        .unwrap();
    assert_eq!(encoded.messages.len(), 3);
    assert!(encoded.messages.iter().all(|message| message.key.is_none()));

    let mut mismatched_step = ydb_cdc_batch().await;
    replace_column(
        &mut mismatched_step,
        23,
        Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
    );
    let error = encoder
        .encode_batch(&mismatched_step, DebeziumSourceDialect::Ydb, usize::MAX)
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not equal transaction step"), "{error}");

    let mut snapshot_operation = ydb_cdc_batch().await;
    replace_column(
        &mut snapshot_operation,
        29,
        Arc::new(StringArray::from(vec!["r", "u", "d"])),
    );
    let error = encoder
        .encode_batch(&snapshot_operation, DebeziumSourceDialect::Ydb, usize::MAX)
        .unwrap_err()
        .to_string();
    assert!(error.contains("stream-only"), "{error}");

    let mut invalid_dynumber = ydb_cdc_batch().await;
    replace_column(
        &mut invalid_dynumber,
        8,
        Arc::new(StringArray::from(vec![
            Some("not-a-number"),
            Some("123.45"),
            None,
        ])),
    );
    let error = encoder
        .encode_batch(&invalid_dynumber, DebeziumSourceDialect::Ydb, usize::MAX)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("DyNumber") && error.contains("malformed"),
        "{error}"
    );

    let mut overflowing_datetime = ydb_cdc_batch().await;
    replace_column(
        &mut overflowing_datetime,
        4,
        Arc::new(TimestampSecondArray::from(vec![
            Some(i64::MAX),
            Some(1_709_210_096),
            None,
        ])),
    );
    let error = encoder
        .encode_batch(
            &overflowing_datetime,
            DebeziumSourceDialect::Ydb,
            usize::MAX,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("Datetime milliseconds overflow"), "{error}");
}

async fn ydb_cdc_batch() -> SinkBatch {
    const USER_COLUMNS: usize = 10;
    let user = [
        SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("raw".to_owned(), DataType::Binary, true),
        SchemaColumn::new("event_date".to_owned(), DataType::Date32, true),
        SchemaColumn::new(
            "event_datetime".to_owned(),
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        SchemaColumn::new(
            "event_timestamp".to_owned(),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        SchemaColumn::new(
            "event_interval".to_owned(),
            DataType::Duration(TimeUnit::Microsecond),
            true,
        ),
        SchemaColumn::new("uuid".to_owned(), DataType::FixedSizeBinary(16), true)
            .with_arrow_extension("arrow.uuid"),
        SchemaColumn::new("dynumber".to_owned(), DataType::Utf8, true)
            .with_arrow_extension("transferia.ydb.dynumber"),
        SchemaColumn::new("document".to_owned(), DataType::Utf8, false)
            .with_arrow_extension("arrow.json"),
    ];
    let mut fields = user
        .iter()
        .map(|column| {
            Field::new(column.name.clone(), column.data_type.clone(), true)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    fields.extend(user.iter().enumerate().map(|(index, column)| {
        let old = SchemaColumn::new(
            format!("_system_old_value_{index}"),
            column.data_type.clone(),
            true,
        )
        .with_old_value_of(column.name.clone());
        let old = if let Some(extension) = column.arrow_extension_name {
            old.with_arrow_extension(extension)
        } else {
            old
        };
        Field::new(old.name.clone(), old.data_type.clone(), true)
            .with_metadata(old.arrow_metadata())
    }));
    let roles = [
        (
            "_system_source_database",
            SYSTEM_ROLE_SOURCE_DATABASE,
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
            DataType::FixedSizeBinary(16),
        ),
        (
            "_system_source_timestamp_ms",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            DataType::Int64,
        ),
    ];
    fields.extend(roles.iter().map(|(name, role, data_type)| {
        Field::new(*name, data_type.clone(), false).with_metadata(
            SchemaColumn::new((*name).to_owned(), data_type.clone(), false)
                .with_system_role(*role)
                .arrow_metadata(),
        )
    }));
    let system_kinds = [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::WriteTimestampMs,
        SystemColumnKind::ChangeOperation,
        SystemColumnKind::ChangedColumns,
    ];
    fields.extend(
        system_kinds
            .iter()
            .map(|kind| Field::new(kind.default_name(), kind.data_type(), false)),
    );

    let current_present = [Some(u64::MAX), Some(u64::MAX), None];
    let old_present = [None, Some(u64::MAX), Some(u64::MAX)];
    let mut transactions = FixedSizeBinaryBuilder::with_capacity(3, 16);
    for (step, tx_id) in [
        (1_700_000_000_001_u64, 41_u64),
        (1_700_000_000_002, 42),
        (1_700_000_000_003, 43),
    ] {
        let mut value = [0_u8; 16];
        value[..8].copy_from_slice(&step.to_be_bytes());
        value[8..].copy_from_slice(&tx_id.to_be_bytes());
        transactions.append_value(value).unwrap();
    }
    let uuid = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    let mut current_uuid = FixedSizeBinaryBuilder::with_capacity(3, 16);
    current_uuid.append_value(uuid).unwrap();
    current_uuid.append_value(uuid).unwrap();
    current_uuid.append_null();
    let mut old_uuid = FixedSizeBinaryBuilder::with_capacity(3, 16);
    old_uuid.append_null();
    old_uuid.append_value(uuid).unwrap();
    old_uuid.append_value(uuid).unwrap();
    let current_payload = [Some("created"), Some("updated"), None];
    let old_payload = [None, Some("created"), Some("updated")];
    let current_raw = [Some(&b"\0\xffA"[..]), Some(&b"\0\xffB"[..]), None];
    let old_raw = [None, Some(&b"\0\xffA"[..]), Some(&b"\0\xffB"[..])];
    let current_i32 = [Some(19_782), Some(19_782), None];
    let old_i32 = [None, Some(19_782), Some(19_782)];
    let current_i64 = [Some(1_709_210_096_i64), Some(1_709_210_096), None];
    let old_i64 = [None, Some(1_709_210_096_i64), Some(1_709_210_096)];
    let current_micros = [
        Some(1_709_210_096_123_456_i64),
        Some(1_709_210_096_123_456),
        None,
    ];
    let old_micros = [
        None,
        Some(1_709_210_096_123_456_i64),
        Some(1_709_210_096_123_456),
    ];
    let current_interval = [Some(-123_456_i64), Some(-123_456), None];
    let old_interval = [None, Some(-123_456_i64), Some(-123_456)];
    let current_text = [Some("123.45"), Some("123.45"), None];
    let old_text = [None, Some("123.45"), Some("123.45")];
    let current_json = [Some(r#"{"a":1}"#), Some(r#"{"a":2}"#), None];
    let old_json = [None, Some(r#"{"a":1}"#), Some(r#"{"a":2}"#)];
    let columns: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(UInt64Array::from(current_present.to_vec())),
        Arc::new(StringArray::from(current_payload.to_vec())),
        Arc::new(BinaryArray::from(current_raw.to_vec())),
        Arc::new(Date32Array::from(current_i32.to_vec())),
        Arc::new(TimestampSecondArray::from(current_i64.to_vec())),
        Arc::new(TimestampMicrosecondArray::from(current_micros.to_vec())),
        Arc::new(DurationMicrosecondArray::from(current_interval.to_vec())),
        Arc::new(current_uuid.finish()),
        Arc::new(StringArray::from(current_text.to_vec())),
        Arc::new(StringArray::from(current_json.to_vec())),
        Arc::new(UInt64Array::from(old_present.to_vec())),
        Arc::new(StringArray::from(old_payload.to_vec())),
        Arc::new(BinaryArray::from(old_raw.to_vec())),
        Arc::new(Date32Array::from(old_i32.to_vec())),
        Arc::new(TimestampSecondArray::from(old_i64.to_vec())),
        Arc::new(TimestampMicrosecondArray::from(old_micros.to_vec())),
        Arc::new(DurationMicrosecondArray::from(old_interval.to_vec())),
        Arc::new(old_uuid.finish()),
        Arc::new(StringArray::from(old_text.to_vec())),
        Arc::new(StringArray::from(old_json.to_vec())),
        Arc::new(StringArray::from(vec!["/local"; 3])),
        Arc::new(StringArray::from(vec!["/local/accounts"; 3])),
        Arc::new(transactions.finish()),
        Arc::new(Int64Array::from(vec![
            1_700_000_000_001_i64,
            1_700_000_000_002,
            1_700_000_000_003,
        ])),
        Arc::new(StringArray::from(vec!["/local/accounts/transferia_cdc"; 3])),
        Arc::new(Int64Array::from(vec![0_i64; 3])),
        Arc::new(Int64Array::from(vec![10_i64, 11, 12])),
        Arc::new(UInt64Array::from(vec![0_u64; 3])),
        Arc::new(Int64Array::from(vec![
            1_700_000_100_001_i64,
            1_700_000_100_002,
            1_700_000_100_003,
        ])),
        Arc::new(StringArray::from(vec!["c", "u", "d"])),
        Arc::new(BinaryArray::from_iter_values([
            &[0xff_u8, 0x03][..],
            &[0x02_u8, 0x00][..],
            &[0xff_u8, 0x03][..],
        ])),
    ];
    assert_eq!(
        USER_COLUMNS * 2 + roles.len() + system_kinds.len(),
        columns.len()
    );
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    SinkBatch {
        table: Arc::from("/local/accounts"),
        is_dlq: false,
        byte_size: batch.get_array_memory_size(),
        batch,
        memory: PipelineMemory::new(1024 * 1024).reserve(1).await,
        system_columns: SystemColumns::new(
            system_kinds
                .iter()
                .enumerate()
                .map(|(offset, kind)| SystemColumn {
                    kind: *kind,
                    index: USER_COLUMNS * 2 + roles.len() + offset,
                    name: Arc::from(kind.default_name()),
                })
                .collect::<Vec<_>>(),
        ),
    }
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
    batch.batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        batch.batch.columns().to_vec(),
    )
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
            Arc::new(Int64Array::from(vec![0_i64, 4_294_967_295, 11, 11, 11])),
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
