use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, Date32Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use bytes::{BufMut, Bytes, BytesMut};

use super::config::{LogicalDecoder, PostgresReplicationConfig};
use super::event::{ChangeEvent, LogicalValue};
use super::pgoutput::PgOutputDecoder;
use super::reader::{
    events_to_table_data, normalize_pgoutput_event, normalize_wal2json_event, parse_lsn,
    parse_postgres_char,
};
use super::wal2json;
use crate::connectors::postgres::source::{DiscoveredTable, TableConfig};
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, META_CHANGE_OPERATION, META_OLD_KEY_OF, META_OLD_VALUE_OF,
    META_SYSTEM_ROLE, SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_SOURCE_DATABASE,
    SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::ChangeOperation;

const RELATION_ID: u32 = 42;
const TRANSACTION_ID: u32 = 7;
const COMMIT_MICROS: i64 = 1_234;
const END_LSN: u64 = 101;

#[test]
fn replication_config_requires_valid_slot_and_decoder_settings() {
    let valid = PostgresReplicationConfig {
        slot: "transferia_slot".into(),
        decoder: LogicalDecoder::Pgoutput {
            publication: "transferia_publication".into(),
        },
        max_changes: 4_096,
        poll_interval_ms: 100,
        bootstrap_timeout_ms: 30_000,
    };
    valid.validate().unwrap();

    for invalid in [
        PostgresReplicationConfig {
            slot: "bad-slot".into(),
            ..valid.clone()
        },
        PostgresReplicationConfig {
            max_changes: 0,
            ..valid.clone()
        },
        PostgresReplicationConfig {
            max_changes: usize::try_from(i32::MAX).unwrap() + 1,
            ..valid.clone()
        },
        PostgresReplicationConfig {
            poll_interval_ms: 0,
            ..valid.clone()
        },
        PostgresReplicationConfig {
            bootstrap_timeout_ms: 0,
            ..valid.clone()
        },
        PostgresReplicationConfig {
            decoder: LogicalDecoder::Pgoutput {
                publication: "bad publication".into(),
            },
            ..valid
        },
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn postgres_internal_char_accepts_snapshot_and_logical_text_forms() {
    assert_eq!(
        parse_postgres_char(&LogicalValue::Text(Bytes::from_static(b"A"))).unwrap(),
        Some(65)
    );
    assert_eq!(
        parse_postgres_char(&LogicalValue::Text(Bytes::from_static(b"-2"))).unwrap(),
        Some(-2)
    );
    assert_eq!(parse_postgres_char(&LogicalValue::Null).unwrap(), None);

    let error = parse_postgres_char(&LogicalValue::Text(Bytes::from_static(b"AB")))
        .unwrap_err()
        .to_string();
    assert!(error.contains("PostgreSQL internal char"), "{error}");
}

#[test]
fn pgoutput_and_wal2json_normalize_the_same_transaction_identically() {
    let table = discovered_table();
    let mut decoder = PgOutputDecoder::default();
    assert!(decoder.decode(&relation_message()).unwrap().is_empty());
    assert!(decoder.decode(&begin_message()).unwrap().is_empty());
    assert!(decoder.decode(&insert_message()).unwrap().is_empty());
    assert!(decoder.decode(&update_message()).unwrap().is_empty());
    assert!(decoder.decode(&delete_message()).unwrap().is_empty());
    let pgoutput = decoder
        .decode(&commit_message())
        .unwrap()
        .into_iter()
        .map(|event| normalize_pgoutput_event(&table, event).unwrap())
        .collect::<Vec<_>>();

    let transaction = wal2json::decode(wal2json_transaction().as_bytes()).unwrap();
    assert_eq!(transaction.end_lsn, END_LSN);
    let wal2json = transaction
        .events
        .into_iter()
        .map(|event| normalize_wal2json_event(&table, event).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(pgoutput, wal2json);
    assert_eq!(
        pgoutput
            .iter()
            .map(|event| event.operation.code())
            .collect::<Vec<_>>(),
        ["c", "u", "d"]
    );
    assert!(pgoutput.iter().all(|event| event.lsn == END_LSN));
}

#[test]
fn normalized_cdc_batch_marks_and_indexes_the_operation_column() {
    let table = discovered_table();
    let mut decoder = PgOutputDecoder::default();
    for message in [
        relation_message(),
        begin_message(),
        insert_message(),
        update_message(),
        delete_message(),
    ] {
        assert!(decoder.decode(&message).unwrap().is_empty());
    }
    let events = decoder
        .decode(&commit_message())
        .unwrap()
        .into_iter()
        .map(|event| normalize_pgoutput_event(&table, event).unwrap())
        .collect::<Vec<_>>();
    let commit_timestamp_micros = events[0].commit_timestamp_micros;
    let data = events_to_table_data(&table, "postgres", &events).unwrap();

    let operation = data
        .system_columns
        .get(SystemColumnKind::ChangeOperation)
        .unwrap();
    assert_eq!(operation.name.as_ref(), "_system_change_operation");
    assert_eq!(
        data.batch
            .schema()
            .field(operation.index)
            .metadata()
            .get(META_CHANGE_OPERATION),
        Some(&"true".to_owned())
    );
    let operations = data.batch.column(operation.index);
    let operations = operations.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        operations.iter().collect::<Vec<_>>(),
        [Some("c"), Some("u"), Some("d")]
    );

    let ids = data
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let names = data
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let balances = data
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.iter().collect::<Vec<_>>(), [Some(1), Some(1), Some(1)]);
    assert_eq!(
        names.iter().collect::<Vec<_>>(),
        [Some("alice"), Some("alice-2"), None]
    );
    assert_eq!(
        balances.iter().collect::<Vec<_>>(),
        [Some(10), Some(11), None]
    );
    let old_key = data
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(old_key.iter().collect::<Vec<_>>(), [None, Some(1), Some(1)]);
    assert_eq!(
        data.batch.schema().field(3).metadata().get(META_OLD_KEY_OF),
        Some(&"id".to_owned())
    );
    let changed = data
        .system_columns
        .get(SystemColumnKind::ChangedColumns)
        .unwrap();
    let changed = data
        .batch
        .column(changed.index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(
        changed.iter().collect::<Vec<_>>(),
        [Some(&[0b111][..]), Some(&[0b111][..]), Some(&[0b001][..])]
    );
    let message_index = data
        .system_columns
        .get(SystemColumnKind::MessageIndex)
        .unwrap();
    let message_index = data
        .batch
        .column(message_index.index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(message_index.values(), &[0, 1, 2]);

    let schema = data.batch.schema();
    let role_index = |role: &str| {
        schema
            .fields()
            .iter()
            .position(|field| {
                field.metadata().get(META_SYSTEM_ROLE).map(String::as_str) == Some(role)
            })
            .unwrap()
    };
    let database = data
        .batch
        .column(role_index(SYSTEM_ROLE_SOURCE_DATABASE))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let source_schema = data
        .batch
        .column(role_index(SYSTEM_ROLE_SOURCE_SCHEMA))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let source_table = data
        .batch
        .column(role_index(SYSTEM_ROLE_SOURCE_TABLE))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let transaction_id = data
        .batch
        .column(role_index(SYSTEM_ROLE_SOURCE_TRANSACTION_ID))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let source_timestamp = data
        .batch
        .column(role_index(SYSTEM_ROLE_SOURCE_TIMESTAMP_US))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let event_timestamp = data
        .batch
        .column(role_index(SYSTEM_ROLE_EVENT_TIMESTAMP_NS))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(database.value(0), "postgres");
    assert_eq!(source_schema.value(0), "public");
    assert_eq!(source_table.value(0), "accounts");
    assert_eq!(transaction_id.value(0), u64::from(TRANSACTION_ID));
    assert_eq!(source_timestamp.value(0), commit_timestamp_micros);
    assert!(event_timestamp.value(0) > commit_timestamp_micros);
}

#[test]
fn cdc_temporals_match_snapshot_arrow_types_values_and_utc_semantics() {
    let table = DiscoveredTable {
        config: TableConfig {
            schema: "public".into(),
            name: "temporal_values".into(),
        },
        schema: DatasetSchema::new(vec![
            SchemaColumn::new("day".into(), DataType::Date32, false),
            SchemaColumn::new(
                "created_at".into(),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            SchemaColumn::new(
                "observed_at".into(),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]),
        type_oids: vec![1_082, 1_114, 1_184],
        replica_identity_full: false,
        replica_identity: "d".to_owned(),
        relation_oid: 16_385,
    };
    let events = vec![ChangeEvent {
        schema: Arc::from("public"),
        table: Arc::from("temporal_values"),
        operation: ChangeOperation::Create,
        values: vec![
            LogicalValue::Text(Bytes::from_static(b"2024-01-01")),
            LogicalValue::Text(Bytes::from_static(b"2024-01-01 00:00:00.123456")),
            LogicalValue::Text(Bytes::from_static(b"2024-01-01 03:00:00.123456+03")),
        ],
        old_values: None,
        old_values_kind: None,
        lsn: END_LSN,
        transaction_id: TRANSACTION_ID,
        commit_timestamp_micros: COMMIT_MICROS,
    }];

    let data = events_to_table_data(&table, "postgres", &events).unwrap();
    assert_eq!(data.batch.schema().field(0).data_type(), &DataType::Date32);
    assert_eq!(
        data.batch.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        data.batch.schema().field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let day = data
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    let created_at = data
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let observed_at = data
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(day.value(0), 19_723);
    assert_eq!(created_at.value(0), 1_704_067_200_123_456);
    assert_eq!(observed_at.value(0), created_at.value(0));
}

#[test]
fn default_replica_identity_preserves_the_old_key_when_an_update_renames_it() {
    let table = discovered_table();
    let mut decoder = PgOutputDecoder::default();
    for message in [
        relation_message(),
        begin_message(),
        primary_key_update_message(),
    ] {
        assert!(decoder.decode(&message).unwrap().is_empty());
    }
    let events = decoder
        .decode(&commit_message())
        .unwrap()
        .into_iter()
        .map(|event| normalize_pgoutput_event(&table, event).unwrap())
        .collect::<Vec<_>>();
    let data = events_to_table_data(&table, "postgres", &events).unwrap();

    let current = data
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let old = data
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(current.value(0), 2);
    assert_eq!(old.value(0), 1);
}

#[test]
fn pgoutput_and_wal2json_mark_the_same_unchanged_toast_columns() {
    let table = discovered_table();
    let mut decoder = PgOutputDecoder::default();
    for message in [
        relation_message(),
        begin_message(),
        toasted_update_message(),
    ] {
        assert!(decoder.decode(&message).unwrap().is_empty());
    }
    let pgoutput = decoder
        .decode(&commit_message())
        .unwrap()
        .into_iter()
        .map(|event| normalize_pgoutput_event(&table, event).unwrap())
        .collect::<Vec<_>>();
    let wal2json = wal2json::decode(wal2json_toasted_transaction().as_bytes())
        .unwrap()
        .events
        .into_iter()
        .map(|event| normalize_wal2json_event(&table, event).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(pgoutput, wal2json);
    assert_eq!(pgoutput[0].values[1], LogicalValue::UnchangedToast);
    let data = events_to_table_data(&table, "postgres", &pgoutput).unwrap();
    let changed = data
        .system_columns
        .get(SystemColumnKind::ChangedColumns)
        .unwrap();
    let changed = data
        .batch
        .column(changed.index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(changed.value(0), &[0b101]);
    let names = data
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.is_null(0));
}

#[test]
fn replica_identity_full_emits_bijective_old_columns_and_complete_current_rows() {
    let mut table = discovered_table();
    table.replica_identity_full = true;
    table.replica_identity = "f".to_owned();
    let mut decoder = PgOutputDecoder::default();
    for message in [
        relation_message_with_identity(b'f'),
        begin_message(),
        full_identity_update_message(),
    ] {
        assert!(decoder.decode(&message).unwrap().is_empty());
    }
    let events = decoder
        .decode(&commit_message())
        .unwrap()
        .into_iter()
        .map(|event| normalize_pgoutput_event(&table, event).unwrap())
        .collect::<Vec<_>>();
    let data = events_to_table_data(&table, "postgres", &events).unwrap();

    assert_eq!(data.batch.num_columns(), 3 + 3 + 10 + 6);
    let schema = data.batch.schema();
    for (index, column) in table.schema.columns.iter().enumerate() {
        let old = schema.field(3 + index);
        assert_eq!(old.metadata().get(META_OLD_VALUE_OF), Some(&column.name));
    }
    let current_ids = data
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let current_names = data
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let old_ids = data
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let old_names = data
        .batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(current_ids.value(0), 2);
    assert_eq!(current_names.value(0), "alice");
    assert_eq!(old_ids.value(0), 1);
    assert_eq!(old_names.value(0), "alice");
    let changed = data
        .system_columns
        .get(SystemColumnKind::ChangedColumns)
        .unwrap();
    let changed = data
        .batch
        .column(changed.index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(changed.value(0), &[0b111]);
}

#[test]
fn pgoutput_uses_commit_end_lsn_as_the_durable_offset() {
    let mut decoder = PgOutputDecoder::default();
    for message in [relation_message(), begin_message(), insert_message()] {
        decoder.decode(&message).unwrap();
    }
    let events = decoder.decode(&commit_message()).unwrap();
    assert_eq!(events[0].event.lsn, END_LSN);
}

#[test]
fn pgoutput_rejects_truncation_unknown_relations_and_invalid_transaction_order() {
    let mut decoder = PgOutputDecoder::default();
    assert!(decoder.decode(b"B").is_err());
    assert!(decoder.decode(&commit_message()).is_err());
    assert!(decoder.decode(&truncate_message()).is_err());

    decoder.decode(&begin_message()).unwrap();
    assert!(decoder.decode(&begin_message()).is_err());

    let mut decoder = PgOutputDecoder::default();
    decoder.decode(&begin_message()).unwrap();
    assert!(decoder.decode(&insert_message()).is_err());
}

#[test]
fn pgoutput_rejects_relation_and_tuple_shape_drift() {
    let table = discovered_table();
    let mut decoder = PgOutputDecoder::default();
    for message in [relation_message(), begin_message(), insert_message()] {
        decoder.decode(&message).unwrap();
    }
    let event = decoder.decode(&commit_message()).unwrap().remove(0);
    let mut replaced_table = table.clone();
    replaced_table.relation_oid += 1;
    assert!(normalize_pgoutput_event(&replaced_table, event).is_err());

    let mut decoder = PgOutputDecoder::default();
    let mut relation = relation_message();
    let type_oid_start = relation
        .windows(3)
        .position(|window| window == b"id\0")
        .unwrap()
        + 3;
    relation[type_oid_start..type_oid_start + 4].copy_from_slice(&999_u32.to_be_bytes());
    decoder.decode(&relation).unwrap();
    decoder.decode(&begin_message()).unwrap();
    decoder.decode(&insert_message()).unwrap();
    let event = decoder.decode(&commit_message()).unwrap().remove(0);
    assert!(normalize_pgoutput_event(&table, event).is_err());

    let mut decoder = PgOutputDecoder::default();
    decoder
        .decode(&relation_message_with_identity_and_keys(
            b'd',
            [false, true, false],
        ))
        .unwrap();
    decoder.decode(&begin_message()).unwrap();
    decoder.decode(&insert_message()).unwrap();
    let event = decoder.decode(&commit_message()).unwrap().remove(0);
    let error = normalize_pgoutput_event(&table, event)
        .unwrap_err()
        .to_string();
    assert!(error.contains("replica-identity"), "{error}");

    let mut decoder = PgOutputDecoder::default();
    decoder.decode(&relation_message()).unwrap();
    decoder.decode(&begin_message()).unwrap();
    let mut insert = insert_message();
    let tuple_count = 1 + 4 + 1;
    insert[tuple_count..tuple_count + 2].copy_from_slice(&2_u16.to_be_bytes());
    assert!(decoder.decode(&insert).is_err());
}

#[test]
fn pgoutput_consumes_origin_and_user_defined_type_metadata() {
    let mut decoder = PgOutputDecoder::default();
    assert!(decoder.decode(&origin_message()).unwrap().is_empty());
    assert!(decoder.decode(&type_message()).unwrap().is_empty());

    let mut trailing = type_message();
    trailing.push(0xff);
    assert!(decoder.decode(&trailing).is_err());
}

#[test]
fn wal2json_rejects_unknown_operations_and_shape_or_type_drift() {
    let unknown = wal2json_transaction().replace("\"insert\"", "\"truncate\"");
    assert!(wal2json::decode(unknown.as_bytes()).is_err());

    let missing_oid = wal2json_transaction().replacen(",25,20", ",25", 1);
    assert!(wal2json::decode(missing_oid.as_bytes()).is_err());

    let table = discovered_table();
    let wrong_oid = wal2json_transaction().replacen("[23,25,20]", "[999,25,20]", 1);
    let event = wal2json::decode(wrong_oid.as_bytes())
        .unwrap()
        .events
        .remove(0);
    assert!(normalize_wal2json_event(&table, event).is_err());

    let wrong_old_key = wal2json_transaction().replacen(
        "\"keynames\": [\"id\"], \"keytypeoids\": [23]",
        "\"keynames\": [\"name\"], \"keytypeoids\": [25]",
        1,
    );
    let event = wal2json::decode(wrong_old_key.as_bytes())
        .unwrap()
        .events
        .remove(1);
    let error = normalize_wal2json_event(&table, event)
        .unwrap_err()
        .to_string();
    assert!(error.contains("replica identity"), "{error}");
}

#[test]
fn lsn_parser_is_strict_and_round_trips_protocol_examples() {
    assert_eq!(parse_lsn("0/65").unwrap(), 101);
    assert_eq!(parse_lsn("6E/85004178").unwrap(), 0x0000_006e_8500_4178);
    for invalid in ["", "65", "0/", "/65", "0/xyz", "0/1/2"] {
        assert!(
            parse_lsn(invalid).is_err(),
            "accepted invalid LSN {invalid:?}"
        );
    }
}

fn discovered_table() -> DiscoveredTable {
    DiscoveredTable {
        config: TableConfig {
            schema: "public".into(),
            name: "accounts".into(),
        },
        schema: DatasetSchema::new(vec![
            SchemaColumn::new("id".into(), DataType::Int32, false)
                .with_constraints(true, false, None),
            SchemaColumn::new("name".into(), DataType::Utf8, false),
            SchemaColumn::new("balance".into(), DataType::Int64, false),
        ]),
        type_oids: vec![23, 25, 20],
        replica_identity_full: false,
        replica_identity: "d".to_owned(),
        relation_oid: RELATION_ID,
    }
}

fn relation_message() -> Vec<u8> {
    relation_message_with_identity(b'd')
}

fn relation_message_with_identity(identity: u8) -> Vec<u8> {
    relation_message_with_identity_and_keys(
        identity,
        if identity == b'f' {
            [true, true, true]
        } else {
            [true, false, false]
        },
    )
}

fn relation_message_with_identity_and_keys(identity: u8, keys: [bool; 3]) -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(b'R');
    message.put_u32(RELATION_ID);
    put_cstring(&mut message, "public");
    put_cstring(&mut message, "accounts");
    message.put_u8(identity);
    message.put_u16(3);
    for ((name, oid), key) in [("id", 23), ("name", 25), ("balance", 20)]
        .into_iter()
        .zip(keys)
    {
        message.put_u8(u8::from(key));
        put_cstring(&mut message, name);
        message.put_u32(oid);
        message.put_i32(-1);
    }
    message.to_vec()
}

fn begin_message() -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(b'B');
    message.put_u64(100);
    message.put_i64(COMMIT_MICROS);
    message.put_u32(TRANSACTION_ID);
    message.to_vec()
}

fn commit_message() -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(b'C');
    message.put_u8(0);
    message.put_u64(100);
    message.put_u64(END_LSN);
    message.put_i64(COMMIT_MICROS);
    message.to_vec()
}

fn insert_message() -> Vec<u8> {
    row_message(
        b'I',
        None,
        [
            WireValue::Text("1"),
            WireValue::Text("alice"),
            WireValue::Text("10"),
        ],
    )
}

fn update_message() -> Vec<u8> {
    row_message(
        b'U',
        Some((
            b'K',
            [
                WireValue::Text("1"),
                WireValue::Text("alice"),
                WireValue::Text("10"),
            ],
        )),
        [
            WireValue::Text("1"),
            WireValue::Text("alice-2"),
            WireValue::Text("11"),
        ],
    )
}

fn toasted_update_message() -> Vec<u8> {
    row_message(
        b'U',
        Some((
            b'K',
            [WireValue::Text("1"), WireValue::Null, WireValue::Null],
        )),
        [
            WireValue::Text("1"),
            WireValue::Unchanged,
            WireValue::Text("11"),
        ],
    )
}

fn primary_key_update_message() -> Vec<u8> {
    row_message(
        b'U',
        Some((
            b'K',
            [WireValue::Text("1"), WireValue::Null, WireValue::Null],
        )),
        [
            WireValue::Text("2"),
            WireValue::Text("alice"),
            WireValue::Text("10"),
        ],
    )
}

fn full_identity_update_message() -> Vec<u8> {
    row_message(
        b'U',
        Some((
            b'O',
            [
                WireValue::Text("1"),
                WireValue::Text("alice"),
                WireValue::Text("10"),
            ],
        )),
        [
            WireValue::Text("2"),
            WireValue::Unchanged,
            WireValue::Text("11"),
        ],
    )
}

fn delete_message() -> Vec<u8> {
    row_message(
        b'D',
        Some((
            b'K',
            [
                WireValue::Text("1"),
                WireValue::Text("alice-2"),
                WireValue::Text("11"),
            ],
        )),
        [WireValue::Null, WireValue::Null, WireValue::Null],
    )
}

fn truncate_message() -> Vec<u8> {
    vec![b'T']
}

fn origin_message() -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(b'O');
    message.put_u64(100);
    put_cstring(&mut message, "origin");
    message.to_vec()
}

fn type_message() -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(b'Y');
    message.put_u32(80_000);
    put_cstring(&mut message, "public");
    put_cstring(&mut message, "transferia_mood");
    message.to_vec()
}

#[derive(Clone, Copy)]
enum WireValue<'a> {
    Null,
    Text(&'a str),
    Unchanged,
}

fn row_message<const N: usize>(
    tag: u8,
    old: Option<(u8, [WireValue<'_>; N])>,
    new: [WireValue<'_>; N],
) -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(tag);
    message.put_u32(RELATION_ID);
    if let Some((old_tag, values)) = old {
        message.put_u8(old_tag);
        put_tuple(&mut message, &values);
    }
    if tag != b'D' {
        message.put_u8(b'N');
        put_tuple(&mut message, &new);
    }
    message.to_vec()
}

fn put_tuple(message: &mut BytesMut, values: &[WireValue<'_>]) {
    message.put_u16(u16::try_from(values.len()).unwrap());
    for value in values {
        match value {
            WireValue::Null => message.put_u8(b'n'),
            WireValue::Unchanged => message.put_u8(b'u'),
            WireValue::Text(value) => {
                message.put_u8(b't');
                message.put_u32(u32::try_from(value.len()).unwrap());
                message.put_slice(value.as_bytes());
            }
        }
    }
}

fn put_cstring(message: &mut BytesMut, value: &str) {
    message.put_slice(value.as_bytes());
    message.put_u8(0);
}

fn wal2json_transaction() -> String {
    r#"{
      "xid": 7,
      "nextlsn": "0/65",
      "timestamp": "2000-01-01 00:00:00.001234+00",
      "change": [
        {
          "kind": "insert", "schema": "public", "table": "accounts",
          "columnnames": ["id", "name", "balance"],
          "columntypeoids": [23,25,20],
          "columnvalues": [1, "alice", 10]
        },
        {
          "kind": "update", "schema": "public", "table": "accounts",
          "columnnames": ["id", "name", "balance"],
          "columntypeoids": [23,25,20],
          "columnvalues": [1, "alice-2", 11],
          "oldkeys": {"keynames": ["id"], "keytypeoids": [23], "keyvalues": [1]}
        },
        {
          "kind": "delete", "schema": "public", "table": "accounts",
          "oldkeys": {"keynames": ["id"], "keytypeoids": [23], "keyvalues": [1]}
        }
      ]
    }"#
    .to_owned()
}

fn wal2json_toasted_transaction() -> String {
    r#"{
      "xid": 7,
      "nextlsn": "0/65",
      "timestamp": "2000-01-01 00:00:00.001234+00",
      "change": [
        {
          "kind": "update", "schema": "public", "table": "accounts",
          "columnnames": ["id", "balance"],
          "columntypeoids": [23,20],
          "columnvalues": [1, 11],
          "oldkeys": {"keynames": ["id"], "keytypeoids": [23], "keyvalues": [1]}
        }
      ]
    }"#
    .to_owned()
}
