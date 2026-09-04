use mysql_async::binlog::events::{Event, FormatDescriptionEvent};
use mysql_async::binlog::{BinlogVersion, EventType};
use mysql_async::consts::GeometryType;

use super::super::{
    BinlogDecodeError, DecodedBinlogEvent, MySqlBinlogDecoder, MySqlBinlogPosition,
    MySqlReplicationConfig, MySqlRowOperation, MySqlTransactionIdentity,
};
use super::checksum::{raw_event, raw_event_bytes_with_log_pos};

fn config() -> MySqlReplicationConfig {
    MySqlReplicationConfig {
        server_id: 44,
        max_events: 16,
        max_transaction_bytes: 1 << 20,
        poll_interval_ms: 10,
        bootstrap_timeout_ms: 1_000,
    }
}

fn decoder() -> MySqlBinlogDecoder {
    MySqlBinlogDecoder::new(
        config(),
        MySqlBinlogPosition::new(b"mysql-bin.000001".to_vec(), 4).unwrap(),
    )
    .unwrap()
}

#[test]
fn gtid_row_transaction_preserves_identity_images_and_commit_position() {
    let mut decoder = decoder();
    let gtid_data = gtid_data([0x22; 16], 17);
    let gtid = raw_event(EventType::GTID_EVENT, &gtid_data, 4);
    let started = decoder.decode(&gtid).unwrap();
    let DecodedBinlogEvent::TransactionStarted(marker) = started else {
        panic!("expected transaction start")
    };
    assert_eq!(
        marker.identity,
        MySqlTransactionIdentity::Gtid {
            sid: [0x22; 16],
            tag: None,
            gno: 17,
        }
    );

    let table_start = decoder.current_position().position;
    let table_map = raw_event(EventType::TABLE_MAP_EVENT, &table_map_data(), table_start);
    let mapped = decoder.decode(&table_map).unwrap();
    let DecodedBinlogEvent::TableMapped(table) = mapped else {
        panic!("expected table map")
    };
    assert_eq!(table.table_id, 23);
    assert_eq!(table.database, b"db");
    assert_eq!(table.table, b"items");
    assert_eq!(table.column_identities.len(), 1);
    assert_eq!(table.column_identities[0].name, b"id");
    assert_eq!(table.column_identities[0].unsigned, Some(false));
    assert!(table.column_identities[0].visible);
    assert_eq!(table.column_identities[0].enum_values, None);
    assert_eq!(table.column_identities[0].set_values, None);
    assert_eq!(table.column_identities[0].primary_key_ordinal, Some(1));

    let rows_start = decoder.current_position().position;
    let rows = raw_event(EventType::WRITE_ROWS_EVENT, &write_rows_data(42), rows_start);
    let decoded = decoder.decode(&rows).unwrap();
    let DecodedBinlogEvent::Rows(decoded) = decoded else {
        panic!("expected rows")
    };
    assert_eq!(decoded.operation, MySqlRowOperation::Write);
    assert_eq!(decoded.before_columns, Vec::<bool>::new());
    assert_eq!(decoded.after_columns, vec![true]);
    assert_eq!(decoded.rows.len(), 1);
    assert!(decoded.rows[0].before.is_none());
    assert!(decoded.rows[0].after.is_some());
    assert_eq!(decoded.rows[0].row_in_event, 0);
    assert_eq!(decoded.source_server_id, 9);
    assert_eq!(decoded.event_position.filename, b"mysql-bin.000001");
    assert_eq!(decoded.event_position.position, rows_start);
    assert_eq!(decoded.transaction, marker);

    let xid_start = decoder.current_position().position;
    let xid = raw_event(EventType::XID_EVENT, &99_u64.to_le_bytes(), xid_start);
    let committed = decoder.decode(&xid).unwrap();
    let DecodedBinlogEvent::TransactionCommitted(committed) = committed else {
        panic!("expected commit")
    };
    assert_eq!(committed.xid, Some(99));
    assert_eq!(committed.transaction, marker);
    assert_eq!(committed.next_position, *decoder.current_position());
    assert!(decoder.active_transaction().is_none());
}

#[test]
fn xid_without_transaction_and_rows_without_table_map_fail_without_advancing() {
    let mut decoder = decoder();
    let start = decoder.current_position().clone();
    let xid = raw_event(EventType::XID_EVENT, &1_u64.to_le_bytes(), start.position);
    assert!(matches!(
        decoder.decode(&xid),
        Err(BinlogDecodeError::TransactionNotActive(EventType::XID_EVENT))
    ));
    assert_eq!(decoder.current_position(), &start);

    let gtid = raw_event(EventType::GTID_EVENT, &gtid_data([1; 16], 1), start.position);
    decoder.decode(&gtid).unwrap();
    let before_rows = decoder.current_position().clone();
    let rows = raw_event(
        EventType::WRITE_ROWS_EVENT,
        &write_rows_data(1),
        before_rows.position,
    );
    assert!(matches!(
        decoder.decode(&rows),
        Err(BinlogDecodeError::MissingTableMap(23))
    ));
    assert_eq!(decoder.current_position(), &before_rows);
}

#[test]
fn compressed_transactions_are_rejected_at_the_runtime_boundary() {
    let mut decoder = decoder();
    let payload = raw_event(EventType::TRANSACTION_PAYLOAD_EVENT, &[], 4);
    assert!(matches!(
        decoder.decode(&payload),
        Err(BinlogDecodeError::TransactionCompressionObserved)
    ));
}

#[test]
fn table_map_without_primary_key_remains_decodable_for_unselected_tables() {
    let mut decoder = decoder();
    decoder.retain_rows_for_tables(b"db", vec![b"selected".to_vec()]);
    decoder
        .decode(&raw_event(
            EventType::GTID_EVENT,
            &gtid_data([6; 16], 1),
            4,
        ))
        .unwrap();
    let mapped = decoder
        .decode(&raw_event(
            EventType::TABLE_MAP_EVENT,
            &table_map_data_without_primary_key(),
            decoder.current_position().position,
        ))
        .unwrap();
    assert!(matches!(mapped, DecodedBinlogEvent::Ignored(_)));
    let rows = decoder
        .decode(&raw_event(
            EventType::WRITE_ROWS_EVENT,
            &write_rows_data(7),
            decoder.current_position().position,
        ))
        .unwrap();
    assert!(matches!(rows, DecodedBinlogEvent::Ignored(_)));
}

#[test]
fn full_table_map_preserves_enum_set_geometry_vector_and_visibility() {
    let mut decoder = decoder();
    decoder
        .decode(&raw_event(
            EventType::GTID_EVENT,
            &gtid_data([9; 16], 1),
            4,
        ))
        .unwrap();
    let mapped = decoder
        .decode(&raw_event(
            EventType::TABLE_MAP_EVENT,
            &full_metadata_table_map_data(),
            decoder.current_position().position,
        ))
        .unwrap();
    let DecodedBinlogEvent::TableMapped(table) = mapped else {
        panic!("expected table map")
    };
    assert_eq!(table.column_identities.len(), 4);
    assert_eq!(
        table.column_identities[0].enum_values,
        Some(vec![Vec::new(), b"comma,value".to_vec()])
    );
    assert_eq!(
        table.column_identities[1].set_values,
        Some(vec![b"a".to_vec(), b"b,c".to_vec()])
    );
    assert_eq!(
        table.column_identities[2].geometry_type,
        Some(GeometryType::GEOM_POINT)
    );
    assert_eq!(table.column_identities[3].vector_dimensionality, Some(3));
    assert!(table.column_identities[0].visible);
    assert!(!table.column_identities[1].visible);
    assert!(table.column_identities[2].visible);
    assert!(table.column_identities[3].visible);
}

#[test]
fn full_table_map_rejects_duplicate_optional_identity_fields() {
    let mut decoder = decoder();
    decoder
        .decode(&raw_event(
            EventType::GTID_EVENT,
            &gtid_data([10; 16], 1),
            4,
        ))
        .unwrap();
    let mut table_map = table_map_data();
    table_map.extend_from_slice(&[12, 1, 0x80]);
    assert!(matches!(
        decoder.decode(&raw_event(
            EventType::TABLE_MAP_EVENT,
            &table_map,
            decoder.current_position().position,
        )),
        Err(BinlogDecodeError::DuplicateFullTableMetadata {
            field: "column visibility",
            ..
        })
    ));
}

#[test]
fn transaction_terminal_evicts_connector_owned_table_maps() {
    let mut decoder = decoder();
    decoder
        .decode(&raw_event(
            EventType::GTID_EVENT,
            &gtid_data([7; 16], 1),
            4,
        ))
        .unwrap();
    decoder
        .decode(&raw_event(
            EventType::TABLE_MAP_EVENT,
            &table_map_data(),
            decoder.current_position().position,
        ))
        .unwrap();
    decoder
        .decode(&raw_event(
            EventType::XID_EVENT,
            &1_u64.to_le_bytes(),
            decoder.current_position().position,
        ))
        .unwrap();
    decoder
        .decode(&raw_event(
            EventType::GTID_EVENT,
            &gtid_data([7; 16], 2),
            decoder.current_position().position,
        ))
        .unwrap();
    assert!(matches!(
        decoder.decode(&raw_event(
            EventType::WRITE_ROWS_EVENT,
            &write_rows_data(8),
            decoder.current_position().position,
        )),
        Err(BinlogDecodeError::MissingTableMap(23))
    ));
}

#[test]
fn gtid_auto_position_rebases_only_the_bootstrap_fake_rotate() {
    let mut decoder = MySqlBinlogDecoder::new(
        config(),
        MySqlBinlogPosition::new(b"purged-bin.000001".to_vec(), 9_000).unwrap(),
    )
    .unwrap();
    decoder.enable_gtid_auto_position();
    let rotate = Event::read(
        &FormatDescriptionEvent::new(BinlogVersion::Version4),
        raw_event_bytes_with_log_pos(
            EventType::ROTATE_EVENT,
            &rotate_data(b"mysql-bin.000042", 4),
            0,
            false,
        )
        .as_slice(),
    )
    .unwrap();
    let DecodedBinlogEvent::BinlogRotated(rotated) = decoder.decode(&rotate).unwrap() else {
        panic!("expected GTID auto-position rebase")
    };
    assert_eq!(rotated.next_position.filename, b"mysql-bin.000042");
    assert_eq!(rotated.next_position.position, 4);
    assert!(matches!(
        decoder.decode(&rotate),
        Err(BinlogDecodeError::Checksum(_))
    ));
}

#[test]
fn decoded_row_limit_is_cumulative_across_every_rows_event_in_the_transaction() {
    let mut limited = config();
    limited.max_events = 7;
    let mut decoder = MySqlBinlogDecoder::new(
        limited,
        MySqlBinlogPosition::new(b"mysql-bin.000001".to_vec(), 4).unwrap(),
    )
    .unwrap();
    decoder
        .decode(&raw_event(
            EventType::GTID_EVENT,
            &gtid_data([2; 16], 1),
            4,
        ))
        .unwrap();
    decoder
        .decode(&raw_event(
            EventType::TABLE_MAP_EVENT,
            &table_map_data(),
            decoder.current_position().position,
        ))
        .unwrap();
    let first_rows = decoder
        .decode(&raw_event(
            EventType::WRITE_ROWS_EVENT,
            &write_rows_data_many(&[1, 2, 3]),
            decoder.current_position().position,
        ))
        .unwrap();
    let DecodedBinlogEvent::Rows(first_rows) = first_rows else {
        panic!("expected decoded rows")
    };
    assert_eq!(
        first_rows
            .rows
            .iter()
            .map(|row| row.row_in_event)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let second_start = decoder.current_position().clone();
    let second_rows = decoder
        .decode(&raw_event(
            EventType::WRITE_ROWS_EVENT,
            &write_rows_data_many(&[4, 5]),
            second_start.position,
        ))
        .unwrap();
    let DecodedBinlogEvent::Rows(second_rows) = second_rows else {
        panic!("expected second decoded rows event")
    };
    assert_eq!(second_rows.event_position, second_start);
    assert_ne!(second_rows.event_position, first_rows.event_position);
    assert_eq!(
        second_rows
            .rows
            .iter()
            .map(|row| row.row_in_event)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let before = decoder.current_position().clone();
    let error = decoder
        .decode(&raw_event(
            EventType::WRITE_ROWS_EVENT,
            &write_rows_data_many(&[6, 7, 8]),
            before.position,
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        BinlogDecodeError::TooManyTransactionRows { row_count: 8, .. }
    ));
    assert_eq!(decoder.current_position(), &before);
}

#[test]
fn update_and_delete_preserve_complete_before_and_after_images() {
    let mut decoder = decoder();
    let gtid = raw_event(EventType::GTID_EVENT, &gtid_data([3; 16], 8), 4);
    decoder.decode(&gtid).unwrap();
    let table_map = raw_event(
        EventType::TABLE_MAP_EVENT,
        &table_map_data(),
        decoder.current_position().position,
    );
    decoder.decode(&table_map).unwrap();

    let update = raw_event(
        EventType::UPDATE_ROWS_EVENT,
        &update_rows_data(11, 12),
        decoder.current_position().position,
    );
    let DecodedBinlogEvent::Rows(update) = decoder.decode(&update).unwrap() else {
        panic!("expected update rows")
    };
    assert_eq!(update.operation, MySqlRowOperation::Update);
    assert_eq!(update.before_columns, vec![true]);
    assert_eq!(update.after_columns, vec![true]);
    assert!(update.rows[0].before.is_some());
    assert!(update.rows[0].after.is_some());

    let delete = raw_event(
        EventType::DELETE_ROWS_EVENT,
        &delete_rows_data(12),
        decoder.current_position().position,
    );
    let DecodedBinlogEvent::Rows(delete) = decoder.decode(&delete).unwrap() else {
        panic!("expected delete rows")
    };
    assert_eq!(delete.operation, MySqlRowOperation::Delete);
    assert_eq!(delete.before_columns, vec![true]);
    assert_eq!(delete.after_columns, Vec::<bool>::new());
    assert!(delete.rows[0].before.is_some());
    assert!(delete.rows[0].after.is_none());
}

#[test]
fn partial_row_images_and_rotate_inside_transaction_fail_closed() {
    let mut decoder = decoder();
    let gtid = raw_event(EventType::GTID_EVENT, &gtid_data([4; 16], 9), 4);
    decoder.decode(&gtid).unwrap();
    let table_map = raw_event(
        EventType::TABLE_MAP_EVENT,
        &table_map_data(),
        decoder.current_position().position,
    );
    decoder.decode(&table_map).unwrap();
    let before = decoder.current_position().clone();
    let partial = raw_event(
        EventType::WRITE_ROWS_EVENT,
        &write_rows_data_with_bitmap(7, 0),
        before.position,
    );
    assert!(matches!(
        decoder.decode(&partial),
        Err(BinlogDecodeError::PartialRowImage { .. })
    ));
    assert_eq!(decoder.current_position(), &before);

    let rotate = raw_event(
        EventType::ROTATE_EVENT,
        &rotate_data(b"mysql-bin.000002", 4),
        before.position,
    );
    assert!(matches!(
        decoder.decode(&rotate),
        Err(BinlogDecodeError::RotateInsideTransaction)
    ));
    assert_eq!(decoder.current_position(), &before);
}

fn gtid_data(sid: [u8; 16], gno: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(25);
    data.push(0);
    data.extend_from_slice(&sid);
    data.extend_from_slice(&gno.to_le_bytes());
    data
}

fn table_map_data() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&23_u64.to_le_bytes()[..6]);
    data.extend_from_slice(&0_u16.to_le_bytes());
    data.push(2);
    data.extend_from_slice(b"db");
    data.push(0);
    data.push(5);
    data.extend_from_slice(b"items");
    data.push(0);
    data.push(1);
    data.push(3); // MYSQL_TYPE_LONG
    data.push(0); // metadata length
    data.push(0); // nullable bitmap
    data.extend_from_slice(&[1, 1, 0]); // SIGNEDNESS: one signed numeric column
    data.extend_from_slice(&[4, 3, 2, b'i', b'd']); // COLUMN_NAME: "id"
    data.extend_from_slice(&[12, 1, 0x80]); // COLUMN_VISIBILITY: visible
    data.extend_from_slice(&[8, 1, 0]); // SIMPLE_PRIMARY_KEY: column 0
    data
}

fn table_map_data_without_primary_key() -> Vec<u8> {
    let mut data = table_map_data();
    data.truncate(data.len() - 3);
    data
}

fn full_metadata_table_map_data() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&23_u64.to_le_bytes()[..6]);
    data.extend_from_slice(&0_u16.to_le_bytes());
    data.push(2);
    data.extend_from_slice(b"db");
    data.push(0);
    data.push(5);
    data.extend_from_slice(b"items");
    data.push(0);
    data.push(4);
    data.extend_from_slice(&[254, 254, 255, 242]);
    data.push(6);
    data.extend_from_slice(&[247, 1, 248, 1, 4, 4]);
    data.push(0);
    data.extend_from_slice(&[11, 2, 45, 45]);
    data.extend_from_slice(&[
        4, 29, 6, b'c', b'h', b'o', b'i', b'c', b'e', 5, b'f', b'l', b'a', b'g', b's', 5,
        b's', b'h', b'a', b'p', b'e', 9, b'e', b'm', b'b', b'e', b'd', b'd', b'i', b'n', b'g',
    ]);
    data.extend_from_slice(&[
        5, 7, 2, 1, b'a', 3, b'b', b',', b'c',
    ]);
    data.extend_from_slice(&[
        6, 14, 2, 0, 11, b'c', b'o', b'm', b'm', b'a', b',', b'v', b'a', b'l', b'u', b'e',
    ]);
    data.extend_from_slice(&[7, 1, 1]);
    data.extend_from_slice(&[12, 1, 0xb0]);
    data.extend_from_slice(&[13, 1, 3]);
    data.extend_from_slice(&[8, 1, 0]);
    data
}

fn write_rows_data(value: i32) -> Vec<u8> {
    write_rows_data_with_bitmap(value, 1)
}

fn write_rows_data_many(values: &[i32]) -> Vec<u8> {
    let mut data = rows_event_prefix();
    data.push(1); // full after-image bitmap
    for value in values {
        data.push(0); // row null bitmap
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn write_rows_data_with_bitmap(value: i32, columns_bitmap: u8) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&23_u64.to_le_bytes()[..6]);
    data.extend_from_slice(&0_u16.to_le_bytes());
    data.extend_from_slice(&2_u16.to_le_bytes()); // empty v2 extra data
    data.push(1); // columns
    data.push(columns_bitmap);
    data.push(0); // row null bitmap
    data.extend_from_slice(&value.to_le_bytes());
    data
}

fn update_rows_data(before: i32, after: i32) -> Vec<u8> {
    let mut data = rows_event_prefix();
    data.push(1); // full before-image bitmap
    data.push(1); // full after-image bitmap
    data.push(0); // before-image null bitmap
    data.extend_from_slice(&before.to_le_bytes());
    data.push(0); // after-image null bitmap
    data.extend_from_slice(&after.to_le_bytes());
    data
}

fn delete_rows_data(value: i32) -> Vec<u8> {
    let mut data = rows_event_prefix();
    data.push(1); // full before-image bitmap
    data.push(0); // row null bitmap
    data.extend_from_slice(&value.to_le_bytes());
    data
}

fn rows_event_prefix() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&23_u64.to_le_bytes()[..6]);
    data.extend_from_slice(&0_u16.to_le_bytes());
    data.extend_from_slice(&2_u16.to_le_bytes()); // empty v2 extra data
    data.push(1); // columns
    data
}

fn rotate_data(filename: &[u8], position: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + filename.len());
    data.extend_from_slice(&position.to_le_bytes());
    data.extend_from_slice(filename);
    data
}
