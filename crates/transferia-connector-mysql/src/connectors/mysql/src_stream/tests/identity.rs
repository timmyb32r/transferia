use super::super::{
    encode_snapshot_boundary_identity, encode_transaction_identity, MySqlBinlogPosition,
    MySqlTransactionIdentity,
};
use crate::connectors::mysql::src_batch_and_stream::MySqlBinlogBoundary;

#[test]
fn transaction_identity_encoding_distinguishes_variants_tags_and_raw_filenames() {
    let untagged = encode_transaction_identity(&MySqlTransactionIdentity::Gtid {
        sid: [7; 16],
        tag: None,
        gno: 42,
    })
    .unwrap();
    let tagged = encode_transaction_identity(&MySqlTransactionIdentity::Gtid {
        sid: [7; 16],
        tag: Some("blue_1".to_owned()),
        gno: 42,
    })
    .unwrap();
    let anonymous = encode_transaction_identity(&MySqlTransactionIdentity::Anonymous {
        begin_position: MySqlBinlogPosition::new(vec![0xff, b'1'], 4).unwrap(),
    })
    .unwrap();
    let file_position = encode_transaction_identity(&MySqlTransactionIdentity::FilePosition {
        begin_position: MySqlBinlogPosition::new(vec![0xff, b'1'], 4).unwrap(),
    })
    .unwrap();
    let different_raw_filename =
        encode_transaction_identity(&MySqlTransactionIdentity::FilePosition {
            begin_position: MySqlBinlogPosition::new(vec![0xfe, b'1'], 4).unwrap(),
        })
        .unwrap();

    assert_ne!(untagged, tagged);
    assert_ne!(anonymous, file_position);
    assert_ne!(file_position, different_raw_filename);
}

#[test]
fn length_framing_keeps_boundaries_injective() {
    let first = encode_snapshot_boundary_identity(&MySqlBinlogBoundary {
        filename: "mysql-bin.1".to_owned(),
        position: 23,
        gtid_executed: "ab".to_owned(),
        source_timestamp_micros: 123,
    })
    .unwrap();
    let second = encode_snapshot_boundary_identity(&MySqlBinlogBoundary {
        filename: "mysql-bin.1a".to_owned(),
        position: 23,
        gtid_executed: "b".to_owned(),
        source_timestamp_micros: 123,
    })
    .unwrap();
    let transaction = encode_transaction_identity(&MySqlTransactionIdentity::FilePosition {
        begin_position: MySqlBinlogPosition::new(b"mysql-bin.1".to_vec(), 23).unwrap(),
    })
    .unwrap();

    assert_ne!(first, second);
    assert_ne!(first, transaction);
}
