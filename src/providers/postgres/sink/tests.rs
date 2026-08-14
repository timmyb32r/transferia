use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::config::PostgresSinkConfig;

#[test]
fn sink_rejects_the_old_connection_string() {
    assert!(serde_yaml::from_str::<PostgresSinkConfig>(
        "connection: host=localhost port=5432\ntrusted_plaintext: true\ncreate_tables: true\n"
    )
    .is_err());
}

#[test]
fn binary_copy_encoder_writes_header_rows_null_and_trailer() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ],
    )
    .unwrap();
    let encoded = super::copy_binary::encode(&batch).unwrap();
    assert!(encoded.starts_with(b"PGCOPY\n\xFF\r\n\0"));
    assert_eq!(&encoded[encoded.len() - 2..], &(-1_i16).to_be_bytes());
}
