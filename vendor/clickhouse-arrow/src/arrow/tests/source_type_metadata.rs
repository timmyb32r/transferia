use std::{io::Cursor, sync::Arc};
use arrow::{array::Int32Array, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch};
use crate::{ArrowOptions, arrow::deserialize::ArrowDeserializerState, formats::{DeserializerState, protocol_data::ProtocolData}, native::protocol::DBMS_TCP_PROTOCOL_VERSION};

async fn block_bytes() -> Vec<u8> {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("tag", DataType::Int32, false)])), vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();
    let mut writer = Cursor::new(Vec::new());
    batch.write_async(&mut writer, DBMS_TCP_PROTOCOL_VERSION, None, ArrowOptions::strict()).await.unwrap();
    writer.into_inner()
}

#[tokio::test]
async fn native_source_type_metadata_is_opt_in_and_exact_for_both_decoders() {
    let bytes = block_bytes().await;
    for enabled in [false, true] {
        let options = ArrowOptions::strict().with_source_type_metadata(enabled);
        let mut state = DeserializerState::<ArrowDeserializerState>::default();
        let batch = RecordBatch::read_async(&mut Cursor::new(&bytes), DBMS_TCP_PROTOCOL_VERSION, options, &mut state).await.unwrap();
        assert_eq!(batch.schema().field(0).metadata().get("clickhouse.type").map(String::as_str), enabled.then_some("Int32"));
        let mut state = DeserializerState::<ArrowDeserializerState>::default();
        let batch = RecordBatch::read(&mut Cursor::new(&bytes), DBMS_TCP_PROTOCOL_VERSION, options, &mut state).unwrap();
        assert_eq!(batch.schema().field(0).metadata().get("clickhouse.type").map(String::as_str), enabled.then_some("Int32"));
    }
}

#[tokio::test]
async fn invalid_utf8_column_identifiers_are_rejected_not_replaced() {
    let mut bytes = block_bytes().await;
    let name = bytes.windows(3).position(|window| window == b"tag").unwrap();
    bytes[name] = 0xff;
    let options = ArrowOptions::strict();
    let mut state = DeserializerState::<ArrowDeserializerState>::default();
    assert!(RecordBatch::read_async(&mut Cursor::new(&bytes), DBMS_TCP_PROTOCOL_VERSION, options, &mut state).await.is_err());
    let mut state = DeserializerState::<ArrowDeserializerState>::default();
    assert!(RecordBatch::read(&mut Cursor::new(&bytes), DBMS_TCP_PROTOCOL_VERSION, options, &mut state).is_err());
}
