use std::{io::Cursor, str::FromStr};

use arrow::array::{BinaryArray, Float64Array, ListArray, MapArray, StructArray};
use arrow::record_batch::RecordBatch;

use super::{ch_to_arrow_type, normalize_type};
use crate::{ArrowOptions, Type};
use crate::arrow::{builder::TypedBuilder, deserialize::ArrowDeserializerState};
use crate::formats::{DeserializerState, protocol_data::ProtocolData};
use crate::io::ClickHouseBytesWrite;

fn block(declaration: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.put_var_uint(1).unwrap();
    bytes.put_var_uint(1).unwrap();
    bytes.put_string("value").unwrap();
    bytes.put_string(declaration).unwrap();
    bytes.extend_from_slice(payload);
    bytes
}

fn point_payload() -> Vec<u8> {
    [1.5_f64, -2.75].into_iter().flat_map(f64::to_le_bytes).collect()
}

async fn both_decoders(declaration: &str, payload: &[u8]) -> Vec<RecordBatch> {
    let bytes = block(declaration, payload);
    let options = ArrowOptions::strict().with_source_type_metadata(true);
    let mut sync_state = DeserializerState::<ArrowDeserializerState>::default();
    let sync = RecordBatch::read(&mut Cursor::new(&bytes), 0, options, &mut sync_state).unwrap();
    let mut async_state = DeserializerState::<ArrowDeserializerState>::default();
    let asynchronous = RecordBatch::read_async(&mut Cursor::new(&bytes), 0, options, &mut async_state).await.unwrap();
    for batch in [&sync, &asynchronous] {
        assert_eq!(batch.schema().field(0).metadata().get("clickhouse.type").unwrap(), declaration);
    }
    vec![sync, asynchronous]
}

fn assert_point(point: &StructArray) {
    assert_eq!(point.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 1.5);
    assert_eq!(point.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0), -2.75);
}

#[tokio::test]
async fn native_geo_aliases_decode_without_reaching_unimplemented_builders() {
    for (declaration, depth) in [("Point", 0), ("Ring", 1), ("Polygon", 2), ("MultiPolygon", 3)] {
        let mut payload = Vec::new();
        for _ in 0..depth { payload.extend_from_slice(&1_u64.to_le_bytes()); }
        payload.extend(point_payload());
        for batch in both_decoders(declaration, &payload).await {
            let mut value = batch.column(0).clone();
            for _ in 0..depth {
                value = value.as_any().downcast_ref::<ListArray>().unwrap().value(0);
            }
            assert_point(value.as_any().downcast_ref::<StructArray>().unwrap());
        }
    }
}

#[tokio::test]
async fn native_map_normalizes_binary_keys_and_nested_geo_values_recursively() {
    // One map pair; native String bytes include non-UTF-8, proving no text coercion.
    let mut payload = 1_u64.to_le_bytes().to_vec();
    payload.extend_from_slice(&[3, b'k', 0, 0xff]);
    payload.extend_from_slice(&1_u64.to_le_bytes()); // Ring with one point.
    payload.extend(point_payload());
    for batch in both_decoders("Map(String, Ring)", &payload).await {
        let map = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.keys().as_any().downcast_ref::<BinaryArray>().unwrap().value(0), &[b'k', 0, 0xff]);
        let point = map.values().as_any().downcast_ref::<ListArray>().unwrap().value(0);
        assert_point(point.as_any().downcast_ref::<StructArray>().unwrap());
    }
}

#[test]
fn recursive_normalization_keeps_low_cardinality_and_inner_nullability() {
    let original = Type::from_str("Array(Tuple(Map(String, LowCardinality(Nullable(String))), Polygon))").unwrap();
    let (data_type, _) = ch_to_arrow_type(&original, Some(ArrowOptions::strict())).unwrap();
    let normalized = normalize_type(&original, &data_type).unwrap();
    let expected = Type::Array(Box::new(Type::Tuple(vec![
        Type::Map(Box::new(Type::Binary), Box::new(Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::Binary)))))),
        crate::geo::normalize_geo_type(&Type::Polygon).unwrap(),
    ])));
    assert_eq!(normalized, expected);
    TypedBuilder::try_new(&normalized, &data_type).unwrap();
}
