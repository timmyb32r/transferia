use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array};
use arrow::datatypes::{DataType, i256};

use super::{serialize, serialize_async};
use crate::Type;
use crate::arrow::builder::TypedBuilder;
use crate::arrow::deserialize::ClickHouseArrowDeserializer;

fn signed_wide_values() -> Vec<i256> {
    let mut wide = [0_u8; 32];
    wide[0] = 0x5a;
    wide[16] = 0x91;
    wide[31] = 1;
    vec![i256::from(123_456), i256::from(-987_654), i256::from_le_bytes(wide)]
}

async fn assert_wire_and_roundtrip(column: ArrayRef, values: &[i256]) {
    let expected = values.iter().flat_map(|value| value.to_le_bytes()).collect::<Vec<_>>();
    let mut sync_wire = Vec::new();
    serialize(&Type::Decimal256(17), &mut sync_wire, &column, column.data_type()).unwrap();
    let mut async_wire = Vec::new();
    serialize_async(&Type::Decimal256(17), &mut async_wire, &column, column.data_type()).await.unwrap();
    assert_eq!(sync_wire, expected, "sync encoder must emit native little-endian decimal limbs");
    assert_eq!(async_wire, expected, "async encoder must emit native little-endian decimal limbs");

    let type_hint = Type::Decimal256(17);
    let data_type = DataType::Decimal256(76, 17);
    let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
    let decoded = type_hint.deserialize_arrow(
        &mut builder, &mut sync_wire.as_slice(), &data_type, values.len(), &[], &mut Vec::new(),
    ).unwrap();
    let expected_array = Decimal256Array::from(values.to_vec()).with_precision_and_scale(76, 17).unwrap();
    assert_eq!(decoded.as_any().downcast_ref::<Decimal256Array>().unwrap(), &expected_array);
}

#[tokio::test]
async fn decimal256_signed_wide_values_roundtrip_without_endian_reinterpretation() {
    let values = signed_wide_values();
    let column = Arc::new(Decimal256Array::from(values.clone()).with_precision_and_scale(76, 17).unwrap()) as ArrayRef;
    assert_wire_and_roundtrip(column, &values).await;
}

#[tokio::test]
async fn decimal128_widening_sign_extends_native_decimal256_little_endian() {
    let values = [123_456_i128, -987_654, 0, 10_i128.pow(37), -10_i128.pow(37)];
    let column = Arc::new(Decimal128Array::from(values.to_vec()).with_precision_and_scale(38, 17).unwrap()) as ArrayRef;
    let wide = values.into_iter().map(i256::from_i128).collect::<Vec<_>>();
    assert_wire_and_roundtrip(column, &wide).await;
}
