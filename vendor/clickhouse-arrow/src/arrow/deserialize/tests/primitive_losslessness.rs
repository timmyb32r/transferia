use std::io::Cursor;

use arrow::array::{Array, Decimal256Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, i256};

use super::{TypedBuilder, deserialize_async, primitive};
use crate::Type;
use crate::arrow::ch_to_arrow_type;
use crate::arrow::deserialize::ClickHouseArrowDeserializer;
use crate::io::ClickHouseBytesRead;

async fn decode_datetime64(
    precision: usize,
    ticks: &[i64],
    nulls: &[u8],
) -> crate::Result<arrow::array::ArrayRef> {
    let type_hint = Type::DateTime64(precision, chrono_tz::Europe::Moscow);
    let (data_type, _) = ch_to_arrow_type(&type_hint, None)?;
    let mut builder = TypedBuilder::try_new(&type_hint, &data_type)?;
    let bytes = ticks.iter().flat_map(|value| value.to_le_bytes()).collect::<Vec<_>>();
    deserialize_async(
        &type_hint,
        &mut builder,
        &mut Cursor::new(bytes),
        ticks.len(),
        nulls,
        &mut Vec::new(),
    )
    .await
}

#[tokio::test]
async fn datetime64_wire_ticks_preserve_every_precision_and_timezone() {
    for precision in 0..=9 {
        let multiplier = match precision {
            1 | 4 | 7 => 100,
            2 | 5 | 8 => 10,
            _ => 1,
        };
        let decoded = decode_datetime64(precision, &[123, -123, 0], &[]).await.unwrap();
        assert_eq!(
            decoded.data_type(),
            &ch_to_arrow_type(&Type::DateTime64(precision, chrono_tz::Europe::Moscow), None)
                .unwrap()
                .0,
        );
        let integer_ticks = cast(&decoded, &DataType::Int64).unwrap();
        assert_eq!(
            integer_ticks.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![123 * multiplier, -123 * multiplier, 0]),
            "DateTime64({precision})",
        );
    }
}

#[tokio::test]
async fn datetime64_rescaling_rejects_positive_and_negative_overflow() {
    for precision in [1, 2, 4, 5, 7, 8] {
        for value in [i64::MAX, i64::MIN] {
            let error = decode_datetime64(precision, &[7, value], &[]).await.unwrap_err();
            let message = error.to_string();
            assert!(message.contains("overflow"), "{message}");
            assert!(message.contains("row 1"), "{message}");
            assert!(message.contains(&format!("DateTime64({precision})")), "{message}");
        }
    }
}

#[tokio::test]
async fn datetime64_null_payload_does_not_overflow_or_lose_row_alignment() {
    for precision in [1, 2, 4, 5, 7, 8] {
        let multiplier = if precision % 3 == 1 { 100 } else { 10 };
        let decoded = decode_datetime64(precision, &[7, i64::MAX, -7], &[0, 1, 0])
            .await
            .unwrap();
        let integer_ticks = cast(&decoded, &DataType::Int64).unwrap();
        assert_eq!(
            integer_ticks.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(7 * multiplier), None, Some(-7 * multiplier)]),
        );
    }
}

#[tokio::test]
async fn datetime64_rescaling_keeps_representable_i64_boundaries() {
    for precision in 0..=9 {
        let multiplier = match precision {
            1 | 4 | 7 => 100,
            2 | 5 | 8 => 10,
            _ => 1,
        };
        let ticks = [i64::MAX / multiplier, i64::MIN / multiplier];
        let decoded = decode_datetime64(precision, &ticks, &[]).await.unwrap();
        let integer_ticks = cast(&decoded, &DataType::Int64).unwrap();
        assert_eq!(
            integer_ticks.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![ticks[0] * multiplier, ticks[1] * multiplier]),
        );
    }
}

fn decimal256_values() -> Vec<i256> {
    let mut wide_bytes = [0_u8; 32];
    wide_bytes[0] = 0x5a;
    wide_bytes[16] = 0x91;
    wide_bytes[31] = 1;
    vec![i256::from(123), i256::from(-456), i256::from_le_bytes(wide_bytes)]
}

#[tokio::test]
async fn decimal256_decodes_signed_wide_little_endian_wire_values() {
    let values = decimal256_values();
    let bytes = values.iter().flat_map(|value| value.to_le_bytes()).collect::<Vec<_>>();
    let type_hint = Type::Decimal256(17);
    let data_type = DataType::Decimal256(76, 17);
    let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
    let decoded = deserialize_async(
        &type_hint,
        &mut builder,
        &mut Cursor::new(bytes),
        values.len(),
        &[],
        &mut Vec::new(),
    )
    .await
    .unwrap();
    let expected = Decimal256Array::from(values).with_precision_and_scale(76, 17).unwrap();
    assert_eq!(decoded.as_any().downcast_ref::<Decimal256Array>().unwrap(), &expected);
}

#[test]
fn decimal256_sync_primitive_uses_the_same_little_endian_wire_contract() {
    for value in decimal256_values() {
        let bytes = value.to_le_bytes();
        let mut reader = bytes.as_slice();
        let mut read = || -> crate::Result<i256> { Ok(primitive!(Decimal256 => reader)) };
        assert_eq!(read().unwrap(), value);
    }
}

fn decode_datetime64_sync(
    precision: usize,
    ticks: &[i64],
) -> crate::Result<arrow::array::ArrayRef> {
    let type_hint = Type::DateTime64(precision, chrono_tz::UTC);
    let (data_type, _) = ch_to_arrow_type(&type_hint, None)?;
    let mut builder = TypedBuilder::try_new(&type_hint, &data_type)?;
    let bytes = ticks.iter().flat_map(|value| value.to_le_bytes()).collect::<Vec<_>>();
    type_hint.deserialize_arrow(
        &mut builder,
        &mut bytes.as_slice(),
        &data_type,
        ticks.len(),
        &[],
        &mut Vec::new(),
    )
}

#[test]
fn datetime64_sync_decodes_all_wire_precisions_losslessly() {
    for precision in 0..=9 {
        let multiplier = match precision {
            1 | 4 | 7 => 100,
            2 | 5 | 8 => 10,
            _ => 1,
        };
        let decoded = decode_datetime64_sync(precision, &[123, -123, 0]).unwrap();
        let integer_ticks = cast(&decoded, &DataType::Int64).unwrap();
        assert_eq!(
            integer_ticks.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![123 * multiplier, -123 * multiplier, 0]),
        );
    }
}

#[test]
fn datetime64_sync_rescaling_rejects_overflow() {
    for precision in [1, 2, 4, 5, 7, 8] {
        for value in [i64::MAX, i64::MIN] {
            let error = decode_datetime64_sync(precision, &[7, value]).unwrap_err();
            assert!(error.to_string().contains("overflow"));
            assert!(error.to_string().contains("row 1"));
        }
    }
}

#[tokio::test]
async fn datetime64_empty_native_blocks_do_not_require_aligned_storage() {
    for precision in 0..=9 {
        assert!(decode_datetime64(precision, &[], &[]).await.unwrap().is_empty());
        assert!(decode_datetime64_sync(precision, &[]).unwrap().is_empty());
    }
}
