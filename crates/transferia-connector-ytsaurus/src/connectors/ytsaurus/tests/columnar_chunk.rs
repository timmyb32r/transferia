#![allow(
    clippy::float_cmp,
    reason = "fixtures assert exact decoding of representable binary floating-point values"
)]

use arrow::array::{Array as _, Float32Array, Float64Array};

use super::*;

#[test]
fn unversioned_float_uses_its_four_byte_physical_layout() -> anyhow::Result<()> {
    let data = floating_segment_data(&[1.5_f32.to_le_bytes(), (-2.25_f32).to_le_bytes()], 0);
    let mut builder = ColumnBuilder::new(&DataType::Float32, 2)?;

    append_segment(
        &mut builder,
        &DataType::Float32,
        &segment_meta(2, data.len()),
        &data,
        0,
        2,
    )?;

    let array = builder.finish();
    let array = array
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32 builder must produce Float32Array");
    assert_eq!(array.values(), &[1.5, -2.25]);
    Ok(())
}

#[test]
fn unversioned_double_uses_its_eight_byte_physical_layout() -> anyhow::Result<()> {
    let data = floating_segment_data(&[1.5_f64.to_le_bytes(), (-2.25_f64).to_le_bytes()], 0b10);
    let mut builder = ColumnBuilder::new(&DataType::Float64, 2)?;

    append_segment(
        &mut builder,
        &DataType::Float64,
        &segment_meta(2, data.len()),
        &data,
        0,
        2,
    )?;

    let array = builder.finish();
    let array = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 builder must produce Float64Array");
    assert_eq!(array.value(0), 1.5);
    assert!(array.is_null(1));
    Ok(())
}

fn floating_segment_data<const WIDTH: usize>(values: &[[u8; WIDTH]], null_bitmap: u8) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + values.len() * WIDTH + 8);
    data.extend_from_slice(&(values.len() as u64).to_le_bytes());
    for value in values {
        data.extend_from_slice(value);
    }
    data.push(null_bitmap);
    data.extend_from_slice(&[0; 7]);
    data
}

fn segment_meta(row_count: i64, size: usize) -> SegmentMeta {
    SegmentMeta {
        version: 0,
        segment_type: 0,
        row_count,
        block_index: 0,
        offset: 0,
        chunk_row_count: row_count,
        size: i64::try_from(size).expect("test segment size fits i64"),
        integer_segment_meta: None,
        string_segment_meta: None,
    }
}
