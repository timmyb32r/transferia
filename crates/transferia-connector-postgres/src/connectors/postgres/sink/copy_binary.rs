use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};

use crate::connectors::postgres::temporal::{
    timestamp_has_timezone, timestamp_micros, unix_days_to_postgres_date,
    unix_micros_to_postgres_timestamp,
};

pub fn encode(batch: &RecordBatch) -> anyhow::Result<Bytes> {
    let estimated = batch
        .get_array_memory_size()
        .saturating_add(
            batch
                .num_rows()
                .saturating_mul(batch.num_columns().saturating_mul(6)),
        )
        .saturating_add(21);
    let mut output = BytesMut::with_capacity(estimated);
    output.extend_from_slice(b"PGCOPY\n\xFF\r\n\0");
    output.put_i32(0);
    output.put_i32(0);
    let column_count = i16::try_from(batch.num_columns())?;
    for row in 0..batch.num_rows() {
        output.put_i16(column_count);
        for column in batch.columns() {
            if column.is_null(row) {
                output.put_i32(-1);
                continue;
            }
            encode_value(&mut output, column.as_ref(), row)?;
        }
    }
    output.put_i16(-1);
    Ok(output.freeze())
}

fn field(output: &mut BytesMut, bytes: &[u8]) -> anyhow::Result<()> {
    output.put_i32(i32::try_from(bytes.len())?);
    output.extend_from_slice(bytes);
    Ok(())
}

fn fixed<const N: usize>(output: &mut BytesMut, bytes: [u8; N]) -> anyhow::Result<()> {
    field(output, &bytes)
}

fn encode_value(output: &mut BytesMut, column: &dyn Array, row: usize) -> anyhow::Result<()> {
    match column.data_type() {
        DataType::Boolean => fixed(
            output,
            [u8::from(downcast::<BooleanArray>(column)?.value(row))],
        ),
        DataType::Int8 => fixed(
            output,
            downcast::<Int8Array>(column)?.value(row).to_be_bytes(),
        ),
        DataType::Int16 => fixed(
            output,
            downcast::<Int16Array>(column)?.value(row).to_be_bytes(),
        ),
        DataType::Int32 => fixed(
            output,
            downcast::<Int32Array>(column)?.value(row).to_be_bytes(),
        ),
        DataType::Int64 => fixed(
            output,
            downcast::<Int64Array>(column)?.value(row).to_be_bytes(),
        ),
        DataType::UInt8 => fixed(
            output,
            i16::from(downcast::<UInt8Array>(column)?.value(row)).to_be_bytes(),
        ),
        DataType::UInt16 => fixed(
            output,
            i32::from(downcast::<UInt16Array>(column)?.value(row)).to_be_bytes(),
        ),
        DataType::UInt32 => fixed(
            output,
            downcast::<UInt32Array>(column)?.value(row).to_be_bytes(),
        ),
        DataType::UInt64 => encode_u64_numeric(output, downcast::<UInt64Array>(column)?.value(row)),
        DataType::Float32 => fixed(
            output,
            downcast::<Float32Array>(column)?
                .value(row)
                .to_bits()
                .to_be_bytes(),
        ),
        DataType::Float64 => fixed(
            output,
            downcast::<Float64Array>(column)?
                .value(row)
                .to_bits()
                .to_be_bytes(),
        ),
        DataType::Utf8 => {
            let value = downcast::<StringArray>(column)?.value(row).as_bytes();
            anyhow::ensure!(
                !value.contains(&0),
                "PostgreSQL text cannot store a NUL byte"
            );
            field(output, value)
        }
        DataType::Binary => field(output, downcast::<BinaryArray>(column)?.value(row)),
        DataType::Date32 => fixed(
            output,
            unix_days_to_postgres_date(downcast::<Date32Array>(column)?.value(row))?.to_be_bytes(),
        ),
        DataType::Timestamp(unit, _) => {
            timestamp_has_timezone(column.data_type())?;
            let micros = timestamp_micros(column, row, *unit)?;
            fixed(
                output,
                unix_micros_to_postgres_timestamp(micros)?.to_be_bytes(),
            )
        }
        data_type => {
            anyhow::bail!("unsupported Arrow type {data_type:?} for PostgreSQL binary COPY")
        }
    }
}

fn encode_u64_numeric(output: &mut BytesMut, mut value: u64) -> anyhow::Result<()> {
    const BASE: u64 = 10_000;
    let mut reversed = [0_i16; 5];
    let mut digits = 0_usize;
    while value > 0 {
        reversed[digits] = i16::try_from(value % BASE)?;
        value /= BASE;
        digits += 1;
    }
    let payload_bytes =
        8_usize
            .checked_add(digits.checked_mul(2).ok_or_else(|| {
                anyhow::anyhow!("PostgreSQL numeric binary payload length overflow")
            })?)
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL numeric binary payload length overflow"))?;
    output.put_i32(i32::try_from(payload_bytes)?);
    output.put_i16(i16::try_from(digits)?);
    output.put_i16(if digits == 0 {
        0
    } else {
        i16::try_from(digits - 1)?
    });
    output.put_i16(0);
    output.put_i16(0);
    for digit in reversed[..digits].iter().rev() {
        output.put_i16(*digit);
    }
    Ok(())
}

fn downcast<T: Array + 'static>(column: &dyn Array) -> anyhow::Result<&T> {
    column.as_any().downcast_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Arrow array does not match declared type {:?}",
            column.data_type()
        )
    })
}
