use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};

const POSTGRES_EPOCH_DAYS: i32 = 10_957;
const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

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
        DataType::UInt64 => encode_u64_numeric(
            output,
            downcast::<UInt64Array>(column)?.value(row),
        ),
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
            anyhow::ensure!(!value.contains(&0), "PostgreSQL text cannot store a NUL byte");
            field(output, value)
        }
        DataType::Binary => field(output, downcast::<BinaryArray>(column)?.value(row)),
        DataType::Date32 => fixed(
            output,
            downcast::<Date32Array>(column)?
                .value(row)
                .checked_sub(POSTGRES_EPOCH_DAYS)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL date conversion overflow"))?
                .to_be_bytes(),
        ),
        DataType::Timestamp(unit, None) => {
            let micros = match unit {
                TimeUnit::Second => downcast::<TimestampSecondArray>(column)?
                    .value(row)
                    .checked_mul(1_000_000),
                TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(column)?
                    .value(row)
                    .checked_mul(1_000),
                TimeUnit::Microsecond => {
                    Some(downcast::<TimestampMicrosecondArray>(column)?.value(row))
                }
                TimeUnit::Nanosecond => {
                    let nanos = downcast::<TimestampNanosecondArray>(column)?.value(row);
                    anyhow::ensure!(
                        nanos.rem_euclid(1_000) == 0,
                        "PostgreSQL timestamp has microsecond precision; nanosecond value {nanos} is not lossless"
                    );
                    Some(nanos / 1_000)
                }
            }
            .and_then(|value| value.checked_sub(POSTGRES_EPOCH_MICROS))
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp conversion overflow"))?;
            fixed(output, micros.to_be_bytes())
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
    let payload_bytes = 8_usize
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
