use arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
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
        DataType::Utf8 => field(
            output,
            downcast::<StringArray>(column)?.value(row).as_bytes(),
        ),
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
                    Some(downcast::<TimestampNanosecondArray>(column)?.value(row) / 1_000)
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

fn downcast<T: Array + 'static>(column: &dyn Array) -> anyhow::Result<&T> {
    column.as_any().downcast_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Arrow array does not match declared type {:?}",
            column.data_type()
        )
    })
}
