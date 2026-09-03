use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::{BufMut as _, Bytes, BytesMut};

pub fn encode(batch: &RecordBatch) -> anyhow::Result<Bytes> {
    let mut output = BytesMut::with_capacity(batch.get_array_memory_size());
    for row in 0..batch.num_rows() {
        for (index, column) in batch.columns().iter().enumerate() {
            if index > 0 {
                output.put_u8(b'\t');
            }
            if column.is_null(row) {
                output.extend_from_slice(b"\\N");
            } else {
                encode_value(&mut output, column.as_ref(), row)?;
            }
        }
        output.put_u8(b'\n');
    }
    Ok(output.freeze())
}

fn encode_value(output: &mut BytesMut, column: &dyn Array, row: usize) -> anyhow::Result<()> {
    match column.data_type() {
        DataType::Boolean => output.extend_from_slice(
            if downcast::<BooleanArray>(column)?.value(row) {
                b"t"
            } else {
                b"f"
            },
        ),
        DataType::Int8 => write_postgres_char(output, downcast::<Int8Array>(column)?.value(row)),
        DataType::Int16 => write_integer(output, downcast::<Int16Array>(column)?.value(row)),
        DataType::Int32 => write_integer(output, downcast::<Int32Array>(column)?.value(row)),
        DataType::Int64 => write_integer(output, downcast::<Int64Array>(column)?.value(row)),
        DataType::UInt32 => write_integer(output, downcast::<UInt32Array>(column)?.value(row)),
        DataType::Float32 => write_f32(output, downcast::<Float32Array>(column)?.value(row)),
        DataType::Float64 => write_f64(output, downcast::<Float64Array>(column)?.value(row)),
        DataType::Utf8 => {
            let value = downcast::<StringArray>(column)?.value(row);
            anyhow::ensure!(
                !value.as_bytes().contains(&0),
                "PostgreSQL text cannot store a NUL byte"
            );
            escape_text(output, value.as_bytes());
        }
        DataType::Binary => {
            output.extend_from_slice(b"\\\\x");
            for byte in downcast::<BinaryArray>(column)?.value(row) {
                output.put_u8(hex(byte >> 4));
                output.put_u8(hex(byte & 0x0f));
            }
        }
        DataType::Date32 => {
            let days = i64::from(downcast::<Date32Array>(column)?.value(row));
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| anyhow::anyhow!("invalid Unix epoch date"))?;
            let date = epoch
                .checked_add_signed(chrono::Duration::days(days))
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL date conversion overflow"))?;
            output.extend_from_slice(date.format("%Y-%m-%d").to_string().as_bytes());
        }
        DataType::Timestamp(unit, None) => {
            let micros = timestamp_micros(column, row, unit)?;
            let seconds = micros.div_euclid(1_000_000);
            let subsecond_micros = micros.rem_euclid(1_000_000);
            let nanos = u32::try_from(subsecond_micros)?
                .checked_mul(1_000)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp conversion overflow"))?;
            let timestamp = chrono::DateTime::from_timestamp(seconds, nanos)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp is outside Arrow range"))?
                .naive_utc();
            output.extend_from_slice(
                timestamp
                    .format("%Y-%m-%d %H:%M:%S%.6f")
                    .to_string()
                    .as_bytes(),
            );
        }
        data_type => {
            anyhow::bail!("unsupported Arrow type {data_type:?} for PostgreSQL text COPY")
        }
    }
    Ok(())
}

fn write_integer<T: itoa::Integer>(output: &mut BytesMut, value: T) {
    let mut buffer = itoa::Buffer::new();
    output.extend_from_slice(buffer.format(value).as_bytes());
}

fn write_postgres_char(output: &mut BytesMut, value: i8) {
    let byte = u8::from_ne_bytes(value.to_ne_bytes());
    match byte {
        0 => {}
        1..=127 => escape_text(output, std::slice::from_ref(&byte)),
        _ => {
            output.extend_from_slice(b"\\\\");
            output.put_u8(b'0' + (byte >> 6));
            output.put_u8(b'0' + ((byte >> 3) & 7));
            output.put_u8(b'0' + (byte & 7));
        }
    }
}

fn write_f32(output: &mut BytesMut, value: f32) {
    if value.is_nan() {
        output.extend_from_slice(b"NaN");
    } else if value.is_infinite() {
        output.extend_from_slice(if value.is_sign_negative() {
            b"-Infinity"
        } else {
            b"Infinity"
        });
    } else {
        let mut buffer = ryu::Buffer::new();
        output.extend_from_slice(buffer.format(value).as_bytes());
    }
}

fn write_f64(output: &mut BytesMut, value: f64) {
    if value.is_nan() {
        output.extend_from_slice(b"NaN");
    } else if value.is_infinite() {
        output.extend_from_slice(if value.is_sign_negative() {
            b"-Infinity"
        } else {
            b"Infinity"
        });
    } else {
        let mut buffer = ryu::Buffer::new();
        output.extend_from_slice(buffer.format(value).as_bytes());
    }
}

fn escape_text(output: &mut BytesMut, value: &[u8]) {
    for byte in value {
        match byte {
            b'\\' => output.extend_from_slice(b"\\\\"),
            b'\x08' => output.extend_from_slice(b"\\b"),
            b'\x0c' => output.extend_from_slice(b"\\f"),
            b'\n' => output.extend_from_slice(b"\\n"),
            b'\r' => output.extend_from_slice(b"\\r"),
            b'\t' => output.extend_from_slice(b"\\t"),
            b'\x0b' => output.extend_from_slice(b"\\v"),
            other => output.put_u8(*other),
        }
    }
}

fn timestamp_micros(
    column: &dyn Array,
    row: usize,
    unit: &TimeUnit,
) -> anyhow::Result<i64> {
    let micros = match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(column)?
            .value(row)
            .checked_mul(1_000_000),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(column)?
            .value(row)
            .checked_mul(1_000),
        TimeUnit::Microsecond => Some(downcast::<TimestampMicrosecondArray>(column)?.value(row)),
        TimeUnit::Nanosecond => {
            let nanos = downcast::<TimestampNanosecondArray>(column)?.value(row);
            anyhow::ensure!(
                nanos.rem_euclid(1_000) == 0,
                "PostgreSQL timestamp has microsecond precision; nanosecond value {nanos} is not lossless"
            );
            Some(nanos / 1_000)
        }
    };
    micros.ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp conversion overflow"))
}

fn hex(value: u8) -> u8 {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    DIGITS[usize::from(value)]
}

fn downcast<T: Array + 'static>(column: &dyn Array) -> anyhow::Result<&T> {
    column.as_any().downcast_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Arrow array does not match declared type {:?}",
            column.data_type()
        )
    })
}
