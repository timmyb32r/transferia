use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// JSON Lines (NDJSON) serializer: one JSON object per row.
///
/// Output format: `{"column_name": "column_value", ...}\n`
/// This is an independent Arrow-to-NDJSON projection; standard NDJSON readers
/// can consume the output without knowing the source parser configuration.
///
/// Null values are always emitted explicitly as `"col": null`, matching the
/// Confluent S3 JSON format.
///
/// **Optimization**: Column types are pre-classified into internal writer
/// variants when the encoder is constructed. This eliminates per-value
/// `downcast_ref` overhead — the type check happens once per column,
/// not once per value.
/// A pre-classified, optionally projected view of one Arrow batch.
pub struct JsonBatchEncoder {
    columns: Vec<(String, ColumnWriter)>,
}

impl JsonBatchEncoder {
    pub fn new(
        batch: &RecordBatch,
        mut include_column: impl FnMut(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .enumerate()
            .filter(|(index, _)| include_column(*index))
            .map(|(_, (field, column))| {
                ColumnWriter::classify(field.name(), column.as_ref()).ok_or_else(|| {
                    anyhow::anyhow!(
                        "JSON serializer: unsupported type {:?} for column '{}'",
                        field.data_type(),
                        field.name(),
                    )
                })
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Self { columns })
    }

    /// Append exactly one compact JSON object followed by a newline.
    pub fn write_row(&self, row: usize, output: &mut Vec<u8>) {
        output.push(b'{');
        for (index, (name, writer)) in self.columns.iter().enumerate() {
            if index != 0 {
                output.push(b',');
            }
            write_json_string(output, name);
            output.push(b':');
            if writer.is_null_at(row) {
                output.extend_from_slice(b"null");
            } else {
                writer.write_value(output, row);
            }
        }
        output.extend_from_slice(b"}\n");
    }
}

/// Pre-classified column writer. Arrow array clones retain shared immutable
/// buffers, so the encoder is cheap to own and can outlive the `RecordBatch`
/// wrapper without copying column data.
///
/// Date and Timestamp types map to their integer representation:
///   Date32 → Int32, Date64 → Int64, Timestamp(*) → Int64.
enum ColumnWriter {
    Utf8(arrow::array::StringArray),
    LargeUtf8(arrow::array::LargeStringArray),
    Int8(arrow::array::Int8Array),
    Int16(arrow::array::Int16Array),
    Int32(arrow::array::Int32Array),
    Int64(arrow::array::Int64Array),
    UInt8(arrow::array::UInt8Array),
    UInt16(arrow::array::UInt16Array),
    UInt32(arrow::array::UInt32Array),
    UInt64(arrow::array::UInt64Array),
    Float32(arrow::array::Float32Array),
    Float64(arrow::array::Float64Array),
    Boolean(arrow::array::BooleanArray),
    Date32(arrow::array::Date32Array),
    Date64(arrow::array::Date64Array),
    TimestampSecond(arrow::array::TimestampSecondArray),
    TimestampMillisecond(arrow::array::TimestampMillisecondArray),
    TimestampMicrosecond(arrow::array::TimestampMicrosecondArray),
    TimestampNanosecond(arrow::array::TimestampNanosecondArray),
}

impl ColumnWriter {
    /// Classify an Arrow array into the appropriate writer variant.
    /// Returns `None` for unsupported types.
    fn classify(name: &str, array: &dyn Array) -> Option<(String, Self)> {
        use arrow::array::{
            BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
            UInt8Array,
        };

        let dt = array.data_type();
        let writer = match *dt {
            DataType::Utf8 => {
                let a = array.as_any().downcast_ref::<StringArray>()?;
                Self::Utf8(a.clone())
            }
            DataType::LargeUtf8 => {
                let a = array.as_any().downcast_ref::<LargeStringArray>()?;
                Self::LargeUtf8(a.clone())
            }
            DataType::Int8 => {
                let a = array.as_any().downcast_ref::<Int8Array>()?;
                Self::Int8(a.clone())
            }
            DataType::Int16 => {
                let a = array.as_any().downcast_ref::<Int16Array>()?;
                Self::Int16(a.clone())
            }
            DataType::Int32 => {
                let a = array.as_any().downcast_ref::<Int32Array>()?;
                Self::Int32(a.clone())
            }
            DataType::Int64 => {
                let a = array.as_any().downcast_ref::<Int64Array>()?;
                Self::Int64(a.clone())
            }
            DataType::Date32 => Self::Date32(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::Date32Array>()?
                    .clone(),
            ),
            DataType::Date64 => Self::Date64(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::Date64Array>()?
                    .clone(),
            ),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => Self::TimestampSecond(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()?
                    .clone(),
            ),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                Self::TimestampMillisecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMillisecondArray>()?
                        .clone(),
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                Self::TimestampMicrosecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?
                        .clone(),
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                Self::TimestampNanosecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampNanosecondArray>()?
                        .clone(),
                )
            }
            DataType::UInt8 => {
                let a = array.as_any().downcast_ref::<UInt8Array>()?;
                Self::UInt8(a.clone())
            }
            DataType::UInt16 => {
                let a = array.as_any().downcast_ref::<UInt16Array>()?;
                Self::UInt16(a.clone())
            }
            DataType::UInt32 => {
                let a = array.as_any().downcast_ref::<UInt32Array>()?;
                Self::UInt32(a.clone())
            }
            DataType::UInt64 => {
                let a = array.as_any().downcast_ref::<UInt64Array>()?;
                Self::UInt64(a.clone())
            }
            DataType::Float32 => {
                let a = array.as_any().downcast_ref::<Float32Array>()?;
                Self::Float32(a.clone())
            }
            DataType::Float64 => {
                let a = array.as_any().downcast_ref::<Float64Array>()?;
                Self::Float64(a.clone())
            }
            DataType::Boolean => {
                let a = array.as_any().downcast_ref::<BooleanArray>()?;
                Self::Boolean(a.clone())
            }
            DataType::Null
            | DataType::Float16
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8View
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(..)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Union(..)
            | DataType::Dictionary(..)
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Map(..)
            | DataType::RunEndEncoded(..) => return None,
        };
        Some((name.to_string(), writer))
    }

    /// Write the value at the given row index into the buffer.
    /// No dynamic dispatch: the variant is pre-determined.
    #[inline]
    fn write_value(&self, buf: &mut Vec<u8>, row: usize) {
        match self {
            Self::Utf8(a) => write_json_string(buf, a.value(row)),
            Self::LargeUtf8(a) => write_json_string(buf, a.value(row)),
            Self::Int8(a) => write_int(buf, a.value(row)),
            Self::Int16(a) => write_int(buf, a.value(row)),
            Self::Int32(a) => write_int(buf, a.value(row)),
            Self::Int64(a) => write_int(buf, a.value(row)),
            Self::UInt8(a) => write_uint(buf, a.value(row)),
            Self::UInt16(a) => write_uint(buf, a.value(row)),
            Self::UInt32(a) => write_uint(buf, a.value(row)),
            Self::UInt64(a) => write_uint(buf, a.value(row)),
            Self::Float32(a) => {
                write_float(buf, a.value(row));
            }
            Self::Float64(a) => {
                write_float(buf, a.value(row));
            }
            Self::Boolean(a) => {
                buf.extend_from_slice(if a.value(row) { b"true" } else { b"false" });
            }
            Self::Date32(a) => write_int(buf, a.value(row)),
            Self::Date64(a) => write_int(buf, a.value(row)),
            Self::TimestampSecond(a) => write_int(buf, a.value(row)),
            Self::TimestampMillisecond(a) => write_int(buf, a.value(row)),
            Self::TimestampMicrosecond(a) => write_int(buf, a.value(row)),
            Self::TimestampNanosecond(a) => write_int(buf, a.value(row)),
        }
    }

    /// Check if the value is null at the given row.
    #[inline]
    fn is_null_at(&self, row: usize) -> bool {
        match self {
            Self::Utf8(a) => a.is_null(row),
            Self::LargeUtf8(a) => a.is_null(row),
            Self::Int8(a) => a.is_null(row),
            Self::Int16(a) => a.is_null(row),
            Self::Int32(a) => a.is_null(row),
            Self::Int64(a) => a.is_null(row),
            Self::UInt8(a) => a.is_null(row),
            Self::UInt16(a) => a.is_null(row),
            Self::UInt32(a) => a.is_null(row),
            Self::UInt64(a) => a.is_null(row),
            Self::Float32(a) => a.is_null(row),
            Self::Float64(a) => a.is_null(row),
            Self::Boolean(a) => a.is_null(row),
            Self::Date32(a) => a.is_null(row),
            Self::Date64(a) => a.is_null(row),
            Self::TimestampSecond(a) => a.is_null(row),
            Self::TimestampMillisecond(a) => a.is_null(row),
            Self::TimestampMicrosecond(a) => a.is_null(row),
            Self::TimestampNanosecond(a) => a.is_null(row),
        }
    }
}

/// Fast integer formatting via `itoa`.
#[inline]
fn write_int<T: itoa::Integer>(buf: &mut Vec<u8>, v: T) {
    buf.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

/// Fast unsigned formatting via `itoa`.
#[inline]
fn write_uint<T: itoa::Integer>(buf: &mut Vec<u8>, v: T) {
    buf.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

#[inline]
fn write_float<T: ryu::Float>(buf: &mut Vec<u8>, value: T) {
    let mut formatter = ryu::Buffer::new();
    let formatted = formatter.format(value);
    if matches!(formatted, "NaN" | "inf" | "-inf") {
        buf.extend_from_slice(b"null");
    } else {
        buf.extend_from_slice(formatted.as_bytes());
    }
}

/// Write a JSON-escaped string (with surrounding quotes) into the buffer.
fn write_json_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for &b in s.as_bytes() {
        match b {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            0x00..=0x1F => {
                buf.extend_from_slice(format!("\\u{b:04x}").as_bytes());
            }
            _ => buf.push(b),
        }
    }
    buf.push(b'"');
}

#[cfg(test)]
mod tests;
