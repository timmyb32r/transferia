use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;

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
    columns: Vec<JsonColumnWriter>,

    non_finite_floats: NonFiniteFloatEncoding,
}

#[derive(Clone, Copy)]
enum NonFiniteFloatEncoding {
    Null,
    ProtobufJsonString,
}

pub(crate) struct JsonColumnProjection {
    pub(crate) output_name: String,

    pub(crate) source_index: Option<usize>,
}

struct JsonColumnWriter {
    source_index: Option<usize>,
    output_name: String,
    writer: ColumnWriter,
}

impl JsonBatchEncoder {
    pub fn new(
        batch: &RecordBatch,
        mut include_column: impl FnMut(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let projection = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| include_column(*index))
            .map(|(index, field)| JsonColumnProjection {
                output_name: field.name().to_owned(),
                source_index: Some(index),
            })
            .collect::<Vec<_>>();
        Self::projected(batch, projection)
    }

    pub(crate) fn projected(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
    ) -> anyhow::Result<Self> {
        Self::projected_with_float_encoding(batch, projection, NonFiniteFloatEncoding::Null)
    }

    pub(crate) fn projected_debezium(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
    ) -> anyhow::Result<Self> {
        Self::projected_with_float_encoding(
            batch,
            projection,
            NonFiniteFloatEncoding::ProtobufJsonString,
        )
    }

    fn projected_with_float_encoding(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
        non_finite_floats: NonFiniteFloatEncoding,
    ) -> anyhow::Result<Self> {
        let schema = batch.schema();
        let columns = projection
            .into_iter()
            .map(|projection| {
                let writer = if let Some(index) = projection.source_index {
                    let field = schema.field(index);
                    ColumnWriter::classify(batch.column(index).as_ref()).ok_or_else(|| {
                        anyhow::anyhow!(
                            "JSON serializer: unsupported type {:?} for column '{}'",
                            field.data_type(),
                            field.name(),
                        )
                    })?
                } else {
                    ColumnWriter::Null
                };
                Ok(JsonColumnWriter {
                    source_index: projection.source_index,
                    output_name: projection.output_name,
                    writer,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            columns,
            non_finite_floats,
        })
    }

    /// Append exactly one compact JSON object followed by a newline.
    pub fn write_row(&self, row: usize, output: &mut Vec<u8>) {
        self.write_object(row, output);
        output.push(b'\n');
    }

    pub(crate) fn write_object(&self, row: usize, output: &mut Vec<u8>) {
        self.write_object_with(row, output, |_, _, _| false);
    }

    pub(crate) fn write_object_with(
        &self,
        row: usize,
        output: &mut Vec<u8>,
        mut override_value: impl FnMut(Option<usize>, &str, &mut Vec<u8>) -> bool,
    ) {
        output.push(b'{');
        for (index, column) in self.columns.iter().enumerate() {
            if index != 0 {
                output.push(b',');
            }
            write_json_string(output, &column.output_name);
            output.push(b':');
            if override_value(column.source_index, &column.output_name, output) {
                continue;
            }
            if column.writer.is_null_at(row) {
                output.extend_from_slice(b"null");
            } else {
                column
                    .writer
                    .write_value(output, row, self.non_finite_floats);
            }
        }
        output.push(b'}');
    }

    pub(crate) fn row_equals(&self, other: &Self, row: usize) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(&other.columns)
                .all(|(left, right)| {
                    left.output_name == right.output_name
                        && left.writer.value_equals(&right.writer, row)
                })
    }
}

/// Pre-classified column writer. Arrow array clones retain shared immutable
/// buffers, so the encoder is cheap to own and can outlive the `RecordBatch`
/// wrapper without copying column data.
///
/// Date and Timestamp types map to their integer representation:
///   Date32 → Int32, Date64 → Int64, Timestamp(*) → Int64.
enum ColumnWriter {
    Null,
    Utf8(arrow::array::StringArray),
    LargeUtf8(arrow::array::LargeStringArray),
    Binary(arrow::array::BinaryArray),
    LargeBinary(arrow::array::LargeBinaryArray),
    FixedSizeBinary(arrow::array::FixedSizeBinaryArray),
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
    fn classify(array: &dyn Array) -> Option<Self> {
        use arrow::array::{
            BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
            Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
            StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
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
            DataType::Binary => {
                let a = array.as_any().downcast_ref::<BinaryArray>()?;
                Self::Binary(a.clone())
            }
            DataType::LargeBinary => {
                let a = array.as_any().downcast_ref::<LargeBinaryArray>()?;
                Self::LargeBinary(a.clone())
            }
            DataType::FixedSizeBinary(_) => {
                let a = array.as_any().downcast_ref::<FixedSizeBinaryArray>()?;
                Self::FixedSizeBinary(a.clone())
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
        Some(writer)
    }

    /// Write the value at the given row index into the buffer.
    /// No dynamic dispatch: the variant is pre-determined.
    #[inline]
    fn write_value(
        &self,
        buf: &mut Vec<u8>,
        row: usize,
        non_finite_floats: NonFiniteFloatEncoding,
    ) {
        match self {
            Self::Null => buf.extend_from_slice(b"null"),
            Self::Utf8(a) => write_json_string(buf, a.value(row)),
            Self::LargeUtf8(a) => write_json_string(buf, a.value(row)),
            Self::Binary(a) => write_base64(buf, a.value(row)),
            Self::LargeBinary(a) => write_base64(buf, a.value(row)),
            Self::FixedSizeBinary(a) => write_base64(buf, a.value(row)),
            Self::Int8(a) => write_int(buf, a.value(row)),
            Self::Int16(a) => write_int(buf, a.value(row)),
            Self::Int32(a) => write_int(buf, a.value(row)),
            Self::Int64(a) => write_int(buf, a.value(row)),
            Self::UInt8(a) => write_uint(buf, a.value(row)),
            Self::UInt16(a) => write_uint(buf, a.value(row)),
            Self::UInt32(a) => write_uint(buf, a.value(row)),
            Self::UInt64(a) => write_uint(buf, a.value(row)),
            Self::Float32(a) => {
                write_float(buf, a.value(row), non_finite_floats);
            }
            Self::Float64(a) => {
                write_float(buf, a.value(row), non_finite_floats);
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
            Self::Null => true,
            Self::Utf8(a) => a.is_null(row),
            Self::LargeUtf8(a) => a.is_null(row),
            Self::Binary(a) => a.is_null(row),
            Self::LargeBinary(a) => a.is_null(row),
            Self::FixedSizeBinary(a) => a.is_null(row),
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

    fn value_equals(&self, other: &Self, row: usize) -> bool {
        if self.is_null_at(row) || other.is_null_at(row) {
            return self.is_null_at(row) == other.is_null_at(row);
        }
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Utf8(left), Self::Utf8(right)) => left.value(row) == right.value(row),
            (Self::LargeUtf8(left), Self::LargeUtf8(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::Binary(left), Self::Binary(right)) => left.value(row) == right.value(row),
            (Self::LargeBinary(left), Self::LargeBinary(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::FixedSizeBinary(left), Self::FixedSizeBinary(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::Int8(left), Self::Int8(right)) => left.value(row) == right.value(row),
            (Self::Int16(left), Self::Int16(right)) => left.value(row) == right.value(row),
            (Self::Int32(left), Self::Int32(right)) => left.value(row) == right.value(row),
            (Self::Int64(left), Self::Int64(right)) => left.value(row) == right.value(row),
            (Self::UInt8(left), Self::UInt8(right)) => left.value(row) == right.value(row),
            (Self::UInt16(left), Self::UInt16(right)) => left.value(row) == right.value(row),
            (Self::UInt32(left), Self::UInt32(right)) => left.value(row) == right.value(row),
            (Self::UInt64(left), Self::UInt64(right)) => left.value(row) == right.value(row),
            (Self::Float32(left), Self::Float32(right)) => {
                left.value(row).to_bits() == right.value(row).to_bits()
            }
            (Self::Float64(left), Self::Float64(right)) => {
                left.value(row).to_bits() == right.value(row).to_bits()
            }
            (Self::Boolean(left), Self::Boolean(right)) => left.value(row) == right.value(row),
            (Self::Date32(left), Self::Date32(right)) => left.value(row) == right.value(row),
            (Self::Date64(left), Self::Date64(right)) => left.value(row) == right.value(row),
            (Self::TimestampSecond(left), Self::TimestampSecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::TimestampMillisecond(left), Self::TimestampMillisecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::TimestampMicrosecond(left), Self::TimestampMicrosecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::TimestampNanosecond(left), Self::TimestampNanosecond(right)) => {
                left.value(row) == right.value(row)
            }
            _ => false,
        }
    }
}

fn write_base64(buf: &mut Vec<u8>, value: &[u8]) {
    buf.push(b'"');
    let encoded_len = base64::encoded_len(value.len(), true)
        .expect("base64 length cannot overflow for an allocated Arrow value");
    let start = buf.len();
    buf.resize(start + encoded_len, 0);
    let written = base64::engine::general_purpose::STANDARD
        .encode_slice(value, &mut buf[start..])
        .expect("preallocated base64 output has the exact encoded length");
    debug_assert_eq!(written, encoded_len);
    buf.push(b'"');
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
fn write_float<T: ryu::Float>(
    buf: &mut Vec<u8>,
    value: T,
    non_finite_floats: NonFiniteFloatEncoding,
) {
    let mut formatter = ryu::Buffer::new();
    let formatted = formatter.format(value);
    match formatted {
        "NaN" => match non_finite_floats {
            NonFiniteFloatEncoding::Null => buf.extend_from_slice(b"null"),
            NonFiniteFloatEncoding::ProtobufJsonString => buf.extend_from_slice(b"\"NaN\""),
        },
        "inf" => match non_finite_floats {
            NonFiniteFloatEncoding::Null => buf.extend_from_slice(b"null"),
            NonFiniteFloatEncoding::ProtobufJsonString => {
                buf.extend_from_slice(b"\"Infinity\"")
            }
        },
        "-inf" => match non_finite_floats {
            NonFiniteFloatEncoding::Null => buf.extend_from_slice(b"null"),
            NonFiniteFloatEncoding::ProtobufJsonString => {
                buf.extend_from_slice(b"\"-Infinity\"")
            }
        },
        finite => buf.extend_from_slice(finite.as_bytes()),
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
#[path = "tests/json_serializer.rs"]
mod tests;
