use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use super::Serializer;

/// JSON Lines (NDJSON) serializer: one JSON object per row.
///
/// Output format: `{"column_name": "column_value", ...}\n`
/// This is the exact inverse of the JSON parser — the output can be
/// read back by the S3 source or YDS source without modification.
///
/// Null values are always emitted explicitly as `"col": null`, matching the
/// Confluent S3 JSON format.
///
/// **Optimization**: Column types are pre-classified into [`ColumnWriter`]
/// variants during the first serialization. This eliminates per-value
/// `downcast_ref` overhead — the type check happens once per column,
/// not once per value.
#[derive(Default)]
pub struct JsonSerializer;

/// A pre-classified, optionally projected view of one Arrow batch.
pub struct JsonBatchEncoder<'batch> {
    columns: Vec<(String, ColumnWriter<'batch>)>,
}

impl<'batch> JsonBatchEncoder<'batch> {
    pub fn new(
        batch: &'batch RecordBatch,
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

/// Pre-classified column writer: holds a typed reference to the Arrow array
/// so we never need `downcast_ref` during the value-writing loop.
///
/// Date and Timestamp types map to their integer representation:
///   Date32 → Int32, Date64 → Int64, Timestamp(*) → Int64.
enum ColumnWriter<'array> {
    Utf8(&'array arrow::array::StringArray),
    LargeUtf8(&'array arrow::array::LargeStringArray),
    Int8(&'array arrow::array::Int8Array),
    Int16(&'array arrow::array::Int16Array),
    Int32(&'array arrow::array::Int32Array),
    Int64(&'array arrow::array::Int64Array),
    UInt8(&'array arrow::array::UInt8Array),
    UInt16(&'array arrow::array::UInt16Array),
    UInt32(&'array arrow::array::UInt32Array),
    UInt64(&'array arrow::array::UInt64Array),
    Float32(&'array arrow::array::Float32Array),
    Float64(&'array arrow::array::Float64Array),
    Boolean(&'array arrow::array::BooleanArray),
    Date32(&'array arrow::array::Date32Array),
    Date64(&'array arrow::array::Date64Array),
    TimestampSecond(&'array arrow::array::TimestampSecondArray),
    TimestampMillisecond(&'array arrow::array::TimestampMillisecondArray),
    TimestampMicrosecond(&'array arrow::array::TimestampMicrosecondArray),
    TimestampNanosecond(&'array arrow::array::TimestampNanosecondArray),
}

impl<'array> ColumnWriter<'array> {
    /// Classify an Arrow array into the appropriate writer variant.
    /// Returns `None` for unsupported types.
    fn classify(name: &str, array: &'array dyn Array) -> Option<(String, Self)> {
        use arrow::array::{
            BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
            UInt8Array,
        };

        let dt = array.data_type();
        let writer = match *dt {
            DataType::Utf8 => {
                let a = array.as_any().downcast_ref::<StringArray>()?;
                ColumnWriter::Utf8(a)
            }
            DataType::LargeUtf8 => {
                let a = array.as_any().downcast_ref::<LargeStringArray>()?;
                ColumnWriter::LargeUtf8(a)
            }
            DataType::Int8 => {
                let a = array.as_any().downcast_ref::<Int8Array>()?;
                ColumnWriter::Int8(a)
            }
            DataType::Int16 => {
                let a = array.as_any().downcast_ref::<Int16Array>()?;
                ColumnWriter::Int16(a)
            }
            DataType::Int32 => {
                let a = array.as_any().downcast_ref::<Int32Array>()?;
                ColumnWriter::Int32(a)
            }
            DataType::Int64 => {
                let a = array.as_any().downcast_ref::<Int64Array>()?;
                ColumnWriter::Int64(a)
            }
            DataType::Date32 => {
                ColumnWriter::Date32(array.as_any().downcast_ref::<arrow::array::Date32Array>()?)
            }
            DataType::Date64 => {
                ColumnWriter::Date64(array.as_any().downcast_ref::<arrow::array::Date64Array>()?)
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                ColumnWriter::TimestampSecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampSecondArray>()?,
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                ColumnWriter::TimestampMillisecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMillisecondArray>()?,
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                ColumnWriter::TimestampMicrosecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?,
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                ColumnWriter::TimestampNanosecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampNanosecondArray>()?,
                )
            }
            DataType::UInt8 => {
                let a = array.as_any().downcast_ref::<UInt8Array>()?;
                ColumnWriter::UInt8(a)
            }
            DataType::UInt16 => {
                let a = array.as_any().downcast_ref::<UInt16Array>()?;
                ColumnWriter::UInt16(a)
            }
            DataType::UInt32 => {
                let a = array.as_any().downcast_ref::<UInt32Array>()?;
                ColumnWriter::UInt32(a)
            }
            DataType::UInt64 => {
                let a = array.as_any().downcast_ref::<UInt64Array>()?;
                ColumnWriter::UInt64(a)
            }
            DataType::Float32 => {
                let a = array.as_any().downcast_ref::<Float32Array>()?;
                ColumnWriter::Float32(a)
            }
            DataType::Float64 => {
                let a = array.as_any().downcast_ref::<Float64Array>()?;
                ColumnWriter::Float64(a)
            }
            DataType::Boolean => {
                let a = array.as_any().downcast_ref::<BooleanArray>()?;
                ColumnWriter::Boolean(a)
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
            ColumnWriter::Utf8(a) => write_json_string(buf, a.value(row)),
            ColumnWriter::LargeUtf8(a) => write_json_string(buf, a.value(row)),
            ColumnWriter::Int8(a) => write_int(buf, a.value(row)),
            ColumnWriter::Int16(a) => write_int(buf, a.value(row)),
            ColumnWriter::Int32(a) => write_int(buf, a.value(row)),
            ColumnWriter::Int64(a) => write_int(buf, a.value(row)),
            ColumnWriter::UInt8(a) => write_uint(buf, a.value(row)),
            ColumnWriter::UInt16(a) => write_uint(buf, a.value(row)),
            ColumnWriter::UInt32(a) => write_uint(buf, a.value(row)),
            ColumnWriter::UInt64(a) => write_uint(buf, a.value(row)),
            ColumnWriter::Float32(a) => {
                buf.extend_from_slice(ryu::Buffer::new().format(a.value(row)).as_bytes());
            }
            ColumnWriter::Float64(a) => {
                buf.extend_from_slice(ryu::Buffer::new().format(a.value(row)).as_bytes());
            }
            ColumnWriter::Boolean(a) => {
                buf.extend_from_slice(if a.value(row) { b"true" } else { b"false" });
            }
            ColumnWriter::Date32(a) => write_int(buf, a.value(row)),
            ColumnWriter::Date64(a) => write_int(buf, a.value(row)),
            ColumnWriter::TimestampSecond(a) => write_int(buf, a.value(row)),
            ColumnWriter::TimestampMillisecond(a) => write_int(buf, a.value(row)),
            ColumnWriter::TimestampMicrosecond(a) => write_int(buf, a.value(row)),
            ColumnWriter::TimestampNanosecond(a) => write_int(buf, a.value(row)),
        }
    }

    /// Check if the value is null at the given row.
    #[inline]
    fn is_null_at(&self, row: usize) -> bool {
        match self {
            ColumnWriter::Utf8(a) => a.is_null(row),
            ColumnWriter::LargeUtf8(a) => a.is_null(row),
            ColumnWriter::Int8(a) => a.is_null(row),
            ColumnWriter::Int16(a) => a.is_null(row),
            ColumnWriter::Int32(a) => a.is_null(row),
            ColumnWriter::Int64(a) => a.is_null(row),
            ColumnWriter::UInt8(a) => a.is_null(row),
            ColumnWriter::UInt16(a) => a.is_null(row),
            ColumnWriter::UInt32(a) => a.is_null(row),
            ColumnWriter::UInt64(a) => a.is_null(row),
            ColumnWriter::Float32(a) => a.is_null(row),
            ColumnWriter::Float64(a) => a.is_null(row),
            ColumnWriter::Boolean(a) => a.is_null(row),
            ColumnWriter::Date32(a) => a.is_null(row),
            ColumnWriter::Date64(a) => a.is_null(row),
            ColumnWriter::TimestampSecond(a) => a.is_null(row),
            ColumnWriter::TimestampMillisecond(a) => a.is_null(row),
            ColumnWriter::TimestampMicrosecond(a) => a.is_null(row),
            ColumnWriter::TimestampNanosecond(a) => a.is_null(row),
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

impl Serializer for JsonSerializer {
    fn serialize_batch(&self, batch: &RecordBatch) -> anyhow::Result<Bytes> {
        let encoder = JsonBatchEncoder::new(batch, |_| true)?;
        let num_rows = batch.num_rows();
        let num_cols = encoder.columns.len();

        // Estimate buffer size: each row has JSON overhead + values.
        // 2 = `{` + `}`, N-1 commas, plus per-column overhead.
        let est_per_row = 2 + num_cols.saturating_sub(1) + num_cols * 24;
        let mut buf = Vec::with_capacity(num_rows * est_per_row.max(64));

        for row in 0..num_rows {
            encoder.write_row(row, &mut buf);
        }

        Ok(Bytes::from(buf))
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
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::parsers::Parser as _;

    #[test]
    fn serialize_simple_batch() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let id_arr = Int64Array::from(vec![1, 2, 3]);
        let mut name_arr = StringBuilder::with_capacity(3, 64);
        name_arr.append_value("Alice");
        name_arr.append_value("Bob");
        name_arr.append_value("Charlie");
        let bool_arr = BooleanArray::from(vec![true, false, true]);
        let floats: Vec<f64> = vec![1.5, 2.5, 3.5];
        let float_arr = Float64Array::from(floats);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_arr),
                Arc::new(name_arr.finish()),
                Arc::new(bool_arr),
                Arc::new(float_arr),
            ],
        )?;

        let serializer = JsonSerializer;
        let output = serializer.serialize_batch(&batch)?;
        let text = String::from_utf8(output.to_vec())?;

        let lines: Vec<&str> = text.lines().collect();
        anyhow::ensure!(lines.len() == 3, "3 rows \u{2192} 3 JSON lines");

        for line in &lines {
            let val: serde_json::Value = serde_json::from_str(line)?;
            anyhow::ensure!(val.get("id").is_some(), "id missing in {val}");
            anyhow::ensure!(val.get("name").is_some(), "name missing in {val}");
        }
        Ok(())
    }

    #[test]
    fn serialize_with_nulls_default() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Utf8, true),
        ]));
        let x_arr = Int64Array::from(vec![1, 2]);
        let mut y_builder = StringBuilder::with_capacity(2, 32);
        y_builder.append_value("hello");
        y_builder.append_null();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(x_arr), Arc::new(y_builder.finish())])?;

        let serializer = JsonSerializer;
        let output = serializer.serialize_batch(&batch)?;
        let text = String::from_utf8(output.to_vec())?;

        let lines: Vec<&str> = text.lines().collect();
        anyhow::ensure!(lines.len() == 2, "expected 2 lines, got {}", lines.len());

        let row2: serde_json::Value = serde_json::from_str(lines[1])?;
        anyhow::ensure!(
            row2.get("y").is_some(),
            "null column should be present as \"y\": null"
        );
        anyhow::ensure!(row2["y"].is_null(), "y should be null");
        Ok(())
    }

    #[test]
    fn roundtrip_json_parser_compatible() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let id_arr = Int64Array::from(vec![10, 20]);
        let mut val_builder = StringBuilder::with_capacity(2, 32);
        val_builder.append_value("foo");
        val_builder.append_value("bar");

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_arr), Arc::new(val_builder.finish())],
        )?;

        let serializer = JsonSerializer;
        let output = serializer.serialize_batch(&batch)?;

        let parser_config = crate::parsers::json_parser::JsonParserConfig {
            columns: vec![
                crate::parsers::json_parser::ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Int64".into(),
                    nullable: false,
                },
                crate::parsers::json_parser::ColumnMapping {
                    jsonpath: "$.val".into(),
                    column_name: "val".into(),
                    arrow_type: "Utf8".into(),
                    nullable: true,
                },
            ],
            chunk_splitter: crate::parsers::json_parser::ChunkSplitter::NewLine,
        };

        let parser = crate::parsers::json_parser::JsonParser::new(
            &parser_config,
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )?;
        let mut ws = crate::parsers::json_parser::ParserWorkspace::new();
        let msgs = vec![crate::types::message::Message::new(output)];

        let (good, _dlq) = parser.parse_into(msgs, 0, &mut ws)?;
        anyhow::ensure!(
            good.batch.num_rows() == 2,
            "roundtrip: 2 rows in \u{2192} 2 rows out"
        );
        let parsed_id_arr = good
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("column 0 is not Int64Array"))?;
        let val_arr = good
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("column 1 is not StringArray"))?;
        anyhow::ensure!(parsed_id_arr.value(0) == 10);
        anyhow::ensure!(val_arr.value(1) == "bar");
        Ok(())
    }
}
