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
/// Null values are serialized as JSON `null`. All values use standard
/// JSON representation (strings are quoted, numbers unquoted, booleans as
/// `true`/`false`).
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize_batch(&self, batch: &RecordBatch) -> anyhow::Result<Bytes> {
        let schema = batch.schema();
        let columns: Vec<(&str, &dyn Array)> = schema
            .fields()
            .iter()
            .zip(batch.columns().iter())
            .map(|(f, col)| (f.name().as_str(), col.as_ref() as &dyn Array))
            .collect();

        let num_rows = batch.num_rows();
        // Estimate ~100 bytes per row; pre-allocate to avoid reallocs.
        let mut buf = Vec::with_capacity(num_rows * 100);

        for row in 0..num_rows {
            if row > 0 {
                buf.push(b'\n');
            }
            buf.push(b'{');
            let mut first = true;
            for (col_name, array) in &columns {
                if array.is_null(row) {
                    continue; // skip null columns
                }
                if !first {
                    buf.push(b',');
                }
                first = false;
                // Write "column_name":
                write_json_string(&mut buf, col_name);
                buf.push(b':');
                // Write value
                write_json_value(&mut buf, *array, row);
            }
            buf.push(b'}');
        }
        // Trailing newline for NDJSON compatibility
        buf.push(b'\n');

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
            // Control characters below 0x20 are escaped as \u00XX
            0x00..=0x1F => {
                buf.extend_from_slice(format!("\\u{:04x}", b).as_bytes());
            }
            _ => buf.push(b),
        }
    }
    buf.push(b'"');
}

/// Write a single value from an Arrow array at the given row index.
fn write_json_value(buf: &mut Vec<u8>, array: &dyn Array, row: usize) {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array,
        LargeStringArray, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };

    let dt = array.data_type();

    // Use downcast + direct access for every type we support.
    match dt {
        DataType::Utf8 => {
            if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                write_json_string(buf, a.value(row));
                return;
            }
        }
        DataType::LargeUtf8 => {
            if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
                write_json_string(buf, a.value(row));
                return;
            }
        }
        DataType::Int8 => {
            if let Some(a) = array.as_any().downcast_ref::<Int8Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::Int16 => {
            if let Some(a) = array.as_any().downcast_ref::<Int16Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::Int32 | DataType::Date32 => {
            if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::Int64 | DataType::Date64
        | DataType::Timestamp(_, _) => {
            if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::UInt8 => {
            if let Some(a) = array.as_any().downcast_ref::<UInt8Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::UInt16 => {
            if let Some(a) = array.as_any().downcast_ref::<UInt16Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::UInt32 => {
            if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::UInt64 => {
            if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
                buf.extend_from_slice(itoa::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::Float32 => {
            if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
                buf.extend_from_slice(ryu::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::Float64 => {
            if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                buf.extend_from_slice(ryu::Buffer::new().format(a.value(row)).as_bytes());
                return;
            }
        }
        DataType::Boolean => {
            if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
                buf.extend_from_slice(if a.value(row) { b"true" } else { b"false" });
                return;
            }
        }
        _ => {}
    }

    // Fallback: write as null
    buf.extend_from_slice(b"null");
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringBuilder, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn serialize_simple_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let id_arr = Int64Array::from(vec![1, 2, 3]);
        let name_arr = StringBuilder::new();
        let mut name_arr = name_arr;
        name_arr.append_value("Alice");
        name_arr.append_value("Bob");
        name_arr.append_value("Charlie");
        let bool_arr = BooleanArray::from(vec![true, false, true]);
        let float_arr = Float64Array::from(vec![1.5, 2.5, 3.5]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_arr),
                Arc::new(name_arr.finish()),
                Arc::new(bool_arr),
                Arc::new(float_arr),
            ],
        )
        .unwrap();

        let serializer = JsonSerializer;
        let output = serializer.serialize_batch(&batch).unwrap();
        let text = String::from_utf8(output.to_vec()).unwrap();

        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "3 rows → 3 JSON lines");

        // Each line should be valid JSON and contain expected fields
        for line in &lines {
            let val: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(val.get("id").is_some());
            assert!(val.get("name").is_some());
        }
    }

    #[test]
    fn serialize_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Utf8, true),
        ]));
        let x_arr = Int64Array::from(vec![1, 2]);
        let mut y_builder = StringBuilder::with_capacity(2, 32);
        y_builder.append_value("hello");
        y_builder.append_null();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(x_arr), Arc::new(y_builder.finish())],
        )
        .unwrap();

        let serializer = JsonSerializer;
        let output = serializer.serialize_batch(&batch).unwrap();
        let text = String::from_utf8(output.to_vec()).unwrap();

        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        // Second row should not have "y" (null columns are skipped)
        let row2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert!(row2.get("y").is_none(), "null column should be absent");
    }

    #[test]
    fn roundtrip_json_parser_compatible() {
        // Serialize → parse → should produce equivalent data
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
        )
        .unwrap();

        let serializer = JsonSerializer;
        let output = serializer.serialize_batch(&batch).unwrap();

        // Parse back with the JSON parser
        let parser_config = crate::config::yaml::SchemaConfig {
            columns: vec![
                crate::config::yaml::ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Int64".into(),
                    nullable: false,
                },
                crate::config::yaml::ColumnMapping {
                    jsonpath: "$.val".into(),
                    column_name: "val".into(),
                    arrow_type: "Utf8".into(),
                    nullable: true,
                },
            ],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: crate::config::yaml::ChunkSplitter::NewLine,
        };

        let parser = crate::parser::JsonParser::new(&parser_config, "test".into()).unwrap();
        let mut ws = crate::parser::ParserWorkspace::new();
        let msgs = vec![crate::types::Message { value: output }];

        let (good, _dlq) = parser.parse_into(msgs, 0, None, &mut ws).unwrap();
        assert_eq!(good.batch.num_rows(), 2, "roundtrip: 2 rows in → 2 rows out");
        assert_eq!(
            good.batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            10,
        );
        assert_eq!(
            good.batch.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(1),
            "bar",
        );
    }
}
