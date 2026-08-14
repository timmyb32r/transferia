use super::*;
use arrow::array::{BooleanArray, Float64Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn encode_batch(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let encoder = JsonBatchEncoder::new(batch, |_| true)?;
    let mut output = Vec::new();
    for row in 0..batch.num_rows() {
        encoder.write_row(row, &mut output);
    }
    Ok(output)
}

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

    let text = String::from_utf8(encode_batch(&batch)?)?;

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

    let batch = RecordBatch::try_new(schema, vec![Arc::new(x_arr), Arc::new(y_builder.finish())])?;

    let text = String::from_utf8(encode_batch(&batch)?)?;

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
fn non_finite_floats_are_valid_json_nulls() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.5,
        ]))],
    )?;
    let output = String::from_utf8(encode_batch(&batch)?)?;
    let rows = output
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(rows[..3].iter().all(|row| row["value"].is_null()));
    anyhow::ensure!(rows[3]["value"] == serde_json::json!(1.5));
    Ok(())
}
