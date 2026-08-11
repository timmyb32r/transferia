use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use transferia::parsers::json_parser::{
    ChunkSplitter, ColumnMapping, JsonParser, JsonParserConfig, ParserWorkspace,
};
use transferia::parsers::{Parser as _, SystemColumnsConfig};
use transferia::serializer::JsonBatchEncoder;
use transferia::types::message::Message;

#[test]
fn json_serializer_output_can_be_parsed() -> anyhow::Result<()> {
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
    let encoder = JsonBatchEncoder::new(&batch, |_| true)?;
    let mut output = Vec::new();
    for row in 0..batch.num_rows() {
        encoder.write_row(row, &mut output);
    }

    let parser_config = JsonParserConfig {
        columns: vec![
            ColumnMapping {
                jsonpath: "$.id".into(),
                column_name: "id".into(),
                arrow_type: "Int64".into(),
                nullable: false,
            },
            ColumnMapping {
                jsonpath: "$.val".into(),
                column_name: "val".into(),
                arrow_type: "Utf8".into(),
                nullable: true,
            },
        ],
        chunk_splitter: ChunkSplitter::NewLine,
    };
    let parser = JsonParser::new(
        &parser_config,
        &SystemColumnsConfig::default(),
        "test".into(),
    )?;
    let mut workspace = ParserWorkspace::new();
    let (good, _dlq) = parser.parse_into(vec![Message::new(output.into())], 0, &mut workspace)?;

    anyhow::ensure!(good.batch.num_rows() == 2, "expected two parsed rows");
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
