use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, StringArray};
use bytes::Bytes;
use transferia_core::data::message::Message;
use transferia_delivery_contracts::parser::ParserFactory as _;

use super::{TskvParser, TskvParserConfig};
use crate::parsers::SystemColumnsConfig;

#[test]
fn public_schema_omits_json_only_column_fields() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(TskvParserConfig))?;
    let column = &schema["$defs"]["TskvColumnMapping"]["properties"];
    assert!(column.get("column_name").is_some());
    assert!(column.get("arrow_type").is_some());
    assert!(column.get("jsonpath").is_none());
    assert!(column.get("json_data_type").is_none());
    Ok(())
}

#[test]
fn parses_strings_and_converts_only_to_configured_arrow_types() -> anyhow::Result<()> {
    let config: TskvParserConfig = serde_yaml::from_str(
        r#"
columns:
  - { column_name: level, arrow_type: Utf8 }
  - { column_name: count, arrow_type: Int64 }
  - { column_name: ready, arrow_type: Boolean }
unknown_fields: { action: drop }
keys: [level]
"#,
    )?;
    let parser = Arc::new(TskvParser::new(
        &config,
        &SystemColumnsConfig::default(),
        Arc::from("events"),
    )?);
    let mut session = parser.create_session(1024 * 1024);
    let (table, dlq) = session.parse_into(vec![Message::new(Bytes::from_static(
        b"tskv\tlevel=INFO\tcount=42\tready=true",
    ))])?;
    assert!(dlq.is_none());
    assert_eq!(
        table.batch.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0),
        "INFO"
    );
    assert_eq!(
        table.batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        42
    );
    assert!(
        table.batch.column(2).as_any().downcast_ref::<BooleanArray>().unwrap().value(0)
    );
    Ok(())
}

#[test]
fn detector_returns_tskv_columns_without_json_mapping_fields() {
    let detections = crate::parsers::detection::detect(
        b"tskv\tlevel=INFO\tcount=42\tready=true",
    );
    let detection = detections
        .iter()
        .find(|detection| detection.key == "tskv")
        .expect("TSKV detector");
    let columns = detection.config["tskv"]["columns"]
        .as_array()
        .expect("TSKV inferred columns");
    assert!(columns.iter().all(|column| column.get("jsonpath").is_none()));
    assert!(columns
        .iter()
        .all(|column| column.get("json_data_type").is_none()));
    assert_eq!(columns[0]["column_name"], "count");
    assert_eq!(columns[0]["arrow_type"], "Int64");
}
