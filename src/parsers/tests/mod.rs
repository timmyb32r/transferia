use super::*;

#[test]
fn benchmark_discard_rejects_unknown_configuration() {
    let config: ParserConfig = serde_yaml::from_str(
        "common: { table_naming: { type: from_config, name: events } }\nbenchmark_discard: { typo: true }",
    )
    .unwrap();
    assert!(ParserPlan::from_config(&config, "topic").is_err());
}

#[test]
fn table_name_from_topic_has_an_explicit_config_value() -> anyhow::Result<()> {
    let common: CommonParserConfig =
        serde_yaml::from_str("table_naming: { type: from_topic_name }")?;
    assert!(matches!(common.table_naming, TableNaming::FromTopicName));
    assert!(
        serde_yaml::from_str::<CommonParserConfig>("table_naming: { type: from_topic }").is_err()
    );
    Ok(())
}

#[test]
fn parser_plan_schemas_follow_system_column_visibility() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { offset: source_offset, message_index: source_message_index }\njson_parser:\n  json_framing: single_document\n  columns:\n    - { jsonpath: $.value, column_name: value, json_data_type: integer, arrow_type: Int64, nullable: false }\n  conversion_error: dlq\n  unknown_fields: { action: fail }\n  primary_key: [value, source_offset]\n",
    )?;
    let plan = ParserPlan::from_config(&config, "topic")?;

    let hidden = plan.sink_schema(false);
    assert_eq!(
        hidden
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["value"]
    );
    let visible = plan.sink_schema(true);
    assert_eq!(
        visible
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["value", "source_offset", "source_message_index"]
    );
    assert!(visible.columns[0].primary_key);
    assert!(visible.columns[1].primary_key);
    assert_eq!(plan.dlq_schema(false).columns.len(), 3);
    assert_eq!(plan.dlq_schema(true).columns.len(), 5);
    Ok(())
}
