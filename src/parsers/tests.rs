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
fn parser_plan_schemas_follow_system_column_visibility() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { offset: true, message_index: true }\njson_parser:\n  chunk_splitter: one-message-one-row\n  columns:\n    - { jsonpath: $.value, column_name: value, arrow_type: Int64, nullable: false }\n",
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
        ["value", "_system_offset", "_system_message_index"]
    );
    assert_eq!(plan.dlq_schema(false).columns.len(), 3);
    assert_eq!(plan.dlq_schema(true).columns.len(), 5);
    Ok(())
}
