use super::*;

mod detection;

#[test]
fn benchmark_discard_rejects_unknown_configuration() {
    let config: ParserConfig = serde_yaml::from_str(
        "common: { table_naming: { type: from_config, name: events } }\nbenchmark_discard: { typo: true }",
    )
    .unwrap();
    assert!(ParserPlan::from_config(&config, "topic").is_err());
}

#[test]
fn benchmark_discard_schema_has_no_visible_settings() {
    let schema = schemars::schema_for!(crate::parsers::config::BenchmarkDiscardParserSchema);
    let value = serde_json::to_value(schema).expect("schema must serialize");

    assert_eq!(value["properties"]["common"]["x-ui"]["widget"], "hidden");
    let parser_schema =
        serde_json::to_value(schemars::schema_for!(crate::parsers::config::ParserSchema))
            .expect("parser schema must serialize");
    assert!(parser_schema["anyOf"]
        .as_array()
        .expect("parser variants")
        .iter()
        .any(|variant| variant["title"] == "Discard messages (for benchmarks)"));
    assert_eq!(
        value["properties"]["benchmark_discard"]["x-ui"]["widget"],
        "hidden"
    );
    assert_eq!(
        value["properties"]["common"]["default"]["table_naming"]["type"],
        "from_topic_name"
    );
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
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { offset: source_offset, message_index: source_message_index }\njson_parser:\n  json_framing: single_document\n  columns:\n    - { jsonpath: $.value, column_name: value, json_data_type: number, arrow_type: Int64, nullable: false }\n  conversion_error: dlq\n  unknown_fields: { action: fail }\n  keys: [value, source_offset]\n",
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

#[test]
fn schema_registry_rejects_message_index_system_column() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { message_index: source_message_index }\nschema_registry:\n  connection:\n    url: http://localhost:8081\n    request_timeout_ms: 1000\n    auth: { type: none }\n  json_parser:\n    columns:\n      - { jsonpath: $.value, column_name: value, json_data_type: string, arrow_type: Utf8, nullable: false }\n    conversion_error: fail\n    unknown_fields: { action: fail }\n",
    )?;
    let error = ParserPlan::from_config(&config, "topic")
        .err()
        .ok_or_else(|| anyhow::anyhow!("message_index must be rejected"))?;
    assert!(error.to_string().contains("message_index"), "{error:#}");
    Ok(())
}

#[test]
fn parser_plan_rejects_unknown_duplicate_and_nullable_keys() -> anyhow::Result<()> {
    for (keys, expected) in [
        ("[missing]", "is not produced"),
        ("[value, value]", "repeats column"),
        ("[optional]", "must be non-nullable"),
    ] {
        let config: ParserConfig = serde_yaml::from_str(&format!(
            "common:\n  table_naming: {{ type: from_config, name: events }}\njson_parser:\n  columns:\n    - {{ jsonpath: $.value, column_name: value, json_data_type: number, arrow_type: Int64, nullable: false }}\n    - {{ jsonpath: $.optional, column_name: optional, json_data_type: string, arrow_type: Utf8, nullable: true }}\n  conversion_error: drop\n  unknown_fields: {{ action: drop }}\n  keys: {keys}\n"
        ))?;
        let error = ParserPlan::from_config(&config, "topic")
            .err()
            .ok_or_else(|| anyhow::anyhow!("keys {keys} must be rejected"))?;
        assert!(error.to_string().contains(expected), "{error:#}");
    }
    Ok(())
}
