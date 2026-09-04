use super::*;

mod detection;

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestPluginConfig {
    column: String,
}

struct TestPluginDetector;

impl ParserDetector for TestPluginDetector {
    fn try_parse(&self, payload: &[u8]) -> anyhow::Result<Option<ParserDetection>> {
        Ok((payload == b"plugin").then(|| ParserDetection {
            key: "test_plugin".to_owned(),
            label: "Test plugin".to_owned(),
            config: serde_json::json!({}),
            inferred_columns: Vec::new(),
            sample_rows: Vec::new(),
            preview_tabs: vec![ParserPreviewTab {
                key: "test_pretty".to_owned(),
                label: "Pretty print".to_owned(),
                content: "plugin tree".to_owned(),
                truncated: false,
            }],
            sampled_messages: 1,
            sampled_rows: 0,
        }))
    }
}

#[test]
fn parser_plugins_are_typed_scoped_and_executable() -> anyhow::Result<()> {
    let mut plugins = ParserPluginRegistry::default();
    plugins.register::<TestPluginConfig, _>(
        ParserPluginSpec {
            kind: "test_plugin",
            title: "Test plugin",
            connectors: &["kafka"],
        },
        |common, config, source_name| {
            ParserPlan::from_plugin(
                common,
                source_name,
                Arc::new(benchmark_discard::BenchmarkDiscardParser::new(Arc::from(
                    source_name,
                ))),
                DatasetSchema::new(vec![SchemaColumn::new(
                    config.column,
                    arrow::datatypes::DataType::Utf8,
                    false,
                )]),
                None,
            )
        },
    )?;
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\ntest_plugin: { column: payload }\n",
    )?;
    let plan = ParserPlan::from_config_with_plugins(&config, "topic", &plugins)?;
    assert_eq!(plan.table().as_ref(), "events");
    assert_eq!(plan.dataset_schema().columns[0].name, "payload");
    assert_eq!(plugins.variants_for("kafka").count(), 1);
    assert_eq!(plugins.variants_for("logbroker").count(), 0);
    Ok(())
}

#[test]
fn parser_plugins_can_contribute_detection_and_pretty_print_without_global_hooks(
) -> anyhow::Result<()> {
    let mut plugins = ParserPluginRegistry::default();
    plugins.register::<TestPluginConfig, _>(
        ParserPluginSpec {
            kind: "test_plugin",
            title: "Test plugin",
            connectors: &["kafka"],
        },
        |common, _config, source_name| {
            ParserPlan::from_plugin(
                common,
                source_name,
                Arc::new(benchmark_discard::BenchmarkDiscardParser::new(Arc::from(
                    source_name,
                ))),
                DatasetSchema::default(),
                None,
            )
        },
    )?;
    plugins.register_detector("test_plugin", TestPluginDetector)?;
    assert!(plugins
        .register_detector("test_plugin", TestPluginDetector)
        .is_err());
    assert!(plugins
        .register_detector("missing_plugin", TestPluginDetector)
        .is_err());

    let detections = plugins.detect_samples(&[b"plugin"], 10);
    assert_eq!(detections.len(), 1);
    assert_eq!(detections[0].key, "test_plugin");
    assert_eq!(detections[0].preview_tabs[0].content, "plugin tree");
    Ok(())
}

#[test]
fn parser_plugins_reject_ambiguous_registration_and_invalid_output_schema() -> anyhow::Result<()> {
    let mut plugins = ParserPluginRegistry::default();
    let register = |plugins: &mut ParserPluginRegistry, kind| {
        plugins.register::<TestPluginConfig, _>(
            ParserPluginSpec {
                kind,
                title: "Test plugin",
                connectors: &["kafka"],
            },
            |common, _config, source_name| {
                ParserPlan::from_plugin(
                    common,
                    source_name,
                    Arc::new(benchmark_discard::BenchmarkDiscardParser::new(Arc::from(
                        source_name,
                    ))),
                    DatasetSchema::default(),
                    None,
                )
            },
        )
    };
    assert!(register(&mut plugins, "json_parser").is_err());
    register(&mut plugins, "test_plugin")?;
    assert!(register(&mut plugins, "test_plugin").is_err());
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\ntest_plugin: { column: payload }\n",
    )?;
    assert!(ParserPlan::from_config_with_plugins(&config, "topic", &plugins).is_err());
    Ok(())
}

#[test]
fn raw_to_table_public_schema_is_selectable_and_has_lossless_defaults() {
    let schema = serde_json::to_value(schemars::schema_for!(crate::parsers::config::ParserSchema))
        .expect("parser schema must serialize");
    schema["anyOf"]
        .as_array()
        .expect("parser variants")
        .iter()
        .find(|variant| variant["title"] == "Raw to table parser")
        .expect("raw_to_table variant");
    let variant_text = schema["$defs"]["RawToTableParserSchema"].to_string();
    assert!(variant_text.contains("raw_to_table"));
    assert!(!variant_text.contains("system_columns"));
    let config_text = schema["$defs"]["RawToTableParserConfig"].to_string();
    assert!(config_text.contains("preserve_key"));
    assert!(config_text.contains("preserve_headers"));
    assert!(config_text.contains("preserve_write_timestamp"));
    assert!(config_text.contains("Add message key"));
    assert!(config_text.contains("Add headers"));
    assert!(config_text.contains("Add write timestamp"));
    assert!(!config_text.contains("Preserve message"));
    assert!(!config_text.contains("Preserve write timestamp"));
}

#[test]
fn raw_to_table_plan_uses_source_coordinates_as_primary_key() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\nraw_to_table: {}\n",
    )?;
    let plan = ParserPlan::from_config(&config, "account/topic")?;
    assert_eq!(
        plan.dataset_schema()
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["topic", "partition", "offset"]
    );
    assert_eq!(
        plan.dlq_schema(false)
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        [
            "topic",
            "partition",
            "offset",
            "timestamp",
            "headers",
            "key",
            "tombstone",
            "value",
            "failure_reason"
        ]
    );
    Ok(())
}

#[test]
fn raw_to_table_rejects_duplicate_generic_system_columns() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str(
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { offset: duplicate_offset }\nraw_to_table: {}\n",
    )?;
    let error = ParserPlan::from_config(&config, "account/topic")
        .err()
        .ok_or_else(|| anyhow::anyhow!("generic system columns must be rejected"))?;
    assert!(error.to_string().contains("system_columns"), "{error:#}");
    Ok(())
}

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
    let discard = parser_schema["anyOf"]
        .as_array()
        .expect("parser variants")
        .iter()
        .find(|variant| variant["title"] == "Discard messages (for benchmarks)")
        .expect("discard parser variant");
    assert_eq!(discard["x-ui"]["order"], 1_000_000);
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

    let hidden = plan.stored_schema(false);
    assert_eq!(
        hidden
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["value"]
    );
    let visible = plan.incoming_schema();
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
        "common:\n  table_naming: { type: from_config, name: events }\n  system_columns: { message_index: source_message_index }\nschema_registry:\n  connection:\n    url: http://localhost:8081\n    request_timeout_ms: 1000\n    auth: { type: none }\n",
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
