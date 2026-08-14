use super::*;

fn valid_config() -> &'static str {
    "delivery_id: server-test\ndurable_storage: { type: local_file, path: /tmp/transferia-server-test-state }\nsource:\n  s3:\n    bucket: demo\n    prefix: input\n    region: us-east-1\n    allow_http: true\n    host: localhost\n    port: 4566\n    credentials: { access_key: test, secret_key: test }\n    parser:\n      common:\n        table_naming: { type: from_config, name: events }\n      json_parser:\n        conversion_error: dlq\n        unknown_fields: { action: fail }\n        columns:\n          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }\nsink:\n  discard: {}\nmiddlewares: []\n"
}

#[test]
fn static_assets_are_embedded() {
    assert!(INDEX_HTML.contains("New delivery"));
    assert!(!INDEX_HTML.contains("<textarea"));
    assert!(!INDEX_HTML.contains("identity-form"));
    assert!(!INDEX_HTML.contains("value=\"demo-delivery\""));
    assert!(INDEX_HTML.contains("source-form"));
    assert!(APP_JS.contains("/api/discover"));
    assert!(APP_JS.contains("renderUnion"));
    assert!(APP_JS.contains("new AbortController()"));
    assert!(APP_JS.contains("sequence !== discoverySequence"));
    assert!(APP_JS.contains("element('label', 'switch-row')"));
    assert!(APP_JS.contains("createDropdown"));
    assert!(APP_JS.contains("label.removeAttribute('for')"));
    assert!(APP_JS.contains("renderColumnMappings"));
    assert!(APP_JS.contains("renderSystemColumnsEditor"));
    assert!(APP_JS.contains("'System columns'"));
    assert!(APP_JS.contains("'Not selected'"));
    assert!(APP_JS.contains("search.placeholder = 'Search'"));
    assert!(APP_JS.contains("deliveryCompatibilityIssue"));
    assert!(APP_JS.contains("/api/config/yaml"));
    assert!(APP_JS.contains("navigator.clipboard.writeText"));
    assert!(APP_JS.contains("crypto.getRandomValues"));
    assert!(APP_JS.contains("updateSaveState"));
    assert!(!APP_JS.contains("document.createElement('select')"));
    assert!(APP_JS.contains("Advanced settings"));
    assert!(!APP_JS
        .chars()
        .any(|character| ('А'..='я').contains(&character)));
    assert!(STYLE_CSS.contains(".dataset"));
    assert!(STYLE_CSS.contains(".discovery-loading"));
    assert!(STYLE_CSS.contains("top: calc(100% + 4px)"));
    assert!(STYLE_CSS.contains(".select-chevron"));
    assert!(STYLE_CSS.contains(".select-search"));
    assert!(STYLE_CSS.contains(".column-grid-row"));
}

#[test]
fn form_schema_exposes_provider_unions_and_ui_hints() -> anyhow::Result<()> {
    let definition = config_form_definition()?;
    let schema = definition.schema.to_string();
    assert!(schema.contains("oneOf"));
    assert!(schema.contains("PostgreSQL"));
    assert!(schema.contains("PQv1"));
    assert!(!schema.contains("PostgreSQL batch"));
    assert!(!schema.contains("PQv1 stream"));
    assert!(schema.contains("delivery_modes"));
    assert!(schema.contains("x-ui"));
    assert!(schema.contains("password"));
    assert!(schema.contains("native port"));
    assert!(schema.contains("advanced"));
    assert!(schema.contains("column_mappings"));
    assert!(schema.contains("system_columns"));
    assert!(!schema.contains("sorting_key"));
    assert!(!schema.contains("Delivery ID"));
    assert!(!schema.contains("durable_storage"));
    assert!(definition.source_presets.contains_key("s3"));
    assert!(definition.source_presets.contains_key("ydb_topic"));
    assert!(definition.sink_presets.contains_key("discard"));
    assert_eq!(
        definition.source_presets["ydb_topic"]["topology_discovery"],
        "topic_api"
    );
    assert_eq!(definition.source_presets["ydb_topic"]["host"], "localhost");
    assert!(definition.source_presets["ydb_topic"]
        .get("hosts")
        .is_none());
    assert!(definition.source_presets["ydb_topic"]
        .get("database")
        .is_none());
    assert_eq!(
        definition.source_presets["ydb_topic"]["auth"]["type"],
        "token"
    );
    assert_eq!(
        definition.source_presets["clickhouse"]["port"],
        transferia::providers::clickhouse::DEFAULT_NATIVE_PORT
    );
    assert_eq!(
        definition.sink_presets["clickhouse"]["port"],
        transferia::providers::clickhouse::DEFAULT_NATIVE_PORT
    );
    assert_eq!(definition.sink_presets["clickhouse"]["database"], "");
    assert_eq!(definition.sink_presets["clickhouse"]["username"], "");
    assert_eq!(definition.initial["delivery_type"], Value::Null);
    assert_eq!(
        definition.source_presets["pqv1"]["parser"],
        serde_json::json!({})
    );
    assert_eq!(
        definition.source_presets["ydb_topic"]["parser"],
        serde_json::json!({})
    );
    Ok(())
}

#[test]
fn structured_form_data_renders_as_runtime_yaml() -> anyhow::Result<()> {
    let definition = config_form_definition()?;
    assert_eq!(definition.initial["source"], serde_json::json!({}));
    assert_eq!(definition.initial["sink"], serde_json::json!({}));
    let mut configured = definition.initial.clone();
    configured["delivery_type"] = serde_json::json!("stream");
    configured["source"] = serde_json::json!({
        "pqv1": definition.source_presets["pqv1"].clone()
    });
    configured["source"]["pqv1"]["parser"] = serde_json::json!({
        "common": { "table_naming": { "type": "from_config", "name": "events" } },
        "json_parser": {
            "columns": [{
                "jsonpath": "$.id",
                "column_name": "id",
                "json_data_type": "integer",
                "arrow_type": "Int64",
                "nullable": false
            }],
            "conversion_error": "dlq",
            "unknown_fields": { "action": "fail" }
        }
    });
    configured["sink"] = serde_json::json!({
        "clickhouse": definition.sink_presets["clickhouse"].clone()
    });
    let yaml = config_yaml_from_json(&configured)?;
    let config = Config::from_yaml(&yaml)?;
    assert_eq!(config.delivery_id, "demo-delivery");
    assert_eq!(
        config.delivery_type,
        transferia::config::yaml::DeliveryType::Stream
    );
    assert_eq!(config.source.kind()?, "pqv1");
    assert_eq!(config.sink.kind()?, "clickhouse");
    Ok(())
}

#[test]
fn incomplete_form_still_renders_copyable_yaml() -> anyhow::Result<()> {
    let definition = config_form_definition()?;
    let yaml = render_config_yaml(&definition.initial)?;
    assert!(yaml.contains("delivery_type: null"));
    assert!(yaml.contains("source: {}"));
    Ok(())
}

#[tokio::test]
async fn file_state_round_trips_atomically() -> anyhow::Result<()> {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "transferia-server-test-{}-{unique}",
        std::process::id()
    ));
    tokio::fs::create_dir_all(&path).await?;
    let mut state = StoredState::default();
    state.deliveries.insert(
        "one".into(),
        StoredDelivery {
            id: "one".into(),
            name: "test".into(),
            config_yaml: valid_config().replacen(
                "delivery_id: server-test\n",
                "delivery_id: server-test\ndelivery_type: batch\n",
                1,
            ),
            status: DeliveryStatus::Created,
            config_path: None,
            log_path: None,
        },
    );
    save_state(&path, &state).await?;
    let loaded = load_state(&path).await?;
    assert_eq!(loaded.deliveries["one"].name, "test");
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_invalid_config_before_persisting() {
    let error = discover("source: nope\nsink: nope\n")
        .await
        .expect_err("invalid providers must fail discovery");
    assert!(format!("{error:#}").contains("source"));
}
