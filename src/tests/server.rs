use super::*;

fn valid_config() -> &'static str {
    "delivery_id: server-test\ndurable_storage: { type: local_file, path: /tmp/transferia-server-test-state }\nsource:\n  s3:\n    bucket: demo\n    prefix: input\n    region: us-east-1\n    allow_http: true\n    host: localhost\n    port: 4566\n    credentials: { access_key: test, secret_key: test }\n    parser:\n      common:\n        table_naming: { type: from_config, name: events }\n      json_parser:\n        conversion_error: dlq\n        unknown_fields: { action: fail }\n        columns:\n          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }\nsink:\n  discard: {}\nmiddlewares: []\n"
}

#[test]
fn static_assets_are_embedded() {
    assert!(INDEX_HTML.contains("New delivery"));
    assert!(!INDEX_HTML.contains("<textarea"));
    assert!(INDEX_HTML.contains("source-form"));
    assert!(APP_JS.contains("/api/discover"));
    assert!(APP_JS.contains("renderUnion"));
    assert!(APP_JS.contains("new AbortController()"));
    assert!(APP_JS.contains("sequence !== discoverySequence"));
    assert!(APP_JS.contains("element('label', 'switch-row')"));
    assert!(APP_JS.contains("Расширенные настройки"));
    assert!(STYLE_CSS.contains(".dataset"));
    assert!(STYLE_CSS.contains(".discovery-loading"));
}

#[test]
fn form_schema_exposes_provider_unions_and_ui_hints() -> anyhow::Result<()> {
    let definition = config_form_definition()?;
    let schema = definition.schema.to_string();
    assert!(schema.contains("oneOf"));
    assert!(schema.contains("PostgreSQL batch"));
    assert!(schema.contains("PQv1 stream"));
    assert!(schema.contains("x-ui"));
    assert!(schema.contains("password"));
    assert!(schema.contains("native port"));
    assert!(schema.contains("advanced"));
    assert!(!schema.contains("sorting_key"));
    assert!(definition.source_presets.contains_key("s3"));
    assert!(definition.sink_presets.contains_key("discard"));
    assert_eq!(definition.sink_presets["clickhouse"]["database"], "");
    assert_eq!(definition.sink_presets["clickhouse"]["username"], "");
    Ok(())
}

#[test]
fn structured_form_data_renders_as_runtime_yaml() -> anyhow::Result<()> {
    let definition = config_form_definition()?;
    let yaml = config_yaml_from_json(&definition.initial)?;
    let config = Config::from_yaml(&yaml)?;
    assert_eq!(config.delivery_id, "demo-delivery");
    assert_eq!(config.source.kind()?, "pqv1");
    assert_eq!(config.sink.kind()?, "clickhouse");
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
            config_yaml: valid_config().into(),
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
