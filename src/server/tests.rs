use super::*;

fn valid_config() -> &'static str {
    "source:\n  s3:\n    bucket: demo\n    prefix: input\n    region: us-east-1\n    allow_http: true\n    endpoint: http://localhost:4566\n    credentials: { access_key: test, secret_key: test }\n    parser:\n      common:\n        table_naming: { type: from_config, name: events }\n      json_parser:\n        conversion_error: dlq\n        unknown_fields: { action: fail }\n        columns:\n          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }\nsink:\n  discard: {}\nmiddlewares: []\n"
}

#[test]
fn static_assets_are_embedded() {
    assert!(INDEX_HTML.contains("Create delivery"));
    assert!(APP_JS.contains("/api/discover"));
    assert!(STYLE_CSS.contains(".dataset"));
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
