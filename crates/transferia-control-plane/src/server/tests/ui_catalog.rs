use super::build_ui_catalog;

#[test]
fn database_type_examples_reach_the_ui_catalog_without_losing_errors() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    for key in ["postgres", "clickhouse", "mysql", "ydb", "ytsaurus", "iceberg", "opensearch"] {
        let connector = catalog.connectors.iter().find(|connector| connector.key == key).unwrap();
        for endpoint in [connector.source.as_ref(), connector.sink.as_ref()] {
            let mapping = endpoint.unwrap().type_mapping.as_ref().expect("native endpoints must expose runtime examples");
            assert!(!mapping.context.is_empty());
            assert!(mapping.rows.iter().any(|row| row.output.is_some()), "{key}: every example was rejected");
            assert!(mapping.rows.iter().all(|row| row.output.is_some() != row.error.is_some()));
            let inputs = mapping.rows.iter().map(|row| &row.input).collect::<std::collections::BTreeSet<_>>();
            assert_eq!(inputs.len(), mapping.rows.len(), "{key}: duplicate example inputs");
        }
    }
    Ok(())
}

#[test]
fn middleware_schema_is_derived_from_registered_components() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    let alternatives = catalog.common_schema["properties"]["middlewares"]["items"]["oneOf"]
        .as_array()
        .expect("middleware items must be a oneOf schema");
    let keys = alternatives
        .iter()
        .map(|alternative| {
            alternative["required"][0]
                .as_str()
                .expect("each middleware variant must require its action")
        })
        .collect::<Vec<_>>();

    assert_eq!(keys, ["filter", "rename_table", "datafusion"]);
    assert_eq!(
        alternatives[2]["properties"]["datafusion"]["properties"]["sql"]["type"],
        "string"
    );
    Ok(())
}

#[test]
fn each_transform_variant_exposes_one_shared_table_scope() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    let alternatives = catalog.common_schema["properties"]["middlewares"]["items"]["oneOf"]
        .as_array()
        .unwrap();
    for alternative in alternatives {
        let scope = &alternative["properties"]["tables"];
        assert_eq!(scope["properties"]["include"]["type"], "string");
        assert_eq!(scope["required"], serde_json::json!(["include"]));
        assert_eq!(scope["additionalProperties"], false);
        assert_eq!(
            scope["properties"]["include_mode"]["enum"],
            serde_json::json!(["glob", "regex"])
        );
        assert_eq!(alternative["properties"].as_object().unwrap().len(), 2);
    }
    Ok(())
}
