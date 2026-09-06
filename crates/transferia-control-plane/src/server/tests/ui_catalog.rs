use super::build_ui_catalog;

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

    assert_eq!(keys, ["filter", "datafusion"]);
    assert_eq!(
        alternatives[1]["properties"]["datafusion"]["properties"]["sql"]["type"],
        "string"
    );
    Ok(())
}

#[test]
fn each_transform_variant_exposes_one_shared_table_scope() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    let alternatives = catalog.common_schema["properties"]["middlewares"]["items"]["oneOf"]
        .as_array().unwrap();
    for alternative in alternatives {
        let scope = &alternative["properties"]["tables"];
        assert_eq!(scope["properties"]["include"]["type"], "string");
        assert_eq!(scope["required"], serde_json::json!(["include"]));
        assert_eq!(scope["additionalProperties"], false);
        assert_eq!(scope["properties"]["include_mode"]["enum"], serde_json::json!(["glob", "regex"]));
        assert_eq!(alternative["properties"].as_object().unwrap().len(), 2);
    }
    Ok(())
}
