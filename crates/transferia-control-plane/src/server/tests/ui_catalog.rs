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
            alternative["properties"]
                .as_object()
                .and_then(|properties| properties.keys().next())
                .map(String::as_str)
                .expect("each middleware variant must own one property")
        })
        .collect::<Vec<_>>();

    assert_eq!(keys, ["filter", "datafusion"]);
    assert_eq!(
        alternatives[1]["properties"]["datafusion"]["properties"]["sql"]["type"],
        "string"
    );
    Ok(())
}
