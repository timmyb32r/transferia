use super::*;

#[test]
fn catalog_defines_every_runtime_endpoint_once() -> anyhow::Result<()> {
    let metrics = Arc::new(MetricsRegistry::new());
    let catalog = build_provider_catalog(&metrics)?;
    let keys = catalog
        .definitions()
        .iter()
        .map(|definition| definition.key)
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        [
            "logbroker",
            "postgres",
            "clickhouse",
            "s3",
            "ytsaurus",
            "discard"
        ]
    );
    assert!(catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "logbroker")
        .is_some_and(|definition| definition.source.is_some() && definition.sink.is_some()));
    assert!(catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "logbroker")
        .is_some_and(|definition| definition.title == "Logbroker"));
    assert!(catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "discard")
        .is_some_and(|definition| definition.source.is_none()));

    let logbroker = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "logbroker")
        .and_then(|definition| definition.source.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing YDB Topic source definition"))?;
    let schema = serde_json::to_string(&logbroker.schema)?;
    assert!(!schema.contains("topology_discovery"));
    assert!(schema.contains("partitions"));
    assert!(schema.contains("pqv1"));
    assert!(logbroker.partitioned);

    let sink = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "logbroker")
        .and_then(|definition| definition.sink.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing Logbroker sink definition"))?;
    let sink_schema = serde_json::to_string(&sink.schema)?;
    assert!(sink_schema.contains("YDB"));
    assert!(sink_schema.contains("PQv1"));
    assert!(!sink_schema.contains("network_timeout_ms"));
    assert_eq!(sink.initial["driver"], "ydb");
    Ok(())
}

#[test]
fn every_endpoint_has_a_schema_and_object_initial_value() -> anyhow::Result<()> {
    let metrics = Arc::new(MetricsRegistry::new());
    let catalog = build_provider_catalog(&metrics)?;

    for definition in catalog.definitions() {
        if let Some(source) = &definition.source {
            assert!(source.schema.is_object());
            assert!(source.initial.is_object());
        }
        if let Some(sink) = &definition.sink {
            assert!(sink.schema.is_object());
            assert!(sink.initial.is_object());
        }
    }
    Ok(())
}

#[test]
fn installation_is_the_first_field_of_every_connected_endpoint() -> anyhow::Result<()> {
    let catalog = build_provider_catalog(&Arc::new(MetricsRegistry::new()))?;

    for definition in catalog.definitions() {
        for endpoint in [definition.source.as_ref(), definition.sink.as_ref()]
            .into_iter()
            .flatten()
        {
            let Some(properties) = endpoint.schema["properties"].as_object() else {
                continue;
            };
            if properties.contains_key("installation") {
                assert_eq!(
                    properties.keys().next().map(String::as_str),
                    Some("installation"),
                    "{}.installation must be rendered first",
                    definition.key
                );
            }
        }
    }
    Ok(())
}
