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
    assert!(!sink_schema.contains("access_token"));
    assert_eq!(
        sink.initial.pointer("/auth/type"),
        Some(&serde_json::json!("token"))
    );
    Ok(())
}

#[test]
fn typed_endpoint_decoder_rejects_unknown_fields_before_factory() -> anyhow::Result<()> {
    let catalog = build_provider_catalog(&Arc::new(MetricsRegistry::new()))?;
    let Err(error) = catalog.build_sink("discard", serde_yaml::from_str("unexpected: true\n")?)
    else {
        panic!("unknown fields unexpectedly reached the provider factory");
    };
    assert!(error.to_string().contains("unknown field `unexpected`"));
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
fn every_network_endpoint_exposes_a_connection_check() -> anyhow::Result<()> {
    let catalog = build_provider_catalog(&Arc::new(MetricsRegistry::new()))?;
    for definition in catalog
        .definitions()
        .iter()
        .filter(|definition| definition.key != "discard")
    {
        if let Some(source) = &definition.source {
            assert!(
                source.connection_check,
                "{} source is missing its connection check",
                definition.key
            );
        }
        if let Some(sink) = &definition.sink {
            assert!(
                sink.connection_check,
                "{} sink is missing its connection check",
                definition.key
            );
        }
    }

    let clickhouse = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "clickhouse")
        .and_then(|definition| definition.sink.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing ClickHouse sink"))?;
    assert_eq!(clickhouse.initial["shard_group"], "");
    assert_eq!(
        clickhouse
            .schema
            .pointer("/properties/shard_group/x-ui/section"),
        Some(&serde_json::json!("shard_group"))
    );
    Ok(())
}

#[test]
fn provider_descriptors_are_the_authoritative_runtime_catalog() -> anyhow::Result<()> {
    let catalog = build_provider_catalog(&Arc::new(MetricsRegistry::new()))?;
    assert_eq!(catalog.definitions().len(), PROVIDERS.len());
    for (definition, descriptor) in catalog.definitions().iter().zip(PROVIDERS) {
        assert_eq!(definition.key, descriptor.key);
        assert_eq!(definition.title, descriptor.title);
        assert_eq!(definition.source.is_some(), descriptor.source.is_some());
        assert_eq!(definition.sink.is_some(), descriptor.sink.is_some());
    }
    Ok(())
}

#[test]
fn endpoint_factory_receives_the_schema_config_type() -> anyhow::Result<()> {
    #[derive(serde::Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct TypedConfig {
        marker: String,
    }

    let mut catalog = ProviderCatalog::new();
    catalog.register(
        ProviderRegistration::new("discard", true)?.sink::<TypedConfig, _, _>(
            || serde_json::json!({ "marker": "initial" }),
            |config| {
                anyhow::ensure!(config.marker == "typed", "typed config was not delivered");
                Ok(Box::new(
                    crate::providers::discard::provider::DiscardSinkProvider,
                ))
            },
        )?,
    )?;
    catalog.build_sink("discard", serde_yaml::from_str("marker: typed\n")?)?;
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
