use super::*;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::tuning::TuningParameter;
use transferia_registry::EndpointRole;

#[test]
fn catalog_defines_every_runtime_endpoint_once() -> anyhow::Result<()> {
    let metrics = Arc::new(MetricsRegistry::new());
    let catalog = build_connector_catalog(&metrics)?;
    let keys = catalog
        .definitions()
        .iter()
        .map(|definition| definition.key)
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        [
            "logbroker",
            "kafka",
            "mysql",
            "opensearch",
            "postgres",
            "clickhouse",
            "s3",
            "iceberg",
            "ydb",
            "ytsaurus",
            "data_generator",
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
        .find(|definition| definition.key == "ydb")
        .is_some_and(|definition| definition.source.is_some() && definition.sink.is_some()));
    let opensearch = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "opensearch")
        .ok_or_else(|| anyhow::anyhow!("missing OpenSearch definition"))?;
    assert_eq!(opensearch.title, "OpenSearch");
    assert!(opensearch.source.is_some() && opensearch.sink.is_some());
    assert!(catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "discard")
        .is_some_and(|definition| definition.source.is_none()));
    let generator = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "data_generator")
        .and_then(|definition| definition.source.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing data generator source"))?;
    assert_eq!(generator.initial["preset"]["type"], "transfer_logs");
    assert_eq!(generator.initial["amount"]["type"], "rows");
    assert_eq!(generator.initial["amount"]["row_count"], 50_000_000_u64);
    let preset = &generator.schema["properties"]["preset"];
    assert_eq!(preset["title"], "Preset");
    assert_eq!(preset["$ref"], "#/$defs/DataGeneratorPreset");
    assert_eq!(
        generator.schema["$defs"]["DataGeneratorPreset"]["oneOf"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("generator preset must be a selector"))?
            .iter()
            .map(|variant| variant["title"].as_str())
            .collect::<Vec<_>>(),
        vec![
            Some("Transfer logs"),
            Some("ClickBench hits"),
            Some("Numeric")
        ]
    );
    assert_eq!(generator.schema["properties"]["amount"]["title"], "Amount");
    assert_eq!(
        generator.schema["properties"]["amount"]["x-ui"]["control_width"],
        "wide"
    );
    assert_eq!(
        generator.schema["$defs"]["GenerationAmount"]["oneOf"][0]["properties"]["row_count"]
            ["x-ui"]["widget"],
        "grouped_integer"
    );
    assert_eq!(
        generator.schema["$defs"]["GenerationAmount"]["oneOf"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("generator amount must be a selector"))?
            .iter()
            .map(|variant| variant["title"].as_str())
            .collect::<Vec<_>>(),
        vec![Some("Rows"), Some("Data size"), Some("Infinite")]
    );
    assert_eq!(
        catalog
            .definitions()
            .iter()
            .find(|definition| definition.key == "data_generator")
            .map(|definition| definition.title),
        Some("Data generator (for benchmarks)")
    );
    assert_eq!(
        catalog
            .definitions()
            .iter()
            .find(|definition| definition.key == "discard")
            .map(|definition| definition.title),
        Some("Discard (for benchmarks)")
    );

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

    let s3_source = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "s3")
        .and_then(|definition| definition.source.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing S3 source definition"))?;
    assert_eq!(
        s3_source.schema.pointer("/properties/parser/x-ui/widget"),
        Some(&serde_json::json!("parser")),
        "S3 parser must use the shared deferred full-width parser editor"
    );
    assert_eq!(
        s3_source
            .schema
            .pointer("/$defs/S3JsonParserConfig/properties/json_framing/default"),
        Some(&serde_json::json!("json_lines")),
        "S3 JSON parser must default to JSON Lines"
    );
    assert_eq!(
        sink.initial.pointer("/auth/type"),
        Some(&serde_json::json!("token"))
    );
    Ok(())
}

#[test]
fn typed_endpoint_decoder_rejects_unknown_fields_before_factory() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let Err(error) = catalog.build_sink("discard", serde_yaml::from_str("unexpected: true\n")?)
    else {
        panic!("unknown fields unexpectedly reached the connector factory");
    };
    assert!(error.to_string().contains("unknown field `unexpected`"));
    Ok(())
}

#[test]
fn every_endpoint_has_a_schema_and_object_initial_value() -> anyhow::Result<()> {
    let metrics = Arc::new(MetricsRegistry::new());
    let catalog = build_connector_catalog(&metrics)?;

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
fn high_throughput_connectors_expose_only_explicit_safe_tuning_parameters() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let cases: [(&str, EndpointRole, &[&str]); 8] = [
        (
            "clickhouse",
            EndpointRole::Source,
            &[
                "/batch_rows",
                "/snapshot_reader/compression",
                "/snapshot_reader/row_group_rows",
                "/snapshot_reader/decode_threads",
            ],
        ),
        (
            "clickhouse",
            EndpointRole::Sink,
            &[
                "/insert_target_rows",
                "/insert_target_bytes",
                "/insert_concurrency",
                "/compression",
            ],
        ),
        (
            "iceberg",
            EndpointRole::Source,
            &[
                "/read_batch_rows",
                "/read_data_file_concurrency",
                "/read_manifest_concurrency",
                "/parquet_metadata_size_hint_bytes",
                "/parquet_range_coalesce_bytes",
                "/parquet_range_fetch_concurrency",
            ],
        ),
        (
            "iceberg",
            EndpointRole::Sink,
            &[
                "/target_file_size_bytes",
                "/commit_target_size_bytes",
                "/parquet_compression",
                "/parquet_row_group_rows",
                "/write_concurrency",
            ],
        ),
        ("s3", EndpointRole::Source, &["/parser/batch_rows"]),
        (
            "s3",
            EndpointRole::Sink,
            &[
                "/format/compression",
                "/format/row_group/max_rows",
                "/rotation/max_rows",
                "/upload/parallel_parts",
                "/upload/max_in_flight_objects",
            ],
        ),
        ("ytsaurus", EndpointRole::Source, &["/batch_rows"]),
        (
            "ytsaurus",
            EndpointRole::Sink,
            &[
                "/write_target_bytes",
                "/write_concurrency",
                "/write_flush_interval_ms",
                "/write_row_buffer_bytes",
            ],
        ),
    ];

    for (connector, role, expected) in cases {
        let parameters = catalog.tuning_parameters(connector, role)?;
        assert_eq!(
            parameters
                .iter()
                .map(TuningParameter::pointer)
                .collect::<Vec<_>>(),
            expected,
            "unexpected {connector} {role:?} tuning surface"
        );
        for parameter in parameters {
            let pointer = parameter.pointer();
            for forbidden in [
                "auth",
                "credential",
                "identifier",
                "ordering",
                "password",
                "recovery",
                "retry",
                "secret",
                "timeout",
            ] {
                assert!(
                    !pointer.contains(forbidden),
                    "unsafe tuning pointer {connector} {role:?} {pointer}"
                );
            }
            match parameter {
                TuningParameter::SignedInteger { candidates, .. } => {
                    assert!(!candidates.is_empty());
                }
                TuningParameter::UnsignedInteger { candidates, .. } => {
                    assert!(!candidates.is_empty());
                }
                TuningParameter::Number { candidates, .. } => {
                    assert!(!candidates.is_empty());
                }
                TuningParameter::Choice { values, .. } => assert!(!values.is_empty()),
            }
        }
    }
    Ok(())
}

#[test]
fn catalog_publishes_the_same_changelog_boundary_as_runtime_validation() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let changelog_sources = catalog
        .definitions()
        .iter()
        .filter(|definition| {
            definition.source.as_ref().is_some_and(|source| {
                source
                    .record_semantics
                    .contains(&RecordSemantics::Changelog)
            })
        })
        .map(|definition| definition.key)
        .collect::<Vec<_>>();
    let changelog_sinks = catalog
        .definitions()
        .iter()
        .filter(|definition| {
            definition
                .sink
                .as_ref()
                .is_some_and(|sink| sink.record_semantics.contains(&RecordSemantics::Changelog))
        })
        .map(|definition| definition.key)
        .collect::<Vec<_>>();

    assert_eq!(
        changelog_sources,
        ["logbroker", "kafka", "mysql", "postgres", "ydb"]
    );
    assert_eq!(
        changelog_sinks,
        [
            "logbroker",
            "kafka",
            "mysql",
            "postgres",
            "clickhouse",
            "iceberg",
            "ydb",
            "ytsaurus",
            "discard"
        ]
    );
    for definition in catalog.definitions() {
        for endpoint in [definition.source.as_ref(), definition.sink.as_ref()]
            .into_iter()
            .flatten()
        {
            assert!(
                endpoint
                    .record_semantics
                    .contains(&RecordSemantics::AppendOnly),
                "{} must preserve append-only support",
                definition.key
            );
        }
    }
    Ok(())
}

#[test]
fn middleware_catalog_registers_light_and_heavy_components_once() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let definitions = catalog.middleware_definitions();
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.key)
            .collect::<Vec<_>>(),
        ["filter", "datafusion"]
    );
    assert!(!definitions[0].playground);
    assert!(definitions[1].playground);
    assert!(definitions[1].schema.pointer("/properties/sql").is_some());
    assert!(catalog
        .build_middleware(
            "unknown",
            serde_yaml::Value::Mapping(serde_yaml::Mapping::default()),
        )
        .is_err());
    Ok(())
}

#[test]
fn queue_sinks_expose_serializer_selection_to_the_ui() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;

    for key in ["logbroker", "kafka"] {
        let sink = catalog
            .definitions()
            .iter()
            .find(|definition| definition.key == key)
            .and_then(|definition| definition.sink.as_ref())
            .ok_or_else(|| anyhow::anyhow!("missing {key} sink"))?;
        assert_eq!(
            sink.schema.pointer("/properties/serializer/x-ui/widget"),
            Some(&serde_json::json!("serializer")),
            "{key} serializer must be an explicit UI selection"
        );
        let schema = serde_json::to_string(&sink.schema)?;
        assert!(schema.contains("JSON"), "{key} is missing JSON serializer");
        assert!(
            schema.contains("Schema Registry"),
            "{key} is missing Schema Registry serializer"
        );
        for field in ["url", "auth", "ca_certificate", "subject", "format"] {
            assert!(
                schema.contains(field),
                "{key} Schema Registry serializer is missing {field}"
            );
        }
    }
    Ok(())
}

#[test]
fn every_network_endpoint_exposes_a_connection_check() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    for (connector, role) in crate::connectors::catalog::descriptor::networked_connector_roles() {
        let definition = catalog
            .definitions()
            .iter()
            .find(|definition| definition.key == connector)
            .ok_or_else(|| anyhow::anyhow!("missing {connector} connector definition"))?;
        match role {
            EndpointRole::Source => {
                let source = definition
                    .source
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing {connector} source definition"))?;
                assert!(
                    source.connection_check,
                    "{} source is missing its connection check",
                    definition.key
                );
            }
            EndpointRole::Sink => {
                let sink = definition
                    .sink
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing {connector} sink definition"))?;
                assert!(
                    sink.connection_check,
                    "{} sink is missing its connection check",
                    definition.key
                );
            }
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
fn clickhouse_sink_installation_preserves_data_host_count() -> anyhow::Result<()> {
    let source =
        crate::connectors::catalog::installation_contract("clickhouse", EndpointRole::Source)
            .ok_or_else(|| anyhow::anyhow!("missing ClickHouse source installation contract"))?;
    let sink = crate::connectors::catalog::installation_contract("clickhouse", EndpointRole::Sink)
        .ok_or_else(|| anyhow::anyhow!("missing ClickHouse sink installation contract"))?;

    assert!(!source.output_fields.contains(&"data_host_count"));
    assert!(sink.output_fields.contains(&"data_host_count"));
    assert!(!sink.required_output_fields.contains(&"data_host_count"));
    Ok(())
}

#[tokio::test]
async fn logbroker_connection_check_does_not_require_a_parser_configuration() -> anyhow::Result<()>
{
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let config = serde_yaml::to_value(serde_json::json!({
        "host": "",
        "port": 2135,
        "topics": [{ "path": "cdc/prod/logs", "partitions": [] }],
        "consumer_name": "consumer",
        "auth": { "type": "token", "token": "test" },
        "driver": "ydb",
        "trusted_plaintext": true,
        "allow_ttl_rewind": false,
        "parser": {},
        "read_buffer_bytes": 1_048_576
    }))?;

    let error = catalog
        .check_connection("logbroker", crate::extension::EndpointRole::Source, config)
        .await
        .expect_err("the intentionally empty host must fail validation");
    let message = error.to_string();
    assert!(message.contains("logbroker.host"), "{message}");
    assert!(!message.contains("missing field `common`"), "{message}");
    Ok(())
}

#[tokio::test]
async fn incomplete_logbroker_source_check_only_requires_network_access() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let config = serde_yaml::to_value(serde_json::json!({
        "host": "127.0.0.1",
        "port": port,
        "auth": { "type": "token", "token": "test" },
        "trusted_plaintext": true
    }))?;

    let result = catalog
        .check_connection("logbroker", crate::extension::EndpointRole::Source, config)
        .await?;
    assert!(matches!(
        result.status,
        transferia_registry::ConnectionCheckStatus::NetworkReachable
    ));
    Ok(())
}

#[tokio::test]
async fn incomplete_logbroker_sink_check_only_requires_network_access() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let config = serde_yaml::to_value(serde_json::json!({
        "host": "127.0.0.1",
        "port": port,
        "topic": { "type": "topic", "topic_path": "" },
        "auth": { "type": "token", "token": "test" }
    }))?;

    let result = catalog
        .check_connection("logbroker", crate::extension::EndpointRole::Sink, config)
        .await?;
    assert!(matches!(
        result.status,
        transferia_registry::ConnectionCheckStatus::NetworkReachable
    ));
    assert_eq!(
        result.message.as_deref(),
        Some(
            "Logbroker is network-reachable. Authentication and entity access were not checked because topic is incomplete."
        )
    );
    Ok(())
}

#[tokio::test]
async fn logbroker_checks_only_network_when_credentials_are_empty() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;

    for (role, config) in [
        (
            crate::extension::EndpointRole::Source,
            serde_yaml::to_value(serde_json::json!({
                "host": "127.0.0.1",
                "port": port,
                "topics": [{ "path": "topic", "partitions": [] }],
                "consumer_name": "consumer",
                "auth": { "type": "token", "token": "" },
                "trusted_plaintext": true
            }))?,
        ),
        (
            crate::extension::EndpointRole::Sink,
            serde_yaml::to_value(serde_json::json!({
                "host": "127.0.0.1",
                "port": port,
                "topic": { "type": "topic", "topic_path": "" },
                "auth": { "type": "token", "token": "" }
            }))?,
        ),
    ] {
        let result = catalog.check_connection("logbroker", role, config).await?;
        assert!(matches!(
            result.status,
            transferia_registry::ConnectionCheckStatus::NetworkReachable
        ));
    }
    Ok(())
}

#[test]
fn connector_descriptors_are_the_authoritative_runtime_catalog() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    assert_eq!(catalog.definitions().len(), CONNECTORS.len());
    for (definition, descriptor) in catalog.definitions().iter().zip(CONNECTORS) {
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

    let mut builder = RegistryBuilder::new();
    builder.register(
        ComponentRegistration::new("discard", "Discard").sink::<TypedConfig, _, _>(
            || serde_json::json!({ "marker": "initial" }),
            |config| {
                anyhow::ensure!(config.marker == "typed", "typed config was not delivered");
                Ok(Box::new(
                    crate::connectors::discard::connector::DiscardSinkConnector,
                ))
            },
        )?,
    )?;
    let catalog = builder.build();
    catalog.build_sink("discard", serde_yaml::from_str("marker: typed\n")?)?;
    Ok(())
}

#[test]
fn installation_is_the_first_field_of_every_connected_endpoint() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;

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

#[test]
fn kafka_connection_fields_are_owned_by_its_installation() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let kafka = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "kafka")
        .ok_or_else(|| anyhow::anyhow!("missing Kafka connector"))?;

    for endpoint in [kafka.source.as_ref(), kafka.sink.as_ref()]
        .into_iter()
        .flatten()
    {
        assert_eq!(
            endpoint.initial.pointer("/installation/type"),
            Some(&serde_json::json!("on_premise"))
        );
        assert!(endpoint.schema.pointer("/properties/brokers").is_none());
        assert!(endpoint.schema.pointer("/properties/security").is_none());
        assert!(endpoint
            .schema
            .pointer("/properties/installation/oneOf/0/properties/brokers")
            .is_some());
    }
    Ok(())
}

#[test]
fn opensearch_connection_fields_are_owned_by_its_installation() -> anyhow::Result<()> {
    let catalog = build_connector_catalog(&Arc::new(MetricsRegistry::new()))?;
    let opensearch = catalog
        .definitions()
        .iter()
        .find(|definition| definition.key == "opensearch")
        .ok_or_else(|| anyhow::anyhow!("missing OpenSearch connector"))?;

    for endpoint in [opensearch.source.as_ref(), opensearch.sink.as_ref()]
        .into_iter()
        .flatten()
    {
        assert_eq!(
            endpoint.initial.pointer("/installation/type"),
            Some(&serde_json::json!("on_premise"))
        );
        assert_eq!(
            endpoint.initial.pointer("/installation/trusted_plaintext"),
            Some(&serde_json::json!(false)),
            "OpenSearch must not send default Basic credentials over plaintext"
        );
        for field in ["hosts", "port", "trusted_plaintext", "tls_ca_file"] {
            assert!(
                endpoint
                    .schema
                    .pointer(&format!("/properties/{field}"))
                    .is_none(),
                "OpenSearch connection field '{field}' must not be duplicated at the endpoint root"
            );
            assert!(
                endpoint
                    .schema
                    .pointer(&format!(
                        "/properties/installation/oneOf/0/properties/{field}"
                    ))
                    .is_some(),
                "OpenSearch installation must own '{field}'"
            );
        }
    }
    Ok(())
}
