use schemars::JsonSchema;
use serde::Deserialize;
use transferia_delivery_contracts::semantics::RecordSemantics;

use super::*;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestSourceConfig {
    enabled: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestMiddlewareConfig {
    label: String,
}

fn source_registration(key: &'static str) -> anyhow::Result<ComponentRegistration> {
    ComponentRegistration::new(key, "Test source").source::<TestSourceConfig, _, _>(
        vec![DeliveryMode::Stream],
        true,
        || serde_json::json!({ "enabled": false }),
        |config| {
            anyhow::ensure!(config.enabled, "factory received the decoded configuration");
            anyhow::bail!("test factory intentionally has no runtime connector")
        },
    )
}

#[test]
fn endpoint_definitions_publish_their_record_semantics() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?.source_record_semantics(vec![
        RecordSemantics::AppendOnly,
        RecordSemantics::Changelog,
    ])?)?;

    assert_eq!(
        builder.build().definitions()[0]
            .source
            .as_ref()
            .unwrap()
            .record_semantics,
        vec![RecordSemantics::AppendOnly, RecordSemantics::Changelog]
    );
    Ok(())
}

#[test]
fn endpoint_semantics_must_be_nonempty_and_unique() -> anyhow::Result<()> {
    let empty = source_registration("empty")?
        .source_record_semantics(Vec::new())
        .err()
        .expect("an endpoint without semantics must fail");
    assert!(empty.to_string().contains("at least one"));

    let duplicate = source_registration("duplicate")?
        .source_record_semantics(vec![
            RecordSemantics::AppendOnly,
            RecordSemantics::AppendOnly,
        ])
        .err()
        .expect("duplicate semantics must fail");
    assert!(duplicate.to_string().contains("duplicate"));
    Ok(())
}

fn middleware_registration(key: &'static str) -> anyhow::Result<MiddlewareRegistration> {
    MiddlewareRegistration::new::<TestMiddlewareConfig, _, _>(
        key,
        "Test middleware",
        || serde_json::json!({ "label": "initial" }),
        |_config| anyhow::bail!("test factory intentionally has no middleware"),
    )
}

#[tokio::test]
async fn middleware_registration_owns_schema_decoder_and_preview_capability() -> anyhow::Result<()>
{
    let mut builder = RegistryBuilder::new();
    builder.register_middleware(MiddlewareRegistration::new_with_preview::<
        TestMiddlewareConfig,
        _,
        _,
        _,
        _,
    >(
        "transform",
        "Test middleware",
        || serde_json::json!({ "label": "initial" }),
        |_config| anyhow::bail!("test factory intentionally has no middleware"),
        |config, rows| async move {
            Ok(MiddlewarePreview {
                columns: vec![MiddlewarePreviewColumn {
                    name: config.label,
                    arrow_type: "Utf8".to_owned(),
                    nullable: false,
                }],
                rows,
            })
        },
    )?)?;
    let registry = builder.build();

    assert_eq!(registry.middleware_definitions()[0].key, "transform");
    assert!(registry.middleware_definitions()[0].playground);
    assert!(registry.middleware_definitions()[0]
        .schema
        .pointer("/properties/label")
        .is_some());
    let preview = registry
        .preview_middleware(
            "transform",
            serde_yaml::from_str("label: projected\n")?,
            vec![serde_json::json!({ "id": 1 })],
        )
        .await?;
    assert_eq!(preview.columns[0].name, "projected");
    assert_eq!(preview.rows, vec![serde_json::json!({ "id": 1 })]);
    assert!(registry
        .build_middleware("transform", serde_yaml::from_str("unknown: true\n")?)
        .is_err());
    Ok(())
}

#[test]
fn registry_rejects_duplicate_middleware_keys() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register_middleware(middleware_registration("transform")?)?;
    let error = builder
        .register_middleware(middleware_registration("transform")?)
        .err()
        .expect("duplicate middleware must fail");
    assert!(error.to_string().contains("registered more than once"));
    Ok(())
}

#[test]
fn registry_rejects_duplicate_component_keys() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;

    let error = builder
        .register(source_registration("source")?)
        .err()
        .expect("duplicate component must fail");

    assert!(error.to_string().contains("registered more than once"));
    Ok(())
}

#[test]
fn registry_rejects_unknown_ui_dialect_hints() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema =
        serde_json::json!({ "type": "string", "x-ui": { "typo": true } });

    let error = registry
        .replace_definitions(definitions)
        .expect_err("unknown x-ui hints must fail composition");
    assert!(error.to_string().contains("invalid x-ui contract"));
    Ok(())
}

#[test]
fn registry_accepts_conditional_layout_ui_hints() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "x-ui": {
            "indent_variant_details": false,
            "delivery_types": ["batch", "stream", "batch_and_stream"]
        }
    });

    registry.replace_definitions(definitions)?;
    Ok(())
}

#[test]
fn registry_rejects_hidden_required_scalars_without_defaults() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "properties": {
            "region": {
                "type": "string",
                "x-ui": { "widget": "hidden" }
            }
        },
        "required": ["region"]
    });

    let error = registry
        .replace_definitions(definitions)
        .expect_err("hidden required scalar without a default must fail composition");
    assert!(error.to_string().contains("hidden required scalar"));
    Ok(())
}

#[test]
fn registry_resolves_local_refs_before_validating_hidden_required_scalars() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "$defs": {
            "Region": { "type": "string" }
        },
        "type": "object",
        "properties": {
            "region": {
                "$ref": "#/$defs/Region",
                "x-ui": { "widget": "hidden" }
            }
        },
        "required": ["region"]
    });

    let error = registry
        .replace_definitions(definitions)
        .expect_err("a local ref must not bypass hidden-field validation");
    assert!(error.to_string().contains("hidden required scalar"));
    Ok(())
}

#[test]
fn registry_materializes_required_singleton_enums() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "properties": {
            "host_selection": {
                "type": "string",
                "enum": ["first_alive_replica"]
            }
        },
        "required": ["host_selection"]
    });

    registry.replace_definitions(definitions)?;
    Ok(())
}

#[test]
fn registry_rejects_blank_required_singleton_enums() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "properties": {
            "host_selection": {
                "type": "string",
                "enum": [""]
            }
        },
        "required": ["host_selection"]
    });

    let error = registry
        .replace_definitions(definitions)
        .expect_err("blank singleton enum cannot satisfy a hidden required field");
    assert!(error.to_string().contains("must not be empty"));
    Ok(())
}

#[test]
fn registry_rejects_blank_defaults_for_hidden_required_strings() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "properties": {
            "region": {
                "type": "string",
                "default": "",
                "x-ui": { "widget": "hidden" }
            }
        },
        "required": ["region"]
    });

    let error = registry
        .replace_definitions(definitions)
        .expect_err("blank hidden required string defaults must fail composition");
    assert!(error.to_string().contains("must not be empty"));
    Ok(())
}

#[test]
fn registry_revalidates_ui_contracts_after_definition_edits() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();

    let error = registry
        .edit_definitions(|definitions| {
            definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
                "type": "object",
                "properties": {
                    "timeout_ms": {
                        "type": "integer",
                        "x-ui": { "widget": "hidden" }
                    }
                },
                "required": ["timeout_ms"]
            });
            Ok(())
        })
        .expect_err("edited UI contracts must be revalidated");
    assert!(error.to_string().contains("hidden required scalar"));
    Ok(())
}

#[test]
fn registry_preserves_explicit_composition_order() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("second")?)?;
    builder.register(source_registration("first")?)?;
    let registry = builder.build();

    assert_eq!(
        registry
            .definitions()
            .iter()
            .map(|definition| definition.key)
            .collect::<Vec<_>>(),
        ["second", "first"]
    );
    Ok(())
}

#[test]
fn registration_rejects_initial_value_that_its_runtime_decoder_cannot_read() {
    let error = ComponentRegistration::new("source", "Test source")
        .source::<TestSourceConfig, _, _>(
            vec![DeliveryMode::Stream],
            true,
            || serde_json::json!({ "wrong": false }),
            |_config| anyhow::bail!("factory must not run"),
        )
        .err()
        .expect("invalid initial configuration must fail during composition");

    assert!(error
        .to_string()
        .contains("invalid initial source configuration"));
}

#[test]
fn explicitly_incomplete_draft_does_not_weaken_runtime_decoding() -> anyhow::Result<()> {
    let registration = ComponentRegistration::new("source", "Test source")
        .source_draft::<TestSourceConfig, _, _>(
            vec![DeliveryMode::Stream],
            true,
            || serde_json::json!({}),
            |_config| anyhow::bail!("factory must not run"),
        )?;
    let mut builder = RegistryBuilder::new();
    builder.register(registration)?;
    let registry = builder.build();

    let error = registry
        .build_source("source", serde_yaml::from_str("{}")?)
        .err()
        .expect("incomplete draft must still fail at runtime construction");

    assert!(error.to_string().contains("invalid source configuration"));
    Ok(())
}

#[test]
fn explicitly_incomplete_sink_draft_does_not_weaken_runtime_decoding() -> anyhow::Result<()> {
    let registration = ComponentRegistration::new("sink", "Test sink")
        .sink_draft::<TestSourceConfig, _, _>(
            || serde_json::json!({}),
            |_config| anyhow::bail!("factory must not run"),
        )?;
    let mut builder = RegistryBuilder::new();
    builder.register(registration)?;
    let registry = builder.build();

    let error = registry
        .build_sink("sink", serde_yaml::from_str("{}")?)
        .err()
        .expect("incomplete draft must still fail at runtime construction");

    assert!(error.to_string().contains("invalid sink configuration"));
    Ok(())
}

#[test]
fn registry_rejects_connection_checker_without_matching_runtime_role() -> anyhow::Result<()> {
    let registration =
        source_registration("source")?.sink_checker::<TestSourceConfig, _, _>(|_config| async {
            Ok(ConnectionCheckResult::default())
        });
    let mut builder = RegistryBuilder::new();

    let error = builder
        .register(registration)
        .err()
        .expect("orphan checker must fail");

    assert!(error
        .to_string()
        .contains("sink connection check without a sink"));
    Ok(())
}

#[tokio::test]
async fn source_preview_is_a_typed_optional_capability() -> anyhow::Result<()> {
    let registration = source_registration("source")?.source_previewer::<TestSourceConfig, _, _>(
        |config, max_bytes, _cancellation| async move {
            anyhow::ensure!(config.enabled, "preview received decoded configuration");
            Ok(SourcePreview {
                payload: vec![7; max_bytes.min(2)],
                detection_payloads: vec![vec![7]],
                metadata: SourcePreviewMetadata {
                    topic: "topic".to_owned(),
                    partition: 0,
                    partition_session_id: 1,
                    offset: 2,
                    sequence_number: 3,
                    created_at_ms: None,
                    written_at_ms: None,
                    producer_id: String::new(),
                    message_group_id: None,
                    codec: "raw".to_owned(),
                    compressed_size: 1,
                    declared_uncompressed_size: None,
                    message_metadata: Vec::new(),
                    write_session_metadata: std::collections::BTreeMap::default(),
                },
            })
        },
    );
    let mut builder = RegistryBuilder::new();
    builder.register(registration)?;
    let registry = builder.build();

    assert!(
        registry.definitions()[0]
            .source
            .as_ref()
            .unwrap()
            .message_preview
    );
    let preview = registry
        .preview_source(
            "source",
            serde_yaml::from_str("enabled: true")?,
            2,
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    assert_eq!(preview.payload, vec![7, 7]);
    Ok(())
}

#[tokio::test]
async fn source_schema_preview_accepts_an_incomplete_runtime_configuration() -> anyhow::Result<()> {
    let registration = source_registration("source")?.source_schema_previewer(
        |raw, request, _cancellation| async move {
            anyhow::ensure!(
                raw.get("parser").and_then(serde_yaml::Value::as_str) == Some("raw"),
                "schema preview received the parser-only draft"
            );
            Ok(transferia_core::delivery::DeliveryDiscovery {
                source_name: "events".into(),
                source_topology: transferia_core::delivery::SourceTopology::DynamicWorkerLanes,
                schema_origin: transferia_core::delivery::SchemaOrigin::ParserProjection,
                keep_system_columns: request.keep_system_columns,
                datasets: Vec::new(),
                performance_advice: Vec::new(),
            })
        },
    );
    let mut builder = RegistryBuilder::new();
    builder.register(registration)?;
    let registry = builder.build();

    assert!(registry.supports_source_schema_preview("source"));
    let discovery = registry
        .preview_source_schema(
            "source",
            serde_yaml::from_str("parser: raw")?,
            transferia_core::delivery::DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    assert_eq!(&*discovery.source_name, "events");
    assert!(discovery.keep_system_columns);
    Ok(())
}

#[test]
fn verified_connection_result_confirms_entity_access() {
    let result = ConnectionCheckResult::default();
    assert!(matches!(result.status, ConnectionCheckStatus::Verified));
    assert_eq!(
        result.message.as_deref(),
        Some("Connection verified, including access to the configured entities.")
    );
}

#[test]
fn typed_registration_keeps_schema_and_decoder_in_one_contract() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let registry = builder.build();
    let endpoint = registry.definitions()[0]
        .source
        .as_ref()
        .expect("source definition");

    assert_eq!(endpoint.initial, serde_json::json!({ "enabled": false }));
    assert!(endpoint.schema.pointer("/properties/enabled").is_some());

    let decode_error = registry
        .build_source("source", serde_yaml::from_str("unknown: true")?)
        .err()
        .expect("unknown configuration field must fail");
    assert!(decode_error
        .to_string()
        .contains("invalid source configuration"));

    let factory_error = registry
        .build_source("source", serde_yaml::from_str("enabled: true")?)
        .err()
        .expect("test factory intentionally fails");
    assert!(factory_error
        .to_string()
        .contains("test factory intentionally has no runtime connector"));
    Ok(())
}

#[test]
fn ui_definitions_cannot_be_attached_to_different_runtime_components() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].key = "other";

    let error = registry
        .replace_definitions(definitions)
        .expect_err("definition/runtime mismatch must fail");

    assert!(error
        .to_string()
        .contains("do not match the executable registry"));
    Ok(())
}

#[test]
fn failed_definition_edit_is_transactional() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let original_title = registry.definitions()[0].title;

    let error = registry
        .edit_definitions(|definitions| {
            definitions[0].title = "Changed";
            anyhow::bail!("extension edit failed")
        })
        .expect_err("failed edit must be reported");

    assert_eq!(error.to_string(), "extension edit failed");
    assert_eq!(registry.definitions()[0].title, original_title);
    Ok(())
}
