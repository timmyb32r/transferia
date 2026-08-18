use schemars::JsonSchema;
use serde::Deserialize;

use super::*;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestSourceConfig {
    enabled: bool,
}

fn source_registration(key: &'static str) -> anyhow::Result<ComponentRegistration> {
    ComponentRegistration::new(key, "Test source").source::<TestSourceConfig, _, _>(
        vec![DeliveryMode::Stream],
        true,
        || serde_json::json!({ "enabled": false }),
        |config| {
            anyhow::ensure!(config.enabled, "factory received the decoded configuration");
            anyhow::bail!("test factory intentionally has no runtime provider")
        },
    )
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
        .contains("test factory intentionally has no runtime provider"));
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
