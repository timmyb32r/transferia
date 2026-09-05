use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use transferia_core::delivery::{DeliveryDiscovery, SchemaOrigin, SourceTopology};
use transferia_core::source::Source;
use transferia_delivery_contracts::semantics::{
    EndpointDescriptor, RecordSemantics, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use transferia_delivery_contracts::DeliveryType;

use super::*;

#[test]
fn handoff_metadata_is_typed_and_restricted_to_combined_sources() {
    for handoff in ["exact_switchover", "overlapping"] {
        let mut schema = serde_json::json!({"type": "object", "x-ui": {"capabilities": {
            "component": "source", "key": "arbitrary", "delivery_modes": ["batch_and_stream"],
            "record_semantics": ["changelog"], "batch_stream_handoff": handoff
        }}});
        crate::ui_contract::validate_ui_dialect(&schema).unwrap();
        schema["x-ui"]["capabilities"]["component"] = serde_json::json!("destination");
        assert!(crate::ui_contract::validate_ui_dialect(&schema).is_err());
        schema["x-ui"]["capabilities"]["component"] = serde_json::json!("source");
        schema["x-ui"]["capabilities"]["delivery_modes"] = serde_json::json!(["stream"]);
        assert!(crate::ui_contract::validate_ui_dialect(&schema).is_err());
        schema["x-ui"]["capabilities"]["delivery_modes"] = serde_json::json!(["batch_and_stream"]);
        schema["x-ui"]["capabilities"]["batch_stream_handoff"] = serde_json::json!("guessed");
        assert!(crate::ui_contract::validate_ui_dialect(&schema).is_err());
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestSourceConfig {
    enabled: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestSinkConfig {
    enabled: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TestMiddlewareConfig {
    label: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TunableSourceConfig {
    #[schemars(range(min = 1, max = 8))]
    workers: u64,

    mode: TunableMode,

    fixed: String,

    #[serde(default)]
    optional: Option<u64>,

    #[serde(default)]
    #[schemars(extend("multipleOf" = 2))]
    even_workers: u64,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum TunableMode {
    Safe,

    Fast,
}

#[derive(Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum BranchedTunableReader {
    Safe,

    Parallel {
        #[schemars(range(min = 1, max = 16))]
        concurrency: u64,
    },
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BranchedTunableSourceConfig {
    reader: BranchedTunableReader,
}

struct DefaultExecutionPhaseSource;

impl SourceConnector for DefaultExecutionPhaseSource {
    fn compatibility(
        &self,
        _delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::DataGenerator(SourceDescriptor {
            behavior: SourceBehavior::FiniteAppendOnlyRows,
            delivery_modes: SourceDeliveryModes::BATCH_AND_STREAM,
        })
    }

    fn delivery_discovery(
        &self,
        _context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async { anyhow::bail!("unused discovery") })
    }

    fn build_source(
        &self,
        _context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async { anyhow::bail!("unused source") })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        panic!("unused parser")
    }

    fn parses_rows(&self) -> bool {
        true
    }
}

fn empty_discovery(topology: SourceTopology) -> DeliveryDiscovery {
    DeliveryDiscovery {
        source_name: "source".into(),
        source_topology: topology,
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: Vec::new(),
        performance_advice: Vec::new(),
    }
}

#[test]
fn default_execution_phases_are_exact_for_single_mode_delivery() -> anyhow::Result<()> {
    let connector = DefaultExecutionPhaseSource;
    let topology = SourceTopology::StaticPartitions(vec![2, 7]);
    let discovery = empty_discovery(topology.clone());

    assert_eq!(
        connector.execution_phases(DeliveryType::Batch, &discovery)?,
        vec![SourceExecutionPhase {
            phase: SourcePhase::Snapshot,
            topology: topology.clone(),
            finite: true,
        }]
    );
    assert_eq!(
        connector.execution_phases(DeliveryType::Stream, &discovery)?,
        vec![SourceExecutionPhase {
            phase: SourcePhase::Stream,
            topology,
            finite: false,
        }]
    );
    let error = connector
        .execution_phases(DeliveryType::BatchAndStream, &discovery)
        .expect_err("combined delivery must require a connector-owned phase plan");
    assert!(error
        .to_string()
        .contains("explicit connector execution phase plan"));
    Ok(())
}

#[test]
fn combined_delivery_mode_has_stable_catalog_encoding() -> anyhow::Result<()> {
    assert_eq!(
        serde_json::to_value(DeliveryMode::BatchAndStream)?,
        serde_json::json!("batch_and_stream")
    );
    Ok(())
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

fn sink_registration(key: &'static str) -> anyhow::Result<ComponentRegistration> {
    ComponentRegistration::new(key, "Test sink").sink::<TestSinkConfig, _, _>(
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
fn registry_rejects_unknown_schema_extension_in_nested_union_branch() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "array",
        "items": {
            "oneOf": [{
                "type": "object",
                "x-capabilities": { "component": "transformer" }
            }]
        }
    });

    let error = registry
        .replace_definitions(definitions)
        .expect_err("unknown extension in a nested branch must fail composition");
    assert!(error
        .to_string()
        .contains("#/items/oneOf/0: unsupported JSON Schema extension keyword: x-capabilities"));
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
fn registry_accepts_component_capability_ui_hints() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "x-ui": {
            "capabilities": {
                "component": "parser",
                "key": "debezium",
                "record_semantics": ["changelog"],
                "properties": ["playground"]
            }
        }
    });

    registry.replace_definitions(definitions)?;
    Ok(())
}

#[test]
fn registry_accepts_endpoint_capabilities_within_registered_aggregates() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(source_registration("source")?.source_record_semantics(vec![
        RecordSemantics::AppendOnly,
        RecordSemantics::Changelog,
    ])?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "x-ui": {
            "capabilities": {
                "component": "source",
                "key": "replication",
                "delivery_modes": ["stream"],
                "record_semantics": ["changelog"]
            }
        }
    });

    registry.replace_definitions(definitions)?;

    let mut builder = RegistryBuilder::new();
    builder.register(sink_registration("sink")?.sink_record_semantics(vec![
        RecordSemantics::AppendOnly,
        RecordSemantics::Changelog,
    ])?)?;
    let mut registry = builder.build();
    let mut definitions = registry.definitions().to_vec();
    definitions[0].sink.as_mut().unwrap().schema = serde_json::json!({
        "type": "object",
        "x-ui": {
            "capabilities": {
                "component": "destination",
                "key": "upsert",
                "delivery_modes": ["batch"],
                "record_semantics": ["changelog"]
            }
        }
    });
    registry.replace_definitions(definitions)?;
    Ok(())
}

#[test]
fn registry_rejects_invalid_endpoint_capability_contracts() -> anyhow::Result<()> {
    for (capabilities, expected) in [
        (
            serde_json::json!({
                "component": "destination",
                "key": "wrong_role",
                "record_semantics": ["append_only"]
            }),
            "registered role",
        ),
        (
            serde_json::json!({
                "component": "source",
                "key": "unsupported_mode",
                "delivery_modes": ["batch"],
                "record_semantics": ["append_only"]
            }),
            "subset of the registered aggregate",
        ),
        (
            serde_json::json!({
                "component": "source",
                "key": "duplicate",
                "delivery_modes": ["stream", "stream"],
                "record_semantics": ["append_only"]
            }),
            "non-empty and unique",
        ),
        (
            serde_json::json!({
                "component": "parser",
                "key": "not_an_endpoint",
                "delivery_modes": ["stream"],
                "record_semantics": ["append_only"]
            }),
            "cannot declare endpoint delivery_modes",
        ),
    ] {
        let mut builder = RegistryBuilder::new();
        builder.register(source_registration("source")?.source_record_semantics(vec![
            RecordSemantics::AppendOnly,
            RecordSemantics::Changelog,
        ])?)?;
        let mut registry = builder.build();
        let mut definitions = registry.definitions().to_vec();
        definitions[0].source.as_mut().unwrap().schema = serde_json::json!({
            "type": "object",
            "x-ui": { "capabilities": capabilities }
        });
        let error = registry
            .replace_definitions(definitions)
            .expect_err("invalid endpoint capabilities must fail closed");
        assert!(error.to_string().contains(expected), "{error}");
    }
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

fn tuning_parameters() -> Vec<tuning::TuningParameter> {
    vec![
        tuning::TuningParameter::UnsignedInteger {
            pointer: "/workers".to_owned(),
            label: "Workers".to_owned(),
            baseline: 1,
            minimum: 1,
            maximum: 8,
            candidates: vec![1, 2, 4, 8],
            scale: tuning::NumericScale::Logarithmic,
        },
        tuning::TuningParameter::Choice {
            pointer: "/mode".to_owned(),
            label: "Mode".to_owned(),
            baseline: JsonValue::from("safe"),
            values: vec![JsonValue::from("safe"), JsonValue::from("fast")],
        },
    ]
}

fn tunable_registration(key: &'static str) -> anyhow::Result<ComponentRegistration> {
    ComponentRegistration::new(key, "Tunable source")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "untouched" }),
            |config| {
                drop((
                    config.workers,
                    config.mode,
                    config.fixed,
                    config.optional,
                    config.even_workers,
                ));
                anyhow::bail!("test factory intentionally has no runtime connector")
            },
        )?
        .source_tuning_parameters(tuning_parameters())
}

#[test]
fn registry_keeps_only_explicit_connector_tuning_metadata() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    builder.register(tunable_registration("source")?)?;
    let registry = builder.build();

    assert_eq!(
        registry.tuning_parameters("source", EndpointRole::Source)?,
        tuning_parameters()
    );
    assert!(registry
        .tuning_parameters("source", EndpointRole::Sink)
        .unwrap_err()
        .to_string()
        .contains("has no Sink endpoint"));
    assert!(registry
        .tuning_parameters("missing", EndpointRole::Source)
        .unwrap_err()
        .to_string()
        .contains("unknown connector"));
    Ok(())
}

#[test]
fn tuning_metadata_rejects_missing_duplicate_and_out_of_range_values() {
    let initial = serde_json::json!({ "workers": 2, "mode": "safe" });
    let missing = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/missing".to_owned(),
        label: "Missing".to_owned(),
        baseline: 1,
        minimum: 1,
        maximum: 8,
        candidates: vec![1],
        scale: tuning::NumericScale::Linear,
    }];
    assert!(tuning::validate_tuning_parameters(&initial, &missing)
        .unwrap_err()
        .to_string()
        .contains("missing configuration value"));

    let unbounded = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/workers".to_owned(),
        label: "Workers".to_owned(),
        baseline: 2,
        minimum: 1,
        maximum: 8,
        candidates: Vec::new(),
        scale: tuning::NumericScale::Linear,
    }];
    assert!(tuning::validate_tuning_parameters(&initial, &unbounded)
        .unwrap_err()
        .to_string()
        .contains("must declare finite candidates"));

    let duplicate = vec![
        tuning::TuningParameter::Choice {
            pointer: "/mode".to_owned(),
            label: "Mode one".to_owned(),
            baseline: JsonValue::from("safe"),
            values: vec![JsonValue::from("safe")],
        },
        tuning::TuningParameter::Choice {
            pointer: "/mode".to_owned(),
            label: "Mode two".to_owned(),
            baseline: JsonValue::from("safe"),
            values: vec![JsonValue::from("safe")],
        },
    ];
    assert!(tuning::validate_tuning_parameters(&initial, &duplicate)
        .unwrap_err()
        .to_string()
        .contains("registered more than once"));

    let out_of_range = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/workers".to_owned(),
        label: "Workers".to_owned(),
        baseline: 1,
        minimum: 1,
        maximum: 8,
        candidates: vec![1, 16],
        scale: tuning::NumericScale::Linear,
    }];
    assert!(tuning::validate_tuning_parameters(&initial, &out_of_range)
        .unwrap_err()
        .to_string()
        .contains("outside its tuning range"));
}

#[test]
fn registration_rejects_tuning_metadata_outside_authored_json_schema() -> anyhow::Result<()> {
    let wrong_baseline = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/workers".to_owned(),
        label: "Workers".to_owned(),
        baseline: 2,
        minimum: 1,
        maximum: 8,
        candidates: vec![1, 2, 4, 8],
        scale: tuning::NumericScale::Logarithmic,
    }];
    let error = ComponentRegistration::new("baseline", "Baseline")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "fixed" }),
            |_| anyhow::bail!("unused"),
        )?
        .source_tuning_parameters(wrong_baseline)
        .err()
        .expect("baseline must equal the authored connector default");
    assert!(error.to_string().contains("authored default"));

    let out_of_schema_range = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/workers".to_owned(),
        label: "Workers".to_owned(),
        baseline: 1,
        minimum: 1,
        maximum: 16,
        candidates: vec![1, 8, 16],
        scale: tuning::NumericScale::Linear,
    }];
    let error = ComponentRegistration::new("range", "Range")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "fixed" }),
            |_| anyhow::bail!("unused"),
        )?
        .source_tuning_parameters(out_of_schema_range)
        .err()
        .expect("schema-incompatible range must fail");
    assert!(error.to_string().contains("JSON Schema maximum"));

    let out_of_schema_enum = vec![tuning::TuningParameter::Choice {
        pointer: "/mode".to_owned(),
        label: "Mode".to_owned(),
        baseline: JsonValue::from("safe"),
        values: vec![JsonValue::from("safe"), JsonValue::from("turbo")],
    }];
    let error = ComponentRegistration::new("enum", "Enum")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "fixed" }),
            |_| anyhow::bail!("unused"),
        )?
        .source_tuning_parameters(out_of_schema_enum)
        .err()
        .expect("schema-incompatible enum value must fail");
    assert!(error.to_string().contains("JSON Schema enum"));

    let compiled_schema_constraint = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/even_workers".to_owned(),
        label: "Even workers".to_owned(),
        baseline: 2,
        minimum: 2,
        maximum: 3,
        candidates: vec![2, 3],
        scale: tuning::NumericScale::Linear,
    }];
    let error = ComponentRegistration::new("compiled", "Compiled")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "fixed" }),
            |_| anyhow::bail!("unused"),
        )?
        .source_tuning_parameters(compiled_schema_constraint)
        .err()
        .expect("compiled schema constraint must reject a candidate");
    assert!(error
        .to_string()
        .contains("conflicts with endpoint JSON Schema"));

    let missing_optional = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/optional".to_owned(),
        label: "Optional".to_owned(),
        baseline: 1,
        minimum: 1,
        maximum: 8,
        candidates: vec![1, 2, 4, 8],
        scale: tuning::NumericScale::Linear,
    }];
    ComponentRegistration::new("optional", "Optional")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "fixed" }),
            |_| anyhow::bail!("unused"),
        )?
        .source_tuning_parameters(missing_optional)?;

    let outside_schema = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/not_a_field".to_owned(),
        label: "Unknown".to_owned(),
        baseline: 1,
        minimum: 1,
        maximum: 8,
        candidates: vec![1, 2, 4, 8],
        scale: tuning::NumericScale::Logarithmic,
    }];
    let error = ComponentRegistration::new("outside", "Outside")
        .source::<TunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "workers": 1, "mode": "safe", "fixed": "fixed" }),
            |_| anyhow::bail!("unused"),
        )?
        .source_tuning_parameters(outside_schema)
        .err()
        .expect("pointer outside every schema branch must fail");
    assert!(error
        .to_string()
        .contains("outside the endpoint JSON Schema"));
    Ok(())
}

#[tokio::test]
async fn branch_specific_parameter_may_be_absent_from_initial_and_inactive_runtime_branch(
) -> anyhow::Result<()> {
    let parameters = vec![tuning::TuningParameter::UnsignedInteger {
        pointer: "/reader/concurrency".to_owned(),
        label: "Reader concurrency".to_owned(),
        baseline: 2,
        minimum: 1,
        maximum: 16,
        candidates: vec![1, 2, 4, 8, 16],
        scale: tuning::NumericScale::Logarithmic,
    }];
    ComponentRegistration::new("branched", "Branched")
        .source::<BranchedTunableSourceConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || serde_json::json!({ "reader": { "type": "safe" } }),
            |config| {
                match config.reader {
                    BranchedTunableReader::Safe => {}
                    BranchedTunableReader::Parallel { concurrency } => {
                        let _ = concurrency;
                    }
                }
                anyhow::bail!("unused")
            },
        )?
        .source_tuning_parameters(parameters.clone())?;

    let inactive = tuning::tune_endpoint(
        tuning::EndpointTuningRequest {
            configuration: serde_json::json!({ "reader": { "type": "safe" } }),
            parameters: parameters.clone(),
            budget: tuning::TuningBudget {
                max_trials: 8,
                max_duration_ms: Some(1_000),
            },
        },
        CancellationToken::new(),
        |_, _| async { Ok(10.0) },
    )
    .await?;
    assert_eq!(inactive.trials, 1);
    assert!(inactive.parameters.is_empty());

    let active = tuning::tune_endpoint(
        tuning::EndpointTuningRequest {
            configuration: serde_json::json!({
                "reader": { "type": "parallel", "concurrency": 2 }
            }),
            parameters,
            budget: tuning::TuningBudget {
                max_trials: 8,
                max_duration_ms: Some(1_000),
            },
        },
        CancellationToken::new(),
        |configuration, _| async move {
            Ok(configuration["reader"]["concurrency"].as_u64().unwrap() as f64)
        },
    )
    .await?;
    assert_eq!(active.parameters["/reader/concurrency"], 16);
    Ok(())
}

fn endpoint_tuning_request(max_trials: usize) -> tuning::EndpointTuningRequest {
    tuning::EndpointTuningRequest {
        configuration: serde_json::json!({
            "workers": 8,
            "mode": "fast",
            "fixed": { "credential": "never mutate" }
        }),
        parameters: tuning_parameters(),
        budget: tuning::TuningBudget {
            max_trials,
            max_duration_ms: Some(5_000),
        },
    }
}

async fn deterministic_tuning_result(max_trials: usize) -> anyhow::Result<tuning::TuningResult> {
    tuning::tune_endpoint(
        endpoint_tuning_request(max_trials),
        CancellationToken::new(),
        |configuration, _| async move {
            anyhow::ensure!(
                configuration.pointer("/fixed/credential")
                    == Some(&JsonValue::from("never mutate")),
                "optimizer mutated an undeclared field"
            );
            let workers = configuration["workers"].as_u64().unwrap() as f64;
            anyhow::ensure!([1.0, 2.0, 4.0, 8.0].contains(&workers));
            let mode_bonus = if configuration["mode"] == "fast" {
                100.0
            } else {
                0.0
            };
            Ok((workers - 4.0).mul_add(-(workers - 4.0), 1_000.0) + mode_bonus)
        },
    )
    .await
}

#[tokio::test]
async fn optimizer_is_deterministic_and_never_mutates_undeclared_configuration(
) -> anyhow::Result<()> {
    let first = deterministic_tuning_result(12).await?;
    let second = deterministic_tuning_result(12).await?;

    assert_eq!(first, second);
    assert_eq!(
        first.baseline_rows_per_second.to_bits(),
        991.0_f64.to_bits()
    );
    assert_eq!(first.trials, 8);
    assert_eq!(first.parameters["/workers"], 4);
    assert_eq!(first.parameters["/mode"], "fast");
    assert!(first.optimized_rows_per_second > first.baseline_rows_per_second);
    assert!(!serde_json::to_string(&first)?.contains("never mutate"));
    Ok(())
}

#[tokio::test]
async fn optimizer_enumerates_only_the_finite_declared_product() -> anyhow::Result<()> {
    let parameters = vec![
        tuning::TuningParameter::Choice {
            pointer: "/left".to_owned(),
            label: "Left".to_owned(),
            baseline: serde_json::json!("a"),
            values: vec![serde_json::json!("a"), serde_json::json!("b")],
        },
        tuning::TuningParameter::Choice {
            pointer: "/right".to_owned(),
            label: "Right".to_owned(),
            baseline: serde_json::json!(false),
            values: vec![serde_json::json!(false), serde_json::json!(true)],
        },
    ];
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let result = tuning::tune_endpoint(
        tuning::EndpointTuningRequest {
            configuration: serde_json::json!({ "left": "a", "right": false }),
            parameters,
            budget: tuning::TuningBudget {
                max_trials: usize::MAX,
                max_duration_ms: Some(5_000),
            },
        },
        CancellationToken::new(),
        move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(1.0) }
        },
    )
    .await?;

    assert_eq!(result.trials, 4);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    Ok(())
}

#[tokio::test]
async fn optimizer_caps_an_overflowing_cartesian_product_by_the_trial_budget() -> anyhow::Result<()>
{
    let configuration = serde_json::Value::Object(
        (0..usize::BITS)
            .map(|index| (format!("p{index}"), serde_json::json!(false)))
            .collect(),
    );
    let parameters = (0..usize::BITS)
        .map(|index| tuning::TuningParameter::Choice {
            pointer: format!("/p{index}"),
            label: format!("P {index}"),
            baseline: serde_json::json!(false),
            values: vec![serde_json::json!(false), serde_json::json!(true)],
        })
        .collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let result = tuning::tune_endpoint(
        tuning::EndpointTuningRequest {
            configuration,
            parameters,
            budget: tuning::TuningBudget {
                max_trials: 3,
                max_duration_ms: Some(5_000),
            },
        },
        CancellationToken::new(),
        move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(1.0) }
        },
    )
    .await?;

    assert_eq!(result.trials, 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    Ok(())
}

fn binary_parameters(names: &[&str]) -> Vec<tuning::TuningParameter> {
    names
        .iter()
        .map(|name| tuning::TuningParameter::Choice {
            pointer: format!("/{name}"),
            label: (*name).to_owned(),
            baseline: serde_json::json!(false),
            values: vec![serde_json::json!(false), serde_json::json!(true)],
        })
        .collect()
}

async fn tune_binary_parameters(
    parameters: Vec<tuning::TuningParameter>,
    max_trials: usize,
    interaction: bool,
) -> anyhow::Result<tuning::TuningResult> {
    tuning::tune_endpoint(
        tuning::EndpointTuningRequest {
            configuration: serde_json::json!({
                "p0": false,
                "p1": false,
                "p2": false,
                "p3": false,
                "p4": false,
                "p5": false
            }),
            parameters,
            budget: tuning::TuningBudget {
                max_trials,
                max_duration_ms: Some(5_000),
            },
        },
        CancellationToken::new(),
        move |configuration, _| async move {
            let optimum = if interaction {
                configuration["p2"] == true && configuration["p3"] == true
            } else {
                configuration["p5"] == true
            };
            Ok(if optimum { 100.0 } else { 1.0 })
        },
    )
    .await
}

#[tokio::test]
async fn optimizer_covers_a_last_parameter_independently_of_registration_order(
) -> anyhow::Result<()> {
    let mut parameters = binary_parameters(&["p0", "p1", "p2", "p3", "p4", "p5"]);
    let forward = tune_binary_parameters(parameters.clone(), 7, false).await?;
    parameters.reverse();
    let reversed = tune_binary_parameters(parameters, 7, false).await?;

    assert_eq!(forward, reversed);
    assert_eq!(
        forward.optimized_rows_per_second.to_bits(),
        100.0_f64.to_bits()
    );
    assert_eq!(forward.parameters["/p5"], true);
    assert_eq!(forward.trials, 7);
    Ok(())
}

#[tokio::test]
async fn optimizer_covers_every_pair_before_model_acquisition_independently_of_order(
) -> anyhow::Result<()> {
    let mut parameters = binary_parameters(&["p0", "p1", "p2", "p3"]);
    let forward = tune_binary_parameters(parameters.clone(), 11, true).await?;
    parameters.reverse();
    let reversed = tune_binary_parameters(parameters, 11, true).await?;

    assert_eq!(forward, reversed);
    assert_eq!(
        forward.optimized_rows_per_second.to_bits(),
        100.0_f64.to_bits()
    );
    assert_eq!(forward.parameters["/p2"], true);
    assert_eq!(forward.parameters["/p3"], true);
    assert_eq!(forward.trials, 11);
    Ok(())
}

#[tokio::test]
async fn optimizer_obeys_trial_and_time_budgets() -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let error = tuning::tune_endpoint(endpoint_tuning_request(3), cancellation, move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok(1.0) }
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let result = tuning::tune_endpoint(
        endpoint_tuning_request(3),
        CancellationToken::new(),
        move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(1.0) }
        },
    )
    .await?;
    assert_eq!(result.trials, 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    let mut timed = endpoint_tuning_request(3);
    timed.budget.max_duration_ms = Some(10);
    let started = std::time::Instant::now();
    let error = tuning::tune_endpoint(
        timed,
        CancellationToken::new(),
        |_, cancellation| async move {
            cancellation.cancelled().await;
            Ok(1.0)
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("baseline"));
    assert!(started.elapsed() < Duration::from_millis(500));

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let observed_cleanup = Arc::clone(&cleaned_up);
    let mut timed_candidate = endpoint_tuning_request(3);
    timed_candidate.budget.max_duration_ms = Some(20);
    let result = tuning::tune_endpoint(
        timed_candidate,
        CancellationToken::new(),
        move |_, cancellation| {
            let call = observed.fetch_add(1, Ordering::SeqCst);
            let cleaned_up = Arc::clone(&observed_cleanup);
            async move {
                if call > 0 {
                    cancellation.cancelled().await;
                    cleaned_up.store(true, Ordering::SeqCst);
                }
                Ok(1.0)
            }
        },
    )
    .await?;
    assert_eq!(result.trials, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(cleaned_up.load(Ordering::SeqCst));

    let cancellation = CancellationToken::new();
    let trigger = cancellation.clone();
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let observed_cleanup = Arc::clone(&cleaned_up);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        trigger.cancel();
    });
    let error = tuning::tune_endpoint(endpoint_tuning_request(3), cancellation, move |_, trial| {
        let cleaned_up = Arc::clone(&observed_cleanup);
        async move {
            trial.cancelled().await;
            cleaned_up.store(true, Ordering::SeqCst);
            Ok(1.0)
        }
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert!(cleaned_up.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn optimizer_never_masks_cleanup_failure_at_trial_deadline() {
    let mut request = endpoint_tuning_request(1);
    request.budget.max_duration_ms = Some(10);

    let error = tuning::tune_endpoint(
        request,
        CancellationToken::new(),
        |_, cancellation| async move {
            cancellation.cancelled().await;
            anyhow::bail!("mandatory scratch cleanup failed")
        },
    )
    .await
    .expect_err("cleanup failure must escape the optimizer deadline");

    assert!(error
        .to_string()
        .contains("mandatory scratch cleanup failed"));
}

#[tokio::test]
async fn source_and_sink_tuning_start_in_parallel() -> anyhow::Result<()> {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let source_barrier = Arc::clone(&barrier);
    let destination_barrier = Arc::clone(&barrier);
    let source = tuning::EndpointTuningRequest {
        configuration: serde_json::json!({}),
        parameters: Vec::new(),
        budget: tuning::TuningBudget {
            max_trials: 1,
            max_duration_ms: Some(1_000),
        },
    };
    let destination = source.clone();

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        tuning::tune_source_and_sink(
            source,
            destination,
            CancellationToken::new(),
            move |_, _| {
                let barrier = Arc::clone(&source_barrier);
                async move {
                    barrier.wait().await;
                    Ok(10.0)
                }
            },
            move |_, _| {
                let barrier = Arc::clone(&destination_barrier);
                async move {
                    barrier.wait().await;
                    Ok(20.0)
                }
            },
        ),
    )
    .await??;

    assert_eq!(
        result.source.baseline_rows_per_second.to_bits(),
        10.0_f64.to_bits()
    );
    assert_eq!(
        result.destination.baseline_rows_per_second.to_bits(),
        20.0_f64.to_bits()
    );
    Ok(())
}

#[tokio::test]
async fn source_and_sink_tuning_cancels_and_awaits_sibling_after_first_failure() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let source_barrier = Arc::clone(&barrier);
    let destination_barrier = Arc::clone(&barrier);
    let destination_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&destination_calls);
    let destination_cleaned_up = Arc::new(AtomicBool::new(false));
    let observed_cleanup = Arc::clone(&destination_cleaned_up);
    let source = endpoint_tuning_request(3);
    let destination = endpoint_tuning_request(3);

    let error = tokio::time::timeout(
        Duration::from_millis(500),
        tuning::tune_source_and_sink(
            source,
            destination,
            CancellationToken::new(),
            move |_, _| {
                let barrier = Arc::clone(&source_barrier);
                async move {
                    barrier.wait().await;
                    anyhow::bail!("source baseline failed")
                }
            },
            move |_, cancellation| {
                let barrier = Arc::clone(&destination_barrier);
                let cleaned_up = Arc::clone(&observed_cleanup);
                observed_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    barrier.wait().await;
                    cancellation.cancelled().await;
                    cleaned_up.store(true, Ordering::SeqCst);
                    Err(tuning::TuningEvaluationCancelled.into())
                }
            },
        ),
    )
    .await
    .expect("the sibling must stop promptly")
    .expect_err("the first endpoint failure must fail the pair");

    assert!(error.to_string().contains("source baseline failed"));
    assert_eq!(destination_calls.load(Ordering::SeqCst), 1);
    assert!(destination_cleaned_up.load(Ordering::SeqCst));
}
