use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;
use transferia_delivery_contracts::semantics::RecordSemantics;

use crate::{DeliveryMode, EndpointRole};

const EXTERNAL_LINK_VALUE_PLACEHOLDER: &str = "{value}";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UiHints {
    table_membership: Option<TableMembership>,
    widget: Option<String>,
    section: Option<UiSection>,
    initial_items: Option<usize>,
    dynamic_options: Option<String>,
    dynamic_options_dependencies: Option<BTreeMap<String, String>>,
    dynamic_options_control: Option<DynamicOptionsControl>,
    dynamic_options_path_syntax: Option<DynamicOptionsPathSyntax>,
    dynamic_options_entity: Option<DynamicOptionsEntity>,
    external_link_template: Option<String>,
    external_link_dependencies: Option<BTreeMap<String, String>>,
    labels: Option<BTreeMap<String, String>>,
    options: Option<Vec<Value>>,
    control_width: Option<String>,
    item_label: Option<String>,
    order: Option<i64>,
    reveal_rest_on_selection: Option<bool>,
    defer_variant_details: Option<bool>,
    indent_variant_details: Option<bool>,
    delivery_types: Option<Vec<UiDeliveryType>>,
    capabilities: Option<UiCapabilities>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TableMembership {
    Fixed,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UiCapabilities {
    component: UiCapabilityComponent,
    key: String,
    delivery_modes: Option<Vec<UiDeliveryType>>,
    record_semantics: Option<Vec<UiRecordSemantics>>,
    properties: Option<Vec<String>>,
    batch_stream_handoff: Option<UiBatchStreamHandoff>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UiBatchStreamHandoff {
    ExactSwitchover,
    Overlapping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum UiCapabilityComponent {
    Source,
    Destination,
    Parser,
    Serializer,
    Transformer,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum UiRecordSemantics {
    AppendOnly,
    Changelog,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UiSection {
    Advanced,
    AdvancedParquet,
    SystemColumns,
    ShardGroup,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum UiDeliveryType {
    Batch,
    Stream,
    BatchAndStream,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DynamicOptionsControl {
    Path,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DynamicOptionsPathSyntax {
    Plain,
    DoubleSlashAbsolute,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DynamicOptionsEntity {
    Table,
    Topic,
    Consumer,
}

pub fn validate_ui_dialect(value: &Value) -> anyhow::Result<()> {
    validate_node(value, value, "#")
}

fn validate_node(root: &Value, value: &Value, path: &str) -> anyhow::Result<()> {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_node(root, value, &format!("{path}/{index}"))?;
            }
        }
        Value::Object(object) => {
            for key in object.keys() {
                anyhow::ensure!(
                    !key.starts_with("x-") || key == "x-ui",
                    "{path}: unsupported JSON Schema extension keyword: {key}"
                );
            }
            validate_hidden_required_scalars(root, object, path)?;
            if let Some(hints) = object.get("x-ui") {
                let hints: UiHints = serde_json::from_value(hints.clone())
                    .map_err(|error| anyhow::anyhow!("{path}: invalid x-ui contract: {error}"))?;
                if let Some(dependencies) = hints.dynamic_options_dependencies {
                    anyhow::ensure!(
                        dependencies
                            .values()
                            .all(|pointer| pointer.starts_with('/')),
                        "{path}: dynamic option dependencies must be absolute JSON pointers"
                    );
                }
                if let Some(capabilities) = &hints.capabilities {
                    if capabilities.batch_stream_handoff.is_some() {
                        anyhow::ensure!(
                            capabilities.component == UiCapabilityComponent::Source
                                && capabilities.delivery_modes.as_ref().is_some_and(
                                    |modes| modes.contains(&UiDeliveryType::BatchAndStream)
                                ),
                            "{path}: batch_stream_handoff requires a batch_and_stream source"
                        );
                    }
                    anyhow::ensure!(
                        !capabilities.key.is_empty(),
                        "{path}: capability key must not be empty"
                    );
                    anyhow::ensure!(
                        capabilities
                            .properties
                            .as_ref()
                            .is_none_or(|values| values.iter().all(|value| !value.is_empty())),
                        "{path}: capability properties must not contain empty values"
                    );
                    let _ = (
                        &capabilities.component,
                        &capabilities.delivery_modes,
                        &capabilities.record_semantics,
                    );
                }
                if let Some(dependencies) = &hints.external_link_dependencies {
                    anyhow::ensure!(
                        dependencies.iter().all(|(name, pointer)| {
                            !name.is_empty()
                                && name.bytes().all(|byte| {
                                    byte.is_ascii_alphanumeric() || byte == b'_'
                                })
                                && pointer.starts_with('/')
                        }),
                        "{path}: external link dependencies must have safe names and absolute JSON pointers"
                    );
                }
                if let Some(template) = &hints.external_link_template {
                    validate_external_link_template(
                        path,
                        template,
                        hints.external_link_dependencies.as_ref(),
                    )?;
                }
                for (name, value) in [
                    ("widget", hints.widget.as_deref()),
                    ("dynamic_options", hints.dynamic_options.as_deref()),
                    ("control_width", hints.control_width.as_deref()),
                    ("item_label", hints.item_label.as_deref()),
                ] {
                    if let Some(value) = value {
                        anyhow::ensure!(!value.is_empty(), "{path}: x-ui {name} must not be empty");
                    }
                }
                drop((
                    hints.section,
                    hints.initial_items,
                    hints.dynamic_options_control,
                    hints.dynamic_options_path_syntax,
                    hints.dynamic_options_entity,
                    hints.external_link_dependencies,
                    hints.labels,
                    hints.options,
                    hints.order,
                    hints.reveal_rest_on_selection,
                    hints.defer_variant_details,
                    hints.indent_variant_details,
                    hints.delivery_types,
                    hints.table_membership,
                ));
            }
            for (key, value) in object {
                validate_node(root, value, &format!("{path}/{key}"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn validate_endpoint_capabilities(
    value: &Value,
    role: EndpointRole,
    delivery_modes: &[DeliveryMode],
    record_semantics: &[RecordSemantics],
) -> anyhow::Result<()> {
    validate_endpoint_capability_node(value, "#", role, delivery_modes, record_semantics)
}

fn validate_endpoint_capability_node(
    value: &Value,
    path: &str,
    role: EndpointRole,
    delivery_modes: &[DeliveryMode],
    record_semantics: &[RecordSemantics],
) -> anyhow::Result<()> {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_endpoint_capability_node(
                    value,
                    &format!("{path}/{index}"),
                    role,
                    delivery_modes,
                    record_semantics,
                )?;
            }
        }
        Value::Object(object) => {
            if let Some(hints) = object.get("x-ui") {
                let hints: UiHints = serde_json::from_value(hints.clone())
                    .map_err(|error| anyhow::anyhow!("{path}: invalid x-ui contract: {error}"))?;
                if let Some(capabilities) = hints.capabilities {
                    validate_one_endpoint_capability(
                        path,
                        &capabilities,
                        role,
                        delivery_modes,
                        record_semantics,
                    )?;
                }
            }
            for (key, value) in object {
                validate_endpoint_capability_node(
                    value,
                    &format!("{path}/{key}"),
                    role,
                    delivery_modes,
                    record_semantics,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_one_endpoint_capability(
    path: &str,
    capabilities: &UiCapabilities,
    role: EndpointRole,
    delivery_modes: &[DeliveryMode],
    record_semantics: &[RecordSemantics],
) -> anyhow::Result<()> {
    let expected_component = match role {
        EndpointRole::Source => UiCapabilityComponent::Source,
        EndpointRole::Sink => UiCapabilityComponent::Destination,
    };
    if !matches!(
        capabilities.component,
        UiCapabilityComponent::Source | UiCapabilityComponent::Destination
    ) {
        anyhow::ensure!(
            capabilities.delivery_modes.is_none(),
            "{path}: component capabilities cannot declare endpoint delivery_modes"
        );
        return Ok(());
    }
    anyhow::ensure!(
        capabilities.component == expected_component,
        "{path}: endpoint capability component does not match its registered role"
    );
    anyhow::ensure!(
        capabilities.properties.is_none(),
        "{path}: endpoint capabilities cannot declare component properties"
    );
    let declared_semantics = capabilities.record_semantics.as_deref().ok_or_else(|| {
        anyhow::anyhow!("{path}: endpoint capabilities must declare record_semantics")
    })?;
    anyhow::ensure!(
        !declared_semantics.is_empty() && all_unique(declared_semantics),
        "{path}: endpoint record_semantics must be non-empty and unique"
    );
    anyhow::ensure!(
        declared_semantics
            .iter()
            .all(|semantics| record_semantics.contains(&match semantics {
                UiRecordSemantics::AppendOnly => RecordSemantics::AppendOnly,
                UiRecordSemantics::Changelog => RecordSemantics::Changelog,
            })),
        "{path}: endpoint record_semantics must be a subset of the registered aggregate"
    );
    match capabilities.component {
        UiCapabilityComponent::Source => {
            let declared_modes = capabilities.delivery_modes.as_deref().ok_or_else(|| {
                anyhow::anyhow!("{path}: source capabilities must declare delivery_modes")
            })?;
            anyhow::ensure!(
                !declared_modes.is_empty() && all_unique(declared_modes),
                "{path}: source delivery_modes must be non-empty and unique"
            );
            anyhow::ensure!(
                declared_modes
                    .iter()
                    .all(|mode| delivery_modes.contains(&match mode {
                        UiDeliveryType::Batch => DeliveryMode::Batch,
                        UiDeliveryType::Stream => DeliveryMode::Stream,
                        UiDeliveryType::BatchAndStream => DeliveryMode::BatchAndStream,
                    })),
                "{path}: source delivery_modes must be a subset of the registered aggregate"
            );
        }
        UiCapabilityComponent::Destination => {
            if let Some(modes) = &capabilities.delivery_modes {
                anyhow::ensure!(
                    !modes.is_empty() && all_unique(modes),
                    "{path}: destination delivery_modes must be non-empty and unique"
                );
            }
        }
        UiCapabilityComponent::Parser
        | UiCapabilityComponent::Serializer
        | UiCapabilityComponent::Transformer => {
            anyhow::bail!("{path}: non-endpoint capabilities reached endpoint validation")
        }
    }
    Ok(())
}

fn all_unique<T: Eq>(values: &[T]) -> bool {
    !values
        .iter()
        .enumerate()
        .any(|(index, value)| values[index + 1..].contains(value))
}

fn validate_external_link_template(
    path: &str,
    template: &str,
    dependencies: Option<&BTreeMap<String, String>>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        template.starts_with("https://")
            && template.matches(EXTERNAL_LINK_VALUE_PLACEHOLDER).count() == 1,
        "{path}: external link template must be HTTPS and contain one {{value}}"
    );
    let mut unmatched = template.replace(EXTERNAL_LINK_VALUE_PLACEHOLDER, "");
    for name in dependencies.into_iter().flat_map(BTreeMap::keys) {
        let placeholder = format!("{{{name}}}");
        anyhow::ensure!(
            template.matches(&placeholder).count() == 1,
            "{path}: external link template must contain one {placeholder}"
        );
        unmatched = unmatched.replace(&placeholder, "");
    }
    anyhow::ensure!(
        !unmatched.contains(['{', '}']),
        "{path}: external link template contains an undeclared placeholder"
    );
    Ok(())
}

fn validate_hidden_required_scalars(
    root: &Value,
    object: &serde_json::Map<String, Value>,
    path: &str,
) -> anyhow::Result<()> {
    let Some(properties) = object.get("properties").and_then(Value::as_object) else {
        return Ok(());
    };
    let Some(required) = object.get("required").and_then(Value::as_array) else {
        return Ok(());
    };
    for name in required.iter().filter_map(Value::as_str) {
        let Some(property) = properties.get(name).and_then(Value::as_object) else {
            continue;
        };
        let property = resolve_local_schema(root, property, &format!("{path}/properties/{name}"))?;
        let explicitly_hidden = property
            .get("x-ui")
            .and_then(Value::as_object)
            .and_then(|hints| hints.get("widget"))
            .and_then(Value::as_str)
            == Some("hidden");
        let singleton_enum = property
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.len() == 1);
        let constant = property.contains_key("const");
        let hidden = explicitly_hidden || singleton_enum || constant;
        let scalar = matches!(
            property.get("type").and_then(Value::as_str),
            Some("string" | "number" | "integer" | "boolean")
        );
        if hidden && scalar {
            let materialized = property
                .get("default")
                .or_else(|| property.get("const"))
                .or_else(|| {
                    property
                        .get("enum")
                        .and_then(Value::as_array)
                        .filter(|values| values.len() == 1)
                        .and_then(|values| values.first())
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{path}/properties/{name}: hidden required scalar must declare or imply a deterministic value"
                    )
                })?;
            validate_scalar_default(
                &property,
                materialized,
                &format!("{path}/properties/{name}"),
            )?;
        }
    }
    Ok(())
}

fn resolve_local_schema(
    root: &Value,
    schema: &serde_json::Map<String, Value>,
    path: &str,
) -> anyhow::Result<serde_json::Map<String, Value>> {
    let mut resolved = schema.clone();
    let mut seen = BTreeSet::new();
    while let Some(reference) = resolved.get("$ref").and_then(Value::as_str) {
        anyhow::ensure!(
            reference.starts_with("#/"),
            "{path}: external schema reference is not supported: {reference}"
        );
        anyhow::ensure!(
            seen.insert(reference.to_owned()),
            "{path}: cyclic schema reference: {reference}"
        );
        let target = root
            .pointer(&reference[1..])
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("{path}: unresolved schema reference: {reference}"))?;
        let mut merged = target.clone();
        for (name, value) in &resolved {
            if name != "$ref" {
                merged.insert(name.clone(), value.clone());
            }
        }
        resolved = merged;
    }
    Ok(resolved)
}

fn validate_scalar_default(
    schema: &serde_json::Map<String, Value>,
    default: &Value,
    path: &str,
) -> anyhow::Result<()> {
    match schema.get("type").and_then(Value::as_str) {
        Some("string") => {
            anyhow::ensure!(default.is_string(), "{path}: default must be a string");
            anyhow::ensure!(
                default.as_str().is_some_and(|value| !value.is_empty()),
                "{path}: required hidden string default must not be empty"
            );
            if let Some(values) = schema.get("enum").and_then(Value::as_array) {
                anyhow::ensure!(values.contains(default), "{path}: default is not in enum");
            }
        }
        Some("boolean") => {
            anyhow::ensure!(default.is_boolean(), "{path}: default must be a boolean");
        }
        Some("integer") => {
            anyhow::ensure!(
                default.is_i64() || default.is_u64(),
                "{path}: default must be an integer"
            );
            validate_numeric_range(schema, default, path)?;
        }
        Some("number") => {
            anyhow::ensure!(default.is_number(), "{path}: default must be a number");
            validate_numeric_range(schema, default, path)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_numeric_range(
    schema: &serde_json::Map<String, Value>,
    default: &Value,
    path: &str,
) -> anyhow::Result<()> {
    let value = default
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("{path}: numeric default is not finite"))?;
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64) {
        anyhow::ensure!(value >= minimum, "{path}: default is below minimum");
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64) {
        anyhow::ensure!(value <= maximum, "{path}: default exceeds maximum");
    }
    Ok(())
}
