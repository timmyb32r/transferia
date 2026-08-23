use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

const EXTERNAL_LINK_VALUE_PLACEHOLDER: &str = "{value}";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UiHints {
    widget: Option<String>,
    section: Option<UiSection>,
    initial_items: Option<usize>,
    dynamic_options: Option<String>,
    dynamic_options_dependencies: Option<BTreeMap<String, String>>,
    dynamic_options_control: Option<DynamicOptionsControl>,
    external_link_template: Option<String>,
    labels: Option<BTreeMap<String, String>>,
    options: Option<Vec<Value>>,
    control_width: Option<String>,
    item_label: Option<String>,
    order: Option<i64>,
    reveal_rest_on_selection: Option<bool>,
    defer_variant_details: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UiSection {
    Advanced,
    AdvancedParquet,
    SystemColumns,
    ShardGroup,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DynamicOptionsControl {
    Path,
}

pub fn validate_ui_dialect(value: &Value) -> anyhow::Result<()> {
    validate_node(value, "#")
}

fn validate_node(value: &Value, path: &str) -> anyhow::Result<()> {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_node(value, &format!("{path}/{index}"))?;
            }
        }
        Value::Object(object) => {
            validate_hidden_required_defaults(object, path)?;
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
                if let Some(template) = hints.external_link_template {
                    anyhow::ensure!(
                        template.starts_with("https://")
                            && template.matches(EXTERNAL_LINK_VALUE_PLACEHOLDER).count() == 1,
                        "{path}: external link template must be HTTPS and contain one {{value}}"
                    );
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
                    hints.labels,
                    hints.options,
                    hints.order,
                    hints.reveal_rest_on_selection,
                    hints.defer_variant_details,
                ));
            }
            for (key, value) in object {
                validate_node(value, &format!("{path}/{key}"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_hidden_required_defaults(
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
        let hidden = property
            .get("x-ui")
            .and_then(Value::as_object)
            .and_then(|hints| hints.get("widget"))
            .and_then(Value::as_str)
            == Some("hidden");
        let scalar = matches!(
            property.get("type").and_then(Value::as_str),
            Some("string" | "number" | "integer" | "boolean")
        );
        if hidden && scalar {
            let default = property.get("default").ok_or_else(|| {
                anyhow::anyhow!(
                    "{path}/properties/{name}: hidden required scalar must declare a default"
                )
            })?;
            validate_scalar_default(property, default, &format!("{path}/properties/{name}"))?;
        }
    }
    Ok(())
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
