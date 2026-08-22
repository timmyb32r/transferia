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
