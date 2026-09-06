use schemars::{schema_for, JsonSchema};
use serde_json::Value;

use transferia_connectors::extension::Transferia;
use transferia_delivery_contracts::metrics::MetricsConfig;
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::{Composition, MiddlewareDefinition};
pub use transferia_server_contracts::api::UiCatalog;

#[derive(JsonSchema)]
#[expect(dead_code, reason = "fields are consumed by the JsonSchema derive")]
struct CommonConfigSchema {
    #[schemars(
        title = "Delivery name",
        default = "default_delivery_name",
        extend("x-ui" = { "widget": "hidden" })
    )]
    delivery_name: String,

    #[schemars(title = "Delivery type")]
    delivery_type: DeliveryType,

    #[schemars(
        title = "Transforms",
        extend("x-ui" = { "widget": "middlewares" })
    )]
    middlewares: Vec<Value>,

    #[schemars(title = "Pipeline memory limit", extend("x-ui" = { "widget": "byte_size" }))]
    pipeline_memory_limit_bytes: usize,

    metrics: Option<MetricsConfig>,
}

fn default_delivery_name() -> String {
    "Untitled delivery".to_owned()
}

pub fn build_ui_catalog() -> anyhow::Result<UiCatalog> {
    build_ui_catalog_with(&Transferia::public()?)
}

pub fn build_ui_catalog_with(transferia: &Transferia) -> anyhow::Result<UiCatalog> {
    let registry = transferia.build_registry(&std::sync::Arc::new(
        transferia_connectors::metrics::MetricsRegistry::new(),
    ))?;
    let mut common_schema = serde_json::to_value(schema_for!(CommonConfigSchema))?;
    common_schema["properties"]["middlewares"]["items"] =
        middleware_schema(registry.middleware_definitions())?;
    Ok(UiCatalog {
        common_schema,
        initial: serde_json::json!({
            "delivery_id": "demo-delivery",
            "delivery_name": "Untitled delivery",
            "durable_storage": { "type": "local_file", "path": ".transferia-state" },
            "delivery_type": null,
            "source": {},
            "sink": {},
            "middlewares": [],
            "pipeline_memory_limit_bytes": 1_073_741_824,
            "metrics": null
        }),
        connectors: transferia.composition().connector_definitions().to_vec(),
    })
}

fn middleware_schema(definitions: &[MiddlewareDefinition]) -> anyhow::Result<Value> {
    // The scope is embedded below an action variant, so local references from
    // an independent schema root must be inlined before composition.
    let tables = serde_json::to_value(
        schemars::generate::SchemaSettings::default()
            .with(|settings| settings.inline_subschemas = true)
            .into_generator()
            .into_root_schema_for::<transferia_registry::table_selection::TableRule>(),
    )?;
    Ok(serde_json::json!({
        "oneOf": definitions
            .iter()
            .map(|definition| serde_json::json!({
                "title": definition.title,
                "x-ui": {
                    "capabilities": {
                        "component": "transformer",
                        "key": definition.key,
                        "properties": ["playground"]
                    }
                },
                "type": "object",
                "properties": { definition.key: definition.schema, "tables": tables },
                "required": [definition.key],
                "additionalProperties": false
            }))
            .collect::<Vec<_>>()
    }))
}

#[cfg(test)]
#[path = "tests/ui_catalog.rs"]
mod tests;
