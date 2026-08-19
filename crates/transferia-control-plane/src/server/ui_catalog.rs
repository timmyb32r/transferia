use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value;

use transferia_delivery::delivery::config::yaml::DeliveryType;
use transferia_providers::extension::Transferia;
use transferia_providers::metrics::MetricsConfig;
use transferia_registry::{Composition, MiddlewareDefinition, ProviderDefinition};

#[derive(JsonSchema)]
#[expect(dead_code, reason = "fields are consumed by the JsonSchema derive")]
struct CommonConfigSchema {
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

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UiCatalog {
    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonSchema")
    )]
    pub common_schema: Value,

    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub initial: Value,

    pub providers: Vec<ProviderDefinition>,
}

pub fn build_ui_catalog() -> anyhow::Result<UiCatalog> {
    build_ui_catalog_with(&Transferia::public()?)
}

pub fn build_ui_catalog_with(transferia: &Transferia) -> anyhow::Result<UiCatalog> {
    let registry = transferia.build_registry(&std::sync::Arc::new(
        transferia_providers::metrics::MetricsRegistry::new(),
    ))?;
    let mut common_schema = serde_json::to_value(schema_for!(CommonConfigSchema))?;
    common_schema["properties"]["middlewares"]["items"] =
        middleware_schema(registry.middleware_definitions());
    Ok(UiCatalog {
        common_schema,
        initial: serde_json::json!({
            "delivery_id": "demo-delivery",
            "durable_storage": { "type": "local_file", "path": ".transferia-state" },
            "delivery_type": null,
            "source": {},
            "sink": {},
            "middlewares": [],
            "pipeline_memory_limit_bytes": 268_435_456,
            "metrics": null
        }),
        providers: transferia.composition().provider_definitions().to_vec(),
    })
}

fn middleware_schema(definitions: &[MiddlewareDefinition]) -> Value {
    serde_json::json!({
        "oneOf": definitions
            .iter()
            .map(|definition| serde_json::json!({
                "type": "object",
                "properties": { definition.key: definition.schema },
                "required": [definition.key],
                "additionalProperties": false
            }))
            .collect::<Vec<_>>()
    })
}

#[cfg(test)]
#[path = "tests/ui_catalog.rs"]
mod tests;
