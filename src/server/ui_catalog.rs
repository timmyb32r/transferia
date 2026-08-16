use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value;

use transferia::config::yaml::DeliveryType;
use transferia::extension::Transferia;
use transferia::metrics::MetricsConfig;
use transferia::providers::catalog::ProviderDefinition;

#[derive(JsonSchema)]
#[expect(dead_code, reason = "fields are consumed by the JsonSchema derive")]
struct CommonConfigSchema {
    #[schemars(title = "Delivery type")]
    delivery_type: DeliveryType,

    middlewares: Vec<MiddlewareSchema>,

    #[schemars(title = "Pipeline memory limit", extend("x-ui" = { "widget": "byte_size" }))]
    pipeline_memory_limit_bytes: usize,

    metrics: Option<MetricsConfig>,
}

#[derive(JsonSchema)]
#[serde(rename_all = "lowercase")]
#[expect(dead_code, reason = "variants are consumed by the JsonSchema derive")]
enum MiddlewareSchema {
    Filter(transferia::middleware::filter::FilterConfig),
}

#[derive(Clone, Debug, Serialize)]
pub struct UiCatalog {
    pub common_schema: Value,
    pub initial: Value,
    pub providers: Vec<ProviderDefinition>,
}

#[cfg(test)]
pub fn build_ui_catalog() -> anyhow::Result<UiCatalog> {
    build_ui_catalog_with(&Transferia::public()?)
}

pub fn build_ui_catalog_with(transferia: &Transferia) -> anyhow::Result<UiCatalog> {
    Ok(UiCatalog {
        common_schema: serde_json::to_value(schema_for!(CommonConfigSchema))?,
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
