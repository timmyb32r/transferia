use std::sync::Arc;

use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value;

use transferia::config::yaml::DeliveryType;
use transferia::metrics::{MetricsConfig, MetricsRegistry};
use transferia::providers::catalog::{build_provider_catalog, ProviderDefinition};

#[derive(JsonSchema)]
#[expect(dead_code, reason = "fields are consumed by the JsonSchema derive")]
struct CommonConfigSchema {
    #[schemars(title = "Delivery type")]
    delivery_type: DeliveryType,

    middlewares: Vec<MiddlewareSchema>,

    #[schemars(title = "Pipeline memory limit", extend("x-ui" = { "widget": "byte_size" }))]
    pipeline_memory_limit_bytes: usize,

    #[schemars(title = "Keep system columns in sink")]
    keep_system_columns_in_sink: bool,

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

pub fn build_ui_catalog() -> anyhow::Result<UiCatalog> {
    let catalog = build_provider_catalog(&Arc::new(MetricsRegistry::new()))?;
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
            "keep_system_columns_in_sink": false,
            "metrics": null
        }),
        providers: catalog.definitions().to_vec(),
    })
}
