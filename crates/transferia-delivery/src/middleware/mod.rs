use std::collections::HashMap;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use transferia_core::{DatasetSchema, DiscoveredDataset, TableData};
use transferia_delivery_contracts::middleware::{Middleware, MiddlewarePreviewContext};
use transferia_registry::table_selection::{CompiledTableRule, PatternMode, TableRule};
use transferia_registry::Registry;

pub mod preview;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MiddlewareEntry {
    #[serde(default = "all_tables")]
    pub tables: TableRule,

    #[serde(flatten)]
    inner: HashMap<String, Value>,
}

impl MiddlewareEntry {
    pub fn build(&self, registry: &Registry) -> anyhow::Result<Box<dyn Middleware>> {
        Ok(Box::new(ScopedMiddleware {
            tables: self.tables.compile()?,
            action: registry.build_middleware(self.kind()?, self.raw()?.clone())?,
        }))
    }

    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!("middleware: no middleware key found"),
            _ => anyhow::bail!("middleware: expected exactly one middleware key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        let kind = self.kind()?;
        self.inner
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("middleware key '{kind}' is missing from config"))
    }
}

fn all_tables() -> TableRule {
    TableRule {
        include: "*".into(),
        exclude: None,
        include_mode: PatternMode::Glob,
        exclude_mode: PatternMode::Glob,
    }
}

pub fn build_middlewares(
    registry: &Registry,
    entries: &[MiddlewareEntry],
) -> anyhow::Result<Vec<Box<dyn Middleware>>> {
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            entry
                .build(registry)
                .with_context(|| format!("transform step {}", index + 1))
        })
        .collect()
}

struct ScopedMiddleware {
    tables: CompiledTableRule,
    action: Box<dyn Middleware>,
}

#[async_trait::async_trait]
impl Middleware for ScopedMiddleware {
    async fn preview(&self, data: TableData, context: MiddlewarePreviewContext) -> anyhow::Result<TableData> {
        if !data.is_dlq && self.applies_to(data.namespace.as_deref(), &data.table) {
            self.action.preview(data, context).await
        } else {
            Ok(data)
        }
    }
    fn applies_to(&self, namespace: Option<&str>, name: &str) -> bool {
        self.tables.matches(namespace, name)
    }

    async fn output_schema(&self, _: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        anyhow::bail!("scoped transform validation requires a dataset identity")
    }

    async fn output_dataset(&self, dataset: &DiscoveredDataset) -> anyhow::Result<DiscoveredDataset> {
        if self.applies_to(dataset.namespace.as_deref(), &dataset.name) {
            self.action.output_dataset(dataset).await
        } else {
            Ok(dataset.clone())
        }
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        if !data.is_dlq && self.applies_to(data.namespace.as_deref(), &data.table) {
            self.action.process(data).await
        } else {
            Ok(data)
        }
    }
}

#[cfg(test)]
mod tests;
