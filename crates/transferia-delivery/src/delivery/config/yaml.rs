use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::middleware::MiddlewareEntry;
use transferia_delivery_contracts::metrics::MetricsConfig;
pub use transferia_delivery_contracts::DeliveryType;
use transferia_registry::durable::DurableStorageConfig;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub delivery_id: String,

    pub durable_storage: DurableStorageConfig,

    pub delivery_type: DeliveryType,

    pub source: SourceEntry,

    pub sink: SinkEntry,

    #[serde(default)]
    pub middlewares: Vec<MiddlewareEntry>,

    #[serde(default = "default_pipeline_memory_limit")]
    pub pipeline_memory_limit_bytes: usize,

    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("Failed to read config file '{}': {error}", path.display())
        })?;
        Self::from_yaml(&contents)
    }

    pub fn from_yaml(contents: &str) -> anyhow::Result<Self> {
        serde_yaml::from_str(contents)
            .map_err(|error| anyhow::anyhow!("Failed to parse YAML config: {error}"))
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SourceEntry {
    #[serde(flatten)]
    inner: HashMap<String, Value>,
}

impl SourceEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        single_entry_kind("source", &self.inner)
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        entry_value("source", &self.inner)
    }

    pub(crate) fn replace_raw(&mut self, kind: String, value: Value) {
        self.inner.clear();
        self.inner.insert(kind, value);
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SinkEntry {
    #[serde(flatten)]
    inner: HashMap<String, Value>,
}

impl SinkEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        single_entry_kind("sink", &self.inner)
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        entry_value("sink", &self.inner)
    }

    pub(crate) fn replace_raw(&mut self, kind: String, value: Value) {
        self.inner.clear();
        self.inner.insert(kind, value);
    }
}

fn single_entry_kind<'entry>(
    section: &str,
    entries: &'entry HashMap<String, Value>,
) -> anyhow::Result<&'entry str> {
    let keys: Vec<&str> = entries.keys().map(String::as_str).collect();
    match *keys.as_slice() {
        [single] => Ok(single),
        [] => anyhow::bail!("{section}: no provider key found"),
        _ => anyhow::bail!("{section}: expected exactly one provider key, got {keys:?}"),
    }
}

fn entry_value<'entry>(
    section: &str,
    entries: &'entry HashMap<String, Value>,
) -> anyhow::Result<&'entry Value> {
    let kind = single_entry_kind(section, entries)?;
    entries
        .get(kind)
        .ok_or_else(|| anyhow::anyhow!("{section}: provider key '{kind}' is missing from config"))
}

const fn default_pipeline_memory_limit() -> usize {
    256 * 1024 * 1024
}

#[cfg(test)]
#[path = "tests/yaml.rs"]
mod tests;
