use std::collections::HashMap;

use serde::Deserialize;
use serde_yaml::Value;

use crate::durable::DurableStorageConfig;
use crate::metrics::MetricsConfig;
use crate::middleware::MiddlewareEntry;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub delivery_id: String,

    pub durable_storage: DurableStorageConfig,

    pub source: SourceEntry,

    pub sink: SinkEntry,

    #[serde(default)]
    pub middlewares: Vec<MiddlewareEntry>,

    #[serde(default = "default_pipeline_memory_limit")]
    pub pipeline_memory_limit_bytes: usize,

    #[serde(default)]
    pub keep_system_columns_in_sink: bool,

    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|error| anyhow::anyhow!("Failed to read config file '{path}': {error}"))?;
        Self::from_yaml(&contents)
    }

    pub fn from_yaml(contents: &str) -> anyhow::Result<Self> {
        let expanded = shellexpand::env(contents)
            .map_err(|error| anyhow::anyhow!("Failed to expand env vars in config: {error}"))?;
        serde_yaml::from_str(&expanded)
            .map_err(|error| anyhow::anyhow!("Failed to parse YAML config: {error}"))
    }
}

#[derive(Deserialize)]
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
}

#[derive(Deserialize)]
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
