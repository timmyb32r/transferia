use std::collections::HashMap;

use serde::Deserialize;
use serde_yaml::Value;

use crate::metrics::MetricsConfig;
use crate::middleware::MiddlewareEntry;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
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
        let expanded = shellexpand::env(&contents)
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
mod tests {
    use super::*;

    #[test]
    fn rejects_multiple_source_providers() -> anyhow::Result<()> {
        let config: Config = serde_yaml::from_str(
            "source: {a: {}, b: {}}\nsink: {clickhouse: {}}\nmiddlewares: []\n",
        )?;
        anyhow::ensure!(config.source.kind().is_err());
        Ok(())
    }

    #[test]
    fn rejects_provider_specific_top_level_fields() {
        let result = serde_yaml::from_str::<Config>(
            "source: {pqv1: {}}\nsink: {clickhouse: {}}\nrecreate_tables: true\n",
        );
        assert!(result.is_err());
    }

    #[test]
    fn pqv1_to_s3_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
        let config: Config = serde_yaml::from_str(
            r"
source:
  pqv1:
    discovery_endpoint: grpc://localhost
    topic_path: topic-a
    consumer_name: consumer-a
    partition_group_ids: [0]
    auth: { type: access_token, token: test }
    parser:
      common:
        table_naming: { type: from_config, name: events }
        system_columns:
          topic: true
          partition: true
          offset: true
          message_index: true
          write_timestamp_ms: true
      json_parser:
        chunk_splitter: one-message-one-row
        columns:
          - { jsonpath: $.id, column_name: id, arrow_type: Int64, nullable: false }
sink:
  s3:
    bucket: transfer-bucket
    partitioning: { type: source }
keep_system_columns_in_sink: false
",
        )?;
        let source: crate::providers::pqv1::config::PqV1SourceConfig =
            serde_yaml::from_value(config.source.raw()?.clone())?;
        let _: crate::parsers::json_parser::JsonParserConfig =
            serde_yaml::from_value(source.parser.parser.raw()?.clone())?;
        let sink: crate::providers::s3::sink::S3SinkConfig =
            serde_yaml::from_value(config.sink.raw()?.clone())?;
        sink.validate()?;
        Ok(())
    }
}
